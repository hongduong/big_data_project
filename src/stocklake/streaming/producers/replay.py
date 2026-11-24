"""Replay simulator: phát lại nến 1 phút lịch sử vào topic intraday_prices.

- Lấy nến phút từ VCI qua stocklake.sources (<= 2 req/s), cache JSON Lines trong
  <COLLECTOR_STATE_DIR>/replay_cache để chạy lại không gọi VCI nữa.
- Trộn mọi mã theo thời gian nến, gửi với tốc độ mục tiêu (msg/giây),
  source = 'replay', replay_run_id để đối chiếu với raw.
- Logic thuần (bản tin, trộn, nhịp gửi) tách khỏi I/O Kafka để test bằng producer giả.

SourceBlockedError (HTTP 429/403) được để nổi lên: dừng ngay, không thử lại.
"""

from __future__ import annotations

import heapq
import json
import os
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from pathlib import Path
from typing import Any, NamedTuple

from stocklake.common.config import Settings
from stocklake.common.logging import get_logger
from stocklake.contracts import available_at as avail
from stocklake.contracts.available_at import VN_TZ
from stocklake.contracts.topics import INTRADAY_PRICES

log = get_logger(__name__)

UTC = timezone.utc
SOURCE = "replay"
DEFAULT_STATE_DIR = "/var/lib/stocklake/state"
CACHE_SUBDIR = "replay_cache"
# Số nến phút tối đa một ngày (dư so với ~250 nến thật) để tính countBack.
BARS_PER_DAY_MAX = 400
# Chỉ số dùng để suy ra các ngày giao dịch gần nhất.
CALENDAR_SYMBOL = "VNINDEX"
SESSION_CLOSE_VN = dtime(15, 0)


class ReplayBar(NamedTuple):
    """Nến phút đã cache; bar_ts là epoch ms (UTC)."""

    symbol: str
    bar_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class ReplayResult:
    run_id: str
    sent: int
    delivered: int
    failed: int
    seconds: float
    first_bar_ts: int | None  # epoch ms
    last_bar_ts: int | None  # epoch ms

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))


# ----------------------------------------------------------------- thư mục state


def state_dir(path: str | os.PathLike | None = None) -> Path:
    """Thư mục state của collector: tham số > env COLLECTOR_STATE_DIR > mặc định."""
    return Path(path or os.environ.get("COLLECTOR_STATE_DIR") or DEFAULT_STATE_DIR)


def atomic_write_text(path: Path, text: str) -> None:
    """Ghi file qua file tạm + os.replace để không để lại file dở."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# ----------------------------------------------------------------- thuần: bản tin


def new_run_id(now: datetime | None = None) -> str:
    """Vd 'r20261124T030000Z-1a2b3c4d'."""
    now = now or datetime.now(UTC)
    return f"r{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def available_at_ms(bar_ts_ms: int) -> int:
    """available_at (epoch ms) theo contracts.available_at.intraday_bar."""
    start = datetime.fromtimestamp(bar_ts_ms / 1000, UTC)
    return round(avail.intraday_bar(start).timestamp() * 1000)


def bar_to_message(
    bar: ReplayBar, *, source: str, produced_ts: int, replay_run_id: str | None
) -> dict[str, Any]:
    """Nến -> dict đúng contract topic intraday_prices (đủ trường, đúng thứ tự)."""
    return {
        "symbol": bar.symbol,
        "bar_ts": int(bar.bar_ts),
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": int(bar.volume),
        "source": source,
        "produced_ts": int(produced_ts),
        "available_at": available_at_ms(bar.bar_ts),
        "replay_run_id": replay_run_id,
    }


def encode_message(msg: Mapping[str, Any]) -> tuple[bytes, bytes]:
    """(key, value) nhanh, cùng định dạng với producers.base.serialize.

    Không kiểm schema từng bản tin (bar_to_message luôn đủ trường); test đối chiếu
    với serialize để giữ hai bên khớp.
    """
    key = msg["symbol"].encode("utf-8")
    value = json.dumps(msg, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return key, value


def merge_bars(bars_by_symbol: Mapping[str, Iterable[ReplayBar]]) -> Iterator[ReplayBar]:
    """Trộn nến của các mã theo (bar_ts, symbol). Mỗi danh sách vào phải tăng dần."""
    streams = [iter(sorted(bars, key=lambda b: b.bar_ts)) for bars in bars_by_symbol.values()]
    return heapq.merge(*streams, key=lambda b: (b.bar_ts, b.symbol))


# ----------------------------------------------------------------- thuần: nhịp gửi


class Pacer:
    """Điều tốc theo lịch: bản tin thứ i được gửi không sớm hơn t0 + i/rate.

    `take()` trả số bản tin được gửi ngay (>= 1), ngủ khi đi trước lịch.
    Ở tốc độ cao mỗi lần thức gửi cả lô nên không ngủ theo từng bản tin.
    Chậm hơn lịch quá `max_lag_s` thì dời t0 để không dồn cục quá lớn.
    """

    def __init__(
        self,
        rate: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        min_sleep: float = 0.001,
        max_batch: int | None = None,
        max_lag_s: float = 1.0,
    ):
        if rate <= 0:
            raise ValueError("rate phải > 0")
        self.rate = float(rate)
        self._clock = clock
        self._sleep = sleep
        self.min_sleep = min_sleep
        # Lô tối đa ~10 ms bản tin.
        self.max_batch = max_batch or max(1, int(self.rate / 100))
        self.max_lag_s = max_lag_s
        self.t0: float | None = None
        self.issued = 0
        self.sleeps = 0

    def take(self) -> int:
        if self.t0 is None:
            self.t0 = self._clock()
        while True:
            now = self._clock()
            due = int((now - self.t0) * self.rate) + 1  # số bản tin đến hạn tính tới now
            lag = due - self.issued
            if lag > self.rate * self.max_lag_s:
                self.t0 = now - self.issued / self.rate
                lag = 1
            if lag >= 1:
                n = min(lag, self.max_batch)
                self.issued += n
                return n
            wait = self.t0 + self.issued / self.rate - now
            self.sleeps += 1
            self._sleep(max(wait, self.min_sleep))


# ----------------------------------------------------------------- I/O Kafka


class DeliveryCounter:
    """Đếm kết quả giao nhận từ callback của confluent_kafka."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0
        self.first_error: str | None = None

    def __call__(self, err: Any, _msg: Any) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            if self.first_error is None:
                self.first_error = str(err)


def produce_with_backpressure(
    producer: Any, topic: str, key: bytes, value: bytes, on_delivery: Callable
) -> None:
    """produce(); hàng đợi local đầy (BufferError) thì poll cho rảnh rồi thử lại."""
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
            return
        except BufferError:
            producer.poll(0.05)


def flush_all(producer: Any, timeout_s: float, clock: Callable[[], float] = time.monotonic) -> int:
    """Flush tới khi hết hàng đợi hoặc hết giờ; trả số bản tin còn chưa xong."""
    deadline = clock() + timeout_s
    remaining = producer.flush(max(0.0, min(5.0, timeout_s)))
    while remaining and clock() < deadline:
        remaining = producer.flush(min(5.0, max(0.0, deadline - clock())))
    return int(remaining)


def publish_bars(
    bars: Iterable[ReplayBar],
    producer: Any,
    *,
    rate: float,
    run_id: str,
    topic: str = INTRADAY_PRICES.name,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    wall_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    flush_timeout_s: float = 120.0,
) -> ReplayResult:
    """Gửi nến theo thứ tự với tốc độ `rate` msg/giây, flush, trả kết quả đếm chính xác.

    Bản tin chưa giao xong sau flush được tính vào `failed`.
    """
    counter = DeliveryCounter()
    pacer = Pacer(rate, clock=clock, sleep=sleep)
    it = iter(bars)
    sent = 0
    first_ts: int | None = None
    last_ts: int | None = None
    t_start = clock()
    done = False
    while not done:
        quota = pacer.take()
        for _ in range(quota):
            bar = next(it, None)
            if bar is None:
                done = True
                break
            msg = bar_to_message(bar, source=SOURCE, produced_ts=wall_ms(), replay_run_id=run_id)
            key, value = encode_message(msg)
            produce_with_backpressure(producer, topic, key, value, counter)
            sent += 1
            if first_ts is None:
                first_ts = bar.bar_ts
            last_ts = bar.bar_ts
        producer.poll(0)
    remaining = flush_all(producer, flush_timeout_s, clock=clock)
    seconds = clock() - t_start
    failed = counter.failed
    if remaining:
        log.error("Replay %s: %d bản tin chưa giao xong sau flush", run_id, remaining)
        failed += remaining
    if counter.first_error:
        log.error("Replay %s: lỗi giao nhận đầu tiên: %s", run_id, counter.first_error)
    # Callback có thể chưa về hết dù flush trả 0 (không xảy ra với confluent_kafka).
    lost = sent - counter.delivered - failed
    if lost > 0:
        log.error("Replay %s: %d bản tin không rõ kết quả, tính là lỗi", run_id, lost)
        failed += lost
    return ReplayResult(
        run_id, sent, counter.delivered, failed, round(seconds, 3), first_ts, last_ts
    )


# ----------------------------------------------------------------- nạp nến (VCI + cache)


def cache_path(cache_dir: Path, symbol: str, start: date, end: date) -> Path:
    return cache_dir / f"{symbol}_{start.isoformat()}_{end.isoformat()}.jsonl"


def read_cache(path: Path) -> list[ReplayBar]:
    out = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                d = json.loads(line)
                out.append(ReplayBar(**d))
    return out


def write_cache(path: Path, bars: Sequence[ReplayBar]) -> None:
    text = "".join(
        json.dumps(b._asdict(), ensure_ascii=False, separators=(",", ":")) + "\n" for b in bars
    )
    atomic_write_text(path, text)


def vn_day_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """[start 00:00 VN, end+1 00:00 VN) dưới dạng datetime UTC."""
    lo = datetime.combine(start, dtime(0, 0), tzinfo=VN_TZ).astimezone(UTC)
    hi = datetime.combine(end + timedelta(days=1), dtime(0, 0), tzinfo=VN_TZ).astimezone(UTC)
    return lo, hi


def fetch_minute_bars(client: Any, symbol: str, start: date, end: date) -> list[ReplayBar]:
    """Nến phút của một mã trong [start, end] (ngày VN) từ VCI, tăng dần, không trùng."""
    lo, hi = vn_day_bounds(start, end)
    page_size = min(30000, ((end - start).days + 1) * BARS_PER_DAY_MAX)
    seen: dict[int, ReplayBar] = {}
    for page in client.minute_pages(symbol, lo, hi, page_size=page_size):
        for b in page.bars:
            ts = int(b.bar_ts.timestamp() * 1000)
            seen[ts] = ReplayBar(symbol, ts, b.open, b.high, b.low, b.close, int(b.volume))
    return [seen[k] for k in sorted(seen)]


def load_bars(
    symbols: Sequence[str],
    start: date,
    end: date,
    *,
    client: Any = None,
    cache_dir: Path | None = None,
) -> dict[str, list[ReplayBar]]:
    """Nến từng mã: đọc cache nếu có, không thì gọi VCI rồi ghi cache."""
    cache_dir = cache_dir or state_dir() / CACHE_SUBDIR
    out: dict[str, list[ReplayBar]] = {}
    for sym in symbols:
        path = cache_path(cache_dir, sym, start, end)
        if path.exists():
            out[sym] = read_cache(path)
            log.info("Replay cache %s: %d nến", path.name, len(out[sym]))
            continue
        if client is None:
            from stocklake.sources.vci import VciClient

            client = VciClient(max_per_second=2.0)
        bars = fetch_minute_bars(client, sym, start, end)  # SourceBlockedError nổi lên
        if not bars:
            log.warning("Replay %s: VCI không có nến trong %s..%s (không cache)", sym, start, end)
        else:
            write_cache(path, bars)
            log.info("Replay %s: tải %d nến từ VCI, cache %s", sym, len(bars), path)
        out[sym] = bars
    return out


def last_trading_days(client: Any, days: int, now: datetime | None = None) -> tuple[date, date]:
    """(start, end) của `days` ngày giao dịch gần nhất đã đóng phiên, suy từ VNINDEX ngày.

    Hôm nay chỉ tính khi đã qua 15:00 giờ VN (tránh cache nửa phiên). 1 request VCI.
    """
    if days < 1:
        raise ValueError("days phải >= 1")
    now_vn = (now or datetime.now(UTC)).astimezone(VN_TZ)
    today = now_vn.date()
    lookback = today - timedelta(days=days * 2 + 14)
    dates = sorted({b.trade_date for b in client.daily_bars(CALENDAR_SYMBOL, lookback, today)})
    if now_vn.time() < SESSION_CLOSE_VN:
        dates = [d for d in dates if d < today]
    if len(dates) < days:
        raise RuntimeError(f"Chỉ tìm được {len(dates)} ngày giao dịch, cần {days}")
    picked = dates[-days:]
    return picked[0], picked[-1]


# ----------------------------------------------------------------- điểm vào


def default_symbols(settings: Settings | None = None) -> list[str]:
    """Mã cổ phiếu mẫu trong conf/app/symbols.yaml (sample.stocks); không gồm chỉ số."""
    from stocklake.common.config import load_app_conf

    conf = load_app_conf("symbols", settings)
    return list(conf["sample"]["stocks"])


def replay(
    symbols: Sequence[str],
    start: date,
    end: date,
    rate: float = 2000.0,
    run_id: str | None = None,
    *,
    client: Any = None,
    producer: Any = None,
    cache_dir: Path | None = None,
    settings: Settings | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    wall_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    flush_timeout_s: float = 120.0,
) -> ReplayResult:
    """Nạp nến [start, end] (ngày VN) của các mã rồi phát lại vào intraday_prices."""
    run_id = run_id or new_run_id()
    bars = load_bars(symbols, start, end, client=client, cache_dir=cache_dir)
    total = sum(len(v) for v in bars.values())
    log.info(
        "Replay %s: %d nến, %d mã, %s..%s, %.0f msg/s", run_id, total, len(bars), start, end, rate
    )
    if producer is None:
        from stocklake.streaming.producers.base import make_producer

        producer = make_producer(settings)
    result = publish_bars(
        merge_bars(bars),
        producer,
        rate=rate,
        run_id=run_id,
        clock=clock,
        sleep=sleep,
        wall_ms=wall_ms,
        flush_timeout_s=flush_timeout_s,
    )
    log.info("Replay xong: %s", result.to_json())
    return result


def run_replay(
    symbols: Sequence[str] | None = None,
    days: int | None = 5,
    start: date | None = None,
    end: date | None = None,
    rate: float = 2000.0,
    run_id: str | None = None,
    settings: Settings | None = None,
) -> ReplayResult:
    """Điểm vào cho jobs/streaming/replay.py: tự lấy mã mặc định và khoảng ngày."""
    symbols = list(symbols) if symbols else default_symbols(settings)
    client = None
    if start is None or end is None:
        if start is not None or end is not None:
            raise ValueError("Cần cả start và end, hoặc dùng days")
        from stocklake.sources.vci import VciClient

        client = VciClient(max_per_second=2.0)
        start, end = last_trading_days(client, days or 5)
    if end < start:
        raise ValueError("end < start")
    return replay(symbols, start, end, rate=rate, run_id=run_id, client=client, settings=settings)
