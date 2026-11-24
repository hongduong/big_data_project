"""Replay simulator: phát lại nến 1 phút lịch sử vào topic intraday_prices.

- Đọc nến từ bảng HDFS raw/intraday_history (do jobs/ingest/intraday_history.py nạp)
  qua WebHDFS, từng ngày giao dịch một (bộ nhớ chặn: chỉ giữ nến của một ngày).
- Không gọi nguồn dữ liệu nào (module không import stocklake.sources).
- Một replay_run_id và một tốc độ mục tiêu (msg/giây) cho cả lần chạy; nhịp gửi liên tục
  qua ranh giới ngày. source = 'replay'.
- Logic thuần (chọn ngày, filter, trộn, nhịp gửi) tách khỏi pyarrow/Kafka để test bằng giả.
"""

from __future__ import annotations

import heapq
import itertools
import json
import os
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from dataclasses import replace as dc_replace
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
HISTORY_TABLE = ("raw", "intraday_history")
# Năm sớm nhất có nến phút (lịch sử từ 09/2023): giới hạn dưới khi dò `--days`.
FIRST_HISTORY_YEAR = 2023
PREFERRED_SOURCE = "vci"
BAR_COLUMNS = ["symbol", "bar_ts", "open", "high", "low", "close", "volume", "source"]
HINT_COLLECT = "Hãy chạy bộ thu thập lịch sử trước: jobs/ingest/intraday_history.py"


class HistoryUnavailableError(RuntimeError):
    """Bảng raw/intraday_history chưa có hoặc không có nến cho lựa chọn."""


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
    days: int = 0  # số ngày có nến đã phát
    symbols: int = 0  # số mã đã chọn

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))


# ------------------------------------------------------ thư mục state (intraday producer dùng)


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


# ----------------------------------------------------------------- chọn ngày + filter


def vn_day_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """[start 00:00 VN, end+1 00:00 VN) dưới dạng datetime UTC (aware)."""
    lo = datetime.combine(start, dtime(0, 0), tzinfo=VN_TZ).astimezone(UTC)
    hi = datetime.combine(end + timedelta(days=1), dtime(0, 0), tzinfo=VN_TZ).astimezone(UTC)
    return lo, hi


def day_filters(symbols: Sequence[str], day: date) -> list[tuple[str, str, Any]]:
    """Filter DNF (một nhóm AND) cho một ngày VN.

    `year` là cột partition hive (số nguyên, theo năm giờ VN). Mốc bar_ts là datetime UTC
    aware để khớp cột timestamp(us, UTC); Parquet row-group statistics tự loại phần thừa.
    """
    lo, hi = vn_day_bounds(day, day)
    return [
        ("year", "=", day.year),
        ("symbol", "in", sorted(set(symbols))),
        ("bar_ts", ">=", lo),
        ("bar_ts", "<", hi),
    ]


def year_filters(symbols: Sequence[str], year: int) -> list[tuple[str, str, Any]]:
    return [("year", "=", year), ("symbol", "in", sorted(set(symbols)))]


def weekdays(start: date, end: date) -> list[date]:
    """Các ngày thứ 2-6 trong [start, end] (cuối tuần không có phiên)."""
    n = (end - start).days + 1
    return [d for d in (start + timedelta(days=i) for i in range(n)) if d.weekday() < 5]


def last_trade_dates(
    read_year_dates: Callable[[Sequence[str], int], set[date]],
    symbols: Sequence[str],
    n: int,
    today: date | None = None,
) -> list[date]:
    """`n` ngày giao dịch (VN) gần nhất CÓ trong lịch sử của các mã, tăng dần.

    Đi lùi từng năm từ năm hiện tại tới FIRST_HISTORY_YEAR, dừng khi đủ `n` ngày.
    """
    if n < 1:
        raise ValueError("days phải >= 1")
    today = today or datetime.now(UTC).astimezone(VN_TZ).date()
    found: set[date] = set()
    for year in range(today.year, FIRST_HISTORY_YEAR - 1, -1):
        found |= read_year_dates(symbols, year)
        if len(found) >= n:
            break
    if not found:
        raise HistoryUnavailableError(f"Không có nến nào trong lịch sử. {HINT_COLLECT}")
    return sorted(found)[-n:]


# ----------------------------------------------------------------- gộp / sắp xếp nến


def dedup_sort(rows: Iterable[tuple[ReplayBar, str]]) -> list[ReplayBar]:
    """Mỗi (symbol, bar_ts) giữ một nến: ưu tiên nguồn 'vci', sau đó theo tên nguồn.

    Trả danh sách sắp theo (bar_ts, symbol).
    """
    best: dict[tuple[str, int], tuple[tuple[int, str], ReplayBar]] = {}
    for bar, source in rows:
        rank = (0 if source == PREFERRED_SOURCE else 1, source)
        key = (bar.symbol, bar.bar_ts)
        cur = best.get(key)
        if cur is None or rank < cur[0]:
            best[key] = (rank, bar)
    return sorted((v[1] for v in best.values()), key=lambda b: (b.bar_ts, b.symbol))


# ----------------------------------------------------------------- pyarrow (chỗ duy nhất)


def table_to_rows(table: Any) -> list[tuple[ReplayBar, str]]:
    """Bảng pyarrow (BAR_COLUMNS) -> [(ReplayBar, source)]; bar_ts đổi sang epoch ms."""
    if table.num_rows == 0:
        return []
    cols = {name: table.column(name).to_pylist() for name in BAR_COLUMNS}
    out = []
    for sym, ts, o, h, lo, c, v, src in zip(*(cols[n] for n in BAR_COLUMNS), strict=True):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        ms = int(ts.timestamp() * 1000)
        out.append((ReplayBar(sym, ms, float(o), float(h), float(lo), float(c), int(v)), src))
    return out


def table_to_trade_dates(table: Any) -> set[date]:
    """Cột bar_ts -> tập ngày VN, tính bằng số học trên epoch (không cần tz database)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if table.num_rows == 0:
        return set()
    us = pc.cast(table.column("bar_ts"), pa.int64())
    offset_us = int(VN_TZ.utcoffset(datetime(2026, 1, 1)).total_seconds() * 1_000_000)
    # pyarrow.compute không có floor_divide; `divide` trên int64 là chia nguyên cắt về 0,
    # đúng với floor vì epoch sau 1970 luôn dương.
    days = pc.unique(pc.divide(pc.add(us, offset_us), 86_400_000_000))
    return {date(1970, 1, 1) + timedelta(days=int(d)) for d in days.to_pylist()}


# ----------------------------------------------------------------- đọc lịch sử từ HDFS


class HdfsHistory:
    """Đọc raw/intraday_history qua read_parquet_dir (WebHDFS)."""

    def __init__(self, settings: Settings | None = None, fs: Any = None):
        from stocklake.contracts.paths import table_path

        self.path = table_path(*HISTORY_TABLE)
        self._settings = settings
        self._fs = fs

    def _read(self, columns: list[str], filters: list) -> Any:
        from stocklake.common import hdfs

        if self._fs is None:
            self._fs = hdfs.webhdfs_fs(self._settings)
        try:
            return hdfs.read_parquet_dir(self.path, columns=columns, filters=filters, fs=self._fs)
        except FileNotFoundError as e:
            raise HistoryUnavailableError(f"Chưa có bảng {self.path}. {HINT_COLLECT}") from e

    def read_day(self, symbols: Sequence[str], day: date) -> list[ReplayBar]:
        table = self._read(BAR_COLUMNS, day_filters(symbols, day))
        return dedup_sort(table_to_rows(table))

    def read_year_dates(self, symbols: Sequence[str], year: int) -> set[date]:
        return table_to_trade_dates(self._read(["bar_ts"], year_filters(symbols, year)))


# ----------------------------------------------------------------- phát theo ngày


class DayStream:
    """Iterator nến của các ngày, đọc từng ngày một; đếm số ngày có nến."""

    def __init__(
        self,
        days: Sequence[date],
        symbols: Sequence[str],
        read_day: Callable[[Sequence[str], date], list[ReplayBar]],
    ):
        self.days_read = 0
        self._it = self._gen(days, symbols, read_day)

    def _gen(self, days, symbols, read_day) -> Iterator[ReplayBar]:
        for day in days:
            bars = read_day(symbols, day)
            if not bars:
                log.info("Replay %s: không có nến, bỏ qua", day)
                continue
            self.days_read += 1
            log.info("Replay %s: %d nến", day, len(bars))
            yield from bars

    def __iter__(self) -> Iterator[ReplayBar]:
        return self._it


# ----------------------------------------------------------------- điểm vào


def default_symbols(settings: Settings | None = None, group: str = "sample") -> list[str]:
    """Mã trong conf/app/symbols.yaml: group 'sample' (sample.stocks) hoặc 'vn30'."""
    from stocklake.common.config import load_app_conf

    conf = load_app_conf("symbols", settings)
    if group == "sample":
        return list(conf["sample"]["stocks"])
    if group == "vn30":
        symbols = list(conf.get("vn30") or [])
        if not symbols:
            raise ValueError("Danh sách vn30 trong conf/app/symbols.yaml đang rỗng")
        return symbols
    raise ValueError(f"group không hợp lệ: {group!r} (sample|vn30)")


def replay(
    symbols: Sequence[str],
    days: Sequence[date],
    rate: float = 2000.0,
    run_id: str | None = None,
    *,
    read_day: Callable[[Sequence[str], date], list[ReplayBar]],
    producer: Any = None,
    settings: Settings | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    wall_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    flush_timeout_s: float = 120.0,
) -> ReplayResult:
    """Phát lại nến của `days` (ngày VN) cho `symbols`, đọc từng ngày qua `read_day`.

    Không có nến nào -> HistoryUnavailableError (trước khi tạo producer).
    """
    run_id = run_id or new_run_id()
    symbols = list(symbols)
    stream = DayStream(days, symbols, read_day)
    it = iter(stream)
    first = next(it, None)
    if first is None:
        raise HistoryUnavailableError(
            f"Không có nến cho {len(symbols)} mã trong {len(days)} ngày đã chọn. {HINT_COLLECT}"
        )
    log.info("Replay %s: %d mã, %d ngày chọn, %.0f msg/s", run_id, len(symbols), len(days), rate)
    if producer is None:
        from stocklake.streaming.producers.base import make_producer

        producer = make_producer(settings)
    result = publish_bars(
        itertools.chain([first], it),
        producer,
        rate=rate,
        run_id=run_id,
        clock=clock,
        sleep=sleep,
        wall_ms=wall_ms,
        flush_timeout_s=flush_timeout_s,
    )
    result = dc_replace(result, days=stream.days_read, symbols=len(symbols))
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
    group: str = "sample",
    history: Any = None,
) -> ReplayResult:
    """Điểm vào cho jobs/streaming/replay.py: chọn mã, chọn ngày rồi phát lại."""
    symbols = list(symbols) if symbols else default_symbols(settings, group)
    history = history or HdfsHistory(settings)
    if start is not None or end is not None:
        if start is None or end is None:
            raise ValueError("Cần cả start và end, hoặc dùng days")
        if end < start:
            raise ValueError("end < start")
        chosen = weekdays(start, end)
    else:
        chosen = last_trade_dates(history.read_year_dates, symbols, days or 5)
    return replay(
        symbols, chosen, rate=rate, run_id=run_id, read_day=history.read_day, settings=settings
    )
