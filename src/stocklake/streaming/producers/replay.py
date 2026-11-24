"""Replay simulator: phát lại nến 1 phút lịch sử vào topic intraday_prices.

- Đọc nến từ bảng HDFS raw/intraday_history (do jobs/ingest/intraday_history.py nạp)
  qua WebHDFS theo CHUNK THÁNG (giờ VN): mỗi tháng một lần đọc các file tường minh
  `source=<src>/year=<Y>/<SYM>.parquet` (không liệt kê thư mục; row group theo tháng nên
  statistics bar_ts loại phần thừa), rồi cắt thành từng ngày trong bộ nhớ.
- Một luồng đọc nền nạp sẵn chunk tháng kế tiếp (hàng đợi cỡ 1) nên đọc HDFS không làm
  đứng nhịp gửi; tháng đầu được đọc xong TRƯỚC khi nhịp gửi bắt đầu.
- Bộ nhớ: chunk giữ dạng cột pyarrow (~56 B/nến), chỉ đổi sang đối tượng Python một NGÀY.
  Tối đa 3 chunk cùng sống (đang gửi + đang chờ + đang đọc): VN30 (~140k nến/tháng, ~8 MB/chunk)
  đỉnh ~40-60 MB; 412 mã HOSE (~1,7 triệu nến/tháng, ~95 MB/chunk) đỉnh ~300-450 MB kể cả
  bản sao tạm lúc đọc/sắp xếp, cộng ~25 MB đối tượng Python của một ngày.
- Không gọi nguồn dữ liệu nào (module không import stocklake.sources).
- Một replay_run_id và một tốc độ mục tiêu (msg/giây) cho cả lần chạy; nhịp gửi liên tục
  qua ranh giới ngày/tháng. source = 'replay'.
- Logic thuần (cửa sổ tháng, đường dẫn, trộn, nhịp gửi) tách khỏi pyarrow/Kafka để test bằng giả.
"""

from __future__ import annotations

import heapq
import itertools
import json
import os
import queue
import threading
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
# Tháng sớm nhất có nến phút (lịch sử từ 09/2023): giới hạn dưới khi dò `--days`.
FIRST_HISTORY_MONTH = (2023, 9)
PREFERRED_SOURCE = "vci"
# `source` không đọc từ file: lấy từ đường dẫn (partition `source=<src>`).
BAR_COLUMNS = ["symbol", "bar_ts", "open", "high", "low", "close", "volume"]
US_PER_DAY = 86_400_000_000
WARN_RATE_RATIO = 0.95
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
    target_rate: float = 0.0  # msg/giây yêu cầu
    achieved_rate: float = 0.0  # sent / thời gian giữa lần gửi đầu và cuối
    read_seconds: float = 0.0  # tổng thời gian đọc + giải mã lịch sử (luồng đọc)
    months: int = 0  # số chunk tháng đã đọc

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
    t_first = t_last = t_start
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
                t_first = clock()
            last_ts = bar.bar_ts
        t_last = clock()
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
    send_s = t_last - t_first
    achieved = sent / send_s if send_s > 0 else 0.0
    return ReplayResult(
        run_id, sent, counter.delivered, failed, round(seconds, 3), first_ts, last_ts,
        target_rate=float(rate), achieved_rate=round(achieved, 1),
    )  # fmt: skip


# ----------------------------------------------------------------- cửa sổ tháng + đường dẫn


class MonthWindow(NamedTuple):
    """Một tháng giờ VN (đã cắt theo khoảng yêu cầu); lo/hi là datetime UTC aware, [lo, hi)."""

    year: int
    month: int
    lo: datetime
    hi: datetime


def vn_day_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """[start 00:00 VN, end+1 00:00 VN) dưới dạng datetime UTC (aware)."""
    lo = datetime.combine(start, dtime(0, 0), tzinfo=VN_TZ).astimezone(UTC)
    hi = datetime.combine(end + timedelta(days=1), dtime(0, 0), tzinfo=VN_TZ).astimezone(UTC)
    return lo, hi


def month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    """[đầu tháng VN, đầu tháng sau VN) dưới dạng datetime UTC."""
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    return vn_day_bounds(date(year, month, 1), nxt - timedelta(days=1))


def month_windows(start: date, end: date) -> list[MonthWindow]:
    """Các tháng VN phủ [start, end] (gồm cả hai đầu), tăng dần, cắt ở hai đầu khoảng."""
    lo_all, hi_all = vn_day_bounds(start, end)
    out = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        lo, hi = month_bounds(y, m)
        out.append(MonthWindow(y, m, max(lo, lo_all), min(hi, hi_all)))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def weekdays(start: date, end: date) -> list[date]:
    """Các ngày thứ 2-6 trong [start, end] (cuối tuần không có phiên)."""
    n = (end - start).days + 1
    return [d for d in (start + timedelta(days=i) for i in range(n)) if d.weekday() < 5]


def history_paths(root: str, source: str, year: int, symbols: Sequence[str]) -> list[str]:
    """Đường dẫn tường minh `<root>/source=<src>/year=<Y>/<SYM>.parquet` (year = năm VN)."""
    base = f"{root.rstrip('/')}/source={source}/year={year}"
    return [f"{base}/{sym}.parquet" for sym in sorted(set(symbols))]


def parse_sources(entries: Iterable[str]) -> list[str]:
    """Tên nguồn từ kết quả `ls <root>/` (mục `source=<src>`); 'vci' đứng đầu."""
    found = {
        e.rstrip("/").rsplit("/", 1)[-1][len("source=") :]
        for e in entries
        if e.rstrip("/").rsplit("/", 1)[-1].startswith("source=")
    }
    return sorted(found, key=lambda n: (n != PREFERRED_SOURCE, n))


def bar_ts_filters(lo: datetime, hi: datetime) -> list[tuple[str, str, Any]]:
    """Filter DNF theo bar_ts; mốc là datetime UTC aware để khớp cột timestamp có tz."""
    return [("bar_ts", ">=", lo), ("bar_ts", "<", hi)]


def previous_month(year: int, month: int) -> tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)


def last_trade_dates(
    read_month_dates: Callable[[Sequence[str], int, int, datetime, datetime], set[date]],
    symbols: Sequence[str],
    n: int,
    today: date | None = None,
) -> list[date]:
    """`n` ngày giao dịch (VN) gần nhất CÓ trong lịch sử của các mã, tăng dần.

    Đi lùi từng tháng từ tháng hiện tại (chỉ đọc cột bar_ts), dừng ngay khi đủ `n` ngày.
    """
    if n < 1:
        raise ValueError("days phải >= 1")
    today = today or datetime.now(UTC).astimezone(VN_TZ).date()
    found: set[date] = set()
    y, m = today.year, today.month
    while (y, m) >= FIRST_HISTORY_MONTH:
        lo, hi = month_bounds(y, m)
        found |= read_month_dates(symbols, y, m, lo, hi)
        if len(found) >= n:
            break
        y, m = previous_month(y, m)
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


def vn_date_of_ms(bar_ts_ms: int) -> date:
    """Ngày VN của epoch ms (VN cố định UTC+7)."""
    return date(1970, 1, 1) + timedelta(days=(bar_ts_ms + VN_OFFSET_MS) // 86_400_000)


VN_OFFSET_MS = int(VN_TZ.utcoffset(datetime(2026, 1, 1)).total_seconds() * 1000)


class ListChunk:
    """Chunk tháng dạng danh sách Python [(ReplayBar, source)] (test/dữ liệu nhỏ).

    Giao diện chunk: `dates()` (ngày VN có nến, tăng dần) và `day_rows(day)`.
    """

    def __init__(self, rows: Iterable[tuple[ReplayBar, str]]):
        self._by_day: dict[date, list[tuple[ReplayBar, str]]] = {}
        for bar, source in rows:
            self._by_day.setdefault(vn_date_of_ms(bar.bar_ts), []).append((bar, source))

    def dates(self) -> list[date]:
        return sorted(self._by_day)

    def day_rows(self, day: date) -> list[tuple[ReplayBar, str]]:
        return self._by_day.get(day, [])


# ----------------------------------------------------------------- pyarrow (chỗ duy nhất)


def _to_us(column: Any) -> Any:
    """Cột timestamp -> int64 micro giây (theo đơn vị của cột)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    ints = pc.cast(column, pa.int64())
    unit = column.type.unit
    if unit == "ns":
        return pc.divide(ints, 1000)
    if unit == "ms":
        return pc.multiply(ints, 1000)
    if unit == "s":
        return pc.multiply(ints, 1_000_000)
    return ints


def table_to_rows(table: Any, source: str | None = None) -> list[tuple[ReplayBar, str]]:
    """Bảng pyarrow -> [(ReplayBar, source)]; bar_ts đổi sang epoch ms.

    `source` lấy từ cột `source` nếu có, không thì dùng tham số (nguồn suy từ đường dẫn).
    """
    import pyarrow.compute as pc

    n = table.num_rows
    if n == 0:
        return []
    ms = pc.divide(_to_us(table.column("bar_ts")), 1000).to_pylist()
    c = {name: table.column(name).to_pylist() for name in BAR_COLUMNS if name != "bar_ts"}
    srcs = table.column("source").to_pylist() if "source" in table.column_names else [source] * n
    return [
        (ReplayBar(sym, t, float(o), float(h), float(lo), float(cl), int(v)), src)
        for sym, t, o, h, lo, cl, v, src in zip(
            c["symbol"], ms, c["open"], c["high"], c["low"], c["close"], c["volume"], srcs,
            strict=True,
        )
    ]  # fmt: skip


def _day_numbers(table: Any) -> Any:
    """Cột bar_ts -> số ngày VN kể từ 1970-01-01 (số học trên epoch, không cần tz database)."""
    import pyarrow.compute as pc

    shifted = pc.add(_to_us(table.column("bar_ts")), VN_OFFSET_MS * 1000)
    # divide trên int64 cắt về 0; đúng như floor vì epoch sau 1970 luôn dương.
    return pc.divide(shifted, US_PER_DAY)


def table_to_trade_dates(table: Any) -> set[date]:
    """Cột bar_ts -> tập ngày VN."""
    import pyarrow.compute as pc

    if table.num_rows == 0:
        return set()
    days = pc.unique(_day_numbers(table))
    return {date(1970, 1, 1) + timedelta(days=int(d)) for d in days.to_pylist()}


def split_by_vn_day(table: Any) -> list[tuple[date, Any]]:
    """Sắp bảng theo bar_ts rồi cắt thành các lát (không sao chép) theo ngày VN, tăng dần."""
    import pyarrow.compute as pc

    if table.num_rows == 0:
        return []
    table = table.take(pc.sort_indices(table, sort_keys=[("bar_ts", "ascending")]))
    counts = pc.value_counts(_day_numbers(table))
    values, cnts = counts.field("values").to_pylist(), counts.field("counts").to_pylist()
    pairs = sorted(zip(values, cnts, strict=True))
    out, offset = [], 0
    for day_no, cnt in pairs:
        out.append((date(1970, 1, 1) + timedelta(days=int(day_no)), table.slice(offset, cnt)))
        offset += cnt
    return out


class ArrowChunk:
    """Chunk tháng dạng cột pyarrow, đã cắt lát theo ngày; chỉ ngày được hỏi mới thành Python."""

    def __init__(self, parts: Iterable[tuple[str, Any]]):
        self._slices: dict[date, list[tuple[str, Any]]] = {}
        for source, table in parts:
            for day, piece in split_by_vn_day(table):
                self._slices.setdefault(day, []).append((source, piece))

    def dates(self) -> list[date]:
        return sorted(self._slices)

    def day_rows(self, day: date) -> list[tuple[ReplayBar, str]]:
        rows: list[tuple[ReplayBar, str]] = []
        for source, piece in self._slices.get(day, []):
            rows += table_to_rows(piece, source)
        return rows


# ----------------------------------------------------------------- đọc lịch sử từ HDFS


class HdfsHistory:
    """Đọc raw/intraday_history theo chunk tháng bằng hdfs.read_parquet_files (WebHDFS).

    Danh sách nguồn lấy một lần cho cả lần chạy (một `ls`). Chỉ một luồng gọi tại một thời điểm
    (dò `--days` xong mới tới luồng đọc nền), nên không cần khoá.
    """

    def __init__(self, settings: Settings | None = None, fs: Any = None):
        from stocklake.contracts.paths import table_path

        self.path = table_path(*HISTORY_TABLE)
        self._settings = settings
        self._fs = fs
        self._sources: list[str] | None = None

    def _filesystem(self) -> Any:
        if self._fs is None:
            from stocklake.common import hdfs

            self._fs = hdfs.webhdfs_fs(self._settings)
        return self._fs

    def sources(self) -> list[str]:
        if self._sources is None:
            from stocklake.common import hdfs

            try:
                entries = hdfs.ls(self.path, fs=self._filesystem())
            except FileNotFoundError as e:
                raise HistoryUnavailableError(f"Chưa có bảng {self.path}. {HINT_COLLECT}") from e
            self._sources = parse_sources(entries)
            if not self._sources:
                raise HistoryUnavailableError(f"Bảng {self.path} rỗng. {HINT_COLLECT}")
        return self._sources

    def _read(self, symbols: Sequence[str], year: int, lo: datetime, hi: datetime, columns):
        """[(source, bảng)] của các nguồn có nến trong [lo, hi)."""
        from stocklake.common import hdfs

        parts = []
        for source in self.sources():
            table = hdfs.read_parquet_files(
                history_paths(self.path, source, year, symbols),
                columns=columns,
                filters=bar_ts_filters(lo, hi),
                fs=self._filesystem(),
            )
            if table.num_rows:
                parts.append((source, table))
        return parts

    def read_month(
        self, symbols: Sequence[str], year: int, month: int, lo: datetime, hi: datetime
    ) -> ArrowChunk:
        return ArrowChunk(self._read(symbols, year, lo, hi, BAR_COLUMNS))

    def read_month_dates(
        self, symbols: Sequence[str], year: int, month: int, lo: datetime, hi: datetime
    ) -> set[date]:
        dates: set[date] = set()
        for _source, table in self._read(symbols, year, lo, hi, ["bar_ts"]):
            dates |= table_to_trade_dates(table)
        return dates


# ----------------------------------------------------------------- đọc nền + phát theo ngày

ReadMonth = Callable[[Sequence[str], int, int, datetime, datetime], Any]


class SyncChunks:
    """Đọc chunk tuần tự trong luồng gọi (không đọc trước); cùng giao diện PrefetchChunks."""

    def __init__(self, windows: Sequence[MonthWindow], symbols: Sequence[str], read: ReadMonth):
        self.read_seconds = 0.0
        self._it = self._gen(windows, symbols, read)

    def _gen(self, windows, symbols, read) -> Iterator[tuple[MonthWindow, Any]]:
        for w in windows:
            t = time.perf_counter()
            chunk = read(symbols, w.year, w.month, w.lo, w.hi)
            self.read_seconds += time.perf_counter() - t
            yield w, chunk

    def __iter__(self) -> Iterator[tuple[MonthWindow, Any]]:
        return self._it

    def close(self) -> None:
        self._it.close()


class PrefetchChunks:
    """Luồng nền đọc chunk tháng, hàng đợi cỡ 1: đọc tháng kế trong lúc gửi tháng hiện tại.

    - Lỗi trong luồng đọc được chuyển qua hàng đợi và ném lại ở luồng gọi.
    - close(): đặt cờ dừng, xả hàng đợi để luồng không kẹt ở put(), rồi join. Luồng là daemon
      nên một lần đọc HDFS treo không giữ tiến trình sống quá `join_timeout_s`.
    """

    def __init__(
        self,
        windows: Sequence[MonthWindow],
        symbols: Sequence[str],
        read: ReadMonth,
        join_timeout_s: float = 60.0,
    ):
        self.read_seconds = 0.0
        self._q: queue.Queue = queue.Queue(maxsize=1)
        self._stop = threading.Event()
        self._join_timeout_s = join_timeout_s
        self._thread = threading.Thread(
            target=self._run, args=(list(windows), list(symbols), read),
            name="replay-reader", daemon=True,
        )  # fmt: skip
        self._thread.start()

    def _run(self, windows, symbols, read) -> None:
        try:
            for w in windows:
                if self._stop.is_set():
                    return
                t = time.perf_counter()
                chunk = read(symbols, w.year, w.month, w.lo, w.hi)
                self.read_seconds += time.perf_counter() - t
                self._q.put(("chunk", (w, chunk)))
                del chunk  # nhả tham chiếu: chunk giờ do hàng đợi/luồng gửi giữ
            self._q.put(("end", None))
        except BaseException as e:  # noqa: BLE001 - chuyển nguyên lỗi sang luồng gọi
            self._q.put(("error", e))

    def __iter__(self) -> Iterator[tuple[MonthWindow, Any]]:
        while True:
            kind, payload = self._q.get()
            if kind == "end":
                return
            if kind == "error":
                raise payload
            yield payload

    def close(self) -> None:
        self._stop.set()
        deadline = time.monotonic() + self._join_timeout_s
        while self._thread.is_alive() and time.monotonic() < deadline:
            try:
                self._q.get_nowait()
            except queue.Empty:
                pass
            self._thread.join(0.02)
        if self._thread.is_alive():
            log.warning("Luồng đọc lịch sử chưa dừng sau %.0f s (daemon)", self._join_timeout_s)


class MonthStream:
    """Iterator nến theo thứ tự (ngày, bar_ts, symbol); cắt từng ngày từ chunk tháng.

    `iter(stream)` trả generator; gọi `close()` để dừng luồng đọc (cả khi lỗi giữa chừng).
    """

    def __init__(
        self,
        windows: Sequence[MonthWindow],
        symbols: Sequence[str],
        read_month: ReadMonth,
        prefetch: bool = True,
    ):
        self.days_read = 0
        self.months_read = 0
        cls = PrefetchChunks if prefetch else SyncChunks
        self._src = cls(windows, symbols, read_month)
        self._it = self._gen()

    @property
    def read_seconds(self) -> float:
        return self._src.read_seconds

    def _gen(self) -> Iterator[ReplayBar]:
        try:
            for window, chunk in self._src:
                self.months_read += 1
                log.info("Replay %d-%02d: đọc xong chunk tháng", window.year, window.month)
                for day in chunk.dates():
                    bars = dedup_sort(chunk.day_rows(day))
                    if not bars:
                        continue
                    self.days_read += 1
                    log.info("Replay %s: %d nến", day, len(bars))
                    yield from bars
                del chunk
        finally:
            self._src.close()

    def __iter__(self) -> Iterator[ReplayBar]:
        return self._it

    def close(self) -> None:
        self._it.close()
        self._src.close()


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
    start: date,
    end: date,
    rate: float = 2000.0,
    run_id: str | None = None,
    *,
    read_month: ReadMonth,
    producer: Any = None,
    settings: Settings | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    wall_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    flush_timeout_s: float = 120.0,
    prefetch: bool = True,
) -> ReplayResult:
    """Phát lại nến của các ngày VN trong [start, end] cho `symbols`.

    Đọc theo chunk tháng qua `read_month(symbols, year, month, lo_utc, hi_utc)`; chunk tháng
    đầu đọc xong trước khi nhịp gửi bắt đầu. Không có nến nào -> HistoryUnavailableError
    (trước khi tạo producer).
    """
    run_id = run_id or new_run_id()
    symbols = list(symbols)
    windows = month_windows(start, end)
    stream = MonthStream(windows, symbols, read_month, prefetch=prefetch)
    try:
        it = iter(stream)
        first = next(it, None)  # chặn tới khi tháng đầu có mặt, trước khi lấy t0
        if first is None:
            raise HistoryUnavailableError(
                f"Không có nến cho {len(symbols)} mã trong {start}..{end}. {HINT_COLLECT}"
            )
        log.info("Replay %s: %d mã, %s..%s, %.0f msg/s", run_id, len(symbols), start, end, rate)
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
    finally:
        stream.close()
    result = dc_replace(
        result,
        days=stream.days_read,
        symbols=len(symbols),
        read_seconds=round(stream.read_seconds, 3),
        months=stream.months_read,
    )
    if result.seconds >= 1.0 and result.achieved_rate < WARN_RATE_RATIO * rate:
        log.warning(
            "Replay %s: đạt %.0f msg/s < %.0f%% mục tiêu %.0f (đọc lịch sử %.1f s / %d tháng)",
            run_id, result.achieved_rate, WARN_RATE_RATIO * 100, rate, result.read_seconds,
            result.months,
        )  # fmt: skip
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
    prefetch: bool = True,
    producer: Any = None,
) -> ReplayResult:
    """Điểm vào cho jobs/streaming/replay.py: chọn mã, chọn khoảng ngày rồi phát lại."""
    symbols = list(symbols) if symbols else default_symbols(settings, group)
    history = history or HdfsHistory(settings)
    if start is not None or end is not None:
        if start is None or end is None:
            raise ValueError("Cần cả start và end, hoặc dùng days")
        if end < start:
            raise ValueError("end < start")
    else:
        chosen = last_trade_dates(history.read_month_dates, symbols, days or 5)
        start, end = chosen[0], chosen[-1]
    return replay(
        symbols, start, end, rate=rate, run_id=run_id, read_month=history.read_month,
        settings=settings, prefetch=prefetch, producer=producer,
    )  # fmt: skip
