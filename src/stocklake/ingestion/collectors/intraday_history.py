"""Collector lịch sử nến 1 phút -> raw.intraday_history (VCI, không qua Kafka).

Bố cục file (CỐ ĐỊNH, job khác dựa vào):
    <table_path('raw','intraday_history')>/source=vci/year=<YYYY>/<SYMBOL>.parquet
Mỗi (mã, năm giờ VN) một file, ghi đè nguyên tử khi chạy lại; file KHÔNG chứa cột phân vùng.

Thiết kế: runner riêng, KHÔNG kế thừa `BaseCollector`. Vòng lặp của nó là "một WorkUnit ->
lấy từ danh sách nguồn -> ghi", còn ở đây VCI phân trang lùi theo MÃ (6-7 request/mã) nhưng ghi
theo (mã, năm), chỉ có một nguồn và bị chặn thì phải dừng cả lượt, không dự phòng. Runner vẫn dùng
lại `StateStore`/`JsonFileStateStore` và cách đặt khoá `intraday_history/<SYM>/<YYYY>`.

Đơn vị việc/trạng thái = (mã, năm):
- Năm đã qua: khoá `.../<YYYY>`; xong rồi thì bỏ qua, nhưng chỉ đánh dấu sau khi ghi file.
- Năm hiện tại (VN): khoá `.../<YYYY>#live`, luôn lấy lại (không bao giờ được coi là xong).
- Lấy từ "bây giờ" lùi tới 1/1 của năm sớm nhất còn cần (năm của `since`, hoặc năm hiện tại). Chỉ
  ghi file khi lượt lấy phủ TRỌN khoảng đó; lỗi/bị chặn giữa chừng thì không ghi gì, không đổi
  trạng thái của mã đó.
"""

from __future__ import annotations

import os
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Protocol

from stocklake.common.http import SourceBlockedError
from stocklake.common.logging import get_logger
from stocklake.contracts import available_at, get_schema, table_path, to_arrow
from stocklake.contracts.available_at import VN_TZ
from stocklake.ingestion.framework import StateStore, WorkUnit
from stocklake.sources.base import MinuteBar

log = get_logger(__name__)

NAME = "intraday_history"
SOURCE = "vci"
DEFAULT_STATE_DIR = "/var/lib/stocklake/state"
STATE_FILE = "ingestion_state.json"
DEFAULT_SINCE = date(2023, 9, 11)  # nến phút sớm nhất VCI có
BLOCKED_KEY = f"{NAME}/_blocked"
BAR_SECONDS = 60

# Phiên giao dịch giờ VN (thứ 2-6): [bắt đầu, kết thúc)
TRADING_SESSIONS = ((time(9, 0), time(11, 30)), (time(13, 0), time(15, 0)))


class ParquetWriter(Protocol):
    def write(self, table: Any, path: str) -> None: ...


class HdfsParquetWriter:
    """Ghi qua `common.hdfs.write_parquet_atomic` (import muộn)."""

    def __init__(self, fs: Any = None):
        self.fs = fs

    def write(self, table: Any, path: str) -> None:
        from stocklake.common.hdfs import write_parquet_atomic

        write_parquet_atomic(table, path, fs=self.fs)


# ----------------------------------------------------------------- hàm thuần


def in_trading_hours(now: datetime) -> bool:
    """Thứ 2-6, 09:00-11:30 hoặc 13:00-15:00 giờ VN (không xét ngày lễ)."""
    local = now.astimezone(VN_TZ)
    if local.weekday() >= 5:
        return False
    t = local.time()
    return any(a <= t < b for a, b in TRADING_SESSIONS)


def file_columns() -> tuple[str, ...]:
    """Cột nằm trong file parquet = cột schema trừ cột phân vùng."""
    schema = get_schema("raw", NAME)
    return tuple(c for c in schema.column_names if c not in schema.partition_by)


def file_path(symbol: str, year: int, base: str | None = None, source: str = SOURCE) -> str:
    root = table_path("raw", NAME) if base is None else table_path("raw", NAME, base)
    return f"{root}/source={source}/year={year}/{symbol}.parquet"


def state_key(symbol: str, year: int, live: bool = False) -> str:
    return f"{NAME}/{symbol}/{year}" + ("#live" if live else "")


def vn_year(ts: datetime) -> int:
    return ts.astimezone(VN_TZ).year


def new_run_id(now: datetime) -> str:
    return f"h{now.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def years_needed(symbol: str, since_year: int, current_year: int, state: StateStore) -> list[int]:
    """Các năm còn phải làm: năm hiện tại luôn có; năm qua chỉ khi chưa xong trong state."""
    return [
        y
        for y in range(since_year, current_year + 1)
        if y == current_year or not state.is_done(NAME, state_key(symbol, y))
    ]


def fetch_start(earliest_year: int) -> datetime:
    """Mốc lùi của lượt lấy: 0h ngày 1/1 giờ VN của năm sớm nhất còn cần."""
    return datetime(earliest_year, 1, 1, tzinfo=VN_TZ)


def completed_bars(bars: Iterable[MinuteBar], now: datetime) -> list[MinuteBar]:
    """Chỉ giữ nến đã đóng: bar_ts + 60 s <= now."""
    cutoff = now - timedelta(seconds=BAR_SECONDS)
    return [b for b in bars if b.bar_ts <= cutoff]


def dedup_bars(bars: Iterable[MinuteBar]) -> tuple[list[MinuteBar], int]:
    """Bỏ nến trùng bar_ts (trang chồng nhau ở biên), giữ một; sắp tăng dần. Trả (nến, số bỏ)."""
    seen: dict[datetime, MinuteBar] = {}
    n = 0
    for b in bars:
        if b.bar_ts in seen:
            n += 1
        else:
            seen[b.bar_ts] = b
    return [seen[k] for k in sorted(seen)], n


def split_by_year(bars: Iterable[MinuteBar]) -> dict[int, list[MinuteBar]]:
    """Gom nến theo năm của bar_ts tính theo giờ VN; thứ tự trong năm được giữ."""
    out: dict[int, list[MinuteBar]] = defaultdict(list)
    for b in bars:
        out[vn_year(b.bar_ts)].append(b)
    return dict(out)


def build_rows(bars: Iterable[MinuteBar], ingest_ts: datetime, run_id: str) -> list[dict[str, Any]]:
    """MinuteBar -> dict theo thứ tự cột schema (bỏ cột phân vùng)."""
    cols = file_columns()
    rows = []
    for b in bars:
        vals = {
            "symbol": b.symbol, "bar_ts": b.bar_ts,
            "open": b.open, "high": b.high, "low": b.low, "close": b.close,
            "volume": b.volume, "source": b.source,
            "ingest_ts": ingest_ts, "available_at": available_at.intraday_bar(b.bar_ts),
            "ingest_run_id": run_id,
        }  # fmt: skip
        rows.append({c: vals[c] for c in cols})
    return rows


def rows_to_table(rows: list[dict[str, Any]]):
    """dict -> pyarrow.Table đúng kiểu schema (pyarrow import trong hàm)."""
    import pyarrow as pa

    full = to_arrow(get_schema("raw", NAME))
    schema = pa.schema([f for f in full if f.name in file_columns()])
    return pa.Table.from_pylist(rows, schema=schema)


def load_symbols(conf: Mapping[str, Any], group: str) -> list[str]:
    """Mã của nhóm `vn30` hoặc `sample` (chỉ cổ phiếu) trong conf/app/symbols.yaml."""
    if group == "vn30":
        raw = conf.get("vn30") or []
    elif group == "sample":
        raw = (conf.get("sample") or {}).get("stocks") or []
    else:
        raise ValueError(f"Nhóm mã không hợp lệ: {group!r} (vn30 | sample)")
    out: list[str] = []
    for s in raw:
        s = str(s).strip().upper()
        if s and s not in out:
            out.append(s)
    if not out:
        raise ValueError(f"Nhóm {group!r} rỗng trong symbols.yaml; hãy điền hoặc truyền --symbols")
    return out


# ----------------------------------------------------------------- runner


@dataclass
class _Stats:
    done: list[str]
    failed: list[str]
    skipped: list[str]
    files: int = 0
    rows: int = 0
    requests: int = 0
    dups: int = 0
    blocked: dict[str, Any] | None = None
    stopped_reason: str | None = None


class IntradayHistoryCollector:
    """Lấy theo MÃ, ghi theo (mã, năm). Dùng `run()` để chạy cả lượt."""

    def __init__(
        self,
        client: Any,
        state: StateStore,
        writer: ParquetWriter,
        symbols: Sequence[str],
        since: date = DEFAULT_SINCE,
        allow_trading_hours: bool = False,
        max_symbols: int | None = None,
        clock: Callable[[], datetime] | None = None,
        to_table: Callable[[list[dict[str, Any]]], Any] | None = None,
        base: str | None = None,
    ):
        self.client = client
        self.state = state
        self.writer = writer
        self.symbols = list(symbols)
        self.since = since
        self.allow_trading_hours = allow_trading_hours
        self.max_symbols = max_symbols
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.to_table = to_table or (lambda rows: rows_to_table(rows))
        self.base = base
        self.per_year_rows: dict[str, int] = defaultdict(int)

    def plan(self, symbol: str, now: datetime) -> list[WorkUnit]:
        """Các (mã, năm) còn phải làm của một mã (rỗng = bỏ qua mã)."""
        cur = vn_year(now)
        return [
            WorkUnit(
                state_key(symbol, y, y == cur), {"symbol": symbol, "year": y, "live": y == cur}
            )
            for y in years_needed(symbol, self.since.year, cur, self.state)
        ]

    def _fetch(self, symbol: str, earliest_year: int, now: datetime, st: _Stats) -> list[MinuteBar]:
        """Lùi từ `now` tới 1/1 năm sớm nhất; ném lỗi nếu chưa phủ trọn (không trả dở dang)."""
        end = now.replace(microsecond=0)
        bars: list[MinuteBar] = []
        for page in self.client.minute_pages(symbol, fetch_start(earliest_year), end):
            st.requests += 1
            bars.extend(page.bars)
        return bars

    def _write_symbol(
        self, symbol: str, units: list[WorkUnit], bars: list[MinuteBar], now: datetime,
        run_id: str, st: _Stats,
    ) -> None:  # fmt: skip
        by_year = split_by_year(bars)
        for u in units:
            year = u.params["year"]
            ybars = by_year.get(year, [])
            n = len(ybars)
            if ybars:
                rows = build_rows(ybars, now, run_id)
                self.writer.write(self.to_table(rows), file_path(symbol, year, self.base))
                st.files += 1
                st.rows += n
                self.per_year_rows[str(year)] += n
            elif u.params["live"]:
                log.warning("%s: %s rỗng cho năm hiện tại", NAME, symbol)
            # năm trước ngày lịch sử bắt đầu: xong, 0 dòng, không ghi file rỗng
            self.state.mark_done(NAME, u.key, {"source": SOURCE, "records": n})

    def _record_blocked(self, symbol: str, exc: SourceBlockedError, now: datetime) -> dict:
        info = {
            "symbol": symbol,
            "status": getattr(exc, "status", None),
            "retry_after_raw": getattr(exc, "retry_after_raw", None),
            "retry_after_seconds": getattr(exc, "retry_after_seconds", None),
            "at": now.astimezone(timezone.utc).isoformat(),
        }
        self.state.mark_failed(NAME, BLOCKED_KEY, str(exc), info)
        return info

    def run(self) -> dict[str, Any]:
        t0 = self.clock()
        run_id = new_run_id(t0)
        st = _Stats([], [], [])
        if not self.allow_trading_hours and in_trading_hours(t0):
            st.stopped_reason = "trading_hours"
            log.error("%s: đang trong giờ giao dịch, từ chối chạy", NAME)
            return self._summary(run_id, st, t0)
        attempted = 0
        for sym in self.symbols:
            now = self.clock()
            if not self.allow_trading_hours and in_trading_hours(now):
                st.stopped_reason = "trading_hours"
                break
            units = self.plan(sym, now)
            if not units:
                st.skipped.append(sym)
                continue
            if self.max_symbols is not None and attempted >= self.max_symbols:
                st.stopped_reason = "max_symbols"
                break
            attempted += 1
            try:
                bars = self._fetch(sym, units[0].params["year"], now, st)
                bars, dups = dedup_bars(completed_bars(bars, now))
                st.dups += dups
                self._write_symbol(sym, units, bars, now, run_id, st)
            except SourceBlockedError as exc:
                st.blocked = self._record_blocked(sym, exc, now)
                st.stopped_reason = "blocked"
                log.error("%s: %s bị chặn (HTTP %s), dừng cả lượt", NAME, sym, st.blocked["status"])
                break
            except Exception as exc:  # noqa: BLE001 - lỗi một mã: đánh dấu rồi làm mã khác
                log.error("%s: %s lỗi: %s: %s", NAME, sym, type(exc).__name__, exc)
                for u in units:
                    if not self.state.is_done(NAME, u.key) or u.params["live"]:
                        self.state.mark_failed(NAME, u.key, f"{type(exc).__name__}: {exc}")
                st.failed.append(sym)
                continue
            finally:
                bars = None  # nhả bộ nhớ của mã này trước mã kế
            st.done.append(sym)
        return self._summary(run_id, st, t0)

    def _summary(self, run_id: str, st: _Stats, t0: datetime) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "symbols_done": st.done,
            "symbols_failed": st.failed,
            "symbols_skipped": st.skipped,
            "files_written": st.files,
            "rows_written": st.rows,
            "requests": st.requests,
            "duplicates_dropped": st.dups,
            "per_year_rows": dict(sorted(self.per_year_rows.items())),
            "blocked": st.blocked,
            "stopped_reason": st.stopped_reason,
            "seconds": round((self.clock() - t0).total_seconds(), 3),
        }


# ----------------------------------------------------------------- điểm vào tiện dụng


def _state_path() -> str:
    return os.path.join(os.environ.get("COLLECTOR_STATE_DIR", DEFAULT_STATE_DIR), STATE_FILE)


def run_intraday_history(
    symbols: Sequence[str] | None = None,
    group: str | None = None,
    since: date | None = None,
    allow_trading_hours: bool = False,
    max_symbols: int | None = None,
    settings=None,
    *,
    client: Any = None,
    writer: ParquetWriter | None = None,
    state: StateStore | None = None,
    clock: Callable[[], datetime] | None = None,
    to_table: Callable[[list[dict[str, Any]]], Any] | None = None,
    base: str | None = None,
) -> dict[str, Any]:
    """Chạy collector với cấu hình mặc định; tham số keyword chỉ dùng để thay trong test.

    Thứ tự chọn mã: `symbols` > `group` (vn30|sample) > lỗi.
    """
    from stocklake.ingestion.framework import JsonFileStateStore

    if symbols:
        syms = list(dict.fromkeys(s.strip().upper() for s in symbols if s.strip()))
    elif group:
        from stocklake.common.config import load_app_conf

        syms = load_symbols(load_app_conf("symbols", settings), group)
    else:
        raise ValueError("Cần symbols hoặc group (vn30 | sample)")
    if client is None:
        from stocklake.sources.vci import VciClient

        client = VciClient(max_per_second=1.0)  # quyết định 3: 1 request/giây
    col = IntradayHistoryCollector(
        client,
        state or JsonFileStateStore(_state_path()),
        writer or HdfsParquetWriter(),
        syms,
        since or DEFAULT_SINCE,
        allow_trading_hours,
        max_symbols,
        clock,
        to_table,
        base,
    )
    out = col.run()
    log.info("%s: %s", NAME, {k: v for k, v in out.items() if not k.startswith("symbols_")})
    return out
