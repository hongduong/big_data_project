"""Collector lịch sử nến 1 phút -> raw.intraday_history (VCI, không qua Kafka).

Bố cục file (CỐ ĐỊNH, job khác dựa vào):
    <table_path('raw','intraday_history')>/source=vci/year=<YYYY>/<SYMBOL>.parquet
Mỗi (mã, năm giờ VN) một file, ghi đè nguyên tử khi chạy lại; file KHÔNG chứa cột phân vùng.
File có MỘT ROW GROUP MỖI THÁNG giờ VN của bar_ts (dòng sắp tăng theo bar_ts) để người đọc
(replay, Spark) lọc theo thống kê min/max của bar_ts. Năm hiện tại được lấy lại mỗi lượt nhưng
file chỉ được ghi lại khi nến đổi (hoặc file chưa chia row group theo tháng); không đổi thì giữ
nguyên file để thời gian sửa trên HDFS không đổi. `regroup_history_files` chia lại file CŨ trên
HDFS mà không gọi VCI.

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

Nguồn điều chỉnh giá (VCI nhân lại TOÀN BỘ giá lịch sử của mã với một hệ số khi có sự kiện):
- Trước khi ghi năm hiện tại, so giá các nến trùng bar_ts với file đang có
  (`stocklake.ingestion.price_adjustment.compare_prices`).
- Lệch theo MỘT tỷ lệ chung -> trong cùng lượt: đánh dấu mọi năm qua của mã là chưa xong (state
  `failed`, lỗi `pending_refetch: ...`), lấy thêm phần lịch sử còn thiếu tới 1/1 năm của `since`
  rồi ghi lại mọi file năm; sự kiện lưu ở khoá `intraday_history/<SYM>/_adjustment/<run_id>`
  (mỗi lượt một khoá, giữ lịch sử). Bị chặn/lỗi giữa chừng: không ghi gì, các năm vẫn chưa xong
  nên lượt sau tự làm tiếp (và file năm hiện tại chưa đổi nên vẫn phát hiện lại được).
- Lệch không theo một tỷ lệ chung -> chỉ cảnh báo (WARNING + state + JSON), không tải lại năm cũ.
- `refetch_all_years` (cờ `--refetch-all-years`): ép tải lại mọi năm cho các mã chỉ định, cho ca
  file năm hiện tại đã mang hệ số mới còn năm trước vẫn cũ (so sánh không còn thấy lệch).
"""

from __future__ import annotations

import os
import re
import time as _time
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Protocol

from stocklake.common.http import SourceBlockedError
from stocklake.common.logging import get_logger
from stocklake.contracts import available_at, get_schema, table_path, to_arrow
from stocklake.contracts.available_at import VN_TZ
from stocklake.ingestion.framework import StateStore, WorkUnit
from stocklake.ingestion.price_adjustment import (
    IRREGULAR,
    MINOR,
    SCALED,
    compare_prices,
    event_key,
)
from stocklake.sources.base import MinuteBar

log = get_logger(__name__)

NAME = "intraday_history"
SOURCE = "vci"
DEFAULT_STATE_DIR = "/var/lib/stocklake/state"
STATE_FILE = "ingestion_state.json"
DEFAULT_SINCE = date(2023, 9, 11)  # nến phút sớm nhất VCI có
BLOCKED_KEY = f"{NAME}/_blocked"
BAR_SECONDS = 60
MIN_OVERLAP_BARS = 30  # số nến phút trùng tối thiểu để kết luận "lệch theo một tỷ lệ chung"

# Phiên giao dịch giờ VN (thứ 2-6): [bắt đầu, kết thúc)
TRADING_SESSIONS = ((time(9, 0), time(11, 30)), (time(13, 0), time(15, 0)))


COMPARE_COLUMNS = ("bar_ts", "open", "high", "low", "close", "volume")  # cột so sánh khi ghi lại


class ParquetWriter(Protocol):
    def write_groups(self, groups: list[Any], path: str) -> None:
        """Ghi một file, mỗi phần tử của `groups` đúng một row group (theo thứ tự)."""
        ...


class HistoryReader(Protocol):
    def read_rows(self, path: str) -> tuple[int, list[dict[str, Any]]] | None:
        """(số row group, các dòng cột COMPARE_COLUMNS) của file hiện có; None nếu chưa có."""
        ...


class HdfsParquetWriter:
    """Ghi qua `common.hdfs.write_parquet_row_groups_atomic` (import muộn)."""

    def __init__(self, fs: Any = None):
        self.fs = fs

    def write_groups(self, groups: list[Any], path: str) -> None:
        from stocklake.common.hdfs import write_parquet_row_groups_atomic

        write_parquet_row_groups_atomic(groups, path, fs=self.fs)


class HdfsHistoryReader:
    """Đọc file hiện có để so với nến mới (import muộn)."""

    def __init__(self, fs: Any = None):
        self.fs = fs

    def read_rows(self, path: str) -> tuple[int, list[dict[str, Any]]] | None:
        from stocklake.common.hdfs import parquet_file_info, read_parquet_files

        info = parquet_file_info(path, fs=self.fs)
        if info is None:
            return None
        t = read_parquet_files([path], columns=list(COMPARE_COLUMNS), fs=self.fs)
        return info["num_row_groups"], (t.to_pylist() if t.num_rows else [])

    def read_existing(self, path: str, min_row_groups: int) -> list[dict[str, Any]] | None:
        """Giữ tương thích: dòng của file nếu có ít nhất `min_row_groups` row group."""
        got = self.read_rows(path)
        return None if got is None or got[0] < min_row_groups else got[1]


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


def vn_month(ts: datetime) -> tuple[int, int]:
    """(năm, tháng) của ts theo giờ VN."""
    local = ts.astimezone(VN_TZ)
    return local.year, local.month


def month_runs(keys: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    """Các đoạn liên tiếp cùng khoá tháng: [(vị trí đầu, độ dài)]; `keys` đã sắp theo thời gian."""
    runs: list[tuple[int, int]] = []
    for i, k in enumerate(keys):
        if i and k == keys[i - 1]:
            runs[-1] = (runs[-1][0], runs[-1][1] + 1)
        else:
            runs.append((i, 1))
    return runs


def group_rows_by_month(rows: Sequence[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Sắp dòng theo bar_ts rồi chia theo tháng giờ VN: mỗi tháng một nhóm, theo thứ tự tháng."""
    ordered = sorted(rows, key=lambda r: r["bar_ts"])
    runs = month_runs([vn_month(r["bar_ts"]) for r in ordered])
    return [ordered[o : o + n] for o, n in runs]


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts


def _compare_key(r: Mapping[str, Any]) -> tuple:
    return (_utc(r["bar_ts"]),) + tuple(r[c] for c in COMPARE_COLUMNS[1:])


def _ts_key(r: Mapping[str, Any]) -> datetime:
    return _utc(r["bar_ts"])


def bar_prices(b: MinuteBar) -> dict[str, Any]:
    """Nến -> dict giá/khối lượng để so với file đang có."""
    return {"bar_ts": b.bar_ts, "open": b.open, "high": b.high, "low": b.low,
            "close": b.close, "volume": b.volume}  # fmt: skip


def compare_with_existing(existing: Sequence[Mapping[str, Any]], bars: Sequence[MinuteBar]):
    """Phân loại khác biệt giá giữa file năm hiện tại và nến mới (xem price_adjustment)."""
    return compare_prices(
        existing, [bar_prices(b) for b in bars], _ts_key, min_overlap=MIN_OVERLAP_BARS
    )


def bars_identical(
    existing: Sequence[Mapping[str, Any]], rows: Sequence[Mapping[str, Any]]
) -> bool:
    """Cùng số dòng và cùng (bar_ts, open, high, low, close, volume); bỏ qua cột ingest_*."""
    if len(existing) != len(rows):
        return False
    return sorted(map(_compare_key, existing)) == sorted(map(_compare_key, rows))


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
    unchanged: int = 0
    rows: int = 0
    requests: int = 0
    dups: int = 0
    blocked: dict[str, Any] | None = None
    stopped_reason: str | None = None
    adjustments: list[dict[str, Any]] = field(default_factory=list)
    mismatches: list[dict[str, Any]] = field(default_factory=list)
    minor: list[dict[str, Any]] = field(default_factory=list)
    forced: list[dict[str, Any]] = field(default_factory=list)


class _TradingHoursStop(Exception):
    """Vào giờ giao dịch ngay trước khi tải thêm lịch sử: dừng lượt, phần còn lại để lượt sau."""


_READ_FAILED = object()


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
        reader: HistoryReader | None = None,
        refetch_all_years: Sequence[str] = (),
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
        self.reader = reader or HdfsHistoryReader()
        self.per_year_rows: dict[str, int] = defaultdict(int)
        self.refetch_all = {s.strip().upper() for s in refetch_all_years if s.strip()}
        self._cache: dict[str, Any] = {}

    def plan(self, symbol: str, now: datetime, force: bool = False) -> list[WorkUnit]:
        """Các (mã, năm) còn phải làm của một mã (rỗng = bỏ qua mã); `force`: mọi năm."""
        cur = vn_year(now)
        years = (
            list(range(self.since.year, cur + 1))
            if force
            else years_needed(symbol, self.since.year, cur, self.state)
        )
        return [
            WorkUnit(
                state_key(symbol, y, y == cur), {"symbol": symbol, "year": y, "live": y == cur}
            )
            for y in years
        ]

    def _fetch(self, symbol: str, earliest_year: int, now: datetime, st: _Stats) -> list[MinuteBar]:
        """Lùi từ `now` tới 1/1 năm sớm nhất; ném lỗi nếu chưa phủ trọn (không trả dở dang)."""
        return self._fetch_range(symbol, fetch_start(earliest_year), now.replace(microsecond=0), st)

    def _fetch_range(
        self, symbol: str, start: datetime, end: datetime, st: _Stats
    ) -> list[MinuteBar]:
        bars: list[MinuteBar] = []
        for page in self.client.minute_pages(symbol, start, end):
            st.requests += 1
            bars.extend(page.bars)
        return bars

    def _mark_pending(self, symbol: str, years: Iterable[int], reason: str, run_id: str) -> None:
        """Đánh dấu các năm qua là chưa xong: bị chặn giữa chừng thì lượt sau làm tiếp."""
        for y in years:
            self.state.mark_failed(
                NAME, state_key(symbol, y), f"pending_refetch: {reason}", {"run_id": run_id}
            )

    def _check_adjustment(
        self, symbol: str, units: list[WorkUnit], bars: list[MinuteBar], now: datetime,
        run_id: str, st: _Stats,
    ) -> tuple[list[WorkUnit], list[MinuteBar], dict[str, Any] | None]:  # fmt: skip
        """So năm hiện tại với file đang có; lệch theo một tỷ lệ chung -> lấy thêm mọi năm.

        Trả (đơn vị cần ghi, nến, sự kiện điều chỉnh | None).
        """
        cur = vn_year(now)
        cur_bars = [b for b in bars if vn_year(b.bar_ts) == cur]
        existing = self._read(file_path(symbol, cur, self.base)) if cur_bars else None
        if not existing or not existing[1]:
            return units, bars, None
        cmp = compare_with_existing(existing[1], cur_bars)
        at = now.astimezone(timezone.utc).isoformat()
        info = {"symbol": symbol, "year": cur, **cmp.to_dict()}
        if cmp.kind == MINOR:
            log.info(
                "%s: %s nguồn sửa %d/%d nến năm %d", NAME, symbol, cmp.changed, cmp.overlap, cur
            )
            st.minor.append(info)
            return units, bars, None
        if cmp.kind == IRREGULAR:
            log.warning(
                "%s: %s năm %d lệch giá KHÔNG theo một tỷ lệ chung (%s): %d/%d nến lệch, "
                "hệ số ước lượng %s; KHÔNG tự tải lại năm cũ, xem ví dụ trong state/kết quả",
                NAME, symbol, cur, cmp.reason, cmp.changed, cmp.overlap, info["factor"],
            )  # fmt: skip
            st.mismatches.append(info)
            self.state.mark_failed(
                NAME, event_key(NAME, symbol, run_id), f"price_mismatch: {cmp.reason}",
                {**info, "detected_at": at, "detected_date": str(now.astimezone(VN_TZ).date()),
                 "run_id": run_id},
            )  # fmt: skip
            return units, bars, None
        if cmp.kind != SCALED:
            return units, bars, None
        past = list(range(self.since.year, cur))
        event = {
            **info, "detected_at": at, "detected_date": str(now.astimezone(VN_TZ).date()),
            "run_id": run_id, "years_marked": past, "years_rewritten": [],
            "files_rewritten": 0, "refetch": "pending",
        }  # fmt: skip
        log.warning(
            "%s: %s nguồn điều chỉnh giá theo hệ số %s (%d/%d nến khớp); tải lại mọi năm %s",
            NAME, symbol, info["factor"], cmp.matched, cmp.overlap, past + [cur],
        )  # fmt: skip
        self._mark_pending(symbol, past, f"price adjustment x{info['factor']}", run_id)
        self.state.mark_failed(NAME, event_key(NAME, symbol, run_id), "refetch pending", event)
        st.adjustments.append(event)
        earliest = units[0].params["year"]
        if earliest > self.since.year:
            if not self.allow_trading_hours and in_trading_hours(self.clock()):
                raise _TradingHoursStop
            extra = self._fetch_range(
                symbol, fetch_start(self.since.year), fetch_start(earliest), st
            )
            bars, dups = dedup_bars([*bars, *extra])
            st.dups += dups
        return self.plan(symbol, now), bars, event

    def _finish_event(self, event: dict[str, Any], written: list[int]) -> None:
        event.update(years_rewritten=written, files_rewritten=len(written), refetch="done")
        self.state.mark_done(NAME, event_key(NAME, event["symbol"], event["run_id"]), event)

    def _write_symbol(
        self, symbol: str, units: list[WorkUnit], bars: list[MinuteBar], now: datetime,
        run_id: str, st: _Stats,
    ) -> list[int]:  # fmt: skip
        """Ghi từng năm; trả các năm đã ghi file (không tính file không đổi)."""
        written: list[int] = []
        by_year = split_by_year(bars)
        for u in units:
            year = u.params["year"]
            ybars = by_year.get(year, [])
            n = len(ybars)
            if ybars:
                rows = build_rows(ybars, now, run_id)
                groups = group_rows_by_month(rows)
                path = file_path(symbol, year, self.base)
                if self._unchanged(path, rows, len(groups)):
                    st.unchanged += 1
                else:
                    self.writer.write_groups([self.to_table(g) for g in groups], path)
                    written.append(year)
                    st.files += 1
                    st.rows += n
                    self.per_year_rows[str(year)] += n
            elif u.params["live"]:
                log.warning("%s: %s rỗng cho năm hiện tại", NAME, symbol)
            # năm trước ngày lịch sử bắt đầu: xong, 0 dòng, không ghi file rỗng
            self.state.mark_done(NAME, u.key, {"source": SOURCE, "records": n})
        return written

    def _read(self, path: str) -> tuple[int, list[dict[str, Any]]] | None:
        """Đọc file đang có (nhớ trong lượt của mã); lỗi đọc -> None, kèm log."""
        if path not in self._cache:
            try:
                self._cache[path] = self.reader.read_rows(path)
            except Exception as exc:  # noqa: BLE001 - không đọc được thì cứ ghi lại
                log.warning(
                    "%s: không đọc được %s (%s: %s), sẽ ghi lại",
                    NAME, path, type(exc).__name__, exc,
                )  # fmt: skip
                self._cache[path] = _READ_FAILED
        got = self._cache[path]
        return None if got is _READ_FAILED else got

    def _unchanged(self, path: str, rows: list[dict[str, Any]], n_months: int) -> bool:
        """File đã có, đã chia row group theo tháng và nến y hệt -> khỏi ghi lại."""
        existing = self._read(path)
        return (
            existing is not None and existing[0] >= n_months and bars_identical(existing[1], rows)
        )

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
            force = sym in self.refetch_all
            units = self.plan(sym, now, force)
            if not units:
                st.skipped.append(sym)
                continue
            if self.max_symbols is not None and attempted >= self.max_symbols:
                st.stopped_reason = "max_symbols"
                break
            attempted += 1
            self._cache = {}
            try:
                if force:
                    past = [u.params["year"] for u in units if not u.params["live"]]
                    self._mark_pending(sym, past, "forced (--refetch-all-years)", run_id)
                bars = self._fetch(sym, units[0].params["year"], now, st)
                bars, dups = dedup_bars(completed_bars(bars, now))
                st.dups += dups
                units, bars, event = self._check_adjustment(sym, units, bars, now, run_id, st)
                written = self._write_symbol(sym, units, bars, now, run_id, st)
                if event is not None:
                    self._finish_event(event, written)
                if force:
                    years = [u.params["year"] for u in units]
                    st.forced.append({"symbol": sym, "years": years, "files_written": len(written)})
            except _TradingHoursStop:
                st.stopped_reason = "trading_hours"
                log.error("%s: %s vào giờ giao dịch trước khi tải lại lịch sử, dừng", NAME, sym)
                break
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
                self._cache = {}
            st.done.append(sym)
        return self._summary(run_id, st, t0)

    def _summary(self, run_id: str, st: _Stats, t0: datetime) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "symbols_done": st.done,
            "symbols_failed": st.failed,
            "symbols_skipped": st.skipped,
            "files_written": st.files,
            "files_unchanged": st.unchanged,
            "rows_written": st.rows,
            "requests": st.requests,
            "duplicates_dropped": st.dups,
            "per_year_rows": dict(sorted(self.per_year_rows.items())),
            "blocked": st.blocked,
            "stopped_reason": st.stopped_reason,
            "price_adjustments": [
                {
                    k: a[k]
                    for k in (
                        "symbol",
                        "factor",
                        "detected_at",
                        "overlap",
                        "matched",
                        "changed",
                        "years_marked",
                        "years_rewritten",
                        "files_rewritten",
                        "refetch",
                    )
                }
                for a in st.adjustments
            ],  # fmt: skip
            "price_mismatches": st.mismatches,
            "price_revisions_minor": st.minor,
            "forced_refetch": st.forced,
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
    reader: HistoryReader | None = None,
    refetch_all_years: Sequence[str] = (),
) -> dict[str, Any]:
    """Chạy collector với cấu hình mặc định; tham số keyword chỉ dùng để thay trong test.

    Thứ tự chọn mã: `symbols` > `group` (vn30|sample) > lỗi. `refetch_all_years`: mã phải tải
    lại mọi năm (từ `since`) dù năm cũ đã xong; mã không nằm trong danh sách chạy thì bỏ qua.
    """
    from stocklake.ingestion.framework import JsonFileStateStore

    if symbols:
        syms = list(dict.fromkeys(s.strip().upper() for s in symbols if s.strip()))
    elif group:
        from stocklake.common.config import load_app_conf

        syms = load_symbols(load_app_conf("symbols", settings), group)
    else:
        raise ValueError("Cần symbols hoặc group (vn30 | sample)")
    missing = sorted({s.strip().upper() for s in refetch_all_years} - set(syms) - {""})
    if missing:
        log.warning(
            "%s: --refetch-all-years bỏ qua mã không nằm trong lượt chạy: %s", NAME, missing
        )
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
        reader,
        refetch_all_years,
    )
    out = col.run()
    log.info(
        "%s: %s",
        NAME,
        {k: v for k, v in out.items() if not k.startswith(("symbols_", "price_mismatches"))},
    )
    return out


# ----------------------------------------------------------------- chia lại file cũ trên HDFS

_FILE_RE = re.compile(r"/year=(\d{4})/([^/]+)\.parquet$")
MAX_LISTED = 200  # số file tối đa liệt kê chi tiết trong summary


def _rm_quiet(fs: Any, path: str) -> None:
    try:
        if fs.exists(path):
            fs.rm(path)
    except Exception as exc:  # noqa: BLE001 - dọn rác không được che lỗi gốc
        log.warning("%s: không xoá được %s: %s", NAME, path, exc)


def _discover(fs: Any, root: str, symbols: set[str] | None, years: set[int] | None):
    """Liệt kê (mã, năm, đường dẫn) các file <root>/year=YYYY/<SYM>.parquet; bỏ file tạm."""
    try:
        found = sorted(fs.find(root))
    except FileNotFoundError:
        return []
    out = []
    for p in found:
        m = _FILE_RE.search(p)
        if not m:  # file tạm `.regroup-*`, `.tmp-*` không khớp (không kết thúc .parquet)
            continue
        year, sym = int(m.group(1)), m.group(2)
        if (symbols and sym not in symbols) or (years and year not in years):
            continue
        out.append((sym, year, p))
    return out


def _verify(fs: Any, tmp: str, original: Any, expected_groups: list[int]):
    """Kiểm file tạm với bản gốc (đã sắp theo bar_ts). Trả (lý do lỗi | None, parquet_file_info)."""
    import pyarrow.compute as pc

    from stocklake.common.hdfs import parquet_file_info, read_parquet_files

    info = parquet_file_info(tmp, fs=fs)
    if info is None:
        return "file tạm không tồn tại sau khi ghi", None
    if info["num_rows"] != original.num_rows:
        return f"số dòng {info['num_rows']} != {original.num_rows}", info
    if list(info["row_group_rows"]) != expected_groups:
        return f"row group {info['row_group_rows']} != theo tháng {expected_groups}", info
    back = read_parquet_files([tmp], fs=fs)
    if not back.schema.equals(original.schema, check_metadata=False):
        return "schema khác bản gốc", info
    if back.num_rows != original.num_rows:
        return "số dòng đọc lại khác bản gốc", info
    if pc.min_max(back["bar_ts"]).as_py() != pc.min_max(original["bar_ts"]).as_py():
        return "min/max bar_ts khác bản gốc", info
    if pc.sum(back["volume"]).as_py() != pc.sum(original["volume"]).as_py():
        return "tổng volume khác bản gốc", info
    if not back.equals(original):
        return "nội dung khác bản gốc", info
    return None, info


def _replace(fs: Any, path: str, tmp: str, res: dict[str, Any], expect_rows: int, groups: int):
    """Thay `path` bằng `tmp`: path -> bak, tmp -> path, xoá bak. Lỗi giữa chừng ghi vào `res`."""
    from stocklake.common.hdfs import parquet_file_info

    bak = f"{path}.regroup-bak-{uuid.uuid4().hex[:8]}"
    try:
        fs.mv(path, bak)
    except Exception as exc:  # noqa: BLE001
        res["reason"] = f"không đổi tên bản gốc sang bak: {exc}"
    if not (fs.exists(bak) and not fs.exists(path)):
        res.setdefault("reason", "đổi tên bản gốc sang bak không có hiệu lực")
        _rm_quiet(fs, tmp)  # bản gốc còn nguyên
        return
    try:
        fs.mv(tmp, path)
    except Exception as exc:  # noqa: BLE001
        res["reason"] = f"đổi tên tmp -> đích lỗi: {exc}"
    info = parquet_file_info(path, fs=fs) if fs.exists(path) else None
    if info is None or info["num_rows"] != expect_rows or info["num_row_groups"] != groups:
        # hoàn tác: trả bản gốc về chỗ cũ, giữ tmp để điều tra
        if info is None or not fs.exists(path):
            try:
                fs.mv(bak, path)
                res.setdefault("reason", "tmp -> đích không có hiệu lực; đã khôi phục bản gốc")
                return
            except Exception as exc:  # noqa: BLE001
                res["reason"] = f"{res.get('reason', '')} VÀ khôi phục lỗi: {exc}".strip()
        res["critical"] = True
        res["reason"] = (
            f"NGHIÊM TRỌNG: {res.get('reason', 'đích sai sau đổi tên')}. "
            f"Bản gốc giữ ở {bak}, bản mới giữ ở {tmp}; xử lý tay"
        )
        return
    _rm_quiet(fs, bak)
    if fs.exists(bak):
        res["leftover"] = bak  # đã thay xong, chỉ còn bak chưa xoá được


def _regroup_one(fs: Any, path: str, dry_run: bool) -> dict[str, Any]:
    from stocklake.common.hdfs import (
        parquet_file_info,
        read_parquet_files,
        write_parquet_row_groups_atomic,
    )

    res: dict[str, Any] = {"path": path, "status": "failed", "rows": 0}
    info = parquet_file_info(path, fs=fs)
    if info is None:
        res["reason"] = "file không tồn tại"
        return res
    res.update(
        rows=info["num_rows"], row_groups_before=info["num_row_groups"],
        row_groups_after=info["num_row_groups"], bytes_before=info["size_bytes"],
        bytes_after=info["size_bytes"],
    )  # fmt: skip
    orig = read_parquet_files([path], fs=fs)
    if orig.num_rows != info["num_rows"] or "bar_ts" not in orig.column_names:
        res["reason"] = "đọc file không khớp metadata hoặc thiếu cột bar_ts"
        return res
    ordered = orig.sort_by("bar_ts")
    runs = month_runs([vn_month(ts) for ts in ordered["bar_ts"].to_pylist()])
    if info["num_row_groups"] >= len(runs):
        res["status"] = "already_grouped"
        return res
    sizes = [n for _, n in runs]
    tmp = f"{path}.regroup-{uuid.uuid4().hex[:8]}"  # không kết thúc .parquet: reader bỏ qua
    try:
        write_parquet_row_groups_atomic([ordered.slice(o, n) for o, n in runs], tmp, fs=fs)
        reason, tinfo = _verify(fs, tmp, ordered, sizes)
    except Exception as exc:  # noqa: BLE001
        reason, tinfo = f"{type(exc).__name__}: {exc}", None
    if reason:
        _rm_quiet(fs, tmp)  # bản gốc không bị đụng tới
        res["reason"] = f"kiểm tra thất bại: {reason}"
        return res
    res.update(row_groups_after=len(runs), bytes_after=tinfo["size_bytes"])
    if dry_run:
        _rm_quiet(fs, tmp)
        res["status"] = "regrouped"
        return res
    _replace(fs, path, tmp, res, info["num_rows"], len(runs))
    if res.get("reason"):  # thay thế lỗi (bản gốc còn hoặc đã khôi phục, trừ khi critical)
        res["status"] = "failed"
        if not res.get("critical"):  # bản gốc còn nguyên
            res.update(row_groups_after=info["num_row_groups"], bytes_after=info["size_bytes"])
        return res
    res["status"] = "regrouped"
    return res


def regroup_history_files(
    symbols: Sequence[str] | None = None,
    years: Sequence[int] | None = None,
    dry_run: bool = False,
    fs: Any = None,
    base: str | None = None,
    source: str = SOURCE,
    max_listed: int = MAX_LISTED,
) -> dict[str, Any]:
    """Chia lại file lịch sử CŨ trên HDFS thành một row group mỗi tháng VN (không gọi VCI).

    Mỗi file: ghi bản chia lại ra file tạm `<tên>.parquet.regroup-<id>`, kiểm kỹ với bản gốc
    (số dòng, row group, schema, min/max bar_ts, tổng volume, toàn bộ nội dung), rồi mới thay.
    Kiểm lỗi -> chỉ xoá file tạm, bản gốc giữ nguyên. `dry_run` dừng sau bước kiểm.
    """
    t0 = _time.monotonic()
    if fs is None:
        from stocklake.common.hdfs import webhdfs_fs

        fs = webhdfs_fs()
    root = (table_path("raw", NAME) if base is None else table_path("raw", NAME, base)) + (
        f"/source={source}"
    )
    syms = {s.strip().upper() for s in symbols} if symbols else None
    files = _discover(fs, root, syms, set(years) if years else None)
    out: dict[str, Any] = {
        "dry_run": dry_run, "files_total": len(files), "already_grouped": 0, "regrouped": 0,
        "failed": [], "critical": [], "rows_total_before": 0, "rows_total_after": 0,
        "bytes_before": 0, "bytes_after": 0, "files": [],
    }  # fmt: skip
    for _, _, path in files:
        try:
            r = _regroup_one(fs, path, dry_run)
        except Exception as exc:  # noqa: BLE001 - một file lỗi không dừng cả lượt
            r = {"path": path, "status": "failed", "rows": 0,
                 "reason": f"{type(exc).__name__}: {exc}"}  # fmt: skip
        st = r["status"]
        if st == "failed":
            out["failed"].append({"path": path, "reason": r.get("reason")})
            if r.get("critical"):
                out["critical"].append({"path": path, "reason": r.get("reason")})
                log.error("%s: %s", NAME, r["reason"])
        else:
            out["already_grouped" if st == "already_grouped" else "regrouped"] += 1
        out["rows_total_before"] += r["rows"]
        out["rows_total_after"] += r["rows"]  # chia lại không đổi số dòng; lỗi -> giữ bản gốc
        out["bytes_before"] += r.get("bytes_before", 0)
        out["bytes_after"] += r.get("bytes_after", 0)
        if len(out["files"]) < max_listed:
            keys = ("path", "status", "rows", "row_groups_before", "row_groups_after")
            out["files"].append({k: r.get(k) for k in keys})
    out["files_listed_truncated"] = len(files) > max_listed
    out["seconds"] = round(_time.monotonic() - t0, 3)
    log.info("%s: regroup %s", NAME, {k: v for k, v in out.items() if k != "files"})
    return out
