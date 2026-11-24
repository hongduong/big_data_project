"""Collector nến ngày -> raw.daily_ohlcv (VCI trước, VNDirect dự phòng).

Bố cục file (CỐ ĐỊNH, job khác dựa vào):
    <table_path('raw','daily_ohlcv')>/source=<source>/year=<YYYY>/<SYMBOL>.parquet
Mỗi (mã, năm, nguồn) một file, ghi đè nguyên tử khi chạy lại. File KHÔNG chứa cột phân vùng
(`source`, `year`). Khi dự phòng đổi nguồn giữa các lần chạy, file của nguồn kia được giữ
nguyên: raw giữ mọi thứ, lớp clean chọn theo ưu tiên nguồn.

Đơn vị việc = (mã, năm), luôn lấy trọn năm (1/1 -> min(31/12, hôm nay VN)) để file là cả năm.
- Năm đã qua: khoá `daily_ohlcv/<SYMBOL>/<YYYY>`; xong rồi thì lần sau bỏ qua.
- Năm hiện tại (dữ liệu còn tăng): khoá `daily_ohlcv/<SYMBOL>/<YYYY>#live`, luôn lấy lại, nên
  chạy lại không trùng dòng (ghi đè cùng file). Sang năm mới, khoá không-hậu-tố chưa có nên
  năm đó được lấy lại một lần cuối rồi mới được bỏ qua.
"""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any, Protocol

from stocklake.common.logging import get_logger
from stocklake.contracts import available_at, get_schema, table_path, to_arrow
from stocklake.contracts.available_at import VN_TZ
from stocklake.ingestion.framework import BaseCollector, RunResult, StateStore, WorkUnit
from stocklake.sources.base import DailyBar, SourceClient, SourceError

log = get_logger(__name__)

NAME = "daily_ohlcv"
DEFAULT_STATE_DIR = "/var/lib/stocklake/state"
STATE_FILE = "ingestion_state.json"


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


def file_columns() -> tuple[str, ...]:
    """Cột nằm trong file parquet = cột schema trừ cột phân vùng (source; year là suy ra)."""
    schema = get_schema("raw", NAME)
    return tuple(c for c in schema.column_names if c not in schema.partition_by)


def build_rows(bars: Iterable[DailyBar], ingest_ts: datetime) -> list[dict[str, Any]]:
    """DailyBar -> dict theo thứ tự cột của schema, kiểu Python thuần, bỏ cột phân vùng."""
    cols = file_columns()
    rows = []
    for b in bars:
        vals = asdict(b)
        vals["ingest_ts"] = ingest_ts
        vals["available_at"] = available_at.daily_bar(b.trade_date)
        rows.append({c: vals[c] for c in cols})
    return rows


def rows_to_table(rows: list[dict[str, Any]]):
    """dict -> pyarrow.Table đúng kiểu schema (pyarrow import trong hàm)."""
    import pyarrow as pa

    full = to_arrow(get_schema("raw", NAME))
    schema = pa.schema([f for f in full if f.name in file_columns()])
    return pa.Table.from_pylist(rows, schema=schema)


def file_path(symbol: str, source: str, year: int, base: str | None = None) -> str:
    root = table_path("raw", NAME) if base is None else table_path("raw", NAME, base)
    return f"{root}/source={source}/year={year}/{symbol}.parquet"


def load_symbols(conf: Mapping[str, Any]) -> list[str]:
    """Mã cổ phiếu + chỉ số từ conf/app/symbols.yaml (nhóm `sample`, hoặc cấp gốc)."""
    group = conf.get("sample", conf)
    out: list[str] = []
    for key in ("stocks", "indices"):
        for s in group.get(key) or []:
            s = str(s).strip().upper()
            if s and s not in out:
                out.append(s)
    return out


class _LiveAwareState:
    """Bọc StateStore: khoá `live` luôn coi là chưa xong (phải lấy lại)."""

    def __init__(self, inner: StateStore):
        self.inner = inner
        self.live: set[str] = set()

    def is_done(self, collector: str, key: str) -> bool:
        return key not in self.live and self.inner.is_done(collector, key)

    def mark_done(self, collector, key, meta=None):
        self.inner.mark_done(collector, key, meta)

    def mark_failed(self, collector, key, error, meta=None):
        self.inner.mark_failed(collector, key, error, meta)


# ----------------------------------------------------------------- collector


class DailyOhlcvCollector(BaseCollector):
    name = NAME

    def __init__(
        self,
        sources: Sequence[SourceClient],
        state: StateStore,
        writer: ParquetWriter,
        symbols: Sequence[str],
        start: date,
        end: date,
        now: datetime | None = None,
        to_table: Callable[[list[dict[str, Any]]], Any] | None = None,
        base: str | None = None,
    ):
        self._live_state = _LiveAwareState(state)
        super().__init__(sources, self._live_state)
        self.writer = writer
        self.symbols = list(symbols)
        self.start, self.end = start, end
        self.now = now or datetime.now(timezone.utc)
        self.today = self.now.astimezone(VN_TZ).date()
        self.to_table = to_table or (lambda rows: rows_to_table(rows))
        self.base = base
        self.rows_written = 0
        # nguồn -> {"units": số file ghi, "rows": số dòng}
        self.per_source: dict[str, dict[str, int]] = defaultdict(lambda: {"units": 0, "rows": 0})

    def plan(self) -> Iterable[WorkUnit]:
        last = min(self.end, self.today)
        for year in range(self.start.year, last.year + 1):
            y_start, y_end = date(year, 1, 1), min(date(year, 12, 31), self.today)
            live = year == self.today.year
            for sym in self.symbols:
                key = f"{NAME}/{sym}/{year}" + ("#live" if live else "")
                if live:
                    self._live_state.live.add(key)
                yield WorkUnit(
                    key, {"symbol": sym, "year": year, "start": y_start, "end": y_end, "live": live}
                )

    def fetch(self, unit: WorkUnit, source: SourceClient) -> list[DailyBar]:
        p = unit.params
        bars = source.daily_bars(p["symbol"], p["start"], p["end"])
        if not bars and source is not self.sources[0]:
            # Nguồn dự phòng trả rỗng (vd chỉ số không có ở VNDirect): không xác nhận được
            # dữ liệu, coi là lỗi để đơn vị không bị đánh dấu xong nhầm.
            raise SourceError(f"{source.name}: rỗng cho {unit.key}, không xác nhận được")
        return bars

    def write(self, unit: WorkUnit, records: list[DailyBar], source_name: str) -> None:
        p = unit.params
        if not records:
            if p["live"]:
                log.warning("%s: %s rỗng cho năm hiện tại (nguồn %s)", NAME, unit.key, source_name)
            return  # năm trước ngày niêm yết: xong, 0 dòng, không ghi file rỗng
        rows = build_rows(records, datetime.now(timezone.utc))
        path = file_path(p["symbol"], source_name, p["year"], self.base)
        self.writer.write(self.to_table(rows), path)
        self.rows_written += len(rows)
        stat = self.per_source[source_name]
        stat["units"] += 1
        stat["rows"] += len(rows)


# ----------------------------------------------------------------- điểm vào tiện dụng


def _default_window(years_back: int, today: date) -> date:
    try:
        return today.replace(year=today.year - years_back)
    except ValueError:  # 29/02
        return today.replace(year=today.year - years_back, day=28)


def _state_path() -> str:
    return os.path.join(os.environ.get("COLLECTOR_STATE_DIR", DEFAULT_STATE_DIR), STATE_FILE)


def _run(
    symbols=None, start=None, end=None, years_back=1, settings=None,
    *, writer=None, sources=None, state=None,
):  # fmt: skip
    from stocklake.ingestion.framework import JsonFileStateStore

    if symbols is None:
        from stocklake.common.config import load_app_conf

        symbols = load_symbols(load_app_conf("symbols", settings))
    now = datetime.now(timezone.utc)
    today = now.astimezone(VN_TZ).date()
    end = end or today
    start = start or _default_window(years_back, end)
    if sources is None:
        from stocklake.sources.vci import VciClient
        from stocklake.sources.vndirect import VndirectClient

        sources = [VciClient(), VndirectClient()]
    col = DailyOhlcvCollector(
        sources,
        state or JsonFileStateStore(_state_path()),
        writer or HdfsParquetWriter(),
        symbols,
        start,
        end,
        now=now,
    )
    res = col.run()
    log.info(
        "%s: done=%d skipped=%d failed=%d rows=%d per_source=%s blocked=%s",
        NAME, len(res.done), len(res.skipped), len(res.failed), col.rows_written,
        dict(col.per_source), res.stopped_blocked,
    )  # fmt: skip
    return res, col


def run_daily_ohlcv(
    symbols: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    years_back: int = 1,
    settings=None,
    *,
    writer: ParquetWriter | None = None,
    sources: Sequence[SourceClient] | None = None,
    state: StateStore | None = None,
) -> RunResult:
    """Chạy collector với cấu hình mặc định; tham số keyword chỉ dùng để thay trong test."""
    return _run(
        symbols, start, end, years_back, settings, writer=writer, sources=sources, state=state
    )[0]


def run_daily_ohlcv_summary(
    symbols: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    years_back: int = 1,
    settings=None,
    *,
    writer: ParquetWriter | None = None,
    sources: Sequence[SourceClient] | None = None,
    state: StateStore | None = None,
) -> dict[str, Any]:
    """Như `run_daily_ohlcv` nhưng trả tóm tắt dạng dict (cho job in JSON)."""
    res, col = _run(
        symbols, start, end, years_back, settings, writer=writer, sources=sources, state=state
    )
    return {
        "done": len(res.done),
        "skipped": len(res.skipped),
        "failed": len(res.failed),
        "failed_keys": res.failed,
        "rows_written": col.rows_written,
        "per_source": dict(col.per_source),
        "stopped_blocked": res.stopped_blocked,
    }
