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

Nguồn điều chỉnh giá: cả VCI lẫn VNDirect (adOpen/adHigh/adLow/adClose -> open/high/low/close)
đều ghi giá ĐÃ ĐIỀU CHỈNH, mà năm qua đã xong thì không bao giờ lấy lại -> khi nguồn nhân lại
toàn bộ lịch sử với một hệ số, file các năm cũ giữ hệ số cũ. Vì vậy, trước khi ghi năm hiện tại:
- So giá các ngày trùng với file đang có CÙNG nguồn (`price_adjustment.compare_prices`).
- Lệch theo MỘT tỷ lệ chung -> mọi năm ĐANG CÓ của mã (khoá state năm + file trên HDFS, kể cả
  ngoài khoảng start/end) bị đánh dấu chưa xong và đưa vào danh sách chờ
  `daily_ohlcv/<SYM>/_refetch` (năm -> nguồn); trong cùng lượt các năm đó được lấy lại CHỈ từ
  nguồn đã phát hiện (không dự phòng sang nguồn khác, để file nguồn ưu tiên không còn hệ số cũ).
  Bị chặn/lỗi: năm còn trong danh sách chờ, lượt sau tự làm tiếp. Sự kiện lưu ở
  `daily_ohlcv/<SYM>/_adjustment/<run_id>` (mỗi lượt một khoá, giữ lịch sử).
- Lệch không theo một tỷ lệ chung -> chỉ cảnh báo (WARNING + state + tóm tắt JSON).
- `refetch_all_years` (cờ `--refetch-all-years`): ép đưa mọi năm đang có của mã vào danh sách chờ
  (lấy từ nguồn ưu tiên).
"""

from __future__ import annotations

import os
import re
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any, Protocol

from stocklake.common.logging import get_logger
from stocklake.contracts import available_at, get_schema, table_path, to_arrow
from stocklake.contracts.available_at import VN_TZ
from stocklake.ingestion.framework import BaseCollector, RunResult, StateStore, WorkUnit
from stocklake.ingestion.price_adjustment import (
    DEFAULT_QUANTUM,
    IRREGULAR,
    MINOR,
    SCALED,
    compare_prices,
    event_key,
)
from stocklake.sources.base import DailyBar, SourceClient, SourceError

log = get_logger(__name__)

NAME = "daily_ohlcv"
DEFAULT_STATE_DIR = "/var/lib/stocklake/state"
STATE_FILE = "ingestion_state.json"
MIN_OVERLAP_DAYS = 5  # số phiên trùng tối thiểu để kết luận "lệch theo một tỷ lệ chung"
# Lượng tử giá theo nguồn: VCI 2 chữ số thập phân (đồng); VNDirect giả định 2 chữ số (nghìn đồng).
PRICE_QUANTUM = {"vci": 0.01, "vndirect": 0.01}
COMPARE_COLUMNS = ("trade_date", "open", "high", "low", "close", "volume")
_YEAR_KEY_RE = re.compile(r"/(\d{4})(#live)?$")
_FILE_YEAR_RE = re.compile(r"/year=(\d{4})/[^/]+\.parquet$")


class ParquetWriter(Protocol):
    def write(self, table: Any, path: str) -> None: ...


class HdfsParquetWriter:
    """Ghi qua `common.hdfs.write_parquet_atomic` (import muộn)."""

    def __init__(self, fs: Any = None):
        self.fs = fs

    def write(self, table: Any, path: str) -> None:
        from stocklake.common.hdfs import write_parquet_atomic

        write_parquet_atomic(table, path, fs=self.fs)


class HistoryReader(Protocol):
    def read_rows(self, path: str) -> list[dict[str, Any]] | None:
        """Các dòng (cột COMPARE_COLUMNS) của file đang có; None nếu chưa có."""
        ...

    def existing_years(self, symbol: str, base: str | None = None) -> set[int]:
        """Các năm đã có file của mã (mọi nguồn)."""
        ...


class HdfsDailyReader:
    """Đọc file đang có trên HDFS (import muộn)."""

    def __init__(self, fs: Any = None):
        self.fs = fs

    def _fs(self) -> Any:
        if self.fs is None:
            from stocklake.common.hdfs import webhdfs_fs

            self.fs = webhdfs_fs()
        return self.fs

    def read_rows(self, path: str) -> list[dict[str, Any]] | None:
        from stocklake.common.hdfs import read_parquet_files

        fs = self._fs()
        if not fs.exists(path):
            return None
        t = read_parquet_files([path], columns=list(COMPARE_COLUMNS), fs=fs)
        return t.to_pylist() if t.num_rows else []

    def existing_years(self, symbol: str, base: str | None = None) -> set[int]:
        root = table_path("raw", NAME) if base is None else table_path("raw", NAME, base)
        found = self._fs().glob(f"{root}/source=*/year=*/{symbol}.parquet")
        return {int(m.group(1)) for p in found if (m := _FILE_YEAR_RE.search(str(p)))}


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


def year_key(symbol: str, year: int, live: bool = False) -> str:
    return f"{NAME}/{symbol}/{year}" + ("#live" if live else "")


def pending_key(symbol: str) -> str:
    return f"{NAME}/{symbol}/_refetch"


def _as_date(v: Any) -> date:
    return v.date() if isinstance(v, datetime) else v


def compare_with_existing(
    existing: Sequence[Mapping[str, Any]], bars: Sequence[DailyBar], source: str
):
    """Phân loại khác biệt giá giữa file năm hiện tại và nến mới (xem price_adjustment)."""
    new = [
        {"trade_date": b.trade_date, "open": b.open, "high": b.high, "low": b.low,
         "close": b.close, "volume": b.volume}
        for b in bars
    ]  # fmt: skip
    return compare_prices(
        existing, new, lambda r: _as_date(r["trade_date"]),
        quantum=PRICE_QUANTUM.get(source, DEFAULT_QUANTUM), min_overlap=MIN_OVERLAP_DAYS,
    )  # fmt: skip


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
        reader: HistoryReader | None = None,
        refetch_all_years: Sequence[str] = (),
    ):
        """`reader` None = không so giá (không phát hiện điều chỉnh); điểm vào dùng HDFS."""
        self._inner = state
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
        self.reader = reader
        self.refetch_all = {s.strip().upper() for s in refetch_all_years if s.strip()}
        self.run_id = f"d{self.now.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
        self._pending: dict[str, dict[int, str]] = {}  # mã -> {năm: nguồn} còn phải lấy lại
        self._events: dict[str, dict[str, Any]] = {}  # mã -> sự kiện điều chỉnh của lượt này
        self._refetched: dict[str, set[int]] = defaultdict(set)  # năm đã lấy lại trong lượt này
        self.adjustments: list[dict[str, Any]] = []
        self.mismatches: list[dict[str, Any]] = []
        self.minor: list[dict[str, Any]] = []
        self.forced: list[dict[str, Any]] = []

    # -- danh sách chờ lấy lại (bền qua các lượt)
    def _load_pending(self, sym: str) -> dict[int, str]:
        get = getattr(self._inner, "get", None)
        entry = get(NAME, pending_key(sym)) if get else None
        if not entry or entry.get("status") == "done":
            return {}
        return {int(y): s for y, s in (entry.get("meta", {}).get("years") or {}).items()}

    def _save_pending(self, sym: str) -> None:
        years = self._pending.get(sym) or {}
        meta = {"years": {str(y): s for y, s in sorted(years.items())}, "run_id": self.run_id}
        if years:
            self._inner.mark_failed(NAME, pending_key(sym), "pending_refetch", meta)
        else:
            self._inner.mark_done(NAME, pending_key(sym), meta)

    def _known_years(self, sym: str) -> set[int]:
        """Năm qua đang có của mã: khoá state năm (mọi trạng thái) + file trên HDFS (mọi nguồn)."""
        years: set[int] = set()
        entries = getattr(self._inner, "entries", None)
        if entries is not None:
            for k in entries(NAME, f"{NAME}/{sym}/"):
                if m := _YEAR_KEY_RE.search(k):
                    years.add(int(m.group(1)))
        if self.reader is not None:
            try:
                years |= set(self.reader.existing_years(sym, self.base))
            except Exception as exc:  # noqa: BLE001 - chỉ còn năm theo state
                log.warning("%s: không liệt kê được file của %s: %s", NAME, sym, exc)
        return {y for y in years if y < self.today.year}

    def _schedule_refetch(self, sym: str, years: Iterable[int], source: str, reason: str) -> None:
        years = sorted(years)
        for y in years:
            self._inner.mark_failed(
                NAME, year_key(sym, y), f"pending_refetch: {reason}",
                {"run_id": self.run_id, "source": source},
            )  # fmt: skip
        self._pending.setdefault(sym, {}).update({y: source for y in years})
        self._save_pending(sym)

    def _refetch_finished(self, sym: str, year: int, wrote: bool) -> None:
        pend = self._pending.get(sym) or {}
        if year not in pend:
            return
        del pend[year]
        self._refetched[sym].add(year)
        self._save_pending(sym)
        ev = self._events.get(sym)
        if ev is not None:
            if wrote:
                ev["years_rewritten"].append(year)
                ev["files_rewritten"] += 1
            if not pend:
                ev["refetch"] = "done"
                self._inner.mark_done(NAME, event_key(NAME, sym, self.run_id), ev)

    def plan(self) -> Iterable[WorkUnit]:
        for sym in self.symbols:
            self._pending[sym] = self._load_pending(sym)
            if sym in self.refetch_all and self.sources:
                rng = range(self.start.year, min(self.end, self.today).year + 1)
                years = sorted((self._known_years(sym) | set(rng)) - {self.today.year})
                self._schedule_refetch(sym, years, self.sources[0].name, "forced")
                self.forced.append({"symbol": sym, "years": years})
        tried: set[str] = set()  # đơn vị lấy-lại đã đưa ra trong lượt này
        last = min(self.end, self.today)
        for year in range(self.start.year, last.year + 1):
            y_start, y_end = date(year, 1, 1), min(date(year, 12, 31), self.today)
            live = year == self.today.year
            for sym in self.symbols:
                key = year_key(sym, year, live)
                if live:
                    self._live_state.live.add(key)
                params = {"symbol": sym, "year": year, "start": y_start, "end": y_end, "live": live}
                src = None if live else self._pending[sym].get(year)
                if src:
                    params["only_source"] = src
                    tried.add(key)
                yield WorkUnit(key, params)
        # Năm còn chờ lấy lại (từ lượt trước ngoài khoảng start/end, hoặc vừa phát hiện điều chỉnh
        # ở năm hiện tại phía trên): tính lúc tới đây nên gồm cả phát hiện của lượt này.
        for sym in self.symbols:
            for year, src in sorted((self._pending.get(sym) or {}).items()):
                key = year_key(sym, year)
                if key in tried:
                    continue
                tried.add(key)
                yield WorkUnit(key, {
                    "symbol": sym, "year": year, "start": date(year, 1, 1),
                    "end": date(year, 12, 31), "live": False, "only_source": src,
                })  # fmt: skip

    def fetch(self, unit: WorkUnit, source: SourceClient) -> list[DailyBar]:
        p = unit.params
        only = p.get("only_source")
        if only and source.name != only:
            # Lấy lại vì điều chỉnh giá: chỉ nguồn đã phát hiện, để file của nguồn đó hết hệ số cũ.
            raise SourceError(f"{unit.key}: lấy lại chỉ từ nguồn {only}, bỏ qua {source.name}")
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
            if p.get("only_source"):
                self._refetch_finished(p["symbol"], p["year"], False)
            return  # năm trước ngày niêm yết: xong, 0 dòng, không ghi file rỗng
        path = file_path(p["symbol"], source_name, p["year"], self.base)
        if p["live"]:
            self._check_adjustment(p["symbol"], p["year"], records, source_name, path)
        rows = build_rows(records, datetime.now(timezone.utc))
        self.writer.write(self.to_table(rows), path)
        self.rows_written += len(rows)
        stat = self.per_source[source_name]
        stat["units"] += 1
        stat["rows"] += len(rows)
        if p.get("only_source"):
            self._refetch_finished(p["symbol"], p["year"], True)

    def _check_adjustment(
        self, sym: str, year: int, records: list[DailyBar], source: str, path: str
    ) -> None:
        """So năm hiện tại với file đang có cùng nguồn; lệch một tỷ lệ chung -> lên lịch lấy lại."""
        if self.reader is None:
            return
        try:
            existing = self.reader.read_rows(path)
        except Exception as exc:  # noqa: BLE001 - không đọc được: bỏ qua so sánh, vẫn ghi
            log.warning("%s: không đọc được %s để so giá: %s", NAME, path, exc)
            return
        if not existing:
            return
        cmp = compare_with_existing(existing, records, source)
        at = self.now.astimezone(timezone.utc).isoformat()
        info = {"symbol": sym, "year": year, "source": source, **cmp.to_dict()}
        base = {"detected_at": at, "detected_date": str(self.today), "run_id": self.run_id}
        if cmp.kind == MINOR:
            log.info("%s: %s nguồn %s sửa %d/%d phiên", NAME, sym, source, cmp.changed, cmp.overlap)
            self.minor.append(info)
        elif cmp.kind == IRREGULAR:
            log.warning(
                "%s: %s năm %d (nguồn %s) lệch giá KHÔNG theo một tỷ lệ chung (%s): %d/%d phiên "
                "lệch; KHÔNG tự lấy lại năm cũ, xem ví dụ trong state/kết quả",
                NAME, sym, year, source, cmp.reason, cmp.changed, cmp.overlap,
            )  # fmt: skip
            self.mismatches.append(info)
            self._inner.mark_failed(
                NAME, event_key(NAME, sym, self.run_id), f"price_mismatch: {cmp.reason}",
                {**info, **base},
            )  # fmt: skip
        elif cmp.kind == SCALED:
            # năm vừa lấy lại trong lượt này (vd do --refetch-all-years) đã mang hệ số mới
            years = sorted(self._known_years(sym) - self._refetched[sym])
            ev = {**info, **base, "years_marked": years, "years_rewritten": [],
                  "files_rewritten": 0, "refetch": "pending" if years else "done"}  # fmt: skip
            log.warning(
                "%s: %s nguồn %s điều chỉnh giá theo hệ số %s (%d/%d phiên khớp); lấy lại năm %s",
                NAME, sym, source, info["factor"], cmp.matched, cmp.overlap, years,
            )  # fmt: skip
            self._schedule_refetch(sym, years, source, f"price adjustment x{info['factor']}")
            self._events[sym] = ev
            self.adjustments.append(ev)
            if years:
                self._inner.mark_failed(
                    NAME, event_key(NAME, sym, self.run_id), "refetch pending", ev
                )
            else:
                self._inner.mark_done(NAME, event_key(NAME, sym, self.run_id), ev)

    def run(self) -> RunResult:
        res = super().run()
        for sym, ev in self._events.items():  # bị chặn/lỗi: lưu tiến độ, lượt sau làm tiếp
            if ev["refetch"] != "done":
                self._inner.mark_failed(
                    NAME, event_key(NAME, sym, self.run_id), "refetch pending", ev
                )
        return res

    def refetch_pending(self) -> dict[str, list[int]]:
        return {s: sorted(p) for s, p in self._pending.items() if p}


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
    *, writer=None, sources=None, state=None, reader=None, refetch_all_years=(),
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
    missing = sorted({s.strip().upper() for s in refetch_all_years} - set(symbols) - {""})
    if missing:
        log.warning(
            "%s: --refetch-all-years bỏ qua mã không nằm trong lượt chạy: %s", NAME, missing
        )
    col = DailyOhlcvCollector(
        sources,
        state or JsonFileStateStore(_state_path()),
        writer or HdfsParquetWriter(),
        symbols,
        start,
        end,
        now=now,
        reader=reader or HdfsDailyReader(),
        refetch_all_years=refetch_all_years,
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
    reader: HistoryReader | None = None,
    refetch_all_years: Sequence[str] = (),
) -> RunResult:
    """Chạy collector với cấu hình mặc định; tham số keyword chỉ dùng để thay trong test."""
    return _run(
        symbols, start, end, years_back, settings, writer=writer, sources=sources, state=state,
        reader=reader, refetch_all_years=refetch_all_years,
    )[0]  # fmt: skip


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
    reader: HistoryReader | None = None,
    refetch_all_years: Sequence[str] = (),
) -> dict[str, Any]:
    """Như `run_daily_ohlcv` nhưng trả tóm tắt dạng dict (cho job in JSON)."""
    res, col = _run(
        symbols, start, end, years_back, settings, writer=writer, sources=sources, state=state,
        reader=reader, refetch_all_years=refetch_all_years,
    )  # fmt: skip
    return {
        "done": len(res.done),
        "skipped": len(res.skipped),
        "failed": len(res.failed),
        "failed_keys": res.failed,
        "rows_written": col.rows_written,
        "per_source": dict(col.per_source),
        "stopped_blocked": res.stopped_blocked,
        "run_id": col.run_id,
        "price_adjustments": [
            {
                k: a[k]
                for k in (
                    "symbol",
                    "source",
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
            for a in col.adjustments
        ],  # fmt: skip
        "price_mismatches": col.mismatches,
        "price_revisions_minor": col.minor,
        "forced_refetch": col.forced,
        "refetch_pending": col.refetch_pending(),
    }
