"""Nạp và xử lý dữ liệu cho dashboard (Lát 1: đọc tạm lớp clean).

Phần đầu là hàm thuần (chỉ dùng thư viện chuẩn) để test không cần streamlit/pandas.
Các loader có cache import pandas/pyarrow bên trong hàm.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any, TypeVar

from stocklake.contracts import table_path

try:  # test ngoài container không có streamlit: bỏ cache
    import streamlit as st

    _cache = st.cache_data(ttl=60, show_spinner=False)
    _cache_long = st.cache_data(ttl=300, show_spinner=False)  # quét footer: nặng hơn, đổi chậm
except ImportError:  # pragma: no cover

    def _cache(fn):
        return fn

    _cache_long = _cache


VN_TZ = timezone(timedelta(hours=7))  # Asia/Ho_Chi_Minh, không có DST
DAILY_PATH = table_path("clean", "daily_ohlcv")
INTRADAY_PATH = table_path("clean", "intraday_1m")
DAILY_COLUMNS = ["symbol", "trade_date", "open", "high", "low", "close", "raw_close", "volume"]
INTRADAY_COLUMNS = ["symbol", "bar_ts", "open", "high", "low", "close", "volume", "trade_date"]
RETRY_PAUSE_S = 1.5  # nghỉ trước lần đọc lại khi job clean đang thay file phân vùng

_PART_RE = re.compile(r"year=(\d+)/month=(\d+)(?:/|$)")

T = TypeVar("T")

# ------------------------------------------------------------ hàm thuần


def build_filters(
    symbols: Sequence[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    partitions: Sequence[str] = ("year",),
) -> list[tuple[str, str, Any]] | None:
    """Tạo `filters` (AND) cho pyarrow; thêm lọc năm/tháng để cắt phân vùng hive."""
    f: list[tuple[str, str, Any]] = []
    if symbols:
        f.append(("symbol", "in", sorted(set(symbols))))
    if start:
        f.append(("trade_date", ">=", start))
        if "year" in partitions:
            f.append(("year", ">=", start.year))
    if end:
        f.append(("trade_date", "<=", end))
        if "year" in partitions:
            f.append(("year", "<=", end.year))
    return f or None


def intraday_filters(symbol: str, trade_date: date | None = None):
    """Lọc nến phút theo mã và (tuỳ chọn) một ngày; kèm year/month."""
    f: list[tuple[str, str, Any]] = [("symbol", "==", symbol)]
    if trade_date:
        f += [
            ("trade_date", "==", trade_date),
            ("year", "==", trade_date.year),
            ("month", "==", trade_date.month),
        ]
    return f


def intraday_month_filters(symbol: str, trade_date: date) -> list[tuple[str, str, Any]]:
    """Lọc khi đọc THẲNG thư mục `year=Y/month=M` (cột year/month không còn trong schema)."""
    return [("symbol", "==", symbol), ("trade_date", "==", trade_date)]


def intraday_month_path(base: str, trade_date: date) -> str:
    """Thư mục phân vùng hive của tháng chứa `trade_date` (tháng không đệm số 0)."""
    return f"{base.rstrip('/')}/year={trade_date.year}/month={trade_date.month}"


def parse_partition(path: str) -> tuple[int, int] | None:
    """'.../year=2026/month=11[/file]' -> (2026, 11); không phải phân vùng -> None."""
    m = _PART_RE.search(path.rstrip("/") + "/")
    return (int(m.group(1)), int(m.group(2))) if m else None


def latest_partition(paths: Iterable[str]) -> tuple[int, int] | None:
    """Phân vùng (năm, tháng) mới nhất trong danh sách đường dẫn; None nếu không có."""
    parts = [p for p in map(parse_partition, paths) if p]
    return max(parts) if parts else None


def merge_footer_stats(stats: Iterable[tuple[int, Any, Any]]) -> tuple[int, Any, Any]:
    """Gộp (num_rows, min, max) của nhiều file/row group -> (tổng dòng, min nhỏ nhất, max lớn nhất).

    min/max là None (thiếu thống kê) thì bỏ qua; không có giá trị nào -> (rows, None, None).
    """
    rows, lo, hi = 0, None, None
    for n, mn, mx in stats:
        rows += n
        if mn is not None and (lo is None or mn < lo):
            lo = mn
        if mx is not None and (hi is None or mx > hi):
            hi = mx
    return rows, lo, hi


def to_vn_time(ts: datetime) -> datetime:
    """bar_ts (UTC; naive coi là UTC) -> giờ Việt Nam (naive để hiển thị)."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(VN_TZ).replace(tzinfo=None)


def daily_return_pct(values: Sequence[float | None]) -> list[float | None]:
    """Lợi suất ngày (%) = close_t / close_{t-1} - 1; phần tử đầu và chỗ thiếu là None."""
    out: list[float | None] = []
    prev: float | None = None
    for v in values:
        if v is None or prev is None or prev == 0:
            out.append(None)
        else:
            out.append((v / prev - 1) * 100)
        prev = v if v is not None else prev
    return out


def moving_average(values: Sequence[float | None], window: int = 20) -> list[float | None]:
    """Trung bình trượt `window` phiên; None khi chưa đủ dữ liệu hợp lệ."""
    if window < 1:
        raise ValueError("window phải >= 1")
    out: list[float | None] = []
    for i in range(len(values)):
        chunk = values[max(0, i - window + 1) : i + 1]
        if len(chunk) < window or any(v is None for v in chunk):
            out.append(None)
        else:
            out.append(sum(chunk) / window)  # type: ignore[arg-type]
    return out


def retry_once(
    fn: Callable[[], T],
    *,
    pause_s: float = RETRY_PAUSE_S,
    sleep: Callable[[float], None] = time.sleep,
    retry_on: tuple[type[BaseException], ...] = (OSError,),
) -> T:
    """Gọi `fn`; nếu lỗi thuộc `retry_on` thì nghỉ `pause_s` giây rồi gọi lại ĐÚNG MỘT lần.

    Lần hai lỗi thì lỗi lần hai được ném ra như bình thường; lỗi không thuộc `retry_on`
    không được thử lại.
    """
    try:
        return fn()
    except retry_on:
        sleep(pause_s)
    return fn()


def summary_line(rows: int, symbols: int, first: Any, last: Any, symbols_label: str = "mã") -> str:
    """Dòng tóm tắt số dòng / mã / khoảng ngày (`symbols_label` ghi rõ phạm vi đếm mã)."""
    if not rows:
        return "Không có dữ liệu."
    return f"{rows:,} dòng · {symbols} {symbols_label} · từ {first} đến {last}"


# ------------------------------------------------------------ đọc dữ liệu


def _retryable_errors() -> tuple[type[BaseException], ...]:
    """Lỗi đọc có thể do job clean thay file giữa chừng: OSError (gồm FileNotFoundError,
    ArrowIOError, lỗi kết nối WebHDFS) và ArrowInvalid (file Parquet dở dang)."""
    try:
        import pyarrow as pa

        return (OSError, pa.ArrowInvalid)
    except ImportError:  # pragma: no cover
        return (OSError,)


def _read(path: str, columns, filters):
    """Gọi reader chung, thử lại một lần khi lỗi đọc; thiếu đường dẫn -> None.

    Lỗi sau lần thử lại vẫn được ném ra (trang hiện st.error). `st.cache_data` không lưu kết
    quả khi hàm ném lỗi, nên lỗi không bị nhớ; chỉ kết quả rỗng (None) bị cache 60 giây.
    """
    from stocklake.common.hdfs import read_parquet_dir

    try:
        table = retry_once(
            lambda: read_parquet_dir(path, columns=columns, filters=filters),
            retry_on=_retryable_errors(),
        )
    except FileNotFoundError:
        return None
    if table is None or table.num_rows == 0:
        return None
    return table


def _empty(columns: Sequence[str]):
    import pandas as pd

    return pd.DataFrame({c: [] for c in columns})


def _to_pandas(table, columns: Sequence[str]):
    """Arrow -> pandas, chuẩn hoá kiểu ngày/giờ (pandas 3: date32 -> object)."""
    import pandas as pd

    df = table.to_pandas()
    if "trade_date" in df:
        df["trade_date"] = pd.to_datetime(df["trade_date"])
    if "bar_ts" in df:
        ts = pd.to_datetime(df["bar_ts"])
        if ts.dt.tz is not None:
            ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
        df["bar_ts"] = ts
        df["bar_ts_vn"] = ts + pd.Timedelta(hours=7)  # giờ Việt Nam để hiển thị
    return df


@_cache
def load_daily(symbols=None, start: date | None = None, end: date | None = None):
    """Nến ngày clean (đã điều chỉnh) theo mã và khoảng ngày; sắp xếp theo mã, ngày."""
    table = _read(DAILY_PATH, DAILY_COLUMNS, build_filters(symbols, start, end))
    if table is None:
        return _empty(DAILY_COLUMNS)
    df = _to_pandas(table, DAILY_COLUMNS)
    return df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


@_cache
def load_intraday(symbol: str, trade_date: date):
    """Nến 1 phút clean của một mã trong MỘT ngày; thêm cột bar_ts_vn.

    Chỉ đọc thư mục phân vùng của tháng chứa ngày đó (không liệt kê/đọc 38 tháng còn lại).
    Tháng chưa có thư mục hoặc ngày/mã không có nến phút -> DataFrame rỗng.
    """
    path = intraday_month_path(INTRADAY_PATH, trade_date)
    table = _read(path, INTRADAY_COLUMNS, intraday_month_filters(symbol, trade_date))
    if table is None:
        return _empty([*INTRADAY_COLUMNS, "bar_ts_vn"])
    return _to_pandas(table, INTRADAY_COLUMNS).sort_values("bar_ts").reset_index(drop=True)


@_cache
def list_trade_days(symbol: str) -> list[date]:
    """Các ngày giao dịch của mã trong nến ngày clean, mới nhất trước (rẻ: ~1 nghìn dòng)."""
    table = _read(DAILY_PATH, ["symbol", "trade_date"], [("symbol", "==", symbol)])
    if table is None:
        return []
    return sorted(set(table.column("trade_date").to_pylist()), reverse=True)


@_cache
def list_symbols() -> list[str]:
    """Các mã có trong nến ngày clean."""
    import pyarrow.compute as pc

    table = _read(DAILY_PATH, ["symbol"], None)
    if table is None:
        return []
    return sorted(pc.unique(table.column("symbol")).to_pylist())  # chỉ danh sách mã duy nhất


def _column_stats(fs, path: str, col: str) -> tuple[int, Any, Any]:
    """(num_rows, min, max) của cột `col` từ footer một file Parquet; không đọc dữ liệu.

    Thiếu thống kê ở bất kỳ row group nào thì đọc riêng cột đó của file này để lấy min/max.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from stocklake.common.hdfs import read_parquet_files

    with fs.open(path, "rb") as f:
        md = pq.ParquetFile(f).metadata
    rg_stats: list[tuple[int, Any, Any]] = []
    complete = md.num_row_groups > 0
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        idx = next((j for j in range(rg.num_columns) if rg.column(j).path_in_schema == col), None)
        st_ = rg.column(idx).statistics if idx is not None else None
        if st_ is not None and st_.has_min_max:
            rg_stats.append((0, st_.min, st_.max))
        else:
            complete = False
    if complete or md.num_rows == 0:
        _, lo, hi = merge_footer_stats(rg_stats)
        return int(md.num_rows), lo, hi
    table = read_parquet_files([path], columns=[col])  # dự phòng: một file, một cột
    mm = pc.min_max(table.column(col)).as_py()
    return int(md.num_rows), mm["min"], mm["max"]


def _intraday_status() -> dict[str, Any]:
    """Số dòng + khoảng thời gian của nến phút từ footer Parquet; số mã theo tháng gần nhất."""
    import pyarrow.compute as pc

    from stocklake.common.hdfs import ls, read_parquet_files, webhdfs_fs

    fs = webhdfs_fs()
    base = INTRADAY_PATH
    empty = {"rows": 0, "symbols": 0, "first": None, "last": None, "symbols_label": "mã"}
    if not fs.exists(base):
        return empty
    files: list[str] = []  # (năm, tháng) lấy từ đường dẫn; bỏ _build_state.json và file tạm
    for year_dir in (p for p in ls(base, fs) if re.search(r"/year=\d+$", p.rstrip("/"))):
        for month_dir in (p for p in ls(year_dir, fs) if parse_partition(p)):
            for f in ls(month_dir, fs):
                name = f.rstrip("/").rsplit("/", 1)[-1]
                if name.endswith(".parquet") and not name.startswith(("_", ".")):
                    files.append(f)
    if not files:
        return empty
    rows, first, last = merge_footer_stats(_column_stats(fs, f, "bar_ts") for f in files)
    if not rows:
        return empty
    ym = latest_partition(files)
    latest_files = [f for f in files if parse_partition(f) == ym]
    sym_table = read_parquet_files(latest_files, columns=["symbol"], fs=fs)
    n_sym = pc.count_distinct(sym_table.column("symbol")).as_py() if sym_table.num_rows else 0
    label = f"mã trong tháng gần nhất ({ym[0]}-{ym[1]:02d})" if ym else "mã"
    return {"rows": rows, "symbols": n_sym, "first": first, "last": last, "symbols_label": label}


@_cache_long
def table_status(kind: str) -> dict[str, Any]:
    """Số dòng, số mã, khoảng ngày của clean daily ('daily') hoặc intraday ('intraday').

    daily: đọc 2 cột (~380 nghìn dòng). intraday: KHÔNG đọc bảng; số dòng/khoảng thời gian lấy
    từ footer Parquet, số mã chỉ đếm trong tháng gần nhất (khoá `symbols_label` ghi rõ).
    """
    if kind == "intraday":
        return retry_once(_intraday_status, retry_on=_retryable_errors())
    import pyarrow.compute as pc

    table = _read(DAILY_PATH, ["symbol", "trade_date"], None)
    if table is None:
        return {"rows": 0, "symbols": 0, "first": None, "last": None, "symbols_label": "mã"}
    mm = pc.min_max(table.column("trade_date")).as_py()
    return {
        "rows": table.num_rows,
        "symbols": pc.count_distinct(table.column("symbol")).as_py(),
        "first": mm["min"],
        "last": mm["max"],
        "symbols_label": "mã",
    }
