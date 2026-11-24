"""Nạp và xử lý dữ liệu cho dashboard (Lát 1: đọc tạm lớp clean).

Phần đầu là hàm thuần (chỉ dùng thư viện chuẩn) để test không cần streamlit/pandas.
Các loader có cache import pandas/pyarrow bên trong hàm.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any, TypeVar

from stocklake.contracts import table_path

try:  # test ngoài container không có streamlit: bỏ cache
    import streamlit as st

    _cache = st.cache_data(ttl=60, show_spinner=False)
except ImportError:  # pragma: no cover

    def _cache(fn):
        return fn


VN_TZ = timezone(timedelta(hours=7))  # Asia/Ho_Chi_Minh, không có DST
DAILY_PATH = table_path("clean", "daily_ohlcv")
INTRADAY_PATH = table_path("clean", "intraday_1m")
DAILY_COLUMNS = ["symbol", "trade_date", "open", "high", "low", "close", "raw_close", "volume"]
INTRADAY_COLUMNS = ["symbol", "bar_ts", "open", "high", "low", "close", "volume", "trade_date"]
RETRY_PAUSE_S = 1.5  # nghỉ trước lần đọc lại khi job clean đang thay file phân vùng

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


def summary_line(rows: int, symbols: int, first: Any, last: Any) -> str:
    """Dòng tóm tắt số dòng / mã / khoảng ngày."""
    if not rows:
        return "Không có dữ liệu."
    return f"{rows:,} dòng · {symbols} mã · từ {first} đến {last}"


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
def load_intraday(symbol: str, trade_date: date | None = None):
    """Nến 1 phút clean của một mã (tuỳ chọn một ngày); thêm cột bar_ts_vn."""
    table = _read(INTRADAY_PATH, INTRADAY_COLUMNS, intraday_filters(symbol, trade_date))
    if table is None:
        return _empty([*INTRADAY_COLUMNS, "bar_ts_vn"])
    return _to_pandas(table, INTRADAY_COLUMNS).sort_values("bar_ts").reset_index(drop=True)


@_cache
def list_symbols() -> list[str]:
    """Các mã có trong nến ngày clean."""
    table = _read(DAILY_PATH, ["symbol"], None)
    if table is None:
        return []
    return sorted(set(table.column("symbol").to_pylist()))


@_cache
def table_status(kind: str) -> dict[str, Any]:
    """Số dòng, số mã, khoảng ngày của clean daily ('daily') hoặc intraday ('intraday')."""
    import pyarrow.compute as pc

    path, col = (DAILY_PATH, "trade_date") if kind == "daily" else (INTRADAY_PATH, "bar_ts")
    table = _read(path, ["symbol", col], None)
    if table is None:
        return {"rows": 0, "symbols": 0, "first": None, "last": None}
    mm = pc.min_max(table.column(col)).as_py()
    return {
        "rows": table.num_rows,
        "symbols": len(set(table.column("symbol").to_pylist())),
        "first": mm["min"],
        "last": mm["max"],
    }
