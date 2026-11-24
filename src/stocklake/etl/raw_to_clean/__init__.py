"""Biến đổi raw -> clean: hàm nhận và trả DataFrame để test bằng Spark local."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def clean_daily_ohlcv(df: DataFrame) -> DataFrame:
    """Chọn nguồn, đổi đơn vị sang đồng, khử trùng, gắn available_at và dq_flags."""
    raise NotImplementedError("Lát 1")


def clean_intraday(df: DataFrame) -> DataFrame:
    """Khử trùng theo (symbol, bar_ts), thêm year/month, sắp xếp theo symbol."""
    raise NotImplementedError("Lát 1")
