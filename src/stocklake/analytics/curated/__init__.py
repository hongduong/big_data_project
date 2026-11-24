"""Dựng các bảng curated."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def build_market_daily(spark: SparkSession) -> DataFrame:
    """Tổng hợp thị trường theo ngày từ clean (curated/market_daily)."""
    raise NotImplementedError("Lát 2")


def build_symbol_daily_features(spark: SparkSession) -> DataFrame:
    """Bảng feature theo (symbol, trade_date), chỉ dùng bản ghi available_at hợp lệ."""
    raise NotImplementedError("Lát 2")
