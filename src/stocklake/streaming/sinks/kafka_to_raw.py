"""Kafka -> raw (Lát 1/2). pyspark chỉ import trong hàm hoặc dưới TYPE_CHECKING."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQuery


def build_intraday_stream(
    spark: SparkSession,
    bootstrap: str | None = None,
    checkpoint: str | None = None,
    trigger_seconds: int = 10,
) -> StreamingQuery:
    """Đọc topic intraday_prices, parse JSON, đổi epoch ms sang timestamp,
    thêm cột Kafka + spark_batch_ts, ghi Parquet vào raw/intraday_prices
    (phân vùng trade_date) với checkpoint trên HDFS."""
    raise NotImplementedError("Lát 1")


def build_news_stream(
    spark: SparkSession,
    bootstrap: str | None = None,
    checkpoint: str | None = None,
    trigger_seconds: int = 30,
) -> StreamingQuery:
    """Đọc topic market_news, ghi Parquet vào raw/market_news (phân vùng ingest_date)."""
    raise NotImplementedError("Lát 2")
