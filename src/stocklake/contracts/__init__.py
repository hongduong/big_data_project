"""Data contracts dùng chung. Không import pyspark/pyarrow ở mức module."""

from stocklake.contracts import available_at, paths
from stocklake.contracts.convert import to_arrow, to_markdown, to_spark
from stocklake.contracts.paths import checkpoint_path, layer_root, table_path
from stocklake.contracts.schemas import ALL_SCHEMAS, Column, TableSchema, get_schema
from stocklake.contracts.topics import ALL_TOPICS, INTRADAY_PRICES, MARKET_NEWS, TopicSpec

__all__ = [
    "ALL_SCHEMAS",
    "ALL_TOPICS",
    "INTRADAY_PRICES",
    "MARKET_NEWS",
    "Column",
    "TableSchema",
    "TopicSpec",
    "available_at",
    "checkpoint_path",
    "get_schema",
    "layer_root",
    "paths",
    "table_path",
    "to_arrow",
    "to_markdown",
    "to_spark",
]
