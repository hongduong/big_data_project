"""Gom file nhỏ (compaction) cho bảng clean."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def compact_table(spark: SparkSession, layer: str, table: str, target_file_mb: int = 128) -> None:
    """Đọc bảng, ghi lại thành file cỡ ~target_file_mb."""
    raise NotImplementedError("Lát 2")
