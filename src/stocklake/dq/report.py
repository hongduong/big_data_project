"""Ghi kết quả kiểm tra vào curated/dq_report."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from stocklake.dq.checks import CheckResult


def write_report(spark: SparkSession, results: Sequence[CheckResult]) -> None:
    """Ghi results vào bảng curated dq_report."""
    raise NotImplementedError("Lát 2")
