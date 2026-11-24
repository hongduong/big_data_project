"""Ghi kết quả kiểm tra vào curated/dq_report."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from stocklake.contracts import get_schema, table_path, to_spark

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from stocklake.dq.checks import CheckResult


def write_report(spark: SparkSession, results: Sequence[CheckResult]) -> int:
    """Nối (append) results vào bảng curated dq_report, một file nhỏ mỗi lần chạy.

    `run_id`, `run_ts` do bên gọi đặt một lần cho cả lượt chạy. Trả số dòng đã ghi.
    """
    if not results:
        return 0
    schema = get_schema("curated", "dq_report")
    rows = [
        (
            r.run_id,
            r.table_name,
            r.check_name,
            None if r.metric_value is None else float(r.metric_value),
            None if r.threshold is None else float(r.threshold),
            bool(r.passed),
            r.run_ts,
        )
        for r in results
    ]
    df = spark.createDataFrame(rows, to_spark(schema))
    df.coalesce(1).write.mode("append").parquet(table_path("curated", schema.name))
    return len(rows)
