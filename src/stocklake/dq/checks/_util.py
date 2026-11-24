"""Tiện ích dùng chung cho các check. pyspark chỉ import bên trong hàm."""

from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def quantiles(df: DataFrame, col: str, probs: list[float]) -> list[float | None]:
    """approxQuantile an toàn với DataFrame rỗng (trả toàn None)."""
    from pyspark.sql import functions as F

    one = df.select(F.col(col).cast("double").alias(col)).where(F.col(col).isNotNull())
    out = one.approxQuantile(col, probs, 0.001)
    if not out:
        return [None] * len(probs)
    return [float(x) for x in out]


def json_value(v: Any) -> Any:
    """Đổi giá trị Spark Row sang kiểu JSON được (ngày -> ISO, NaN -> None)."""
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def rows_to_dicts(df: DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    rows = (df.limit(limit) if limit else df).collect()
    return [{k: json_value(v) for k, v in r.asDict().items()} for r in rows]
