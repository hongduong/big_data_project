"""Nến 1 phút raw -> clean.

Quyết định:
- `trade_date` tính lại từ `bar_ts` theo giờ VN (UTC+7), không tin cột raw.
- Một dòng cho mỗi (symbol, bar_ts) kể cả khác nguồn: nguồn thật hơn replay, rồi
  `kafka_ts`, `ingest_ts` mới nhất.
- `dates` chỉ chọn tháng cần xử lý: clean phân vùng (year, month) và ghi đè cả phân vùng,
  nên phải đọc lại trọn các tháng chứa những ngày đó.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stocklake.common.logging import get_logger
from stocklake.contracts import get_schema, table_path
from stocklake.etl.raw_to_clean._common import (
    conform,
    is_empty_input_error,
    path_exists,
    select_contract,
    vn_date,
    write_partitioned,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

RAW = get_schema("raw", "intraday_prices")
CLEAN = get_schema("clean", "intraday_1m")

# Nguồn thật (khớp topic intraday_prices); mọi nguồn khác, gồm "replay", xếp sau.
REAL_SOURCES = ("vci", "vndirect", "kbs")
_REQUIRED = ("symbol", "bar_ts", "open", "high", "low", "close", "volume", "available_at")


def _normalise(raw_df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    df = conform(raw_df, RAW)
    valid = F.lit(True)
    for c in _REQUIRED:
        valid = valid & F.col(c).isNotNull()
    # Bản tin thử (vd. send_sample ở Lát 0): source = "replay" nhưng không có replay_run_id.
    run_id = F.trim(F.coalesce(F.col("replay_run_id"), F.lit("")))
    untagged = (F.lower(F.col("source")) == F.lit("replay")) & (run_id == F.lit(""))
    return df.withColumn("is_valid", valid).withColumn("is_untagged_replay", untagged)


def count_rejected(raw_df: DataFrame) -> int:
    """Số dòng raw bị loại do thiếu trường bắt buộc."""
    return _normalise(raw_df).filter("NOT is_valid").count()


def count_untagged_replay(raw_df: DataFrame) -> int:
    """Số dòng hợp lệ bị loại vì source = "replay" mà replay_run_id rỗng."""
    return _normalise(raw_df).filter("is_valid AND is_untagged_replay").count()


def clean_intraday(raw_df: DataFrame) -> DataFrame:
    """Raw -> clean: khử trùng theo (symbol, bar_ts), thêm trade_date (giờ VN), year, month.

    Trả đúng các cột contract clean.intraday_1m rồi tới `year`, `month`.
    Loại bản ghi source = "replay" không có replay_run_id (bản tin thử, không phải dữ liệu).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = _normalise(raw_df).filter("is_valid AND NOT is_untagged_replay")
    prio = F.when(F.lower(F.col("source")).isin(*REAL_SOURCES), F.lit(0)).otherwise(F.lit(1))
    w = Window.partitionBy("symbol", "bar_ts").orderBy(
        prio.asc(),
        F.col("kafka_ts").desc_nulls_last(),
        F.col("ingest_ts").desc_nulls_last(),
        F.col("source").asc_nulls_last(),
    )
    best = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    best = best.withColumn("trade_date", vn_date(F.col("bar_ts")))
    best = best.withColumn("year", F.year("trade_date")).withColumn("month", F.month("trade_date"))
    return select_contract(best, CLEAN)


def read_raw_intraday(spark: SparkSession, dates: list[str] | None = None) -> DataFrame | None:
    """Đọc raw intraday (tôn trọng `_spark_metadata`); None nếu chưa có dữ liệu."""
    from pyspark.sql import functions as F

    root = table_path("raw", RAW.name)
    if not path_exists(spark, root):
        return None
    try:
        df = spark.read.option("basePath", root).parquet(root)
    except Exception as exc:  # noqa: BLE001
        if is_empty_input_error(exc):
            return None
        raise
    if dates:
        months = sorted({str(d)[:7] for d in dates})  # 'YYYY-MM'
        df = df.filter(F.date_format(F.col("trade_date").cast("date"), "yyyy-MM").isin(months))
    return df


def run_intraday_raw_to_clean(
    spark: SparkSession, dates: list[str] | None = None
) -> dict[str, Any]:
    """Chạy job: đọc raw, ghi clean phân vùng (year, month). Trả {raw_rows, clean_rows, ...}."""
    raw = read_raw_intraday(spark, dates)
    result = {
        "raw_rows": 0,
        "clean_rows": 0,
        "rejected": 0,
        "dropped_untagged_replay": 0,
        "partitions": 0,
    }
    if raw is None:
        log.warning("Chưa có raw intraday_prices")
        return result
    raw = raw.cache()
    try:
        result["raw_rows"] = raw.count()
        if result["raw_rows"] == 0:
            return result
        clean = clean_intraday(raw).cache()
        try:
            result["clean_rows"] = clean.count()
            result["rejected"] = count_rejected(raw)
            result["dropped_untagged_replay"] = count_untagged_replay(raw)
            if result["clean_rows"]:
                part = CLEAN.partition_by
                result["partitions"] = clean.select(*part).distinct().count()
                write_partitioned(spark, clean, table_path("clean", CLEAN.name), part)
        finally:
            clean.unpersist()
    finally:
        raw.unpersist()
    log.info("intraday raw->clean: %s", result)
    return result
