"""Nến ngày raw -> clean.

Quyết định:
- Giá giữ kiểu double, KHÔNG làm tròn (VCI trả giá điều chỉnh lẻ như 6489.7); nhân 1000 cho
  nguồn `kvnd` có thể sai số dấu phẩy động rất nhỏ, chấp nhận.
- Dòng bị loại (đếm vào `rejected`): thiếu khoá/giá/khối lượng/available_at, hoặc `price_unit`
  lạ. Dòng sai logic giá (high < low...) được giữ và gắn cờ trong `dq_flags`.
- Chọn nguồn: vci > vndirect > khác; cùng nguồn lấy `ingest_ts` mới nhất.
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
    write_partitioned,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = get_logger(__name__)

RAW = get_schema("raw", "daily_ohlcv")
CLEAN = get_schema("clean", "daily_ohlcv")

# Đơn vị giá chấp nhận -> hệ số nhân sang đồng (khớp PRICE_UNIT_* ở sources/base.py).
UNIT_FACTORS = {"vnd": 1.0, "kvnd": 1000.0}
SOURCE_PRIORITY = ("vci", "vndirect")  # nguồn khác xếp sau cùng
PRICE_COLS = ("open", "high", "low", "close")
_REQUIRED = ("symbol", "trade_date", "source", "available_at", "volume", *PRICE_COLS)
_EPS = 1e-6  # dung sai khi so sánh giá


def _normalise(raw_df: DataFrame) -> DataFrame:
    """Ép kiểu theo raw, đổi giá sang đồng, thêm cột `is_valid`."""
    from pyspark.sql import functions as F

    df = conform(raw_df, RAW)
    unit = F.lower(F.trim(F.col("price_unit")))
    factor = F.lit(None).cast("double")
    for name, value in UNIT_FACTORS.items():
        factor = F.when(unit == name, F.lit(value)).otherwise(factor)
    for c in (*PRICE_COLS, "raw_close"):
        df = df.withColumn(c, F.col(c) * factor)
    valid = factor.isNotNull()
    for c in _REQUIRED:
        valid = valid & F.col(c).isNotNull()
    return df.withColumn("is_valid", valid)


def _flags(df: DataFrame) -> Column:
    from pyspark.sql import functions as F

    o, h, low, c, v = (F.col(x) for x in ("open", "high", "low", "close", "volume"))
    checks = [
        ("non_positive_price", (o <= 0) | (h <= 0) | (low <= 0) | (c <= 0)),
        ("high_lt_low", h < low - _EPS),
        ("close_out_of_range", (c > h + _EPS) | (c < low - _EPS)),
        ("open_out_of_range", (o > h + _EPS) | (o < low - _EPS)),
        ("negative_volume", v < 0),
    ]
    arr = F.array(*[F.when(cond, F.lit(name)) for name, cond in checks])
    return F.filter(arr, lambda x: x.isNotNull())


def count_rejected(raw_df: DataFrame) -> int:
    """Số dòng raw bị loại (thiếu trường bắt buộc hoặc đơn vị giá lạ)."""
    return _normalise(raw_df).filter("NOT is_valid").count()


def clean_daily_ohlcv(raw_df: DataFrame) -> DataFrame:
    """Raw -> clean: đổi đơn vị sang đồng, mỗi (symbol, trade_date) một dòng, gắn dq_flags.

    Trả đúng các cột contract clean.daily_ohlcv rồi tới `year` (cột phân vùng).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = _normalise(raw_df).filter("is_valid")
    prio = F.lit(len(SOURCE_PRIORITY))
    for i, name in reversed(list(enumerate(SOURCE_PRIORITY))):
        prio = F.when(F.col("source") == name, F.lit(i)).otherwise(prio)
    w = Window.partitionBy("symbol", "trade_date").orderBy(
        prio.asc(), F.col("ingest_ts").desc_nulls_last(), F.col("source").asc()
    )
    best = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    best = best.withColumn("dq_flags", _flags(best))
    best = best.withColumn("year", F.year("trade_date"))
    return select_contract(best, CLEAN)


def read_raw_daily(spark: SparkSession, years: list[int] | None = None) -> DataFrame | None:
    """Đọc raw daily (mọi source); None nếu chưa có dữ liệu."""
    from pyspark.sql import functions as F

    root = table_path("raw", RAW.name)
    if not path_exists(spark, root):
        return None
    try:
        df = (
            spark.read.option("basePath", root)
            .option("mergeSchema", "true")
            .option("pathGlobFilter", "*.parquet")  # bỏ qua file tạm `.parquet.tmp-*`
            .parquet(root)
        )
    except Exception as exc:  # noqa: BLE001
        if is_empty_input_error(exc):
            return None
        raise
    if years:
        df = df.filter(F.col("year").cast("int").isin([int(y) for y in years]))
    return df


def run_daily_raw_to_clean(spark: SparkSession, years: list[int] | None = None) -> dict[str, Any]:
    """Chạy job: đọc raw, ghi clean phân vùng theo year. Trả {raw_rows, clean_rows, ...}."""
    raw = read_raw_daily(spark, years)
    result = {"raw_rows": 0, "clean_rows": 0, "rejected": 0, "partitions": 0}
    if raw is None:
        log.warning("Chưa có raw daily_ohlcv")
        return result
    raw = raw.cache()
    try:
        result["raw_rows"] = raw.count()
        if result["raw_rows"] == 0:
            return result
        clean = clean_daily_ohlcv(raw).cache()
        try:
            result["clean_rows"] = clean.count()
            result["rejected"] = count_rejected(raw)
            if result["clean_rows"]:
                part = CLEAN.partition_by
                result["partitions"] = clean.select(*part).distinct().count()
                write_partitioned(spark, clean, table_path("clean", CLEAN.name), part)
        finally:
            clean.unpersist()
    finally:
        raw.unpersist()
    log.info("daily raw->clean: %s", result)
    return result
