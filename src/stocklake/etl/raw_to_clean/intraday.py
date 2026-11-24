"""Nến 1 phút raw -> clean = hợp nhất lịch sử batch (raw/intraday_history) và streaming.

Quyết định:
- `trade_date` tính lại từ `bar_ts` theo giờ VN (UTC+7), không tin cột raw.
- Một dòng cho mỗi (symbol, bar_ts) kể cả khác nguồn. Ưu tiên (nhỏ = thắng):
  0 history của VCI; 1 history khác; 2 streaming nguồn thật (vci/vndirect/kbs);
  3 streaming replay có replay_run_id. Hoà trong cùng mức: `kafka_ts` mới nhất (chỉ
  streaming), rồi `ingest_ts` mới nhất, rồi tên nguồn.
- Streaming source = "replay" không có replay_run_id (bản tin thử) bị loại và đếm riêng.
- `dates` chỉ chọn tháng cần xử lý: clean phân vùng (year, month) và ghi đè cả phân vùng,
  nên mỗi tháng phải dựng lại từ TOÀN BỘ raw của CẢ HAI bảng (xem `run_intraday_raw_to_clean`).
"""

from __future__ import annotations

import math
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
HISTORY_TABLE = "intraday_history"  # schema lấy lười trong hàm (contract do agent khác thêm)
CLEAN = get_schema("clean", "intraday_1m")

# Nguồn thật (khớp topic intraday_prices); mọi nguồn khác, gồm "replay", xếp sau.
REAL_SOURCES = ("vci", "vndirect", "kbs")
_REQUIRED = ("symbol", "bar_ts", "open", "high", "low", "close", "volume", "available_at")
# Cột chung của hai đầu vào sau khi chiếu về một dạng.
_COMMON = (*_REQUIRED, "source", "ingest_ts", "kafka_ts")

# Mức ưu tiên (xem docstring module).
PRIO_HISTORY_VCI, PRIO_HISTORY_OTHER, PRIO_STREAM_REAL, PRIO_STREAM_REPLAY = 0, 1, 2, 3
# Ước lượng ~128 MB Parquet mỗi file với dòng nến phút (~30 byte/dòng nén); cần đo lại.
ROWS_PER_FILE = 4_000_000


def _valid_expr():
    from pyspark.sql import functions as F

    valid = F.lit(True)
    for c in _REQUIRED:
        valid = valid & F.col(c).isNotNull()
    return valid


def _normalise(raw_df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    df = conform(raw_df, RAW)
    valid = _valid_expr()
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


def _project_stream(stream_df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    df = _normalise(stream_df)
    real = F.lower(F.col("source")).isin(*REAL_SOURCES)
    prio = F.when(real, F.lit(PRIO_STREAM_REAL)).otherwise(F.lit(PRIO_STREAM_REPLAY))
    return df.select(
        *_COMMON,
        "is_valid",
        "is_untagged_replay",
        prio.alias("_prio"),
        F.lit("stream").alias("_origin"),
    )


def _project_history(history_df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    df = conform(history_df, get_schema("raw", HISTORY_TABLE))
    df = df.withColumn("kafka_ts", F.lit(None).cast("timestamp"))
    is_vci = F.lower(F.col("source")) == F.lit("vci")
    prio = F.when(is_vci, F.lit(PRIO_HISTORY_VCI)).otherwise(F.lit(PRIO_HISTORY_OTHER))
    return df.select(
        *_COMMON,
        _valid_expr().alias("is_valid"),
        F.lit(False).alias("is_untagged_replay"),
        prio.alias("_prio"),
        F.lit("history").alias("_origin"),
    )


def _combine(stream_df: DataFrame | None, history_df: DataFrame | None) -> DataFrame:
    """Chiếu hai đầu vào về cùng dạng (+ cờ hợp lệ, `_prio`, `_origin`) rồi union."""
    parts = []
    if stream_df is not None:
        parts.append(_project_stream(stream_df))
    if history_df is not None:
        parts.append(_project_history(history_df))
    if not parts:
        raise ValueError("Cần ít nhất một đầu vào (stream_df hoặc history_df)")
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def _dedupe(combined: DataFrame) -> DataFrame:
    """Lọc dòng hợp lệ, chọn một dòng mỗi (symbol, bar_ts); giữ `_prio`, thêm year/month."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = combined.filter("is_valid AND NOT is_untagged_replay")
    w = Window.partitionBy("symbol", "bar_ts").orderBy(
        F.col("_prio").asc(),
        F.col("kafka_ts").desc_nulls_last(),
        F.col("ingest_ts").desc_nulls_last(),
        F.col("source").asc_nulls_last(),
    )
    best = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    best = best.withColumn("trade_date", vn_date(F.col("bar_ts")))
    return best.withColumn("year", F.year("trade_date")).withColumn("month", F.month("trade_date"))


def clean_intraday(
    stream_df: DataFrame | None = None, history_df: DataFrame | None = None
) -> DataFrame:
    """Raw -> clean: hợp nhất history + streaming, khử trùng theo (symbol, bar_ts).

    Trả đúng các cột contract clean.intraday_1m rồi tới `year`, `month`.
    Loại streaming source = "replay" không có replay_run_id (bản tin thử, không phải dữ liệu).
    """
    return select_contract(_dedupe(_combine(stream_df, history_df)), CLEAN)


def _read_hive_parquet(spark: SparkSession, root: str, **options: str) -> DataFrame | None:
    reader = spark.read.option("basePath", root)
    for k, v in options.items():
        reader = reader.option(k, v)
    try:
        return reader.parquet(root)
    except Exception as exc:  # noqa: BLE001
        if is_empty_input_error(exc):
            return None
        raise


def read_raw_intraday(spark: SparkSession, dates: list[str] | None = None) -> DataFrame | None:
    """Đọc raw intraday (tôn trọng `_spark_metadata`); None nếu chưa có dữ liệu."""
    from pyspark.sql import functions as F

    root = table_path("raw", RAW.name)
    if not path_exists(spark, root):
        return None
    df = _read_hive_parquet(spark, root)
    if df is None:
        return None
    if dates:
        months = sorted({str(d)[:7] for d in dates})  # 'YYYY-MM'
        df = df.filter(F.date_format(F.col("trade_date").cast("date"), "yyyy-MM").isin(months))
    return df


def read_raw_history(spark: SparkSession, months: list[str] | None = None) -> DataFrame | None:
    """Đọc raw intraday_history (source, year lấy từ đường dẫn); None nếu chưa có bảng.

    `months` dạng 'YYYY-MM' (tháng theo giờ VN): file chia theo năm nên đọc các năm cần
    rồi lọc dòng theo tháng.
    """
    from pyspark.sql import functions as F

    root = table_path("raw", HISTORY_TABLE)
    if not path_exists(spark, root):
        return None
    df = _read_hive_parquet(
        spark,
        root,
        mergeSchema="true",
        pathGlobFilter="*.parquet",  # bỏ qua file tạm `.parquet.tmp-*`
    )
    if df is None:
        return None
    if months:
        months = sorted({str(m)[:7] for m in months})
        years = sorted({int(m[:4]) for m in months})
        df = df.filter(F.col("year").cast("int").isin(years))
        df = df.filter(F.date_format(vn_date(F.col("bar_ts")), "yyyy-MM").isin(months))
    return df


def _files_per_partition(clean_rows: int, partitions: int) -> int:
    """Số file mỗi phân vùng ~ ROWS_PER_FILE dòng/file; tháng nhỏ một file."""
    return max(1, math.ceil(clean_rows / max(partitions, 1) / ROWS_PER_FILE))


def run_intraday_raw_to_clean(
    spark: SparkSession, dates: list[str] | None = None
) -> dict[str, Any]:
    """Chạy job: đọc history + streaming raw, ghi clean phân vùng (year, month).

    Khi có `dates`, các tháng (giờ VN) chứa chúng được dựng lại từ MỌI dòng raw của cả hai
    bảng trong tháng đó. Bắt buộc: ghi đè phân vùng động thay cả tháng, nên nếu chỉ đọc một
    bảng thì dữ liệu của bảng kia trong tháng sẽ mất khỏi clean.
    """
    months = sorted({str(d)[:7] for d in dates}) if dates else None
    result = {
        "raw_rows": 0,
        "history_rows": 0,
        "stream_rows": 0,
        "clean_rows": 0,
        "clean_from_history": 0,
        "clean_from_stream_real": 0,
        "clean_from_replay": 0,
        "rejected": 0,
        "dropped_untagged_replay": 0,
        "partitions": 0,
    }
    stream = read_raw_intraday(spark, dates)
    history = read_raw_history(spark, months)
    if stream is None and history is None:
        log.warning("Chưa có raw intraday_prices lẫn intraday_history")
        return result

    from pyspark.sql import functions as F

    combined = _combine(stream, history)
    s = combined.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col("_origin") == "history", 1).otherwise(0)).alias("history"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("rejected"),
        F.sum(F.when(F.col("is_valid") & F.col("is_untagged_replay"), 1).otherwise(0)).alias(
            "untagged"
        ),
    ).collect()[0]
    result["raw_rows"] = int(s["total"] or 0)
    result["history_rows"] = int(s["history"] or 0)
    result["stream_rows"] = result["raw_rows"] - result["history_rows"]
    result["rejected"] = int(s["rejected"] or 0)
    result["dropped_untagged_replay"] = int(s["untagged"] or 0)
    if result["raw_rows"] == 0:
        return result

    best = _dedupe(combined).cache()
    try:
        by_prio = {r["_prio"]: r["count"] for r in best.groupBy("_prio").count().collect()}
        result["clean_from_history"] = by_prio.get(PRIO_HISTORY_VCI, 0) + by_prio.get(
            PRIO_HISTORY_OTHER, 0
        )
        result["clean_from_stream_real"] = by_prio.get(PRIO_STREAM_REAL, 0)
        result["clean_from_replay"] = by_prio.get(PRIO_STREAM_REPLAY, 0)
        result["clean_rows"] = sum(by_prio.values())
        if result["clean_rows"]:
            clean = select_contract(best, CLEAN)
            part = CLEAN.partition_by
            result["partitions"] = clean.select(*part).distinct().count()
            n_files = _files_per_partition(result["clean_rows"], result["partitions"])
            write_partitioned(spark, clean, table_path("clean", CLEAN.name), part, n_files)
    finally:
        best.unpersist()
    log.info("intraday raw->clean: %s", result)
    return result
