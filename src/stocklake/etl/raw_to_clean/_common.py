"""Hàm dùng chung cho job raw -> clean. pyspark chỉ import bên trong hàm."""

from __future__ import annotations

from typing import TYPE_CHECKING

from stocklake.contracts import TableSchema, to_spark

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

VN_TZ_OFFSET_SECONDS = 7 * 3600  # Việt Nam UTC+7, không có giờ mùa hè


def _contract_cols(df: DataFrame, schema: TableSchema, names: tuple[str, ...] | None = None):
    from pyspark.sql import functions as F

    types = {f.name: f.dataType for f in to_spark(schema).fields}
    cols = []
    for name in names or schema.column_names:
        if name in df.columns:
            cols.append(F.col(name).cast(types[name]).alias(name))
        else:
            cols.append(F.lit(None).cast(types[name]).alias(name))
    return cols


def conform(df: DataFrame, schema: TableSchema, names: tuple[str, ...] | None = None) -> DataFrame:
    """Chọn các cột theo contract, ép đúng kiểu; cột thiếu thành null."""
    return df.select(*_contract_cols(df, schema, names))


def select_contract(df: DataFrame, schema: TableSchema) -> DataFrame:
    """Cột theo contract (đúng kiểu) rồi tới các cột phân vùng suy ra (year, month)."""
    from pyspark.sql import functions as F

    # Cột phân vùng suy ra phải lấy từ `df` trước khi conform bỏ chúng đi.
    extra = [F.col(c).cast("int").alias(c) for c in schema.derived_partitions]
    return df.select(*_contract_cols(df, schema), *extra)


def vn_date(ts: Column) -> Column:
    """Ngày theo giờ VN của timestamp; không phụ thuộc timezone của session."""
    from pyspark.sql import functions as F

    days = F.floor((ts.cast("long") + VN_TZ_OFFSET_SECONDS) / 86400).cast("int")
    return F.date_add(F.lit("1970-01-01").cast("date"), days)


def path_exists(spark: SparkSession, path: str) -> bool:
    """Kiểm tra đường dẫn qua Hadoop FileSystem của Spark (HDFS hoặc local)."""
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return bool(p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p))


def is_empty_input_error(exc: Exception) -> bool:
    """Spark ném AnalysisException khi thư mục không có file Parquet nào."""
    return "infer schema" in str(exc) or "Path does not exist" in str(exc)


def write_partitioned(
    spark: SparkSession, df: DataFrame, path: str, partition_cols: tuple[str, ...]
) -> None:
    """Ghi Parquet phân vùng, ít file mỗi phân vùng, sắp xếp theo symbol, ghi đè động."""
    key = "spark.sql.sources.partitionOverwriteMode"
    old = spark.conf.get(key, None)
    spark.conf.set(key, "dynamic")
    try:
        (
            df.repartition(*partition_cols)
            .sortWithinPartitions("symbol")
            .write.mode("overwrite")
            .partitionBy(*partition_cols)
            .parquet(path)
        )
    finally:
        if old is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, old)
