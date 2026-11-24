"""Tạo SparkSession. File duy nhất trong common import pyspark."""

from __future__ import annotations

from pyspark.sql import SparkSession

from stocklake.common.config import Settings, get_settings


def get_spark(
    app_name: str,
    extra_conf: dict[str, str] | None = None,
    settings: Settings | None = None,
) -> SparkSession:
    s = settings or get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(s.spark_master)
        .config("spark.hadoop.fs.defaultFS", s.hdfs_uri)
        .config("spark.sql.session.timeZone", "UTC")
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    return builder.getOrCreate()
