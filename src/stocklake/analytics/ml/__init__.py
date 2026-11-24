"""Logistic Regression T+5, baseline và leakage test."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def train_baseline(spark: SparkSession, features: DataFrame) -> DataFrame:
    """Huấn luyện kịch bản sạch (chia theo thời gian) và baseline ngây thơ; trả ml_results."""
    raise NotImplementedError("Lát 3")


def run_leakage_scenarios(spark: SparkSession, features: DataFrame) -> DataFrame:
    """Chạy riêng 3 kịch bản rò rỉ; trả ml_results."""
    raise NotImplementedError("Lát 3")
