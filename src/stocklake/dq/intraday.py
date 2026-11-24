"""Chạy kiểm tra chất lượng nến phút trên bảng clean và ghi dq_report."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from stocklake.common.logging import get_logger
from stocklake.contracts import table_path
from stocklake.dq.checks import intraday_completeness as comp
from stocklake.dq.checks import intraday_volume as vol
from stocklake.dq.checks._util import json_value, rows_to_dicts

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

CALENDAR_SYMBOL = "VNINDEX"
TOP_N = 10


def _read_clean(spark: SparkSession, table: str, cols: Sequence[str]) -> DataFrame:
    root = table_path("clean", table)
    try:
        return spark.read.option("basePath", root).parquet(root).select(*cols)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Không đọc được bảng clean {table} ở {root}: {exc}") from exc


def run_intraday_dq(
    spark: SparkSession,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    write: bool = True,
) -> dict[str, Any]:
    """Tính 2 check (đầy đủ ngày, volume phút vs ngày), ghi dq_report nếu `write`.

    Trả dict JSON-serialisable: results (kèm group), volume_distribution, worst_volume_days,
    thinnest_days, missing_days_by_symbol, bars_per_day, rows_written.
    """
    from pyspark.sql import functions as F

    from stocklake.dq.report import write_report

    intraday = _read_clean(spark, "intraday_1m", ["symbol", "bar_ts", "trade_date", "volume"])
    daily = _read_clean(spark, "daily_ohlcv", ["symbol", "trade_date", "volume"])
    calendar = daily.filter(F.col("symbol") == CALENDAR_SYMBOL).select("trade_date").distinct()

    run_id = f"dq-intraday-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    run_ts = datetime.now(timezone.utc)

    coverage = comp.intraday_day_coverage(intraday, calendar, symbols, start, end).cache()
    diff = vol.intraday_vs_daily_volume(intraday, daily, symbols, start, end).cache()
    try:
        outside = comp.intraday_days_outside_calendar(intraday, calendar, symbols, start, end)
        results = comp.summarize_completeness(
            coverage, outside_df=outside, run_id=run_id, run_ts=run_ts
        )
        groups = {r.check_name: comp.GROUP.value for r in results}
        distribution = vol.volume_diff_distribution(diff)
        vol_results = vol.summarize_volume(
            diff, run_id=run_id, run_ts=run_ts, distribution=distribution
        )
        groups.update({r.check_name: vol.GROUP.value for r in vol_results})
        results = [*results, *vol_results]

        report = {
            "run_id": run_id,
            "results": [
                {
                    **{k: json_value(v) for k, v in asdict(r).items()},
                    "group": groups[r.check_name],
                }
                for r in results
            ],
            "volume_distribution": {k: json_value(v) for k, v in distribution.items()},
            "worst_volume_days": rows_to_dicts(vol.worst_volume_days(diff, TOP_N)),
            "thinnest_days": rows_to_dicts(comp.thinnest_days(coverage, TOP_N)),
            "missing_days_by_symbol": rows_to_dicts(
                comp.completeness_by_symbol(coverage).filter("missing_days > 0"), TOP_N
            ),
            "bars_per_day": {
                k: json_value(v) for k, v in comp.bars_per_day_distribution(coverage).items()
            },
        }
        report["rows_written"] = write_report(spark, results) if write else 0
    finally:
        coverage.unpersist()
        diff.unpersist()
    log.info("intraday dq: run_id=%s rows_written=%s", run_id, report["rows_written"])
    return report
