"""Chạy kiểm tra chất lượng nến phút trên bảng clean và ghi dq_report.

Không chạy cùng lúc với job clean nến phút (job đó ghi đè phân vùng đang được đọc).
"""

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
from stocklake.dq.notes import DqNotes, annotate, load_dq_notes

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from stocklake.dq.checks import CheckResult

log = get_logger(__name__)

CALENDAR_SYMBOL = "VNINDEX"
TOP_N = 10
# Hậu tố check_name của metric tính lại khi bỏ giai đoạn đã biết (dòng riêng trong dq_report).
EXCL_SUFFIX = "__excl_known_periods"


def _read_clean(spark: SparkSession, table: str, cols: Sequence[str]) -> DataFrame:
    root = table_path("clean", table)
    try:
        return spark.read.option("basePath", root).parquet(root).select(*cols)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Không đọc được bảng clean {table} ở {root}: {exc}") from exc


def _json_results(results: Sequence[CheckResult], group: str) -> list[dict[str, Any]]:
    return [{**{k: json_value(v) for k, v in asdict(r).items()}, "group": group} for r in results]


def _with_worst_symbol(rows: list[dict[str, Any]], coverage: DataFrame) -> list[dict[str, Any]]:
    """Gắn mã có tỉ lệ thấp nhất vào kết quả intraday_symbol_coverage_ratio_min."""
    sym, _ = comp.symbol_coverage_min(coverage)
    for r in rows:
        if r["check_name"].startswith("intraday_symbol_coverage_ratio_min"):
            r["symbol"] = sym
    return rows


def _thresholds() -> dict[str, float]:
    return {
        "min_coverage_ratio": comp.DEFAULT_MIN_COVERAGE_RATIO,
        "min_symbol_coverage_ratio": comp.DEFAULT_MIN_SYMBOL_COVERAGE_RATIO,
        "min_bars_per_day": comp.DEFAULT_MIN_BARS_PER_DAY,
        "max_thin_day_share": comp.DEFAULT_MAX_THIN_DAY_SHARE,
        "volume_tolerance": vol.DEFAULT_TOLERANCE,
        "min_volume_match_ratio": vol.DEFAULT_MIN_MATCH_RATIO,
        "max_days_minute_gt_daily": vol.DEFAULT_MAX_DAYS_MINUTE_GT_DAILY,
    }


def run_intraday_dq(
    spark: SparkSession,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    write: bool = True,
    notes: DqNotes | None = None,
) -> dict[str, Any]:
    """Tính 2 check (đầy đủ ngày, volume phút vs ngày), ghi dq_report nếu `write`.

    `notes` None: đọc conf/app/dq_notes.yaml (thiếu file = không có ghi chú). Metric chính
    tính trên toàn bộ dữ liệu; khối `excluding_known_periods` tính lại khi bỏ các giai đoạn đã
    biết và được ghi thêm vào dq_report với check_name có hậu tố EXCL_SUFFIX.

    Trả dict JSON-serialisable: thresholds, results, notes, excluding_known_periods,
    known_period_stats, volume_distribution, worst_volume_days, thinnest_days,
    missing_days_by_symbol, bars_per_day, rows_written.
    """
    from pyspark.sql import functions as F

    from stocklake.dq.report import write_report

    notes = load_dq_notes() if notes is None else notes
    periods = notes.known_periods

    intraday = _read_clean(spark, "intraday_1m", ["symbol", "bar_ts", "trade_date", "volume"])
    daily = _read_clean(spark, "daily_ohlcv", ["symbol", "trade_date", "volume"])
    calendar = daily.filter(F.col("symbol") == CALENDAR_SYMBOL).select("trade_date").distinct()

    run_id = f"dq-intraday-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    run_ts = datetime.now(timezone.utc)

    coverage = comp.intraday_day_coverage(intraday, calendar, symbols, start, end).cache()
    diff = vol.intraday_vs_daily_volume(intraday, daily, symbols, start, end).cache()
    try:
        outside = comp.intraday_days_outside_calendar(intraday, calendar, symbols, start, end)
        comp_results = comp.summarize_completeness(
            coverage, outside_df=outside, run_id=run_id, run_ts=run_ts
        )
        distribution = vol.volume_diff_distribution(diff)
        vol_results = vol.summarize_volume(
            diff, run_id=run_id, run_ts=run_ts, distribution=distribution
        )
        results = [*comp_results, *vol_results]

        excl = comp.exclude_known_periods(coverage, periods)
        excl_results = comp.summarize_completeness(
            excl, run_id=run_id, run_ts=run_ts, suffix=EXCL_SUFFIX
        )

        report = {
            "run_id": run_id,
            "thresholds": _thresholds(),
            "results": _with_worst_symbol(
                [
                    *_json_results(comp_results, comp.GROUP.value),
                    *_json_results(vol_results, vol.GROUP.value),
                ],
                coverage,
            ),
            "notes": notes.to_json(),
            "excluding_known_periods": {
                "periods_excluded": len(periods),
                "results": _with_worst_symbol(_json_results(excl_results, comp.GROUP.value), excl),
                "bars_per_day": {
                    k: json_value(v) for k, v in comp.bars_per_day_distribution(excl).items()
                },
            },
            "known_period_stats": [
                {k: json_value(v) for k, v in st.items()}
                for st in comp.known_period_stats(coverage, intraday, periods, symbols, start, end)
            ],
            "volume_distribution": {k: json_value(v) for k, v in distribution.items()},
            "worst_volume_days": annotate(rows_to_dicts(vol.worst_volume_days(diff, TOP_N)), notes),
            "thinnest_days": annotate(rows_to_dicts(comp.thinnest_days(coverage, TOP_N)), notes),
            "missing_days_by_symbol": annotate(
                rows_to_dicts(
                    comp.completeness_by_symbol(coverage).filter("missing_days > 0"), TOP_N
                ),
                notes,
            ),
            "bars_per_day": {
                k: json_value(v) for k, v in comp.bars_per_day_distribution(coverage).items()
            },
        }
        report["rows_written"] = write_report(spark, [*results, *excl_results]) if write else 0
    finally:
        coverage.unpersist()
        diff.unpersist()
    log.info("intraday dq: run_id=%s rows_written=%s", run_id, report["rows_written"])
    return report
