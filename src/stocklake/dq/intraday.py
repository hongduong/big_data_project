"""Chạy kiểm tra chất lượng nến phút trên bảng clean và ghi dq_report.

Đọc clean dưới khoá đọc `clean.intraday_1m` rồi `clean.daily_ohlcv` (thứ tự cố định); job clean
là bên ghi nên chờ DQ xong (hoặc DQ chờ job clean xong), hai job có thể khởi động cùng lúc.
Quy tắc đạt/không đạt: xem stocklake.dq.verdict.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, TypeVar

from stocklake.common.logging import get_logger
from stocklake.contracts import table_path
from stocklake.dq.checks import intraday_completeness as comp
from stocklake.dq.checks import intraday_volume as vol
from stocklake.dq.checks._util import json_value, rows_to_dicts
from stocklake.dq.notes import EXCLUDE_VOLUME_MINUTE_GT_DAILY, DqNotes, annotate, load_dq_notes
from stocklake.dq.verdict import (
    BASIS_EXCL,
    BASIS_EXCL_ANOMALIES,
    VERDICT_RULE,
    Verdict,
    build_verdict,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

T = TypeVar("T")

log = get_logger(__name__)

CALENDAR_SYMBOL = "VNINDEX"
TOP_N = 10
MINUTE_GT_DAILY_LIST_LIMIT = 50  # danh sách (mã, ngày) volume phút > ngày trong JSON
# Tên khoá của job clean (bên ghi); DQ giữ khoá đọc cả hai, luôn xin theo thứ tự này.
LOCK_INTRADAY = "clean.intraday_1m"
LOCK_DAILY = "clean.daily_ohlcv"
OUTSIDE_CALENDAR = comp.OUTSIDE_CALENDAR


def _read_clean(spark: SparkSession, table: str, cols: Sequence[str]) -> DataFrame:
    root = table_path("clean", table)
    try:
        return spark.read.option("basePath", root).parquet(root).select(*cols)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Không đọc được bảng clean {table} ở {root}: {exc}") from exc


def _group_of(check_name: str) -> str:
    return (vol.GROUP if check_name.startswith("intraday_volume") else comp.GROUP).value


def _basis_of(check_name: str) -> str:
    return BASIS_EXCL_ANOMALIES if check_name == vol.MINUTE_GT_DAILY else BASIS_EXCL


def _jsonify(d: dict[str, Any]) -> dict[str, Any]:
    return {k: json_value(v) for k, v in d.items()}


def _checks_json(
    verdict: Verdict, all_symbol: str | None, excl_symbol: str | None
) -> list[dict[str, Any]]:
    """Check dạng JSON (giá trị an toàn cho json.dumps); gắn mã thấp nhất vào check theo mã."""
    out = []
    for c in verdict.checks:
        c = {**c, "metric": json_value(c["metric"]), "threshold": json_value(c["threshold"])}
        if "reference_all_data" in c:
            ref = {
                **c["reference_all_data"],
                "metric": json_value(c["reference_all_data"]["metric"]),
            }
            c["reference_all_data"] = ref
            if c["check_name"] == "intraday_symbol_coverage_ratio_min":
                c["symbol"], ref["symbol"] = excl_symbol, all_symbol
        out.append(c)
    return out


def run_under_read_locks(lock_fs: Any, fn: Callable[[], T]) -> tuple[T, float]:
    """Chạy `fn` khi giữ khoá đọc intraday_1m rồi daily_ohlcv; trả (kết quả, giây đã chờ).

    Mọi action Spark trên bảng clean phải xong TRONG `fn` (khoá nhả khi ra khỏi hàm).
    Khoá lỗi chờ quá hạn thì `LockTimeout` (TimeoutError) được ném ra.
    """
    from stocklake.common import locks

    with (
        locks.read_lock(lock_fs, LOCK_INTRADAY) as first,
        locks.read_lock(lock_fs, LOCK_DAILY) as second,
    ):
        out = fn()
    return out, round(float(first.waited_s) + float(second.waited_s), 3)


def wait_for_executors(spark: SparkSession) -> None:
    """Chạy một job Spark rỗng: chỉ trả về khi app đã có executor.

    Phải gọi TRƯỚC khi xin khoá đọc. Job batch không đặt `spark.cores.max` nên job clean có thể
    giữ hết core trong lúc chờ khoá ghi; nếu DQ giữ khoá đọc khi còn chờ core thì hai bên chờ
    nhau (khoá <-> core) tới khi job clean hết `wait_timeout_s`. Có executor rồi mới giữ khoá
    thì bên giữ khoá đọc luôn chạy tiếp được (standalone không thu hồi executor).
    """
    spark.sparkContext.parallelize([0], 1).count()


def _thresholds() -> dict[str, float]:
    return {
        "min_coverage_ratio": comp.DEFAULT_MIN_COVERAGE_RATIO,
        "min_symbol_coverage_ratio": comp.DEFAULT_MIN_SYMBOL_COVERAGE_RATIO,
        # ngày kỳ vọng: nến ngày của mã có volume >= giá trị này (cổ phiếu)
        "min_daily_volume_expected_day": comp.MIN_DAILY_VOLUME_FOR_EXPECTED_DAY,
        # ngày thưa: < min_bars_per_day nến VÀ (ngày - phút) > thin_min_gap_ratio * ngày
        # VÀ (ngày - phút) >= thin_min_gap_shares
        "min_bars_per_day": comp.DEFAULT_MIN_BARS_PER_DAY,
        "thin_min_gap_ratio": comp.DEFAULT_THIN_MIN_GAP_RATIO,
        "thin_min_gap_shares": comp.DEFAULT_THIN_MIN_GAP_SHARES,
        "max_thin_day_share": comp.DEFAULT_MAX_THIN_DAY_SHARE,
        # khớp: |rel_diff| <= volume_tolerance HOẶC |phút - ngày| < volume_match_abs_diff_shares
        "volume_tolerance": vol.DEFAULT_TOLERANCE,
        "volume_match_abs_diff_shares": vol.DEFAULT_MATCH_ABS_DIFF_SHARES,
        "min_volume_match_ratio": vol.DEFAULT_MIN_MATCH_RATIO,
        "max_volume_abs_diff_p95": vol.DEFAULT_TOLERANCE,
        "max_days_minute_gt_daily": vol.DEFAULT_MAX_DAYS_MINUTE_GT_DAILY,
    }


def run_intraday_dq(
    spark: SparkSession,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    write: bool = True,
    notes: DqNotes | None = None,
    lock_fs: Any = None,
) -> dict[str, Any]:
    """Tính 2 nhóm check (đầy đủ ngày, volume phút vs ngày), ghi dq_report nếu `write`.

    Đọc clean dưới khoá đọc (`lock_fs` None: `hadoop_fs(spark)`); `LockTimeout` bay ra cho
    bên gọi. Việc ghi dq_report nằm ngoài khoá.

    `notes` None: đọc conf/app/dq_notes.yaml (thiếu file = không có ghi chú). Check có hai bộ số
    đạt/không đạt trên bộ loại trừ: check đầy đủ ngày (tỉ lệ phủ, mã thấp nhất, ngày thiếu, ngày
    thưa, ngày ít nến) bỏ giai đoạn đã biết; volume phút > ngày bỏ ngày bất thường đánh dấu
    `exclude_from: [volume_minute_gt_daily]`. Bộ số toàn bộ dữ liệu là `reference_all_data`
    (JSON) / `<check>__all_data` (dq_report).

    Trả dict JSON-serialisable: run_id, verdict_rule, passed, lock_waited_s, thresholds, results
    (mỗi check: metric, threshold, passed, basis, reference_all_data), known_periods_excluded,
    notes, excluding_known_periods, known_period_stats, volume_distribution, worst_volume_days,
    thinnest_days, missing_days_by_symbol, bars_per_day, rows_written; thêm từ 28/11/2026:
    expected_day_filter, thin_days_top, days_daily_volume_after_last_bar_by_symbol,
    volume_minute_gt_daily_excluded, minute_gt_daily_days, legacy_rules.
    """
    from stocklake.common import locks
    from stocklake.dq.report import write_report

    notes = load_dq_notes() if notes is None else notes
    fs = locks.hadoop_fs(spark) if lock_fs is None else lock_fs
    run_id = f"dq-intraday-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    run_ts = datetime.now(timezone.utc)

    wait_for_executors(spark)
    (report, verdict), waited = run_under_read_locks(
        fs, lambda: _compute(spark, notes, run_id, run_ts, symbols, start, end)
    )
    report["lock_waited_s"] = waited
    report["rows_written"] = write_report(spark, verdict.rows) if write else 0
    log.info(
        "intraday dq: run_id=%s passed=%s lock_waited_s=%s rows_written=%s",
        run_id,
        report["passed"],
        waited,
        report["rows_written"],
    )
    return report


def _compute(
    spark: SparkSession,
    notes: DqNotes,
    run_id: str,
    run_ts: datetime,
    symbols: Sequence[str] | None,
    start: date | str | None,
    end: date | str | None,
) -> tuple[dict[str, Any], Verdict]:
    """Đọc clean và tính toàn bộ báo cáo; mọi thứ đã được thu về Python khi hàm trả.

    Bảng nến phút chỉ được quét một lần (`comp.intraday_per_day`, cache); các khung theo
    (mã, ngày) khác dựng từ đó. known_period_stats vẫn đọc nến của các giai đoạn đã biết.
    """
    from pyspark.sql import functions as F

    periods = notes.known_periods
    gt_anomalies = notes.anomalies_excluded_from(EXCLUDE_VOLUME_MINUTE_GT_DAILY)
    intraday = _read_clean(spark, "intraday_1m", ["symbol", "bar_ts", "trade_date", "volume"])
    daily = _read_clean(spark, "daily_ohlcv", ["symbol", "trade_date", "volume"])
    calendar = daily.filter(F.col("symbol") == CALENDAR_SYMBOL).select("trade_date").distinct()

    per_day = comp.intraday_per_day(intraday, symbols).cache()
    cal_days = comp.intraday_calendar_days(
        intraday, calendar, daily, symbols, start, end, per_day_df=per_day
    ).cache()
    diff = vol.intraday_vs_daily_volume(
        intraday, daily, symbols, start, end, per_day_df=per_day
    ).cache()
    try:
        coverage = comp.expected_days(cal_days)
        outside = comp.intraday_days_outside_calendar(
            intraday, calendar, symbols, start, end, per_day_df=per_day
        )
        after = comp.days_daily_volume_after_last_bar(per_day, calendar, daily, symbols, start, end)
        all_res = comp.summarize_completeness(
            coverage, outside_df=outside, run_id=run_id, run_ts=run_ts
        )
        excl = comp.exclude_known_periods(coverage, periods)
        excl_res = comp.summarize_completeness(excl, run_id=run_id, run_ts=run_ts)
        distribution = vol.volume_diff_distribution(diff)
        vol_res = vol.summarize_volume(
            diff, run_id=run_id, run_ts=run_ts, distribution=distribution
        )
        gt_excl = vol.summarize_minute_gt_daily(
            vol.exclude_anomaly_days(diff, gt_anomalies), run_id=run_id, run_ts=run_ts
        )
        verdict = build_verdict(
            [
                *[r for r in all_res if r.check_name != OUTSIDE_CALENDAR],
                *[r for r in vol_res if r.check_name == vol.MINUTE_GT_DAILY],
            ],
            [*excl_res, gt_excl],
            [
                *[r for r in all_res if r.check_name == OUTSIDE_CALENDAR],
                comp.summarize_after_last_bar(after, run_id=run_id, run_ts=run_ts),
                *[r for r in vol_res if r.check_name != vol.MINUTE_GT_DAILY],
            ],
            _group_of,
            basis_of=_basis_of,
        )
        all_symbol, _ = comp.symbol_coverage_min(coverage)
        excl_symbol, _ = comp.symbol_coverage_min(excl)
        legacy = comp.legacy_completeness(comp.exclude_known_periods(cal_days, periods))
        legacy.update(vol.legacy_volume(distribution))
        report = {
            "run_id": run_id,
            "verdict_rule": VERDICT_RULE,
            "passed": verdict.passed,
            "thresholds": _thresholds(),
            "results": _checks_json(verdict, all_symbol, excl_symbol),
            "known_periods_excluded": len(periods),
            "notes": notes.to_json(),
            "excluding_known_periods": {
                "periods_excluded": len(periods),
                "bars_per_day": _jsonify(comp.bars_per_day_distribution(excl)),
            },
            "known_period_stats": [
                _jsonify(st)
                for st in comp.known_period_stats(
                    coverage,
                    intraday,
                    periods,
                    symbols,
                    start,
                    end,
                    calendar_days_df=cal_days,
                )
            ],
            "expected_day_filter": comp.expected_day_filter_stats(cal_days),
            "volume_distribution": _jsonify(distribution),
            "worst_volume_days": annotate(rows_to_dicts(vol.worst_volume_days(diff, TOP_N)), notes),
            "minute_gt_daily_days": annotate(
                rows_to_dicts(vol.minute_gt_daily_days(diff, MINUTE_GT_DAILY_LIST_LIMIT)), notes
            ),
            "volume_minute_gt_daily_excluded": {
                "exclude_from": EXCLUDE_VOLUME_MINUTE_GT_DAILY,
                "anomalies": [
                    {"date": a.date.isoformat(), "symbols": list(a.symbols), "label": a.label}
                    for a in gt_anomalies
                ],
            },
            "thinnest_days": annotate(rows_to_dicts(comp.thinnest_days(coverage, TOP_N)), notes),
            "thin_days_top": annotate(rows_to_dicts(comp.top_thin_days(coverage, TOP_N)), notes),
            "missing_days_by_symbol": annotate(
                rows_to_dicts(
                    comp.completeness_by_symbol(coverage).filter("missing_days > 0"), TOP_N
                ),
                notes,
            ),
            "days_daily_volume_after_last_bar_by_symbol": rows_to_dicts(
                comp.after_last_bar_by_symbol(after), TOP_N
            ),
            "bars_per_day": _jsonify(comp.bars_per_day_distribution(coverage)),
            "legacy_rules": {
                "description": (
                    "Số theo quy tắc trước 28/11/2026, chỉ để so, không ghi dq_report. Đầy đủ "
                    "ngày: kỳ vọng = mọi ngày lịch trong [ngày nến đầu, ngày nến cuối], ngày "
                    "thưa = < min_bars_per_day nến, bỏ giai đoạn đã biết như bộ kết luận. "
                    "Volume: khớp chỉ khi |rel_diff| <= volume_tolerance, toàn bộ dữ liệu, "
                    "không loại ngày bất thường."
                ),
                "basis_completeness": BASIS_EXCL,
                "basis_volume": "all_data",
                **_jsonify(legacy),
            },
        }
    finally:
        per_day.unpersist()
        cal_days.unpersist()
        diff.unpersist()
    return report, verdict
