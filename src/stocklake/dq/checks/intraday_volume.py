"""Đối chiếu tổng volume nến phút với volume nến ngày (clean daily_ohlcv).

Nhóm báo cáo: CROSS_SOURCE (nhóm gần nhất: đối chiếu chéo hai tập dữ liệu độc lập;
không thêm thành viên enum mới).

Lý do chênh lệch đã biết:
- nến phút không gồm lô lẻ nên tổng volume phút <= volume ngày; ngày lệch thường chỉ vài chục
  cổ phiếu, nên một ngày là KHỚP khi |rel_diff| <= DEFAULT_TOLERANCE HOẶC
  |volume phút - volume ngày| < DEFAULT_MATCH_ABS_DIFF_SHARES (quy tắc 28/11/2026);
- ngày có volume phút > volume ngày là lỗi cứng, trừ các ngày nguồn có nến thừa đã ghi trong
  conf/app/dq_notes.yaml (`exclude_from: [volume_minute_gt_daily]`, runner loại trừ);
- phiên hôm nay chưa kết thúc nên nến phút chưa đủ.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from stocklake.dq.checks import CheckGroup, CheckResult
from stocklake.dq.checks._util import quantiles

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from stocklake.dq.notes import KnownAnomaly

TABLE_NAME = "intraday_1m"
GROUP = CheckGroup.CROSS_SOURCE
# Ngưỡng mặc định (duyệt 24/11/2026, đo trên VN30; ngưỡng tuyệt đối duyệt 28/11/2026).
# Các hàm nhận tham số để ghi đè.
DEFAULT_TOLERANCE = 0.01  # |rel_diff| <= 1% là khớp
DEFAULT_MATCH_ABS_DIFF_SHARES = 1000  # ... hoặc |phút - ngày| < 1000 cổ phiếu cũng là khớp
DEFAULT_MIN_MATCH_RATIO = 0.99  # >= 99% ngày so sánh được phải khớp
DEFAULT_MAX_DAYS_MINUTE_GT_DAILY = 0  # số ngày volume phút > volume ngày
MINUTE_GT_DAILY = "intraday_volume_days_minute_gt_daily"
_SHARE_BUCKETS = (("0.1pct", 0.001), ("1pct", 0.01), ("5pct", 0.05), ("10pct", 0.1))


def _d(v: date | str):
    from pyspark.sql import functions as F

    return F.lit(str(v)).cast("date")


def _restrict(df: DataFrame, symbols, start, end):
    from pyspark.sql import functions as F

    df = df.withColumn("trade_date", F.col("trade_date").cast("date"))
    if symbols:
        df = df.filter(F.col("symbol").isin(list(symbols)))
    if start is not None:
        df = df.filter(F.col("trade_date") >= _d(start))
    if end is not None:
        df = df.filter(F.col("trade_date") <= _d(end))
    return df


def intraday_vs_daily_volume(
    intraday_df: DataFrame,
    daily_df: DataFrame,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    per_day_df: DataFrame | None = None,
) -> DataFrame:
    """Mỗi (symbol, trade_date) có nến phút: minute_volume, daily_volume, rel_diff, n_bars,
    volume_diff.

    rel_diff = (minute_volume - daily_volume) / daily_volume; null khi daily_volume
    null hoặc 0 (hoặc không có dòng daily cho ngày đó). volume_diff = minute_volume -
    daily_volume (null khi không có volume ngày). `per_day_df` (kết quả
    `intraday_completeness.intraday_per_day`, có minute_volume và n_bars): dùng thay cho việc
    quét lại `intraday_df`.
    """
    from pyspark.sql import functions as F

    if per_day_df is not None:
        minute = _restrict(
            per_day_df.select("symbol", "trade_date", "minute_volume", "n_bars"),
            symbols,
            start,
            end,
        )
    else:
        minute = (
            _restrict(intraday_df.select("symbol", "trade_date", "volume"), symbols, start, end)
            .groupBy("symbol", "trade_date")
            .agg(
                F.sum(F.col("volume").cast("double")).alias("minute_volume"),
                F.count(F.lit(1)).alias("n_bars"),
            )
        )
    daily = _restrict(daily_df.select("symbol", "trade_date", "volume"), symbols, start, end)
    daily = daily.select(
        "symbol", "trade_date", F.col("volume").cast("double").alias("daily_volume")
    )
    out = minute.join(daily, ["symbol", "trade_date"], "left")
    ok = F.col("daily_volume").isNotNull() & (F.col("daily_volume") != 0)
    rel = F.when(ok, (F.col("minute_volume") - F.col("daily_volume")) / F.col("daily_volume"))
    return out.select(
        "symbol",
        "trade_date",
        F.col("minute_volume").cast("double").alias("minute_volume"),
        "daily_volume",
        rel.alias("rel_diff"),
        F.col("n_bars").cast("long").alias("n_bars"),
        (F.col("minute_volume") - F.col("daily_volume")).alias("volume_diff"),
    )


def match_condition(
    tolerance: float = DEFAULT_TOLERANCE,
    match_abs_diff_shares: float = DEFAULT_MATCH_ABS_DIFF_SHARES,
) -> Column:
    """Ngày so sánh được là KHỚP: |rel_diff| <= tolerance HOẶC |volume_diff| < số cổ phiếu."""
    from pyspark.sql import functions as F

    rel = F.col("rel_diff")
    return rel.isNotNull() & (
        (F.abs(rel) <= tolerance) | (F.abs(F.col("volume_diff")) < match_abs_diff_shares)
    )


def effective_abs_diff(match_abs_diff_shares: float = DEFAULT_MATCH_ABS_DIFF_SHARES) -> Column:
    """Độ lệch HIỆU DỤNG: 0 nếu |volume_diff| < số cổ phiếu, ngược lại |rel_diff|; null khi
    rel_diff null."""
    from pyspark.sql import functions as F

    rel = F.col("rel_diff")
    small = F.abs(F.col("volume_diff")) < match_abs_diff_shares
    return F.when(rel.isNotNull(), F.when(small, F.lit(0.0)).otherwise(F.abs(rel)))


def volume_diff_distribution(
    diff_df: DataFrame,
    tolerance: float = DEFAULT_TOLERANCE,
    match_abs_diff_shares: float = DEFAULT_MATCH_ABS_DIFF_SHARES,
) -> dict[str, Any]:
    """Phân phối rel_diff trên các ngày so sánh được (rel_diff không null).

    Khoá: days_total, days_comparable, days_no_daily, share_within_{0.1pct,1pct,5pct,10pct},
    abs_p50/p90/p95/p99 (|rel_diff| thô), effective_abs_p50/p90/p95/p99 (độ lệch hiệu dụng,
    `effective_abs_diff`), days_match (khớp theo `match_condition`), days_within_tolerance
    (|rel_diff| <= tolerance, quy tắc cũ), min_rel_diff, max_rel_diff, mean_rel_diff,
    days_minute_gt_daily, days_minute_lt_daily, days_equal. Giá trị None khi không có dữ liệu.
    """
    from pyspark.sql import functions as F

    rel = F.col("rel_diff")
    has = rel.isNotNull()
    aggs = [
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(has, 1).otherwise(0)).alias("n"),
        F.min(rel).alias("lo"),
        F.max(rel).alias("hi"),
        F.avg(rel).alias("mean"),
        F.sum(F.when(rel > 0, 1).otherwise(0)).alias("gt"),
        F.sum(F.when(rel < 0, 1).otherwise(0)).alias("lt"),
        F.sum(F.when(rel == 0, 1).otherwise(0)).alias("eq"),
        F.sum(F.when(match_condition(tolerance, match_abs_diff_shares), 1).otherwise(0)).alias(
            "match"
        ),
        F.sum(F.when(has & (F.abs(rel) <= tolerance), 1).otherwise(0)).alias("within"),
    ]
    for label, thr in _SHARE_BUCKETS:
        aggs.append(F.sum(F.when(has & (F.abs(rel) <= thr), 1).otherwise(0)).alias(f"w_{label}"))
    row = diff_df.agg(*aggs).collect()[0]
    total, n = int(row["total"] or 0), int(row["n"] or 0)
    out: dict[str, Any] = {
        "days_total": total,
        "days_comparable": n,
        "days_no_daily": total - n,
    }
    for label, _ in _SHARE_BUCKETS:
        out[f"share_within_{label}"] = (int(row[f"w_{label}"] or 0) / n) if n else None
    probs = [0.5, 0.9, 0.95, 0.99]
    p50, p90, p95, p99 = quantiles(diff_df.select(F.abs(rel).alias("a")), "a", probs)
    out.update(abs_p50=p50, abs_p90=p90, abs_p95=p95, abs_p99=p99)
    e50, e90, e95, e99 = quantiles(
        diff_df.select(effective_abs_diff(match_abs_diff_shares).alias("e")), "e", probs
    )
    out.update(effective_abs_p50=e50, effective_abs_p90=e90, effective_abs_p95=e95)
    out.update(effective_abs_p99=e99)
    out.update(
        days_match=int(row["match"] or 0),
        days_within_tolerance=int(row["within"] or 0),
        min_rel_diff=row["lo"],
        max_rel_diff=row["hi"],
        mean_rel_diff=row["mean"],
        days_minute_gt_daily=int(row["gt"] or 0),
        days_minute_lt_daily=int(row["lt"] or 0),
        days_equal=int(row["eq"] or 0),
    )
    return out


def worst_volume_days(diff_df: DataFrame, limit: int = 10) -> DataFrame:
    """Các (mã, ngày) lệch |rel_diff| lớn nhất."""
    from pyspark.sql import functions as F

    return (
        diff_df.filter(F.col("rel_diff").isNotNull())
        .orderBy(F.abs("rel_diff").desc(), "symbol", "trade_date")
        .limit(limit)
    )


def minute_gt_daily_days(diff_df: DataFrame, limit: int = 50) -> DataFrame:
    """Các (mã, ngày) có volume phút > volume ngày, thừa nhiều cổ phiếu nhất trước."""
    from pyspark.sql import functions as F

    return (
        diff_df.filter(F.col("rel_diff") > 0)
        .orderBy(F.col("volume_diff").desc(), "symbol", "trade_date")
        .limit(limit)
    )


# ------------------------------------------------------------ ngày bất thường bị loại trừ


def anomaly_condition(anomalies: Sequence[KnownAnomaly]) -> Column:
    """Điều kiện Spark: (symbol, trade_date) rơi vào một ngày bất thường (symbols rỗng = mọi mã)."""
    from pyspark.sql import functions as F

    cond = F.lit(False)
    for a in anomalies:
        c = F.col("trade_date") == _d(a.date)
        if a.symbols:
            c = c & F.col("symbol").isin(list(a.symbols))
        cond = cond | c
    return cond


def exclude_anomaly_days(diff_df: DataFrame, anomalies: Sequence[KnownAnomaly]) -> DataFrame:
    """Bỏ các (mã, ngày) rơi vào ngày bất thường đã cho khỏi `diff_df`."""
    if not anomalies:
        return diff_df
    return diff_df.filter(~anomaly_condition(anomalies))


def summarize_minute_gt_daily(
    diff_df: DataFrame,
    run_id: str = "adhoc",
    run_ts: datetime | None = None,
    max_days_minute_gt_daily: int = DEFAULT_MAX_DAYS_MINUTE_GT_DAILY,
    count: int | None = None,
) -> CheckResult:
    """intraday_volume_days_minute_gt_daily trên `diff_df`: số ngày rel_diff > 0; đạt khi
    <= `max_days_minute_gt_daily`. `count` đã tính sẵn thì khỏi đếm lại."""
    from pyspark.sql import functions as F

    ts = run_ts or datetime.now(timezone.utc)
    gt = diff_df.filter(F.col("rel_diff") > 0).count() if count is None else count
    return CheckResult(
        run_id,
        TABLE_NAME,
        MINUTE_GT_DAILY,
        float(gt),
        float(max_days_minute_gt_daily),
        gt <= max_days_minute_gt_daily,
        ts,
    )


def summarize_volume(
    diff_df: DataFrame,
    tolerance: float = DEFAULT_TOLERANCE,
    min_match_ratio: float = DEFAULT_MIN_MATCH_RATIO,
    run_id: str = "adhoc",
    run_ts: datetime | None = None,
    distribution: dict[str, Any] | None = None,
    max_days_minute_gt_daily: int = DEFAULT_MAX_DAYS_MINUTE_GT_DAILY,
    match_abs_diff_shares: float = DEFAULT_MATCH_ABS_DIFF_SHARES,
) -> list[CheckResult]:
    """Metric tổng hợp trên toàn bộ `diff_df` (table_name = intraday_1m); ngưỡng ở đầu module.

    Có thể truyền `distribution` đã tính (phải cùng `tolerance` / `match_abs_diff_shares`).
    - intraday_volume_match_ratio: tỉ lệ ngày so sánh được KHỚP (`match_condition`: |rel_diff|
      <= tolerance hoặc lệch < match_abs_diff_shares cổ phiếu); đạt khi >= min_match_ratio
      (không có ngày nào so sánh được thì không đạt).
    - intraday_volume_days_minute_gt_daily: số ngày so sánh được có volume phút > volume ngày;
      đạt khi <= max_days_minute_gt_daily (mặc định 0). Runner tính thêm bộ số loại trừ ngày
      bất thường bằng `summarize_minute_gt_daily` và lấy bộ đó làm kết luận.
    - intraday_volume_days_no_daily: số ngày có nến nhưng thiếu/0 volume ngày (chỉ đo).
    - intraday_volume_abs_diff_p95: p95 của độ lệch HIỆU DỤNG (`effective_abs_diff`); đạt khi
      <= tolerance.
    - intraday_volume_abs_diff_p95_raw: p95 của |rel_diff| thô (quy tắc cũ; chỉ đo).
    """
    ts = run_ts or datetime.now(timezone.utc)
    dist = distribution or volume_diff_distribution(diff_df, tolerance, match_abs_diff_shares)
    n = dist["days_comparable"]
    ratio = dist["days_match"] / n if n else None
    p95 = dist["effective_abs_p95"]

    def res(name: str, value: float | None, thr: float | None, ok: bool) -> CheckResult:
        return CheckResult(run_id, TABLE_NAME, name, value, thr, ok, ts)

    return [
        res(
            "intraday_volume_match_ratio",
            ratio,
            min_match_ratio,
            ratio is not None and ratio >= min_match_ratio,
        ),
        summarize_minute_gt_daily(
            diff_df, run_id, ts, max_days_minute_gt_daily, count=dist["days_minute_gt_daily"]
        ),
        res("intraday_volume_days_no_daily", float(dist["days_no_daily"]), None, True),
        res(
            "intraday_volume_abs_diff_p95",
            p95,
            tolerance,
            p95 is not None and p95 <= tolerance,
        ),
        res("intraday_volume_abs_diff_p95_raw", dist["abs_p95"], None, True),
    ]


def legacy_volume(distribution: dict[str, Any]) -> dict[str, Any]:
    """Số volume theo QUY TẮC CŨ (chỉ |rel_diff| <= tolerance; không loại trừ ngày nào)."""
    n = distribution["days_comparable"]
    return {
        "volume_match_ratio": distribution["days_within_tolerance"] / n if n else None,
        "volume_abs_diff_p95": distribution["abs_p95"],
        "volume_days_minute_gt_daily": distribution["days_minute_gt_daily"],
    }
