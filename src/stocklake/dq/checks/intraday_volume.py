"""Đối chiếu tổng volume nến phút với volume nến ngày (clean daily_ohlcv).

Nhóm báo cáo: CROSS_SOURCE (nhóm gần nhất: đối chiếu chéo hai tập dữ liệu độc lập;
không thêm thành viên enum mới).

Lý do chênh lệch đã biết:
- nến phút không gồm lô lẻ nên tổng volume phút <= volume ngày (đúng trên toàn bộ số đo VN30);
  vì vậy ngày có volume phút > volume ngày là lỗi cứng;
- phiên hôm nay chưa kết thúc nên nến phút chưa đủ.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from stocklake.dq.checks import CheckGroup, CheckResult
from stocklake.dq.checks._util import quantiles

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

TABLE_NAME = "intraday_1m"
GROUP = CheckGroup.CROSS_SOURCE
# Ngưỡng mặc định (duyệt 24/11/2026, đo trên VN30). Các hàm nhận tham số để ghi đè.
DEFAULT_TOLERANCE = 0.01  # |rel_diff| <= 1% là khớp
DEFAULT_MIN_MATCH_RATIO = 0.99  # >= 99% ngày so sánh được phải khớp
DEFAULT_MAX_DAYS_MINUTE_GT_DAILY = 0  # số ngày volume phút > volume ngày
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
) -> DataFrame:
    """Mỗi (symbol, trade_date) có nến phút: minute_volume, daily_volume, rel_diff, n_bars.

    rel_diff = (minute_volume - daily_volume) / daily_volume; null khi daily_volume
    null hoặc 0 (hoặc không có dòng daily cho ngày đó).
    """
    from pyspark.sql import functions as F

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
    )


def volume_diff_distribution(diff_df: DataFrame) -> dict[str, Any]:
    """Phân phối rel_diff trên các ngày so sánh được (rel_diff không null).

    Khoá: days_total, days_comparable, days_no_daily, share_within_{0.1pct,1pct,5pct,10pct},
    abs_p50/p90/p95/p99, min_rel_diff, max_rel_diff, mean_rel_diff,
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
    abs_df = diff_df.select(F.abs(rel).alias("abs_rel_diff"))
    p50, p90, p95, p99 = quantiles(abs_df, "abs_rel_diff", [0.5, 0.9, 0.95, 0.99])
    out.update(abs_p50=p50, abs_p90=p90, abs_p95=p95, abs_p99=p99)
    out.update(
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


def summarize_volume(
    diff_df: DataFrame,
    tolerance: float = DEFAULT_TOLERANCE,
    min_match_ratio: float = DEFAULT_MIN_MATCH_RATIO,
    run_id: str = "adhoc",
    run_ts: datetime | None = None,
    distribution: dict[str, Any] | None = None,
    max_days_minute_gt_daily: int = DEFAULT_MAX_DAYS_MINUTE_GT_DAILY,
) -> list[CheckResult]:
    """Metric tổng hợp (table_name = intraday_1m); ngưỡng mặc định ở đầu module.

    Có thể truyền `distribution` đã tính để khỏi tính lại.
    - intraday_volume_match_ratio: tỉ lệ ngày so sánh được có |rel_diff| <= tolerance;
      đạt khi >= min_match_ratio (không có ngày nào so sánh được thì không đạt).
    - intraday_volume_days_minute_gt_daily: số ngày so sánh được có volume phút > volume ngày;
      đạt khi <= max_days_minute_gt_daily (mặc định 0, kiểm cứng).
    - intraday_volume_days_no_daily: số ngày có nến nhưng thiếu/0 volume ngày (chỉ đo).
    - intraday_volume_abs_diff_p95: p95 của |rel_diff|; đạt khi <= tolerance.
    """
    from pyspark.sql import functions as F

    ts = run_ts or datetime.now(timezone.utc)
    dist = distribution or volume_diff_distribution(diff_df)
    n = dist["days_comparable"]
    within = diff_df.filter(F.abs("rel_diff") <= tolerance).count() if n else 0
    ratio = within / n if n else None
    gt = dist["days_minute_gt_daily"]

    def res(name: str, value: float | None, thr: float | None, ok: bool) -> CheckResult:
        return CheckResult(run_id, TABLE_NAME, name, value, thr, ok, ts)

    return [
        res(
            "intraday_volume_match_ratio",
            ratio,
            min_match_ratio,
            ratio is not None and ratio >= min_match_ratio,
        ),
        res(
            "intraday_volume_days_minute_gt_daily",
            float(gt),
            float(max_days_minute_gt_daily),
            gt <= max_days_minute_gt_daily,
        ),
        res("intraday_volume_days_no_daily", float(dist["days_no_daily"]), None, True),
        res(
            "intraday_volume_abs_diff_p95",
            dist["abs_p95"],
            tolerance,
            dist["abs_p95"] is not None and dist["abs_p95"] <= tolerance,
        ),
    ]
