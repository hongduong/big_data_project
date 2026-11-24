"""Kiểm tra đầy đủ của nến phút: mỗi (mã, ngày giao dịch) có nến không.

Lịch giao dịch = các `trade_date` của VNINDEX trong clean daily_ohlcv (do runner dựng).
Nhóm báo cáo: COMPLETENESS.
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
GROUP = CheckGroup.COMPLETENESS
# Ngưỡng tạm, chưa hiệu chỉnh: phải chỉnh lại theo số đo thật (xem bars_per_day_distribution).
DEFAULT_MIN_BARS_PER_DAY = 30
DEFAULT_MIN_COVERAGE_RATIO = 0.99


def _d(v: date | str):
    from pyspark.sql import functions as F

    return F.lit(str(v)).cast("date")


def _bars(intraday_df: DataFrame, symbols: Sequence[str] | None):
    from pyspark.sql import functions as F

    df = intraday_df.select(
        "symbol", "bar_ts", F.col("trade_date").cast("date").alias("trade_date")
    )
    if symbols:
        df = df.filter(F.col("symbol").isin(list(symbols)))
    return df


def _calendar(calendar_df: DataFrame):
    from pyspark.sql import functions as F

    return calendar_df.select(F.col("trade_date").cast("date").alias("cal_date")).distinct()


def _per_day(bars: DataFrame):
    from pyspark.sql import functions as F

    return bars.groupBy("symbol", "trade_date").agg(
        F.count(F.lit(1)).alias("n_bars"),
        F.min("bar_ts").alias("first_bar_ts"),
        F.max("bar_ts").alias("last_bar_ts"),
    )


def intraday_day_coverage(
    intraday_df: DataFrame,
    calendar_df: DataFrame,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
) -> DataFrame:
    """Một dòng cho mỗi (symbol, trade_date) kỳ vọng có nến.

    Kỳ vọng = ngày trong lịch nằm trong [max(start, ngày nến đầu của mã),
    min(end, ngày nến cuối của mã)]: mã không bị đòi dữ liệu trước ngày nó bắt đầu có nến.
    Mốc đầu/cuối tính trên toàn bộ nến của mã, không bị cắt bởi start/end.
    Cột: symbol, trade_date, n_bars, first_bar_ts, last_bar_ts, has_data (n_bars > 0).
    """
    from pyspark.sql import functions as F

    per_day = _per_day(_bars(intraday_df, symbols))
    cal = _calendar(calendar_df)
    bounds = per_day.groupBy("symbol").agg(
        F.min("trade_date").alias("first_date"), F.max("trade_date").alias("last_date")
    )
    lo = F.col("first_date")
    hi = F.col("last_date")
    if start is not None:
        lo = F.greatest(lo, _d(start))
    if end is not None:
        hi = F.least(hi, _d(end))
    ranges = bounds.select("symbol", lo.alias("lo"), hi.alias("hi"))
    expected = ranges.join(
        F.broadcast(cal), (F.col("cal_date") >= F.col("lo")) & (F.col("cal_date") <= F.col("hi"))
    ).select("symbol", F.col("cal_date").alias("trade_date"))
    out = expected.join(per_day, ["symbol", "trade_date"], "left")
    out = out.withColumn("n_bars", F.coalesce(F.col("n_bars"), F.lit(0)).cast("long"))
    out = out.withColumn("has_data", F.col("n_bars") > 0)
    return out.select("symbol", "trade_date", "n_bars", "first_bar_ts", "last_bar_ts", "has_data")


def intraday_days_outside_calendar(
    intraday_df: DataFrame,
    calendar_df: DataFrame,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
) -> DataFrame:
    """(symbol, trade_date, n_bars) của ngày có nến phút nhưng không có trong lịch.

    Lưu ý: lịch lấy từ VNINDEX daily nên phiên hôm nay (daily chưa về) cũng bị tính ở đây.
    """
    from pyspark.sql import functions as F

    bars = _bars(intraday_df, symbols)
    if start is not None:
        bars = bars.filter(F.col("trade_date") >= _d(start))
    if end is not None:
        bars = bars.filter(F.col("trade_date") <= _d(end))
    per_day = bars.groupBy("symbol", "trade_date").agg(F.count(F.lit(1)).alias("n_bars"))
    cal = _calendar(calendar_df)
    return per_day.join(F.broadcast(cal), per_day["trade_date"] == cal["cal_date"], "left_anti")


def summarize_completeness(
    coverage_df: DataFrame,
    min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY,
    outside_df: DataFrame | None = None,
    min_coverage_ratio: float = DEFAULT_MIN_COVERAGE_RATIO,
    run_id: str = "adhoc",
    run_ts: datetime | None = None,
) -> list[CheckResult]:
    """Các metric tổng hợp (table_name = intraday_1m).

    - intraday_day_coverage_ratio: tỉ lệ (mã, ngày) kỳ vọng có >= 1 nến; đạt khi >= ngưỡng
      (mặc định 0.99, chưa hiệu chỉnh). Không có cặp nào để kiểm thì metric None, không đạt.
    - intraday_days_missing: số (mã, ngày) kỳ vọng mà không có nến (chỉ đo, luôn đạt).
    - intraday_thin_days: số ngày có dữ liệu nhưng < `min_bars_per_day` nến; đạt khi = 0.
      `min_bars_per_day` mặc định 30 là mức lỏng tạm thời, PHẢI chỉnh theo số đo thật
      (phân phối số nến/ngày) trước khi dùng làm kết luận.
    - intraday_days_outside_calendar (nếu có `outside_df`): số (mã, ngày) có nến ngoài lịch.
    """
    from pyspark.sql import functions as F

    ts = run_ts or datetime.now(timezone.utc)
    row = coverage_df.agg(
        F.count(F.lit(1)).alias("expected"),
        F.sum(F.when(F.col("has_data"), 1).otherwise(0)).alias("with_data"),
        F.sum(
            F.when(F.col("has_data") & (F.col("n_bars") < min_bars_per_day), 1).otherwise(0)
        ).alias("thin"),
    ).collect()[0]
    expected, with_data, thin = (int(row[k] or 0) for k in ("expected", "with_data", "thin"))
    ratio = with_data / expected if expected else None

    def res(name: str, value: float | None, thr: float | None, ok: bool) -> CheckResult:
        return CheckResult(run_id, TABLE_NAME, name, value, thr, ok, ts)

    results = [
        res(
            "intraday_day_coverage_ratio",
            ratio,
            min_coverage_ratio,
            ratio is not None and ratio >= min_coverage_ratio,
        ),
        res("intraday_days_missing", float(expected - with_data), None, True),
        res("intraday_thin_days", float(thin), 0.0, thin == 0),
    ]
    if outside_df is not None:
        n_out = outside_df.count()
        results.append(res("intraday_days_outside_calendar", float(n_out), 0.0, n_out == 0))
    return results


def completeness_by_symbol(
    coverage_df: DataFrame, min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY
) -> DataFrame:
    """Theo mã: expected_days, days_with_data, missing_days, thin_days, coverage_ratio.

    Sắp xếp xấu nhất trước (thiếu nhiều ngày nhất, rồi nhiều ngày mỏng nhất).
    """
    from pyspark.sql import functions as F

    out = coverage_df.groupBy("symbol").agg(
        F.count(F.lit(1)).alias("expected_days"),
        F.sum(F.when(F.col("has_data"), 1).otherwise(0)).alias("days_with_data"),
        F.sum(
            F.when(F.col("has_data") & (F.col("n_bars") < min_bars_per_day), 1).otherwise(0)
        ).alias("thin_days"),
    )
    out = out.withColumn("missing_days", F.col("expected_days") - F.col("days_with_data"))
    out = out.withColumn("coverage_ratio", F.col("days_with_data") / F.col("expected_days"))
    return out.select(
        "symbol",
        "expected_days",
        "days_with_data",
        "missing_days",
        "thin_days",
        "coverage_ratio",
    ).orderBy(F.col("missing_days").desc(), F.col("thin_days").desc(), "symbol")


def thinnest_days(coverage_df: DataFrame, limit: int = 10) -> DataFrame:
    """Các (mã, ngày) có dữ liệu nhưng ít nến nhất."""
    from pyspark.sql import functions as F

    return (
        coverage_df.filter("has_data")
        .orderBy(F.col("n_bars").asc(), "symbol", "trade_date")
        .limit(limit)
    )


def bars_per_day_distribution(coverage_df: DataFrame) -> dict[str, Any]:
    """min/p5/p50/p95/max của n_bars trên các ngày có dữ liệu (dùng để chọn ngưỡng)."""
    from pyspark.sql import functions as F

    has = coverage_df.filter("has_data")
    mm = has.agg(
        F.count(F.lit(1)).alias("days"), F.min("n_bars").alias("lo"), F.max("n_bars").alias("hi")
    ).collect()[0]
    p5, p50, p95 = quantiles(has, "n_bars", [0.05, 0.5, 0.95])
    return {
        "days_with_data": int(mm["days"] or 0),
        "min": mm["lo"],
        "p5": p5,
        "p50": p50,
        "p95": p95,
        "max": mm["hi"],
    }
