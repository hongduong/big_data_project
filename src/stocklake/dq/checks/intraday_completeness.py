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
    from pyspark.sql import Column, DataFrame

    from stocklake.dq.notes import KnownPeriod

TABLE_NAME = "intraday_1m"
GROUP = CheckGroup.COMPLETENESS
# Ngưỡng mặc định (duyệt 24/11/2026, đo trên VN30). Các hàm nhận tham số để ghi đè.
DEFAULT_MIN_COVERAGE_RATIO = 0.998  # tỉ lệ (mã, ngày) kỳ vọng có >= 1 nến, toàn bộ
DEFAULT_MIN_SYMBOL_COVERAGE_RATIO = 0.98  # cùng tỉ lệ đó, cho từng mã
DEFAULT_MIN_BARS_PER_DAY = 100  # ngày có dữ liệu mà < 100 nến là ngày mỏng
DEFAULT_MAX_THIN_DAY_SHARE = 0.01  # ngày mỏng <= 1% số (mã, ngày) có dữ liệu
_VN_OFFSET_SECONDS = 7 * 3600


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


# ------------------------------------------------------------ giai đoạn đã biết


def period_condition(periods: Sequence[KnownPeriod]) -> Column:
    """Điều kiện Spark: (symbol, trade_date) nằm trong một giai đoạn đã biết."""
    from pyspark.sql import functions as F

    cond = F.lit(False)
    for p in periods:
        c = F.col("symbol") == F.lit(p.symbol)
        if p.start is not None:
            c = c & (F.col("trade_date") >= _d(p.start))
        if p.end is not None:
            c = c & (F.col("trade_date") <= _d(p.end))
        cond = cond | c
    return cond


def exclude_known_periods(coverage_df: DataFrame, periods: Sequence[KnownPeriod]) -> DataFrame:
    """Bỏ các (mã, ngày) thuộc giai đoạn đã biết (gồm cả ngày trống) khỏi coverage."""
    if not periods:
        return coverage_df
    return coverage_df.filter(~period_condition(periods))


def known_period_stats(
    coverage_df: DataFrame,
    intraday_df: DataFrame,
    periods: Sequence[KnownPeriod],
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
) -> list[dict[str, Any]]:
    """Mỗi giai đoạn: số ngày kỳ vọng, phân phối nến/ngày, số nến ngoài 09:00–15:00 giờ VN.

    Thêm số nến trước 09:15, trong giờ nghỉ trưa (11:30–13:00), sau 14:45 để so với mẫu HOSE.
    """
    from pyspark.sql import functions as F

    bars = _bars(intraday_df, symbols)
    if start is not None:
        bars = bars.filter(F.col("trade_date") >= _d(start))
    if end is not None:
        bars = bars.filter(F.col("trade_date") <= _d(end))
    m = (((F.col("bar_ts").cast("long") + _VN_OFFSET_SECONDS) % 86400) / 60).cast("int")
    bars = bars.withColumn("m", m)  # phút trong ngày, giờ VN

    def n(cond):
        return F.sum(F.when(cond, 1).otherwise(0))

    out = []
    for p in periods:
        cov = coverage_df.filter(period_condition([p]))
        cnt = (
            bars.filter(period_condition([p]))
            .agg(
                F.count(F.lit(1)).alias("bars"),
                n((F.col("m") < 9 * 60) | (F.col("m") > 15 * 60)).alias("outside"),
                n(F.col("m") < 9 * 60 + 15).alias("pre"),
                n((F.col("m") > 11 * 60 + 30) & (F.col("m") < 13 * 60)).alias("lunch"),
                n(F.col("m") > 14 * 60 + 45).alias("post"),
            )
            .collect()[0]
        )
        out.append(
            {
                "symbol": p.symbol,
                "label": p.label,
                "start": p.start.isoformat() if p.start else None,
                "end": p.end.isoformat() if p.end else None,
                "expected_days": cov.count(),
                "bars_per_day": bars_per_day_distribution(cov),
                "bars": int(cnt["bars"] or 0),
                "bars_outside_0900_1500_vn": int(cnt["outside"] or 0),
                "bars_before_0915_vn": int(cnt["pre"] or 0),
                "bars_lunch_1130_1300_vn": int(cnt["lunch"] or 0),
                "bars_after_1445_vn": int(cnt["post"] or 0),
            }
        )
    return out


# ------------------------------------------------------------ tổng hợp


def symbol_coverage_min(coverage_df: DataFrame) -> tuple[str | None, float | None]:
    """(mã, tỉ lệ ngày có nến / ngày kỳ vọng) của mã thấp nhất; hoà thì lấy mã đứng trước."""
    from pyspark.sql import functions as F

    rows = (
        coverage_df.groupBy("symbol")
        .agg((F.sum(F.col("has_data").cast("int")) / F.count(F.lit(1))).alias("r"))
        .orderBy(F.col("r").asc(), "symbol")
        .limit(1)
        .collect()
    )
    if not rows:
        return None, None
    return rows[0]["symbol"], float(rows[0]["r"])


def summarize_completeness(
    coverage_df: DataFrame,
    min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY,
    outside_df: DataFrame | None = None,
    min_coverage_ratio: float = DEFAULT_MIN_COVERAGE_RATIO,
    run_id: str = "adhoc",
    run_ts: datetime | None = None,
    min_symbol_coverage_ratio: float = DEFAULT_MIN_SYMBOL_COVERAGE_RATIO,
    max_thin_day_share: float = DEFAULT_MAX_THIN_DAY_SHARE,
    suffix: str = "",
) -> list[CheckResult]:
    """Các metric tổng hợp (table_name = intraday_1m); ngưỡng mặc định ở đầu module.

    - intraday_day_coverage_ratio: tỉ lệ (mã, ngày) kỳ vọng có >= 1 nến; đạt khi
      >= `min_coverage_ratio`. Không có cặp nào để kiểm thì metric None, không đạt.
    - intraday_symbol_coverage_ratio_min: tỉ lệ đó của mã thấp nhất (mã nào: xem
      `symbol_coverage_min`); đạt khi >= `min_symbol_coverage_ratio`.
    - intraday_days_missing: số (mã, ngày) kỳ vọng mà không có nến (chỉ đo, luôn đạt).
    - intraday_thin_days: số ngày có dữ liệu nhưng < `min_bars_per_day` nến (chỉ đo).
    - intraday_thin_day_share: thin_days / số (mã, ngày) có dữ liệu; đạt khi
      <= `max_thin_day_share`.
    - intraday_days_outside_calendar (nếu có `outside_df`): số (mã, ngày) có nến ngoài lịch.
    `suffix` nối vào check_name (vd khối tính lại khi bỏ giai đoạn đã biết).
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
    thin_share = thin / with_data if with_data else None
    _, sym_min = symbol_coverage_min(coverage_df)

    def res(name: str, value: float | None, thr: float | None, ok: bool) -> CheckResult:
        return CheckResult(run_id, TABLE_NAME, name + suffix, value, thr, ok, ts)

    results = [
        res(
            "intraday_day_coverage_ratio",
            ratio,
            min_coverage_ratio,
            ratio is not None and ratio >= min_coverage_ratio,
        ),
        res(
            "intraday_symbol_coverage_ratio_min",
            sym_min,
            min_symbol_coverage_ratio,
            sym_min is not None and sym_min >= min_symbol_coverage_ratio,
        ),
        res("intraday_days_missing", float(expected - with_data), None, True),
        res("intraday_thin_days", float(thin), None, True),
        res(
            "intraday_thin_day_share",
            thin_share,
            max_thin_day_share,
            thin_share is not None and thin_share <= max_thin_day_share,
        ),
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
    """min/p5/p50/p95/max của n_bars trên các ngày có dữ liệu."""
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
