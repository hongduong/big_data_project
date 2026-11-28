"""Kiểm tra đầy đủ của nến phút: mỗi (mã, ngày giao dịch) kỳ vọng có nến không.

Lịch giao dịch = các `trade_date` của VNINDEX trong clean daily_ohlcv (do runner dựng).
Ngày kỳ vọng của một mã (từ 28/11/2026) = ngày trong lịch, trong [ngày nến phút đầu, ngày nến
phút cuối] của mã, MÀ nến ngày của chính mã đó có volume >= MIN_DAILY_VOLUME_FOR_EXPECTED_DAY.
Ngày không có dòng nến ngày, volume null hoặc < ngưỡng không phải ngày kỳ vọng.

Hai khái niệm "ít nến" (xem `thin_day_condition`, `low_bar_condition`):
- ngày THƯA (thin, dùng cho kiểm tra): có dữ liệu, < DEFAULT_MIN_BARS_PER_DAY nến VÀ volume phút
  thiếu so với volume ngày > DEFAULT_THIN_MIN_GAP_RATIO * volume ngày VÀ
  >= DEFAULT_THIN_MIN_GAP_SHARES cổ phiếu;
- ngày ÍT NẾN (low bar, quy tắc cũ, chỉ đo): có dữ liệu và < DEFAULT_MIN_BARS_PER_DAY nến.
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
# Ngày kỳ vọng có nến phút: nến ngày của mã có volume >= 100 cổ phiếu (duyệt 28/11/2026).
MIN_DAILY_VOLUME_FOR_EXPECTED_DAY = 100
# Ngưỡng mặc định (duyệt 24/11/2026, đo trên VN30; định nghĩa ngày thưa duyệt 28/11/2026).
# Các hàm nhận tham số để ghi đè.
DEFAULT_MIN_COVERAGE_RATIO = 0.998  # tỉ lệ (mã, ngày) kỳ vọng có >= 1 nến, toàn bộ
DEFAULT_MIN_SYMBOL_COVERAGE_RATIO = 0.98  # cùng tỉ lệ đó, cho từng mã
DEFAULT_MIN_BARS_PER_DAY = 100  # ngày có dữ liệu mà < 100 nến: ít nến (điều kiện 1 của ngày thưa)
DEFAULT_THIN_MIN_GAP_RATIO = 0.01  # ngày thưa: volume ngày - volume phút > 1% volume ngày
DEFAULT_THIN_MIN_GAP_SHARES = 1000  # ... và >= 1000 cổ phiếu
DEFAULT_MAX_THIN_DAY_SHARE = 0.01  # ngày thưa <= 1% số (mã, ngày) kỳ vọng có dữ liệu
OUTSIDE_CALENDAR = "intraday_days_outside_calendar"
AFTER_LAST_BAR = "intraday_days_daily_volume_after_last_bar"
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


def _daily_volume(daily_df: DataFrame, symbols: Sequence[str] | None):
    from pyspark.sql import functions as F

    df = daily_df.select(
        "symbol",
        F.col("trade_date").cast("date").alias("trade_date"),
        F.col("volume").cast("double").alias("daily_volume"),
    )
    if symbols:
        df = df.filter(F.col("symbol").isin(list(symbols)))
    return df


def intraday_per_day(intraday_df: DataFrame, symbols: Sequence[str] | None = None) -> DataFrame:
    """Lần quét DUY NHẤT bảng nến phút mà runner cần: một dòng mỗi (symbol, trade_date) có nến.

    Cột: symbol, trade_date, n_bars, first_bar_ts, last_bar_ts, minute_volume (tổng volume nến
    phút; null nếu mọi volume null). Runner cache kết quả rồi truyền `per_day_df` cho các hàm khác.
    """
    from pyspark.sql import functions as F

    df = intraday_df.select(
        "symbol", "bar_ts", F.col("trade_date").cast("date").alias("trade_date"), "volume"
    )
    if symbols:
        df = df.filter(F.col("symbol").isin(list(symbols)))
    return df.groupBy("symbol", "trade_date").agg(
        F.count(F.lit(1)).cast("long").alias("n_bars"),
        F.min("bar_ts").alias("first_bar_ts"),
        F.max("bar_ts").alias("last_bar_ts"),
        F.sum(F.col("volume").cast("double")).alias("minute_volume"),
    )


def _per_day_or_compute(
    intraday_df: DataFrame, symbols: Sequence[str] | None, per_day_df: DataFrame | None
) -> DataFrame:
    from pyspark.sql import functions as F

    if per_day_df is None:
        return intraday_per_day(intraday_df, symbols)
    if symbols:
        return per_day_df.filter(F.col("symbol").isin(list(symbols)))
    return per_day_df


def intraday_calendar_days(
    intraday_df: DataFrame,
    calendar_df: DataFrame,
    daily_df: DataFrame,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    per_day_df: DataFrame | None = None,
    min_daily_volume: float = MIN_DAILY_VOLUME_FOR_EXPECTED_DAY,
) -> DataFrame:
    """Một dòng cho MỌI ngày trong lịch nằm trong [lo, hi] của từng mã có nến (quy tắc cũ).

    lo = max(start, ngày nến đầu của mã), hi = min(end, ngày nến cuối của mã); mốc đầu/cuối tính
    trên toàn bộ nến của mã, không bị cắt bởi start/end. Cột `expected` đánh dấu ngày kỳ vọng
    theo quy tắc mới (nến ngày của mã có volume >= `min_daily_volume`).
    Cột: symbol, trade_date, n_bars, first_bar_ts, last_bar_ts, has_data (n_bars > 0),
    minute_volume, daily_volume (null nếu không có dòng nến ngày), expected.
    """
    from pyspark.sql import functions as F

    per_day = _per_day_or_compute(intraday_df, symbols, per_day_df)
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
    in_range = ranges.join(
        F.broadcast(cal), (F.col("cal_date") >= F.col("lo")) & (F.col("cal_date") <= F.col("hi"))
    ).select("symbol", F.col("cal_date").alias("trade_date"))
    out = in_range.join(per_day, ["symbol", "trade_date"], "left").join(
        _daily_volume(daily_df, symbols), ["symbol", "trade_date"], "left"
    )
    out = out.withColumn("n_bars", F.coalesce(F.col("n_bars"), F.lit(0)).cast("long"))
    out = out.withColumn("has_data", F.col("n_bars") > 0)
    expected = F.col("daily_volume").isNotNull() & (F.col("daily_volume") >= min_daily_volume)
    out = out.withColumn("expected", F.coalesce(expected, F.lit(False)))
    return out.select(
        "symbol",
        "trade_date",
        "n_bars",
        "first_bar_ts",
        "last_bar_ts",
        "has_data",
        "minute_volume",
        "daily_volume",
        "expected",
    )


def expected_days(calendar_days_df: DataFrame) -> DataFrame:
    """Chỉ giữ ngày kỳ vọng (cột `expected`) của kết quả `intraday_calendar_days`."""
    from pyspark.sql import functions as F

    return calendar_days_df.filter(F.col("expected"))


def intraday_day_coverage(
    intraday_df: DataFrame,
    calendar_df: DataFrame,
    daily_df: DataFrame,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    per_day_df: DataFrame | None = None,
    min_daily_volume: float = MIN_DAILY_VOLUME_FOR_EXPECTED_DAY,
) -> DataFrame:
    """Một dòng cho mỗi (symbol, trade_date) KỲ VỌNG có nến (quy tắc 28/11/2026).

    Kỳ vọng = ngày trong lịch, trong [lo, hi] như `intraday_calendar_days`, MÀ nến ngày của mã
    có volume >= `min_daily_volume`. Mã không có dòng nến ngày nào thì không có ngày kỳ vọng.
    Cùng cột với `intraday_calendar_days` (expected luôn True).
    """
    return expected_days(
        intraday_calendar_days(
            intraday_df,
            calendar_df,
            daily_df,
            symbols,
            start,
            end,
            per_day_df=per_day_df,
            min_daily_volume=min_daily_volume,
        )
    )


def intraday_days_outside_calendar(
    intraday_df: DataFrame,
    calendar_df: DataFrame,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    per_day_df: DataFrame | None = None,
) -> DataFrame:
    """(symbol, trade_date, n_bars) của ngày có nến phút nhưng không có trong lịch.

    Lưu ý: lịch lấy từ VNINDEX daily nên phiên hôm nay (daily chưa về) cũng bị tính ở đây.
    """
    from pyspark.sql import functions as F

    per_day = _per_day_or_compute(intraday_df, symbols, per_day_df).select(
        "symbol", "trade_date", "n_bars"
    )
    if start is not None:
        per_day = per_day.filter(F.col("trade_date") >= _d(start))
    if end is not None:
        per_day = per_day.filter(F.col("trade_date") <= _d(end))
    cal = _calendar(calendar_df)
    return per_day.join(F.broadcast(cal), per_day["trade_date"] == cal["cal_date"], "left_anti")


def days_daily_volume_after_last_bar(
    per_day_df: DataFrame,
    calendar_df: DataFrame,
    daily_df: DataFrame,
    symbols: Sequence[str] | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
    min_daily_volume: float = MIN_DAILY_VOLUME_FOR_EXPECTED_DAY,
) -> DataFrame:
    """(mã, ngày) trong lịch SAU ngày nến phút cuối của mã mà nến ngày có volume >= ngưỡng.

    Phát hiện nguồn ngừng cấp nến phút cho mã vẫn giao dịch (các ngày này nằm ngoài [đầu, cuối]
    nên không là ngày kỳ vọng). Ngày cuối tính trên toàn bộ nến của mã; ngày lịch cắt theo
    start/end. Cột: symbol, trade_date, last_bar_date, daily_volume.
    """
    from pyspark.sql import functions as F

    per_day = per_day_df
    if symbols:
        per_day = per_day.filter(F.col("symbol").isin(list(symbols)))
    last = per_day.groupBy("symbol").agg(F.max("trade_date").alias("last_bar_date"))
    cal = _calendar(calendar_df)
    if start is not None:
        cal = cal.filter(F.col("cal_date") >= _d(start))
    if end is not None:
        cal = cal.filter(F.col("cal_date") <= _d(end))
    after = last.join(F.broadcast(cal), F.col("cal_date") > F.col("last_bar_date")).select(
        "symbol", F.col("cal_date").alias("trade_date"), "last_bar_date"
    )
    out = after.join(_daily_volume(daily_df, symbols), ["symbol", "trade_date"])
    return out.filter(F.col("daily_volume") >= min_daily_volume).select(
        "symbol", "trade_date", "last_bar_date", "daily_volume"
    )


def after_last_bar_by_symbol(after_df: DataFrame) -> DataFrame:
    """Theo mã: last_bar_date, days (số ngày sau ngày nến cuối có volume ngày), first_day,
    last_day; nhiều ngày nhất trước."""
    from pyspark.sql import functions as F

    return (
        after_df.groupBy("symbol")
        .agg(
            F.first("last_bar_date").alias("last_bar_date"),
            F.count(F.lit(1)).alias("days"),
            F.min("trade_date").alias("first_day"),
            F.max("trade_date").alias("last_day"),
        )
        .orderBy(F.col("days").desc(), "symbol")
    )


def summarize_after_last_bar(
    after_df: DataFrame, run_id: str = "adhoc", run_ts: datetime | None = None
) -> CheckResult:
    """intraday_days_daily_volume_after_last_bar: số dòng của `after_df` (chỉ đo, luôn đạt)."""
    ts = run_ts or datetime.now(timezone.utc)
    n = after_df.count()
    return CheckResult(run_id, TABLE_NAME, AFTER_LAST_BAR, float(n), None, True, ts)


# ------------------------------------------------------------ ngày thưa / ít nến


def low_bar_condition(min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY) -> Column:
    """Ngày ÍT NẾN (quy tắc cũ, chỉ đo): có dữ liệu và n_bars < `min_bars_per_day`."""
    from pyspark.sql import functions as F

    return F.col("has_data") & (F.col("n_bars") < min_bars_per_day)


def volume_gap() -> Column:
    """volume ngày - volume phút (volume phút null coi như 0); null khi không có volume ngày."""
    from pyspark.sql import functions as F

    return F.col("daily_volume") - F.coalesce(F.col("minute_volume"), F.lit(0.0))


def thin_day_condition(
    min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY,
    min_gap_ratio: float = DEFAULT_THIN_MIN_GAP_RATIO,
    min_gap_shares: float = DEFAULT_THIN_MIN_GAP_SHARES,
) -> Column:
    """Ngày THƯA (quy tắc 28/11/2026): ít nến VÀ có volume ngày VÀ volume_gap > ratio * volume
    ngày VÀ volume_gap >= `min_gap_shares` cổ phiếu. Cần cột n_bars, has_data, minute_volume,
    daily_volume. Giá trị null (thiếu volume ngày) được coi là không thưa khi dùng trong when()."""
    from pyspark.sql import functions as F

    gap = volume_gap()
    return (
        low_bar_condition(min_bars_per_day)
        & F.col("daily_volume").isNotNull()
        & (gap > F.col("daily_volume") * min_gap_ratio)
        & (gap >= min_gap_shares)
    )


def _count_if(cond: Column):
    from pyspark.sql import functions as F

    return F.sum(F.when(cond, 1).otherwise(0))


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
    calendar_days_df: DataFrame | None = None,
    min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY,
) -> list[dict[str, Any]]:
    """Mỗi giai đoạn: số ngày kỳ vọng, phân phối nến/ngày, số nến ngoài 09:00–15:00 giờ VN.

    `coverage_df` là ngày kỳ vọng (quy tắc mới). Khoá: expected_days (ngày kỳ vọng),
    thin_days (ngày thưa, quy tắc mới), low_bar_days (< `min_bars_per_day` nến, quy tắc cũ),
    calendar_days (mọi ngày lịch trong [đầu, cuối] của mã — quy tắc cũ; chỉ có khi truyền
    `calendar_days_df`), bars_per_day (trên ngày kỳ vọng có dữ liệu). Thêm số nến trước 09:15,
    trong giờ nghỉ trưa (11:30–13:00), sau 14:45 để so với mẫu HOSE.
    """
    from pyspark.sql import functions as F

    bars = _bars(intraday_df, symbols)
    if start is not None:
        bars = bars.filter(F.col("trade_date") >= _d(start))
    if end is not None:
        bars = bars.filter(F.col("trade_date") <= _d(end))
    m = (((F.col("bar_ts").cast("long") + _VN_OFFSET_SECONDS) % 86400) / 60).cast("int")
    bars = bars.withColumn("m", m)  # phút trong ngày, giờ VN

    out = []
    for p in periods:
        cov = coverage_df.filter(period_condition([p]))
        cov_row = cov.agg(
            F.count(F.lit(1)).alias("expected"),
            _count_if(thin_day_condition(min_bars_per_day)).alias("thin"),
            _count_if(low_bar_condition(min_bars_per_day)).alias("low"),
        ).collect()[0]
        cnt = (
            bars.filter(period_condition([p]))
            .agg(
                F.count(F.lit(1)).alias("bars"),
                _count_if((F.col("m") < 9 * 60) | (F.col("m") > 15 * 60)).alias("outside"),
                _count_if(F.col("m") < 9 * 60 + 15).alias("pre"),
                _count_if((F.col("m") > 11 * 60 + 30) & (F.col("m") < 13 * 60)).alias("lunch"),
                _count_if(F.col("m") > 14 * 60 + 45).alias("post"),
            )
            .collect()[0]
        )
        st: dict[str, Any] = {
            "symbol": p.symbol,
            "label": p.label,
            "start": p.start.isoformat() if p.start else None,
            "end": p.end.isoformat() if p.end else None,
            "expected_days": int(cov_row["expected"] or 0),
            "thin_days": int(cov_row["thin"] or 0),
            "low_bar_days": int(cov_row["low"] or 0),
        }
        if calendar_days_df is not None:
            st["calendar_days"] = calendar_days_df.filter(period_condition([p])).count()
        st.update(
            {
                "bars_per_day": bars_per_day_distribution(cov),
                "bars": int(cnt["bars"] or 0),
                "bars_outside_0900_1500_vn": int(cnt["outside"] or 0),
                "bars_before_0915_vn": int(cnt["pre"] or 0),
                "bars_lunch_1130_1300_vn": int(cnt["lunch"] or 0),
                "bars_after_1445_vn": int(cnt["post"] or 0),
            }
        )
        out.append(st)
    return out


# ------------------------------------------------------------ tổng hợp


def symbol_coverage_min(coverage_df: DataFrame) -> tuple[str | None, float | None]:
    """(mã, tỉ lệ ngày có nến / số dòng của mã) của mã thấp nhất; hoà thì lấy mã đứng trước.

    Mỗi dòng của `coverage_df` được coi là một ngày kỳ vọng (truyền ngày kỳ vọng cho quy tắc
    mới, hoặc mọi ngày lịch cho quy tắc cũ).
    """
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
    min_gap_ratio: float = DEFAULT_THIN_MIN_GAP_RATIO,
    min_gap_shares: float = DEFAULT_THIN_MIN_GAP_SHARES,
) -> list[CheckResult]:
    """Các metric tổng hợp (table_name = intraday_1m); ngưỡng mặc định ở đầu module.

    `coverage_df`: ngày kỳ vọng (`intraday_day_coverage`), mỗi dòng một (mã, ngày) kỳ vọng.
    - intraday_day_coverage_ratio: tỉ lệ (mã, ngày) kỳ vọng có >= 1 nến; đạt khi
      >= `min_coverage_ratio`. Không có cặp nào để kiểm thì metric None, không đạt.
    - intraday_symbol_coverage_ratio_min: tỉ lệ đó của mã thấp nhất (mã nào: xem
      `symbol_coverage_min`); đạt khi >= `min_symbol_coverage_ratio`.
    - intraday_days_missing: số (mã, ngày) kỳ vọng mà không có nến (chỉ đo, luôn đạt).
    - intraday_thin_days: số ngày THƯA (`thin_day_condition`, quy tắc mới; chỉ đo).
    - intraday_thin_day_share: ngày thưa / số (mã, ngày) kỳ vọng có dữ liệu; đạt khi
      <= `max_thin_day_share`.
    - intraday_low_bar_days, intraday_low_bar_day_share: như hai dòng trên nhưng theo quy tắc
      cũ (< `min_bars_per_day` nến; `low_bar_condition`); chỉ đo, threshold None, luôn đạt.
    - intraday_days_outside_calendar (nếu có `outside_df`): số (mã, ngày) có nến ngoài lịch.
    `suffix` nối vào check_name (vd khối tính lại khi bỏ giai đoạn đã biết).
    """
    from pyspark.sql import functions as F

    ts = run_ts or datetime.now(timezone.utc)
    row = coverage_df.agg(
        F.count(F.lit(1)).alias("expected"),
        _count_if(F.col("has_data")).alias("with_data"),
        _count_if(thin_day_condition(min_bars_per_day, min_gap_ratio, min_gap_shares)).alias(
            "thin"
        ),
        _count_if(low_bar_condition(min_bars_per_day)).alias("low"),
    ).collect()[0]
    expected, with_data, thin, low = (
        int(row[k] or 0) for k in ("expected", "with_data", "thin", "low")
    )
    ratio = with_data / expected if expected else None
    thin_share = thin / with_data if with_data else None
    low_share = low / with_data if with_data else None
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
        res("intraday_low_bar_days", float(low), None, True),
        res("intraday_low_bar_day_share", low_share, None, True),
    ]
    if outside_df is not None:
        n_out = outside_df.count()
        results.append(res(OUTSIDE_CALENDAR, float(n_out), 0.0, n_out == 0))
    return results


def legacy_completeness(
    calendar_days_df: DataFrame, min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY
) -> dict[str, Any]:
    """Số theo QUY TẮC CŨ (trước 28/11/2026) để so, không ghi dq_report.

    `calendar_days_df`: kết quả `intraday_calendar_days` (mọi ngày lịch trong [đầu, cuối] của
    mã là kỳ vọng). Khoá: day_coverage_ratio, symbol_coverage_ratio_min, symbol_coverage_min_symbol,
    days_expected, days_missing, thin_days và thin_day_share (ngày thưa cũ = < `min_bars_per_day`
    nến, trên số ngày có dữ liệu).
    """
    from pyspark.sql import functions as F

    row = calendar_days_df.agg(
        F.count(F.lit(1)).alias("expected"),
        _count_if(F.col("has_data")).alias("with_data"),
        _count_if(low_bar_condition(min_bars_per_day)).alias("low"),
    ).collect()[0]
    expected, with_data, low = (int(row[k] or 0) for k in ("expected", "with_data", "low"))
    sym, sym_min = symbol_coverage_min(calendar_days_df)
    return {
        "day_coverage_ratio": with_data / expected if expected else None,
        "symbol_coverage_ratio_min": sym_min,
        "symbol_coverage_min_symbol": sym,
        "days_expected": expected,
        "days_missing": expected - with_data,
        "thin_days": low,
        "thin_day_share": low / with_data if with_data else None,
    }


def expected_day_filter_stats(calendar_days_df: DataFrame) -> dict[str, int]:
    """Ngày lịch trong [đầu, cuối] chia theo quy tắc ngày kỳ vọng mới.

    Khoá: calendar_days, expected_days, not_expected_days, not_expected_with_bars (ngày có nến
    phút nhưng không kỳ vọng), not_expected_no_daily_volume (không có dòng nến ngày hoặc volume
    null).
    """
    from pyspark.sql import functions as F

    row = calendar_days_df.agg(
        F.count(F.lit(1)).alias("n"),
        _count_if(F.col("expected")).alias("exp"),
        _count_if(~F.col("expected") & F.col("has_data")).alias("ne_bars"),
        _count_if(~F.col("expected") & F.col("daily_volume").isNull()).alias("ne_nodaily"),
    ).collect()[0]
    n, exp = int(row["n"] or 0), int(row["exp"] or 0)
    return {
        "calendar_days": n,
        "expected_days": exp,
        "not_expected_days": n - exp,
        "not_expected_with_bars": int(row["ne_bars"] or 0),
        "not_expected_no_daily_volume": int(row["ne_nodaily"] or 0),
    }


def completeness_by_symbol(
    coverage_df: DataFrame, min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY
) -> DataFrame:
    """Theo mã (trên ngày kỳ vọng): expected_days, days_with_data, missing_days, thin_days
    (ngày thưa, quy tắc mới), low_bar_days (< `min_bars_per_day` nến, quy tắc cũ),
    coverage_ratio.

    Sắp xếp xấu nhất trước (thiếu nhiều ngày nhất, rồi nhiều ngày thưa nhất).
    """
    from pyspark.sql import functions as F

    out = coverage_df.groupBy("symbol").agg(
        F.count(F.lit(1)).alias("expected_days"),
        _count_if(F.col("has_data")).alias("days_with_data"),
        _count_if(thin_day_condition(min_bars_per_day)).alias("thin_days"),
        _count_if(low_bar_condition(min_bars_per_day)).alias("low_bar_days"),
    )
    out = out.withColumn("missing_days", F.col("expected_days") - F.col("days_with_data"))
    out = out.withColumn("coverage_ratio", F.col("days_with_data") / F.col("expected_days"))
    return out.select(
        "symbol",
        "expected_days",
        "days_with_data",
        "missing_days",
        "thin_days",
        "low_bar_days",
        "coverage_ratio",
    ).orderBy(F.col("missing_days").desc(), F.col("thin_days").desc(), "symbol")


def _with_thin_cols(coverage_df: DataFrame, min_bars_per_day: int) -> DataFrame:
    from pyspark.sql import functions as F

    return coverage_df.withColumn("volume_gap", volume_gap()).withColumn(
        "thin", F.coalesce(thin_day_condition(min_bars_per_day), F.lit(False))
    )


def thinnest_days(
    coverage_df: DataFrame, limit: int = 10, min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY
) -> DataFrame:
    """Các (mã, ngày) có dữ liệu nhưng ít nến nhất (theo n_bars, không theo định nghĩa thưa).

    Thêm cột volume_gap (volume ngày - volume phút) và thin (có phải ngày thưa theo quy tắc
    mới không).
    """
    from pyspark.sql import functions as F

    return (
        _with_thin_cols(coverage_df.filter("has_data"), min_bars_per_day)
        .orderBy(F.col("n_bars").asc(), "symbol", "trade_date")
        .limit(limit)
    )


def top_thin_days(
    coverage_df: DataFrame, limit: int = 10, min_bars_per_day: int = DEFAULT_MIN_BARS_PER_DAY
) -> DataFrame:
    """Các ngày THƯA (quy tắc mới) thiếu volume nhiều nhất (volume_gap giảm dần)."""
    from pyspark.sql import functions as F

    return (
        _with_thin_cols(coverage_df, min_bars_per_day)
        .filter(F.col("thin"))
        .orderBy(F.col("volume_gap").desc(), "symbol", "trade_date")
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
