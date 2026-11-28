import datetime as dt

import pytest

pytestmark = pytest.mark.spark
pytest.importorskip("pyspark")

UTC = dt.timezone.utc
DAYS = [dt.date(2026, 1, d) for d in (5, 6, 7, 8, 9)]  # T2-T6
SAT = dt.date(2026, 1, 10)
BIG = 1_000_000  # volume ngày lớn: ngày < 100 nến luôn là ngày thưa


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("test_dq_intraday")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def bars(symbol, day, n, volume=10, hhmm=(2, 0)):
    # Mặc định nến đầu 02:00 UTC = 09:00 giờ VN.
    base = dt.datetime(day.year, day.month, day.day, *hhmm, tzinfo=UTC)
    return [(symbol, base + dt.timedelta(minutes=i), day, volume) for i in range(n)]


def bars_total(symbol, day, n, total, hhmm=(2, 0)):
    """n nến, tổng volume đúng bằng `total` (nến đầu gánh phần dư)."""
    out = bars(symbol, day, n, volume=1, hhmm=hhmm)
    first = out[0]
    return [(*first[:3], total - (n - 1)), *out[1:]]


def intraday(spark, rows):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("symbol", T.StringType()),
            T.StructField("bar_ts", T.TimestampType()),
            T.StructField("trade_date", T.DateType()),
            T.StructField("volume", T.LongType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def calendar(spark, days=DAYS):
    return spark.createDataFrame([(d,) for d in days], "trade_date date")


def daily(spark, rows):
    return spark.createDataFrame(rows, "symbol string, trade_date date, volume long")


def daily_for(symbols, days=DAYS, volume=BIG):
    return [(s, d, volume) for s in symbols for d in days]


def symbols_of(rows):
    return sorted({r[0] for r in rows})


def cov_df(spark, rows, daily_rows=None, days=DAYS, **kw):
    from stocklake.dq.checks.intraday_completeness import intraday_day_coverage

    if daily_rows is None:
        daily_rows = daily_for(symbols_of(rows), days)
    return intraday_day_coverage(
        intraday(spark, rows), calendar(spark, days), daily(spark, daily_rows), **kw
    )


def coverage(spark, rows, daily_rows=None, **kw):
    out = cov_df(spark, rows, daily_rows, **kw)
    return {(r["symbol"], r["trade_date"]): r for r in out.collect()}


def by_name(results):
    return {r.check_name: r for r in results}


def test_full_coverage(spark):
    rows = [b for d in DAYS for b in bars("A", d, 40)]
    cov = coverage(spark, rows)
    assert len(cov) == 5 and all(r["has_data"] and r["n_bars"] == 40 for r in cov.values())
    assert cov[("A", DAYS[0])]["first_bar_ts"] < cov[("A", DAYS[0])]["last_bar_ts"]


def test_missing_day_is_expected_and_flagged(spark):
    rows = [b for d in DAYS if d != DAYS[2] for b in bars("B", d, 40)]
    cov = coverage(spark, rows)
    assert len(cov) == 5
    assert cov[("B", DAYS[2])]["has_data"] is False and cov[("B", DAYS[2])]["n_bars"] == 0


def test_symbol_not_expected_before_its_first_bar(spark):
    rows = [b for d in DAYS[2:] for b in bars("C", d, 40)]
    cov = coverage(spark, rows)
    assert sorted(d for _, d in cov) == DAYS[2:]


def test_start_end_window(spark):
    rows = [b for d in DAYS for b in bars("A", d, 40)]
    cov = coverage(spark, rows, start=DAYS[1], end="2026-01-08")
    assert sorted(d for _, d in cov) == DAYS[1:4]
    assert len(coverage(spark, rows, symbols=["ZZZ"])) == 0


def test_summary_metrics(spark):
    from stocklake.dq.checks.intraday_completeness import (
        intraday_days_outside_calendar,
        summarize_completeness,
    )

    rows = (
        [b for d in DAYS for b in bars("A", d, 40)]
        + [b for d in DAYS if d != DAYS[2] for b in bars("B", d, 40)]
        + bars("D", DAYS[0], 10)  # ngày mỏng
        + bars("E", SAT, 5)  # ngoài lịch
    )
    df = intraday(spark, rows)
    cov = cov_df(spark, rows)
    outside = intraday_days_outside_calendar(df, calendar(spark))
    res = by_name(summarize_completeness(cov, min_bars_per_day=30, outside_df=outside))
    assert res["intraday_days_missing"].metric_value == 1.0
    assert res["intraday_thin_days"].metric_value == 1.0 and res["intraday_thin_days"].passed
    # 1 ngày thưa (10 nến, thiếu gần hết volume ngày) / 10 (mã, ngày) có dữ liệu
    share = res["intraday_thin_day_share"]
    assert share.metric_value == pytest.approx(0.1) and not share.passed
    assert share.threshold == 0.01
    # quy tắc cũ (< 30 nến) chỉ đo: cùng ngày đó
    assert res["intraday_low_bar_days"].metric_value == 1.0
    low = res["intraday_low_bar_day_share"]
    assert low.metric_value == pytest.approx(0.1) and low.passed and low.threshold is None
    # B: 4/5 là mã thấp nhất
    sym_min = res["intraday_symbol_coverage_ratio_min"]
    assert sym_min.metric_value == pytest.approx(0.8) and not sym_min.passed
    assert res["intraday_days_outside_calendar"].metric_value == 1.0
    assert not res["intraday_days_outside_calendar"].passed
    # A: 5, B: 5 (1 thiếu), D: 1 -> expected 11 (E không có ngày nào trong lịch)
    assert res["intraday_day_coverage_ratio"].metric_value == pytest.approx(10 / 11)
    assert all(r.table_name == "intraday_1m" for r in res.values())


def test_by_symbol_and_distribution(spark):
    from stocklake.dq.checks.intraday_completeness import (
        bars_per_day_distribution,
        completeness_by_symbol,
        thinnest_days,
        top_thin_days,
    )

    rows = [b for d in DAYS if d != DAYS[2] for b in bars("B", d, 40)] + bars("D", DAYS[0], 10)
    cov = cov_df(spark, rows)
    worst = completeness_by_symbol(cov).collect()
    assert worst[0]["symbol"] == "B" and worst[0]["missing_days"] == 1
    # 40 nến < 100 và thiếu gần hết volume ngày -> mọi ngày có dữ liệu đều thưa
    assert worst[0]["thin_days"] == 4 and worst[0]["low_bar_days"] == 4
    thinnest = thinnest_days(cov, 1).collect()[0]
    assert thinnest["symbol"] == "D" and thinnest["thin"] is True
    assert thinnest["volume_gap"] == BIG - 100
    assert top_thin_days(cov, 1).collect()[0]["symbol"] == "D"  # thiếu nhiều cổ phiếu nhất
    dist = bars_per_day_distribution(cov)
    assert dist["min"] == 10 and dist["max"] == 40


def test_empty_input_does_not_crash(spark):
    from stocklake.dq.checks.intraday_completeness import (
        bars_per_day_distribution,
        summarize_completeness,
    )

    cov = cov_df(spark, [], daily_rows=[])
    res = by_name(summarize_completeness(cov))
    assert res["intraday_day_coverage_ratio"].metric_value is None
    assert not res["intraday_day_coverage_ratio"].passed
    assert res["intraday_symbol_coverage_ratio_min"].metric_value is None
    assert res["intraday_thin_day_share"].metric_value is None
    assert not res["intraday_thin_day_share"].passed
    assert res["intraday_low_bar_day_share"].metric_value is None
    assert res["intraday_low_bar_day_share"].passed
    assert bars_per_day_distribution(cov)["p50"] is None


def test_volume_equal_off_by_3pct_and_daily_zero(spark):
    from stocklake.dq.checks.intraday_volume import (
        intraday_vs_daily_volume,
        summarize_volume,
        volume_diff_distribution,
    )

    d = DAYS[0]
    # lệch 3% = 3.000 cổ phiếu (>= 1.000 nên không được coi là khớp nhờ ngưỡng tuyệt đối)
    rows = bars("X", d, 10, volume=10000) + bars("Y", d, 10, volume=10300) + bars("Z", d, 10)
    dl = daily(spark, [("X", d, 100000), ("Y", d, 100000), ("Z", d, 0)])
    diff = intraday_vs_daily_volume(intraday(spark, rows), dl)
    r = {x["symbol"]: x for x in diff.collect()}
    assert r["X"]["rel_diff"] == pytest.approx(0.0)
    assert r["Y"]["rel_diff"] == pytest.approx(0.03) and r["Y"]["n_bars"] == 10
    assert r["Y"]["minute_volume"] == 103000.0 and r["Y"]["daily_volume"] == 100000.0
    assert r["Y"]["volume_diff"] == 3000.0
    assert r["Z"]["rel_diff"] is None
    dist = volume_diff_distribution(diff)
    assert dist["days_total"] == 3 and dist["days_comparable"] == 2 and dist["days_no_daily"] == 1
    assert dist["share_within_1pct"] == 0.5 and dist["share_within_5pct"] == 1.0
    assert dist["days_minute_gt_daily"] == 1 and dist["days_equal"] == 1
    assert dist["max_rel_diff"] == pytest.approx(0.03)
    assert dist["days_match"] == 1 and dist["days_within_tolerance"] == 1
    # distribution phải cùng tolerance: ở đây để hàm tự tính lại với 5%
    res = by_name(summarize_volume(diff, tolerance=0.05, min_match_ratio=0.95))
    assert res["intraday_volume_match_ratio"].metric_value == 1.0
    assert res["intraday_volume_match_ratio"].passed
    tight = by_name(summarize_volume(diff))  # mặc định: 1%, 99%
    assert tight["intraday_volume_match_ratio"].metric_value == 0.5
    assert tight["intraday_volume_match_ratio"].threshold == 0.99
    assert not tight["intraday_volume_match_ratio"].passed


def test_volume_empty_and_no_daily_row(spark):
    from stocklake.dq.checks.intraday_volume import (
        intraday_vs_daily_volume,
        volume_diff_distribution,
    )

    diff = intraday_vs_daily_volume(intraday(spark, bars("Q", DAYS[0], 3)), daily(spark, []))
    dist = volume_diff_distribution(diff)
    assert dist["days_comparable"] == 0 and dist["abs_p50"] is None
    assert dist["share_within_1pct"] is None


# ------------------------------------------------------------ ngưỡng mặc định (duyệt 24/11/2026)

CAL50 = [dt.date(2026, 3, 1) + dt.timedelta(days=i) for i in range(50)]


def test_default_thresholds_values():
    from stocklake.dq.checks import intraday_completeness as comp
    from stocklake.dq.checks import intraday_volume as vol

    assert comp.DEFAULT_MIN_COVERAGE_RATIO == 0.998
    assert comp.DEFAULT_MIN_SYMBOL_COVERAGE_RATIO == 0.98
    assert comp.DEFAULT_MIN_BARS_PER_DAY == 100
    assert comp.DEFAULT_MAX_THIN_DAY_SHARE == 0.01
    assert comp.MIN_DAILY_VOLUME_FOR_EXPECTED_DAY == 100
    assert (comp.DEFAULT_THIN_MIN_GAP_RATIO, comp.DEFAULT_THIN_MIN_GAP_SHARES) == (0.01, 1000)
    assert (vol.DEFAULT_TOLERANCE, vol.DEFAULT_MIN_MATCH_RATIO) == (0.01, 0.99)
    assert vol.DEFAULT_MATCH_ABS_DIFF_SHARES == 1000
    assert vol.DEFAULT_MAX_DAYS_MINUTE_GT_DAILY == 0


def test_symbol_min_and_thin_share_with_defaults(spark):
    from stocklake.dq.checks.intraday_completeness import (
        summarize_completeness,
        symbol_coverage_min,
    )

    a = [b for i, d in enumerate(CAL50) for b in bars("A", d, 99 if i == 10 else 100)]
    b_ = [b for i, d in enumerate(CAL50) if i != 20 for b in bars("B", d, 100)]
    dl = daily_for(["A", "B", "C"], CAL50)
    cov = cov_df(spark, a + b_, dl, days=CAL50)
    res = by_name(summarize_completeness(cov))
    # 99/100 < 0.998; B = 49/50 = 0.98 vừa đạt; 1 ngày mỏng / 99 > 1%
    assert res["intraday_day_coverage_ratio"].metric_value == pytest.approx(0.99)
    assert not res["intraday_day_coverage_ratio"].passed
    assert res["intraday_symbol_coverage_ratio_min"].metric_value == pytest.approx(0.98)
    assert res["intraday_symbol_coverage_ratio_min"].passed
    assert symbol_coverage_min(cov) == ("B", pytest.approx(0.98))
    assert res["intraday_thin_day_share"].metric_value == pytest.approx(1 / 99)
    assert not res["intraday_thin_day_share"].passed
    # Thêm mã C đủ ngày: 1 / 149 <= 1% -> đạt
    c = [b for d in CAL50 for b in bars("C", d, 100)]
    cov = cov_df(spark, a + b_ + c, dl, days=CAL50)
    res = by_name(summarize_completeness(cov))
    assert res["intraday_thin_day_share"].metric_value == pytest.approx(1 / 149)
    assert res["intraday_thin_day_share"].passed
    # B thiếu 2 ngày: 48/50 = 0.96 < 0.98
    b2 = [b for i, d in enumerate(CAL50) if i not in (20, 21) for b in bars("B", d, 100)]
    cov = cov_df(spark, a + b2, dl, days=CAL50)
    sym_min = by_name(summarize_completeness(cov))["intraday_symbol_coverage_ratio_min"]
    assert sym_min.metric_value == pytest.approx(0.96) and not sym_min.passed


def test_minute_gt_daily_is_hard_failure(spark):
    from stocklake.dq.checks.intraday_volume import intraday_vs_daily_volume, summarize_volume

    d = DAYS[0]
    # X khớp; Y +0,5% (trong 1% nhưng phút > ngày); Z -2% = 20.000 cổ phiếu
    rows = (
        bars("X", d, 10, volume=100)
        + bars("Y", d, 10, volume=1005)
        + bars("Z", d, 10, volume=98000)
    )
    dl = daily(spark, [("X", d, 1000), ("Y", d, 10000), ("Z", d, 1000000)])
    res = by_name(summarize_volume(intraday_vs_daily_volume(intraday(spark, rows), dl)))
    gt = res["intraday_volume_days_minute_gt_daily"]
    assert gt.metric_value == 1.0 and gt.threshold == 0.0 and not gt.passed
    assert res["intraday_volume_match_ratio"].metric_value == pytest.approx(2 / 3)
    ok_rows = bars("X", d, 10, volume=100) + bars("Z", d, 10, volume=99000)  # Z đúng -1%
    ok = by_name(summarize_volume(intraday_vs_daily_volume(intraday(spark, ok_rows), dl)))
    assert ok["intraday_volume_days_minute_gt_daily"].passed
    assert ok["intraday_volume_match_ratio"].metric_value == 1.0
    assert ok["intraday_volume_match_ratio"].passed


def _periods():
    from stocklake.dq.notes import KnownPeriod

    return (
        KnownPeriod("P", None, DAYS[1], "truoc_chuyen_san"),
        KnownPeriod("P", DAYS[2], DAYS[3], "khoang_trong_chuyen_san"),
    )


def _period_rows():
    # A đủ 5 ngày; P: 2 ngày đầu mỏng, bắt đầu 08:50 giờ VN; 2 ngày trống; ngày cuối đủ.
    return (
        [b for d in DAYS for b in bars("A", d, 100)]
        + [b for d in DAYS[:2] for b in bars("P", d, 20, hhmm=(1, 50))]
        + bars("P", DAYS[4], 100)
    )


def test_known_period_exclusion_changes_metrics(spark):
    from stocklake.dq.checks.intraday_completeness import (
        bars_per_day_distribution,
        exclude_known_periods,
        summarize_completeness,
    )

    cov = cov_df(spark, _period_rows())
    head = by_name(summarize_completeness(cov))
    assert head["intraday_day_coverage_ratio"].metric_value == pytest.approx(8 / 10)
    assert head["intraday_days_missing"].metric_value == 2.0
    assert head["intraday_thin_day_share"].metric_value == pytest.approx(2 / 8)
    assert head["intraday_low_bar_day_share"].metric_value == pytest.approx(2 / 8)
    assert head["intraday_symbol_coverage_ratio_min"].metric_value == pytest.approx(3 / 5)

    excl = exclude_known_periods(cov, _periods())
    res = by_name(summarize_completeness(excl, suffix="__excl_known_periods"))
    assert set(res) == {n + "__excl_known_periods" for n in head}
    assert res["intraday_day_coverage_ratio__excl_known_periods"].metric_value == 1.0
    assert res["intraday_days_missing__excl_known_periods"].metric_value == 0.0
    assert res["intraday_thin_day_share__excl_known_periods"].metric_value == 0.0
    assert res["intraday_low_bar_days__excl_known_periods"].metric_value == 0.0
    assert res["intraday_symbol_coverage_ratio_min__excl_known_periods"].passed
    assert bars_per_day_distribution(excl)["min"] == 100
    assert exclude_known_periods(cov, ()).count() == cov.count()


def test_known_period_stats(spark):
    from stocklake.dq.checks.intraday_completeness import (
        intraday_calendar_days,
        known_period_stats,
    )

    df = intraday(spark, _period_rows())
    # P: nến ngày đủ trừ ngày trống thứ hai (volume 0) -> 1 ngày kỳ vọng trong khoảng trống
    dl = daily_for(["A"]) + [("P", d, 0 if d == DAYS[3] else BIG) for d in DAYS]
    days = intraday_calendar_days(df, calendar(spark), daily(spark, dl))
    cov = days.filter("expected")
    pre, gap = known_period_stats(cov, df, _periods(), calendar_days_df=days)
    assert (pre["label"], pre["start"], pre["end"]) == ("truoc_chuyen_san", None, "2026-01-06")
    assert pre["expected_days"] == 2 and pre["bars_per_day"]["max"] == 20
    assert pre["thin_days"] == 2 and pre["low_bar_days"] == 2 and pre["calendar_days"] == 2
    assert pre["bars"] == 40
    assert pre["bars_outside_0900_1500_vn"] == 20  # 08:50..08:59 mỗi ngày
    assert pre["bars_before_0915_vn"] == 40
    assert gap["expected_days"] == 1 and gap["calendar_days"] == 2 and gap["bars"] == 0
    assert gap["bars_per_day"]["days_with_data"] == 0


def test_worst_lists_flag_known_period_and_anomaly(spark):
    from stocklake.dq.checks._util import rows_to_dicts
    from stocklake.dq.checks.intraday_completeness import thinnest_days
    from stocklake.dq.checks.intraday_volume import intraday_vs_daily_volume, worst_volume_days
    from stocklake.dq.notes import DqNotes, KnownAnomaly, annotate

    notes = DqNotes(_periods(), (KnownAnomaly(DAYS[4], "ngay_la"),))
    df = intraday(spark, _period_rows())
    cov = cov_df(spark, _period_rows())
    thin = annotate(rows_to_dicts(thinnest_days(cov, 3)), notes)
    assert [(r["symbol"], r["note"]) for r in thin[:2]] == [("P", "truoc_chuyen_san")] * 2
    dl = daily(spark, [("A", DAYS[4], 2000), ("A", DAYS[0], 1000), ("P", DAYS[4], 1000)])
    worst = annotate(rows_to_dicts(worst_volume_days(intraday_vs_daily_volume(df, dl), 5)), notes)
    by_key = {(r["symbol"], r["trade_date"]): r["note"] for r in worst}
    assert by_key[("A", "2026-01-09")] == "ngay_la"
    assert by_key[("A", "2026-01-05")] is None
    assert by_key[("P", "2026-01-09")] == "ngay_la"


# ------------------------------------------------------------ quy tắc 28/11/2026


def test_expected_day_requires_daily_volume_at_least_100(spark):
    from stocklake.dq.checks.intraday_completeness import (
        MIN_DAILY_VOLUME_FOR_EXPECTED_DAY,
        expected_day_filter_stats,
        intraday_calendar_days,
    )

    assert MIN_DAILY_VOLUME_FOR_EXPECTED_DAY == 100
    # A có nến DAYS[0] và DAYS[4] (3 ngày giữa không có nến); N có nến nhưng không có nến ngày.
    rows = (
        bars("A", DAYS[0], 100, volume=10)
        + bars("A", DAYS[4], 100, volume=10)
        + bars("N", DAYS[0], 100)
        + bars("N", DAYS[1], 100)
    )
    dl = [
        ("A", DAYS[0], 1000),
        ("A", DAYS[1], 99),  # < 100: không kỳ vọng
        ("A", DAYS[2], 100),  # đúng 100: kỳ vọng (thiếu nến)
        ("A", DAYS[3], None),  # volume null: không kỳ vọng
        # DAYS[4]: không có dòng nến ngày -> không kỳ vọng dù có nến phút
    ]
    cov = coverage(spark, rows, dl)
    assert sorted(cov) == [("A", DAYS[0]), ("A", DAYS[2])]
    assert cov[("A", DAYS[2])]["has_data"] is False
    assert not any(sym == "N" for sym, _ in cov)  # mã không có dòng nến ngày nào
    days = intraday_calendar_days(
        intraday(spark, rows), calendar(spark), daily(spark, dl)
    ).collect()
    assert len(days) == 5 + 2  # quy tắc cũ: mọi ngày lịch trong [đầu, cuối]
    stats = expected_day_filter_stats(
        intraday_calendar_days(intraday(spark, rows), calendar(spark), daily(spark, dl))
    )
    assert stats == {
        "calendar_days": 7,
        "expected_days": 2,
        "not_expected_days": 5,
        "not_expected_with_bars": 3,  # A DAYS[4], N DAYS[0], N DAYS[1]
        "not_expected_no_daily_volume": 4,  # A DAYS[3] (null), A DAYS[4], N x2
    }


def test_coverage_from_cached_per_day_matches_direct(spark):
    from stocklake.dq.checks.intraday_completeness import intraday_day_coverage, intraday_per_day

    rows = _period_rows()
    df, cal, dl = intraday(spark, rows), calendar(spark), daily(spark, daily_for(["A", "P"]))
    per_day = intraday_per_day(df)
    direct = intraday_day_coverage(df, cal, dl).collect()
    via = intraday_day_coverage(df, cal, dl, per_day_df=per_day).collect()
    assert sorted(direct) == sorted(via)
    only_p = intraday_day_coverage(df, cal, dl, symbols=["P"], per_day_df=per_day).collect()
    assert {r["symbol"] for r in only_p} == {"P"}


def test_thin_day_needs_volume_gap_over_1pct_and_1000_shares(spark):
    from stocklake.dq.checks.intraday_completeness import (
        completeness_by_symbol,
        summarize_completeness,
    )

    rows = (
        bars_total("T", DAYS[0], 50, 49_001)  # thiếu 999 (2%): không thưa
        + bars_total("T", DAYS[1], 50, 49_000)  # thiếu 1.000 (2%): THƯA
        + bars_total("T", DAYS[2], 50, 99_000)  # thiếu 1.000 = đúng 1%: không thưa
        + bars_total("T", DAYS[3], 50, 98_999)  # thiếu 1.001 (> 1%): THƯA
        + bars_total("T", DAYS[4], 100, 100)  # 100 nến: không ít nến dù thiếu gần hết
    )
    dl = [
        ("T", DAYS[0], 50_000),
        ("T", DAYS[1], 50_000),
        ("T", DAYS[2], 100_000),
        ("T", DAYS[3], 100_000),
        ("T", DAYS[4], 100_000),
    ]
    cov = cov_df(spark, rows, dl)
    res = by_name(summarize_completeness(cov))
    assert res["intraday_thin_days"].metric_value == 2.0
    assert res["intraday_thin_day_share"].metric_value == pytest.approx(2 / 5)
    assert not res["intraday_thin_day_share"].passed
    assert res["intraday_low_bar_days"].metric_value == 4.0
    assert res["intraday_low_bar_day_share"].metric_value == pytest.approx(4 / 5)
    (row,) = completeness_by_symbol(cov).collect()
    assert (row["thin_days"], row["low_bar_days"]) == (2, 4)
    # ngưỡng tuyệt đối qua tham số
    loose = by_name(summarize_completeness(cov, min_gap_shares=999))
    assert loose["intraday_thin_days"].metric_value == 3.0


def test_thin_day_with_matching_volume_is_not_thin(spark):
    from stocklake.dq.checks.intraday_completeness import summarize_completeness

    # mã ít giao dịch: 30 nến/ngày nhưng volume phút chỉ thiếu lô lẻ
    rows = [b for d in DAYS for b in bars_total("L", d, 30, 20_000)]
    cov = cov_df(spark, rows, daily_for(["L"], volume=20_060))
    res = by_name(summarize_completeness(cov))
    assert res["intraday_thin_days"].metric_value == 0.0
    assert res["intraday_thin_day_share"].passed
    assert res["intraday_low_bar_days"].metric_value == 5.0


def test_days_daily_volume_after_last_bar(spark):
    from stocklake.dq.checks.intraday_completeness import (
        AFTER_LAST_BAR,
        after_last_bar_by_symbol,
        days_daily_volume_after_last_bar,
        intraday_per_day,
        summarize_after_last_bar,
    )

    rows = [b for d in DAYS[:2] for b in bars("S", d, 100)] + [
        b for d in DAYS for b in bars("F", d, 100)
    ]
    dl = (
        daily_for(["F"])
        + [("S", d, BIG) for d in DAYS[:3]]  # DAYS[2]: sau nến cuối, có volume
        + [("S", DAYS[3], 99), ("S", DAYS[4], 100)]  # 99: không tính; 100: tính
        + [("X", d, BIG) for d in DAYS]  # mã không có nến phút nào: không tính
    )
    per_day = intraday_per_day(intraday(spark, rows))
    after = days_daily_volume_after_last_bar(per_day, calendar(spark), daily(spark, dl))
    got = sorted((r["symbol"], r["trade_date"], r["last_bar_date"]) for r in after.collect())
    assert got == [("S", DAYS[2], DAYS[1]), ("S", DAYS[4], DAYS[1])]
    res = summarize_after_last_bar(after)
    assert res.check_name == AFTER_LAST_BAR == "intraday_days_daily_volume_after_last_bar"
    assert res.metric_value == 2.0 and res.threshold is None and res.passed
    (top,) = after_last_bar_by_symbol(after).collect()
    assert (top["symbol"], top["days"], top["first_day"]) == ("S", 2, DAYS[2])
    cut = days_daily_volume_after_last_bar(per_day, calendar(spark), daily(spark, dl), end=DAYS[3])
    assert cut.count() == 1
    # ngày sau nến cuối không phải ngày kỳ vọng (độ đủ không đổi)
    assert sorted(d for s, d in coverage(spark, rows, dl) if s == "S") == DAYS[:2]


def test_legacy_completeness_uses_all_calendar_days(spark):
    from stocklake.dq.checks.intraday_completeness import (
        intraday_calendar_days,
        legacy_completeness,
    )

    rows = bars("A", DAYS[0], 100) + bars("A", DAYS[4], 40)
    dl = [("A", DAYS[0], BIG), ("A", DAYS[4], BIG)]  # 3 ngày giữa: không có nến ngày
    days = intraday_calendar_days(intraday(spark, rows), calendar(spark), daily(spark, dl))
    leg = legacy_completeness(days)
    assert leg["days_expected"] == 5 and leg["days_missing"] == 3
    assert leg["day_coverage_ratio"] == pytest.approx(2 / 5)
    assert leg["symbol_coverage_min_symbol"] == "A"
    assert leg["thin_days"] == 1 and leg["thin_day_share"] == pytest.approx(1 / 2)


def test_volume_match_absolute_threshold_and_effective_p95(spark):
    from stocklake.dq.checks.intraday_volume import (
        intraday_vs_daily_volume,
        legacy_volume,
        summarize_volume,
        volume_diff_distribution,
    )

    d = DAYS[0]
    rows = (
        bars_total("M", d, 10, 19_001)  # thiếu 999 (~5%): khớp nhờ ngưỡng tuyệt đối
        + bars_total("N", d, 10, 19_000)  # thiếu 1.000 (5%): không khớp
        + bars_total("O", d, 10, 20_999)  # thừa 999: khớp nhưng phút > ngày
        + bars_total("Q", d, 10, 99_000)  # thiếu 1.000 = đúng 1%: khớp theo tỉ lệ
    )
    dl = daily(spark, [("M", d, 20_000), ("N", d, 20_000), ("O", d, 20_000), ("Q", d, 100_000)])
    diff = intraday_vs_daily_volume(intraday(spark, rows), dl)
    dist = volume_diff_distribution(diff)
    assert dist["days_comparable"] == 4
    assert dist["days_match"] == 3 and dist["days_within_tolerance"] == 1
    assert dist["effective_abs_p99"] == pytest.approx(0.05)
    res = by_name(summarize_volume(diff, distribution=dist))
    assert res["intraday_volume_match_ratio"].metric_value == pytest.approx(3 / 4)
    assert res["intraday_volume_days_minute_gt_daily"].metric_value == 1.0
    p95 = res["intraday_volume_abs_diff_p95"]
    assert p95.metric_value == pytest.approx(0.05) and p95.threshold == 0.01 and not p95.passed
    raw = res["intraday_volume_abs_diff_p95_raw"]
    assert raw.metric_value == pytest.approx(0.05) and raw.passed and raw.threshold is None
    leg = legacy_volume(dist)
    assert leg["volume_match_ratio"] == pytest.approx(1 / 4)
    assert leg["volume_days_minute_gt_daily"] == 1
    # ngày lệch dưới 1.000 cổ phiếu có độ lệch hiệu dụng 0 -> p95 đạt dù |rel_diff| thô 5%
    ok = bars_total("M", d, 10, 19_001) + bars_total("Q", d, 10, 99_000)
    ok_res = by_name(summarize_volume(intraday_vs_daily_volume(intraday(spark, ok), dl)))
    assert ok_res["intraday_volume_abs_diff_p95"].metric_value == pytest.approx(0.01)
    assert ok_res["intraday_volume_abs_diff_p95"].passed
    assert ok_res["intraday_volume_abs_diff_p95_raw"].metric_value == pytest.approx(999 / 20_000)


def test_minute_gt_daily_excluding_marked_anomaly_days(spark):
    from stocklake.dq.checks.intraday_volume import (
        MINUTE_GT_DAILY,
        exclude_anomaly_days,
        intraday_vs_daily_volume,
        minute_gt_daily_days,
        summarize_minute_gt_daily,
    )
    from stocklake.dq.notes import KnownAnomaly

    rows = [b for d in DAYS[:2] for b in bars_total("G", d, 10, 3_700)] + bars_total(
        "H", DAYS[0], 10, 3_700
    )
    dl = daily(spark, [("G", DAYS[0], 3_500), ("G", DAYS[1], 3_500), ("H", DAYS[0], 3_500)])
    diff = intraday_vs_daily_volume(intraday(spark, rows), dl)
    assert summarize_minute_gt_daily(diff).metric_value == 3.0
    day0 = KnownAnomaly(DAYS[0], "nen_thua", exclude_from=("volume_minute_gt_daily",))
    left = exclude_anomaly_days(diff, [day0])
    res = summarize_minute_gt_daily(left)
    assert res.check_name == MINUTE_GT_DAILY and res.metric_value == 1.0 and not res.passed
    both = exclude_anomaly_days(diff, [day0, KnownAnomaly(DAYS[1], "x", symbols=("G",))])
    assert summarize_minute_gt_daily(both).passed
    only_h = exclude_anomaly_days(diff, [KnownAnomaly(DAYS[0], "y", symbols=("H",))])
    assert summarize_minute_gt_daily(only_h).metric_value == 2.0
    assert exclude_anomaly_days(diff, []).count() == 3
    top = minute_gt_daily_days(diff, 1).collect()[0]
    assert top["volume_diff"] == 200.0


# ------------------------------------------------------------ runner: kết luận + khoá đọc


def _run_runner(spark, monkeypatch, notes, rows=None, daily_rows=None):
    import json

    pytest.importorskip("stocklake.common.locks", reason="module khoá chưa có")
    from stocklake.common.locks import MemoryLockFs
    from stocklake.dq import intraday as dq

    ivs = intraday(spark, _period_rows() if rows is None else rows)
    if daily_rows is None:
        # A: 100 nến x 10 = 1.000 = volume ngày; P: volume ngày lớn (ngày ít nến là ngày thưa)
        daily_rows = [("A", d, 1000) for d in DAYS] + daily_for(["P"])
    dvs = daily(spark, [("VNINDEX", d, 1) for d in DAYS] + daily_rows)
    tables = {"intraday_1m": ivs, "daily_ohlcv": dvs}
    monkeypatch.setattr(dq, "_read_clean", lambda _spark, table, cols: tables[table].select(*cols))
    report = dq.run_intraday_dq(spark, write=False, notes=notes, lock_fs=MemoryLockFs())
    json.dumps(report)  # phải serialise được
    return report


def test_runner_verdict_excludes_known_periods(spark, monkeypatch):
    from stocklake.dq.notes import DqNotes

    rep = _run_runner(spark, monkeypatch, DqNotes(_periods(), ()))
    checks = {c["check_name"]: c for c in rep["results"]}
    cov = checks["intraday_day_coverage_ratio"]
    assert cov["basis"] == "excluding_known_periods" and cov["passed"] is True
    assert cov["metric"] == 1.0
    assert cov["reference_all_data"]["metric"] == pytest.approx(8 / 10)
    assert cov["reference_all_data"]["passed"] is False
    assert checks["intraday_symbol_coverage_ratio_min"]["reference_all_data"]["symbol"] == "P"
    assert checks["intraday_volume_match_ratio"]["basis"] == "all_data"
    assert "reference_all_data" not in checks["intraday_volume_match_ratio"]
    assert rep["known_periods_excluded"] == 2 and rep["verdict_rule"]
    assert isinstance(rep["passed"], bool) and rep["lock_waited_s"] == 0.0
    assert rep["rows_written"] == 0


def test_runner_without_known_periods_reports_same_numbers(spark, monkeypatch):
    from stocklake.dq.notes import DqNotes

    rep = _run_runner(spark, monkeypatch, DqNotes())
    cov = next(c for c in rep["results"] if c["check_name"] == "intraday_day_coverage_ratio")
    assert cov["basis"] == "excluding_known_periods" and rep["known_periods_excluded"] == 0
    assert cov["metric"] == pytest.approx(8 / 10)
    assert cov["reference_all_data"]["metric"] == cov["metric"]
    assert cov["passed"] is False and rep["passed"] is False


def test_runner_waits_for_executors_before_taking_read_locks(spark, monkeypatch):
    """Có executor rồi mới xin khoá đọc (tránh chờ vòng khoá <-> core với job clean)."""
    pytest.importorskip("stocklake.common.locks", reason="module khoá chưa có")
    from stocklake.common import locks
    from stocklake.dq import intraday as dq
    from stocklake.dq.notes import DqNotes

    events = []
    real_wait, real_read_lock = dq.wait_for_executors, locks.read_lock

    def wait(spark_):
        events.append("executors")
        real_wait(spark_)

    def read_lock(fs, name, **kw):
        events.append(name)
        return real_read_lock(fs, name, **kw)

    monkeypatch.setattr(dq, "wait_for_executors", wait)
    monkeypatch.setattr(locks, "read_lock", read_lock)
    _run_runner(spark, monkeypatch, DqNotes())
    assert events == ["executors", dq.LOCK_INTRADAY, dq.LOCK_DAILY]


def _gt_rows():
    # A: 100 nến x 10 = 1.000; volume ngày 900 ở DAYS[0] (ngày bị loại) và DAYS[1] (tuỳ test)
    return [b for d in DAYS for b in bars("A", d, 100)]


def _gt_daily(day1_volume):
    return [("A", DAYS[0], 900), ("A", DAYS[1], day1_volume)] + [("A", d, 1000) for d in DAYS[2:]]


def test_runner_minute_gt_daily_verdict_excludes_marked_days(spark, monkeypatch):
    from stocklake.dq.notes import DqNotes, KnownAnomaly

    marked = KnownAnomaly(DAYS[0], "nen_thua", exclude_from=("volume_minute_gt_daily",))
    plain = KnownAnomaly(DAYS[1], "ghi_chu_thuong")  # không exclude_from: không bị loại
    rep = _run_runner(spark, monkeypatch, DqNotes((), (marked, plain)), _gt_rows(), _gt_daily(1000))
    checks = {c["check_name"]: c for c in rep["results"]}
    gt = checks["intraday_volume_days_minute_gt_daily"]
    assert gt["basis"] == "excluding_known_anomalies" and gt["group"] == "cross_source"
    assert gt["metric"] == 0.0 and gt["passed"] is True
    assert gt["reference_all_data"] == {"metric": 1.0, "passed": False}
    assert rep["volume_minute_gt_daily_excluded"]["anomalies"] == [
        {"date": "2026-01-05", "symbols": [], "label": "nen_thua"}
    ]
    assert rep["notes"]["known_anomalies"][0]["exclude_from"] == ["volume_minute_gt_daily"]
    assert rep["notes"]["known_anomalies"][1]["exclude_from"] == []
    assert [r["note"] for r in rep["minute_gt_daily_days"]] == ["nen_thua"]
    assert rep["legacy_rules"]["volume_days_minute_gt_daily"] == 1
    assert rep["passed"] is True

    # DAYS[1] cũng phút > ngày nhưng chỉ có ghi chú thường -> kết luận không đạt
    rep = _run_runner(spark, monkeypatch, DqNotes((), (marked, plain)), _gt_rows(), _gt_daily(900))
    gt = next(
        c for c in rep["results"] if c["check_name"] == "intraday_volume_days_minute_gt_daily"
    )
    assert gt["metric"] == 1.0 and gt["passed"] is False
    assert gt["reference_all_data"]["metric"] == 2.0
    assert rep["passed"] is False


def test_runner_report_has_new_and_legacy_blocks(spark, monkeypatch):
    from stocklake.dq.notes import DqNotes

    rep = _run_runner(spark, monkeypatch, DqNotes(_periods(), ()))
    names = {c["check_name"] for c in rep["results"]}
    assert {
        "intraday_low_bar_days",
        "intraday_low_bar_day_share",
        "intraday_days_daily_volume_after_last_bar",
        "intraday_volume_abs_diff_p95_raw",
    } <= names
    after = next(
        c for c in rep["results"] if c["check_name"] == "intraday_days_daily_volume_after_last_bar"
    )
    assert after["basis"] == "all_data" and after["threshold"] is None and after["passed"]
    th = rep["thresholds"]
    assert th["min_daily_volume_expected_day"] == 100
    assert (th["thin_min_gap_ratio"], th["thin_min_gap_shares"]) == (0.01, 1000)
    assert th["volume_match_abs_diff_shares"] == 1000
    leg = rep["legacy_rules"]
    assert leg["basis_completeness"] == "excluding_known_periods"
    assert leg["day_coverage_ratio"] == 1.0 and leg["thin_day_share"] == 0.0
    assert {"days_missing", "symbol_coverage_ratio_min", "volume_match_ratio"} <= set(leg)
    assert rep["expected_day_filter"]["expected_days"] == 10
    assert rep["days_daily_volume_after_last_bar_by_symbol"] == []
    pre = rep["known_period_stats"][0]
    assert pre["thin_days"] == 2 and pre["calendar_days"] == 2
