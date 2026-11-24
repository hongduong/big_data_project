import datetime as dt

import pytest

pytestmark = pytest.mark.spark
pytest.importorskip("pyspark")

UTC = dt.timezone.utc
DAYS = [dt.date(2026, 1, d) for d in (5, 6, 7, 8, 9)]  # T2-T6
SAT = dt.date(2026, 1, 10)


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


def bars(symbol, day, n, volume=10):
    base = dt.datetime(day.year, day.month, day.day, 2, 0, tzinfo=UTC)
    return [(symbol, base + dt.timedelta(minutes=i), day, volume) for i in range(n)]


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


def coverage(spark, rows, **kw):
    from stocklake.dq.checks.intraday_completeness import intraday_day_coverage

    out = intraday_day_coverage(intraday(spark, rows), calendar(spark), **kw)
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
    from stocklake.dq.checks.intraday_completeness import intraday_day_coverage

    cov = intraday_day_coverage(df, calendar(spark))
    outside = intraday_days_outside_calendar(df, calendar(spark))
    res = by_name(summarize_completeness(cov, min_bars_per_day=30, outside_df=outside))
    assert res["intraday_days_missing"].metric_value == 1.0
    assert res["intraday_thin_days"].metric_value == 1.0 and not res["intraday_thin_days"].passed
    assert res["intraday_days_outside_calendar"].metric_value == 1.0
    assert not res["intraday_days_outside_calendar"].passed
    # A: 5, B: 5 (1 thiếu), D: 1 -> expected 11 (E không có ngày nào trong lịch)
    assert res["intraday_day_coverage_ratio"].metric_value == pytest.approx(10 / 11)
    assert all(r.table_name == "intraday_1m" for r in res.values())


def test_by_symbol_and_distribution(spark):
    from stocklake.dq.checks.intraday_completeness import (
        bars_per_day_distribution,
        completeness_by_symbol,
        intraday_day_coverage,
        thinnest_days,
    )

    rows = [b for d in DAYS if d != DAYS[2] for b in bars("B", d, 40)] + bars("D", DAYS[0], 10)
    cov = intraday_day_coverage(intraday(spark, rows), calendar(spark))
    worst = completeness_by_symbol(cov).collect()
    assert worst[0]["symbol"] == "B" and worst[0]["missing_days"] == 1
    assert thinnest_days(cov, 1).collect()[0]["symbol"] == "D"
    dist = bars_per_day_distribution(cov)
    assert dist["min"] == 10 and dist["max"] == 40


def test_empty_input_does_not_crash(spark):
    from stocklake.dq.checks.intraday_completeness import (
        bars_per_day_distribution,
        intraday_day_coverage,
        summarize_completeness,
    )

    cov = intraday_day_coverage(intraday(spark, []), calendar(spark))
    res = by_name(summarize_completeness(cov))
    assert res["intraday_day_coverage_ratio"].metric_value is None
    assert not res["intraday_day_coverage_ratio"].passed
    assert bars_per_day_distribution(cov)["p50"] is None


def test_volume_equal_off_by_3pct_and_daily_zero(spark):
    from stocklake.dq.checks.intraday_volume import (
        intraday_vs_daily_volume,
        summarize_volume,
        volume_diff_distribution,
    )

    d = DAYS[0]
    rows = bars("X", d, 10, volume=100) + bars("Y", d, 10, volume=103) + bars("Z", d, 10)
    dl = daily(spark, [("X", d, 1000), ("Y", d, 1000), ("Z", d, 0)])
    diff = intraday_vs_daily_volume(intraday(spark, rows), dl)
    r = {x["symbol"]: x for x in diff.collect()}
    assert r["X"]["rel_diff"] == pytest.approx(0.0)
    assert r["Y"]["rel_diff"] == pytest.approx(0.03) and r["Y"]["n_bars"] == 10
    assert r["Y"]["minute_volume"] == 1030.0 and r["Y"]["daily_volume"] == 1000.0
    assert r["Z"]["rel_diff"] is None
    dist = volume_diff_distribution(diff)
    assert dist["days_total"] == 3 and dist["days_comparable"] == 2 and dist["days_no_daily"] == 1
    assert dist["share_within_1pct"] == 0.5 and dist["share_within_5pct"] == 1.0
    assert dist["days_minute_gt_daily"] == 1 and dist["days_equal"] == 1
    assert dist["max_rel_diff"] == pytest.approx(0.03)
    res = by_name(summarize_volume(diff, tolerance=0.05, distribution=dist))
    assert res["intraday_volume_match_ratio"].metric_value == 1.0
    assert res["intraday_volume_match_ratio"].passed
    tight = by_name(summarize_volume(diff, tolerance=0.01))
    assert tight["intraday_volume_match_ratio"].metric_value == 0.5
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
