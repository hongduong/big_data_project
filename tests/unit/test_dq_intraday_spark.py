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


def bars(symbol, day, n, volume=10, hhmm=(2, 0)):
    # Mặc định nến đầu 02:00 UTC = 09:00 giờ VN.
    base = dt.datetime(day.year, day.month, day.day, *hhmm, tzinfo=UTC)
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
    assert res["intraday_thin_days"].metric_value == 1.0 and res["intraday_thin_days"].passed
    # 1 ngày mỏng / 10 (mã, ngày) có dữ liệu
    share = res["intraday_thin_day_share"]
    assert share.metric_value == pytest.approx(0.1) and not share.passed
    assert share.threshold == 0.01
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
    assert res["intraday_symbol_coverage_ratio_min"].metric_value is None
    assert res["intraday_thin_day_share"].metric_value is None
    assert not res["intraday_thin_day_share"].passed
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
    res = by_name(summarize_volume(diff, tolerance=0.05, min_match_ratio=0.95, distribution=dist))
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
    assert (vol.DEFAULT_TOLERANCE, vol.DEFAULT_MIN_MATCH_RATIO) == (0.01, 0.99)
    assert vol.DEFAULT_MAX_DAYS_MINUTE_GT_DAILY == 0


def test_symbol_min_and_thin_share_with_defaults(spark):
    from stocklake.dq.checks.intraday_completeness import (
        intraday_day_coverage,
        summarize_completeness,
        symbol_coverage_min,
    )

    a = [b for i, d in enumerate(CAL50) for b in bars("A", d, 99 if i == 10 else 100)]
    b_ = [b for i, d in enumerate(CAL50) if i != 20 for b in bars("B", d, 100)]
    cal = calendar(spark, CAL50)
    cov = intraday_day_coverage(intraday(spark, a + b_), cal)
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
    cov = intraday_day_coverage(intraday(spark, a + b_ + c), cal)
    res = by_name(summarize_completeness(cov))
    assert res["intraday_thin_day_share"].metric_value == pytest.approx(1 / 149)
    assert res["intraday_thin_day_share"].passed
    # B thiếu 2 ngày: 48/50 = 0.96 < 0.98
    b2 = [b for i, d in enumerate(CAL50) if i not in (20, 21) for b in bars("B", d, 100)]
    cov = intraday_day_coverage(intraday(spark, a + b2), cal)
    sym_min = by_name(summarize_completeness(cov))["intraday_symbol_coverage_ratio_min"]
    assert sym_min.metric_value == pytest.approx(0.96) and not sym_min.passed


def test_minute_gt_daily_is_hard_failure(spark):
    from stocklake.dq.checks.intraday_volume import intraday_vs_daily_volume, summarize_volume

    d = DAYS[0]
    # X khớp; Y +0,5% (trong 1% nhưng phút > ngày); Z -2%
    rows = bars("X", d, 10, volume=100) + bars("Y", d, 10, volume=1005) + bars("Z", d, 10, 98)
    dl = daily(spark, [("X", d, 1000), ("Y", d, 10000), ("Z", d, 1000)])
    res = by_name(summarize_volume(intraday_vs_daily_volume(intraday(spark, rows), dl)))
    gt = res["intraday_volume_days_minute_gt_daily"]
    assert gt.metric_value == 1.0 and gt.threshold == 0.0 and not gt.passed
    assert res["intraday_volume_match_ratio"].metric_value == pytest.approx(2 / 3)
    ok_rows = bars("X", d, 10, volume=100) + bars("Z", d, 10, volume=99)
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
        intraday_day_coverage,
        summarize_completeness,
    )

    cov = intraday_day_coverage(intraday(spark, _period_rows()), calendar(spark))
    head = by_name(summarize_completeness(cov))
    assert head["intraday_day_coverage_ratio"].metric_value == pytest.approx(8 / 10)
    assert head["intraday_days_missing"].metric_value == 2.0
    assert head["intraday_thin_day_share"].metric_value == pytest.approx(2 / 8)
    assert head["intraday_symbol_coverage_ratio_min"].metric_value == pytest.approx(3 / 5)

    excl = exclude_known_periods(cov, _periods())
    res = by_name(summarize_completeness(excl, suffix="__excl_known_periods"))
    assert set(res) == {n + "__excl_known_periods" for n in head}
    assert res["intraday_day_coverage_ratio__excl_known_periods"].metric_value == 1.0
    assert res["intraday_days_missing__excl_known_periods"].metric_value == 0.0
    assert res["intraday_thin_day_share__excl_known_periods"].metric_value == 0.0
    assert res["intraday_symbol_coverage_ratio_min__excl_known_periods"].passed
    assert bars_per_day_distribution(excl)["min"] == 100
    assert exclude_known_periods(cov, ()).count() == cov.count()


def test_known_period_stats(spark):
    from stocklake.dq.checks.intraday_completeness import (
        intraday_day_coverage,
        known_period_stats,
    )

    df = intraday(spark, _period_rows())
    cov = intraday_day_coverage(df, calendar(spark))
    pre, gap = known_period_stats(cov, df, _periods())
    assert (pre["label"], pre["start"], pre["end"]) == ("truoc_chuyen_san", None, "2026-01-06")
    assert pre["expected_days"] == 2 and pre["bars_per_day"]["max"] == 20
    assert pre["bars"] == 40
    assert pre["bars_outside_0900_1500_vn"] == 20  # 08:50..08:59 mỗi ngày
    assert pre["bars_before_0915_vn"] == 40
    assert gap["expected_days"] == 2 and gap["bars"] == 0
    assert gap["bars_per_day"]["days_with_data"] == 0


def test_worst_lists_flag_known_period_and_anomaly(spark):
    from stocklake.dq.checks._util import rows_to_dicts
    from stocklake.dq.checks.intraday_completeness import intraday_day_coverage, thinnest_days
    from stocklake.dq.checks.intraday_volume import intraday_vs_daily_volume, worst_volume_days
    from stocklake.dq.notes import DqNotes, KnownAnomaly, annotate

    notes = DqNotes(_periods(), (KnownAnomaly(DAYS[4], "ngay_la"),))
    df = intraday(spark, _period_rows())
    cov = intraday_day_coverage(df, calendar(spark))
    thin = annotate(rows_to_dicts(thinnest_days(cov, 3)), notes)
    assert [(r["symbol"], r["note"]) for r in thin[:2]] == [("P", "truoc_chuyen_san")] * 2
    dl = daily(spark, [("A", DAYS[4], 2000), ("A", DAYS[0], 1000), ("P", DAYS[4], 1000)])
    worst = annotate(rows_to_dicts(worst_volume_days(intraday_vs_daily_volume(df, dl), 5)), notes)
    by_key = {(r["symbol"], r["trade_date"]): r["note"] for r in worst}
    assert by_key[("A", "2026-01-09")] == "ngay_la"
    assert by_key[("A", "2026-01-05")] is None
    assert by_key[("P", "2026-01-09")] == "ngay_la"


# ------------------------------------------------------------ runner: kết luận + khoá đọc


def _run_runner(spark, monkeypatch, notes):
    import json

    pytest.importorskip("stocklake.common.locks", reason="module khoá chưa có")
    from stocklake.common.locks import MemoryLockFs
    from stocklake.dq import intraday as dq

    ivs = intraday(spark, _period_rows())
    dvs = daily(
        spark,
        [("VNINDEX", d, 1) for d in DAYS] + [("A", d, 1000) for d in DAYS],
    )
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
