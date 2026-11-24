"""Quy tắc kết luận DQ (24/11/2026) và khoá đọc: logic thuần, không cần Spark."""

import contextlib
import datetime as dt
import json

import pytest

from stocklake.dq.checks import CheckResult
from stocklake.dq.verdict import (
    ALL_DATA_SUFFIX,
    BASIS_ALL,
    BASIS_EXCL,
    VERDICT_RULE,
    build_verdict,
)

TS = dt.datetime(2026, 11, 24, tzinfo=dt.timezone.utc)


def r(name, value, thr, ok):
    return CheckResult("run-1", "intraday_1m", name, value, thr, ok, TS)


def group_of(name):
    return "cross_source" if name.startswith("intraday_volume") else "completeness"


# Số đo VN30: ngày mỏng 2,19% toàn bộ (tham khảo) -> 0,81% khi bỏ giai đoạn đã biết (kết luận).
ALL = [
    r("intraday_day_coverage_ratio", 0.9961, 0.998, False),
    r("intraday_thin_day_share", 0.0219, 0.01, False),
    r("intraday_days_missing", 13.0, None, True),
]
EXCL = [
    r("intraday_day_coverage_ratio", 0.9990, 0.998, True),
    r("intraday_thin_day_share", 0.0081, 0.01, True),
    r("intraday_days_missing", 3.0, None, True),
]
SINGLE = [
    r("intraday_days_outside_calendar", 0.0, 0.0, True),
    r("intraday_volume_match_ratio", 0.995, 0.99, True),
]


def test_verdict_comes_from_excluding_set_and_reference_keeps_all_data():
    v = build_verdict(ALL, EXCL, SINGLE, group_of)
    assert v.passed is True  # dù toàn bộ dữ liệu không đạt
    thin = next(c for c in v.checks if c["check_name"] == "intraday_thin_day_share")
    assert thin["metric"] == 0.0081 and thin["passed"] is True
    assert thin["basis"] == BASIS_EXCL == "excluding_known_periods"
    assert thin["threshold"] == 0.01 and thin["group"] == "completeness"
    assert thin["reference_all_data"] == {"metric": 0.0219, "passed": False}


def test_checks_without_variant_keep_all_data_basis_and_no_reference():
    v = build_verdict(ALL, EXCL, SINGLE, group_of)
    vol = next(c for c in v.checks if c["check_name"] == "intraday_volume_match_ratio")
    assert vol["basis"] == BASIS_ALL == "all_data" and "reference_all_data" not in vol
    assert vol["group"] == "cross_source"
    outside = next(c for c in v.checks if c["check_name"] == "intraday_days_outside_calendar")
    assert outside["basis"] == "all_data"


def test_dq_report_rows_naming():
    v = build_verdict(ALL, EXCL, SINGLE, group_of)
    by = {row.check_name: row for row in v.rows}
    assert len(by) == len(v.rows) == 3 + 3 + 2  # kết luận + tham khảo + không biến thể
    assert (
        by["intraday_thin_day_share"].metric_value == 0.0081
        and by["intraday_thin_day_share"].passed
    )
    ref = by["intraday_thin_day_share" + ALL_DATA_SUFFIX]
    assert ALL_DATA_SUFFIX == "__all_data"
    assert ref.metric_value == 0.0219 and not ref.passed and ref.threshold == 0.01
    assert by["intraday_volume_match_ratio"].metric_value == 0.995
    assert "intraday_volume_match_ratio__all_data" not in by
    assert not any("excl_known" in n for n in by)
    # cùng run_id / run_ts / table_name, đúng hợp đồng dq_report
    assert {(x.run_id, x.run_ts, x.table_name) for x in v.rows} == {("run-1", TS, "intraday_1m")}


def test_overall_passed_false_when_a_verdict_check_fails():
    bad_excl = [*EXCL[:1], r("intraday_thin_day_share", 0.02, 0.01, False), EXCL[2]]
    assert build_verdict(ALL, bad_excl, SINGLE, group_of).passed is False
    bad_single = [r("intraday_volume_days_minute_gt_daily", 2.0, 0.0, False)]
    assert build_verdict(ALL, EXCL, bad_single, group_of).passed is False


def test_reference_failure_alone_does_not_fail_overall():
    v = build_verdict(ALL, EXCL, [], group_of)
    assert not any(c["reference_all_data"]["passed"] for c in v.checks[:2])
    assert v.passed is True


def test_no_known_periods_both_sets_identical():
    v = build_verdict(ALL, ALL, SINGLE, group_of)
    for c in v.checks:
        if "reference_all_data" in c:
            assert c["basis"] == "excluding_known_periods"
            assert c["reference_all_data"] == {"metric": c["metric"], "passed": c["passed"]}
    assert v.passed is False  # kết luận = toàn bộ dữ liệu, vốn không đạt


def test_missing_excluding_result_is_an_error():
    with pytest.raises(ValueError, match="intraday_thin_day_share"):
        build_verdict(ALL, [EXCL[0], EXCL[2]], SINGLE, group_of)


def test_json_serialisable_and_rule_text():
    v = build_verdict(ALL, EXCL, SINGLE, group_of)
    json.dumps(v.checks)
    assert "excluding_known_periods" in VERDICT_RULE and "all_data" in VERDICT_RULE


# ------------------------------------------------------------ khoá đọc


def test_run_under_read_locks_order_and_release(monkeypatch):
    locks = pytest.importorskip(
        "stocklake.common.locks", reason="module khoá chưa có (agent khác đang thêm)"
    )
    from stocklake.dq import intraday as dq

    events = []

    class Handle:
        def __init__(self, waited):
            self.waited_s = waited

    @contextlib.contextmanager
    def fake_read_lock(fs, name, **kw):
        events.append(("acquire", name))
        try:
            yield Handle(1.25 if name == "clean.intraday_1m" else 0.5)
        finally:
            events.append(("release", name))

    monkeypatch.setattr(locks, "read_lock", fake_read_lock)
    out, waited = dq.run_under_read_locks(object(), lambda: events.append(("work",)) or "x")
    assert out == "x" and waited == pytest.approx(1.75)
    assert events == [
        ("acquire", "clean.intraday_1m"),
        ("acquire", "clean.daily_ohlcv"),
        ("work",),
        ("release", "clean.daily_ohlcv"),
        ("release", "clean.intraday_1m"),
    ]


def test_run_under_read_locks_releases_and_propagates_errors(monkeypatch):
    locks = pytest.importorskip("stocklake.common.locks", reason="module khoá chưa có")
    from stocklake.dq import intraday as dq

    released = []

    @contextlib.contextmanager
    def fake_read_lock(fs, name, **kw):
        try:
            yield type("H", (), {"waited_s": 0.0})()
        finally:
            released.append(name)

    monkeypatch.setattr(locks, "read_lock", fake_read_lock)

    def boom():
        raise RuntimeError("đọc lỗi")

    with pytest.raises(RuntimeError, match="đọc lỗi"):
        dq.run_under_read_locks(object(), boom)
    assert released == ["clean.daily_ohlcv", "clean.intraday_1m"]


def test_lock_timeout_propagates(monkeypatch):
    locks = pytest.importorskip("stocklake.common.locks", reason="module khoá chưa có")
    from stocklake.dq import intraday as dq

    @contextlib.contextmanager
    def timing_out(fs, name, **kw):
        raise locks.LockTimeout(f"chờ {name} quá hạn")
        yield  # pragma: no cover

    monkeypatch.setattr(locks, "read_lock", timing_out)
    with pytest.raises(TimeoutError, match="clean.intraday_1m"):
        dq.run_under_read_locks(object(), lambda: None)
