"""Logic tăng dần của clean.intraday_1m và khoá ghi: Python thuần, không cần Spark."""

import calendar
import json
from types import SimpleNamespace

import pytest

from stocklake.etl.raw_to_clean import _incremental as inc
from stocklake.etl.raw_to_clean._incremental import DirInfo

H, S = inc.HISTORY, inc.STREAM


def fp(rows=10, checksum="1"):
    return {S: {"rows": rows, "max_bar_us": 1, "sum_volume": "5", "checksum": checksum}}


def state(months=None, horizon=1_000, history=(), stream=()):
    return {
        "version": inc.STATE_VERSION,
        "built_at": "2026-11-24T00:00:00+00:00",
        "run_id": "r0",
        "mode": "full",
        "probe_ms": horizon,
        "horizon_ms": horizon,
        "inputs": {H: list(history), S: list(stream)},
        "months": months or {},
    }


def entry(f=None, rows=10, **kw):
    return {"fingerprint": f or fp(), "clean_rows": rows, "built_at": "x", "run_id": "r0", **kw}


# ---------------------------------------------------------------- tháng


def test_normalise_months_accepts_dates_and_months():
    assert inc.normalise_months(["2026-01-05", "2026-01", "2025-12-31"]) == ["2025-12", "2026-01"]
    with pytest.raises(ValueError):
        inc.normalise_months(["2026-13-01"])
    with pytest.raises(ValueError):
        inc.normalise_months(["hôm qua"])


def test_month_bounds_are_vietnam_midnight():
    lo, hi = inc.month_bounds_utc("2026-01")
    assert lo == calendar.timegm((2025, 12, 31, 17, 0, 0))
    assert hi == calendar.timegm((2026, 1, 31, 17, 0, 0))
    lo, hi = inc.month_bounds_utc("2025-12")
    assert hi == calendar.timegm((2026, 1, 1, 0, 0, 0)) - 7 * 3600


def test_month_ranges_merge_consecutive_across_year():
    got = inc.month_ranges(["2026-01", "2025-11", "2025-12", "2026-03"])
    assert got == [("2025-11", "2026-01"), ("2026-03", "2026-03")]
    assert inc.month_last_day("2024-02") == "2024-02-29"


def test_months_of_dir_history_year_is_twelve_months():
    got = inc.months_of_dir(H, "source=vci/year=2025")
    assert got == [f"2025-{m:02d}" for m in range(1, 13)]
    assert inc.months_of_dir(S, "trade_date=2026-02-27") == ["2026-02"]
    assert inc.months_of_dir(S, "trade_date=__HIVE_DEFAULT_PARTITION__") == []
    assert inc.months_of_dir(H, "source=vci") == []


def test_dir_name_from_full_paths():
    p = "hdfs://namenode:8020/data/raw/intraday_history/source=vci/year=2026"
    assert inc.dir_name(H, p) == "source=vci/year=2026"
    p = "file:/tmp/x/data/raw/intraday_prices/trade_date=2026-01-05/"
    assert inc.dir_name(S, p) == "trade_date=2026-01-05"
    assert inc.dir_name(S, "hdfs://nn/data/raw/intraday_prices/_spark_metadata") is None


def test_clean_months_from_files():
    files = [
        "hdfs://nn/data/clean/intraday_1m/year=2026/month=1/part-0.snappy.parquet",
        "hdfs://nn/data/clean/intraday_1m/year=2025/month=12/part-1.parquet",
        "hdfs://nn/data/clean/intraday_1m/_build_state.json",
    ]
    assert inc.clean_months_from_files(files) == {"2026-01", "2025-12"}


# ---------------------------------------------------------------- trạng thái


def test_state_roundtrip():
    st = state({"2026-01": entry()}, history=["source=vci/year=2026"])
    text = inc.dump_state(st)
    assert text.isascii()
    assert inc.parse_state(text) == st
    assert inc.parse_state(text.encode()) == st


@pytest.mark.parametrize(
    "text",
    [
        None,
        "",
        "{not json",
        "[]",
        json.dumps({**state(), "version": 2}),
        json.dumps({**state(), "months": []}),
        json.dumps({**state(), "horizon_ms": "1"}),
        json.dumps({**state(), "months": {"2026-1": {}}}),
        json.dumps({**state(), "inputs": {H: "x"}}),
    ],
)
def test_state_invalid_or_other_version_is_none(text):
    assert inc.parse_state(text) is None


def test_choose_mode():
    st = state()
    assert inc.choose_mode(False, [], st) == "incremental"
    assert inc.choose_mode(False, [], None) == "full"
    assert inc.choose_mode(True, [], st) == "full"
    assert inc.choose_mode(False, ["2026-01"], None) == "dates"
    with pytest.raises(ValueError):
        inc.choose_mode(True, ["2026-01"], st)


# ---------------------------------------------------------------- ứng viên


def test_horizon_uses_namenode_times_only():
    dirs = {H: [DirInfo("source=vci/year=2026", 5_000)], S: [DirInfo("trade_date=2026-01-05", 7)]}
    # Thay đổi mới nhất đã cũ hơn probe - lag: horizon = mtime lớn nhất.
    assert inc.compute_horizon(100_000, dirs, lag_ms=10_000) == 5_000
    # Thay đổi còn trong khoảng lag: horizon = probe - lag (để lượt sau kiểm lại).
    assert inc.compute_horizon(12_000, dirs, lag_ms=10_000) == 2_000
    assert inc.compute_horizon(12_000, {H: [], S: []}, lag_ms=10_000) == 2_000


def test_candidates_from_mtime_new_and_removed_dirs():
    st = state(
        horizon=1_000,
        history=["source=vci/year=2025", "source=vci/year=2026"],
        stream=["trade_date=2026-01-05", "trade_date=2026-02-02", "trade_date=2026-03-02"],
    )
    dirs = {
        H: [DirInfo("source=vci/year=2025", 900), DirInfo("source=vci/year=2026", 1_001)],
        S: [
            DirInfo("trade_date=2026-01-05", 1_000),  # = horizon: không đổi
            DirInfo("trade_date=2026-02-02", 500),
            DirInfo("trade_date=2027-04-01", 10),  # thư mục mới dù mtime cũ (copy giữ mtime)
        ],
    }
    got = inc.candidate_months(dirs, st)
    assert set(got) == {f"2026-{m:02d}" for m in range(1, 13)} | {"2027-04"}
    assert got["2026-03"] == ["history:modified", "stream:removed"]
    assert got["2027-04"] == ["stream:new_dir"]
    assert "history:modified" in got["2026-01"]


def test_no_candidates_when_nothing_changed():
    st = state(horizon=1_000, stream=["trade_date=2026-01-05"])
    dirs = {H: [], S: [DirInfo("trade_date=2026-01-05", 999)]}
    assert inc.candidate_months(dirs, st) == {}


def test_months_to_fingerprint():
    st = state({"2026-01": entry(), "2026-02": entry(), "2026-03": entry(orphan=True)})
    clean = {"2026-01"}
    assert inc.months_to_fingerprint("full", st, [], clean, []) is None
    assert inc.months_to_fingerprint("dates", st, ["2026-05"], clean, ["2026-04"]) == ["2026-04"]
    # 2026-02 thiếu phân vùng clean -> phải kiểm; orphan không tính.
    got = inc.months_to_fingerprint("incremental", st, {"2026-05": []}, clean, [])
    assert got == ["2026-02", "2026-05"]


# ---------------------------------------------------------------- kế hoạch


def _possible(*months):
    return set(months)


def test_decide_incremental_changed_new_missing_skip_orphan():
    st = state(
        {
            "2026-01": entry(fp(checksum="1")),
            "2026-02": entry(fp(checksum="2")),
            "2026-03": entry(fp(checksum="3")),
            "2026-04": entry(fp(checksum="4")),
            "2025-01": entry(),  # không còn thư mục đầu vào nào
        }
    )
    fps = {
        "2026-01": fp(checksum="1"),  # không đổi
        "2026-02": fp(checksum="22"),  # đổi
        "2026-03": fp(checksum="3"),  # không đổi nhưng mất phân vùng clean
        "2026-05": fp(checksum="5"),  # mới
        # 2026-04: đã kiểm mà không còn dòng nào -> mồ côi
    }
    compared = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05"]
    clean = {"2026-01", "2026-02", "2026-04", "2025-01"}
    possible = {f"2026-{m:02d}" for m in range(1, 13)}
    plan = inc.decide("incremental", st, compared, fps, clean, possible)
    assert plan.rebuild == ["2026-02", "2026-03", "2026-05"]
    assert plan.reasons == {"2026-02": "changed", "2026-03": "missing_partition", "2026-05": "new"}
    assert plan.orphans == ["2025-01", "2026-04"]
    assert plan.compared == compared


def test_decide_incremental_reingest_same_fingerprint_is_noop():
    st = state({"2026-01": entry(fp())})
    plan = inc.decide("incremental", st, ["2026-01"], {"2026-01": fp()}, {"2026-01"}, {"2026-01"})
    assert plan.rebuild == [] and plan.orphans == []


def test_decide_orphan_with_returning_input_is_rebuilt():
    st = state({"2026-01": {"orphan": True}})
    plan = inc.decide("incremental", st, ["2026-01"], {"2026-01": fp()}, {"2026-01"}, {"2026-01"})
    assert plan.rebuild == ["2026-01"]


def test_decide_full_and_dates():
    st = state({"2025-12": entry()})
    fps = {"2026-01": fp(), "2026-02": fp(rows=0)}
    plan = inc.decide("full", st, None, fps, {"2026-01", "2025-11"}, set())
    assert plan.rebuild == ["2026-01"]
    assert plan.orphans == ["2025-11", "2025-12"]
    # dates: ép dựng lại kể cả fingerprint không đổi; tháng ép mà không có dòng -> mồ côi.
    st = state({"2026-01": entry(fp()), "2026-03": entry()})
    fps = {"2026-01": fp()}
    plan = inc.decide(
        "dates", st, ["2026-01", "2026-03"], fps, {"2026-01"}, set(), ["2026-01", "2026-03"]
    )
    assert plan.rebuild == ["2026-01"]
    assert plan.reasons == {"2026-01": "forced"}
    assert plan.orphans == ["2026-03"]


# ---------------------------------------------------------------- trạng thái kế tiếp


DIRS = {H: [DirInfo("source=vci/year=2026", 50)], S: [DirInfo("trade_date=2026-01-05", 60)]}


def _next(prev, plan, fps, rows, horizon=2_000):
    return inc.next_state(
        prev,
        plan,
        fps,
        rows,
        horizon_ms=horizon,
        probe_ms=3_000,
        dirs=DIRS,
        run_id="r1",
        now_iso="t1",
    )


def test_next_state_incremental_updates_only_rebuilt_and_advances_horizon():
    prev = state({"2026-01": entry(fp(checksum="1")), "2026-02": entry(fp(checksum="2"))})
    plan = inc.Plan("incremental", ["2026-02"], {"2026-02": "changed"}, ["2026-02"], ["2025-01"])
    new = _next(prev, plan, {"2026-02": fp(checksum="22")}, {"2026-02": 7})
    assert new["months"]["2026-01"] == prev["months"]["2026-01"]
    assert new["months"]["2026-02"]["fingerprint"] == fp(checksum="22")
    assert new["months"]["2026-02"]["clean_rows"] == 7
    assert new["months"]["2025-01"] == {"orphan": True}
    assert new["horizon_ms"] == 2_000 and new["probe_ms"] == 3_000
    assert new["inputs"] == {H: ["source=vci/year=2026"], S: ["trade_date=2026-01-05"]}
    assert inc.parse_state(inc.dump_state(new)) == new
    assert inc.clean_rows_total(new) == 17


def test_next_state_full_rewrites_and_dates_keeps_horizon():
    prev = state({"2025-01": entry()}, horizon=100)
    plan = inc.Plan("full", ["2026-01"], {"2026-01": "full"}, ["2026-01"], [])
    new = _next(prev, plan, {"2026-01": fp()}, {"2026-01": 3})
    assert list(new["months"]) == ["2026-01"] and new["mode"] == "full"

    plan = inc.Plan("dates", ["2026-01"], {"2026-01": "forced"}, ["2026-01"], [])
    new = _next(prev, plan, {"2026-01": fp()}, {"2026-01": 3})
    assert set(new["months"]) == {"2025-01", "2026-01"}
    assert new["horizon_ms"] == 100 and new["inputs"] == prev["inputs"]
    # dates khi chưa có trạng thái: không ghi (lượt sau tự chạy full).
    assert _next(None, plan, {"2026-01": fp()}, {"2026-01": 3}) is None


def test_merge_state_keeps_other_runs_months_and_larger_horizon():
    plan = inc.Plan("incremental", ["2026-02"], {"2026-02": "changed"}, ["2026-02"], [])
    ours = state({"2026-01": entry(fp(checksum="old")), "2026-02": entry(fp(checksum="ours"))}, 500)
    disk = state(
        {"2026-01": entry(fp(checksum="other")), "2026-02": entry(fp(checksum="x"))},
        900,
        stream=["trade_date=2026-02-02"],
    )
    got = inc.merge_state(disk, ours, plan)
    assert got["months"]["2026-01"]["fingerprint"] == fp(checksum="other")
    assert got["months"]["2026-02"]["fingerprint"] == fp(checksum="ours")
    assert got["horizon_ms"] == 900 and got["inputs"] == disk["inputs"]
    assert inc.merge_state(None, ours, plan) == ours


def test_needs_state_write():
    prev = state({"2026-01": entry()}, stream=["trade_date=2026-01-05"])
    dirs = {H: [], S: [DirInfo("trade_date=2026-01-05", 1)]}
    noop = inc.Plan("incremental")
    assert not inc.needs_state_write(noop, prev, {}, dirs)
    # Có ứng viên nhưng fingerprint khớp: ghi để dời horizon (lượt sau không tính lại).
    assert inc.needs_state_write(noop, prev, {"2026-01": ["stream:modified"]}, dirs)
    assert inc.needs_state_write(inc.Plan("incremental", orphans=["2026-01"]), prev, {}, dirs)
    assert inc.needs_state_write(inc.Plan("full"), None, {}, dirs)
    assert not inc.needs_state_write(inc.Plan("dates"), None, {}, dirs)


# ---------------------------------------------------------------- khoá ghi


def _fake_spark():
    return SimpleNamespace(sparkContext=SimpleNamespace(appName="job", applicationId="app-1"))


def _locks():
    try:
        return pytest.importorskip(
            "stocklake.common.locks", reason="stocklake.common.locks chưa có (agent khác đang thêm)"
        )
    except Exception as exc:  # noqa: BLE001 - module đang sửa dở
        pytest.skip(f"stocklake.common.locks import lỗi: {exc}")


def test_clean_write_lock_acquires_and_releases():
    locks = _locks()
    from stocklake.etl.raw_to_clean._common import clean_write_lock

    fs = locks.MemoryLockFs()
    with clean_write_lock(_fake_spark(), "intraday_1m", fs) as h:
        assert h.waited_s >= 0
        held = [p for p in fs.files if p.endswith("writer.lock")]
        assert held == ["/locks/clean.intraday_1m/writer.lock"]
        assert json.loads(fs.files[held[0]])["owner"] == "job:app-1"
    assert fs.files == {}


def test_clean_write_lock_timeout_is_clear_error_and_body_not_run():
    locks = _locks()
    from stocklake.etl.raw_to_clean._common import clean_write_lock

    fs = locks.MemoryLockFs()
    ran = []
    with locks.read_lock(fs, "clean.daily_ohlcv", owner="dashboard"):
        with pytest.raises(TimeoutError, match="clean.daily_ohlcv"):
            with clean_write_lock(
                _fake_spark(), "daily_ohlcv", fs, wait_timeout_s=0.05, poll_s=0.01
            ):
                ran.append(1)
    assert ran == []
    assert fs.files == {}  # writer.lock đã dọn


def test_clean_write_lock_disabled_and_body_errors_propagate():
    locks = _locks()
    from stocklake.etl.raw_to_clean._common import clean_write_lock

    with clean_write_lock(_fake_spark(), "intraday_1m", False) as h:
        assert h.waited_s == 0.0
    fs = locks.MemoryLockFs()
    with pytest.raises(locks.LockTimeout):  # LockTimeout từ thân không bị đổi thành lỗi khoá
        with clean_write_lock(_fake_spark(), "intraday_1m", fs):
            raise locks.LockTimeout("từ thân")
    assert fs.files == {}
