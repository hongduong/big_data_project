import ast
import importlib.util
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "dashboard"


def _load():
    spec = importlib.util.spec_from_file_location("dashboard_data", DASH / "data.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def d():
    return _load()


def test_build_filters_empty(d):
    assert d.build_filters() is None


def test_build_filters_full(d):
    f = d.build_filters(["VNM", "FPT", "VNM"], date(2025, 3, 1), date(2026, 2, 1))
    assert f == [
        ("symbol", "in", ["FPT", "VNM"]),
        ("trade_date", ">=", date(2025, 3, 1)),
        ("year", ">=", 2025),
        ("trade_date", "<=", date(2026, 2, 1)),
        ("year", "<=", 2026),
    ]


def test_intraday_filters(d):
    assert d.intraday_filters("VNM") == [("symbol", "==", "VNM")]
    f = d.intraday_filters("VNM", date(2026, 11, 20))
    assert ("month", "==", 11) in f and ("year", "==", 2026) in f


def test_to_vn_time(d):
    # 02:00 UTC = 09:00 giờ VN; naive coi là UTC
    assert d.to_vn_time(datetime(2026, 11, 20, 2, 0)) == datetime(2026, 11, 20, 9, 0)
    aware = datetime(2026, 11, 20, 18, 0, tzinfo=timezone.utc)
    assert d.to_vn_time(aware) == datetime(2026, 11, 21, 1, 0)


def test_daily_return_pct(d):
    out = d.daily_return_pct([100.0, 110.0, None, 121.0])
    assert out[0] is None and out[2] is None
    assert out[1] == pytest.approx(10.0)
    assert out[3] == pytest.approx(10.0)  # so với phiên hợp lệ gần nhất


def test_moving_average(d):
    assert d.moving_average([1, 2, 3, 4], 2) == [None, 1.5, 2.5, 3.5]
    assert d.moving_average([1, None, 3], 2) == [None, None, None]
    with pytest.raises(ValueError):
        d.moving_average([1], 0)


def test_summary_line(d):
    assert d.summary_line(0, 0, None, None) == "Không có dữ liệu."
    assert "1,234 dòng" in d.summary_line(1234, 10, "2025-01-01", "2025-12-31")


def test_retry_once_success_first_time(d):
    calls, sleeps = [], []

    def fn():
        calls.append(1)
        return "ok"

    assert d.retry_once(fn, pause_s=1.5, sleep=sleeps.append, retry_on=(OSError,)) == "ok"
    assert len(calls) == 1 and sleeps == []


def test_retry_once_fails_once_then_succeeds(d):
    calls, sleeps = [], []

    def fn():
        calls.append(1)
        if len(calls) == 1:
            raise FileNotFoundError("part-0001.parquet")
        return "ok"

    assert d.retry_once(fn, pause_s=2.0, sleep=sleeps.append, retry_on=(OSError,)) == "ok"
    assert len(calls) == 2 and sleeps == [2.0]


def test_retry_once_fails_twice_raises_second_error(d):
    sleeps, errors = [], [OSError("lần 1"), OSError("lần 2")]

    def fn():
        raise errors.pop(0)

    with pytest.raises(OSError, match="lần 2"):
        d.retry_once(fn, pause_s=1.0, sleep=sleeps.append, retry_on=(OSError,))
    assert sleeps == [1.0] and errors == []


def test_retry_once_does_not_retry_other_errors(d):
    calls, sleeps = [], []

    def fn():
        calls.append(1)
        raise KeyError("x")

    with pytest.raises(KeyError):
        d.retry_once(fn, pause_s=1.0, sleep=sleeps.append, retry_on=(OSError,))
    assert len(calls) == 1 and sleeps == []


def test_dashboard_imports_only_contracts_and_common():
    allowed = ("stocklake.contracts", "stocklake.common")
    for path in DASH.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            else:
                continue
            for n in names:
                assert n.split(".")[0] != "pyspark", f"{path}: {n}"
                if n.split(".")[0] == "stocklake":
                    assert n.startswith(allowed), f"{path}: {n}"


def test_intraday_month_helpers(d):
    day = date(2026, 3, 5)
    assert d.intraday_month_path("/data/clean/intraday_1m/", day) == (
        "/data/clean/intraday_1m/year=2026/month=3"
    )
    # đọc thẳng thư mục tháng: không được lọc theo year/month (không có trong schema)
    assert d.intraday_month_filters("VNM", day) == [
        ("symbol", "==", "VNM"),
        ("trade_date", "==", day),
    ]


def test_parse_and_latest_partition(d):
    assert d.parse_partition("/data/x/year=2026/month=11/part-0.parquet") == (2026, 11)
    assert d.parse_partition("/data/x/year=2025/month=9") == (2025, 9)
    assert d.parse_partition("/data/x/_build_state.json") is None
    paths = [
        "/t/year=2025/month=12/a.parquet",
        "/t/year=2026/month=2/a.parquet",
        "/t/year=2026/month=10/a.parquet",
        "/t/_build_state.json",
    ]
    assert d.latest_partition(paths) == (2026, 10)  # so sánh số, không so chuỗi
    assert d.latest_partition(["/t/_build_state.json"]) is None


def test_merge_footer_stats(d):
    t = datetime
    got = d.merge_footer_stats(
        [(10, t(2026, 1, 5), t(2026, 1, 9)), (5, None, None), (7, t(2025, 12, 1), t(2026, 1, 2))]
    )
    assert got == (22, t(2025, 12, 1), t(2026, 1, 9))
    assert d.merge_footer_stats([]) == (0, None, None)


def test_summary_line_label(d):
    s = d.summary_line(5, 3, "a", "b", "mã trong tháng gần nhất (2026-11)")
    assert "3 mã trong tháng gần nhất (2026-11)" in s


def test_column_stats_footer_only(d, tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    pytest.importorskip("fsspec")
    pytest.importorskip("stocklake.common.hdfs")
    import fsspec

    t1 = pa.table({"bar_ts": [datetime(2026, 1, 5), datetime(2026, 1, 6)], "symbol": ["A", "A"]})
    t2 = pa.table({"bar_ts": [datetime(2026, 1, 2)], "symbol": ["B"]})
    path = tmp_path / "f.parquet"
    with pq.ParquetWriter(str(path), t1.schema, write_statistics=True) as w:
        w.write_table(t1)
        w.write_table(t2)
    rows, lo, hi = d._column_stats(fsspec.filesystem("file"), str(path), "bar_ts")
    assert (rows, lo, hi) == (3, datetime(2026, 1, 2), datetime(2026, 1, 6))
