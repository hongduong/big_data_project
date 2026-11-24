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
