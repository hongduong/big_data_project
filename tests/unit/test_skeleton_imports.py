import ast
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ("ingestion", "streaming", "etl", "dq", "analytics")
HEAVY = ("pyspark", "pyarrow", "confluent_kafka", "streamlit", "fsspec", "pandas")


def test_skeleton_packages_import_without_heavy_deps():
    code = (
        "import sys\n"
        "import stocklake.streaming, stocklake.etl, stocklake.dq, stocklake.analytics\n"
        f"bad = [m for m in {HEAVY!r} if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def _imported_modules(path: Path) -> list[str]:
    names: list[str] = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.append(node.module)
    return names


def test_no_sibling_imports_between_modules():
    src = ROOT / "src" / "stocklake"
    for pkg in PACKAGES:
        pkg_dir = src / pkg
        if not pkg_dir.exists():
            continue
        others = {f"stocklake.{p}" for p in PACKAGES if p != pkg}
        for py in pkg_dir.rglob("*.py"):
            for mod in _imported_modules(py):
                hit = [o for o in others if mod == o or mod.startswith(o + ".")]
                assert not hit, f"{py.relative_to(ROOT)} import {mod}"


def test_dashboard_does_not_import_spark():
    for py in (ROOT / "dashboard").rglob("*.py"):
        for mod in _imported_modules(py):
            assert mod.split(".")[0] != "pyspark", f"{py.relative_to(ROOT)} import {mod}"
            assert not mod.startswith("stocklake.") or mod.split(".")[1] in ("contracts", "common")
