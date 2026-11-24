import ast
import pathlib
import subprocess
import sys

DQ = pathlib.Path(__file__).resolve().parents[2] / "src" / "stocklake" / "dq"
FORBIDDEN = ("etl", "ingestion", "streaming", "analytics")


def test_dq_imports_without_pyspark():
    code = (
        "import sys\n"
        "import stocklake.dq, stocklake.dq.checks, stocklake.dq.report, stocklake.dq.intraday\n"
        "import stocklake.dq.notes, stocklake.dq.verdict\n"
        "import stocklake.dq.checks.intraday_completeness, stocklake.dq.checks.intraday_volume\n"
        "bad = [m for m in ('pyspark', 'pyarrow', 'fsspec') if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_dq_does_not_import_sibling_modules():
    for path in DQ.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            for name in names:
                parts = name.split(".")
                assert not (parts[0] == "stocklake" and len(parts) > 1 and parts[1] in FORBIDDEN), (
                    f"{path.name} import {name}"
                )
