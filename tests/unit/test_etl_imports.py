import subprocess
import sys


def test_etl_imports_without_pyspark():
    code = (
        "import sys\n"
        "import stocklake.etl, stocklake.etl.raw_to_clean, stocklake.etl.compaction\n"
        "import stocklake.etl.raw_to_clean.daily, stocklake.etl.raw_to_clean.intraday\n"
        "bad = [m for m in ('pyspark', 'pyarrow', 'fsspec') if m in sys.modules]\n"
        "assert not bad, bad\n"
        "assert callable(stocklake.etl.raw_to_clean.clean_daily_ohlcv)\n"
    )
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
