"""Job raw -> clean cho nến ngày.

Chạy: spark-submit jobs/etl/daily_raw_to_clean.py [--years 2025 2026]
"""

import argparse
import json

from stocklake.common.spark import get_spark
from stocklake.etl.raw_to_clean.daily import run_daily_raw_to_clean

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--years", nargs="*", type=int, help="Chỉ xử lý các năm này (mặc định: tất cả)")
args = parser.parse_args()

print(json.dumps(run_daily_raw_to_clean(get_spark("daily_raw_to_clean"), args.years or None)))
