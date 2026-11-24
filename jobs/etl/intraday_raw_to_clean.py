"""Job raw -> clean cho nến phút.

Chạy: spark-submit jobs/etl/intraday_raw_to_clean.py [--dates YYYY-MM-DD ...]
"""

import argparse
import json

from stocklake.common.spark import get_spark
from stocklake.etl.raw_to_clean.intraday import run_intraday_raw_to_clean

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--dates", nargs="*", help="Xử lý trọn các tháng chứa những ngày này (mặc định: tất cả)"
)
args = parser.parse_args()

print(json.dumps(run_intraday_raw_to_clean(get_spark("intraday_raw_to_clean"), args.dates or None)))
