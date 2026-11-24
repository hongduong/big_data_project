"""Job kiểm tra chất lượng nến phút (đầy đủ ngày, volume phút vs ngày).

Chạy: spark-submit jobs/dq/intraday_dq.py [--symbols FPT VNM] [--start D] [--end D] [--no-write]
"""

import argparse
import json

from stocklake.common.spark import get_spark
from stocklake.dq.intraday import run_intraday_dq

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--symbols", nargs="*", help="Chỉ kiểm tra các mã này (mặc định: tất cả)")
parser.add_argument("--start", help="Ngày bắt đầu YYYY-MM-DD")
parser.add_argument("--end", help="Ngày kết thúc YYYY-MM-DD")
parser.add_argument("--no-write", action="store_true", help="Không ghi curated/dq_report")
args = parser.parse_args()

report = run_intraday_dq(
    get_spark("intraday_dq"),
    symbols=args.symbols or None,
    start=args.start,
    end=args.end,
    write=not args.no_write,
)
print(json.dumps(report, default=str))
