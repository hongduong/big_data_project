"""Job raw -> clean cho nến phút (tăng dần theo tháng).

Chạy: spark-submit jobs/etl/intraday_raw_to_clean.py [--full | --dates YYYY-MM-DD ...]
Mặc định chỉ dựng lại tháng có đầu vào thay đổi; in một dòng JSON kết quả.
"""

import argparse
import json

from stocklake.common.spark import get_spark
from stocklake.etl.raw_to_clean.intraday import run_intraday_raw_to_clean

parser = argparse.ArgumentParser(description=__doc__)
group = parser.add_mutually_exclusive_group()
group.add_argument(
    "--dates", nargs="*", help="Ép dựng lại trọn các tháng chứa những ngày này (bỏ qua fingerprint)"
)
group.add_argument(
    "--full", action="store_true", help="Dựng lại mọi tháng và viết lại trạng thái build"
)
args = parser.parse_args()

print(
    json.dumps(
        run_intraday_raw_to_clean(get_spark("intraday_raw_to_clean"), args.dates or None, args.full)
    )
)
