"""Điểm vào mỏng: so số bản ghi phân biệt ở raw với số replay đã gửi (spark-submit)."""

import argparse
import json
import sys

from stocklake.streaming.sinks.kafka_to_raw import verify_replay


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-id", required=True)
    p.add_argument("--expected", type=int, required=True, help="Số bản tin replay đã gửi")
    p.add_argument("--output", help="Thư mục raw (mặc định /data/raw/intraday_prices)")
    a = p.parse_args()
    r = verify_replay(a.run_id, a.expected, a.output)
    print(json.dumps(r), flush=True)
    sys.exit(0 if r["ok"] else 1)


if __name__ == "__main__":
    main()
