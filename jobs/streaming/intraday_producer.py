"""Điểm vào mỏng: producer nến phút realtime VCI -> Kafka (vòng lặp hoặc --once)."""

import argparse
import json
from dataclasses import asdict

from stocklake.streaming.producers.intraday import run_intraday_producer


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbols", help="Danh sách mã, cách nhau dấu phẩy (mặc định: sample.stocks)")
    p.add_argument("--once", action="store_true", help="Poll một lần (bỏ qua giờ giao dịch)")
    p.add_argument("--interval", type=float, default=60.0, help="Giây giữa hai lần poll")
    a = p.parse_args()
    symbols = [s.strip().upper() for s in a.symbols.split(",") if s.strip()] if a.symbols else None
    result = run_intraday_producer(symbols, once=a.once, interval_s=a.interval)
    if result is not None:
        print(json.dumps(asdict(result), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
