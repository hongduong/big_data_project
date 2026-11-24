"""Điểm vào mỏng: phát lại nến phút lịch sử vào Kafka; in ReplayResult một dòng JSON."""

import argparse
import sys
from datetime import date

from stocklake.streaming.producers.replay import run_replay


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbols", help="Danh sách mã, cách nhau dấu phẩy (mặc định: sample.stocks)")
    p.add_argument("--days", type=int, default=5, help="N ngày giao dịch gần nhất đã đóng phiên")
    p.add_argument("--start", type=date.fromisoformat, help="Ngày đầu (VN), YYYY-MM-DD")
    p.add_argument("--end", type=date.fromisoformat, help="Ngày cuối (VN), YYYY-MM-DD")
    p.add_argument("--rate", type=float, default=2000.0, help="msg/giây")
    p.add_argument("--run-id", help="replay_run_id (mặc định tự sinh)")
    a = p.parse_args()
    symbols = [s.strip().upper() for s in a.symbols.split(",") if s.strip()] if a.symbols else None
    result = run_replay(symbols, a.days, a.start, a.end, a.rate, a.run_id)
    print(result.to_json(), flush=True)
    sys.exit(1 if result.failed > 0 else 0)


if __name__ == "__main__":
    main()
