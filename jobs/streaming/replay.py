"""Điểm vào mỏng: phát lại nến phút lịch sử vào Kafka; in ReplayResult một dòng JSON."""

import argparse
import sys
from datetime import date

from stocklake.streaming.producers.replay import HistoryUnavailableError, run_replay


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbols", help="Danh sách mã, cách nhau dấu phẩy (mặc định theo --group)")
    p.add_argument("--group", choices=["sample", "vn30"], default="sample", help="Rổ mã mặc định")
    p.add_argument("--days", type=int, default=5, help="N ngày giao dịch gần nhất có trong lịch sử")
    p.add_argument("--start", type=date.fromisoformat, help="Ngày đầu (VN), YYYY-MM-DD")
    p.add_argument("--end", type=date.fromisoformat, help="Ngày cuối (VN), YYYY-MM-DD")
    p.add_argument("--rate", type=float, default=2000.0, help="msg/giây")
    p.add_argument("--run-id", help="replay_run_id (mặc định tự sinh)")
    a = p.parse_args()
    symbols = [s.strip().upper() for s in a.symbols.split(",") if s.strip()] if a.symbols else None
    try:
        result = run_replay(symbols, a.days, a.start, a.end, a.rate, a.run_id, group=a.group)
    except (HistoryUnavailableError, ValueError) as e:
        print(f"Replay thất bại: {e}", file=sys.stderr)
        sys.exit(1)
    print(result.to_json(), flush=True)
    sys.exit(1 if result.failed > 0 else 0)


if __name__ == "__main__":
    main()
