"""Điểm vào: nạp nến ngày vào raw.daily_ohlcv. Chỉ đọc tham số rồi gọi một hàm."""

import argparse
import json
import sys
from datetime import date

from stocklake.ingestion.collectors.daily_ohlcv import run_daily_ohlcv_summary


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbols", help="Danh sách mã, vd FPT,VNM (mặc định: conf symbols.yaml)")
    ap.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD")
    ap.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD (mặc định hôm nay VN)")
    ap.add_argument("--years-back", type=int, default=1)
    a = ap.parse_args()
    symbols = [s.strip().upper() for s in a.symbols.split(",") if s.strip()] if a.symbols else None
    out = run_daily_ohlcv_summary(symbols, a.start, a.end, a.years_back)
    print(json.dumps(out, ensure_ascii=False))
    return 1 if out["failed"] or out["stopped_blocked"] else 0


if __name__ == "__main__":
    sys.exit(main())
