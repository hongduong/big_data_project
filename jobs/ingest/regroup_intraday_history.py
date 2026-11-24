"""Điểm vào: chia lại file raw.intraday_history cũ thành một row group mỗi tháng (không gọi VCI).

In một dòng JSON; mã thoát 1 nếu có file không qua kiểm tra, 0 nếu ổn.
"""

import argparse
import json
import sys

from stocklake.ingestion.collectors.intraday_history import regroup_history_files


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbols", help="Danh sách mã, vd FPT,VNM (mặc định: mọi mã)")
    ap.add_argument("--years", help="Danh sách năm, vd 2025,2026 (mặc định: mọi năm)")
    ap.add_argument("--dry-run", action="store_true", help="Chỉ ghi tạm và kiểm, không thay file")
    a = ap.parse_args()
    symbols = [s.strip().upper() for s in a.symbols.split(",") if s.strip()] if a.symbols else None
    years = [int(y) for y in a.years.split(",") if y.strip()] if a.years else None
    out = regroup_history_files(symbols, years, a.dry_run)
    print(json.dumps(out, ensure_ascii=False))
    return 1 if out["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
