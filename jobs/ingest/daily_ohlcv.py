"""Điểm vào: nạp nến ngày vào raw.daily_ohlcv. Chỉ đọc tham số rồi gọi một hàm.

Lệch giá phát hiện được nằm ở các khoá `price_adjustments`, `price_mismatches`,
`price_revisions_minor`, `forced_refetch`, `refetch_pending` của JSON in ra (không đổi mã thoát).
"""

import argparse
import json
import sys
from datetime import date

from stocklake.ingestion.collectors.daily_ohlcv import run_daily_ohlcv_summary


def _split(arg: str | None) -> list[str] | None:
    return [s.strip().upper() for s in arg.split(",") if s.strip()] if arg else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbols", help="Danh sách mã, vd FPT,VNM (mặc định: conf symbols.yaml)")
    ap.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD")
    ap.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD (mặc định hôm nay VN)")
    ap.add_argument("--years-back", type=int, default=1)
    ap.add_argument(
        "--refetch-all-years",
        metavar="SYMS",
        help="Ép lấy lại MỌI năm đang có (state + file, kể cả ngoài --start/--end) cho các mã này, "
        "vd FPT,VNM; dùng khi nguồn đã điều chỉnh giá mà file năm hiện tại đã mang hệ số mới.",
    )
    a = ap.parse_args()
    symbols = _split(a.symbols)
    out = run_daily_ohlcv_summary(
        symbols, a.start, a.end, a.years_back, refetch_all_years=_split(a.refetch_all_years) or ()
    )
    print(json.dumps(out, ensure_ascii=False))
    return 1 if out["failed"] or out["stopped_blocked"] else 0


if __name__ == "__main__":
    sys.exit(main())
