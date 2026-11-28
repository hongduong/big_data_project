"""Điểm vào: tải lịch sử nến 1 phút vào raw.intraday_history. Chỉ đọc tham số rồi gọi một hàm.

Mã thoát: 0 ổn, 1 có mã lỗi, 2 bị chặn (429/403), 3 từ chối/dừng vì giờ giao dịch.
Lệch giá phát hiện được nằm ở các khoá `price_adjustments`, `price_mismatches`,
`price_revisions_minor`, `forced_refetch` của JSON in ra (không đổi mã thoát).
"""

import argparse
import json
import sys
from datetime import date

from stocklake.ingestion.collectors.intraday_history import run_intraday_history


def _split(arg: str | None) -> list[str] | None:
    return [s.strip().upper() for s in arg.split(",") if s.strip()] if arg else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbols", help="Danh sách mã, vd FPT,VNM (thắng --group)")
    ap.add_argument("--group", choices=["vn30", "sample"], help="Nhóm mã trong symbols.yaml")
    ap.add_argument("--since", type=date.fromisoformat, help="YYYY-MM-DD (mặc định 2023-09-11)")
    ap.add_argument("--allow-trading-hours", action="store_true")
    ap.add_argument("--max-symbols", type=int)
    ap.add_argument(
        "--refetch-all-years",
        metavar="SYMS",
        help="Ép tải lại MỌI năm (từ --since) cho các mã này, vd FPT,VNM; dùng khi nguồn đã điều "
        "chỉnh giá mà file năm hiện tại đã mang hệ số mới. Mã phải nằm trong lượt chạy.",
    )
    a = ap.parse_args()
    symbols = _split(a.symbols)
    if not symbols and not a.group:
        ap.error("cần --symbols hoặc --group")
    out = run_intraday_history(
        symbols, a.group, a.since, a.allow_trading_hours, a.max_symbols,
        refetch_all_years=_split(a.refetch_all_years) or (),
    )  # fmt: skip
    print(json.dumps(out, ensure_ascii=False))
    reason = out["stopped_reason"]
    if out["blocked"] or reason == "blocked":
        return 2
    if reason == "trading_hours":
        return 3
    return 1 if out["symbols_failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
