"""VNDirect finfo (api-finfo.vndirect.com.vn): nến ngày từ 02/01/2013, có cả giá gốc
(`close`) và giá điều chỉnh (`adClose`), đơn vị nghìn đồng; khối ngoại `foreigns` từ 30/08/2018
(gồm VNINDEX); `stocks` (danh sách mã); `events` (sự kiện, có `disclosureDate`).
Nến phút dchart: FPT từ 11/2021, thưa hơn VCI. Planning mục 0.2.

Hiện cài `daily_bars` (endpoint `/v4/stock_prices`). Đã kiểm 24/11/2026:
- Response: {"data": [...], "currentPage", "size", "totalElements", "totalPages"}; trang 2 trở đi
  chỉ có `currentPage` và `size` (không có tổng), nên tổng lấy từ trang 1.
- Không hỗ trợ chỉ số: `code:VNINDEX` trả 200 với data rỗng.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

from stocklake.common.http import HttpClient
from stocklake.common.logging import get_logger
from stocklake.sources.base import (
    PRICE_UNIT_KVND,
    DailyBar,
    SourceClient,
    SourceParseError,
)

log = get_logger(__name__)

SOURCE_NAME = "vndirect"
BASE_URL = "https://api-finfo.vndirect.com.vn"
PRICES_URL = f"{BASE_URL}/v4/stock_prices"
DSTOCK_URL = "https://dstock.vndirect.com.vn"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Referer": f"{DSTOCK_URL}/",
    "Origin": DSTOCK_URL,
}

PAGE_SIZE = 500  # một năm ~250 phiên nên thường chỉ 1 trang
MAX_PAGES = 200  # chặn vòng lặp vô hạn nếu API trả trang lạ


# ----------------------------------------------------------------- parse thuần


def _num(x: Any) -> float | None:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def parse_stock_prices_page(payload: Any, symbol: str | None = None) -> list[DailyBar]:
    """Phân tích `data` của một trang stock_prices thành DailyBar (chưa lọc/sắp xếp).

    Ánh xạ: adOpen/adHigh/adLow/adClose -> open/high/low/close (giá điều chỉnh),
    close -> raw_close (giá gốc). Khối lượng lấy `nmVolume` (khớp lệnh); thoả thuận
    (`ptVolume`) KHÔNG cộng vào. Đã đối chiếu 10 phiên FPT 11/2026: VCI volume == nmVolume.
    Dòng thiếu giá/ngày bị bỏ, có log.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise SourceParseError("VNDirect: payload phải là object có `data` là list")
    out: list[DailyBar] = []
    skipped = 0
    for row in payload["data"]:
        if not isinstance(row, dict):
            raise SourceParseError(f"VNDirect: dòng phải là object, nhận {type(row).__name__}")
        try:
            d = datetime.strptime(str(row.get("date")), "%Y-%m-%d").date()
        except ValueError:
            skipped += 1
            continue
        code = row.get("code")
        if not isinstance(code, str) or not code:
            skipped += 1
            continue
        if symbol is not None and code != symbol:
            skipped += 1
            continue
        o, h, lo, c = (_num(row.get(k)) for k in ("adOpen", "adHigh", "adLow", "adClose"))
        vol = _num(row.get("nmVolume"))
        if None in (o, h, lo, c, vol):
            skipped += 1
            continue
        out.append(
            DailyBar(
                code, d, o, h, lo, c, int(round(vol)),  # type: ignore[arg-type]
                _num(row.get("close")), PRICE_UNIT_KVND, SOURCE_NAME,
            )
        )  # fmt: skip
    if skipped:
        log.warning("VNDirect: bỏ %d/%d dòng không hợp lệ", skipped, len(payload["data"]))
    return out


def page_info(payload: dict) -> tuple[int | None, int | None]:
    """(totalPages, size) nếu có; trang sau trang 1 không có totalPages."""
    tp, size = payload.get("totalPages"), payload.get("size")
    return (
        tp if isinstance(tp, int) and not isinstance(tp, bool) else None,
        size if isinstance(size, int) and not isinstance(size, bool) else None,
    )


# ----------------------------------------------------------------- client


class VndirectClient(SourceClient):
    name = SOURCE_NAME

    def __init__(self, http: HttpClient | None = None, max_per_second: float = 2.0):
        self.http = http or HttpClient(SOURCE_NAME, max_per_second=max_per_second)

    @staticmethod
    def build_query(symbol: str, start: date, end: date) -> str:
        return f"code:{symbol}~date:gte:{start.isoformat()}~date:lte:{end.isoformat()}"

    def daily_bars(self, symbol: str, start: date, end: date) -> list[DailyBar]:
        """Nến ngày điều chỉnh trong [start, end] (gồm hai đầu), tăng dần.

        Chỉ số (VNINDEX, ...) không có ở endpoint này: trả list rỗng.
        """
        if end < start:
            raise ValueError("end < start")
        q = self.build_query(symbol, start, end)
        bars: list[DailyBar] = []
        total_pages: int | None = None
        for page in range(1, MAX_PAGES + 1):
            params = {"q": q, "sort": "date:asc", "size": PAGE_SIZE, "page": page}
            payload = self.http.get_json(PRICES_URL, params=params, headers=dict(HEADERS))
            rows = parse_stock_prices_page(payload, symbol)
            n_raw = len(payload["data"])
            tp, size = page_info(payload)
            if page == 1:
                total_pages = tp
            bars.extend(rows)
            if n_raw == 0:
                break
            if total_pages is not None:
                if page >= total_pages:
                    break
            elif n_raw < (size or PAGE_SIZE):
                break
        else:
            log.warning("VNDirect %s: chạm MAX_PAGES=%d", symbol, MAX_PAGES)
        bars = [b for b in bars if start <= b.trade_date <= end]
        bars.sort(key=lambda b: b.trade_date)
        return bars
