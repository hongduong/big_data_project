"""Client VCI (Vietcap): nến ngày, nến 1 phút, danh sách mã.

Endpoint (Planning mục 0.2):
- POST /api/chart/OHLCChart/gap-chart  — mỗi request một mã, giá điều chỉnh, đơn vị đồng.
- GET  /api/price/symbols/getAll       — danh sách mã và sàn.
- GET  /api/price/symbols/getByGroup   — mã thuộc một rổ (vd group=VN30).
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, NamedTuple

from stocklake.common.http import HttpClient
from stocklake.common.logging import get_logger
from stocklake.contracts.available_at import VN_TZ
from stocklake.sources.base import (
    PRICE_UNIT_VND,
    DailyBar,
    MinuteBar,
    SourceClient,
    SourceParseError,
    SymbolInfo,
)

log = get_logger(__name__)

UTC = timezone.utc
SOURCE_NAME = "vci"
BASE_URL = "https://trading.vietcap.com.vn"
CHART_URL = f"{BASE_URL}/api/chart/OHLCChart/gap-chart"
SYMBOLS_URL = f"{BASE_URL}/api/price/symbols/getAll"
GROUP_URL = f"{BASE_URL}/api/price/symbols/getByGroup"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Referer": f"{BASE_URL}/",
    "Origin": BASE_URL,
}

DAILY_PAGE_SIZE = 5000  # đã thử countBack 6000 cho FPT: OK
MINUTE_PAGE_SIZE = 30000  # tối đa 30000 dòng/request
BOARD_TO_EXCHANGE = {"HSX": "HOSE"}
EXCHANGE_TO_BOARD = {"HOSE": "HSX"}


class OhlcRow(NamedTuple):
    ts: int  # epoch giây
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class MinutePage:
    """Một trang nến phút (cũ dần); `next_to` là con trỏ để chạy tiếp, None nếu hết."""

    bars: list[MinuteBar]
    next_to: int | None


# ----------------------------------------------------------------- parse thuần


def _num(x: Any) -> float | None:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def parse_ohlc_response(payload: Any, symbol: str) -> list[OhlcRow]:
    """Phân tích response gap-chart của một mã; trả các dòng tăng dần theo thời gian.

    `[]` nghĩa là hết dữ liệu. Dòng thiếu/không đọc được bị bỏ, có đếm và log.
    """
    if not isinstance(payload, list):
        raise SourceParseError(f"VCI: payload phải là list, nhận {type(payload).__name__}")
    if not payload:
        return []
    items = [p for p in payload if isinstance(p, dict) and p.get("symbol") == symbol]
    if not items:
        raise SourceParseError(f"VCI: không có phần tử cho mã {symbol}")
    item = items[0]
    cols = {}
    for key in ("t", "o", "h", "l", "c", "v"):
        arr = item.get(key)
        if not isinstance(arr, list):
            raise SourceParseError(f"VCI {symbol}: thiếu mảng {key!r}")
        cols[key] = arr
    n = len(cols["t"])
    if any(len(a) != n for a in cols.values()):
        raise SourceParseError(f"VCI {symbol}: các mảng t,o,h,l,c,v lệch độ dài")

    rows: list[OhlcRow] = []
    skipped = 0
    for i in range(n):
        ts = _num(cols["t"][i])
        o, h, lo, c, v = (_num(cols[k][i]) for k in ("o", "h", "l", "c", "v"))
        if None in (ts, o, h, lo, c, v):
            skipped += 1
            continue
        rows.append(OhlcRow(int(ts), o, h, lo, c, int(round(v))))  # type: ignore[arg-type]
    if skipped:
        log.warning("VCI %s: bỏ %d/%d dòng không hợp lệ", symbol, skipped, n)
    rows.sort(key=lambda r: r.ts)
    return rows


def rows_to_daily(rows: list[OhlcRow], symbol: str) -> list[DailyBar]:
    out = []
    for r in rows:
        d = datetime.fromtimestamp(r.ts, UTC).astimezone(VN_TZ).date()
        out.append(
            DailyBar(
                symbol, d, r.open, r.high, r.low, r.close, r.volume, None,
                PRICE_UNIT_VND, SOURCE_NAME,
            )
        )  # fmt: skip
    return out


def rows_to_minute(rows: list[OhlcRow], symbol: str) -> list[MinuteBar]:
    return [
        MinuteBar(
            symbol, datetime.fromtimestamp(r.ts, UTC), r.open, r.high, r.low, r.close,
            r.volume, SOURCE_NAME,
        )
        for r in rows
    ]  # fmt: skip


def parse_symbols(payload: Any, source: str = SOURCE_NAME) -> list[SymbolInfo]:
    """Phân tích response getAll (HSX được đổi thành HOSE)."""
    if not isinstance(payload, list):
        raise SourceParseError(f"VCI getAll: payload phải là list, nhận {type(payload).__name__}")
    out: list[SymbolInfo] = []
    skipped = 0
    for obj in payload:
        sym = obj.get("symbol") if isinstance(obj, dict) else None
        if not isinstance(sym, str) or not sym.strip():
            skipped += 1
            continue
        board = obj.get("board")
        exchange = BOARD_TO_EXCHANGE.get(board, board) if isinstance(board, str) else None
        icb = obj.get("icbCode2")
        out.append(
            SymbolInfo(
                symbol=sym.strip(),
                exchange=exchange,
                company_name=obj.get("organName") or obj.get("enOrganName") or None,
                icb_code=str(icb) if icb not in (None, "") else None,
                listed_date=None,
                symbol_type=obj.get("type"),
                source=source,
            )
        )
    if skipped:
        log.warning("VCI getAll: bỏ %d/%d mục không có symbol", skipped, len(payload))
    return out


def parse_group(payload: Any) -> list[str]:
    """Phân tích response getByGroup -> danh sách mã (viết hoa, không trùng, giữ thứ tự)."""
    if not isinstance(payload, list):
        raise SourceParseError(
            f"VCI getByGroup: payload phải là list, nhận {type(payload).__name__}"
        )
    out: list[str] = []
    for obj in payload:
        sym = obj.get("symbol") if isinstance(obj, dict) else None
        if isinstance(sym, str) and sym.strip() and sym.strip().upper() not in out:
            out.append(sym.strip().upper())
    return out


# ----------------------------------------------------------------- client


def _epoch(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError("Cần datetime có múi giờ")
    return int(dt.timestamp())


class VciClient(SourceClient):
    name = SOURCE_NAME

    def __init__(self, http: HttpClient | None = None, max_per_second: float = 2.0):
        # Bulk nến phút theo quyết định 3: truyền max_per_second=1.0
        self.http = http or HttpClient(SOURCE_NAME, max_per_second=max_per_second)

    # -- HTTP
    def build_chart_body(self, symbol: str, timeframe: str, to: int, count_back: int) -> dict:
        return {"timeFrame": timeframe, "symbols": [symbol], "to": to, "countBack": count_back}

    def _chart(self, symbol: str, timeframe: str, to: int, count_back: int) -> list[OhlcRow]:
        body = self.build_chart_body(symbol, timeframe, to, count_back)
        headers = {**HEADERS, "Content-Type": "application/json"}
        payload = self.http.post_json(CHART_URL, json=body, headers=headers)
        return parse_ohlc_response(payload, symbol)

    def _pages_back(
        self, symbol: str, timeframe: str, to: int, stop_ts: int, size: int
    ) -> Iterator[tuple[list[OhlcRow], int | None]]:
        """Lùi theo trang; mỗi lần yield (dòng của trang, con trỏ `to` kế tiếp hoặc None)."""
        while True:
            rows = self._chart(symbol, timeframe, to, size)
            if not rows:
                return
            earliest = rows[0].ts
            if earliest >= to:  # không tiến triển
                log.warning("VCI %s %s: con trỏ không lùi (to=%d), dừng", symbol, timeframe, to)
                return
            last_page = earliest <= stop_ts or len(rows) < size
            to = earliest - 1
            yield rows, (None if last_page else to)
            if last_page:
                return

    # -- công khai
    def daily_bars(self, symbol: str, start: date, end: date) -> list[DailyBar]:
        """Nến ngày trong [start, end] (gồm cả hai đầu), tăng dần."""
        if end < start:
            raise ValueError("end < start")
        to = _epoch(datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=UTC))
        start_ts = _epoch(datetime.combine(start, datetime.min.time(), tzinfo=UTC))
        size = min(DAILY_PAGE_SIZE, (end - start).days + 1)
        bars: list[DailyBar] = []
        for rows, _ in self._pages_back(symbol, "ONE_DAY", to, start_ts, size):
            bars.extend(rows_to_daily(rows, symbol))
        bars = [b for b in bars if start <= b.trade_date <= end]
        bars.sort(key=lambda b: b.trade_date)
        return bars

    def minute_pages(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        max_pages: int | None = None,
        resume_to: int | None = None,
        page_size: int = MINUTE_PAGE_SIZE,
    ) -> Iterator[MinutePage]:
        """Các trang nến phút, mới -> cũ. `resume_to` (epoch giây) để chạy tiếp từ con trỏ cũ."""
        start_ts, end_ts = _epoch(start), _epoch(end)
        to = end_ts if resume_to is None else min(resume_to, end_ts)
        pages = self._pages_back(symbol, "ONE_MINUTE", to, start_ts, page_size)
        for n, (rows, nxt) in enumerate(pages, 1):
            bars = rows_to_minute([r for r in rows if start_ts <= r.ts < end_ts], symbol)
            yield MinutePage(bars, nxt)
            if nxt is None or (max_pages is not None and n >= max_pages):
                return

    def minute_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        max_pages: int | None = None,
        resume_to: int | None = None,
    ) -> Iterator[list[MinuteBar]]:
        """Như `minute_pages` nhưng chỉ yield danh sách nến (mới -> cũ giữa các trang)."""
        for page in self.minute_pages(symbol, start, end, max_pages, resume_to):
            yield page.bars

    def list_symbols(
        self, board: str | None = None, symbol_types: tuple[str, ...] | None = ("STOCK",)
    ) -> list[SymbolInfo]:
        """Danh sách mã; `board` nhận HOSE/HSX/HNX/UPCOM/DELISTED, None = mọi sàn."""
        payload = self.http.get_json(SYMBOLS_URL, headers=dict(HEADERS))
        infos = parse_symbols(payload)
        if symbol_types is not None:
            infos = [i for i in infos if i.symbol_type in symbol_types]
        if board is not None:
            want = BOARD_TO_EXCHANGE.get(board.upper(), board.upper())
            infos = [i for i in infos if i.exchange == want]
        return infos

    def list_group(self, group: str) -> list[str]:
        """Mã thuộc rổ `group` (VN30, HNX30, ...) hiện tại, theo thứ tự nguồn trả."""
        payload = self.http.get_json(GROUP_URL, params={"group": group}, headers=dict(HEADERS))
        return parse_group(payload)
