"""Giao diện chung cho client nguồn dữ liệu và các bản ghi đã phân tích.

Bản ghi ở đây chưa có metadata nạp (`ingest_ts`, `available_at`); collector thêm sau.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from datetime import date, datetime

PRICE_UNIT_VND = "vnd"  # đồng
PRICE_UNIT_KVND = "kvnd"  # nghìn đồng (VNDirect)


class SourceError(Exception):
    """Gốc của lỗi riêng của lớp sources."""


class SourceCapabilityError(SourceError):
    """Nguồn không hỗ trợ thao tác được gọi."""


class SourceParseError(SourceError):
    """Payload trả về không đúng cấu trúc mong đợi."""


@dataclass(frozen=True)
class DailyBar:
    """Nến ngày; khớp raw.daily_ohlcv (trừ ingest_ts, available_at)."""

    symbol: str
    trade_date: date  # ngày theo giờ VN
    open: float
    high: float
    low: float
    close: float
    volume: int
    raw_close: float | None
    price_unit: str
    source: str


@dataclass(frozen=True)
class MinuteBar:
    """Nến 1 phút; khớp intraday_prices (trừ các cột Kafka/metadata)."""

    symbol: str
    bar_ts: datetime  # UTC, thời điểm mở nến
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str


@dataclass(frozen=True)
class SymbolInfo:
    """Thông tin mã; khớp raw.company_profile (trừ snapshot_date, metadata)."""

    symbol: str
    exchange: str | None  # HOSE, HNX, UPCOM, DELISTED
    company_name: str | None
    icb_code: str | None
    listed_date: date | None
    symbol_type: str | None  # STOCK, ETF, ...
    source: str


class SourceClient(ABC):  # noqa: B024 - không có abstract method cố ý
    """Client nguồn. Lớp con ghi đè những khả năng nó có; phần còn lại báo lỗi."""

    name: str = ""

    def daily_bars(self, symbol: str, start: date, end: date) -> list[DailyBar]:
        raise SourceCapabilityError(f"{self.name}: không hỗ trợ daily_bars")

    def minute_bars(self, symbol: str, start: datetime, end: datetime):
        raise SourceCapabilityError(f"{self.name}: không hỗ trợ minute_bars")

    def list_symbols(self, board: str | None = None) -> list[SymbolInfo]:
        raise SourceCapabilityError(f"{self.name}: không hỗ trợ list_symbols")

    @property
    def capabilities(self) -> frozenset[str]:
        """Tập khả năng thật sự được cài (lớp con có ghi đè)."""
        out = set()
        for cap in ("daily_bars", "minute_bars", "list_symbols"):
            if getattr(type(self), cap) is not getattr(SourceClient, cap):
                out.add(cap)
        return frozenset(out)
