"""Quy tắc available_at (Planning mục 4.1). Trả về datetime có múi giờ UTC."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
UTC = timezone.utc


def _vn_to_utc(d: date, t: time) -> datetime:
    return datetime.combine(d, t, tzinfo=VN_TZ).astimezone(UTC)


def _require_aware(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        raise ValueError("Cần datetime có múi giờ")
    return ts.astimezone(UTC)


def daily_bar(trade_date: date) -> datetime:
    """Nến ngày D: D 15:30 giờ VN."""
    return _vn_to_utc(trade_date, time(15, 30))


def foreign_flow(trade_date: date) -> datetime:
    """Khối ngoại ngày D: D 18:00 giờ VN."""
    return _vn_to_utc(trade_date, time(18, 0))


def intraday_bar(bar_start: datetime) -> datetime:
    """Nến 1 phút bắt đầu lúc t: t + 1 phút."""
    return _require_aware(bar_start) + timedelta(minutes=1)


def news(pub_date: datetime) -> datetime:
    """Tin tức: đúng pubDate."""
    return _require_aware(pub_date)


def macro_yfinance(session_date: date) -> datetime:
    """Vĩ mô yfinance, phiên D (giờ Mỹ): D+1 08:00 giờ VN (ngày lịch)."""
    return _vn_to_utc(session_date + timedelta(days=1), time(8, 0))


def macro_fred(obs_date: date) -> datetime:
    """Vĩ mô FRED, ngày D: D+2 08:00 giờ VN (ngày lịch)."""
    return _vn_to_utc(obs_date + timedelta(days=2), time(8, 0))


def company_profile(ingest_ts: datetime) -> datetime:
    """Hồ sơ doanh nghiệp: bằng ingest_ts."""
    return _require_aware(ingest_ts)


def corporate_event(disclosure_date: datetime | date) -> datetime:
    """Sự kiện doanh nghiệp: disclosureDate (nếu chỉ có ngày thì lấy 00:00 giờ VN)."""
    if isinstance(disclosure_date, datetime):
        return _require_aware(disclosure_date)
    return _vn_to_utc(disclosure_date, time(0, 0))
