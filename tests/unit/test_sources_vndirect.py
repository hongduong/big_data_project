import json
from datetime import date
from pathlib import Path

import pytest

from stocklake.common.http import HttpClient, RetryPolicy, SourceBlockedError
from stocklake.sources.base import PRICE_UNIT_KVND, SourceParseError
from stocklake.sources.vndirect import (
    PRICES_URL,
    VndirectClient,
    parse_stock_prices_page,
)

FX = Path(__file__).parent.parent / "fixtures" / "vndirect"
VCI_FX = Path(__file__).parent.parent / "fixtures" / "vci"


def load(name, base=FX):
    return json.loads((base / name).read_text(encoding="utf-8"))


class FakeResponse:
    def __init__(self, payload, status=200):
        self.status_code = status
        self._p = payload

    def json(self):
        return self._p


class FakeSession:
    def __init__(self, *payloads):
        self.payloads = list(payloads)
        self.calls = []

    def request(self, method, url, **kw):
        self.calls.append((method, url, kw))
        p = self.payloads.pop(0)
        return p if isinstance(p, FakeResponse) else FakeResponse(p)


def client(*payloads):
    s = FakeSession(*payloads)
    http = HttpClient(
        "vndirect", max_per_second=1000, retry=RetryPolicy(jitter=0.0), session=s,
        sleep=lambda _: None,
    )  # fmt: skip
    return VndirectClient(http), s


# ------------------------------------------------------------- parse


def test_parse_maps_adjusted_and_raw():
    bars = parse_stock_prices_page(load("fpt_2013_page1.json"), "FPT")
    assert len(bars) == 5
    b = bars[0]
    assert b.symbol == "FPT" and b.source == "vndirect" and b.price_unit == PRICE_UNIT_KVND
    assert b.trade_date == date.fromisoformat(load("fpt_2013_page1.json")["data"][0]["date"])
    row = load("fpt_2013_page1.json")["data"][0]
    assert (b.open, b.high, b.low, b.close) == (
        row["adOpen"], row["adHigh"], row["adLow"], row["adClose"],
    )  # fmt: skip
    assert b.raw_close == row["close"] and b.raw_close != b.close  # 2013: đã điều chỉnh khác gốc
    assert isinstance(b.volume, int) and b.volume == int(row["nmVolume"])


def test_volume_is_matched_only_and_equals_vci():
    """VCI volume == nmVolume (không cộng ptVolume) trên 10 phiên FPT 11/2026."""
    vnd = {b.trade_date: b for b in parse_stock_prices_page(load("fpt_daily_nov2026.json"))}
    vci = load("fpt_daily.json", VCI_FX)[0]
    from datetime import datetime, timezone

    from stocklake.contracts.available_at import VN_TZ

    n = 0
    for t, c, v in zip(vci["t"], vci["c"], vci["v"], strict=True):
        d = datetime.fromtimestamp(int(t), timezone.utc).astimezone(VN_TZ).date()
        if d in vnd:
            n += 1
            assert vnd[d].volume == int(v)
            assert vnd[d].close * 1000 == pytest.approx(float(c))  # kvnd -> đồng
    assert n == 10


def test_parse_empty_and_bad_shape():
    assert parse_stock_prices_page(load("empty.json")) == []
    assert parse_stock_prices_page(load("vnindex_daily.json")) == []
    for bad in (None, [], {"data": None}, {"x": 1}, {"data": [1]}):
        with pytest.raises(SourceParseError):
            parse_stock_prices_page(bad)


def test_parse_skips_bad_rows():
    row = dict(load("fpt_2013_page1.json")["data"][0])
    bad_date = {**row, "date": "xx"}
    no_price = {**row, "adClose": None}
    other = {**row, "code": "VNM"}
    out = parse_stock_prices_page({"data": [bad_date, no_price, other, row]}, "FPT")
    assert len(out) == 1


# ------------------------------------------------------------- client


def test_paging_uses_total_pages_from_first_page():
    p1, p2 = load("fpt_2013_page1.json"), load("fpt_2013_page2.json")
    assert "totalPages" not in p2
    last = {"data": [], "currentPage": 3, "size": 5}
    c, s = client(p1, p2, last)
    # p1 báo totalPages=5 nên phải tiếp tục tới khi gặp trang rỗng
    bars = c.daily_bars("FPT", date(2013, 1, 1), date(2013, 1, 31))
    assert len(bars) == 10
    assert [b.trade_date for b in bars] == sorted(b.trade_date for b in bars)
    assert len(s.calls) == 3
    params = [call[2]["params"] for call in s.calls]
    assert [p["page"] for p in params] == [1, 2, 3]
    assert params[0]["q"] == "code:FPT~date:gte:2013-01-01~date:lte:2013-01-31"
    assert params[0]["sort"] == "date:asc"
    assert s.calls[0][1] == PRICES_URL
    assert s.calls[0][2]["headers"]["Referer"].startswith("https://dstock.vndirect.com.vn")


def test_single_page_stops_at_total_pages():
    c, s = client(load("fpt_daily_nov2026.json"))
    bars = c.daily_bars("FPT", date(2026, 11, 10), date(2026, 11, 23))
    assert len(bars) == 10 and len(s.calls) == 1


def test_stops_on_short_page_without_totals():
    row = load("fpt_2013_page1.json")["data"][0]
    c, s = client({"data": [row], "currentPage": 1, "size": 500})
    assert len(c.daily_bars("FPT", date(2013, 1, 1), date(2013, 1, 31))) == 1
    assert len(s.calls) == 1


def test_index_returns_empty():
    c, _ = client(load("vnindex_daily.json"))
    assert c.daily_bars("VNINDEX", date(2026, 11, 17), date(2026, 11, 23)) == []


def test_range_filter_and_blocked():
    c, _ = client(load("fpt_daily_nov2026.json"))
    bars = c.daily_bars("FPT", date(2026, 11, 12), date(2026, 11, 13))
    assert [b.trade_date.day for b in bars] == [12, 13]
    c, _ = client(FakeResponse({}, 429))
    with pytest.raises(SourceBlockedError):
        c.daily_bars("FPT", date(2026, 11, 1), date(2026, 11, 2))
    with pytest.raises(ValueError):
        c.daily_bars("FPT", date(2026, 11, 2), date(2026, 11, 1))


def test_capabilities():
    c, _ = client()
    assert c.capabilities == frozenset({"daily_bars"})
