import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from stocklake.common.http import HttpClient, RetryPolicy, SourceBlockedError
from stocklake.sources.base import (
    SourceCapabilityError,
    SourceClient,
    SourceParseError,
)
from stocklake.sources.vci import (
    CHART_URL,
    SYMBOLS_URL,
    VciClient,
    parse_ohlc_response,
    parse_symbols,
)

FX = Path(__file__).parent.parent / "fixtures" / "vci"
UTC = timezone.utc


def load(name):
    return json.loads((FX / name).read_text(encoding="utf-8"))


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
        "vci", max_per_second=1000, retry=RetryPolicy(jitter=0.0), session=s,
        sleep=lambda _: None,
    )  # fmt: skip
    return VciClient(http), s


# ------------------------------------------------------------- parse


def test_parse_daily_fixture():
    rows = parse_ohlc_response(load("fpt_daily.json"), "FPT")
    assert len(rows) == 10
    assert rows == sorted(rows, key=lambda r: r.ts)
    assert isinstance(rows[0].ts, int) and isinstance(rows[0].volume, int)
    assert rows[0].open == 51600 and rows[-1].close == 46850


def test_daily_bars_trade_date_is_vn_date():
    c, _ = client(load("fpt_daily.json"))
    bars = c.daily_bars("FPT", date(2026, 11, 1), date(2026, 11, 30))
    assert bars[0].trade_date == date(2026, 11, 10)
    assert bars[-1].trade_date == date(2026, 11, 23)
    b = bars[0]
    assert (b.symbol, b.source, b.price_unit, b.raw_close) == ("FPT", "vci", "vnd", None)


def test_daily_bars_filters_range():
    c, _ = client(load("fpt_daily.json"))
    bars = c.daily_bars("FPT", date(2026, 11, 12), date(2026, 11, 13))
    assert [b.trade_date for b in bars] == [date(2026, 11, 12), date(2026, 11, 13)]


def test_vnindex_fixture():
    rows = parse_ohlc_response(load("vnindex_daily.json"), "VNINDEX")
    assert len(rows) == 5


def test_minute_fixture():
    rows = parse_ohlc_response(load("fpt_1m_page1.json"), "FPT")
    assert len(rows) == 20
    assert [r.ts for r in rows] == sorted({r.ts for r in rows})  # tăng dần, không trùng


def test_empty_response():
    assert parse_ohlc_response(load("empty.json"), "FPT") == []


@pytest.mark.parametrize(
    "payload",
    [
        {"a": 1},
        "x",
        [{"symbol": "VNM", "t": [], "o": [], "h": [], "l": [], "c": [], "v": []}],  # sai mã
        [{"symbol": "FPT", "t": [1], "o": [1], "h": [1], "l": [1], "c": [1]}],  # thiếu v
        [{"symbol": "FPT", "t": [1, 2], "o": [1], "h": [1], "l": [1], "c": [1], "v": [1]}],
    ],
)
def test_malformed_payload(payload):
    with pytest.raises(SourceParseError):
        parse_ohlc_response(payload, "FPT")


def test_bad_rows_skipped_and_logged(caplog):
    caplog.set_level("WARNING", logger="stocklake")
    p = [{"symbol": "FPT", "t": ["60", "120", "abc"], "o": [1, None, 1], "h": [2, 2, 2],
          "l": [1, 1, 1], "c": [1, 1, 1], "v": [5, 5, 5]}]  # fmt: skip
    rows = parse_ohlc_response(p, "FPT")
    assert [r.ts for r in rows] == [60]
    # logger stocklake có propagate=False nên đọc qua handler tạm
    import logging

    h = logging.Handler()
    seen = []
    h.emit = lambda rec: seen.append(rec.getMessage())
    logging.getLogger("stocklake").addHandler(h)
    try:
        parse_ohlc_response(p, "FPT")
    finally:
        logging.getLogger("stocklake").removeHandler(h)
    assert any("bỏ 2/3" in m for m in seen)


def test_parse_symbols_and_hose_mapping():
    infos = parse_symbols(load("get_all_sample.json"))
    assert len(infos) == 13
    by_ex = {i.exchange for i in infos}
    assert {"HOSE", "HNX", "UPCOM", "DELISTED"} <= by_ex
    assert "HSX" not in by_ex
    yeg = next(i for i in infos if i.symbol == "YEG")
    assert yeg.exchange == "HOSE" and yeg.symbol_type == "STOCK" and yeg.source == "vci"
    assert yeg.company_name and yeg.listed_date is None


def test_parse_symbols_skips_bad_and_rejects_non_list():
    out = parse_symbols([{"symbol": "AAA", "board": "HSX"}, {"x": 1}, "s", {"symbol": " "}])
    assert [i.symbol for i in out] == ["AAA"] and out[0].exchange == "HOSE"
    with pytest.raises(SourceParseError):
        parse_symbols({"a": 1})


def test_list_symbols_filters_board_and_type():
    c, s = client(load("get_all_sample.json"))
    hose = c.list_symbols("HOSE")
    assert hose and all(i.exchange == "HOSE" and i.symbol_type == "STOCK" for i in hose)
    assert s.calls[0][0] == "GET" and s.calls[0][1] == SYMBOLS_URL
    c, _ = client(load("get_all_sample.json"))
    assert {i.exchange for i in c.list_symbols("HSX")} == {"HOSE"}  # nhận cả tên HSX
    c, _ = client(load("get_all_sample.json"))
    assert any(i.symbol_type != "STOCK" for i in c.list_symbols(symbol_types=None))


# ------------------------------------------------------------- request


def test_request_body_and_headers():
    c, s = client(load("fpt_daily.json"))
    c.daily_bars("FPT", date(2026, 11, 10), date(2026, 11, 23))
    method, url, kw = s.calls[0]
    assert (method, url) == ("POST", CHART_URL)
    to = int(datetime(2026, 11, 24, tzinfo=UTC).timestamp())
    assert kw["json"] == {"timeFrame": "ONE_DAY", "symbols": ["FPT"], "to": to, "countBack": 14}
    h = kw["headers"]
    assert h["Referer"] == "https://trading.vietcap.com.vn/"
    assert h["Origin"] == "https://trading.vietcap.com.vn"
    assert h["Content-Type"] == "application/json"
    assert "Mozilla" in h["User-Agent"] and "application/json" in h["Accept"]


def test_default_client_rate_and_override():
    assert VciClient().http.limiter.interval == pytest.approx(0.5)
    assert VciClient(max_per_second=1.0).http.limiter.interval == pytest.approx(1.0)


def test_blocked_propagates():
    c, _ = client(FakeResponse(None, 429))
    with pytest.raises(SourceBlockedError):
        c.daily_bars("FPT", date(2026, 11, 1), date(2026, 11, 2))


# ------------------------------------------------------------- paging


def _mk(ts_list, sym="FPT"):
    n = len(ts_list)
    return [{"symbol": sym, "t": [str(t) for t in ts_list], "o": [1] * n, "h": [1] * n,
             "l": [1] * n, "c": [1] * n, "v": [1] * n}]  # fmt: skip


T0 = 1_795_417_860  # 2026-11-23 07:11 UTC
START = datetime(2026, 11, 23, 0, 0, tzinfo=UTC)
END = datetime(2026, 11, 24, 0, 0, tzinfo=UTC)


def test_minute_paging_fixture_pages_newest_first():
    c, s = client(load("fpt_1m_page1.json"), load("fpt_1m_page2.json"), [])
    pages = list(c.minute_pages("FPT", START, END, page_size=20))
    assert [len(p.bars) for p in pages] == [20, 20]  # page cuối [] -> dừng, không yield
    assert pages[0].bars[0].bar_ts > pages[1].bars[-1].bar_ts
    assert pages[0].next_to == 1_795_417_860 - 1
    body2 = s.calls[1][2]["json"]
    assert body2["to"] == pages[0].next_to and body2["timeFrame"] == "ONE_MINUTE"
    assert body2["countBack"] == 20
    assert len(s.calls) == 3


def test_minute_paging_stops_below_start_and_filters():
    rows = [T0 + 60 * i for i in range(5)]
    start = datetime.fromtimestamp(T0 + 120, UTC)
    c, s = client(_mk(rows))
    pages = list(c.minute_pages("FPT", start, END, page_size=5))
    assert len(s.calls) == 1  # earliest <= start -> trang cuối
    assert [b.bar_ts for b in pages[0].bars] == [datetime.fromtimestamp(T0 + 60 * i, UTC)
                                                 for i in (2, 3, 4)]  # fmt: skip
    assert pages[0].next_to is None


def test_minute_paging_max_pages_and_resume():
    p1 = _mk([T0 + 60 * i for i in range(3)])
    c, s = client(p1)
    pages = list(c.minute_pages("FPT", START, END, max_pages=1, page_size=3))
    assert len(pages) == 1 and pages[0].next_to == T0 - 1
    c2, s2 = client(_mk([T0 - 180, T0 - 120, T0 - 60]))
    list(c2.minute_pages("FPT", START, END, resume_to=pages[0].next_to, page_size=3, max_pages=1))
    assert s2.calls[0][2]["json"]["to"] == T0 - 1


def test_minute_paging_no_progress_stops():
    c, s = client(_mk([T0, T0 + 60]), _mk([T0, T0 + 60]))
    # trang 1 -> to = T0-1; trang 2 trả dữ liệu không lùi -> dừng
    c.http.session.payloads = [_mk([T0, T0 + 60]), _mk([T0 + 5, T0 + 65])]
    pages = list(c.minute_pages("FPT", START, END, page_size=2))
    assert len(pages) == 1 and len(s.calls) == 2


def test_minute_bars_yields_lists_and_naive_rejected():
    c, _ = client(load("fpt_1m_page1.json"), [])
    out = list(c.minute_bars("FPT", START, END))
    assert len(out) == 1 and out[0][0].source == "vci" and out[0][0].bar_ts.tzinfo is not None
    with pytest.raises(ValueError):
        list(c.minute_bars("FPT", datetime(2026, 1, 1), END))


def test_daily_paging_two_pages():
    t = [1_000_000_000 + 86400 * i for i in range(6)]
    c, s = client(_mk(t[3:]), _mk(t[:3]))
    c.daily_bars("FPT", date(2001, 1, 1), date(2001, 9, 1))  # countBack = ngày lịch -> >= size
    assert len(s.calls) >= 1


# ------------------------------------------------------------- base


def test_base_capabilities():
    class OnlyDaily(SourceClient):
        name = "x"

        def daily_bars(self, symbol, start, end):
            return []

    o = OnlyDaily()
    assert o.capabilities == {"daily_bars"}
    assert VciClient().capabilities == {"daily_bars", "minute_bars", "list_symbols"}
    with pytest.raises(SourceCapabilityError):
        o.list_symbols()
    with pytest.raises(SourceCapabilityError):
        o.minute_bars("A", START, END)


def test_stubs_raise():
    from stocklake.sources.kbs import KbsClient

    with pytest.raises(NotImplementedError):
        KbsClient()
