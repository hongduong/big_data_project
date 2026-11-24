import json
from datetime import datetime, timedelta, timezone

import pytest

from stocklake.common.http import HttpError, SourceBlockedError
from stocklake.contracts.topics import INTRADAY_PRICES
from stocklake.sources.base import MinuteBar
from stocklake.sources.vci import MinutePage
from stocklake.streaming.producers.base import serialize
from stocklake.streaming.producers.intraday import (
    IntradayState,
    is_trading_time,
    poll_once,
    run_loop,
    select_new_bars,
)
from stocklake.streaming.producers.replay import ReplayBar

UTC = timezone.utc
# 24/11/2026 (thứ 3) 10:05:30 giờ VN
NOW = datetime(2026, 11, 24, 3, 5, 30, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)
M = 60_000


def _minute(i_from_now: int) -> int:
    """bar_ts của nến mở lúc (phút hiện tại - i)."""
    return NOW_MS - NOW_MS % M - i_from_now * M


class FakeVci:
    def __init__(self, bars_by_symbol, errors=None):
        self.bars = bars_by_symbol
        self.errors = errors or {}
        self.calls = []

    def minute_pages(self, symbol, start, end, max_pages=None, resume_to=None, page_size=30000):
        self.calls.append((symbol, start, end, max_pages, page_size))
        if symbol in self.errors:
            raise self.errors[symbol]
        bars = [
            MinuteBar(symbol, datetime.fromtimestamp(ts / 1000, UTC), 1.0, 2.0, 0.5, 1.5, 10, "vci")
            for ts in self.bars.get(symbol, [])
            if start.timestamp() * 1000 <= ts < end.timestamp() * 1000
        ]
        yield MinutePage(bars[::-1], None)


class FakeProducer:
    def __init__(self, fail_keys=()):
        self.queue = []
        self.sent = []
        self.fail_keys = set(fail_keys)

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.queue.append((topic, key, value, on_delivery))

    def poll(self, timeout=0):
        self.flush()
        return 0

    def flush(self, timeout=None):
        q, self.queue = self.queue, []
        for topic, key, value, cb in q:
            self.sent.append((topic, key, value))
            cb("boom" if key in self.fail_keys else None, None)
        return 0


def _msgs(prod):
    return [json.loads(v) for _, _, v in prod.sent]


def test_select_new_bars_completed_and_newer():
    bars = [ReplayBar("FPT", _minute(i), 1, 1, 1, 1, 1) for i in (0, 1, 2, 3, 2)]
    # nến phút hiện tại (i=0) chưa đóng; last = nến i=3
    out = select_new_bars(bars, _minute(3), NOW_MS)
    assert [b.bar_ts for b in out] == [_minute(2), _minute(1)]
    assert [b.bar_ts for b in select_new_bars(bars, None, NOW_MS)] == [
        _minute(3), _minute(2), _minute(1),
    ]  # fmt: skip
    # Biên: bar_ts + 60 s == now là đã đóng.
    edge = _minute(1)
    assert select_new_bars([ReplayBar("FPT", edge, 1, 1, 1, 1, 1)], None, edge + M)


def test_poll_once_dedup_across_polls():
    client = FakeVci({"FPT": [_minute(i) for i in range(5)], "VNM": [_minute(i) for i in range(3)]})
    prod = FakeProducer()
    state = IntradayState()
    r1 = poll_once(["FPT", "VNM"], state, client=client, producer=prod, now=NOW, wall_ms=lambda: 7)
    assert r1.per_symbol == {"FPT": 4, "VNM": 2}
    assert (r1.sent, r1.delivered, r1.failed) == (6, 6, 0)
    assert state.last_bar_ts == {"FPT": _minute(1), "VNM": _minute(1)}
    msgs = _msgs(prod)
    for m in msgs:
        assert serialize(m, INTRADAY_PRICES)  # đúng contract
        assert m["source"] == "vci" and m["replay_run_id"] is None
        assert m["available_at"] == m["bar_ts"] + M and m["produced_ts"] == 7
    assert client.calls[0][3] == 1 and client.calls[0][4] == 30  # 1 trang, countBack nhỏ

    # Lần sau, cùng thời điểm: không gửi lại gì.
    r2 = poll_once(["FPT", "VNM"], state, client=client, producer=prod, now=NOW)
    assert r2.sent == 0
    # Một phút sau: nến i=0 đã đóng -> chỉ gửi nến đó.
    later = NOW + timedelta(minutes=1)
    client.bars["FPT"].append(_minute(-1))
    r3 = poll_once(["FPT"], state, client=client, producer=prod, now=later)
    assert r3.per_symbol == {"FPT": 1}
    assert _msgs(prod)[-1]["bar_ts"] == _minute(0)


def test_poll_once_failed_delivery_keeps_state():
    client = FakeVci({"FPT": [_minute(i) for i in range(1, 3)], "VNM": [_minute(1)]})
    prod = FakeProducer(fail_keys={b"VNM"})
    state = IntradayState({"VNM": _minute(5)})
    r = poll_once(["FPT", "VNM"], state, client=client, producer=prod, now=NOW)
    assert (r.sent, r.delivered, r.failed) == (3, 2, 1)
    assert state.last_bar_ts == {"FPT": _minute(1), "VNM": _minute(5)}


def test_poll_once_source_errors():
    client = FakeVci({"FPT": [_minute(1)]}, errors={"VNM": HttpError("HTTP 500", 500)})
    prod = FakeProducer()
    r = poll_once(["VNM", "FPT"], IntradayState(), client=client, producer=prod, now=NOW)
    assert r.per_symbol == {"FPT": 1} and "VNM" in r.errors

    blocked = FakeVci({}, errors={"FPT": SourceBlockedError("HTTP 429", 429)})
    with pytest.raises(SourceBlockedError):
        poll_once(["FPT"], IntradayState(), client=blocked, producer=prod, now=NOW)


def test_state_roundtrip_atomic(tmp_path):
    path = tmp_path / "sub" / "state.json"
    IntradayState({"FPT": 123}).save(path)
    assert IntradayState.load(path).last_bar_ts == {"FPT": 123}
    assert IntradayState.load(tmp_path / "missing.json").last_bar_ts == {}
    assert not list(path.parent.glob(".*.tmp"))


@pytest.mark.parametrize(
    "vn, expected",
    [
        ("2026-11-24 08:59", False),
        ("2026-11-24 09:00", True),
        ("2026-11-24 11:31", True),  # grace 2 phút
        ("2026-11-24 11:33", False),
        ("2026-11-24 12:30", False),
        ("2026-11-24 13:00", True),
        ("2026-11-24 14:59", True),
        ("2026-11-24 15:03", False),
        ("2026-11-28 10:00", False),  # thứ 7
    ],
)
def test_is_trading_time(vn, expected):
    t = datetime.strptime(vn, "%Y-%m-%d %H:%M").replace(tzinfo=timezone(timedelta(hours=7)))
    assert is_trading_time(t) is expected


def test_run_loop_once_and_trading_hours(tmp_path):
    state_path = tmp_path / "state.json"
    client = FakeVci({"FPT": [_minute(i) for i in range(3)]})
    prod = FakeProducer()
    r = run_loop(["FPT"], once=True, state_path=state_path, client=client, producer=prod,
                 now_fn=lambda: NOW)  # fmt: skip
    assert r.sent == 2
    assert json.loads(state_path.read_text())["last_bar_ts"]["FPT"] == _minute(1)

    # Ngoài giờ: không gọi nguồn, chỉ ngủ.
    sleeps = []
    night = datetime(2026, 11, 24, 14, 0, tzinfo=UTC)  # 21:00 VN
    calls_before = len(client.calls)
    run_loop(["FPT"], state_path=state_path, client=client, producer=prod,
             now_fn=lambda: night, sleep=sleeps.append, max_iterations=3)  # fmt: skip
    assert len(client.calls) == calls_before and len(sleeps) == 3
