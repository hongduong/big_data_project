import json
from datetime import date, datetime, timedelta, timezone

import pytest

from stocklake.contracts.topics import INTRADAY_PRICES
from stocklake.sources.base import DailyBar, MinuteBar
from stocklake.sources.vci import MinutePage
from stocklake.streaming.producers.base import serialize
from stocklake.streaming.producers.replay import (
    Pacer,
    ReplayBar,
    ReplayResult,
    bar_to_message,
    cache_path,
    encode_message,
    fetch_minute_bars,
    last_trading_days,
    load_bars,
    merge_bars,
    publish_bars,
    replay,
    vn_day_bounds,
)

UTC = timezone.utc
T0 = int(datetime(2026, 11, 20, 2, 15, tzinfo=UTC).timestamp() * 1000)  # 09:15 VN


class FakeClock:
    def __init__(self, t: float = 1000.0):
        self.t = t
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.t += s


class FakeProducer:
    """Giả confluent_kafka.Producer: callback chạy khi poll/flush."""

    def __init__(self, clock=None, buffer_cap=None, fail_keys=(), per_msg_s=0.0):
        self.clock = clock
        self.buffer_cap = buffer_cap
        self.fail_keys = set(fail_keys)
        self.per_msg_s = per_msg_s
        self.queue: list = []
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.polls = 0
        self.buffer_errors = 0

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.buffer_cap is not None and len(self.queue) >= self.buffer_cap:
            self.buffer_errors += 1
            raise BufferError("Local: Queue full")
        if self.clock is not None:
            self.clock.t += self.per_msg_s
        self.queue.append((topic, key, value, on_delivery))

    def _drain(self):
        q, self.queue = self.queue, []
        for topic, key, value, cb in q:
            self.sent.append((topic, key, value))
            err = "boom" if key in self.fail_keys else None
            cb(err, None)

    def poll(self, timeout=0):
        self.polls += 1
        self._drain()
        return 0

    def flush(self, timeout=None):
        self._drain()
        return 0


def _bars(symbol: str, n: int, start: int = T0, step: int = 60_000) -> list[ReplayBar]:
    return [
        ReplayBar(symbol, start + i * step, 100.0 + i, 101.0 + i, 99.0 + i, 100.5 + i, 10 * i)
        for i in range(n)
    ]


# ------------------------------------------------------------------ bản tin


def test_message_matches_contract_and_serialize():
    bar = ReplayBar("FPT", T0, 100.0, 101.0, 99.0, 100.5, 1200)
    msg = bar_to_message(bar, source="replay", produced_ts=T0 + 5, replay_run_id="r1")
    assert list(msg) == list(INTRADAY_PRICES.schema.column_names)
    assert msg["available_at"] == T0 + 60_000
    assert msg["source"] == "replay" and msg["replay_run_id"] == "r1"
    assert encode_message(msg) == serialize(msg, INTRADAY_PRICES)
    assert isinstance(msg["volume"], int) and isinstance(msg["open"], float)


def test_merge_orders_by_bar_ts_then_symbol():
    merged = list(merge_bars({"VNM": _bars("VNM", 3), "FPT": _bars("FPT", 3)[::-1]}))
    assert [(b.bar_ts, b.symbol) for b in merged] == sorted((b.bar_ts, b.symbol) for b in merged)
    assert len(merged) == 6
    assert merged[0].symbol == "FPT"


# ------------------------------------------------------------------ nhịp gửi


@pytest.mark.parametrize("rate", [500, 2000, 5000, 10000])
def test_pacer_hits_target_rate(rate):
    clock = FakeClock()
    pacer = Pacer(rate, clock=clock, sleep=clock.sleep)
    n_total = rate * 3
    sent = 0
    t_start = clock()
    while sent < n_total:
        n = pacer.take()
        sent += n
        clock.t += n * 2e-6  # chi phí gửi giả
    elapsed = clock() - t_start
    assert elapsed == pytest.approx(3.0, rel=0.02)
    # Không ngủ theo từng bản tin ở tốc độ cao.
    if rate >= 5000:
        assert pacer.sleeps < sent / 4


def test_pacer_limits_burst_after_stall():
    clock = FakeClock()
    pacer = Pacer(1000, clock=clock, sleep=clock.sleep, max_lag_s=1.0)
    pacer.take()
    clock.t += 10.0  # treo 10 s
    total = sum(pacer.take() for _ in range(200))
    assert total <= 1000 * 1.0 + 200 * pacer.max_batch


def test_pacer_rejects_bad_rate():
    with pytest.raises(ValueError):
        Pacer(0)


# ------------------------------------------------------------------ publish


def test_publish_counts_and_rate():
    clock = FakeClock()
    prod = FakeProducer(clock=clock, per_msg_s=5e-6)
    bars = list(merge_bars({"FPT": _bars("FPT", 1500), "VNM": _bars("VNM", 1500)}))
    wall = iter(range(10**9, 10**9 + 10**6))
    res = publish_bars(
        bars, prod, rate=2000, run_id="r1", clock=clock, sleep=clock.sleep,
        wall_ms=lambda: next(wall),
    )  # fmt: skip
    assert (res.sent, res.delivered, res.failed) == (3000, 3000, 0)
    assert res.seconds == pytest.approx(1.5, rel=0.03)
    assert res.first_bar_ts == T0 and res.last_bar_ts == T0 + 1499 * 60_000
    msgs = [json.loads(v) for _, _, v in prod.sent]
    assert all(m["replay_run_id"] == "r1" and m["source"] == "replay" for m in msgs)
    assert [m["bar_ts"] for m in msgs] == sorted(m["bar_ts"] for m in msgs)
    assert prod.sent[0][1] == b"FPT"
    produced = [m["produced_ts"] for m in msgs]
    assert produced == sorted(produced)


def test_publish_buffer_error_and_failures():
    clock = FakeClock()
    prod = FakeProducer(clock=clock, buffer_cap=7, fail_keys={b"VNM"})
    bars = list(merge_bars({"FPT": _bars("FPT", 50), "VNM": _bars("VNM", 30)}))
    res = publish_bars(bars, prod, rate=10000, run_id="r2", clock=clock, sleep=clock.sleep)
    assert prod.buffer_errors > 0
    assert (res.sent, res.delivered, res.failed) == (80, 50, 30)


def test_publish_counts_unflushed_as_failed():
    class Stuck(FakeProducer):
        def flush(self, timeout=None):
            return len(self.queue)

        def poll(self, timeout=0):
            return 0

    clock = FakeClock()
    res = publish_bars(
        _bars("FPT", 5), Stuck(), rate=1000, run_id="r3", clock=clock, sleep=clock.sleep,
        flush_timeout_s=0.0,
    )  # fmt: skip
    assert (res.sent, res.delivered, res.failed) == (5, 0, 5)


# ------------------------------------------------------------------ nạp nến + cache


class FakeVci:
    def __init__(self, bars_by_symbol=None, daily_dates=()):
        self.bars = bars_by_symbol or {}
        self.daily_dates = list(daily_dates)
        self.calls: list[tuple] = []

    def minute_pages(self, symbol, start, end, max_pages=None, resume_to=None, page_size=30000):
        self.calls.append(("minute", symbol, start, end, page_size))
        bars = [
            MinuteBar(symbol, datetime.fromtimestamp(b.bar_ts / 1000, UTC), b.open, b.high,
                      b.low, b.close, b.volume, "vci")
            for b in self.bars.get(symbol, [])
            if start.timestamp() * 1000 <= b.bar_ts < end.timestamp() * 1000
        ]  # fmt: skip
        yield MinutePage(bars[::-1], None)

    def daily_bars(self, symbol, start, end):
        self.calls.append(("daily", symbol, start, end))
        return [
            DailyBar(symbol, d, 1, 1, 1, 1, 1, None, "vnd", "vci")
            for d in self.daily_dates
            if start <= d <= end
        ]


def test_vn_day_bounds():
    lo, hi = vn_day_bounds(date(2026, 11, 20), date(2026, 11, 20))
    assert lo == datetime(2026, 11, 19, 17, 0, tzinfo=UTC)
    assert hi - lo == timedelta(days=1)


def test_fetch_filters_sorts_dedups():
    bars = _bars("FPT", 3) + _bars("FPT", 1)  # trùng nến đầu
    bars.append(ReplayBar("FPT", T0 + 3 * 86_400_000, 1, 1, 1, 1, 1))  # ngoài khoảng
    client = FakeVci({"FPT": bars})
    out = fetch_minute_bars(client, "FPT", date(2026, 11, 20), date(2026, 11, 20))
    assert [b.bar_ts for b in out] == [T0, T0 + 60_000, T0 + 120_000]
    assert client.calls[0][4] == 400


def test_load_bars_uses_cache_second_time(tmp_path):
    client = FakeVci({"FPT": _bars("FPT", 4), "VNM": _bars("VNM", 2)})
    d = date(2026, 11, 20)
    first = load_bars(["FPT", "VNM"], d, d, client=client, cache_dir=tmp_path)
    assert len(client.calls) == 2
    assert cache_path(tmp_path, "FPT", d, d).name == "FPT_2026-11-20_2026-11-20.jsonl"
    again = load_bars(["FPT", "VNM"], d, d, client=client, cache_dir=tmp_path)
    assert len(client.calls) == 2  # không gọi lại nguồn
    assert again == first
    assert not list(tmp_path.glob(".*.tmp"))


def test_blocked_source_propagates(tmp_path):
    from stocklake.common.http import SourceBlockedError

    class Blocked(FakeVci):
        def minute_pages(self, *a, **k):
            raise SourceBlockedError("HTTP 429", 429)

    with pytest.raises(SourceBlockedError):
        load_bars(["FPT"], date(2026, 11, 20), date(2026, 11, 20), client=Blocked(),
                  cache_dir=tmp_path)  # fmt: skip
    assert not list(tmp_path.iterdir())


def test_last_trading_days_excludes_open_session():
    days = [date(2026, 11, d) for d in (16, 17, 18, 19, 20, 23, 24)]
    client = FakeVci(daily_dates=days)
    # 24/11 10:00 VN: phiên chưa đóng -> bỏ ngày 24.
    now = datetime(2026, 11, 24, 3, 0, tzinfo=UTC)
    assert last_trading_days(client, 5, now) == (date(2026, 11, 17), date(2026, 11, 23))
    # 24/11 16:00 VN: đã đóng.
    now = datetime(2026, 11, 24, 9, 0, tzinfo=UTC)
    assert last_trading_days(client, 5, now) == (date(2026, 11, 18), date(2026, 11, 24))
    with pytest.raises(RuntimeError):
        last_trading_days(client, 30, now)


def test_replay_end_to_end_with_fakes(tmp_path):
    client = FakeVci({"FPT": _bars("FPT", 20), "VNM": _bars("VNM", 10, start=T0 + 30_000)})
    clock = FakeClock()
    prod = FakeProducer(clock=clock)
    d = date(2026, 11, 20)
    res = replay(
        ["FPT", "VNM"], d, d, rate=500, client=client, producer=prod, cache_dir=tmp_path,
        clock=clock, sleep=clock.sleep,
    )  # fmt: skip
    assert isinstance(res, ReplayResult)
    assert (res.sent, res.delivered, res.failed) == (30, 30, 0)
    assert res.run_id.startswith("r")
    out = json.loads(res.to_json())
    assert set(out) == {
        "run_id", "sent", "delivered", "failed", "seconds", "first_bar_ts", "last_bar_ts",
    }  # fmt: skip
    keys = {(json.loads(v)["symbol"], json.loads(v)["bar_ts"]) for _, _, v in prod.sent}
    assert len(keys) == 30
