import ast
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from stocklake.contracts.topics import INTRADAY_PRICES
from stocklake.streaming.producers import replay as replay_mod
from stocklake.streaming.producers.base import serialize
from stocklake.streaming.producers.replay import (
    HdfsHistory,
    HistoryUnavailableError,
    Pacer,
    ReplayBar,
    bar_to_message,
    day_filters,
    dedup_sort,
    encode_message,
    last_trade_dates,
    merge_bars,
    publish_bars,
    replay,
    run_replay,
    vn_day_bounds,
    weekdays,
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


# ------------------------------------------------------------------ chọn ngày, filter, gộp


def _day_bars(day: date, symbols=("FPT", "VNM"), n=10) -> list[ReplayBar]:
    lo, _ = vn_day_bounds(day, day)
    start = int(lo.timestamp() * 1000) + 9 * 3_600_000
    out = []
    for sym in symbols:
        out += _bars(sym, n, start=start)
    return dedup_sort((b, "vci") for b in out)


class FakeHistory:
    """Lịch sử giả: {ngày: nến}; ghi lại các lần đọc."""

    def __init__(self, days: dict[date, list[ReplayBar]]):
        self.days = days
        self.reads: list[date] = []

    def read_day(self, symbols, day):
        self.reads.append(day)
        return [b for b in self.days.get(day, []) if b.symbol in symbols]

    def read_year_dates(self, symbols, year):
        return {d for d, bars in self.days.items() if d.year == year and bars}


def test_vn_day_bounds_utc():
    lo, hi = vn_day_bounds(date(2026, 11, 20), date(2026, 11, 20))
    assert lo == datetime(2026, 11, 19, 17, 0, tzinfo=UTC)
    assert hi == datetime(2026, 11, 20, 17, 0, tzinfo=UTC)
    assert lo.tzinfo is not None  # aware: khớp cột timestamp có tz


def test_day_filters_output():
    f = day_filters(["VNM", "FPT", "FPT"], date(2026, 11, 20))
    assert f == [
        ("year", "=", 2026),
        ("symbol", "in", ["FPT", "VNM"]),
        ("bar_ts", ">=", datetime(2026, 11, 19, 17, 0, tzinfo=UTC)),
        ("bar_ts", "<", datetime(2026, 11, 20, 17, 0, tzinfo=UTC)),
    ]
    assert isinstance(f[0][2], int)


def test_day_filters_year_is_vn_year():
    # 01/01 VN bắt đầu từ 31/12 17:00 UTC nhưng year vẫn là năm VN.
    f = day_filters(["FPT"], date(2027, 1, 1))
    assert f[0] == ("year", "=", 2027)
    assert f[2][2] == datetime(2026, 12, 31, 17, 0, tzinfo=UTC)


def test_weekdays_skips_weekend():
    assert weekdays(date(2026, 11, 20), date(2026, 11, 24)) == [
        date(2026, 11, 20),
        date(2026, 11, 23),
        date(2026, 11, 24),
    ]


def test_dedup_prefers_vci_and_orders():
    a = ReplayBar("FPT", T0, 1, 1, 1, 1, 1)
    b = ReplayBar("FPT", T0, 2, 2, 2, 2, 2)
    c = ReplayBar("ACB", T0, 3, 3, 3, 3, 3)
    d = ReplayBar("FPT", T0 - 60_000, 4, 4, 4, 4, 4)
    out = dedup_sort([(b, "vndirect"), (a, "vci"), (c, "x"), (d, "x")])
    assert out == [d, c, a]
    assert dedup_sort([(a, "vci"), (b, "vndirect")]) == [a]


def test_last_trade_dates_from_fake_history():
    h = FakeHistory({date(2026, 11, d): [object()] for d in (16, 17, 18, 19, 20, 23)})
    h.days[date(2025, 12, 30)] = [object()]
    got = last_trade_dates(h.read_year_dates, ["FPT"], 3, today=date(2026, 11, 24))
    assert got == [date(2026, 11, 19), date(2026, 11, 20), date(2026, 11, 23)]
    # Thiếu trong năm nay -> lùi sang năm trước.
    got = last_trade_dates(h.read_year_dates, ["FPT"], 7, today=date(2026, 11, 24))
    assert got[0] == date(2025, 12, 30) and len(got) == 7
    # Ít hơn N: trả những gì có.
    assert len(last_trade_dates(h.read_year_dates, ["FPT"], 50, today=date(2026, 11, 24))) == 7
    with pytest.raises(HistoryUnavailableError):
        last_trade_dates(lambda s, y: set(), ["FPT"], 3, today=date(2026, 11, 24))
    with pytest.raises(ValueError):
        last_trade_dates(h.read_year_dates, ["FPT"], 0)


# ------------------------------------------------------------------ replay nhiều ngày


def test_replay_days_order_pacing_and_result():
    d1, d2, d3 = date(2026, 11, 19), date(2026, 11, 20), date(2026, 11, 23)
    hist = FakeHistory({d1: _day_bars(d1), d3: _day_bars(d3)})  # d2 không có nến
    clock = FakeClock()
    prod = FakeProducer(clock=clock, per_msg_s=5e-6)
    res = replay(
        ["FPT", "VNM"], [d1, d2, d3], rate=10, run_id="rX", read_day=hist.read_day,
        producer=prod, clock=clock, sleep=clock.sleep,
    )  # fmt: skip
    assert hist.reads == [d1, d2, d3]  # từng ngày một
    assert (res.sent, res.delivered, res.failed) == (40, 40, 0)
    assert (res.days, res.symbols, res.run_id) == (2, 2, "rX")
    # Nhịp liên tục qua ranh giới ngày: 40 bản tin ở 10 msg/s ~ 4 s, không reset theo ngày.
    assert res.seconds == pytest.approx(4.0, rel=0.05)
    msgs = [json.loads(v) for _, _, v in prod.sent]
    keys = [(m["bar_ts"], m["symbol"]) for m in msgs]
    assert keys[:20] == sorted(keys[:20]) and keys[20:] == sorted(keys[20:])
    assert max(k[0] for k in keys[:20]) < min(k[0] for k in keys[20:])
    assert {m["replay_run_id"] for m in msgs} == {"rX"}
    assert all(m["source"] == "replay" for m in msgs)
    out = json.loads(res.to_json())
    assert {"run_id", "sent", "delivered", "failed", "days", "symbols"} <= set(out)


def test_replay_reads_lazily_one_day_at_a_time():
    d1, d2 = date(2026, 11, 19), date(2026, 11, 20)
    hist = FakeHistory({d1: _day_bars(d1), d2: _day_bars(d2)})
    seen_at_send: list[int] = []

    class Spy(FakeProducer):
        def produce(self, *a, **k):
            seen_at_send.append(len(hist.reads))
            super().produce(*a, **k)

    clock = FakeClock()
    replay(["FPT", "VNM"], [d1, d2], rate=1000, read_day=hist.read_day, producer=Spy(),
           clock=clock, sleep=clock.sleep)  # fmt: skip
    assert seen_at_send[0] == 1 and seen_at_send[-1] == 2  # ngày 2 chỉ đọc sau khi hết ngày 1


def test_replay_no_bars_raises_before_producer():
    hist = FakeHistory({})
    with pytest.raises(HistoryUnavailableError, match="intraday_history.py"):
        replay(["FPT"], [date(2026, 11, 20)], read_day=hist.read_day, producer=object())


def test_hdfs_history_missing_table_message():
    class NoFs:
        def exists(self, path):
            return False

    pytest.importorskip("pyarrow")
    with pytest.raises(HistoryUnavailableError, match="intraday_history.py"):
        HdfsHistory(fs=NoFs()).read_day(["FPT"], date(2026, 11, 20))


def test_table_helpers_with_pyarrow():
    pa = pytest.importorskip("pyarrow")
    from stocklake.streaming.producers.replay import table_to_rows, table_to_trade_dates

    utc = timezone.utc
    # 2026-11-20 16:59 UTC = 2026-11-20 23:59 VN; 17:00 UTC = 2026-11-21 00:00 VN
    ts = [datetime(2026, 11, 20, 2, 15, tzinfo=utc), datetime(2026, 11, 20, 16, 59, tzinfo=utc),
          datetime(2026, 11, 20, 17, 0, tzinfo=utc)]  # fmt: skip
    t = pa.table({
        "symbol": ["FPT"] * 3, "bar_ts": pa.array(ts, pa.timestamp("us", tz="UTC")),
        "open": [1.0] * 3, "high": [1.0] * 3, "low": [1.0] * 3, "close": [1.0] * 3,
        "volume": pa.array([10, 20, 30], pa.int64()), "source": ["vci"] * 3,
    })  # fmt: skip
    assert table_to_trade_dates(t) == {date(2026, 11, 20), date(2026, 11, 21)}
    rows = table_to_rows(t)
    assert rows[0][0].bar_ts == int(ts[0].timestamp() * 1000) and rows[0][1] == "vci"
    assert table_to_trade_dates(t.slice(0, 0)) == set()


def test_replay_with_days_selected_from_history():
    ds = [date(2026, 11, d) for d in (17, 18, 19, 20)]
    hist = FakeHistory({d: _day_bars(d) for d in ds})
    clock = FakeClock()
    chosen = last_trade_dates(hist.read_year_dates, ["FPT"], 2, today=date(2026, 11, 24))
    res = replay(["FPT", "VNM"], chosen, rate=1000, read_day=hist.read_day,
                 producer=FakeProducer(clock=clock), clock=clock, sleep=clock.sleep)  # fmt: skip
    assert chosen == ds[-2:] and res.days == 2 and res.sent == 40


def test_run_replay_start_end_rejects_partial_and_reversed():
    with pytest.raises(ValueError):
        run_replay(["FPT"], start=date(2026, 11, 20), history=FakeHistory({}))
    with pytest.raises(ValueError):
        run_replay(["FPT"], start=date(2026, 11, 21), end=date(2026, 11, 20),
                   history=FakeHistory({}))  # fmt: skip


def test_run_replay_start_end_reads_weekdays_only():
    hist = FakeHistory({})
    with pytest.raises(HistoryUnavailableError):
        run_replay(["FPT"], start=date(2026, 11, 20), end=date(2026, 11, 23), history=hist)
    assert hist.reads == [date(2026, 11, 20), date(2026, 11, 23)]


# ------------------------------------------------------------------ ranh giới module


def test_replay_module_has_no_sources_or_cache():
    src = Path(replay_mod.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    mods = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            mods += [a.name for a in n.names]
        elif isinstance(n, ast.ImportFrom):
            mods.append(n.module or "")
    assert not [m for m in mods if m.startswith("stocklake.sources")]
    assert not [m for m in mods if m.split(".")[0] in ("ingestion", "etl", "dq", "analytics")]
    top = {n.names[0].name.split(".")[0] for n in tree.body if isinstance(n, ast.Import)}
    top |= {n.module.split(".")[0] for n in tree.body if isinstance(n, ast.ImportFrom) and n.module}
    assert not top & {"pyarrow", "confluent_kafka", "fsspec"}
    assert "VciClient" not in src and "replay_cache" not in src
