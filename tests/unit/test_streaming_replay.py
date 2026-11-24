import ast
import json
import logging
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from stocklake.contracts.topics import INTRADAY_PRICES
from stocklake.streaming.producers import replay as replay_mod
from stocklake.streaming.producers.base import serialize
from stocklake.streaming.producers.replay import (
    HdfsHistory,
    HistoryUnavailableError,
    ListChunk,
    MonthWindow,
    Pacer,
    ReplayBar,
    bar_to_message,
    bar_ts_filters,
    dedup_sort,
    encode_message,
    history_paths,
    last_trade_dates,
    merge_bars,
    month_windows,
    parse_sources,
    publish_bars,
    replay,
    run_replay,
    vn_date_of_ms,
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
    assert res.target_rate == 2000
    assert res.achieved_rate == pytest.approx(2000, rel=0.03)
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


# ------------------------------------------------------------------ cửa sổ tháng, đường dẫn


def test_vn_day_bounds_utc():
    lo, hi = vn_day_bounds(date(2026, 11, 20), date(2026, 11, 20))
    assert lo == datetime(2026, 11, 19, 17, 0, tzinfo=UTC)
    assert hi == datetime(2026, 11, 20, 17, 0, tzinfo=UTC)
    assert lo.tzinfo is not None  # aware: khớp cột timestamp có tz


def test_month_windows_year_boundary_and_clipping():
    w = month_windows(date(2025, 12, 15), date(2026, 1, 10))
    assert w == [
        MonthWindow(2025, 12, datetime(2025, 12, 14, 17, tzinfo=UTC),
                    datetime(2025, 12, 31, 17, tzinfo=UTC)),
        MonthWindow(2026, 1, datetime(2025, 12, 31, 17, tzinfo=UTC),
                    datetime(2026, 1, 10, 17, tzinfo=UTC)),
    ]  # fmt: skip
    # Tháng giữa không bị cắt; 01/01 VN bắt đầu từ 31/12 17:00 UTC.
    mid = month_windows(date(2025, 11, 30), date(2026, 2, 2))
    assert [(x.year, x.month) for x in mid] == [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]
    assert mid[2].lo == datetime(2025, 12, 31, 17, tzinfo=UTC)
    assert mid[2].hi == datetime(2026, 1, 31, 17, tzinfo=UTC)
    assert mid[0].lo == datetime(2025, 11, 29, 17, tzinfo=UTC)
    # Cửa sổ liền nhau, không chồng, không hở.
    assert all(a.hi == b.lo for a, b in zip(mid, mid[1:], strict=False))
    one = month_windows(date(2026, 11, 20), date(2026, 11, 20))
    assert one == [MonthWindow(2026, 11, *vn_day_bounds(date(2026, 11, 20), date(2026, 11, 20)))]


def test_history_paths_and_sources():
    root = "/data/raw/intraday_history/"
    assert history_paths(root, "vci", 2026, ["VNM", "FPT", "FPT"]) == [
        "/data/raw/intraday_history/source=vci/year=2026/FPT.parquet",
        "/data/raw/intraday_history/source=vci/year=2026/VNM.parquet",
    ]
    entries = ["/r/source=vndirect", "/r/_SUCCESS", "/r/source=vci", "/r/source=kbs"]
    assert parse_sources(entries) == ["vci", "kbs", "vndirect"]
    assert parse_sources(["/r/_x"]) == []


def test_bar_ts_filters_are_aware():
    lo, hi = vn_day_bounds(date(2027, 1, 1), date(2027, 1, 1))
    f = bar_ts_filters(lo, hi)
    assert f == [("bar_ts", ">=", datetime(2026, 12, 31, 17, tzinfo=UTC)),
                 ("bar_ts", "<", datetime(2027, 1, 1, 17, tzinfo=UTC))]  # fmt: skip
    assert f[0][2].tzinfo is not None


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


def test_chunk_slices_days_in_vietnam_time():
    ms = lambda dt: int(dt.timestamp() * 1000)  # noqa: E731
    edge = ms(datetime(2025, 12, 31, 17, 0, tzinfo=UTC))  # 01/01/2026 00:00 VN
    before = edge - 60_000  # 31/12/2025 23:59 VN
    assert vn_date_of_ms(edge) == date(2026, 1, 1)
    assert vn_date_of_ms(before) == date(2025, 12, 31)
    bars = [ReplayBar("FPT", t, 1, 1, 1, 1, 1) for t in (edge + 60_000, before, edge)]
    chunk = ListChunk([(b, "vci") for b in bars])
    assert chunk.dates() == [date(2025, 12, 31), date(2026, 1, 1)]
    assert [b.bar_ts for b, _ in chunk.day_rows(date(2026, 1, 1))] == [edge + 60_000, edge]
    assert chunk.day_rows(date(2026, 1, 2)) == []


# ------------------------------------------------------------------ lịch sử giả (theo tháng)


def _day_bars(day: date, symbols=("FPT", "VNM"), n=10) -> list[ReplayBar]:
    lo, _ = vn_day_bounds(day, day)
    start = int(lo.timestamp() * 1000) + 9 * 3_600_000
    out = []
    for sym in symbols:
        out += _bars(sym, n, start=start)
    return dedup_sort((b, "vci") for b in out)


def _in(b: ReplayBar, lo: datetime, hi: datetime) -> bool:
    return lo <= datetime.fromtimestamp(b.bar_ts / 1000, UTC) < hi


class FakeHistory:
    """Lịch sử giả {ngày: nến}; ghi lại các lần đọc tháng (thread-safe nhờ list.append)."""

    def __init__(self, days: dict[date, list[ReplayBar]]):
        self.days = days
        self.month_reads: list[tuple[int, int]] = []
        self.date_reads: list[tuple[int, int]] = []

    def _bars(self, symbols, lo, hi):
        return [b for bars in self.days.values() for b in bars if b.symbol in symbols
                and _in(b, lo, hi)]  # fmt: skip

    def read_month(self, symbols, year, month, lo, hi):
        self.month_reads.append((year, month))
        return ListChunk((b, "vci") for b in self._bars(symbols, lo, hi))

    def read_month_dates(self, symbols, year, month, lo, hi):
        self.date_reads.append((year, month))
        return {vn_date_of_ms(b.bar_ts) for b in self._bars(symbols, lo, hi)}


D_OCT, D_NOV, D_DEC = date(2026, 10, 15), date(2026, 11, 19), date(2026, 12, 17)
SYMS = ["FPT", "VNM"]


def _three_months() -> FakeHistory:
    return FakeHistory({d: _day_bars(d) for d in (D_OCT, D_NOV, D_DEC)})


def _alive_readers() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "replay-reader" and t.is_alive()]


# ------------------------------------------------------------------ --days dò sớm dừng


def test_last_trade_dates_stops_early_and_goes_back_across_year():
    nov = [date(2026, 11, d) for d in (16, 17, 18, 19, 20, 23)]
    h = FakeHistory({d: _day_bars(d) for d in nov})
    h.days[date(2025, 12, 30)] = _day_bars(date(2025, 12, 30))
    got = last_trade_dates(h.read_month_dates, SYMS, 3, today=date(2026, 11, 24))
    assert got == [date(2026, 11, 19), date(2026, 11, 20), date(2026, 11, 23)]
    assert h.date_reads == [(2026, 11)]  # đủ ngay ở tháng đầu: không đọc tháng trước
    h.date_reads.clear()
    got = last_trade_dates(h.read_month_dates, SYMS, 7, today=date(2026, 11, 24))
    assert got[0] == date(2025, 12, 30) and len(got) == 7
    assert h.date_reads[0] == (2026, 11) and h.date_reads[-1] == (2025, 12)
    assert len(h.date_reads) == 12  # lùi từng tháng 11/2026 -> 12/2025 rồi dừng
    # Ít hơn N: trả những gì có (dò tới tháng sớm nhất).
    assert len(last_trade_dates(h.read_month_dates, SYMS, 50, today=date(2026, 11, 24))) == 7
    with pytest.raises(HistoryUnavailableError):
        last_trade_dates(lambda *a: set(), SYMS, 3, today=date(2026, 11, 24))
    with pytest.raises(ValueError):
        last_trade_dates(h.read_month_dates, SYMS, 0)


# ------------------------------------------------------------------ replay nhiều tháng


def _run(hist, start, end, *, rate=10, prefetch=True, producer=None, clock=None):
    clock = clock or FakeClock()
    prod = producer or FakeProducer(clock=clock, per_msg_s=5e-6)
    res = replay(
        SYMS, start, end, rate=rate, run_id="rX", read_month=hist.read_month, producer=prod,
        clock=clock, sleep=clock.sleep, prefetch=prefetch,
    )  # fmt: skip
    return res, prod


@pytest.mark.parametrize("prefetch", [True, False])
def test_replay_months_order_pacing_and_result(prefetch):
    hist = _three_months()
    hist.days[date(2026, 11, 20)] = []  # ngày rỗng bị bỏ qua
    res, prod = _run(hist, date(2026, 10, 1), date(2026, 12, 31), prefetch=prefetch)
    assert sorted(hist.month_reads) == [(2026, 10), (2026, 11), (2026, 12)]
    assert (res.sent, res.delivered, res.failed) == (60, 60, 0)
    assert (res.days, res.symbols, res.run_id, res.months) == (3, 2, "rX", 3)
    # Nhịp liên tục qua ranh giới tháng: 60 bản tin ở 10 msg/s ~ 6 s, không reset theo tháng.
    assert res.seconds == pytest.approx(6.0, rel=0.05)
    assert res.target_rate == 10 and res.achieved_rate == pytest.approx(10, rel=0.1)
    assert res.read_seconds >= 0
    msgs = [json.loads(v) for _, _, v in prod.sent]
    keys = [(m["bar_ts"], m["symbol"]) for m in msgs]
    for k in (0, 20, 40):
        assert keys[k : k + 20] == sorted(keys[k : k + 20])
    assert keys[19][0] < keys[20][0] and keys[39][0] < keys[40][0]
    assert {m["replay_run_id"] for m in msgs} == {"rX"}
    assert all(m["source"] == "replay" for m in msgs)
    out = json.loads(res.to_json())
    expected = (
        "run_id sent delivered failed seconds first_bar_ts last_bar_ts days symbols "
        "target_rate achieved_rate read_seconds months"
    ).split()
    assert set(expected) <= set(out)
    assert not _alive_readers()


def test_replay_clips_to_range_and_reads_only_needed_months():
    hist = _three_months()
    res, _ = _run(hist, D_NOV, D_NOV, rate=1000)
    assert hist.month_reads == [(2026, 11)]
    assert (res.sent, res.days, res.months) == (20, 1, 1)


def test_prefetch_reads_next_month_while_sending_current():
    hist = _three_months()
    started = {m: threading.Event() for m in (10, 11, 12)}
    inner = hist.read_month
    first_send: list[tuple[int, bool]] = []

    def read_month(symbols, year, month, lo, hi):
        started[month].set()
        return inner(symbols, year, month, lo, hi)

    class Spy(FakeProducer):
        n = 0

        def produce(self, *a, **k):
            Spy.n += 1
            if Spy.n == 1:  # chunk tháng đầu đã có trước bản tin đầu tiên
                first_send.append((10, started[10].is_set()))
            if Spy.n == 3:  # còn đang gửi tháng 10: tháng 11 phải đã/đang được đọc nền
                first_send.append((11, started[11].wait(5)))
            super().produce(*a, **k)

    clock = FakeClock()
    res = replay(SYMS, date(2026, 10, 1), date(2026, 12, 31), rate=1000, read_month=read_month,
                 producer=Spy(clock=clock), clock=clock, sleep=clock.sleep)  # fmt: skip
    assert first_send == [(10, True), (11, True)]
    assert res.sent == 60 and not _alive_readers()


def test_sync_mode_reads_month_only_when_needed():
    hist = _three_months()
    seen: list[int] = []

    class Spy(FakeProducer):
        def produce(self, *a, **k):
            seen.append(len(hist.month_reads))
            super().produce(*a, **k)

    clock = FakeClock()
    replay(SYMS, date(2026, 10, 1), date(2026, 12, 31), rate=1000, read_month=hist.read_month,
           producer=Spy(), clock=clock, sleep=clock.sleep, prefetch=False)  # fmt: skip
    assert seen[0] == 1 and seen[-1] == 3 and seen == sorted(seen)


def test_reader_exception_propagates_and_thread_stops():
    hist = _three_months()

    def read_month(symbols, year, month, lo, hi):
        if month == 11:
            raise OSError("webhdfs down")
        return hist.read_month(symbols, year, month, lo, hi)

    clock = FakeClock()
    prod = FakeProducer(clock=clock)
    with pytest.raises(OSError, match="webhdfs down"):
        replay(SYMS, date(2026, 10, 1), date(2026, 12, 31), rate=1000, read_month=read_month,
               producer=prod, clock=clock, sleep=clock.sleep)  # fmt: skip
    assert len(prod.sent) + len(prod.queue) == 20  # tháng 10 đã gửi trước khi lỗi tới
    assert not _alive_readers()


def test_reader_exception_in_first_month_raises_before_producer():
    def read_month(*a):
        raise OSError("boom")

    with pytest.raises(OSError, match="boom"):
        replay(
            SYMS, date(2026, 10, 1), date(2026, 10, 31), read_month=read_month, producer=object()
        )
    assert not _alive_readers()


def test_sender_error_stops_blocked_reader():
    hist = FakeHistory({d: _day_bars(d) for d in (D_OCT, D_NOV, D_DEC, date(2027, 1, 14))})

    class Boom(FakeProducer):
        def produce(self, *a, **k):
            raise RuntimeError("kafka down")

    clock = FakeClock()
    with pytest.raises(RuntimeError, match="kafka down"):
        replay(SYMS, date(2026, 10, 1), date(2027, 1, 31), rate=1000, read_month=hist.read_month,
               producer=Boom(), clock=clock, sleep=clock.sleep)  # fmt: skip
    assert not _alive_readers()  # luồng đọc có thể đang kẹt ở put() hàng đợi đầy


def test_replay_no_bars_raises_before_producer():
    hist = FakeHistory({})
    with pytest.raises(HistoryUnavailableError, match="intraday_history.py"):
        replay(SYMS, date(2026, 11, 20), date(2026, 11, 20), read_month=hist.read_month,
               producer=object())  # fmt: skip
    assert not _alive_readers()


def test_warns_when_below_target_rate(caplog):
    hist = FakeHistory({D_NOV: _day_bars(D_NOV, n=2000)})
    clock = FakeClock()
    prod = FakeProducer(clock=clock, per_msg_s=0.001)  # gửi chậm: ~1000 msg/s < 95% của 2000
    with caplog.at_level(logging.WARNING):
        res = replay(SYMS, D_NOV, D_NOV, rate=2000, read_month=hist.read_month, producer=prod,
                     clock=clock, sleep=clock.sleep)  # fmt: skip
    assert res.achieved_rate < 0.95 * res.target_rate
    assert any(
        r.levelno == logging.WARNING and "mục tiêu" in r.getMessage() for r in caplog.records
    )


def test_run_replay_days_selects_range_from_history():
    today = datetime.now(UTC).astimezone(replay_mod.VN_TZ).date()
    ds = weekdays(today - timedelta(days=9), today)[-4:]
    hist = FakeHistory({d: _day_bars(d) for d in ds})
    clock = FakeClock()
    res = run_replay(SYMS, days=2, rate=100000, history=hist, prefetch=False,
                     producer=FakeProducer(clock=clock))  # fmt: skip
    assert res.days == 2 and res.sent == 40
    assert hist.date_reads[0] == (today.year, today.month)


def test_run_replay_start_end_rejects_partial_and_reversed():
    with pytest.raises(ValueError):
        run_replay(["FPT"], start=date(2026, 11, 20), history=FakeHistory({}))
    with pytest.raises(ValueError):
        run_replay(["FPT"], start=date(2026, 11, 21), end=date(2026, 11, 20),
                   history=FakeHistory({}))  # fmt: skip


def test_run_replay_start_end_reads_months_in_range():
    hist = FakeHistory({})
    with pytest.raises(HistoryUnavailableError):
        run_replay(["FPT"], start=date(2026, 11, 20), end=date(2026, 12, 3), history=hist)
    assert sorted(hist.month_reads) == [(2026, 11), (2026, 12)]


# ------------------------------------------------------------------ HDFS (fake fs / pyarrow)


def test_hdfs_history_missing_table_message():
    class NoFs:
        def ls(self, path, detail=False):
            raise FileNotFoundError(path)

    with pytest.raises(HistoryUnavailableError, match="intraday_history.py"):
        HdfsHistory(fs=NoFs()).read_month(["FPT"], 2026, 11, *vn_day_bounds(D_NOV, D_NOV))


def test_hdfs_history_reads_explicit_paths_once_per_source(monkeypatch):
    pytest.importorskip("pyarrow")
    import pyarrow as pa

    from stocklake.common import hdfs

    calls = []
    lo, hi = vn_day_bounds(D_NOV, D_NOV)
    ts = [datetime(2026, 11, 19, 2, 15, tzinfo=UTC), datetime(2026, 11, 19, 2, 16, tzinfo=UTC)]

    def fake_read(paths, columns=None, filters=None, fs=None, missing_ok=True):
        calls.append((list(paths), columns, filters))
        px = 100.0 if "source=vci" in paths[0] else 200.0
        return pa.table({
            "symbol": ["FPT", "FPT"], "bar_ts": pa.array(ts, pa.timestamp("us", tz="UTC")),
            "open": [px] * 2, "high": [px] * 2, "low": [px] * 2, "close": [px] * 2,
            "volume": pa.array([1, 2], pa.int64()),
        })  # fmt: skip

    class Fs:
        ls_calls = 0

        def ls(self, path, detail=False):
            Fs.ls_calls += 1
            return [f"{path}/source=vndirect", f"{path}/source=vci", f"{path}/_SUCCESS"]

    monkeypatch.setattr(hdfs, "read_parquet_files", fake_read, raising=False)
    h = HdfsHistory(fs=Fs())
    chunk = h.read_month(["VNM", "FPT"], 2026, 11, lo, hi)
    h.read_month(["FPT"], 2026, 11, lo, hi)
    assert Fs.ls_calls == 1  # nguồn dò một lần
    base = "/data/raw/intraday_history"
    assert calls[0] == (
        [f"{base}/source=vci/year=2026/FPT.parquet", f"{base}/source=vci/year=2026/VNM.parquet"],
        ["symbol", "bar_ts", "open", "high", "low", "close", "volume"],
        bar_ts_filters(lo, hi),
    )
    assert calls[1][0][0].startswith(f"{base}/source=vndirect/")
    assert chunk.dates() == [D_NOV]
    assert [b.open for b in dedup_sort(chunk.day_rows(D_NOV))] == [100.0, 100.0]  # ưu tiên vci


def test_table_helpers_with_pyarrow():
    pa = pytest.importorskip("pyarrow")
    from stocklake.streaming.producers.replay import (
        ArrowChunk,
        split_by_vn_day,
        table_to_rows,
        table_to_trade_dates,
    )

    # 2026-11-20 16:59 UTC = 2026-11-20 23:59 VN; 17:00 UTC = 2026-11-21 00:00 VN
    ts = [datetime(2026, 11, 20, 17, 0, tzinfo=UTC), datetime(2026, 11, 20, 2, 15, tzinfo=UTC),
          datetime(2026, 11, 20, 16, 59, tzinfo=UTC)]  # fmt: skip
    t = pa.table({
        "symbol": ["FPT"] * 3, "bar_ts": pa.array(ts, pa.timestamp("us", tz="UTC")),
        "open": [1.0] * 3, "high": [1.0] * 3, "low": [1.0] * 3, "close": [1.0] * 3,
        "volume": pa.array([10, 20, 30], pa.int64()),
    })  # fmt: skip
    assert table_to_trade_dates(t) == {date(2026, 11, 20), date(2026, 11, 21)}
    rows = table_to_rows(t, "vci")
    assert rows[0][0].bar_ts == int(ts[0].timestamp() * 1000) and rows[0][1] == "vci"
    assert table_to_trade_dates(t.slice(0, 0)) == set()
    days = split_by_vn_day(t)
    assert [d for d, _ in days] == [date(2026, 11, 20), date(2026, 11, 21)]
    assert [p.num_rows for _, p in days] == [2, 1]
    chunk = ArrowChunk([("vci", t)])
    assert chunk.dates() == [date(2026, 11, 20), date(2026, 11, 21)]
    day1 = chunk.day_rows(date(2026, 11, 20))
    assert [b.bar_ts for b, _ in day1] == sorted(b.bar_ts for b, _ in day1)


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
