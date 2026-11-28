"""Collector nến phút: phát hiện nguồn điều chỉnh giá, tải lại mọi năm, cờ ép tải lại."""

import dataclasses
import importlib.util
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from stocklake.common.http import SourceBlockedError
from stocklake.ingestion.collectors import intraday_history as mod
from stocklake.ingestion.collectors.intraday_history import IntradayHistoryCollector, file_path
from stocklake.ingestion.framework import JsonFileStateStore
from stocklake.ingestion.price_adjustment import adjustment_events
from stocklake.sources.base import MinuteBar
from stocklake.sources.vci import MinutePage

UTC = timezone.utc
VN = mod.VN_TZ
NOW = datetime(2026, 11, 24, 12, 0, tzinfo=UTC)  # 19:00 thứ 3 giờ VN
IDENT = lambda rows: rows  # noqa: E731
T25 = datetime(2025, 6, 2, 2, 0, tzinfo=UTC)
T26 = datetime(2026, 11, 10, 2, 0, tzinfo=UTC)
P25 = file_path("FPT", 2025)
P26 = file_path("FPT", 2026)
N = "intraday_history"


def pbar(sym, ts, p, f=1.0):
    r = lambda x: round(x * f, 2)  # noqa: E731 - VCI: giá điều chỉnh 2 chữ số thập phân
    return MinuteBar(sym, ts, r(p), r(p + 50), r(p - 50), r(p + 20.37), 1000, "vci")


def history(f=1.0, sym="FPT", n25=40, n26=60):
    """Nến 2025 + 2026 với giá thay đổi; `f` là hệ số điều chỉnh của nguồn."""
    b = [pbar(sym, T25 + timedelta(minutes=i), 22309.84 + 13 * (i % 17), f) for i in range(n25)]
    b += [pbar(sym, T26 + timedelta(minutes=i), 23100.5 + 9 * (i % 23), f) for i in range(n26)]
    return b


class FakeClient:
    """minute_pages: một trang mỗi lần gọi; `block_when(start)` -> ném 429."""

    def __init__(self, data, block_when=None):
        self.data, self.block_when, self.calls = data, block_when, []

    def minute_pages(self, symbol, start, end, **_):
        self.calls.append((symbol, start, end))
        if self.block_when and self.block_when(start):
            raise SourceBlockedError("chặn", 429, "u", "60", 60.0)
        bars = [b for b in self.data.get(symbol, []) if start <= b.bar_ts < end]
        yield MinutePage(bars, None)


class FakeWriter:
    def __init__(self):
        self.writes = []

    def write_groups(self, groups, path):
        self.writes.append((path, [r for g in groups for r in g]))

    @property
    def by_path(self):
        return dict(self.writes)


class FakeReader:
    def __init__(self, store=None):
        self.store = dict(store or {})

    def read_rows(self, path):
        return self.store.get(path)

    def absorb(self, writer):
        """File trên 'HDFS' sau khi `writer` ghi."""
        for p, rows in writer.writes:
            self.store[p] = (12, rows)
        return self


def collector(tmp_path, data, reader, clock=lambda: NOW, **kw):
    cli = FakeClient(data, kw.pop("block_when", None))
    w = FakeWriter()
    st = JsonFileStateStore(tmp_path / "s.json")
    col = IntradayHistoryCollector(
        cli, st, w, ["FPT"], date(2025, 1, 1), clock=clock, to_table=IDENT, reader=reader, **kw
    )
    return col, cli, w, st


def first_run(tmp_path, f=1.0):
    reader = FakeReader()
    col, _, w, st = collector(tmp_path, {"FPT": history(f)}, reader)
    col.run()
    assert st.is_done(N, f"{N}/FPT/2025")
    return reader.absorb(w)


def closes(rows):
    return [r["close"] for r in rows]


# ------------------------------------------------------------- (a) lệch theo một tỷ lệ chung


def test_scaled_refetches_and_rewrites_every_year_same_run(tmp_path):
    reader = first_run(tmp_path)
    col, cli, w, st = collector(tmp_path, {"FPT": history(0.9775)}, reader)
    out = col.run()
    # lượt thường: 1 request lùi tới 1/1/2026; phát hiện -> thêm 1 request cho [1/1/2025, 1/1/2026)
    assert [(c[1], c[2]) for c in cli.calls][1] == (
        datetime(2025, 1, 1, tzinfo=VN),
        datetime(2026, 1, 1, tzinfo=VN),
    )
    assert len(cli.calls) == 2 and out["requests"] == 2
    assert set(w.by_path) == {P25, P26}
    assert closes(w.by_path[P25]) == [b.close for b in history(0.9775)[:40]]
    assert st.is_done(N, f"{N}/FPT/2025")
    adj = out["price_adjustments"]
    assert len(adj) == 1 and adj[0]["symbol"] == "FPT"
    assert adj[0]["factor"] == pytest.approx(0.9775, abs=2e-6)
    assert adj[0]["years_rewritten"] == [2025, 2026] and adj[0]["files_rewritten"] == 2
    assert adj[0]["refetch"] == "done" and adj[0]["matched"] == 60
    assert out["price_mismatches"] == [] and out["symbols_done"] == ["FPT"]
    ev = adjustment_events(JsonFileStateStore(tmp_path / "s.json"), N, "FPT")
    assert len(ev) == 1 and ev[0]["status"] == "done"
    m = ev[0]["meta"]
    assert m["factor"] == pytest.approx(0.9775, abs=2e-6) and m["symbol"] == "FPT"
    assert m["detected_at"].startswith("2026-11-24T12:00") and m["detected_date"] == "2026-11-24"
    assert m["run_id"] == out["run_id"] and m["years_marked"] == [2025]


def test_scaled_history_is_appended_not_overwritten(tmp_path):
    reader = first_run(tmp_path)
    col, _, w, _ = collector(tmp_path, {"FPT": history(0.9775)}, reader)
    col.run()
    reader.absorb(w)
    later = lambda: NOW + timedelta(days=2)  # noqa: E731
    col, _, _, st = collector(tmp_path, {"FPT": history(0.9775 * 0.95)}, reader, clock=later)
    out = col.run()
    assert out["price_adjustments"][0]["factor"] == pytest.approx(0.95, abs=2e-6)
    ev = adjustment_events(st, N, "FPT")
    assert [round(e["meta"]["factor"], 4) for e in ev] == [0.9775, 0.95]


def test_scaled_without_extra_request_when_all_years_already_fetched(tmp_path):
    reader = first_run(tmp_path)
    st = JsonFileStateStore(tmp_path / "s.json")
    st.mark_failed(N, f"{N}/FPT/2025", "lỗi cũ")  # năm 2025 chưa xong: lượt này lùi tới 2025
    col, cli, w, _ = collector(tmp_path, {"FPT": history(0.9775)}, reader)
    out = col.run()
    assert len(cli.calls) == 1 and set(w.by_path) == {P25, P26}
    assert out["price_adjustments"][0]["files_rewritten"] == 2


# ------------------------------------------------------------- (b) lệch không đều


def test_irregular_warns_without_refetching_past_years(tmp_path, caplog):
    reader = first_run(tmp_path)
    data = history()
    data[40:70] = history(0.9775)[40:70]  # nửa đầu năm 2026 lệch hệ số, nửa sau không
    col, cli, w, st = collector(tmp_path, {"FPT": data}, reader)
    out = col.run()
    assert len(cli.calls) == 1 and list(w.by_path) == [P26]  # vẫn ghi năm hiện tại như cũ
    assert st.is_done(N, f"{N}/FPT/2025")
    mm = out["price_mismatches"]
    assert len(mm) == 1 and mm[0]["symbol"] == "FPT" and mm[0]["kind"] == "irregular"
    assert (mm[0]["changed"], mm[0]["overlap"]) == (30, 60) and len(mm[0]["examples"]) == 5
    assert out["price_adjustments"] == []
    ev = adjustment_events(st, N, "FPT")
    assert len(ev) == 1 and ev[0]["status"] == "failed"
    assert ev[0]["error"].startswith("price_mismatch") and ev[0]["meta"]["changed"] == 30
    assert any("KHÔNG theo một tỷ lệ chung" in r.getMessage() for r in caplog.records)


def test_minor_revision_reported_without_warning_state(tmp_path, caplog):
    reader = first_run(tmp_path)
    data = history()
    b = data[-1]
    data[-1] = MinuteBar(b.symbol, b.bar_ts, b.open, b.high + 30, b.low, b.close + 10, 999, "vci")
    col, cli, w, st = collector(tmp_path, {"FPT": data}, reader)
    out = col.run()
    assert len(cli.calls) == 1 and list(w.by_path) == [P26]
    assert out["price_revisions_minor"][0]["changed"] == 1
    assert out["price_mismatches"] == [] and adjustment_events(st, N) == []
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_too_few_overlapping_bars_only_warns(tmp_path):
    reader = FakeReader()
    data = history(n26=10)
    col, _, w, _ = collector(tmp_path, {"FPT": data}, reader)
    col.run()
    reader.absorb(w)
    col, cli, w, st = collector(tmp_path, {"FPT": history(0.9775, n26=10)}, reader)
    out = col.run()
    assert len(cli.calls) == 1 and list(w.by_path) == [P26]
    assert out["price_mismatches"][0]["reason"] == "too_few_overlap"
    assert st.is_done(N, f"{N}/FPT/2025")


# ------------------------------------------------------------- (c) không đổi


def test_unchanged_keeps_old_behaviour(tmp_path):
    reader = first_run(tmp_path)
    col, cli, w, _ = collector(tmp_path, {"FPT": history()}, reader)
    out = col.run()
    assert len(cli.calls) == 1 and out["requests"] == 1 and w.writes == []
    assert out["files_unchanged"] == 1
    for k in ("price_adjustments", "price_mismatches", "price_revisions_minor", "forced_refetch"):
        assert out[k] == []


def test_volume_only_change_rewrites_current_year_without_report(tmp_path):
    reader = first_run(tmp_path)
    data = [dataclasses.replace(b, volume=b.volume + 1) for b in history()]
    col, cli, w, _ = collector(tmp_path, {"FPT": data}, reader)
    out = col.run()
    assert len(cli.calls) == 1 and list(w.by_path) == [P26]
    assert out["price_adjustments"] == out["price_mismatches"] == out["price_revisions_minor"] == []


# ------------------------------------------------------------- (d) cờ ép tải lại


def test_force_refetch_all_years_rewrites_stale_past_year(tmp_path):
    """Ca FPT: file 2026 đã mang hệ số mới, 2025 còn cũ -> so sánh không thấy; ép tải lại."""
    reader = first_run(tmp_path)  # 2025 và 2026 hệ số cũ; 2025 đã xong
    new = history(0.9775)
    reader.store[P26] = (12, mod.build_rows(new[40:], NOW, "r-truoc"))  # 2026 đã mang hệ số mới
    col, cli, w2, st = collector(tmp_path, {"FPT": new}, reader)
    plain = col.run()  # không ép: không phát hiện được, năm 2025 giữ hệ số cũ
    assert plain["price_adjustments"] == [] and w2.writes == [] and len(cli.calls) == 1

    col, cli, w3, st = collector(tmp_path, {"FPT": new}, reader, refetch_all_years=["fpt"])
    out = col.run()
    assert len(cli.calls) == 1 and cli.calls[0][1] == datetime(2025, 1, 1, tzinfo=VN)
    assert list(w3.by_path) == [P25]  # 2026 không đổi -> không ghi lại
    assert out["forced_refetch"] == [{"symbol": "FPT", "years": [2025, 2026], "files_written": 1}]
    assert st.is_done(N, f"{N}/FPT/2025")


def test_force_respects_trading_hours(tmp_path):
    t = datetime(2026, 11, 24, 10, 0, tzinfo=VN)
    col, cli, w, _ = collector(
        tmp_path, {"FPT": history()}, FakeReader(), clock=lambda: t, refetch_all_years=["FPT"]
    )
    out = col.run()
    assert out["stopped_reason"] == "trading_hours" and cli.calls == [] and w.writes == []


def test_force_blocked_leaves_past_years_pending_for_next_run(tmp_path):
    first_run(tmp_path)
    col, _, w, st = collector(
        tmp_path, {"FPT": history()}, FakeReader(), refetch_all_years=["FPT"],
        block_when=lambda start: True,
    )  # fmt: skip
    out = col.run()
    assert out["stopped_reason"] == "blocked" and w.writes == []
    e = st.get(N, f"{N}/FPT/2025")
    assert e["status"] == "failed" and e["error"].startswith("pending_refetch: forced")
    col, cli, w, st = collector(tmp_path, {"FPT": history()}, FakeReader())  # không cờ
    col.run()
    assert cli.calls[0][1] == datetime(2025, 1, 1, tzinfo=VN) and set(w.by_path) == {P25, P26}
    assert st.is_done(N, f"{N}/FPT/2025")


def test_job_passes_refetch_flag(monkeypatch, capsys):
    path = Path(__file__).parents[2] / "jobs/ingest/intraday_history.py"
    spec = importlib.util.spec_from_file_location("job_intraday_history_adj", path)
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)
    seen = {}

    def fake(*a, **k):
        seen.update(args=a, kw=k)
        return {"blocked": None, "stopped_reason": None, "symbols_failed": []}

    monkeypatch.setattr(job, "run_intraday_history", fake)
    monkeypatch.setattr(sys, "argv", ["x", "--group", "vn30", "--refetch-all-years", "fpt, vnm"])
    assert job.main() == 0
    assert seen["kw"]["refetch_all_years"] == ["FPT", "VNM"]
    monkeypatch.setattr(sys, "argv", ["x", "--symbols", "FPT"])
    job.main()
    assert seen["kw"]["refetch_all_years"] == ()
    capsys.readouterr()


# ------------------------------------------------------------- (f) bị chặn giữa lúc tải lại


def test_blocked_during_refetch_stops_cleanly_and_resumes(tmp_path):
    reader = first_run(tmp_path)
    old26 = list(reader.store[P26][1])
    col, cli, w, st = collector(
        tmp_path, {"FPT": history(0.9775)}, reader,
        block_when=lambda start: start.year == 2025,  # request lấy thêm 2025 bị 429
    )  # fmt: skip
    out = col.run()
    assert out["stopped_reason"] == "blocked" and out["blocked"]["status"] == 429
    assert w.writes == []  # không ghi gì, kể cả năm hiện tại
    e = st.get(N, f"{N}/FPT/2025")
    assert e["status"] == "failed" and e["error"].startswith("pending_refetch: price adjustment")
    assert out["price_adjustments"][0]["refetch"] == "pending"
    ev = adjustment_events(st, N, "FPT")
    assert ev[0]["status"] == "failed" and ev[0]["meta"]["refetch"] == "pending"
    assert reader.store[P26][1] == old26

    col, cli, w, st = collector(tmp_path, {"FPT": history(0.9775)}, reader)
    out = col.run()
    assert len(cli.calls) == 1 and cli.calls[0][1] == datetime(2025, 1, 1, tzinfo=VN)
    assert set(w.by_path) == {P25, P26} and st.is_done(N, f"{N}/FPT/2025")
    assert out["price_adjustments"][0]["refetch"] == "done"  # file 2026 còn cũ -> phát hiện lại


def test_trading_hours_before_extra_fetch_stops_and_keeps_pending(tmp_path):
    reader = first_run(tmp_path)
    times = iter([NOW, NOW])  # t0, trước mã; lần sau: đã vào phiên
    clock = lambda: next(times, datetime(2026, 11, 25, 9, 5, tzinfo=VN))  # noqa: E731
    col, cli, w, st = collector(tmp_path, {"FPT": history(0.9775)}, reader, clock=clock)
    out = col.run()
    assert out["stopped_reason"] == "trading_hours" and len(cli.calls) == 1 and w.writes == []
    assert st.get(N, f"{N}/FPT/2025")["status"] == "failed"
