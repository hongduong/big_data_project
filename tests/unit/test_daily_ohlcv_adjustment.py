"""Collector nến ngày: phát hiện nguồn điều chỉnh giá, lấy lại mọi năm đang có, cờ ép."""

import dataclasses
import importlib.util
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from stocklake.common.http import SourceBlockedError
from stocklake.ingestion.collectors import daily_ohlcv as mod
from stocklake.ingestion.collectors.daily_ohlcv import (
    DailyOhlcvCollector,
    file_path,
    run_daily_ohlcv_summary,
)
from stocklake.ingestion.framework import JsonFileStateStore
from stocklake.ingestion.price_adjustment import adjustment_events
from stocklake.sources.base import DailyBar, SourceClient

UTC = timezone.utc
NOW = datetime(2026, 11, 24, 11, 0, tzinfo=UTC)  # 18:00 VN
IDENT = lambda rows: rows  # noqa: E731
N = "daily_ohlcv"


def dbar(sym, d, p, f=1.0, source="vci"):
    r = lambda x: round(x * f, 2)  # noqa: E731
    return DailyBar(sym, d, r(p), r(p + 300), r(p - 250), r(p + 120.37), 1000, None, "vnd", source)


def year_bars(year, n, f=1.0, sym="FPT", source="vci", first=None):
    d0 = first or date(year, 3, 2)
    return [dbar(sym, d0 + timedelta(days=i), 95000 + 700 * (i % 11), f, source) for i in range(n)]


def data(f=1.0, source="vci"):
    return {
        ("FPT", 2023): year_bars(2023, 8, f, source=source),
        ("FPT", 2025): year_bars(2025, 8, f, source=source),
        ("FPT", 2026): year_bars(2026, 12, f, source=source),
    }


class FakeSource(SourceClient):
    def __init__(self, name, data=None, block_years=()):
        self.name, self.data, self.block_years, self.calls = name, data or {}, block_years, []

    def daily_bars(self, symbol, start, end):
        self.calls.append((symbol, start.year))
        if start.year in self.block_years:
            raise SourceBlockedError("chặn", 429)
        return [b for b in self.data.get((symbol, start.year), []) if start <= b.trade_date <= end]


class FakeWriter:
    def __init__(self):
        self.writes = []

    def write(self, table, path):
        self.writes.append((path, table))

    @property
    def by_path(self):
        return dict(self.writes)


class FakeReader:
    def __init__(self, store=None, extra_years=()):
        self.store, self.extra_years = dict(store or {}), set(extra_years)

    def read_rows(self, path):
        return self.store.get(path)

    def existing_years(self, symbol, base=None):
        ys = {int(p.split("/year=")[1][:4]) for p in self.store if p.endswith(f"/{symbol}.parquet")}
        return ys | self.extra_years

    def absorb(self, writer):
        self.store.update(writer.writes)
        return self


def make(tmp_path, sources, reader, start=date(2025, 3, 1), **kw):
    w = FakeWriter()
    st = JsonFileStateStore(tmp_path / "s.json")
    col = DailyOhlcvCollector(
        sources, st, w, ["FPT"], start, date(2026, 11, 24), now=NOW, to_table=IDENT,
        reader=reader, **kw,
    )  # fmt: skip
    return col, w, st


def path(year, source="vci"):
    return file_path("FPT", source, year)


def seeded(tmp_path):
    """Lượt đầu từ 2023: ghi file 2023, 2025, 2026 (2024 rỗng -> xong, không file)."""
    reader = FakeReader()
    col, w, st = make(tmp_path, [FakeSource("vci", data())], reader, start=date(2023, 1, 1))
    res = col.run()
    assert res.failed == [] and st.is_done(N, f"{N}/FPT/2023")
    return reader.absorb(w)


# ------------------------------------------------------------- lệch theo một tỷ lệ chung


def test_scaled_refetches_every_existing_year_even_outside_range(tmp_path):
    reader = seeded(tmp_path)
    reader.extra_years = {2022}  # file có trên HDFS nhưng state không có khoá
    vci = FakeSource("vci", data(0.9775))
    col, w, st = make(tmp_path, [vci, FakeSource("vndirect")], reader)
    res = col.run()
    assert res.failed == []
    # 2025 đã xong -> bỏ qua ở vòng thường; phát hiện ở 2026 -> lấy lại 2022..2025
    assert vci.calls == [("FPT", 2026), ("FPT", 2022), ("FPT", 2023), ("FPT", 2024), ("FPT", 2025)]
    assert set(w.by_path) == {path(2023), path(2025), path(2026)}
    assert [r["close"] for r in w.by_path[path(2023)]] == [
        b.close for b in data(0.9775)[("FPT", 2023)]
    ]
    for y in (2022, 2023, 2024, 2025):
        assert st.is_done(N, f"{N}/FPT/{y}")
    assert st.get(N, f"{N}/FPT/_refetch")["status"] == "done"
    ev = adjustment_events(st, N, "FPT")
    assert len(ev) == 1 and ev[0]["status"] == "done"
    m = ev[0]["meta"]
    assert m["factor"] == pytest.approx(0.9775, abs=2e-6) and m["source"] == "vci"
    assert m["years_marked"] == [2022, 2023, 2024, 2025]
    assert m["years_rewritten"] == [2023, 2025] and m["detected_date"] == "2026-11-24"
    assert col.refetch_pending() == {}


def test_summary_json_lists_adjustment(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "rows_to_table", IDENT)
    reader = seeded(tmp_path)
    out = run_daily_ohlcv_summary(
        ["FPT"], start=date(2025, 3, 1), end=date(2026, 11, 24), writer=FakeWriter(),
        sources=[FakeSource("vci", data(0.9775))], state=JsonFileStateStore(tmp_path / "s.json"),
        reader=reader,
    )  # fmt: skip
    adj = out["price_adjustments"]
    assert len(adj) == 1 and adj[0]["symbol"] == "FPT" and adj[0]["refetch"] == "done"
    assert adj[0]["factor"] == pytest.approx(0.9775, abs=2e-6) and adj[0]["files_rewritten"] == 2
    assert out["price_mismatches"] == [] and out["refetch_pending"] == {}
    assert out["run_id"].startswith("d")


# ------------------------------------------------------------- lệch không đều / sửa lẻ / không đổi


def test_irregular_only_warns(tmp_path):
    reader = seeded(tmp_path)
    d = data()
    d[("FPT", 2026)] = year_bars(2026, 6, 0.9775) + year_bars(2026, 12)[6:]
    vci = FakeSource("vci", d)
    col, w, st = make(tmp_path, [vci], reader)
    col.run()
    assert vci.calls == [("FPT", 2026)] and list(w.by_path) == [path(2026)]
    assert len(col.mismatches) == 1 and col.mismatches[0]["changed"] == 6
    ev = adjustment_events(st, N, "FPT")
    assert ev[0]["status"] == "failed" and ev[0]["error"].startswith("price_mismatch")
    assert st.is_done(N, f"{N}/FPT/2023") and col.adjustments == []


def test_minor_revision_and_unchanged_add_no_requests(tmp_path):
    reader = seeded(tmp_path)
    d = data()
    b = d[("FPT", 2026)][-1]
    d[("FPT", 2026)][-1] = dataclasses.replace(b, close=b.close + 100)
    vci = FakeSource("vci", d)
    col, _, st = make(tmp_path, [vci], reader)
    col.run()
    assert len(vci.calls) == 1 and col.minor[0]["changed"] == 1 and col.mismatches == []
    assert adjustment_events(st, N) == []

    vci = FakeSource("vci", data())
    col, w, _ = make(tmp_path, [vci], seeded(tmp_path / "b"))
    col.run()
    assert len(vci.calls) == 1 and col.adjustments == col.mismatches == col.minor == []


def test_too_few_days_only_warns(tmp_path):
    reader = seeded(tmp_path)
    reader.store[path(2026)] = reader.store[path(2026)][:3]
    vci = FakeSource("vci", data(0.9775))
    col, _, _ = make(tmp_path, [vci], reader)
    col.run()
    assert len(vci.calls) == 1 and col.mismatches[0]["reason"] == "too_few_overlap"


def test_no_reader_means_old_behaviour(tmp_path):
    seeded(tmp_path)
    vci = FakeSource("vci", data(0.9775))
    col, _, _ = make(tmp_path, [vci], None)
    col.run()
    assert len(vci.calls) == 1 and col.adjustments == []


# ------------------------------------------------------------- cờ ép


def test_force_refetches_all_known_years_from_primary(tmp_path):
    reader = seeded(tmp_path)
    vci = FakeSource("vci", data(0.9775))
    col, w, st = make(tmp_path, [vci], reader, refetch_all_years=["fpt"])
    col.run()
    assert sorted(y for _, y in vci.calls) == [2023, 2024, 2025, 2026]
    assert set(w.by_path) == {path(2023), path(2025), path(2026)}
    assert col.forced == [{"symbol": "FPT", "years": [2023, 2024, 2025]}]
    assert st.get(N, f"{N}/FPT/_refetch")["status"] == "done"
    # phát hiện ở năm hiện tại vẫn chạy (file 2026 còn hệ số cũ) nhưng không lấy trùng năm
    assert len(vci.calls) == 4
    adj = col.adjustments[0]
    # 2025 đã lấy lại ở vòng thường nên không đánh dấu lại; 2023/2024 (ngoài khoảng) đang chờ
    assert adj["years_marked"] == [2023, 2024] and adj["years_rewritten"] == [2023]
    assert adj["refetch"] == "done"


# ------------------------------------------------------------- bị chặn giữa lúc lấy lại


def test_blocked_during_refetch_keeps_pending_and_never_falls_back(tmp_path):
    reader = seeded(tmp_path)
    vci = FakeSource("vci", data(0.9775), block_years=(2023,))
    vnd = FakeSource("vndirect", data(0.9775, "vndirect"))
    col, w, st = make(tmp_path, [vci, vnd], reader)
    res = col.run()
    assert set(w.by_path) == {path(2026)}  # không ghi file vndirect thay cho năm cũ
    assert vnd.calls == [] and len(res.failed) == 3
    assert col.refetch_pending() == {"FPT": [2023, 2024, 2025]}
    assert not st.is_done(N, f"{N}/FPT/2023") and not st.is_done(N, f"{N}/FPT/2025")
    ev = adjustment_events(st, N, "FPT")
    assert ev[0]["status"] == "failed" and ev[0]["meta"]["refetch"] == "pending"

    reader.absorb(w)
    vci = FakeSource("vci", data(0.9775))
    col, w, st = make(tmp_path, [vci, vnd], reader)  # lượt sau: khoảng 2025-2026, không cờ
    res = col.run()
    assert res.failed == [] and set(w.by_path) == {path(2023), path(2025), path(2026)}
    assert st.get(N, f"{N}/FPT/_refetch")["status"] == "done"
    assert all(st.is_done(N, f"{N}/FPT/{y}") for y in (2023, 2024, 2025))
    assert col.adjustments == []  # file 2026 đã mang hệ số mới; việc còn lại lấy từ state


def test_job_passes_refetch_flag(monkeypatch, capsys):
    p = Path(__file__).parents[2] / "jobs" / "ingest" / "daily_ohlcv.py"
    spec = importlib.util.spec_from_file_location("job_daily_adj", p)
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)
    seen = {}

    def fake(*a, **k):
        seen.update(k)
        return {"failed": 0, "stopped_blocked": False}

    monkeypatch.setattr(job, "run_daily_ohlcv_summary", fake)
    monkeypatch.setattr("sys.argv", ["x", "--refetch-all-years", "fpt,VNM"])
    assert job.main() == 0
    assert seen["refetch_all_years"] == ["FPT", "VNM"]
    capsys.readouterr()
