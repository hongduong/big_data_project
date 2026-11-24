import importlib.util
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from stocklake.common.http import SourceBlockedError
from stocklake.contracts import get_schema
from stocklake.ingestion.collectors import daily_ohlcv as mod
from stocklake.ingestion.collectors.daily_ohlcv import (
    DailyOhlcvCollector,
    build_rows,
    file_columns,
    file_path,
    load_symbols,
    run_daily_ohlcv,
    run_daily_ohlcv_summary,
)
from stocklake.ingestion.framework import JsonFileStateStore
from stocklake.sources.base import DailyBar, SourceClient

UTC = timezone.utc
NOW = datetime(2026, 11, 24, 3, 0, tzinfo=UTC)  # 10:00 ngày 24/11/2026 giờ VN
IDENT = lambda rows: rows  # noqa: E731 - thay bước dict -> pyarrow.Table


def bar(sym, d, source, close=10.0):
    return DailyBar(sym, d, close, close, close, close, 100, close * 2, "x", source)


class FakeSource(SourceClient):
    def __init__(self, name, data=None, error=None):
        self.name = name
        self.data = data or {}  # (symbol, year) -> list[DailyBar]
        self.error = error
        self.calls = []

    def daily_bars(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        if self.error:
            raise self.error
        return [b for b in self.data.get((symbol, start.year), []) if start <= b.trade_date <= end]


class FakeWriter:
    def __init__(self):
        self.writes = []  # (path, rows)

    def write(self, table, path):
        self.writes.append((path, table))

    @property
    def by_path(self):
        return {p: r for p, r in self.writes}


def vci_data():
    return {
        ("FPT", 2025): [bar("FPT", date(2025, 6, 2), "vci"), bar("FPT", date(2025, 6, 3), "vci")],
        ("FPT", 2026): [bar("FPT", date(2026, 11, 10), "vci")],
        ("VNM", 2026): [bar("VNM", date(2026, 1, 5), "vci")],
    }


def make(tmp_path, sources, symbols=("FPT",), start=date(2025, 3, 1), end=date(2026, 11, 24)):
    state = JsonFileStateStore(tmp_path / "s.json")
    w = FakeWriter()
    col = DailyOhlcvCollector(sources, state, w, symbols, start, end, now=NOW, to_table=IDENT)
    return col, w


# ------------------------------------------------------------- plan / layout


def test_plan_one_unit_per_symbol_year(tmp_path):
    col, _ = make(tmp_path, [FakeSource("vci")], ("FPT", "VNM"))
    units = list(col.plan())
    assert [u.key for u in units] == [
        "daily_ohlcv/FPT/2025",
        "daily_ohlcv/VNM/2025",
        "daily_ohlcv/FPT/2026#live",
        "daily_ohlcv/VNM/2026#live",
    ]
    live = units[2].params
    assert (live["start"], live["end"]) == (date(2026, 1, 1), date(2026, 11, 24))
    past = units[0].params
    assert (past["start"], past["end"]) == (date(2025, 1, 1), date(2025, 12, 31))


def test_plan_clips_to_range_and_today(tmp_path):
    col, _ = make(tmp_path, [FakeSource("vci")], start=date(2024, 5, 1), end=date(2025, 1, 1))
    assert [u.params["year"] for u in col.plan()] == [2024, 2025]
    col, _ = make(tmp_path, [FakeSource("vci")], start=date(2027, 1, 1), end=date(2027, 3, 1))
    assert list(col.plan()) == []


def test_path_layout_and_columns(tmp_path):
    vci = FakeSource("vci", vci_data())
    col, w = make(tmp_path, [vci])
    res = col.run()
    assert not res.failed
    paths = sorted(w.by_path)
    assert paths == [
        "/data/raw/daily_ohlcv/source=vci/year=2025/FPT.parquet",
        "/data/raw/daily_ohlcv/source=vci/year=2026/FPT.parquet",
    ]
    schema = get_schema("raw", "daily_ohlcv")
    expect = [c for c in schema.column_names if c not in ("source",)]
    for rows in w.by_path.values():
        for r in rows:
            assert list(r) == expect
            assert "source" not in r and "year" not in r
    assert file_columns() == tuple(expect)
    assert col.rows_written == 3
    assert col.per_source["vci"] == {"units": 2, "rows": 3}
    assert file_path("FPT", "vndirect", 2020, "/x/") == (
        "/x/raw/daily_ohlcv/source=vndirect/year=2020/FPT.parquet"
    )


def test_available_at_and_types():
    ts = datetime(2026, 11, 24, 3, 0, tzinfo=UTC)
    rows = build_rows([bar("FPT", date(2026, 11, 10), "vci")], ts)
    r = rows[0]
    assert r["available_at"] == datetime(2026, 11, 10, 8, 30, tzinfo=UTC)  # 15:30 VN
    assert r["ingest_ts"] == ts and r["trade_date"] == date(2026, 11, 10)
    assert r["raw_close"] == 20.0 and r["volume"] == 100 and r["price_unit"] == "x"


# ------------------------------------------------------------- idempotent


def test_rerun_refetches_only_live_and_rows_identical(tmp_path):
    vci = FakeSource("vci", vci_data())
    col, w = make(tmp_path, [vci])
    r1 = col.run()
    assert len(r1.done) == 2 and r1.skipped == []
    first = {p: [{k: v for k, v in r.items() if k != "ingest_ts"} for r in rows]
             for p, rows in w.by_path.items()}  # fmt: skip
    n_calls = len(vci.calls)

    col2, w2 = make(tmp_path, [vci])  # cùng file state
    r2 = col2.run()
    assert r2.skipped == ["daily_ohlcv/FPT/2025"]
    assert r2.done == ["daily_ohlcv/FPT/2026#live"]
    assert len(vci.calls) == n_calls + 1
    assert list(w2.by_path) == ["/data/raw/daily_ohlcv/source=vci/year=2026/FPT.parquet"]
    again = {p: [{k: v for k, v in r.items() if k != "ingest_ts"} for r in rows]
             for p, rows in w2.by_path.items()}  # fmt: skip
    assert again[next(iter(again))] == first[next(iter(again))]


def test_year_rollover_refetches_once(tmp_path):
    vci = FakeSource("vci", vci_data())
    col, _ = make(tmp_path, [vci])
    col.run()
    later = DailyOhlcvCollector(
        [vci], JsonFileStateStore(tmp_path / "s.json"), FakeWriter(), ["FPT"],
        date(2026, 1, 1), date(2027, 1, 2), now=datetime(2027, 1, 2, 3, tzinfo=UTC), to_table=IDENT,
    )  # fmt: skip
    res = later.run()
    assert res.done == ["daily_ohlcv/FPT/2026", "daily_ohlcv/FPT/2027#live"]


# ------------------------------------------------------------- empty


def test_empty_past_year_is_done_without_file(tmp_path):
    vci = FakeSource("vci", {("FPT", 2026): vci_data()[("FPT", 2026)]})
    col, w = make(tmp_path, [vci])
    res = col.run()
    assert res.failed == [] and "daily_ohlcv/FPT/2025" in res.done
    assert list(w.by_path) == ["/data/raw/daily_ohlcv/source=vci/year=2026/FPT.parquet"]


def test_empty_current_year_no_file_but_done(tmp_path):
    col, w = make(tmp_path, [FakeSource("vci", {})])
    res = col.run()
    assert len(res.done) == 2 and w.writes == []


# ------------------------------------------------------------- fallback


def test_fallback_to_second_source(tmp_path):
    vci = FakeSource("vci", error=RuntimeError("boom"))
    vnd = FakeSource(
        "vndirect",
        {
            ("FPT", 2026): [bar("FPT", date(2026, 3, 2), "vndirect")],
            ("FPT", 2025): [bar("FPT", date(2025, 3, 3), "vndirect")],
        },
    )
    col, w = make(tmp_path, [vci, vnd])
    res = col.run()
    assert res.failed == []
    assert sorted(w.by_path) == [
        "/data/raw/daily_ohlcv/source=vndirect/year=2025/FPT.parquet",
        "/data/raw/daily_ohlcv/source=vndirect/year=2026/FPT.parquet",
    ]
    assert col.per_source["vndirect"]["rows"] == 2


def test_blocked_first_source_not_called_again(tmp_path):
    vci = FakeSource("vci", error=SourceBlockedError("x", 429))
    data = {**vci_data(), ("VNM", 2025): [bar("VNM", date(2025, 4, 1), "vndirect")]}
    vnd = FakeSource("vndirect", data)
    col, w = make(tmp_path, [vci, vnd], ("FPT", "VNM"))
    res = col.run()
    assert len(vci.calls) == 1  # bị chặn ở lần đầu, các unit sau bỏ qua
    assert not res.stopped_blocked and res.failed == []
    assert len(w.writes) == 4


def test_empty_from_fallback_fails_while_primary_blocked(tmp_path):
    """Nguồn phụ trả rỗng không xác nhận được (chỉ số, trước 2013): failed, chạy lại sau."""
    vci = FakeSource("vci", error=SourceBlockedError("x", 429))
    col, w = make(tmp_path, [vci, FakeSource("vndirect", vci_data())], ("VNM",))
    res = col.run()
    assert res.failed == ["daily_ohlcv/VNM/2025"] and len(w.writes) == 1


def test_all_blocked_stops(tmp_path):
    a = FakeSource("vci", error=SourceBlockedError("x", 429))
    b = FakeSource("vndirect", error=SourceBlockedError("y", 403))
    col, w = make(tmp_path, [a, b], ("FPT", "VNM"))
    res = col.run()
    assert res.stopped_blocked and len(res.failed) == 1 and w.writes == []


def test_all_fail_marks_failed_and_continues(tmp_path):
    a, b = FakeSource("vci", error=RuntimeError("a")), FakeSource("vndirect", error=OSError("b"))
    col, w = make(tmp_path, [a, b], ("FPT", "VNM"))
    res = col.run()
    assert len(res.failed) == 4 and not res.stopped_blocked and w.writes == []
    st = JsonFileStateStore(tmp_path / "s.json")
    assert st.get("daily_ohlcv", "daily_ohlcv/FPT/2025")["status"] == "failed"


def test_index_empty_on_fallback_is_failure(tmp_path):
    vci = FakeSource("vci", error=RuntimeError("down"))
    vnd = FakeSource("vndirect", {})  # VNDirect không có chỉ số: trả rỗng
    col, w = make(tmp_path, [vci, vnd], ("VNINDEX",))
    res = col.run()
    assert len(res.failed) == 2 and w.writes == []


def test_writer_error_marks_failed(tmp_path):
    class Boom(FakeWriter):
        def write(self, table, path):
            raise OSError("hdfs down")

    state = JsonFileStateStore(tmp_path / "s.json")
    col = DailyOhlcvCollector(
        [FakeSource("vci", vci_data())], state, Boom(), ["FPT"], date(2026, 1, 1),
        date(2026, 11, 24), now=NOW, to_table=IDENT,
    )  # fmt: skip
    res = col.run()
    assert res.failed == ["daily_ohlcv/FPT/2026#live"]


# ------------------------------------------------------------- entry points


def test_load_symbols():
    conf = {"sample": {"stocks": ["fpt", "VNM", "FPT"], "indices": ["VNINDEX"]}}
    assert load_symbols(conf) == ["FPT", "VNM", "VNINDEX"]
    assert load_symbols({"stocks": ["A"]}) == ["A"]


def test_run_daily_ohlcv_with_injected(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "rows_to_table", IDENT, raising=True)
    w = FakeWriter()
    res = run_daily_ohlcv(
        ["FPT"], start=date(2026, 1, 1), end=date(2026, 11, 24), writer=w,
        sources=[FakeSource("vci", vci_data())], state=JsonFileStateStore(tmp_path / "s.json"),
    )  # fmt: skip
    assert len(res.done) == 1 and len(w.writes) == 1


def test_summary_dict(tmp_path):
    out = run_daily_ohlcv_summary(
        ["FPT"], start=date(2026, 1, 1), end=date(2026, 11, 24), writer=FakeWriter(),
        sources=[FakeSource("vci", error=RuntimeError("x"))],
        state=JsonFileStateStore(tmp_path / "s.json"),
    )  # fmt: skip
    assert out["failed"] == 1 and out["done"] == 0 and out["stopped_blocked"] is False


def test_state_path_env(monkeypatch):
    monkeypatch.setenv("COLLECTOR_STATE_DIR", "/tmp/zz")
    assert mod._state_path() == "/tmp/zz/ingestion_state.json"
    monkeypatch.delenv("COLLECTOR_STATE_DIR")
    assert mod._state_path() == "/var/lib/stocklake/state/ingestion_state.json"


def load_job():
    p = Path(__file__).parent.parent.parent / "jobs" / "ingest" / "daily_ohlcv.py"
    spec = importlib.util.spec_from_file_location("job_daily", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.mark.parametrize(
    "summary,code",
    [
        ({"failed": 0, "stopped_blocked": False}, 0),
        ({"failed": 2, "stopped_blocked": False}, 1),
        ({"failed": 0, "stopped_blocked": True}, 1),
    ],
)
def test_job_exit_code(monkeypatch, capsys, summary, code):
    job = load_job()
    seen = {}

    def fake(symbols, start, end, years_back):
        seen.update(symbols=symbols, start=start, years_back=years_back)
        return summary

    monkeypatch.setattr(job, "run_daily_ohlcv_summary", fake)
    monkeypatch.setattr("sys.argv", ["x", "--symbols", "fpt,vnm", "--start", "2025-01-01"])
    assert job.main() == code
    assert json.loads(capsys.readouterr().out) == summary
    assert seen == {"symbols": ["FPT", "VNM"], "start": date(2025, 1, 1), "years_back": 1}


# ------------------------------------------------------------- pyarrow


def test_rows_to_table_types():
    pa = pytest.importorskip("pyarrow")
    rows = build_rows([bar("FPT", date(2026, 11, 10), "vci")], NOW)
    t = mod.rows_to_table(rows)
    assert t.column_names == list(file_columns())
    assert t.schema.field("trade_date").type == pa.date32()
    assert t.schema.field("volume").type == pa.int64()
    assert t.schema.field("available_at").type == pa.timestamp("us", tz="UTC")
    assert "source" not in t.column_names and "year" not in t.column_names
