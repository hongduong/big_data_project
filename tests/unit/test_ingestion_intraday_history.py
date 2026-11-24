import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from stocklake.common.http import SourceBlockedError
from stocklake.ingestion.collectors import intraday_history as mod
from stocklake.ingestion.collectors.intraday_history import (
    IntradayHistoryCollector,
    build_rows,
    dedup_bars,
    file_columns,
    file_path,
    in_trading_hours,
    load_symbols,
    run_intraday_history,
    split_by_year,
    years_needed,
)
from stocklake.ingestion.framework import JsonFileStateStore
from stocklake.sources.base import MinuteBar
from stocklake.sources.vci import MinutePage

UTC = timezone.utc
NOW = datetime(2026, 11, 24, 12, 0, tzinfo=UTC)  # 19:00 thứ 3 giờ VN: ngoài giờ giao dịch
IDENT = lambda rows: rows  # noqa: E731 - thay bước dict -> pyarrow.Table


def bar(sym, ts, close=10.0):
    return MinuteBar(sym, ts, close, close, close, close, 100, "vci")


class FakeClient:
    """Giả VciClient.minute_pages: trang mới -> cũ, lọc [start, end), lỗi sau `fail_after` trang."""

    def __init__(self, data, page_size=3, overlap=0, fail_after=None, error=None):
        self.data = data  # symbol -> list[MinuteBar]
        self.page_size, self.overlap = page_size, overlap
        self.fail_after, self.error = fail_after, error
        self.calls = []

    def minute_pages(self, symbol, start, end, max_pages=None, resume_to=None, page_size=0):
        self.calls.append((symbol, start, end))
        bars = sorted(
            (b for b in self.data.get(symbol, []) if start <= b.bar_ts < end),
            key=lambda b: b.bar_ts,
            reverse=True,
        )
        n, i = 0, 0
        while i < len(bars):
            if self.fail_after is not None and n >= self.fail_after:
                raise self.error
            chunk = bars[max(0, i - self.overlap) if i else 0 : i + self.page_size]
            i += self.page_size
            n += 1
            yield MinutePage(list(reversed(chunk)), None if i >= len(bars) else 1)


class FakeWriter:
    def __init__(self):
        self.writes = []

    def write(self, table, path):
        self.writes.append((path, table))

    @property
    def by_path(self):
        return dict(self.writes)


def mins(sym, first, n):
    return [bar(sym, first + timedelta(minutes=i)) for i in range(n)]


def make(tmp_path, client, symbols=("FPT",), since=date(2025, 1, 1), clock=lambda: NOW, **kw):
    state = JsonFileStateStore(tmp_path / "s.json")
    w = FakeWriter()
    col = IntradayHistoryCollector(
        client, state, w, symbols, since, clock=clock, to_table=IDENT, **kw
    )
    return col, w, state


T25 = datetime(2025, 6, 2, 3, 0, tzinfo=UTC)
T26 = datetime(2026, 11, 10, 3, 0, tzinfo=UTC)


def two_years():
    return {"FPT": mins("FPT", T25, 4) + mins("FPT", T26, 5)}


# ------------------------------------------------------------- trạng thái / kế hoạch


def test_years_needed_skips_done_past_but_not_current(tmp_path):
    st = JsonFileStateStore(tmp_path / "s.json")
    assert years_needed("FPT", 2024, 2026, st) == [2024, 2025, 2026]
    st.mark_done("intraday_history", "intraday_history/FPT/2024")
    st.mark_done("intraday_history", "intraday_history/FPT/2026")  # khoá năm nay không tính
    st.mark_done("intraday_history", "intraday_history/FPT/2026#live")
    assert years_needed("FPT", 2024, 2026, st) == [2025, 2026]


def test_plan_keys_live_suffix(tmp_path):
    col, _, _ = make(tmp_path, FakeClient({}), since=date(2025, 3, 1))
    assert [u.key for u in col.plan("FPT", NOW)] == [
        "intraday_history/FPT/2025",
        "intraday_history/FPT/2026#live",
    ]


def test_only_current_year_needed_fetches_back_to_jan_1(tmp_path):
    cli = FakeClient(two_years())
    col, w, st = make(tmp_path, cli)
    st.mark_done("intraday_history", "intraday_history/FPT/2025")
    out = col.run()
    sym, start, end = cli.calls[0]
    assert start == datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=7)))
    assert start.astimezone(UTC) == datetime(2025, 12, 31, 17, 0, tzinfo=UTC)
    assert list(w.by_path) == ["/data/raw/intraday_history/source=vci/year=2026/FPT.parquet"]
    assert out["symbols_done"] == ["FPT"] and out["per_year_rows"] == {"2026": 5}


def test_fetch_back_to_earliest_needed_year(tmp_path):
    cli = FakeClient(two_years())
    col, w, _ = make(tmp_path, cli)
    col.run()
    assert cli.calls[0][1].year == 2025 and (cli.calls[0][1].month, cli.calls[0][1].day) == (1, 1)
    assert len(w.writes) == 2


# ------------------------------------------------------------- tách năm / bố cục / cột


def test_split_by_vietnam_year_around_new_year():
    a = bar("FPT", datetime(2025, 12, 31, 16, 59, tzinfo=UTC))  # 23:59 31/12 VN
    b = bar("FPT", datetime(2025, 12, 31, 17, 0, tzinfo=UTC))  # 00:00 01/01/2026 VN
    out = split_by_year([a, b])
    assert out == {2025: [a], 2026: [b]}


def test_path_layout_and_partition_columns_absent(tmp_path):
    cli = FakeClient(two_years())
    col, w, _ = make(tmp_path, cli)
    col.run()
    assert set(w.by_path) == {
        "/data/raw/intraday_history/source=vci/year=2025/FPT.parquet",
        "/data/raw/intraday_history/source=vci/year=2026/FPT.parquet",
    }
    assert (
        file_path("VNM", 2026, base="/x")
        == "/x/raw/intraday_history/source=vci/year=2026/VNM.parquet"
    )
    cols = file_columns()
    assert "source" not in cols and "year" not in cols
    assert cols[:2] == ("symbol", "bar_ts") and cols[-1] == "ingest_run_id"
    for rows in w.by_path.values():
        assert all(tuple(r) == cols for r in rows)


def test_rows_available_at_run_id_and_sorted(tmp_path):
    cli = FakeClient({"FPT": mins("FPT", T26, 5)}, page_size=2)
    col, w, _ = make(tmp_path, cli, since=date(2026, 1, 1))
    out = col.run()
    rows = next(iter(w.by_path.values()))
    ts = [r["bar_ts"] for r in rows]
    assert ts == sorted(ts) and len(ts) == 5
    assert all(r["available_at"] - r["bar_ts"] == timedelta(seconds=60) for r in rows)
    assert all(r["ingest_ts"] == NOW and r["ingest_run_id"] == out["run_id"] for r in rows)
    assert out["run_id"].startswith("h20261124T120000Z-") and len(out["run_id"]) == 18 + 8
    assert out["requests"] == 3 and out["rows_written"] == 5 and out["files_written"] == 1


def test_build_rows_values():
    r = build_rows([bar("FPT", T26, 7.5)], NOW, "rid")[0]
    assert "source" not in r
    assert r["close"] == 7.5 and r["volume"] == 100 and r["symbol"] == "FPT"


def test_incomplete_last_bar_excluded(tmp_path):
    last_done = NOW - timedelta(minutes=1)  # bar_ts + 60 s == now: đủ điều kiện
    still_open = NOW - timedelta(seconds=30)
    cli = FakeClient({"FPT": [bar("FPT", last_done), bar("FPT", still_open)]})
    col, w, _ = make(tmp_path, cli, since=date(2026, 1, 1))
    col.run()
    rows = next(iter(w.by_path.values()))
    assert [r["bar_ts"] for r in rows] == [last_done]


def test_overlap_dedup_counts_dropped(tmp_path):
    cli = FakeClient({"FPT": mins("FPT", T26, 6)}, page_size=3, overlap=1)
    col, w, _ = make(tmp_path, cli, since=date(2026, 1, 1))
    out = col.run()
    rows = next(iter(w.by_path.values()))
    assert len(rows) == 6 and out["duplicates_dropped"] == 1


def test_dedup_bars_keeps_first_sorted():
    a, b = bar("X", T26, 1.0), bar("X", T26 + timedelta(minutes=1))
    out, n = dedup_bars([b, a, bar("X", T26, 2.0)])
    assert n == 1 and [x.bar_ts for x in out] == [a.bar_ts, b.bar_ts] and out[0].close == 1.0


# ------------------------------------------------------------- chạy lại / idempotent


def test_rerun_skips_past_year_and_rewrites_current(tmp_path):
    data = two_years()
    col, w, st = make(tmp_path, FakeClient(data))
    col.run()
    first = {p: list(r) for p, r in w.writes}
    assert st.is_done("intraday_history", "intraday_history/FPT/2025")
    assert not st.is_done("intraday_history", "intraday_history/FPT/2026")

    cli2 = FakeClient(data)
    col2, w2, _ = make(tmp_path, cli2)
    out = col2.run()
    assert list(w2.by_path) == ["/data/raw/intraday_history/source=vci/year=2026/FPT.parquet"]
    p = "/data/raw/intraday_history/source=vci/year=2026/FPT.parquet"
    strip = lambda rows: [{k: v for k, v in r.items() if k != "ingest_run_id"} for r in rows]  # noqa: E731
    assert strip(w2.by_path[p]) == strip(first[p])
    assert cli2.calls[0][1].year == 2026 and out["symbols_done"] == ["FPT"]


def test_year_before_history_start_is_done_without_file(tmp_path):
    cli = FakeClient({"FPT": mins("FPT", T26, 3)})
    col, w, st = make(tmp_path, cli, since=date(2024, 1, 1))
    col.run()
    assert len(w.writes) == 1
    for y in (2024, 2025):
        assert st.is_done("intraday_history", f"intraday_history/FPT/{y}")
    assert st.get("intraday_history", "intraday_history/FPT/2024")["meta"]["records"] == 0


def test_all_done_symbol_is_skipped_only_when_no_current_year(tmp_path):
    cli = FakeClient(two_years())
    col, _, _ = make(tmp_path, cli)
    out = col.run()
    assert out["symbols_skipped"] == []  # năm nay luôn làm lại


# ------------------------------------------------------------- lỗi / bị chặn


def test_blocked_midsymbol_writes_nothing_and_state_untouched(tmp_path):
    exc = SourceBlockedError("chặn", 429, "u", "120", 120.0)
    data = {"FPT": mins("FPT", T25, 4) + mins("FPT", T26, 5), "VNM": mins("VNM", T26, 3)}
    cli = FakeClient(data, page_size=3, fail_after=1, error=exc)
    col, w, st = make(tmp_path, cli, symbols=("FPT", "VNM"))
    out = col.run()
    assert w.writes == []
    assert not st.is_done("intraday_history", "intraday_history/FPT/2025")
    assert st.get("intraday_history", "intraday_history/FPT/2025") is None
    assert [c[0] for c in cli.calls] == ["FPT"]  # không gọi VNM
    assert out["stopped_reason"] == "blocked" and out["symbols_done"] == []
    b = out["blocked"]
    assert b["symbol"] == "FPT" and b["status"] == 429
    assert b["retry_after_raw"] == "120" and b["retry_after_seconds"] == 120.0 and b["at"]
    entry = st.get("intraday_history", "intraday_history/_blocked")
    assert entry["meta"]["symbol"] == "FPT" and entry["meta"]["retry_after_seconds"] == 120.0
    saved = json.loads((tmp_path / "s.json").read_text())
    assert set(saved["intraday_history"]) == {"intraday_history/_blocked"}


def test_blocked_without_retry_after_attrs(tmp_path):
    class Old(SourceBlockedError):
        def __init__(self):
            Exception.__init__(self, "x")
            self.status = 403

    cli = FakeClient({"FPT": mins("FPT", T26, 3)}, fail_after=0, error=Old())
    col, _, _ = make(tmp_path, cli, since=date(2026, 1, 1))
    b = col.run()["blocked"]
    assert b["status"] == 403 and b["retry_after_raw"] is None and b["retry_after_seconds"] is None


def test_other_error_marks_symbol_failed_and_continues(tmp_path):
    class Boom(FakeClient):
        def minute_pages(self, symbol, *a, **k):
            if symbol == "FPT":
                raise RuntimeError("hỏng")
            yield from super().minute_pages(symbol, *a, **k)

    cli = Boom({"VNM": mins("VNM", T26, 3)})
    col, w, st = make(tmp_path, cli, symbols=("FPT", "VNM"), since=date(2026, 1, 1))
    out = col.run()
    assert out["symbols_failed"] == ["FPT"] and out["symbols_done"] == ["VNM"]
    assert st.get("intraday_history", "intraday_history/FPT/2026#live")["status"] == "failed"
    assert len(w.writes) == 1 and out["stopped_reason"] is None


def test_interrupted_non_block_writes_nothing(tmp_path):
    cli = FakeClient(two_years(), page_size=3, fail_after=1, error=RuntimeError("đứt"))
    col, w, st = make(tmp_path, cli)
    out = col.run()
    assert w.writes == [] and out["symbols_failed"] == ["FPT"]
    assert not st.is_done("intraday_history", "intraday_history/FPT/2025")


def test_max_symbols(tmp_path):
    data = {s: mins(s, T26, 2) for s in ("AAA", "BBB", "CCC")}
    col, _, _ = make(tmp_path, FakeClient(data), symbols=("AAA", "BBB", "CCC"),
                     since=date(2026, 1, 1), max_symbols=2)  # fmt: skip
    out = col.run()
    assert out["symbols_done"] == ["AAA", "BBB"] and out["stopped_reason"] == "max_symbols"


# ------------------------------------------------------------- giờ giao dịch


@pytest.mark.parametrize(
    "vn_dt,expected",
    [
        (datetime(2026, 11, 24, 8, 59), False),
        (datetime(2026, 11, 24, 9, 0), True),
        (datetime(2026, 11, 24, 11, 29), True),
        (datetime(2026, 11, 24, 11, 30), False),
        (datetime(2026, 11, 24, 12, 30), False),
        (datetime(2026, 11, 24, 13, 0), True),
        (datetime(2026, 11, 24, 14, 59), True),
        (datetime(2026, 11, 24, 15, 0), False),
        (datetime(2026, 11, 28, 10, 0), False),  # thứ 7
    ],
)
def test_in_trading_hours(vn_dt, expected):
    assert in_trading_hours(vn_dt.replace(tzinfo=mod.VN_TZ).astimezone(UTC)) is expected


def test_refuses_during_trading_hours(tmp_path):
    t = datetime(2026, 11, 24, 10, 0, tzinfo=mod.VN_TZ)
    cli = FakeClient(two_years())
    col, w, _ = make(tmp_path, cli, clock=lambda: t)
    out = col.run()
    assert out["stopped_reason"] == "trading_hours" and cli.calls == [] and w.writes == []


def test_allow_trading_hours_override(tmp_path):
    t = datetime(2026, 11, 24, 10, 0, tzinfo=mod.VN_TZ)
    col, w, _ = make(tmp_path, FakeClient(two_years()), clock=lambda: t, allow_trading_hours=True)
    assert col.run()["stopped_reason"] is None and w.writes


def test_stops_between_symbols_when_session_opens(tmp_path):
    times = iter(
        [
            datetime(2026, 11, 24, 8, 0, tzinfo=mod.VN_TZ),  # t0
            datetime(2026, 11, 24, 8, 0, tzinfo=mod.VN_TZ),  # trước mã 1
            datetime(2026, 11, 24, 9, 0, tzinfo=mod.VN_TZ),  # trước mã 2
        ]
    )
    last = datetime(2026, 11, 24, 9, 0, tzinfo=mod.VN_TZ)
    clock = lambda: next(times, last)  # noqa: E731
    data = {"AAA": mins("AAA", datetime(2026, 11, 20, 3, 0, tzinfo=UTC), 2), "BBB": []}
    col, _, _ = make(tmp_path, FakeClient(data), symbols=("AAA", "BBB"), clock=clock,
                     since=date(2026, 1, 1))  # fmt: skip
    out = col.run()
    assert out["symbols_done"] == ["AAA"] and out["stopped_reason"] == "trading_hours"


# ------------------------------------------------------------- điểm vào / symbols / job


def test_load_symbols_groups():
    conf = {"vn30": ["fpt", "VNM", "FPT"], "sample": {"stocks": ["A", "B"], "indices": ["VNINDEX"]}}
    assert load_symbols(conf, "vn30") == ["FPT", "VNM"]
    assert load_symbols(conf, "sample") == ["A", "B"]
    with pytest.raises(ValueError, match="rỗng"):
        load_symbols({"vn30": []}, "vn30")
    with pytest.raises(ValueError):
        load_symbols(conf, "xx")


def test_repo_symbols_yaml_vn30_has_30():
    yaml = pytest.importorskip("yaml")
    conf = yaml.safe_load((Path(__file__).parents[2] / "conf/app/symbols.yaml").read_text())
    syms = load_symbols(conf, "vn30")
    assert len(syms) == 30 and syms == sorted(syms)
    from stocklake.ingestion.collectors.daily_ohlcv import load_symbols as daily_load

    assert "FPT" in daily_load(conf)  # file vẫn đọc được bởi collector ngày


def test_run_intraday_history_entrypoint_explicit_symbols_win(tmp_path):
    w = FakeWriter()
    out = run_intraday_history(
        ["fpt"], group="vn30", since=date(2026, 1, 1),
        client=FakeClient({"FPT": mins("FPT", T26, 3)}), writer=w,
        state=JsonFileStateStore(tmp_path / "s.json"), clock=lambda: NOW, to_table=IDENT,
    )  # fmt: skip
    assert out["symbols_done"] == ["FPT"] and out["rows_written"] == 3
    with pytest.raises(ValueError):
        run_intraday_history(client=FakeClient({}))


def _job(monkeypatch, summary):
    path = Path(__file__).parents[2] / "jobs/ingest/intraday_history.py"
    spec = importlib.util.spec_from_file_location("job_intraday_history", path)
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)
    monkeypatch.setattr(job, "run_intraday_history", lambda *a, **k: summary)
    monkeypatch.setattr(sys, "argv", ["x", "--symbols", "FPT"])
    return job


base_summary = {"blocked": None, "stopped_reason": None, "symbols_failed": []}


@pytest.mark.parametrize(
    "patch,code",
    [
        ({}, 0),
        ({"symbols_failed": ["FPT"]}, 1),
        ({"blocked": {"symbol": "FPT"}, "stopped_reason": "blocked"}, 2),
        ({"stopped_reason": "trading_hours"}, 3),
    ],
)
def test_job_exit_codes(monkeypatch, capsys, patch, code):
    job = _job(monkeypatch, {**base_summary, **patch})
    assert job.main() == code
    assert len(capsys.readouterr().out.strip().splitlines()) == 1


def test_rows_to_table_schema_pyarrow():
    pa = pytest.importorskip("pyarrow")
    rows = build_rows([bar("FPT", T26)], NOW, "rid")
    t = mod.rows_to_table(rows)
    assert t.column_names == list(file_columns())
    assert pa.types.is_timestamp(t.schema.field("bar_ts").type)
    assert t.num_rows == 1
