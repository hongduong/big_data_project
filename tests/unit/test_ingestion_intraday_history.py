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
    bars_identical,
    build_rows,
    dedup_bars,
    file_columns,
    file_path,
    group_rows_by_month,
    in_trading_hours,
    load_symbols,
    month_runs,
    regroup_history_files,
    run_intraday_history,
    split_by_year,
    vn_month,
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
        self.writes = []  # (path, [nhóm dòng theo tháng])

    def write_groups(self, groups, path):
        self.writes.append((path, list(groups)))

    @property
    def by_path(self):
        """path -> toàn bộ dòng (các nhóm nối lại)."""
        return {p: [r for g in gs for r in g] for p, gs in self.writes}

    @property
    def groups_by_path(self):
        return dict(self.writes)


class FakeReader:
    """Giả HdfsHistoryReader: path -> (số row group, dòng đã có)."""

    def __init__(self, store=None):
        self.store = dict(store or {})
        self.calls = []

    def read_existing(self, path, min_row_groups):
        self.calls.append((path, min_row_groups))
        if path not in self.store:
            return None
        n_groups, rows = self.store[path]
        return None if n_groups < min_row_groups else rows

    @classmethod
    def after(cls, writer, groups_override=None):
        """Reader mô tả các file mà `writer` vừa ghi (giữ nguyên số row group)."""
        return cls({p: (len(gs), [r for g in gs for r in g]) for p, gs in writer.writes})


def mins(sym, first, n):
    return [bar(sym, first + timedelta(minutes=i)) for i in range(n)]


def make(tmp_path, client, symbols=("FPT",), since=date(2025, 1, 1), clock=lambda: NOW, **kw):
    state = JsonFileStateStore(tmp_path / "s.json")
    w = FakeWriter()
    kw.setdefault("reader", FakeReader())
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


# ------------------------------------------------------------- row group theo tháng


def utc(*a):
    return datetime(*a, tzinfo=UTC)


def test_vn_month_boundaries():
    assert vn_month(utc(2025, 10, 31, 16, 59)) == (2025, 10)
    assert vn_month(utc(2025, 10, 31, 17, 0)) == (2025, 11)  # 00:00 01/11 giờ VN
    assert vn_month(utc(2025, 12, 31, 17, 0)) == (2026, 1)  # qua năm mới


def test_month_runs():
    assert month_runs([]) == []
    assert month_runs([(1, 1), (1, 1), (1, 2), (1, 2), (1, 2)]) == [(0, 2), (2, 3)]


def test_group_rows_by_month_sorted_around_boundaries():
    ts = [utc(2025, 11, 30, 16, 59), utc(2025, 10, 31, 17, 0), utc(2025, 10, 31, 16, 59),
          utc(2025, 11, 30, 17, 0)]  # fmt: skip
    rows = build_rows([bar("FPT", t) for t in ts], NOW, "r")  # cố ý lộn xộn
    groups = group_rows_by_month(rows)
    assert [[r["bar_ts"] for r in g] for g in groups] == [
        [utc(2025, 10, 31, 16, 59)],
        [utc(2025, 10, 31, 17, 0), utc(2025, 11, 30, 16, 59)],
        [utc(2025, 11, 30, 17, 0)],
    ]  # 10/2025 | 11/2025 | 12/2025 giờ VN


def test_writer_gets_one_group_per_vn_month_in_order(tmp_path):
    bars = [
        bar("FPT", utc(2026, 1, 31, 16, 59)),  # 23:59 31/01 VN
        bar("FPT", utc(2026, 1, 31, 17, 0)),  # 00:00 01/02 VN
        bar("FPT", utc(2026, 1, 5, 3, 0)),
        bar("FPT", utc(2026, 3, 2, 3, 0)),
    ]
    col, w, _ = make(tmp_path, FakeClient({"FPT": bars}), since=date(2026, 1, 1))
    col.run()
    groups = w.groups_by_path["/data/raw/intraday_history/source=vci/year=2026/FPT.parquet"]
    assert [[r["bar_ts"] for r in g] for g in groups] == [
        [utc(2026, 1, 5, 3, 0), utc(2026, 1, 31, 16, 59)],
        [utc(2026, 1, 31, 17, 0)],
        [utc(2026, 3, 2, 3, 0)],
    ]


def test_new_year_split_files_each_month_grouped(tmp_path):
    bars = [bar("FPT", utc(2025, 12, 31, 16, 59)), bar("FPT", utc(2025, 12, 31, 17, 0))]
    col, w, _ = make(tmp_path, FakeClient({"FPT": bars}), since=date(2025, 1, 1))
    col.run()
    assert {p[-21:]: len(g) for p, g in w.groups_by_path.items()} == {
        "year=2025/FPT.parquet": 1,
        "year=2026/FPT.parquet": 1,
    }


# ------------------------------------------------------------- không ghi lại khi không đổi

P26 = "/data/raw/intraday_history/source=vci/year=2026/FPT.parquet"


def rerun(tmp_path, data, reader):
    col, w, st = make(tmp_path, FakeClient(data), reader=reader)
    return col.run(), w


def first_run(tmp_path, data):
    col, w, _ = make(tmp_path, FakeClient(data))
    col.run()
    return w


def test_unchanged_current_year_not_rewritten(tmp_path):
    data = two_years()
    reader = FakeReader.after(first_run(tmp_path, data))
    out, w = rerun(tmp_path, data, reader)
    assert list(w.by_path) == []  # 2025 đã done (bỏ qua); 2026 y hệt -> không ghi
    assert out["files_unchanged"] == 1 and out["files_written"] == 0 and out["rows_written"] == 0
    assert out["symbols_done"] == ["FPT"]


def test_unchanged_only_ingest_columns_differ(tmp_path):
    data = {"FPT": mins("FPT", T26, 5)}
    w1 = first_run(tmp_path, data)
    path, rows = next(iter(w1.by_path.items()))
    rows = [{**r, "ingest_ts": T26, "ingest_run_id": "khac", "available_at": T26} for r in rows]
    out, w = rerun(tmp_path, data, FakeReader({path: (1, rows)}))
    assert out["files_unchanged"] == 1 and w.writes == []


def test_extra_bar_rewrites(tmp_path):
    w1 = first_run(tmp_path, {"FPT": mins("FPT", T26, 5)})
    out, w = rerun(tmp_path, {"FPT": mins("FPT", T26, 6)}, FakeReader.after(w1))
    assert out["files_written"] == 1 and out["files_unchanged"] == 0 and out["rows_written"] == 6
    assert len(w.by_path[P26]) == 6


def test_changed_close_rewrites(tmp_path):
    data = {"FPT": mins("FPT", T26, 5)}
    reader = FakeReader.after(first_run(tmp_path, data))
    data["FPT"][2] = bar("FPT", data["FPT"][2].bar_ts, close=99.0)
    out, w = rerun(tmp_path, data, reader)
    assert out["files_written"] == 1 and out["files_unchanged"] == 0
    assert [r["close"] for r in w.by_path[P26]].count(99.0) == 1


def test_single_row_group_file_is_upgraded_even_if_identical(tmp_path):
    bars = [bar("FPT", utc(2026, 1, 5, 3, 0)), bar("FPT", utc(2026, 3, 2, 3, 0))]  # 2 tháng
    w1 = first_run(tmp_path, {"FPT": bars})
    rows = w1.by_path[P26]
    reader = FakeReader({P26: (1, rows)})  # file cũ: 1 row group, dữ liệu y hệt
    out, w = rerun(tmp_path, {"FPT": bars}, reader)
    assert reader.calls == [(P26, 2)]
    assert out["files_written"] == 1 and out["files_unchanged"] == 0
    assert len(w.groups_by_path[P26]) == 2


def test_reader_error_falls_back_to_rewrite(tmp_path):
    class Bad:
        def read_existing(self, path, n):
            raise OSError("hdfs lỗi")

    out, w = rerun(tmp_path, {"FPT": mins("FPT", T26, 3)}, Bad())
    assert out["files_written"] == 1 and out["files_unchanged"] == 0


def test_bars_identical_order_insensitive_and_strict():
    rows = build_rows(mins("FPT", T26, 3), NOW, "a")
    ex = [{k: r[k] for k in ("bar_ts", "open", "high", "low", "close", "volume")} for r in rows]
    assert bars_identical(list(reversed(ex)), rows)
    assert not bars_identical(ex[:2], rows)
    assert not bars_identical([{**ex[0], "volume": 1}, *ex[1:]], rows)
    naive = [{**ex[0], "bar_ts": ex[0]["bar_ts"].replace(tzinfo=None)}, *ex[1:]]
    assert bars_identical(naive, rows)  # bar_ts không múi giờ coi là UTC


# ------------------------------------------------------------- chạy lại / idempotent


def test_rerun_skips_past_year_and_rewrites_current(tmp_path):
    data = two_years()
    col, w, st = make(tmp_path, FakeClient(data))
    col.run()
    first = w.by_path
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
        reader=FakeReader(),
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


def test_regroup_job_exit_codes(monkeypatch, capsys):
    path = Path(__file__).parents[2] / "jobs/ingest/regroup_intraday_history.py"
    spec = importlib.util.spec_from_file_location("job_regroup", path)
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)
    seen = {}

    def fake(symbols, years, dry_run):
        seen.update(symbols=symbols, years=years, dry_run=dry_run)
        return {"failed": [{"path": "p", "reason": "x"}] if dry_run else []}

    monkeypatch.setattr(job, "regroup_history_files", fake)
    monkeypatch.setattr(sys, "argv", ["x", "--symbols", "fpt,vnm", "--years", "2025", "--dry-run"])
    assert job.main() == 1
    assert seen == {"symbols": ["FPT", "VNM"], "years": [2025], "dry_run": True}
    monkeypatch.setattr(sys, "argv", ["x"])
    assert job.main() == 0
    assert len(capsys.readouterr().out.strip().splitlines()) == 2


# ------------------------------------------------------------- regroup / ghi thật (pyarrow)
# Chạy trong image collector; ở máy không có pyarrow/fsspec thì bỏ qua.

REGROUP_TS = [
    utc(2025, 10, 31, 16, 59),
    utc(2025, 10, 31, 17, 0),
    utc(2025, 11, 5, 3, 0),
    utc(2025, 11, 30, 17, 0),
    utc(2025, 12, 20, 3, 0),
]  # fmt: skip  -> tháng VN: 10 | 11 11 | 12 12


class Env:
    def __init__(self):
        self.pa = pytest.importorskip("pyarrow")
        self.pq = pytest.importorskip("pyarrow.parquet")
        fsspec = pytest.importorskip("fsspec")
        self.fs = fsspec.filesystem("memory")
        self.base = f"/t{id(self):x}"
        self.fs.makedirs(self.base, exist_ok=True)

    def put(self, sym, year, bars, row_group_size=None):
        rows = build_rows(sorted(bars, key=lambda b: b.bar_ts), NOW, "rid")
        t = mod.rows_to_table(rows)
        path = file_path(sym, year, self.base)
        self.fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        with self.fs.open(path, "wb") as f:
            self.pq.write_table(t, f, row_group_size=row_group_size)
        return path, t

    def info(self, path):
        with self.fs.open(path, "rb") as f:
            md = self.pq.ParquetFile(f).metadata
            return md.num_rows, md.num_row_groups

    def read(self, path):
        with self.fs.open(path, "rb") as f:
            return self.pq.read_table(f)

    def files(self):
        return sorted(self.fs.find(self.base))


@pytest.fixture
def env():
    return Env()


def test_regroup_splits_by_month_and_preserves_data(env):
    path, orig = env.put("FPT", 2025, [bar("FPT", t, 1.0 + i) for i, t in enumerate(REGROUP_TS)])
    assert env.info(path) == (5, 1)
    out = regroup_history_files(["FPT"], [2025], fs=env.fs, base=env.base)
    assert out["files_total"] == 1 and out["regrouped"] == 1 and out["failed"] == []
    assert out["rows_total_before"] == out["rows_total_after"] == 5
    assert env.info(path) == (5, 3)
    assert env.read(path).equals(orig.sort_by("bar_ts"))
    assert env.files() == [path]  # không sót file tạm/bak
    assert out["files"] == [
        {
            "path": path,
            "status": "regrouped",
            "rows": 5,
            "row_groups_before": 1,
            "row_groups_after": 3,
        }  # fmt: skip
    ]
    row_groups = env.pq.ParquetFile(env.fs.open(path, "rb")).metadata
    assert [row_groups.row_group(i).num_rows for i in range(3)] == [1, 2, 2]


def test_regroup_skips_already_grouped(env):
    env.put("FPT", 2025, [bar("FPT", t) for t in REGROUP_TS])
    regroup_history_files(fs=env.fs, base=env.base)
    before = env.files()
    out = regroup_history_files(fs=env.fs, base=env.base)
    assert out["already_grouped"] == 1 and out["regrouped"] == 0 and env.files() == before


def test_regroup_dry_run_replaces_nothing(env):
    path, orig = env.put("FPT", 2025, [bar("FPT", t) for t in REGROUP_TS])
    raw_before = env.fs.cat(path)
    out = regroup_history_files(dry_run=True, fs=env.fs, base=env.base)
    assert out["dry_run"] and out["regrouped"] == 1 and out["files"][0]["row_groups_after"] == 3
    assert env.fs.cat(path) == raw_before and env.files() == [path]


def test_regroup_verification_failure_keeps_original(env, monkeypatch):
    import stocklake.common.hdfs as hdfs

    path, _ = env.put("FPT", 2025, [bar("FPT", t) for t in REGROUP_TS])
    raw_before = env.fs.cat(path)
    real = hdfs.write_parquet_row_groups_atomic

    def lossy(tables, p, fs=None):  # đánh rơi dòng cuối của nhóm cuối
        tables = list(tables)
        tables[-1] = tables[-1].slice(0, 0)
        real(tables, p, fs=fs)

    monkeypatch.setattr(hdfs, "write_parquet_row_groups_atomic", lossy)
    out = regroup_history_files(fs=env.fs, base=env.base)
    assert out["regrouped"] == 0 and len(out["failed"]) == 1 and out["critical"] == []
    assert out["failed"][0]["path"] == path
    assert env.fs.cat(path) == raw_before and env.files() == [path]  # file tạm đã xoá


def test_regroup_filters_symbols_and_years(env):
    env.put("FPT", 2025, [bar("FPT", t) for t in REGROUP_TS])
    other, _ = env.put("VNM", 2025, [bar("VNM", t) for t in REGROUP_TS])
    out = regroup_history_files(["FPT"], [2025], fs=env.fs, base=env.base)
    assert out["files_total"] == 1 and env.info(other) == (5, 1)


def test_collector_real_io_month_groups_unchanged_and_upgrade(tmp_path, env):
    from stocklake.ingestion.collectors.intraday_history import HdfsHistoryReader, HdfsParquetWriter

    data = {"FPT": []}
    clock = lambda: utc(2026, 11, 24, 12, 0)  # noqa: E731

    def run():
        st = JsonFileStateStore(tmp_path / "s.json")
        col = IntradayHistoryCollector(
            FakeClient(data), st, HdfsParquetWriter(env.fs), ["FPT"], date(2026, 1, 1),
            clock=clock, base=env.base, reader=HdfsHistoryReader(env.fs),
        )  # fmt: skip
        return col.run()

    data["FPT"] = [bar("FPT", utc(2026, m, d, 3, 0)) for m, d in ((1, 5), (1, 6), (2, 5), (4, 5))]
    p = file_path("FPT", 2026, env.base)
    out = run()
    assert out["files_written"] == 1 and env.info(p) == (4, 3)
    before = env.fs.cat(p)
    out = run()
    assert out["files_unchanged"] == 1 and out["files_written"] == 0 and env.fs.cat(p) == before
    # file cũ một row group, cùng dữ liệu -> được nâng cấp
    env.put("FPT", 2026, data["FPT"])
    assert env.info(p) == (4, 1)
    out = run()
    assert out["files_written"] == 1 and env.info(p) == (4, 3)
