import json

import pytest

from stocklake.common.http import SourceBlockedError
from stocklake.ingestion.framework import (
    AllSourcesFailedError,
    BaseCollector,
    JsonFileStateStore,
    WorkUnit,
    fetch_with_fallback,
)
from stocklake.sources.base import SourceClient


class Src(SourceClient):
    def __init__(self, name, result=None, error=None):
        self.name = name
        self.result = result
        self.error = error
        self.calls = 0

    def get(self, key):
        self.calls += 1
        if self.error:
            raise self.error
        return self.result or [key]


# ------------------------------------------------------------- fallback


def test_fallback_order_and_source_name():
    a, b, c = Src("a", error=ValueError("x")), Src("b", result=[1, 2]), Src("c")
    res, name = fetch_with_fallback([a, b, c], lambda s: s.get("k"))
    assert (res, name) == ([1, 2], "b")
    assert (a.calls, b.calls, c.calls) == (1, 1, 0)


def test_all_failed_carries_errors_and_blocked():
    e1, e2 = ValueError("v"), SourceBlockedError("blocked", 429)
    with pytest.raises(AllSourcesFailedError) as ei:
        fetch_with_fallback([Src("a", error=e1), Src("b", error=e2)], lambda s: s.get("k"))
    assert ei.value.errors == [("a", e1), ("b", e2)]
    assert ei.value.blocked_sources == ["b"]
    assert "a:" in str(ei.value) and "b:" in str(ei.value)


def test_blocked_source_falls_through():
    a = Src("a", error=SourceBlockedError("x", 403))
    res, name = fetch_with_fallback([a, Src("b")], lambda s: s.get("k"))
    assert name == "b" and res == ["k"]


def test_no_sources():
    with pytest.raises(AllSourcesFailedError):
        fetch_with_fallback([], lambda s: 1)


# ------------------------------------------------------------- state


def test_state_round_trip_and_atomic(tmp_path):
    p = tmp_path / "sub" / "state.json"
    st = JsonFileStateStore(p)
    assert not st.is_done("c", "k")
    st.mark_failed("c", "k", "boom")
    assert not st.is_done("c", "k")
    st.mark_done("c", "k", {"records": 3})
    st.mark_done("c", "k2")
    assert st.is_done("c", "k")
    st2 = JsonFileStateStore(p)  # đọc lại từ đĩa
    assert st2.is_done("c", "k") and st2.is_done("c", "k2") and not st2.is_done("d", "k")
    assert st2.get("c", "k")["meta"] == {"records": 3}
    assert json.loads(p.read_text(encoding="utf-8"))["c"]["k"]["status"] == "done"
    assert [f.name for f in p.parent.iterdir()] == ["state.json"]  # không sót file tạm


# ------------------------------------------------------------- collector


class Fake(BaseCollector):
    name = "fake"

    def __init__(self, sources, state, keys=("A", "B", "C")):
        super().__init__(sources, state)
        self.keys = keys
        self.written = []

    def plan(self):
        return [WorkUnit(k, {"symbol": k}) for k in self.keys]

    def fetch(self, unit, source):
        return source.get(unit.key)

    def write(self, unit, records, source_name):
        self.written.append((unit.key, records, source_name))


def test_run_idempotent(tmp_path):
    st = JsonFileStateStore(tmp_path / "s.json")
    src = Src("a")
    col = Fake([src], st)
    r1 = col.run()
    assert r1.done == ["A", "B", "C"] and r1.skipped == [] and not r1.stopped_blocked
    r2 = Fake([src], JsonFileStateStore(tmp_path / "s.json")).run()
    assert r2.done == [] and r2.skipped == ["A", "B", "C"]
    assert src.calls == 3
    assert st.get("fake", "A")["meta"] == {"source": "a", "records": 1}


def test_run_uses_fallback_and_records_failure(tmp_path):
    st = JsonFileStateStore(tmp_path / "s.json")

    class Picky(Src):
        def get(self, key):
            if key == "B":
                raise ValueError("no B")
            return super().get(key)

    col = Fake([Picky("a"), Picky("b")], st)
    r = col.run()
    assert r.failed == ["B"] and r.done == ["A", "C"]
    assert not st.is_done("fake", "B") and "no B" in st.get("fake", "B")["error"]
    assert [w[2] for w in col.written] == ["a", "a"]


def test_blocked_source_stops_run(tmp_path):
    st = JsonFileStateStore(tmp_path / "s.json")
    blocked = Src("a", error=SourceBlockedError("x", 429))
    col = Fake([blocked], st)
    r = col.run()
    assert r.stopped_blocked and r.failed == ["A"] and r.done == []
    assert blocked.calls == 1  # không gọi tiếp B, C
    assert not st.is_done("fake", "A")


def test_blocked_primary_skipped_for_rest_of_run(tmp_path):
    st = JsonFileStateStore(tmp_path / "s.json")
    a = Src("a", error=SourceBlockedError("x", 403))
    b = Src("b")
    r = Fake([a, b], st).run()
    assert r.done == ["A", "B", "C"] and not r.stopped_blocked
    assert a.calls == 1 and b.calls == 3


def test_write_error_marks_failed_and_continues(tmp_path):
    st = JsonFileStateStore(tmp_path / "s.json")

    class BadWrite(Fake):
        def write(self, unit, records, source_name):
            if unit.key == "A":
                raise OSError("disk")
            super().write(unit, records, source_name)

    r = BadWrite([Src("a")], st).run()
    assert r.failed == ["A"] and r.done == ["B", "C"]
