"""Phân loại khác biệt giá giữa file đang có và dữ liệu mới (hàm thuần)."""

import pytest

from stocklake.ingestion.framework import JsonFileStateStore
from stocklake.ingestion.price_adjustment import (
    IRREGULAR,
    MINOR,
    NO_OVERLAP,
    SCALED,
    UNCHANGED,
    adjustment_events,
    compare_prices,
    event_key,
    tolerance,
)


def row(i, p, vol=100):
    """Nến khoá i, giá quanh p, bốn giá khác nhau (2 chữ số thập phân)."""
    return {"k": i, "open": round(p, 2), "high": round(p * 1.004, 2),
            "low": round(p * 0.996, 2), "close": round(p * 1.001, 2), "volume": vol}  # fmt: skip


def series(n, base, step):
    return [row(i, base + step * (i % 37)) for i in range(n)]


def scale(rows, f):
    """Nguồn điều chỉnh: nhân mọi giá với f rồi làm tròn 2 chữ số (như VCI)."""
    return [
        {**r, **{c: round(r[c] * f, 2) for c in ("open", "high", "low", "close")}} for r in rows
    ]


KEY = lambda r: r["k"]  # noqa: E731


def cmp(old, new, **kw):
    return compare_prices(old, new, KEY, **kw)


def test_no_overlap_and_unchanged():
    old = series(50, 22000, 13.37)
    assert cmp(old, [{**r, "k": r["k"] + 1000} for r in old]).kind == NO_OVERLAP
    c = cmp(old, old)
    assert c.kind == UNCHANGED and c.overlap == 50 and c.changed == 0 and c.factor == 1.0


def test_volume_only_change_is_unchanged_price():
    old = series(50, 22000, 13.37)
    new = [{**r, "volume": r["volume"] + 7} if r["k"] % 5 == 0 else r for r in old]
    c = cmp(old, new)
    assert c.kind == UNCHANGED and c.volume_changed == 10


@pytest.mark.parametrize("base,step", [(3000, 1.0), (3950, 7.5), (150000, 250), (22309.84, 13.37)])
@pytest.mark.parametrize("f", [0.9775, 0.999, 0.5, 1.15])
def test_scaled_low_and_high_prices_with_rounding(base, step, f):
    old = series(400, base, step)
    c = cmp(old, scale(old, f))
    assert c.kind == SCALED, c
    assert c.matched == 400 and c.changed == 400
    assert c.factor == pytest.approx(f, abs=2e-6)


def test_rounding_noise_at_factor_one_is_unchanged():
    old = series(100, 3000, 1.0)
    new = [{**r, "close": round(r["close"] + 0.01 * (1 if r["k"] % 2 else -1), 2)} for r in old]
    assert cmp(old, new).kind == UNCHANGED


def test_tolerance_boundary_absolute():
    assert tolerance(3000, 1.0) == pytest.approx(0.02, abs=1e-5)
    assert tolerance(150000, 0.9775) == pytest.approx(0.019775 + 150000e-9, abs=1e-9)
    old = series(100, 3000, 1.0)
    within = [{**r, "close": r["close"] + 0.019} if r["k"] == 3 else r for r in old]
    assert cmp(old, within).kind == UNCHANGED
    beyond = [{**r, "close": r["close"] + 0.05} if r["k"] == 3 else r for r in old]
    c = cmp(old, beyond)
    assert c.kind == MINOR and c.changed == 1 and c.examples[0]["key"] == "3"


def test_scaled_tolerates_up_to_one_percent_outliers():
    old = series(300, 22000, 13.37)
    new = scale(old, 0.9775)
    new[10] = {**new[10], "close": new[10]["close"] + 40}  # nguồn còn sửa 1 nến khác
    new[20] = {**new[20], "high": new[20]["high"] + 15}
    c = cmp(old, new)
    assert c.kind == SCALED and c.matched == 298
    new[30] = {**new[30], "low": new[30]["low"] - 9}
    new[40] = {**new[40], "open": new[40]["open"] - 9}  # 4/300 > 1%
    assert cmp(old, new).kind == IRREGULAR


def test_min_overlap_boundary():
    old = series(30, 3500, 3.0)
    assert cmp(old, scale(old, 0.9775), min_overlap=30).kind == SCALED
    c = cmp(old[:29], scale(old[:29], 0.9775), min_overlap=30)
    assert c.kind == IRREGULAR and c.reason == "too_few_overlap" and c.examples


def test_few_bars_revised_is_minor_many_is_irregular():
    old = series(2000, 22000, 13.37)
    new = [{**r, "close": r["close"] + 100} if r["k"] < 2 else r for r in old]
    c = cmp(old, new)
    assert c.kind == MINOR and c.changed == 2 and c.reason == "few_bars_revised"
    # 0,1% của 2000 = 2 nến; tối thiểu 3 -> 3 nến vẫn là minor, 4 nến là irregular
    new = [{**r, "close": r["close"] + 100} if r["k"] < 4 else r for r in old]
    c = cmp(old, new)
    assert c.kind == IRREGULAR and c.reason == "not_single_ratio" and len(c.examples) == 4


def test_half_scaled_half_unchanged_is_irregular():
    old = series(200, 22000, 13.37)
    new = scale(old[:100], 0.9775) + old[100:]
    c = cmp(old, new)
    assert c.kind == IRREGULAR and c.changed == 100 and c.overlap == 200
    d = c.to_dict()
    assert d["kind"] == "irregular" and len(d["examples"]) == 5


def test_only_high_low_changed_not_scaled():
    old = series(200, 22000, 13.37)
    new = [{**r, "high": round(r["high"] * 1.01, 2), "low": round(r["low"] * 0.99, 2)} for r in old]
    assert cmp(old, new).kind == IRREGULAR


def test_adjustment_events_history(tmp_path):
    st = JsonFileStateStore(tmp_path / "s.json")
    st.mark_done("c", event_key("c", "FPT", "r1"), {"detected_at": "2026-11-25T01:00:00+00:00"})
    st.mark_failed(
        "c", event_key("c", "FPT", "r2"), "x", {"detected_at": "2026-11-27T01:00:00+00:00"}
    )
    st.mark_done("c", event_key("c", "VNM", "r3"), {"detected_at": "2026-11-26T01:00:00+00:00"})
    st.mark_done("c", "c/FPT/2025")
    again = JsonFileStateStore(tmp_path / "s.json")  # đọc lại từ file
    ev = adjustment_events(again, "c", "FPT")
    assert [e["key"] for e in ev] == ["c/FPT/_adjustment/r1", "c/FPT/_adjustment/r2"]
    assert [e["status"] for e in ev] == ["done", "failed"]
    assert len(adjustment_events(again, "c")) == 3
    assert adjustment_events(object(), "c") == []
