import datetime as dt
import pathlib

import pytest

from stocklake.common.config import Settings
from stocklake.dq.notes import DqNotes, KnownPeriod, annotate, load_dq_notes, parse_notes

CONF_DIR = pathlib.Path(__file__).resolve().parents[2] / "conf" / "app"


def test_repo_dq_notes_yaml_is_valid():
    pytest.importorskip("yaml")
    notes = load_dq_notes(Settings(app_conf_dir=str(CONF_DIR)))
    assert notes.source and notes.source.endswith("dq_notes.yaml")
    periods = {(p.symbol, p.label): p for p in notes.known_periods}
    assert set(periods) == {
        ("BSR", "truoc_chuyen_san"),
        ("BSR", "khoang_trong_chuyen_san"),
        ("MCH", "truoc_chuyen_san"),
        ("MCH", "khoang_trong_chuyen_san"),
    }
    assert periods[("BSR", "truoc_chuyen_san")].start is None
    assert periods[("BSR", "truoc_chuyen_san")].end == dt.date(2025, 1, 6)
    assert periods[("BSR", "khoang_trong_chuyen_san")].end == dt.date(2025, 1, 16)
    assert periods[("MCH", "truoc_chuyen_san")].end == dt.date(2025, 12, 17)
    assert periods[("MCH", "khoang_trong_chuyen_san")].end == dt.date(2025, 12, 24)
    for p in notes.known_periods:
        assert p.symbol == p.symbol.upper() and "Suy ra từ dữ liệu" in p.note
        assert "giữ nguyên ở raw và clean" in p.note
    (a,) = notes.known_anomalies
    assert a.date == dt.date(2024, 11, 15) and a.symbols == ()
    assert "chưa giải thích" in a.status and "24/11/2026" in a.status
    js = notes.to_json()
    assert js["known_anomalies"][0]["date"] == "2024-11-15"
    assert {p["end"] for p in js["known_periods"]} >= {"2025-01-06", "2025-12-17"}


def test_missing_file_gives_empty_notes(tmp_path):
    pytest.importorskip("yaml")
    notes = load_dq_notes(Settings(app_conf_dir=str(tmp_path)))
    assert notes == DqNotes() and notes.source is None


def test_parse_accepts_strings_and_dates():
    notes = parse_notes(
        {
            "known_periods": [
                {"symbol": "ABC", "start": "2025-01-01", "end": dt.date(2025, 2, 1), "label": "x"}
            ],
            "known_anomalies": [{"date": "2024-11-15", "label": "y", "symbols": ["VHM"]}],
        }
    )
    assert notes.known_periods[0].start == dt.date(2025, 1, 1)
    assert notes.known_anomalies[0].symbols == ("VHM",)
    assert parse_notes(None) == DqNotes()


@pytest.mark.parametrize(
    "conf",
    [
        {"known_periods": [{"symbol": "bsr", "end": "2025-01-06", "label": "x"}]},
        {"known_periods": [{"symbol": " BSR", "end": "2025-01-06", "label": "x"}]},
        {"known_periods": [{"symbol": "BSR", "end": "2025-13-01", "label": "x"}]},
        {"known_periods": [{"symbol": "BSR", "end": "06/01/2025", "label": "x"}]},
        {
            "known_periods": [
                {"symbol": "BSR", "start": "2025-02-01", "end": "2025-01-01", "label": "x"}
            ]
        },
        {"known_periods": [{"symbol": "BSR", "end": "2025-01-06"}]},
        {"known_periods": {"symbol": "BSR"}},
        {"known_anomalies": [{"label": "x"}]},
        {"known_anomalies": [{"date": dt.datetime(2024, 11, 15, 9), "label": "x"}]},
        {"known_anomalies": [{"date": "2024-11-15", "label": "x", "symbols": ["vhm"]}]},
        ["not", "a", "mapping"],
    ],
)
def test_parse_rejects_malformed(conf):
    with pytest.raises(ValueError):
        parse_notes(conf)


def test_annotate_rows():
    notes = parse_notes(
        {
            "known_periods": [{"symbol": "BSR", "end": "2025-01-06", "label": "truoc"}],
            "known_anomalies": [{"date": "2024-11-15", "label": "ngay_la"}],
        }
    )
    rows = [
        {"symbol": "BSR", "trade_date": "2024-11-15"},
        {"symbol": "BSR", "trade_date": dt.date(2025, 1, 7)},
        {"symbol": "VHM", "trade_date": "2024-11-15"},
        {"symbol": "BSR"},  # danh sách theo mã: mọi giai đoạn của mã
        {"symbol": "FPT"},
    ]
    assert [r["note"] for r in annotate(rows, notes)] == [
        "truoc, ngay_la",
        None,
        "ngay_la",
        "truoc",
        None,
    ]
    p = KnownPeriod("BSR", dt.date(2025, 1, 7), None, "sau")
    assert p.contains("BSR", dt.date(2030, 1, 1)) and not p.contains("BSR", dt.date(2025, 1, 6))
