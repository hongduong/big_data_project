import datetime as dt
import json
import pathlib

import pytest

from stocklake.common.config import Settings
from stocklake.dq.notes import (
    EXCLUDE_VOLUME_MINUTE_GT_DAILY,
    DqNotes,
    KnownAnomaly,
    KnownPeriod,
    KnownSourceGap,
    annotate,
    load_dq_notes,
    parse_notes,
)

CONF_DIR = pathlib.Path(__file__).resolve().parents[2] / "conf" / "app"


def test_repo_dq_notes_yaml_is_valid():
    pytest.importorskip("yaml")
    notes = load_dq_notes(Settings(app_conf_dir=str(CONF_DIR)))
    assert notes.source and notes.source.endswith("dq_notes.yaml")
    # 4 mục BSR/MCH + 51 mục của 26 mã nghi chuyển sàn (rà 28/11/2026)
    assert len(notes.known_periods) == 55
    assert len({p.symbol for p in notes.known_periods}) == 28
    assert [(p.symbol, p.label) for p in notes.known_periods[:4]] == [
        ("BSR", "truoc_chuyen_san"),
        ("BSR", "khoang_trong_chuyen_san"),
        ("MCH", "truoc_chuyen_san"),
        ("MCH", "khoang_trong_chuyen_san"),
    ]
    periods = {(p.symbol, p.label): p for p in notes.known_periods[:4]}
    assert len(notes.known_anomalies) == 6
    # khoảng trống nguồn: 7 mã, 66 (mã, ngày)
    gaps = {g.symbol: g for g in notes.known_source_gaps}
    assert set(gaps) == {"BTT", "CLW", "COM", "DTT", "LGC", "SBV", "SC5"}
    assert {s: len(g.dates) for s, g in gaps.items()} == {
        "BTT": 4,
        "CLW": 6,
        "COM": 12,
        "DTT": 7,
        "LGC": 6,
        "SBV": 20,
        "SC5": 11,
    }
    pairs = notes.source_gap_pairs()
    assert len(pairs) == len(set(pairs)) == 66
    for g in notes.known_source_gaps:
        assert g.label == "nguon_khong_co_nen_phut"
        assert f"cho {len(g.dates)} ngày" in g.note and "giữ nguyên ở raw và clean" in g.note
    assert ("DTT", dt.date(2024, 11, 20)) in pairs
    assert periods[("BSR", "truoc_chuyen_san")].start is None
    assert periods[("BSR", "truoc_chuyen_san")].end == dt.date(2025, 1, 6)
    assert periods[("BSR", "khoang_trong_chuyen_san")].end == dt.date(2025, 1, 16)
    assert periods[("MCH", "truoc_chuyen_san")].end == dt.date(2025, 12, 17)
    assert periods[("MCH", "khoang_trong_chuyen_san")].end == dt.date(2025, 12, 24)
    for p in notes.known_periods:
        assert p.symbol == p.symbol.upper() and "Suy ra từ dữ liệu" in p.note
        assert "giữ nguyên ở raw và clean" in p.note
    a, *source_days = notes.known_anomalies
    assert a.date == dt.date(2024, 11, 15) and a.symbols == ()
    assert "chưa giải thích" in a.status and "24/11/2026" in a.status
    assert a.exclude_from == ()  # 2024-11-15 KHÔNG bị loại khỏi kiểm tra nào
    # 5 ngày nguồn có nến phút thừa: toàn thị trường, loại khỏi kiểm tra cứng phút > ngày
    affected = {
        dt.date(2025, 5, 6): 21,
        dt.date(2025, 5, 26): 7,
        dt.date(2025, 11, 20): 13,
        dt.date(2026, 3, 16): 5,
        dt.date(2026, 11, 23): 1,
    }
    assert {x.date for x in source_days} == set(affected)
    for x in source_days:
        assert x.symbols == () and x.exclude_from == (EXCLUDE_VOLUME_MINUTE_GT_DAILY,)
        assert f"{affected[x.date]} mã" in x.note
        assert "chưa rõ nguyên nhân phía nguồn" in x.note.lower()
        assert "giữ nguyên ở raw và clean" in x.note
    assert sum(affected.values()) == 47
    excluded = notes.anomalies_excluded_from(EXCLUDE_VOLUME_MINUTE_GT_DAILY)
    assert {x.date for x in excluded} == set(affected)
    js = notes.to_json()
    assert js["known_anomalies"][0]["date"] == "2024-11-15"
    assert js["known_anomalies"][0]["exclude_from"] == []
    assert js["known_anomalies"][1]["exclude_from"] == ["volume_minute_gt_daily"]
    assert {p["end"] for p in js["known_periods"]} >= {"2025-01-06", "2025-12-17"}
    assert len(js["known_source_gaps"]) == 7
    assert sum(len(g["dates"]) for g in js["known_source_gaps"]) == 66
    json.dumps(js)


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
    assert notes.known_anomalies[0].exclude_from == ()  # thiếu trường: mặc định rỗng
    assert parse_notes(None) == DqNotes()


def test_parse_exclude_from():
    notes = parse_notes(
        {
            "known_anomalies": [
                {"date": "2025-05-06", "label": "a", "exclude_from": ["volume_minute_gt_daily"]},
                {"date": "2025-05-07", "label": "b", "exclude_from": None},
                {"date": "2025-05-08", "label": "c"},
            ]
        }
    )
    a, b, c = notes.known_anomalies
    assert a.exclude_from == ("volume_minute_gt_daily",)
    assert b.exclude_from == () and c.exclude_from == ()
    assert notes.anomalies_excluded_from("volume_minute_gt_daily") == (a,)
    assert notes.anomalies_excluded_from("khac") == ()
    js = notes.to_json()
    assert [x["exclude_from"] for x in js["known_anomalies"]] == [
        ["volume_minute_gt_daily"],
        [],
        [],
    ]
    json.dumps(js)
    # nhãn ghi chú vẫn gắn như cũ, không phụ thuộc exclude_from
    assert annotate([{"symbol": "X", "trade_date": "2025-05-06"}], notes)[0]["note"] == "a"
    assert KnownAnomaly(dt.date(2025, 1, 1), "z").exclude_from == ()


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
        {"known_anomalies": [{"date": "2024-11-15", "label": "x", "exclude_from": ["sai_ten"]}]},
        {"known_anomalies": [{"date": "2024-11-15", "label": "x", "exclude_from": "a"}]},
        {"known_anomalies": [{"date": "2024-11-15", "label": "x", "exclude_from": [["a"]]}]},
        ["not", "a", "mapping"],
        # known_source_gaps
        {"known_source_gaps": [{"symbol": "dtt", "dates": ["2024-11-20"], "label": "x"}]},
        {"known_source_gaps": [{"symbol": "DTT", "dates": [], "label": "x"}]},
        {"known_source_gaps": [{"symbol": "DTT", "label": "x"}]},
        {"known_source_gaps": [{"symbol": "DTT", "dates": "2024-11-20", "label": "x"}]},
        {"known_source_gaps": [{"symbol": "DTT", "dates": ["2024-02-30"], "label": "x"}]},
        {"known_source_gaps": [{"symbol": "DTT", "dates": ["20/11/2024"], "label": "x"}]},
        {"known_source_gaps": [{"symbol": "DTT", "dates": [None], "label": "x"}]},
        {"known_source_gaps": [{"symbol": "DTT", "dates": ["2024-11-20"]}]},
        {
            "known_source_gaps": [
                {"symbol": "DTT", "dates": ["2024-11-20", "2024-11-20"], "label": "x"}
            ]
        },
        {
            "known_source_gaps": [
                {"symbol": "DTT", "dates": ["2024-11-20"], "label": "x"},
                {"symbol": "DTT", "dates": ["2024-11-20"], "label": "y"},
            ]
        },
        {"known_source_gaps": {"symbol": "DTT"}},
        # hai giai đoạn của cùng mã chồng nhau
        {
            "known_periods": [
                {"symbol": "BSR", "end": "2025-01-06", "label": "a"},
                {"symbol": "BSR", "start": "2025-01-06", "end": "2025-01-16", "label": "b"},
            ]
        },
        {
            "known_periods": [
                {"symbol": "BSR", "start": "2025-01-07", "end": "2025-01-16", "label": "b"},
                {"symbol": "BSR", "end": "2025-01-10", "label": "a"},
            ]
        },
        {
            "known_periods": [
                {"symbol": "BSR", "end": "2025-01-06", "label": "a"},
                {"symbol": "BSR", "end": "2024-01-06", "label": "b"},
            ]
        },
        {
            "known_periods": [
                {"symbol": "BSR", "start": "2025-01-07", "label": "a"},
                {"symbol": "BSR", "start": "2026-01-01", "end": "2026-02-01", "label": "b"},
            ]
        },
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


def test_periods_of_same_symbol_may_touch_but_not_overlap():
    notes = parse_notes(
        {
            "known_periods": [
                {"symbol": "BSR", "start": "2025-01-07", "end": "2025-01-16", "label": "b"},
                {"symbol": "BSR", "end": "2025-01-06", "label": "a"},
                {"symbol": "BSR", "start": "2025-01-17", "label": "c"},
                {"symbol": "MCH", "end": "2025-01-10", "label": "a"},  # mã khác: không xét
            ]
        }
    )
    assert [p.label for p in notes.known_periods] == ["b", "a", "c", "a"]  # giữ thứ tự file


def test_parse_known_source_gaps_and_annotate():
    notes = parse_notes(
        {
            "known_periods": [{"symbol": "BSR", "end": "2025-01-06", "label": "truoc"}],
            "known_source_gaps": [
                {
                    "symbol": "DTT",
                    "dates": ["2023-10-16", dt.date(2024, 11, 20)],
                    "label": "nguon_khong_co_nen_phut",
                    "note": " ghi chú ",
                },
                {"symbol": "SBV", "dates": ["2024-11-20"], "label": "khac"},
            ],
        }
    )
    dtt, sbv = notes.known_source_gaps
    assert dtt == KnownSourceGap(
        "DTT", (dt.date(2023, 10, 16), dt.date(2024, 11, 20)), "nguon_khong_co_nen_phut", "ghi chú"
    )
    assert notes.source_gap_pairs() == (
        ("DTT", dt.date(2023, 10, 16)),
        ("DTT", dt.date(2024, 11, 20)),
        ("SBV", dt.date(2024, 11, 20)),
    )
    assert dtt.contains("DTT", dt.date(2024, 11, 20)) and not dtt.contains(
        "DTT", dt.date(2024, 11, 21)
    )
    assert not dtt.contains("SBV", dt.date(2024, 11, 20))
    rows = [
        {"symbol": "DTT", "trade_date": "2024-11-20"},
        {"symbol": "DTT", "trade_date": "2024-11-21"},
        {"symbol": "SBV", "trade_date": dt.date(2024, 11, 20)},
        {"symbol": "DTT"},  # danh sách theo mã
        {"symbol": "BSR", "trade_date": "2024-11-20"},
    ]
    assert [r["note"] for r in annotate(rows, notes)] == [
        "nguon_khong_co_nen_phut",
        None,
        "khac",
        "nguon_khong_co_nen_phut",
        "truoc",
    ]
    js = notes.to_json()
    assert js["known_source_gaps"][0] == {
        "symbol": "DTT",
        "dates": ["2023-10-16", "2024-11-20"],
        "label": "nguon_khong_co_nen_phut",
        "note": "ghi chú",
    }
    json.dumps(js)
    # thiếu mục: rỗng
    assert parse_notes({"known_periods": []}).known_source_gaps == ()
    assert parse_notes({"known_source_gaps": None}).source_gap_pairs() == ()
    assert DqNotes().to_json()["known_source_gaps"] == []
