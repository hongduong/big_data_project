import json
from datetime import datetime, timezone

import pytest

from stocklake.contracts.topics import INTRADAY_PRICES
from stocklake.streaming.producers.base import serialize
from stocklake.streaming.producers.sample import build_sample_intraday

NOW = datetime(2026, 11, 24, 3, 0, 30, tzinfo=timezone.utc)


def test_sample_is_valid():
    rec = build_sample_intraday("FPT", now=NOW)
    key, value = serialize(rec, INTRADAY_PRICES)
    assert key == b"FPT"
    assert json.loads(value) == rec
    assert b" " not in value  # JSON gọn
    assert rec["source"] == "replay"
    assert rec["available_at"] == rec["bar_ts"] + 60_000
    assert rec["bar_ts"] % 60_000 == 0
    assert rec["produced_ts"] == int(NOW.timestamp() * 1000)


def test_missing_required_field_rejected():
    rec = build_sample_intraday(now=NOW)
    del rec["close"]
    with pytest.raises(ValueError, match="close"):
        serialize(rec, INTRADAY_PRICES)


def test_null_required_field_rejected():
    rec = build_sample_intraday(now=NOW)
    rec["volume"] = None
    with pytest.raises(ValueError, match="volume"):
        serialize(rec, INTRADAY_PRICES)


def test_unknown_field_rejected():
    rec = build_sample_intraday(now=NOW)
    rec["extra"] = 1
    with pytest.raises(ValueError, match="extra"):
        serialize(rec, INTRADAY_PRICES)


def test_nullable_field_may_be_absent():
    rec = build_sample_intraday(now=NOW)
    del rec["replay_run_id"]
    serialize(rec, INTRADAY_PRICES)
