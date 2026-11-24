import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

import pytest

from stocklake.contracts import (
    ALL_SCHEMAS,
    ALL_TOPICS,
    INTRADAY_PRICES,
    MARKET_NEWS,
    Column,
    TableSchema,
    available_at,
    checkpoint_path,
    get_schema,
    layer_root,
    table_path,
    to_markdown,
)
from stocklake.contracts.schemas import RAW_INTRADAY_HISTORY

UTC = timezone.utc

PLAN_TABLES = {
    "topic": {"intraday_prices", "market_news"},
    "raw": {
        "daily_ohlcv",
        "foreign_flow",
        "company_profile",
        "macro_daily",
        "intraday_history",
        "intraday_prices",
        "market_news",
    },
    "clean": {
        "daily_ohlcv",
        "foreign_flow",
        "company_profile",
        "macro_daily",
        "intraday_1m",
        "market_news",
        "trading_calendar",
    },
    "curated": {
        "market_daily",
        "symbol_daily_features",
        "dq_report",
        "ml_results",
        "benchmark_results",
    },
}


def test_registry_matches_plan():
    found: dict[str, set[str]] = {}
    for s in ALL_SCHEMAS:
        found.setdefault(s.layer, set()).add(s.name)
    assert found == PLAN_TABLES


@pytest.mark.parametrize("schema", ALL_SCHEMAS, ids=lambda s: f"{s.layer}.{s.name}")
def test_schema_keys_and_partitions(schema):
    names = set(schema.column_names)
    assert set(schema.primary_key) <= names
    for p in schema.partition_by:
        assert p in names or p in schema.derived_partitions
    assert get_schema(schema.layer, schema.name) is schema


@pytest.mark.parametrize("schema", [s for s in ALL_SCHEMAS if s.layer == "raw"])
def test_raw_has_metadata(schema):
    cols = {c.name: c for c in schema.columns}
    assert cols["source"].type == "string" and not cols["source"].nullable
    assert cols["ingest_ts"].type == "timestamp" and not cols["ingest_ts"].nullable
    assert cols["available_at"].type == "timestamp" and not cols["available_at"].nullable


def test_raw_intraday_history():
    s = get_schema("raw", "intraday_history")
    assert s is RAW_INTRADAY_HISTORY
    assert s.column_names == (
        "symbol", "bar_ts", "open", "high", "low", "close", "volume",
        "source", "ingest_ts", "available_at", "ingest_run_id",
    )  # fmt: skip
    assert all(not c.nullable for c in s.columns)
    assert s.primary_key == ("symbol", "bar_ts", "source")
    assert s.partition_by == ("source", "year") and s.derived_partitions == ("year",)
    assert not {"produced_ts", "replay_run_id"} & set(s.column_names)
    assert not [n for n in s.column_names if n.startswith("kafka_") or n == "spark_batch_ts"]
    assert table_path("raw", "intraday_history") == "/data/raw/intraday_history"


def test_schema_validation_errors():
    col = Column("a", "string")
    with pytest.raises(ValueError):
        TableSchema("t", "raw", (col, col), ("a",))
    with pytest.raises(ValueError):
        TableSchema("t", "raw", (col,), ("zzz",))
    with pytest.raises(ValueError):
        TableSchema("t", "raw", (col,), ("a",), partition_by=("year",))
    with pytest.raises(ValueError):
        Column("a", "varchar")
    with pytest.raises(KeyError):
        get_schema("raw", "nope")


def test_topics_consistent():
    assert {t.name for t in ALL_TOPICS} == {"intraday_prices", "market_news"}
    assert INTRADAY_PRICES.partitions == 6 and INTRADAY_PRICES.key_field == "symbol"
    assert MARKET_NEWS.partitions == 1 and MARKET_NEWS.key_field == "news_id"
    for t in ALL_TOPICS:
        assert t.schema.layer == "topic" and t.schema.name == t.name
        assert t.key_field in t.schema.column_names
        assert set(t.dedup_key) <= set(t.schema.column_names)
        assert t.dedup_key == t.schema.primary_key


def test_paths():
    assert layer_root("raw") == "/data/raw"
    assert table_path("raw", "daily_ohlcv") == "/data/raw/daily_ohlcv"
    assert table_path("clean", "intraday_1m", base="/x/") == "/x/clean/intraday_1m"
    assert checkpoint_path("kafka_to_raw") == "/checkpoints/kafka_to_raw"
    with pytest.raises(ValueError):
        layer_root("topic")


def test_available_at_rules():
    assert available_at.daily_bar(date(2026, 11, 20)) == datetime(2026, 11, 20, 8, 30, tzinfo=UTC)
    assert available_at.foreign_flow(date(2026, 11, 20)) == datetime(
        2026, 11, 20, 11, 0, tzinfo=UTC
    )
    t = datetime(2026, 11, 20, 2, 15, tzinfo=UTC)
    assert available_at.intraday_bar(t) == t + timedelta(minutes=1)
    assert available_at.news(t) == t
    # yfinance: D+1 08:00 VN = D+1 01:00 UTC
    assert available_at.macro_yfinance(date(2026, 11, 20)) == datetime(
        2026, 11, 21, 1, 0, tzinfo=UTC
    )
    # FRED: D+2 theo ngày lịch (thứ Sáu -> thứ Bảy... cộng đúng 2 ngày)
    assert available_at.macro_fred(date(2026, 11, 20)) == datetime(2026, 11, 22, 1, 0, tzinfo=UTC)
    assert available_at.company_profile(t) == t
    assert available_at.corporate_event(date(2026, 11, 20)) == datetime(
        2026, 11, 19, 17, 0, tzinfo=UTC
    )


def test_available_at_returns_aware_utc_and_converts_zones():
    vn = timezone(timedelta(hours=7))
    out = available_at.news(datetime(2026, 11, 20, 9, 0, tzinfo=vn))
    assert out == datetime(2026, 11, 20, 2, 0, tzinfo=UTC)
    assert out.utcoffset() == timedelta(0)
    with pytest.raises(ValueError):
        available_at.news(datetime(2026, 11, 20, 9, 0))


def test_to_markdown():
    md = to_markdown()
    assert "## raw.daily_ohlcv" in md and "`feature_available_at`" in md
    assert len(md) > 1000


@pytest.mark.parametrize("module", ["stocklake.contracts", "stocklake.common"])
def test_import_without_heavy_deps(module):
    code = (
        f"import sys, {module}; "
        "bad = [m for m in ('pyspark', 'pyarrow', 'fsspec') if m in sys.modules]; "
        "sys.exit(1 if bad else 0)"
    )
    assert subprocess.run([sys.executable, "-c", code]).returncode == 0
