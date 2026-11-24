import datetime as dt

import pytest

from stocklake.contracts import get_schema, to_spark

pytestmark = pytest.mark.spark
pytest.importorskip("pyspark")

RAW = get_schema("raw", "intraday_prices")
CLEAN = get_schema("clean", "intraday_1m")
UTC = dt.timezone.utc


def utc(*args):
    return dt.datetime(*args, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("test_etl_intraday")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def _nullable(schema):
    from pyspark.sql import types as T

    return T.StructType([T.StructField(f.name, f.dataType, True) for f in schema.fields])


def raw_row(symbol="FPT", bar=None, source="vci", c=105.0, kafka=0, ingest=0, offset=0):
    bar = bar or utc(2026, 1, 5, 3, 0)
    t = utc(2026, 1, 5, 4, 0, 0)
    k = utc(2026, 1, 5, 4, 0, kafka)
    i = utc(2026, 1, 5, 4, 0, ingest)
    return {
        "symbol": symbol,
        "bar_ts": bar,
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": c,
        "volume": 10,
        "produced_ts": t,
        "replay_run_id": None,
        "trade_date": dt.date(2026, 1, 5),
        "source": source,
        "ingest_ts": i,
        "available_at": bar + dt.timedelta(minutes=1),
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_ts": k,
        "spark_batch_ts": t,
    }


def make(spark, rows):
    schema = _nullable(to_spark(RAW))
    return spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema)


def clean(spark, rows):
    from stocklake.etl.raw_to_clean.intraday import clean_intraday

    return clean_intraday(make(spark, rows))


def test_output_matches_contract(spark):
    df = clean(spark, [raw_row()])
    expected = to_spark(CLEAN)
    assert df.columns == [*CLEAN.column_names, *CLEAN.derived_partitions]
    for f in expected.fields:
        assert df.schema[f.name].dataType == f.dataType, f.name
    row = df.collect()[0]
    assert (row["year"], row["month"]) == (2026, 1)


def test_real_source_beats_replay_even_if_older(spark):
    bar = utc(2026, 1, 5, 3, 1)
    rows = [
        raw_row(bar=bar, source="replay", c=1.0, kafka=50, ingest=50),
        raw_row(bar=bar, source="vci", c=2.0, kafka=1, ingest=1),
    ]
    out = clean(spark, rows).collect()
    assert len(out) == 1 and out[0]["close"] == 2.0


def test_same_priority_latest_kafka_ts_wins(spark):
    bar = utc(2026, 1, 5, 3, 2)
    rows = [
        raw_row(bar=bar, source="vci", c=1.0, kafka=1),
        raw_row(bar=bar, source="kbs", c=3.0, kafka=9),
        raw_row(bar=bar, source="vci", c=2.0, kafka=5),
    ]
    out = clean(spark, rows).collect()
    assert len(out) == 1 and out[0]["close"] == 3.0


def test_duplicates_collapse_per_symbol_bar(spark):
    b1, b2 = utc(2026, 1, 5, 3, 0), utc(2026, 1, 5, 3, 1)
    rows = [
        raw_row("FPT", b1, offset=1),
        raw_row("FPT", b1, offset=2),
        raw_row("FPT", b2),
        raw_row("VNM", b1),
    ]
    assert clean(spark, rows).count() == 3


def test_vietnam_date_around_midnight_utc(spark):
    rows = [
        raw_row("A", utc(2026, 1, 5, 16, 59)),  # 23:59 VN, 5/1
        raw_row("B", utc(2026, 1, 5, 17, 0)),  # 00:00 VN, 6/1
        raw_row("C", utc(2025, 12, 31, 17, 0)),  # 00:00 VN, 1/1/2026
        raw_row("D", utc(2026, 1, 31, 17, 0)),  # 00:00 VN, 1/2/2026
    ]
    r = {row["symbol"]: row for row in clean(spark, rows).collect()}
    assert r["A"]["trade_date"] == dt.date(2026, 1, 5)
    assert r["B"]["trade_date"] == dt.date(2026, 1, 6)
    assert (r["C"]["trade_date"], r["C"]["year"], r["C"]["month"]) == (dt.date(2026, 1, 1), 2026, 1)
    assert (r["D"]["trade_date"], r["D"]["month"]) == (dt.date(2026, 2, 1), 2)


def test_null_required_rejected_and_counted(spark):
    from stocklake.etl.raw_to_clean.intraday import count_rejected

    bad = raw_row("BAD")
    bad["close"] = None
    rows = [bad, raw_row("OK")]
    assert count_rejected(make(spark, rows)) == 1
    assert [r["symbol"] for r in clean(spark, rows).collect()] == ["OK"]


def test_idempotent(spark):
    bar = utc(2026, 1, 5, 3, 0)
    rows = [raw_row(bar=bar, kafka=1), raw_row(bar=bar, kafka=2, c=7.0), raw_row("VNM", bar)]
    a = sorted(map(tuple, clean(spark, rows).collect()))
    b = sorted(map(tuple, clean(spark, rows).collect()))
    assert a == b
