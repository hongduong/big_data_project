import datetime as dt

import pytest

from stocklake.contracts import get_schema, to_spark

pytestmark = pytest.mark.spark
pytest.importorskip("pyspark")

RAW = get_schema("raw", "daily_ohlcv")
CLEAN = get_schema("clean", "daily_ohlcv")
UTC = dt.timezone.utc
D1 = dt.date(2026, 1, 5)
D2 = dt.date(2026, 1, 6)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("test_etl_daily")
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


def raw_row(
    symbol="FPT",
    d=D1,
    source="vci",
    unit="vnd",
    o=100.0,
    h=110.0,
    low=90.0,
    c=105.0,
    vol=1000,
    raw_close=None,
    ingest=0,
    available=None,
):
    ts = dt.datetime(2026, 1, 10, 0, 0, ingest, tzinfo=UTC)
    return (symbol, d, o, h, low, c, vol, raw_close, unit, source, ts, available or ts)


def make(spark, rows):
    return spark.createDataFrame(rows, _nullable(to_spark(RAW)))


def clean(spark, rows):
    from stocklake.etl.raw_to_clean.daily import clean_daily_ohlcv

    return clean_daily_ohlcv(make(spark, rows))


def by_key(df):
    return {(r["symbol"], r["trade_date"]): r for r in df.collect()}


def test_output_matches_contract(spark):
    df = clean(spark, [raw_row()])
    expected = to_spark(CLEAN)
    assert df.columns == [*CLEAN.column_names, *CLEAN.derived_partitions]
    for f in expected.fields:
        assert df.schema[f.name].dataType == f.dataType, f.name
    assert df.schema["year"].dataType.simpleString() == "int"
    assert df.collect()[0]["year"] == 2026


def test_unit_normalisation(spark):
    rows = [
        raw_row("AAA", unit="kvnd", o=10.0, h=11.0, low=9.0, c=10.5, raw_close=10.2),
        raw_row("BBB", unit="vnd", o=10.0, h=11.0, low=9.0, c=10.5, raw_close=10.2),
        raw_row("CCC", unit="vnd", c=6489.7, h=7000.0, low=6000.0, o=6100.0),
    ]
    r = by_key(clean(spark, rows))
    a, b, c = r[("AAA", D1)], r[("BBB", D1)], r[("CCC", D1)]
    assert (a["open"], a["high"], a["low"], a["close"]) == (10000.0, 11000.0, 9000.0, 10500.0)
    assert a["raw_close"] == pytest.approx(10200.0)
    assert b["close"] == 10.5 and b["raw_close"] == 10.2
    assert c["close"] == 6489.7  # không làm tròn
    assert a["raw_close"] is not None
    assert by_key(clean(spark, [raw_row(raw_close=None)]))[("FPT", D1)]["raw_close"] is None


def test_unknown_unit_and_null_keys_rejected(spark):
    from stocklake.etl.raw_to_clean.daily import count_rejected

    rows = [
        raw_row("AAA", unit="usd"),
        raw_row("BBB", unit=None),
        raw_row("CCC", d=None),
        raw_row(None),
        raw_row("OK"),
    ]
    assert count_rejected(make(spark, rows)) == 4
    out = clean(spark, rows)
    assert [r["symbol"] for r in out.collect()] == ["OK"]


def test_source_priority_and_latest_ingest(spark):
    rows = [
        raw_row("FPT", source="vndirect", unit="kvnd", c=1.0, h=2.0, low=0.5, o=1.0, ingest=5),
        raw_row("FPT", source="vci", c=100.0, ingest=1),
        raw_row("FPT", source="vci", c=101.0, ingest=9),  # vci mới nhất
        raw_row("FPT", source="other", c=999.0, h=1000.0, ingest=30),
        raw_row("VNM", source="vndirect", unit="kvnd", c=50.0, h=60.0, low=40.0, o=45.0),
        raw_row("VNM", source="other", c=1.0, ingest=50),
    ]
    out = clean(spark, rows)
    assert out.count() == 2
    r = by_key(out)
    assert r[("FPT", D1)]["source"] == "vci" and r[("FPT", D1)]["close"] == 101.0
    assert r[("VNM", D1)]["source"] == "vndirect" and r[("VNM", D1)]["close"] == 50000.0


def test_available_at_carried_from_chosen_row(spark):
    av = dt.datetime(2026, 1, 5, 8, 30, tzinfo=UTC)
    rows = [raw_row(source="vci", available=av), raw_row(source="vndirect", ingest=50)]
    row = clean(spark, rows).selectExpr("cast(available_at as long) AS t").collect()[0]
    assert row["t"] == int(av.timestamp())


def test_dq_flags(spark):
    rows = [
        raw_row("GOOD"),
        raw_row("NEG", o=0.0, h=10.0, low=-1.0, c=5.0),
        raw_row("HL", h=80.0, low=90.0, o=85.0, c=85.0),
        raw_row("RNG", c=200.0),
        raw_row("VOL", vol=-5),
    ]
    r = by_key(clean(spark, rows))
    flags = {k[0]: set(v["dq_flags"]) for k, v in r.items()}
    assert flags["GOOD"] == set()
    assert "non_positive_price" in flags["NEG"]
    assert "high_lt_low" in flags["HL"]
    assert flags["RNG"] == {"close_out_of_range"}
    assert flags["VOL"] == {"negative_volume"}
    assert len(r) == 5  # không bỏ dòng nào


def test_one_row_per_symbol_date_and_idempotent(spark):
    from pyspark.sql import functions as F

    from stocklake.etl.raw_to_clean.daily import clean_daily_ohlcv

    rows = [raw_row("FPT", d=D1), raw_row("FPT", d=D2), raw_row("FPT", d=D1, ingest=3)]
    first = clean_daily_ohlcv(make(spark, rows))
    assert first.count() == 2
    second = clean_daily_ohlcv(make(spark, rows))
    assert sorted(map(tuple, first.collect())) == sorted(map(tuple, second.collect()))
    # Đưa kết quả clean (đã là đồng) qua lại bước raw: giá không đổi.
    back = first.drop("dq_flags", "year").withColumn("price_unit", F.lit("vnd"))
    back = back.withColumn("ingest_ts", F.col("available_at"))
    third = clean_daily_ohlcv(back)
    assert sorted(map(tuple, first.collect())) == sorted(map(tuple, third.collect()))
