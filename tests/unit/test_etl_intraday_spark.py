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


def raw_row(
    symbol="FPT", bar=None, source="vci", c=105.0, kafka=0, ingest=0, offset=0, run_id="auto"
):
    # Bản ghi replay thật luôn có run id; nguồn thật thì không.
    if run_id == "auto":
        run_id = "r-test" if source == "replay" else None
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
        "replay_run_id": run_id,
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


def hist_row(symbol="FPT", bar=None, source="vci", c=105.0, ingest=0):
    bar = bar or utc(2026, 1, 5, 3, 0)
    return {
        "symbol": symbol,
        "bar_ts": bar,
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": c,
        "volume": 10,
        "source": source,
        "ingest_ts": utc(2026, 1, 6, 0, 0, ingest),
        "available_at": bar + dt.timedelta(minutes=1),
        "ingest_run_id": f"h-{ingest}",
    }


def make_hist(spark, rows):
    schema = _nullable(to_spark(get_schema("raw", "intraday_history")))
    return spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema)


def clean(spark, rows, hist=None):
    from stocklake.etl.raw_to_clean.intraday import clean_intraday

    stream_df = make(spark, rows) if rows is not None else None
    history_df = make_hist(spark, hist) if hist is not None else None
    return clean_intraday(stream_df, history_df)


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


def test_untagged_replay_dropped_and_counted(spark):
    from stocklake.etl.raw_to_clean.intraday import count_rejected, count_untagged_replay

    b1, b2, b3, b4 = (utc(2026, 1, 5, 3, m) for m in range(4))
    rows = [
        raw_row("FPT", b1, source="replay", run_id=None, c=100200.0),  # bản tin thử
        raw_row("FPT", b2, source="replay", run_id="  ", c=100200.0),  # run id toàn khoảng trắng
        raw_row("FPT", b3, source="replay", run_id="r-1", c=3.0),  # replay thật: giữ
        raw_row("FPT", b4, source="vci", run_id=None, c=4.0),  # nguồn thật không có run id: giữ
    ]
    raw = make(spark, rows)
    assert count_untagged_replay(raw) == 2
    assert count_rejected(raw) == 0
    assert sorted(r["close"] for r in clean(spark, rows).collect()) == [3.0, 4.0]


def test_untagged_replay_does_not_shadow_tagged_bar(spark):
    bar = utc(2026, 1, 5, 3, 5)
    rows = [
        raw_row(bar=bar, source="replay", run_id=None, c=100200.0, kafka=50),
        raw_row(bar=bar, source="replay", run_id="r-1", c=5.0, kafka=1),
    ]
    out = clean(spark, rows).collect()
    assert len(out) == 1 and out[0]["close"] == 5.0


def test_history_beats_stream_real_beats_tagged_replay(spark):
    bar = utc(2026, 1, 5, 3, 7)
    stream = [
        raw_row(bar=bar, source="replay", c=3.0, kafka=50),
        raw_row(bar=bar, source="kbs", c=2.0, kafka=40),
    ]
    out = clean(spark, stream, [hist_row(bar=bar, source="vci", c=1.0)]).collect()
    assert len(out) == 1 and out[0]["close"] == 1.0
    out = clean(spark, stream).collect()
    assert len(out) == 1 and out[0]["close"] == 2.0


def test_history_vci_beats_other_history(spark):
    bar = utc(2026, 1, 5, 3, 8)
    hist = [
        hist_row(bar=bar, source="vndirect", c=2.0, ingest=50),
        hist_row(bar=bar, source="vci", c=1.0, ingest=1),
    ]
    out = clean(spark, None, hist).collect()
    assert len(out) == 1 and out[0]["close"] == 1.0


def test_history_only_and_stream_only(spark):
    b1, b2 = utc(2026, 1, 5, 3, 0), utc(2026, 1, 5, 3, 1)
    only_hist = clean(spark, None, [hist_row(bar=b1), hist_row(bar=b2)])
    assert only_hist.count() == 2
    only_stream = clean(spark, [raw_row(bar=b1), raw_row(bar=b2)], None)
    assert only_stream.count() == 2
    mixed = clean(spark, [raw_row(bar=b1)], [hist_row(bar=b2)])
    assert mixed.count() == 2


def test_empty_stream_with_history(spark):
    out = clean(spark, [], [hist_row()])
    assert out.count() == 1


def test_history_duplicate_keeps_latest_ingest_ts(spark):
    bar = utc(2026, 1, 5, 3, 9)
    hist = [hist_row(bar=bar, c=1.0, ingest=1), hist_row(bar=bar, c=2.0, ingest=9)]
    out = clean(spark, None, hist).collect()
    assert len(out) == 1 and out[0]["close"] == 2.0


def test_history_null_field_rejected(spark):
    bad = hist_row("BAD")
    bad["close"] = None
    out = clean(spark, None, [bad, hist_row("OK")]).collect()
    assert [r["symbol"] for r in out] == ["OK"]


def test_untagged_replay_dropped_with_history_present(spark):
    bar = utc(2026, 1, 5, 3, 10)
    rows = [raw_row(bar=bar, source="replay", run_id=None, c=100200.0)]
    out = clean(spark, rows, [hist_row("VNM", bar=bar)]).collect()
    assert [r["symbol"] for r in out] == ["VNM"]


def test_output_matches_contract_with_history(spark):
    df = clean(spark, [raw_row()], [hist_row("VNM")])
    expected = to_spark(CLEAN)
    assert df.columns == [*CLEAN.column_names, *CLEAN.derived_partitions]
    for f in expected.fields:
        assert df.schema[f.name].dataType == f.dataType, f.name
