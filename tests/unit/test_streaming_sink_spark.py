"""Test Spark local cho sinks.kafka_to_raw (chạy trên PC: Python 3.10, pyspark 3.5.9)."""

import json
from datetime import date, datetime, timezone

import pytest

from stocklake.contracts.convert import to_spark
from stocklake.contracts.schemas import RAW_INTRADAY_PRICES

pytestmark = pytest.mark.spark

UTC = timezone.utc


def _ms(*args) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


BAR_0915 = _ms(2026, 11, 20, 2, 15)  # 09:15 VN ngày 20/11
BAR_2359 = _ms(2026, 11, 20, 16, 59)  # 23:59 VN ngày 20/11
BAR_0000 = _ms(2026, 11, 20, 17, 0)  # 00:00 VN ngày 21/11
BAR_0630 = _ms(2026, 11, 20, 23, 30)  # 06:30 VN ngày 21/11 (gần nửa đêm UTC)


def _msg(symbol, bar_ts, **over):
    m = {
        "symbol": symbol,
        "bar_ts": bar_ts,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1200,
        "source": "replay",
        "produced_ts": bar_ts + 5000,
        "available_at": bar_ts + 60000,
        "replay_run_id": "r1",
    }
    m.update(over)
    return m


def _enc(m) -> bytearray:
    return bytearray(json.dumps(m).encode("utf-8"))


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("test_streaming_sink")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture()
def kafka_df(spark):
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("key", T.BinaryType()),
            T.StructField("value", T.BinaryType()),
            T.StructField("topic", T.StringType()),
            T.StructField("partition", T.IntegerType()),
            T.StructField("offset", T.LongType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("timestampType", T.IntegerType()),
        ]
    )
    ts = datetime(2026, 11, 24, 3, 0, 0)
    no_close = _msg("FPT", BAR_0915)
    del no_close["close"]
    values = [
        _enc(_msg("FPT", BAR_0915)),  # 0 hợp lệ
        _enc(_msg("FPT", BAR_0915)),  # 1 trùng của 0 (producer gửi lại)
        _enc(_msg("VNM", BAR_2359)),  # 2 hợp lệ
        _enc(_msg("VNM", BAR_0000)),  # 3 hợp lệ
        _enc(_msg("HPG", BAR_0630, replay_run_id=None, source="vci")),  # 4 hợp lệ, live
        _enc(_msg("FPT", BAR_0915, replay_run_id="r2")),  # 5 lần phát lại khác: giữ
        bytearray(b"{not json"),  # 6 hỏng
        _enc(no_close),  # 7 thiếu trường bắt buộc
        _enc(_msg("FPT", BAR_0915, volume="abc")),  # 8 sai kiểu
        None,  # 9 value null
    ]
    rows = [(bytearray(b"K"), v, "intraday_prices", i % 6, i, ts, 0) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, schema)


def test_parse_columns_and_types(kafka_df):
    from stocklake.streaming.sinks.kafka_to_raw import parse_intraday

    out = parse_intraday(kafka_df)
    # Đúng thứ tự cột của contract raw.
    assert out.columns == list(RAW_INTRADAY_PRICES.column_names)
    want = {f.name: f.dataType for f in to_spark(RAW_INTRADAY_PRICES).fields}
    assert {f.name: f.dataType for f in out.schema.fields} == want
    assert out.count() == 6  # 0..5, chưa khử trùng


def test_parse_values_and_trade_date(spark, kafka_df):
    from pyspark.sql import functions as F

    from stocklake.streaming.sinks.kafka_to_raw import parse_intraday

    # trade_date không phụ thuộc session timezone.
    for tz in ("UTC", "America/Los_Angeles"):
        spark.conf.set("spark.sql.session.timeZone", tz)
        try:
            rows = (
                parse_intraday(kafka_df)
                .select(
                    "symbol",
                    "trade_date",
                    F.expr("unix_millis(bar_ts)").alias("bar_ms"),
                    F.expr("unix_millis(produced_ts)").alias("prod_ms"),
                    F.expr("unix_millis(available_at)").alias("avail_ms"),
                    "kafka_offset",
                    "kafka_partition",
                    "replay_run_id",
                    "source",
                    "spark_batch_ts",
                    "ingest_ts",
                )
                .collect()
            )
        finally:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
        by_off = {r["kafka_offset"]: r for r in rows}
        assert sorted(by_off) == [0, 1, 2, 3, 4, 5]
        assert by_off[0]["trade_date"] == date(2026, 11, 20)
        assert by_off[2]["trade_date"] == date(2026, 11, 20)
        assert by_off[3]["trade_date"] == date(2026, 11, 21)
        assert by_off[4]["trade_date"] == date(2026, 11, 21)
        assert by_off[0]["bar_ms"] == BAR_0915
        assert by_off[0]["prod_ms"] == BAR_0915 + 5000
        assert by_off[0]["avail_ms"] == BAR_0915 + 60000
        assert by_off[4]["replay_run_id"] is None and by_off[4]["source"] == "vci"
        assert by_off[3]["kafka_partition"] == 3
        assert all(r["spark_batch_ts"] is not None and r["ingest_ts"] is not None for r in rows)


def test_rejected_records_exposed(kafka_df):
    from stocklake.streaming.sinks.kafka_to_raw import rejected_intraday

    bad = rejected_intraday(kafka_df)
    assert bad.columns == ["value", "kafka_partition", "kafka_offset", "kafka_ts"]
    assert sorted(r["kafka_offset"] for r in bad.collect()) == [6, 7, 8, 9]


def test_dedup_keeps_other_replay_runs(kafka_df):
    from stocklake.streaming.sinks.kafka_to_raw import dedup_intraday, parse_intraday

    out = dedup_intraday(parse_intraday(kafka_df))
    keys = sorted(
        (r["symbol"], r["source"], r["replay_run_id"] or "")
        for r in out.select("symbol", "source", "replay_run_id").collect()
    )
    # Offset 1 (trùng của 0) bị bỏ; offset 5 (run r2) giữ lại.
    assert len(keys) == 5
    assert keys.count(("FPT", "replay", "r1")) == 1
    assert ("FPT", "replay", "r2") in keys


def test_observe_does_not_change_result(kafka_df):
    from stocklake.streaming.sinks.kafka_to_raw import parse_intraday

    assert parse_intraday(kafka_df, observe_name="intraday_parse_test").count() == 6


def test_count_replay_in_raw(spark, kafka_df, tmp_path):
    from stocklake.streaming.sinks.kafka_to_raw import (
        count_replay_in_raw,
        parse_intraday,
    )

    # URI file:// rõ ràng: đường dẫn không scheme sẽ theo fs.defaultFS của image (HDFS).
    out = (tmp_path / "raw_intraday").as_uri()
    parse_intraday(kafka_df).write.partitionBy("trade_date").parquet(out)
    c = count_replay_in_raw(spark, "r1", out)
    # r1: offset 0, 1 (trùng), 2, 3 -> 4 dòng, 3 phân biệt.
    assert c == {"run_id": "r1", "raw_total": 4, "raw_distinct": 3}
