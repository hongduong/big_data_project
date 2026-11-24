import datetime as dt
import json

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
    assert "source" in df.columns
    for f in expected.fields:
        assert df.schema[f.name].dataType == f.dataType, f.name
    row = df.collect()[0]
    assert (row["year"], row["month"]) == (2026, 1)
    assert row["source"] == "vci"


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
    assert len(out) == 1 and (out[0]["close"], out[0]["source"]) == (1.0, "vci")
    out = clean(spark, stream).collect()
    assert len(out) == 1 and (out[0]["close"], out[0]["source"]) == (2.0, "kbs")
    out = clean(spark, stream[:1]).collect()
    assert len(out) == 1 and (out[0]["close"], out[0]["source"]) == (3.0, "replay")


def test_history_vci_beats_other_history(spark):
    bar = utc(2026, 1, 5, 3, 8)
    hist = [
        hist_row(bar=bar, source="vndirect", c=2.0, ingest=50),
        hist_row(bar=bar, source="vci", c=1.0, ingest=1),
    ]
    out = clean(spark, None, hist).collect()
    assert len(out) == 1 and (out[0]["close"], out[0]["source"]) == (1.0, "vci")
    # Chỉ có history khác VCI: source là nguồn của file đó.
    out = clean(spark, None, hist[:1]).collect()
    assert (out[0]["close"], out[0]["source"]) == (2.0, "vndirect")


def test_source_is_chosen_row_source_normalised(spark):
    b1, b2, b3 = (utc(2026, 1, 5, 3, m) for m in (20, 21, 22))
    stream = [
        raw_row("FPT", b1, source="KBS", c=1.0),  # nguồn thật viết hoa
        raw_row("FPT", b2, source="replay", run_id="r-9", c=2.0),
        raw_row("FPT", b3, source="replay", run_id="r-9", c=3.0),
    ]
    hist = [hist_row("FPT", bar=b3, source="vci", c=4.0)]
    out = {r["close"]: r["source"] for r in clean(spark, stream, hist).collect()}
    # b3: history vci thắng replay; b1: nguồn thật ghi chữ thường; b2: replay có run id.
    assert out == {1.0: "kbs", 2.0: "replay", 4.0: "vci"}


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
    assert {r["symbol"]: r["source"] for r in df.collect()} == {"FPT": "vci", "VNM": "vci"}


# ---------------------------------------------------------------- fingerprint


def _fps(spark, stream=None, hist=None):
    from stocklake.etl.raw_to_clean.intraday import month_fingerprints

    return month_fingerprints(
        make(spark, stream) if stream is not None else None,
        make_hist(spark, hist) if hist is not None else None,
    )


def test_fingerprint_stable_under_reingest(spark):
    b1, b2 = utc(2026, 1, 5, 3, 0), utc(2026, 2, 5, 3, 0)
    stream = [raw_row("FPT", b1, kafka=1, ingest=1, offset=1), raw_row("VNM", b2)]
    hist = [hist_row("FPT", b1, ingest=1)]
    a = _fps(spark, stream, hist)
    # Nạp lại y hệt các nến: khác ingest_ts / kafka_ts / offset / ingest_run_id, khác thứ tự.
    stream2 = [raw_row("VNM", b2, kafka=9, ingest=9, offset=7), raw_row("FPT", b1, kafka=5)]
    hist2 = [hist_row("FPT", b1, ingest=30)]
    assert _fps(spark, stream2, hist2) == a
    assert set(a) == {"2026-01", "2026-02"}
    assert set(a["2026-01"]) == {"history", "stream"} and set(a["2026-02"]) == {"stream"}
    assert a["2026-01"]["stream"]["rows"] == 1
    # Kafka giao lại cùng nến (bản sao chỉ khác cột Kafka/ingest) cũng không phải thay đổi.
    assert _fps(spark, [*stream, raw_row("FPT", b1, kafka=40, ingest=40, offset=9)], hist) == a
    # Dòng bị transform loại (replay không run id) không làm đổi fingerprint.
    junk = raw_row("FPT", b1, source="replay", run_id=None, c=1.0)
    assert _fps(spark, [*stream, junk], hist) == a


def test_fingerprint_sensitive_to_any_value_change(spark):
    b1 = utc(2026, 1, 5, 3, 0)
    base = _fps(spark, [raw_row("FPT", b1, c=105.0)], [hist_row("VNM", b1)])
    changed = _fps(spark, [raw_row("FPT", b1, c=105.5)], [hist_row("VNM", b1)])
    assert changed["2026-01"]["stream"] != base["2026-01"]["stream"]
    assert changed["2026-01"]["history"] == base["2026-01"]["history"]
    h = hist_row("VNM", b1)
    h["volume"] = 11
    assert (
        _fps(spark, [raw_row("FPT", b1, c=105.0)], [h])["2026-01"]["history"]
        != (base["2026-01"]["history"])
    )


# ---------------------------------------------------------------- chạy tăng dần end-to-end


class CountingLockFs:
    """Bọc MemoryLockFs, đếm số lần giành khoá ghi."""

    def __init__(self):
        from stocklake.common.locks import MemoryLockFs

        self.inner = MemoryLockFs()
        self.writer_acquired = 0

    def create_exclusive(self, path, data):
        ok = self.inner.create_exclusive(path, data)
        if ok and path.endswith("writer.lock"):
            self.writer_acquired += 1
        return ok

    def __getattr__(self, name):
        return getattr(self.inner, name)


def _write_hist(spark, base_dir, rows, year=2026, source="vci"):
    df = make_hist(spark, rows).drop("source")
    path = f"file://{base_dir}/raw/intraday_history/source={source}/year={year}"
    df.coalesce(1).write.mode("append").parquet(path)


def _write_stream(spark, base_dir, rows, day):
    path = f"file://{base_dir}/raw/intraday_prices/trade_date={day}"
    make(spark, rows).drop("trade_date").coalesce(1).write.mode("append").parquet(path)


def _clean_files(base_dir):
    root = base_dir / "clean" / "intraday_1m"
    return {
        str(p.relative_to(root)): p.stat().st_mtime_ns
        for p in sorted(root.glob("year=*/month=*/*"))
    }


def _state(base_dir):
    return json.loads((base_dir / "clean" / "intraday_1m" / "_build_state.json").read_text())


def _run(spark, base_dir, lock_fs, **kw):
    from stocklake.etl.raw_to_clean.intraday import run_intraday_raw_to_clean

    kw.setdefault("safety_lag_s", 0.0)
    return run_intraday_raw_to_clean(spark, lock_fs=lock_fs, base=f"file://{base_dir}", **kw)


def test_incremental_end_to_end(spark, tmp_path):
    pytest.importorskip("stocklake.common.locks")
    import shutil
    import time

    jan, feb = utc(2026, 1, 5, 3, 0), utc(2026, 2, 5, 3, 0)
    _write_hist(spark, tmp_path, [hist_row("FPT", jan), hist_row("FPT", feb)])
    _write_stream(spark, tmp_path, [raw_row("VNM", jan)], "2026-01-05")
    locks = CountingLockFs()

    # 1. Chưa có trạng thái -> full.
    r = _run(spark, tmp_path, locks)
    assert r["mode"] == "full" and r["months_rebuilt"] == ["2026-01", "2026-02"]
    assert r["clean_rows"] == 3 and r["clean_rows_total"] == 3 and r["state_written"]
    assert locks.writer_acquired == 1 and r["lock_waited_s"] >= 0
    st = _state(tmp_path)
    assert st["version"] == 1 and set(st["months"]) == {"2026-01", "2026-02"}
    assert st["months"]["2026-01"]["clean_rows"] == 2
    before = _clean_files(tmp_path)

    # 2. Không có gì đổi: không ghi, không khoá, không đụng file.
    time.sleep(1.1)
    r = _run(spark, tmp_path, locks)
    assert r["mode"] == "incremental" and r["months_rebuilt"] == []
    assert r["months_candidates"] == 0 and r["months_skipped"] == 2
    assert not r["state_written"] and locks.writer_acquired == 1
    assert r["clean_rows"] == 0 and r["clean_rows_total"] == 3
    assert _clean_files(tmp_path) == before

    # 3. Thêm dòng cho tháng 2 -> chỉ tháng 2 dựng lại; file tháng 1 giữ nguyên tên + mtime.
    time.sleep(1.1)
    _write_stream(spark, tmp_path, [raw_row("VNM", utc(2026, 2, 10, 3, 0))], "2026-02-10")
    r = _run(spark, tmp_path, locks)
    assert r["months_rebuilt"] == ["2026-02"] and r["months_candidates"] == 1
    assert r["clean_rows"] == 2 and r["clean_rows_total"] == 4 and locks.writer_acquired == 2
    after = _clean_files(tmp_path)
    jan_files = {k: v for k, v in before.items() if k.startswith("year=2026/month=1/")}
    assert jan_files and all(after.get(k) == v for k, v in jan_files.items())
    assert {k for k in after if "month=2/" in k} != {k for k in before if "month=2/" in k}

    # 4. Nạp lại y hệt nến tháng 1 (khác ingest/kafka) -> ứng viên nhưng không dựng lại.
    time.sleep(1.1)
    _write_stream(spark, tmp_path, [raw_row("VNM", jan, kafka=30, ingest=30)], "2026-01-05")
    r = _run(spark, tmp_path, locks)
    assert r["months_candidates"] == 1 and r["months_rebuilt"] == []
    assert r["state_written"] and locks.writer_acquired == 2
    # ...và lượt sau đó không còn ứng viên (horizon đã dời).
    r = _run(spark, tmp_path, locks)
    assert r["months_candidates"] == 0 and not r["state_written"]

    # 5. Mất phân vùng clean -> dựng lại dù raw không đổi.
    shutil.rmtree(tmp_path / "clean" / "intraday_1m" / "year=2026" / "month=2")
    r = _run(spark, tmp_path, locks)
    assert r["months_rebuilt"] == ["2026-02"]

    # 6. --dates ép dựng lại; --full dựng lại tất cả.
    r = _run(spark, tmp_path, locks, dates=["2026-01-20"])
    assert r["mode"] == "dates" and r["months_rebuilt"] == ["2026-01"] and r["clean_rows"] == 2
    r = _run(spark, tmp_path, locks, full=True)
    assert r["mode"] == "full" and r["months_rebuilt"] == ["2026-01", "2026-02"]
    assert r["clean_rows_total"] == 4

    out = spark.read.parquet(f"file://{tmp_path}/clean/intraday_1m")
    assert out.count() == 4

    # 7. Trạng thái hỏng -> tự chạy full. Xoá file .crc của LocalFileSystem (HDFS không có) để
    # nội dung hỏng được đọc ra như trên HDFS, thay vì ChecksumException của file:// .
    state_dir = tmp_path / "clean" / "intraday_1m"
    (state_dir / "._build_state.json.crc").unlink(missing_ok=True)
    (state_dir / "_build_state.json").write_text("{hỏng")
    r = _run(spark, tmp_path, locks)
    assert r["mode"] == "full" and _state(tmp_path)["version"] == 1


def test_lock_timeout_writes_nothing(spark, tmp_path):
    from unittest import mock

    locks = pytest.importorskip("stocklake.common.locks")
    from stocklake.etl.raw_to_clean import _common, intraday

    real = _common.clean_write_lock

    def short_wait(spark_, table, lock_fs=None, **kw):
        return real(spark_, table, lock_fs, wait_timeout_s=0.05, poll_s=0.01)

    _write_hist(spark, tmp_path, [hist_row("FPT")])
    fs = locks.MemoryLockFs()
    with locks.read_lock(fs, "clean.intraday_1m", owner="dq"):
        with mock.patch.object(intraday, "clean_write_lock", short_wait):
            with pytest.raises(TimeoutError, match="clean.intraday_1m"):
                _run(spark, tmp_path, fs)
    root = tmp_path / "clean" / "intraday_1m"
    assert not list(root.glob("year=*")) and not (root / "_build_state.json").exists()


def test_daily_write_takes_lock(spark, tmp_path):
    locks = pytest.importorskip("stocklake.common.locks")
    from stocklake.etl.raw_to_clean.daily import RAW as DAILY_RAW
    from stocklake.etl.raw_to_clean.daily import run_daily_raw_to_clean

    ts = utc(2026, 1, 10)
    row = ("FPT", dt.date(2026, 1, 5), 1.0, 2.0, 0.5, 1.5, 10, None, "vnd", "vci", ts, ts)
    from pyspark.sql import functions as F

    df = spark.createDataFrame([row], _nullable(to_spark(DAILY_RAW)))
    df.withColumn("year", F.year("trade_date")).write.partitionBy("source", "year").parquet(
        f"file://{tmp_path}/raw/daily_ohlcv"
    )
    fs = CountingLockFs()
    r = run_daily_raw_to_clean(spark, None, fs, base=f"file://{tmp_path}")
    assert r["clean_rows"] == 1 and r["lock_waited_s"] >= 0 and fs.writer_acquired == 1
    assert fs.inner.files == {}
    with locks.read_lock(fs.inner, "clean.daily_ohlcv", owner="dashboard"):
        with pytest.raises(TimeoutError):
            from unittest import mock

            from stocklake.etl.raw_to_clean import _common, daily

            real = _common.clean_write_lock
            with mock.patch.object(
                daily,
                "clean_write_lock",
                lambda s, t, f=None, **k: real(s, t, f, wait_timeout_s=0.05, poll_s=0.01),
            ):
                run_daily_raw_to_clean(spark, None, fs.inner, base=f"file://{tmp_path}")
