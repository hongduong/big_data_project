"""Kafka -> raw bằng Spark Structured Streaming. pyspark chỉ import trong hàm.

Luồng intraday_prices:
  Kafka (key, value, partition, offset, timestamp)
  -> parse_intraday: from_json theo schema topic (contracts), bỏ bản tin hỏng ra khỏi
     luồng chính và đếm qua observe() -> listener ghi log/metrics mỗi batch
  -> dedup_intraday: watermark trên kafka_ts + dropDuplicatesWithinWatermark
  -> Parquet raw/intraday_prices, phân vùng trade_date, checkpoint trên HDFS.

Khử trùng ở đây chỉ bắt bản trùng đến gần nhau (producer gửi lại, trong 10 phút
theo kafka_ts). Bản trùng cách xa hơn do job batch raw -> clean khử theo khoá
(symbol, bar_ts, source). Không dùng watermark trên bar_ts vì nến phát lại có
thời gian quá khứ, sẽ bị coi là trễ và bỏ hết.

Exactly-once vào file sink dựa vào checkpoint + _spark_metadata; không gom file ở đây.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from stocklake.common.logging import get_logger
from stocklake.contracts.paths import checkpoint_path, table_path
from stocklake.contracts.schemas import RAW_INTRADAY_PRICES
from stocklake.contracts.topics import INTRADAY_PRICES

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

log = get_logger(__name__)

APP_NAME = "stocklake-intraday-to-raw"
QUERY_NAME = "intraday_to_raw"
CHECKPOINT_JOB = "intraday_to_raw"
OBSERVE_NAME = "intraday_parse"
DEDUP_WATERMARK = "10 minutes"
# Khoá khử trùng trong luồng: khoá contract + replay_run_id, để hai lần phát lại
# cùng dữ liệu (vd benchmark ở nhiều tốc độ) không nuốt nhau.
STREAM_DEDUP_KEY = (*INTRADAY_PRICES.dedup_key, "replay_run_id")
# Giờ VN cố định UTC+7 (không có giờ mùa hè) -> tính trade_date không phụ thuộc
# spark.sql.session.timeZone.
VN_OFFSET_MS = 7 * 3600 * 1000
DAY_MS = 86_400_000
STREAM_CONF = {
    "spark.cores.max": "2",  # Planning 2.2: chừa core cho job batch
    # Số partition state của dedup = số file mỗi batch; cố định trong checkpoint.
    "spark.sql.shuffle.partitions": "2",
}
_KAFKA_COLS = ("kafka_partition", "kafka_offset", "kafka_ts")


# ----------------------------------------------------------------- biến đổi thuần


def _required_topic_fields() -> list[str]:
    return [c.name for c in INTRADAY_PRICES.schema.columns if not c.nullable]


def decode_intraday(kafka_df: DataFrame) -> DataFrame:
    """Giải mã value JSON; giữ cột Kafka; thêm `_valid` (bool, không null) và `_value`."""
    from pyspark.sql import functions as F

    from stocklake.contracts.convert import to_spark

    topic_schema = to_spark(INTRADAY_PRICES.schema)
    value = F.col("value").cast("string")
    df = kafka_df.select(
        value.alias("_value"),
        F.from_json(value, topic_schema, {"mode": "PERMISSIVE"}).alias("_m"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )
    cond = F.col("_m").isNotNull()
    for name in _required_topic_fields():
        cond = cond & F.col(f"_m.{name}").isNotNull()
    return df.withColumn("_valid", F.coalesce(cond, F.lit(False)))


def _to_raw_columns(decoded: DataFrame) -> DataFrame:
    """Chọn đúng cột (thứ tự, kiểu) của raw.intraday_prices từ bản tin hợp lệ."""
    from pyspark.sql import functions as F

    from stocklake.contracts.convert import to_spark

    raw_struct = to_spark(RAW_INTRADAY_PRICES)
    topic_types = {c.name: c.type for c in INTRADAY_PRICES.schema.columns}
    exprs: dict[str, Any] = {}
    for f in raw_struct.fields:
        name = f.name
        if name in topic_types:
            col = F.col(f"_m.{name}")
            if topic_types[name] == "long" and f.dataType.typeName() == "timestamp":
                col = F.expr(f"timestamp_millis(_m.{name})")  # epoch ms -> timestamp
            exprs[name] = col
    for name in _KAFKA_COLS:
        exprs[name] = F.col(name)
    exprs["trade_date"] = F.expr(
        f"date_from_unix_date(CAST(FLOOR((_m.bar_ts + {VN_OFFSET_MS}) / {DAY_MS}) AS INT))"
    )
    exprs["ingest_ts"] = F.current_timestamp()
    exprs["spark_batch_ts"] = F.current_timestamp()
    missing = [f.name for f in raw_struct.fields if f.name not in exprs]
    if missing:
        raise KeyError(f"Không suy ra được cột raw: {missing}")
    return decoded.select(
        *[exprs[f.name].cast(f.dataType).alias(f.name) for f in raw_struct.fields]
    )


def parse_intraday(kafka_df: DataFrame, observe_name: str | None = None) -> DataFrame:
    """DataFrame dạng nguồn Kafka -> đúng cột raw.intraday_prices (chỉ bản tin hợp lệ).

    Bản tin hỏng (JSON lỗi, thiếu/null trường bắt buộc, sai kiểu) bị lọc ra; nếu có
    `observe_name` thì đếm `rows`/`rejected` mỗi batch qua observe() (listener ghi lại).
    Xem bản tin hỏng bằng `rejected_intraday`. Không khử trùng ở đây.
    """
    from pyspark.sql import functions as F

    decoded = decode_intraday(kafka_df)
    if observe_name:
        decoded = decoded.observe(
            observe_name,
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("_valid"), 0).otherwise(1)).alias("rejected"),
        )
    return _to_raw_columns(decoded.filter(F.col("_valid")))


def rejected_intraday(kafka_df: DataFrame) -> DataFrame:
    """Bản tin bị parse_intraday loại: value gốc + vị trí Kafka."""
    from pyspark.sql import functions as F

    decoded = decode_intraday(kafka_df)
    return decoded.filter(~F.col("_valid")).select(
        F.col("_value").alias("value"), *[F.col(c) for c in _KAFKA_COLS]
    )


def dedup_intraday(raw_df: DataFrame, watermark: str = DEDUP_WATERMARK) -> DataFrame:
    """Khử bản trùng đến gần nhau theo STREAM_DEDUP_KEY, event-time = kafka_ts.

    Với DataFrame tĩnh (Spark không cho dropDuplicatesWithinWatermark), dùng dropDuplicates
    theo khoá.
    """
    if not raw_df.isStreaming:
        return raw_df.dropDuplicates(list(STREAM_DEDUP_KEY))
    return raw_df.withWatermark("kafka_ts", watermark).dropDuplicatesWithinWatermark(
        list(STREAM_DEDUP_KEY)
    )


# ----------------------------------------------------------------- metrics theo batch


def _ts_ms(iso: str | None) -> int | None:
    """'2026-11-24T03:00:00.123Z' -> epoch ms."""
    if not iso:
        return None
    s = iso.rstrip("Z")
    fmt = "%Y-%m-%dT%H:%M:%S.%f" if "." in s else "%Y-%m-%dT%H:%M:%S"
    dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
    return round(dt.timestamp() * 1000)


def progress_dict(progress: Any) -> dict[str, Any]:
    """StreamingQueryProgress (đối tượng hoặc dict, tuỳ bản pyspark) -> dict."""
    if isinstance(progress, Mapping):
        return dict(progress)
    j = getattr(progress, "json", None)
    if callable(j):
        j = j()
    if isinstance(j, str):
        return json.loads(j)
    raise TypeError(f"Không đọc được progress kiểu {type(progress).__name__}")


def progress_to_metrics(p: Mapping[str, Any], observe_name: str = OBSERVE_NAME) -> dict[str, Any]:
    """Một dòng metrics gọn từ progress JSON của Spark.

    commit_ms = trigger_start_ms + trigger_execution_ms (Planning 5b).
    """
    duration = p.get("durationMs") or {}
    start_ms = _ts_ms(p.get("timestamp"))
    trig = duration.get("triggerExecution")
    observed = (p.get("observedMetrics") or {}).get(observe_name) or {}
    dropped = sum(
        int(op.get("numRowsDroppedByWatermark") or 0) for op in p.get("stateOperators") or []
    )
    return {
        "query": p.get("name"),
        "run_id": p.get("runId"),
        "batch_id": p.get("batchId"),
        "num_input_rows": p.get("numInputRows"),
        "input_rows_per_second": p.get("inputRowsPerSecond"),
        "processed_rows_per_second": p.get("processedRowsPerSecond"),
        "trigger_start": p.get("timestamp"),
        "trigger_start_ms": start_ms,
        "trigger_execution_ms": trig,
        "commit_ms": start_ms + trig if start_ms is not None and trig is not None else None,
        "parsed_rows": observed.get("rows"),
        "rejected_rows": observed.get("rejected") or 0,
        "dropped_by_watermark": dropped,
    }


class MetricsRecorder:
    """Ghi log + một dòng JSON mỗi batch (không ghi trùng batch_id của cùng run)."""

    def __init__(self, metrics_file: str | None = None, observe_name: str = OBSERVE_NAME):
        self.path = Path(metrics_file) if metrics_file else None
        self.observe_name = observe_name
        self._seen: set[tuple[Any, Any]] = set()
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, progress: Any) -> dict[str, Any] | None:
        m = progress_to_metrics(progress_dict(progress), self.observe_name)
        key = (m["run_id"], m["batch_id"])
        if key in self._seen:
            return None
        self._seen.add(key)
        log.info(
            "batch %s: %s dòng, %.1f dòng/s, trigger %s ms, hỏng %s, trễ bị bỏ %s",
            m["batch_id"],
            m["num_input_rows"],
            m["processed_rows_per_second"] or 0.0,
            m["trigger_execution_ms"],
            m["rejected_rows"],
            m["dropped_by_watermark"],
        )
        if m["rejected_rows"]:
            log.warning("batch %s: loại %s bản tin hỏng", m["batch_id"], m["rejected_rows"])
        if self.path:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
        return m


def make_listener(recorder: MetricsRecorder):
    """StreamingQueryListener (Planning 5b) chuyển progress cho recorder."""
    from pyspark.sql.streaming import StreamingQueryListener

    class _Listener(StreamingQueryListener):
        def onQueryStarted(self, event):
            log.info("query bắt đầu: id=%s runId=%s", event.id, event.runId)

        def onQueryProgress(self, event):
            try:
                recorder.record(event.progress)
            except Exception:  # listener không được làm hỏng query
                log.exception("listener: không ghi được metrics")

        def onQueryIdle(self, event):
            pass

        def onQueryTerminated(self, event):
            log.info("query kết thúc: runId=%s lỗi=%s", event.runId, event.exception)

    return _Listener()


# ----------------------------------------------------------------- query


def start_intraday_stream(
    spark: SparkSession,
    *,
    starting_offsets: str = "earliest",
    trigger: str = "10 seconds",
    available_now: bool = False,
    checkpoint: str | None = None,
    output: str | None = None,
    max_offsets_per_trigger: int | None = None,
    fail_on_data_loss: bool = True,
    bootstrap: str | None = None,
    dedup_watermark: str = DEDUP_WATERMARK,
) -> StreamingQuery:
    """Khởi động query Kafka intraday_prices -> raw Parquet (phân vùng trade_date).

    failOnDataLoss mặc định true: offset đã mất (hết retention, topic bị tạo lại khi
    checkpoint còn) thì query dừng có lỗi thay vì bỏ qua âm thầm -> số đếm raw không lệch.
    `starting_offsets` chỉ dùng khi checkpoint mới; có checkpoint thì chạy tiếp từ đó.
    """
    from stocklake.common.config import get_settings

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap or get_settings().kafka_bootstrap)
        .option("subscribe", INTRADAY_PRICES.name)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "true" if fail_on_data_loss else "false")
    )
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", int(max_offsets_per_trigger))
    raw = parse_intraday(reader.load(), observe_name=OBSERVE_NAME)
    out = dedup_intraday(raw, dedup_watermark)
    writer = (
        out.writeStream.format("parquet")
        .queryName(QUERY_NAME)
        .outputMode("append")
        .option("path", output or table_path("raw", RAW_INTRADAY_PRICES.name))
        .option("checkpointLocation", checkpoint or checkpoint_path(CHECKPOINT_JOB))
        .partitionBy(*RAW_INTRADAY_PRICES.partition_by)
    )
    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime=trigger)
    )
    return writer.start()


def await_or_stop_on_sigterm(query: Any, poll_s: float = 5.0) -> None:
    """Chờ query kết thúc; nhận SIGTERM thì gọi query.stop() rồi trả về (dừng sạch).

    Handler chỉ bật cờ: không gọi py4j trong handler vì luồng chính có thể đang nằm trong
    một lệnh py4j (SIGINT mặc định của PySpark làm vậy và gây lỗi "reentrant call").
    Checkpoint giữ nguyên; chạy lại cùng checkpoint thì tiếp tục từ offset đã commit.
    Dừng job: `kill -TERM <pid python>` (không phải JVM spark-submit).
    """
    import signal
    import threading

    stop = threading.Event()

    def _on_term(signum: int, _frame: Any) -> None:
        stop.set()

    try:
        old = signal.signal(signal.SIGTERM, _on_term)
    except ValueError:  # không ở luồng chính: chờ như cũ
        query.awaitTermination()
        return
    try:
        while not stop.is_set():
            if query.awaitTermination(poll_s):
                return
        log.info("nhận SIGTERM: dừng query %s", QUERY_NAME)
        query.stop()
        query.awaitTermination()  # ném lỗi nếu query đã hỏng
    finally:
        signal.signal(signal.SIGTERM, old)


def run_intraday_to_raw(
    *,
    available_now: bool = False,
    trigger: str = "10 seconds",
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int | None = None,
    metrics_file: str | None = None,
    checkpoint: str | None = None,
    output: str | None = None,
) -> dict[str, Any]:
    """Điểm vào cho jobs/streaming/intraday_to_raw.py: tạo session, chạy tới khi dừng."""
    from stocklake.common.spark import get_spark

    spark = get_spark(APP_NAME, extra_conf=STREAM_CONF)
    recorder = MetricsRecorder(metrics_file)
    spark.streams.addListener(make_listener(recorder))
    query = start_intraday_stream(
        spark,
        starting_offsets=starting_offsets,
        trigger=trigger,
        available_now=available_now,
        checkpoint=checkpoint,
        output=output,
        max_offsets_per_trigger=max_offsets_per_trigger,
    )
    try:
        await_or_stop_on_sigterm(query)
    finally:
        # Listener chạy bất đồng bộ: bù các batch nó chưa kịp ghi trước khi thoát.
        for p in query.recentProgress or []:
            try:
                recorder.record(p)
            except Exception:
                log.exception("không ghi được progress còn lại")
    batches = [progress_dict(p) for p in query.recentProgress or []]
    return {
        "query": QUERY_NAME,
        "batches": len(batches),
        "input_rows": sum(int(b.get("numInputRows") or 0) for b in batches),
    }


def count_replay_in_raw(spark: SparkSession, run_id: str, output: str | None = None) -> dict:
    """Đếm bản ghi raw của một lần phát lại: tổng và phân biệt theo (symbol, bar_ts, source)."""
    from pyspark.sql import functions as F

    df = spark.read.parquet(output or table_path("raw", RAW_INTRADAY_PRICES.name))
    row = (
        df.filter(F.col("replay_run_id") == run_id)
        .agg(
            F.count(F.lit(1)).alias("total"),
            F.countDistinct(*INTRADAY_PRICES.dedup_key).alias("distinct"),
        )
        .collect()[0]
    )
    return {"run_id": run_id, "raw_total": int(row["total"]), "raw_distinct": int(row["distinct"])}


def verify_replay(run_id: str, expected: int, output: str | None = None) -> dict:
    """Điểm vào cho jobs/streaming/verify_replay.py."""
    from stocklake.common.spark import get_spark

    spark = get_spark("stocklake-verify-replay")
    try:
        c = count_replay_in_raw(spark, run_id, output)
    finally:
        spark.stop()
    return {
        "run_id": run_id,
        "sent_expected": int(expected),
        "raw_total": c["raw_total"],
        "raw_distinct": c["raw_distinct"],
        "ok": c["raw_distinct"] == int(expected),
    }


def build_news_stream(
    spark: SparkSession,
    bootstrap: str | None = None,
    checkpoint: str | None = None,
    trigger_seconds: int = 30,
) -> StreamingQuery:
    """Đọc topic market_news, ghi Parquet vào raw/market_news (phân vùng ingest_date)."""
    raise NotImplementedError("Lát 2")
