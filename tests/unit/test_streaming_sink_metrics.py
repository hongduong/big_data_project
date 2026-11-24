"""Phần không cần Spark của sinks.kafka_to_raw: metrics theo batch, import nhẹ."""

import json
import subprocess
import sys

from stocklake.contracts.schemas import RAW_INTRADAY_PRICES
from stocklake.streaming.sinks.kafka_to_raw import (
    STREAM_CONF,
    STREAM_DEDUP_KEY,
    MetricsRecorder,
    progress_dict,
    progress_to_metrics,
)

PROGRESS = {
    "id": "q1",
    "runId": "run-1",
    "name": "intraday_to_raw",
    "timestamp": "2026-11-24T03:00:00.250Z",
    "batchId": 7,
    "numInputRows": 2000,
    "inputRowsPerSecond": 1990.5,
    "processedRowsPerSecond": 4000.0,
    "durationMs": {"triggerExecution": 500, "addBatch": 400},
    "stateOperators": [{"numRowsDroppedByWatermark": 3, "customMetrics": {}}],
    "observedMetrics": {"intraday_parse": {"rows": 2002, "rejected": 2}},
}


def test_progress_to_metrics():
    m = progress_to_metrics(PROGRESS)
    assert m["batch_id"] == 7 and m["num_input_rows"] == 2000
    assert m["trigger_start_ms"] == 1795489200250
    assert m["commit_ms"] == 1795489200250 + 500
    assert m["processed_rows_per_second"] == 4000.0
    assert (m["parsed_rows"], m["rejected_rows"], m["dropped_by_watermark"]) == (2002, 2, 3)


def test_progress_without_optional_parts():
    p = {"runId": "r", "batchId": 0, "timestamp": "2026-11-24T03:00:00Z", "numInputRows": 0}
    m = progress_to_metrics(p)
    assert m["trigger_execution_ms"] is None and m["commit_ms"] is None
    assert m["rejected_rows"] == 0 and m["dropped_by_watermark"] == 0


def test_progress_dict_accepts_object_with_json_property_or_method():
    class Prop:
        json = json.dumps(PROGRESS)

    class Meth:
        def json(self):
            return json.dumps(PROGRESS)

    assert progress_dict(Prop())["batchId"] == 7
    assert progress_dict(Meth())["batchId"] == 7
    assert progress_dict(PROGRESS)["batchId"] == 7


def test_recorder_appends_once_per_batch(tmp_path):
    path = tmp_path / "m" / "metrics.jsonl"
    rec = MetricsRecorder(str(path))
    assert rec.record(PROGRESS) is not None
    assert rec.record(PROGRESS) is None  # listener + recentProgress không ghi trùng
    rec.record({**PROGRESS, "batchId": 8})
    lines = [json.loads(x) for x in path.read_text().splitlines()]
    assert [x["batch_id"] for x in lines] == [7, 8]


def test_stream_conf_and_dedup_key():
    assert STREAM_CONF["spark.cores.max"] == "2"
    assert STREAM_DEDUP_KEY == ("symbol", "bar_ts", "source", "replay_run_id")
    assert set(STREAM_DEDUP_KEY) <= set(RAW_INTRADAY_PRICES.column_names)
    assert "trade_date" in RAW_INTRADAY_PRICES.partition_by


def test_streaming_modules_import_without_heavy_deps():
    code = (
        "import sys\n"
        "import stocklake.streaming.producers.replay, stocklake.streaming.producers.intraday\n"
        "import stocklake.streaming.sinks.kafka_to_raw\n"
        "bad = [m for m in ('pyspark', 'pyarrow', 'confluent_kafka') if m in sys.modules]\n"
        "assert not bad, bad\n"
    )
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


class _FakeQuery:
    """Query giả: lần chờ thứ `term_at` tự gửi SIGTERM cho tiến trình (như `kill -TERM`)."""

    def __init__(self, term_at=None, ends_at=None):
        self.term_at, self.ends_at = term_at, ends_at
        self.waits = 0
        self.stopped = False

    def awaitTermination(self, timeout=None):
        import os
        import signal

        if timeout is None:  # chờ hẳn: chỉ được gọi sau stop()
            assert self.stopped
            return None
        self.waits += 1
        if self.waits == self.term_at:
            os.kill(os.getpid(), signal.SIGTERM)
        return self.waits == self.ends_at

    def stop(self):
        self.stopped = True


def test_sigterm_stops_query_and_restores_handler():
    import signal

    from stocklake.streaming.sinks.kafka_to_raw import await_or_stop_on_sigterm

    before = signal.getsignal(signal.SIGTERM)
    q = _FakeQuery(term_at=2)
    await_or_stop_on_sigterm(q, poll_s=0.01)
    assert q.stopped and q.waits == 2
    assert signal.getsignal(signal.SIGTERM) is before


def test_query_ending_by_itself_is_not_stopped():
    from stocklake.streaming.sinks.kafka_to_raw import await_or_stop_on_sigterm

    q = _FakeQuery(ends_at=3)
    await_or_stop_on_sigterm(q, poll_s=0.01)
    assert not q.stopped and q.waits == 3
