"""Điểm vào mỏng: Structured Streaming Kafka intraday_prices -> HDFS raw (spark-submit)."""

import argparse
import json

from stocklake.streaming.sinks.kafka_to_raw import run_intraday_to_raw


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--available-now", action="store_true", help="Xử lý hết dữ liệu có sẵn rồi dừng")
    p.add_argument("--trigger", default="10 seconds", help="processingTime trigger")
    p.add_argument("--starting-offsets", default="earliest", help="Chỉ dùng khi checkpoint mới")
    p.add_argument("--max-offsets-per-trigger", type=int)
    p.add_argument("--metrics-file", help="File JSON Lines metrics mỗi batch (trên driver)")
    p.add_argument("--checkpoint", help="Ghi đè checkpoint (mặc định /checkpoints/intraday_to_raw)")
    p.add_argument("--output", help="Ghi đè thư mục ra (mặc định /data/raw/intraday_prices)")
    a = p.parse_args()
    summary = run_intraday_to_raw(
        available_now=a.available_now,
        trigger=a.trigger,
        starting_offsets=a.starting_offsets,
        max_offsets_per_trigger=a.max_offsets_per_trigger,
        metrics_file=a.metrics_file,
        checkpoint=a.checkpoint,
        output=a.output,
    )
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
