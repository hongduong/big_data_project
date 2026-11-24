# jobs/streaming

Chủ: Tuyền.

Điểm vào mỏng cho producer, replay và job Kafka -> raw.

| File | Chạy ở | Việc |
|---|---|---|
| `send_sample.py` | collector | Gửi vài bản tin mẫu |
| `replay.py` | collector | Phát lại nến phút lịch sử (`--days 5` hoặc `--start/--end`, `--rate`, `--run-id`); in ReplayResult JSON |
| `intraday_producer.py` | collector | Producer nến phút realtime VCI (`--once` hoặc vòng lặp trong giờ giao dịch) |
| `intraday_to_raw.py` | spark-driver (spark-submit) | Kafka `intraday_prices` -> `/data/raw/intraday_prices` (`--available-now`, `--metrics-file`) |
| `verify_replay.py` | spark-driver (spark-submit) | So số bản ghi phân biệt ở raw với số đã gửi của một `replay_run_id` |

Dừng sạch job `intraday_to_raw.py` chạy liên tục: gửi SIGTERM cho tiến trình Python (không phải
JVM spark-submit), job gọi `query.stop()` rồi thoát mã 0; checkpoint giữ nguyên.

```
docker exec spark-driver pkill -TERM -f '^/usr/bin/python3 /opt/stocklake/jobs/streaming/intraday_to_raw.py'
```

Không dùng Ctrl-C/SIGINT: handler SIGINT của PySpark gọi py4j trong lúc luồng chính đang chờ
`awaitTermination` và thoát với lỗi `reentrant call` (dữ liệu vẫn đúng nhờ checkpoint).
