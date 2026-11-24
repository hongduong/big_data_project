# jobs/streaming

Chủ: Tuyền.

Điểm vào mỏng cho producer, replay và job Kafka -> raw.

| File | Chạy ở | Việc |
|---|---|---|
| `send_sample.py` | collector | Gửi vài bản tin mẫu |
| `replay.py` | collector | Phát lại nến phút từ HDFS `raw/intraday_history` (đọc theo chunk tháng; `--days 5` hoặc `--start/--end`, `--group sample\|vn30`, `--symbols`, `--rate`, `--run-id`); in ReplayResult JSON |
| `intraday_producer.py` | collector | Producer nến phút realtime VCI (`--once` hoặc vòng lặp trong giờ giao dịch) |
| `intraday_to_raw.py` | spark-driver (spark-submit) | Kafka `intraday_prices` -> `/data/raw/intraday_prices` (`--available-now`, `--metrics-file`) |
| `verify_replay.py` | spark-driver (spark-submit) | So số bản ghi phân biệt ở raw với số đã gửi của một `replay_run_id` |

## Replay

Replay không gọi nguồn dữ liệu: đọc nến từ HDFS `/data/raw/intraday_history`, nên cần chạy
`jobs/ingest/intraday_history.py` trước. Thiếu bảng hoặc không có nến cho lựa chọn thì job in lỗi
ra stderr và thoát mã 1. `--days N` = N ngày giao dịch gần nhất có trong lịch sử của các mã đã
chọn; `--start/--end` là ngày giờ VN, gồm cả hai đầu. Một lần chạy dùng một `replay_run_id` và một
tốc độ `--rate` cố định (liên tục qua các ngày và tháng).

Cách đọc (để đạt tốc độ cao với container collector 0,5 CPU / 1 GB):

- Đọc theo **chunk tháng giờ VN**: mỗi tháng một lần đọc các file tường minh
  `source=<src>/year=<Y>/<MÃ>.parquet` của các mã đã chọn (không liệt kê thư mục; danh sách nguồn
  lấy một lần bằng một `ls`), lọc `bar_ts` theo khoảng tháng nên row group theo tháng được chọn
  bằng statistics. Chunk giữ dạng cột pyarrow, chỉ đổi sang đối tượng Python từng ngày; cùng
  `(mã, bar_ts)` ở nhiều nguồn thì ưu tiên `vci`.
- Một **luồng đọc nền** nạp sẵn chunk tháng kế tiếp (hàng đợi cỡ 1) trong lúc gửi tháng hiện
  tại; tháng đầu đọc xong trước khi nhịp gửi bắt đầu nên thời gian khởi động không tính vào tốc độ.
  Lỗi đọc được ném lại ở luồng chính, luồng đọc luôn được dừng khi kết thúc hoặc lỗi.
- `--days N` dò lùi từng tháng (chỉ đọc cột `bar_ts`) và dừng ngay khi đủ N ngày.
- Bộ nhớ: tối đa 3 chunk cùng sống; VN30 khoảng 40-60 MB, 412 mã HOSE khoảng 300-450 MB.

```
python jobs/streaming/replay.py --days 5 --rate 2000
python jobs/streaming/replay.py --group vn30 --start 2026-11-16 --end 2026-11-20 --rate 5000
```

Dòng JSON kết quả: `run_id, sent, delivered, failed, seconds, first_bar_ts, last_bar_ts, days,
symbols, target_rate, achieved_rate, read_seconds, months`. `achieved_rate` = `sent` / thời gian
giữa lần gửi đầu và cuối (msg/giây thực tế); `read_seconds` = tổng thời gian đọc + giải mã lịch
sử (nằm trong luồng nền, không cộng vào `seconds`); `months` = số chunk tháng đã đọc. Nếu
`achieved_rate` < 95% `target_rate` (và chạy từ 1 giây trở lên) job ghi cảnh báo WARNING.

Dừng sạch job `intraday_to_raw.py` chạy liên tục: gửi SIGTERM cho tiến trình Python (không phải
JVM spark-submit), job gọi `query.stop()` rồi thoát mã 0; checkpoint giữ nguyên.

```
docker exec spark-driver pkill -TERM -f '^/usr/bin/python3 /opt/stocklake/jobs/streaming/intraday_to_raw.py'
```

Không dùng Ctrl-C/SIGINT: handler SIGINT của PySpark gọi py4j trong lúc luồng chính đang chờ
`awaitTermination` và thoát với lỗi `reentrant call` (dữ liệu vẫn đúng nhờ checkpoint).
