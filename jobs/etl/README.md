# jobs/etl

Chủ: Hùng Dương.

Điểm vào mỏng cho job raw -> clean và compaction.

## intraday_raw_to_clean

- Mặc định tăng dần: chỉ dựng lại tháng (giờ VN) có đầu vào raw thay đổi. Trạng thái ở
  `/data/clean/intraday_1m/_build_state.json`; thiếu/hỏng/khác phiên bản thì tự chạy full.
- `--full`: dựng lại mọi tháng, viết lại trạng thái. `--dates D ...`: ép dựng lại các tháng chứa D.
- Tháng hết đầu vào chỉ báo trong `orphan_months`, không xoá.
- `--full`/`--dates` (và lần đầu chưa có trạng thái) đọc raw MỘT lần: hai bảng raw được chiếu,
  union, chia theo (symbol, bar_ts) và persist (MEMORY_AND_DISK, thiếu RAM thì tràn ra đĩa
  executor); fingerprint ghi vào trạng thái và clean cùng lấy từ lần quét đó. Chạy tăng dần vẫn
  quét tháng ứng viên để so fingerprint, rồi chỉ đọc lại tháng đổi để dựng.
- Thời gian trong JSON kết quả: `detect_seconds` tới khi có kế hoạch (full/dates: gồm cả lần
  quét chung), `fingerprint_seconds` riêng bước fingerprint, `build_seconds` khử trùng + đếm
  (tăng dần: gồm đọc tháng đổi), `write_seconds` từ sau kế hoạch tới khi ghi xong trạng thái.

## Khoá

Lệnh ghi clean giữ khoá ghi `clean.intraday_1m` / `clean.daily_ohlcv` (`stocklake.common.locks`,
mặc định của module: TTL 5 phút, chờ tối đa 900 s). Trong lúc giữ, luồng heartbeat gia hạn khoá
mỗi TTL/3 nên lệnh ghi dài vẫn giữ khoá; job chết thì khoá hết hạn sau tối đa 5 phút. Hết giờ chờ,
hoặc khoá bị chiếm khi đang chờ reader: job lỗi, chưa ghi gì. Lượt intraday không có gì đổi không
lấy khoá.

`"lock_lost": true` trong JSON kết quả (kèm log ERROR): heartbeat không gia hạn được và khoá đã
mất/bị chiếm trong lúc ghi. Dữ liệu và trạng thái đã ghi xong, job không lỗi, nhưng một job đọc
(DQ) hoặc ghi khác có thể đã chạy chồng: kiểm tra lại kết quả của job đó.
