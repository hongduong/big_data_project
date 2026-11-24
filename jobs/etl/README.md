# jobs/etl

Chủ: Hùng Dương.

Điểm vào mỏng cho job raw -> clean và compaction.

## intraday_raw_to_clean

- Mặc định tăng dần: chỉ dựng lại tháng (giờ VN) có đầu vào raw thay đổi. Trạng thái ở
  `/data/clean/intraday_1m/_build_state.json`; thiếu/hỏng/khác phiên bản thì tự chạy full.
- `--full`: dựng lại mọi tháng, viết lại trạng thái. `--dates D ...`: ép dựng lại các tháng chứa D.
- Tháng hết đầu vào chỉ báo trong `orphan_months`, không xoá.

## Khoá

Lệnh ghi clean giữ khoá ghi `clean.intraday_1m` / `clean.daily_ohlcv` (`stocklake.common.locks`,
TTL 1800 s, chờ tối đa 900 s; hết giờ thì job lỗi, chưa ghi gì). Lượt intraday không có gì đổi
không lấy khoá.
