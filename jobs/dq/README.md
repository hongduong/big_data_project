# jobs/dq

Chủ: Hồng Dương.

Điểm vào mỏng cho job kiểm tra chất lượng dữ liệu.

## Job hiện có

- `intraday_dq.py`: kiểm tra nến phút trên bảng clean — đầy đủ theo ngày giao dịch
  (lịch = VNINDEX trong clean daily) và đối chiếu tổng volume phút với volume ngày.
  Ghi `curated/dq_report` (trừ khi `--no-write`), in một dòng JSON.

**Không chạy cùng lúc với job clean nến phút** (`intraday_raw_to_clean`, trong `make etl-clean`):
job đó ghi đè phân vùng `clean/intraday_1m` đang được đọc và DQ lỗi `FileNotFoundException`.
Chạy tuần tự: clean xong rồi mới DQ.

## Ngưỡng (duyệt 24/11/2026, đo trên VN30)

Giá trị mặc định là hằng số đầu `stocklake/dq/checks/intraday_completeness.py` và
`intraday_volume.py`; hàm nhận tham số để ghi đè.

| check_name | Metric | Đạt khi |
|---|---|---|
| `intraday_day_coverage_ratio` | tỉ lệ (mã, ngày giao dịch) kỳ vọng có >= 1 nến, toàn bộ | >= 0.998 |
| `intraday_symbol_coverage_ratio_min` | tỉ lệ trên của mã thấp nhất (mã nằm ở trường `symbol` trong JSON) | >= 0.98 |
| `intraday_thin_day_share` | số ngày mỏng (có dữ liệu nhưng < 100 nến) / số (mã, ngày) có dữ liệu | <= 0.01 |
| `intraday_volume_match_ratio` | tỉ lệ ngày so sánh được có \|rel_diff\| <= 1% | >= 0.99 |
| `intraday_volume_days_minute_gt_daily` | số ngày volume phút > volume ngày (nến phút không gồm lô lẻ nên luôn <=) | = 0 (kiểm cứng) |
| `intraday_volume_abs_diff_p95` | p95 của \|rel_diff\| | <= 0.01 |
| `intraday_days_outside_calendar` | số (mã, ngày) có nến nhưng không có trong lịch | = 0 |
| `intraday_days_missing`, `intraday_thin_days`, `intraday_volume_days_no_daily` | chỉ đo | luôn đạt |

`rel_diff = (volume phút - volume ngày) / volume ngày`; ngày thiếu/0 volume ngày không so sánh được.

## Ghi chú riêng (`conf/app/dq_notes.yaml`)

Dữ liệu, không phải code; thiếu file thì chạy không có ghi chú. Không lọc gì ở raw/clean.

- `known_periods`: giai đoạn đã biết của một mã (hiện có BSR, MCH: trước khi chuyển sang HOSE
  giờ giao dịch khác mẫu HOSE, rồi vài phiên không có nến). Suy ra từ dữ liệu, chưa đối chiếu
  nguồn niêm yết.
- `known_anomalies`: ngày bất thường đã biết (2024-11-15: volume phút thấp hơn volume ngày
  trên toàn thị trường; chưa giải thích, không điều tra thêm).

Trong JSON của job:

- `results`: metric chính trên TOÀN BỘ dữ liệu; đây là các metric so với ngưỡng.
- `notes`: nội dung ghi chú đã nạp.
- `excluding_known_periods`: tính lại tỉ lệ phủ (toàn bộ và mã thấp nhất), số ngày thiếu, tỉ lệ
  ngày mỏng, phân phối nến/ngày khi bỏ các `known_periods` (gồm cả ngày trống). Cũng được ghi
  vào `dq_report` với `check_name` có hậu tố `__excl_known_periods`, cùng ngưỡng.
- `known_period_stats`: mỗi giai đoạn — số ngày, phân phối nến/ngày, số nến ngoài 09:00–15:00
  giờ VN (kèm trước 09:15, nghỉ trưa, sau 14:45).
- `worst_volume_days`, `thinnest_days`, `missing_days_by_symbol`: trường `note` ghi nhãn giai
  đoạn / ngày bất thường mà dòng đó rơi vào (null nếu không).
