# jobs/dq

Chủ: Hồng Dương.

Điểm vào mỏng cho job kiểm tra chất lượng dữ liệu.

## Job hiện có

- `intraday_dq.py`: kiểm tra nến phút trên bảng clean — đầy đủ theo ngày giao dịch
  (lịch = VNINDEX trong clean daily) và đối chiếu tổng volume phút với volume ngày.
  Ghi `curated/dq_report` (trừ khi `--no-write`), in một dòng JSON.

## Khoá đọc: DQ và job clean chạy cùng lúc được

`run_intraday_dq` giữ khoá đọc `clean.intraday_1m` rồi `clean.daily_ohlcv` (luôn theo thứ tự này,
để không kẹt khoá) trong suốt thời gian đọc bảng clean; mọi action Spark trên hai bảng xong trước
khi nhả. Ghi `dq_report` làm sau, ngoài khoá. Job clean xin khoá ghi cùng tên
(`stocklake.common.locks`), nên:

- nhiều lượt DQ đọc cùng lúc được; job clean đang ghi thì DQ chờ, DQ đang đọc thì job clean chờ
  (bên ghi đang đợi thì bên đọc mới cũng đợi, tránh bỏ đói bên ghi);
- chờ tối đa 900 giây (mặc định `wait_timeout_s`); quá hạn thì job in lỗi `LockTimeout` ra stderr
  và thoát mã khác 0, không ghi gì;
- khoá tự hết hạn sau 3600 giây nếu tiến trình chết giữa chừng;
- JSON có `lock_waited_s` (tổng giây đã chờ cả hai khoá).
- DQ chạy một job Spark rỗng để chắc đã có executor RỒI mới xin khoá đọc: job clean không đặt
  `spark.cores.max` nên có thể giữ hết core trong lúc chờ khoá ghi; nếu DQ giữ khoá đọc mà chưa có
  core thì hai job chờ nhau tới khi job clean hết 900 giây và lỗi (đã gặp khi đo 24/11/2026).

Quy tắc cũ "không chạy cùng lúc với job clean nến phút" không còn.

## Quy tắc đạt / không đạt (quyết định 24/11/2026)

**Đạt/không đạt tính trên bộ số loại trừ giai đoạn đã biết; bộ số toàn bộ dữ liệu chỉ để tham
khảo.** Áp dụng cho các check có cả hai bộ số: `intraday_day_coverage_ratio`,
`intraday_symbol_coverage_ratio_min`, `intraday_days_missing`, `intraday_thin_days`,
`intraday_thin_day_share` (giai đoạn đã biết lấy từ `conf/app/dq_notes.yaml`). Check volume và
`intraday_days_outside_calendar` không có bộ số loại trừ nên tính trên toàn bộ dữ liệu. Thiếu file
ghi chú hoặc không có giai đoạn nào thì hai bộ số trùng nhau (JSON ghi `known_periods_excluded: 0`).
`passed` tổng của báo cáo = mọi check kết luận đều đạt.

Ví dụ đo trên VN30, `intraday_thin_day_share` (ngưỡng <= 1%):

| Bộ số | Giá trị | Vai trò | Kết quả |
|---|---|---|---|
| toàn bộ dữ liệu | 2,19% | tham khảo (`__all_data`) | không đạt, không quyết định kết luận |
| loại trừ giai đoạn đã biết | 0,81% | kết luận (`intraday_thin_day_share`) | đạt |

### Tên trong `dq_report.check_name` (đổi từ 24/11/2026)

Cột của `dq_report` giữ nguyên (`run_id, table_name, check_name, metric_value, threshold, passed,
run_ts`); chỉ đổi cách đặt tên:

| check_name | Nội dung |
|---|---|
| `<check>` (vd `intraday_thin_day_share`) | KẾT LUẬN: metric và `passed` tính trên bộ số loại trừ giai đoạn đã biết |
| `<check>__all_data` | THAM KHẢO: cùng ngưỡng, tính trên toàn bộ dữ liệu; `passed` chỉ là so sánh ngưỡng |
| check không có bộ số loại trừ (volume, ngày ngoài lịch) | tên thường, toàn bộ dữ liệu |

Đọc các lượt chạy cũ: trước ngày đổi tên, tên thường là bộ số TOÀN BỘ dữ liệu và bộ số loại trừ nằm
ở `<check>__excl_known_periods`. Lượt cũ nhận ra bằng việc có dòng `__excl_known_periods`; lượt mới
có dòng `__all_data` và không còn `__excl_known_periods`. Dòng cũ vẫn nằm trong bảng, không migrate.
Khi so xu hướng qua hai kiểu tên: lượt cũ lấy `<check>__excl_known_periods`, lượt mới lấy `<check>`.

### Hình dạng JSON của job

Mỗi check có bộ số loại trừ:

```json
{"check_name": "intraday_thin_day_share", "group": "completeness", "table_name": "intraday_1m",
 "metric": 0.0081, "threshold": 0.01, "passed": true, "basis": "excluding_known_periods",
 "reference_all_data": {"metric": 0.0219, "passed": false}}
```

Check không có bộ số loại trừ: cùng khoá nhưng `"basis": "all_data"` và không có
`reference_all_data`. Cấp cao nhất có thêm `verdict_rule` (mô tả quy tắc), `passed` (boolean
tổng), `known_periods_excluded`, `lock_waited_s`. `intraday_symbol_coverage_ratio_min` có thêm
`symbol` (mã thấp nhất của bộ số kết luận) và `reference_all_data.symbol`.

## Ngưỡng (duyệt 24/11/2026, đo trên VN30)

Giá trị mặc định là hằng số đầu `stocklake/dq/checks/intraday_completeness.py` và
`intraday_volume.py`; hàm nhận tham số để ghi đè.

| check_name | Metric | Đạt khi |
|---|---|---|
| `intraday_day_coverage_ratio` | tỉ lệ (mã, ngày giao dịch) kỳ vọng có >= 1 nến | >= 0.998 |
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

- `results`: các check kết luận (xem trên), kèm `reference_all_data` khi có.
- `notes`: nội dung ghi chú đã nạp.
- `excluding_known_periods`: `periods_excluded` và phân phối nến/ngày của bộ số loại trừ (gồm cả
  ngày trống của giai đoạn đã biết); các metric của bộ số này nằm ở `results`.
- `known_period_stats`: mỗi giai đoạn — số ngày, phân phối nến/ngày, số nến ngoài 09:00–15:00
  giờ VN (kèm trước 09:15, nghỉ trưa, sau 14:45).
- `worst_volume_days`, `thinnest_days`, `missing_days_by_symbol`, `bars_per_day`: tính trên toàn
  bộ dữ liệu; trường `note` ghi nhãn giai đoạn / ngày bất thường mà dòng đó rơi vào (null nếu không).
