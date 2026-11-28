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
- khoá đọc có hạn 600 giây, được luồng nền gia hạn mỗi 200 giây trong lúc DQ còn chạy; tiến trình
  chết giữa chừng thì khoá hết hạn sau tối đa 600 giây kể từ lần gia hạn cuối (file khoá hết hạn
  do job clean xoá khi xin khoá ghi);
- JSON có `lock_waited_s` (tổng giây đã chờ cả hai khoá).
- DQ chạy một job Spark rỗng để chắc đã có executor RỒI mới xin khoá đọc: job clean không đặt
  `spark.cores.max` nên có thể giữ hết core trong lúc chờ khoá ghi; nếu DQ giữ khoá đọc mà chưa có
  core thì hai job chờ nhau tới khi job clean hết 900 giây và lỗi (đã gặp khi đo 24/11/2026).

Quy tắc cũ "không chạy cùng lúc với job clean nến phút" không còn.

## Định nghĩa (quyết định 28/11/2026)

Ba thay đổi so với quy tắc 24/11/2026; số theo quy tắc cũ vẫn có trong JSON ở khối `legacy_rules`
để so (không ghi `dq_report`).

1. **Ngày kỳ vọng có nến** của một mã = ngày trong lịch (VNINDEX trong clean daily), nằm trong
   [ngày nến phút đầu, ngày nến phút cuối] của mã (cắt theo `--start/--end` như cũ), MÀ nến ngày
   của chính mã đó có volume >= 100 cổ phiếu (`MIN_DAILY_VOLUME_FOR_EXPECTED_DAY`). Ngày không có
   dòng nến ngày, volume null hoặc < 100 không còn là ngày kỳ vọng.
   *Lý do:* ngày không giao dịch hoặc chỉ khớp lô lẻ không phải thiếu dữ liệu. Đo trên 382 mã HOSE
   ngoài VN30: 19.912 (mã, ngày) thiếu nến gồm 6.342 không có dòng nến ngày, 9.231 volume ngày = 0,
   3.971 volume ngày < 100, chỉ 368 có volume ngày >= 100.
   Ngày SAU ngày nến phút cuối không là ngày kỳ vọng; để phát hiện nguồn ngừng cấp nến, chỉ số
   chỉ đo `intraday_days_daily_volume_after_last_bar` đếm (mã, ngày) trong lịch sau ngày nến cuối
   có nến ngày volume >= 100 (JSON kèm `days_daily_volume_after_last_bar_by_symbol`, tối đa 10 mã
   nhiều ngày nhất). Nếu daily về trước nến phút của phiên cuối thì mỗi mã có thể bị đếm 1 ngày.
2. **Ngày thưa** = (mã, ngày) kỳ vọng có dữ liệu mà < 100 nến VÀ có volume ngày VÀ
   (volume ngày − volume phút) > 1% volume ngày VÀ (volume ngày − volume phút) >= 1.000 cổ phiếu.
   `intraday_thin_days`, `intraday_thin_day_share` dùng định nghĩa này. Số ngày < 100 nến theo
   cách cũ vẫn ghi nhưng chỉ đo: `intraday_low_bar_days`, `intraday_low_bar_day_share`.
   *Lý do:* nửa số mã có dưới 41 nến ở ngày điển hình, nên ngưỡng 100 nến đơn thuần đánh dấu
   60,31% số ngày của 412 mã (65,08% của 382 mã ngoài VN30); ngày ít nến chỉ đáng ngờ khi volume
   phút cũng hụt so với volume ngày.
3. **Khớp volume**: một ngày so sánh được là KHỚP khi |rel_diff| <= 1% HOẶC
   |volume phút − volume ngày| < 1.000 cổ phiếu (`DEFAULT_MATCH_ABS_DIFF_SHARES`).
   `intraday_volume_abs_diff_p95` tính trên độ lệch HIỆU DỤNG = 0 nếu ngày đó lệch < 1.000 cổ
   phiếu, ngược lại |rel_diff| — hệ quả của định nghĩa khớp mới (ngày đã "khớp" không kéo p95
   lên); p95 của |rel_diff| thô vẫn ghi ở `intraday_volume_abs_diff_p95_raw` (chỉ đo).
   *Lý do:* nến phút không gồm lô lẻ: 98,5% số ngày lệch > 1% có volume ngày không chia hết cho
   100; mức thiếu điển hình là 60 cổ phiếu (trung vị của độ lệch tuyệt đối trên các ngày lệch
   > 1%).
   `intraday_volume_days_minute_gt_daily` (kiểm cứng, ngưỡng 0) tính đạt/không đạt trên bộ số đã
   bỏ các ngày bất thường đánh dấu `exclude_from: [volume_minute_gt_daily]` trong
   `conf/app/dq_notes.yaml`. *Lý do:* 47 (mã, ngày) có volume phút > volume ngày dồn vào 5 ngày
   nguồn có nến thừa (đã kiểm 6 ca: gọi lại VCI vẫn trả đúng các nến đang lưu, nến ngày của VCI và
   VNDirect khớp nhau ở con số thấp hơn); chưa rõ nguyên nhân phía nguồn, dữ liệu giữ nguyên ở raw
   và clean. Ngày phút > ngày ngoài 5 ngày này vẫn làm kiểm tra không đạt.

## Quy tắc đạt / không đạt (quyết định 24/11/2026, mở rộng 28/11/2026)

**Đạt/không đạt tính trên bộ số loại trừ; bộ số toàn bộ dữ liệu chỉ để tham khảo.** Bộ loại trừ
tuỳ check (trường `basis` trong JSON):

| Check | Bộ loại trừ (`basis`) |
|---|---|
| `intraday_day_coverage_ratio`, `intraday_symbol_coverage_ratio_min`, `intraday_days_missing`, `intraday_thin_days`, `intraday_thin_day_share`, `intraday_low_bar_days`, `intraday_low_bar_day_share` | bỏ giai đoạn đã biết (`excluding_known_periods`) |
| `intraday_volume_days_minute_gt_daily` | bỏ ngày bất thường có `exclude_from: [volume_minute_gt_daily]` (`excluding_known_anomalies`) |

Các check còn lại (volume khác, `intraday_days_outside_calendar`,
`intraday_days_daily_volume_after_last_bar`) không có bộ loại trừ nên tính trên toàn bộ dữ liệu
(`basis: all_data`). Thiếu file ghi chú hoặc không có gì để loại thì hai bộ số trùng nhau.
`passed` tổng của báo cáo = mọi check kết luận đều đạt.

Ví dụ đo trên VN30 (24/11/2026, định nghĩa ngày mỏng cũ), `intraday_thin_day_share` (ngưỡng <= 1%):

| Bộ số | Giá trị | Vai trò | Kết quả |
|---|---|---|---|
| toàn bộ dữ liệu | 2,19% | tham khảo (`__all_data`) | không đạt, không quyết định kết luận |
| loại trừ giai đoạn đã biết | 0,81% | kết luận (`intraday_thin_day_share`) | đạt |

### Tên trong `dq_report.check_name` (đổi từ 24/11/2026)

Cột của `dq_report` giữ nguyên (`run_id, table_name, check_name, metric_value, threshold, passed,
run_ts`); chỉ đổi cách đặt tên:

| check_name | Nội dung |
|---|---|
| `<check>` (vd `intraday_thin_day_share`) | KẾT LUẬN: metric và `passed` tính trên bộ loại trừ |
| `<check>__all_data` | THAM KHẢO: cùng ngưỡng, tính trên toàn bộ dữ liệu; `passed` chỉ là so sánh ngưỡng |
| check không có bộ loại trừ | tên thường, toàn bộ dữ liệu |

Từ 28/11/2026: có thêm `intraday_volume_days_minute_gt_daily__all_data` (trước đó dòng tên thường
là toàn bộ dữ liệu), các dòng mới `intraday_low_bar_days[__all_data]`,
`intraday_low_bar_day_share[__all_data]`, `intraday_days_daily_volume_after_last_bar`,
`intraday_volume_abs_diff_p95_raw`. `intraday_thin_*`, `intraday_day_coverage_ratio`,
`intraday_symbol_coverage_ratio_min`, `intraday_days_missing`, `intraday_volume_match_ratio`,
`intraday_volume_abs_diff_p95` giữ tên nhưng đổi định nghĩa (xem trên): so xu hướng qua mốc
28/11/2026 phải nhớ điều này.

Đọc các lượt chạy cũ: trước 24/11/2026, tên thường là bộ số TOÀN BỘ dữ liệu và bộ số loại trừ nằm
ở `<check>__excl_known_periods`. Lượt cũ nhận ra bằng việc có dòng `__excl_known_periods`; lượt mới
có dòng `__all_data` và không còn `__excl_known_periods`. Dòng cũ vẫn nằm trong bảng, không migrate.
Khi so xu hướng qua hai kiểu tên: lượt cũ lấy `<check>__excl_known_periods`, lượt mới lấy `<check>`.

### Hình dạng JSON của job

Mỗi check có bộ loại trừ:

```json
{"check_name": "intraday_thin_day_share", "group": "completeness", "table_name": "intraday_1m",
 "metric": 0.0081, "threshold": 0.01, "passed": true, "basis": "excluding_known_periods",
 "reference_all_data": {"metric": 0.0219, "passed": false}}
```

Check không có bộ loại trừ: cùng khoá nhưng `"basis": "all_data"` và không có
`reference_all_data`. Cấp cao nhất có thêm `verdict_rule` (mô tả quy tắc), `passed` (boolean
tổng), `known_periods_excluded`, `lock_waited_s`, `thresholds`. `intraday_symbol_coverage_ratio_min`
có thêm `symbol` (mã thấp nhất của bộ số kết luận) và `reference_all_data.symbol`.

## Ngưỡng (duyệt 24/11/2026 trên VN30; định nghĩa đổi 28/11/2026)

Giá trị mặc định là hằng số đầu `stocklake/dq/checks/intraday_completeness.py` và
`intraday_volume.py`; hàm nhận tham số để ghi đè. JSON ghi lại ở `thresholds`.

| check_name | Metric | Đạt khi |
|---|---|---|
| `intraday_day_coverage_ratio` | tỉ lệ (mã, ngày) kỳ vọng có >= 1 nến | >= 0.998 |
| `intraday_symbol_coverage_ratio_min` | tỉ lệ trên của mã thấp nhất (mã nằm ở trường `symbol` trong JSON) | >= 0.98 |
| `intraday_thin_day_share` | số ngày thưa / số (mã, ngày) kỳ vọng có dữ liệu | <= 0.01 |
| `intraday_volume_match_ratio` | tỉ lệ ngày so sánh được KHỚP (\|rel_diff\| <= 1% hoặc lệch < 1.000 cổ phiếu) | >= 0.99 |
| `intraday_volume_days_minute_gt_daily` | số ngày volume phút > volume ngày, đã bỏ ngày đánh dấu `exclude_from` | = 0 (kiểm cứng) |
| `intraday_volume_abs_diff_p95` | p95 của độ lệch hiệu dụng (0 nếu lệch < 1.000 cổ phiếu, ngược lại \|rel_diff\|) | <= 0.01 |
| `intraday_days_outside_calendar` | số (mã, ngày) có nến nhưng không có trong lịch | = 0 |
| `intraday_days_missing`, `intraday_thin_days`, `intraday_low_bar_days`, `intraday_low_bar_day_share`, `intraday_days_daily_volume_after_last_bar`, `intraday_volume_days_no_daily`, `intraday_volume_abs_diff_p95_raw` | chỉ đo | luôn đạt |

`rel_diff = (volume phút - volume ngày) / volume ngày`; ngày thiếu/0 volume ngày không so sánh được.
Volume tính trên mọi ngày có nến phút (không chỉ ngày kỳ vọng), toàn bộ dữ liệu.

## Ghi chú riêng (`conf/app/dq_notes.yaml`)

Dữ liệu, không phải code; thiếu file thì chạy không có ghi chú. Không lọc gì ở raw/clean.

- `known_periods`: giai đoạn đã biết của một mã (hiện có BSR, MCH: trước khi chuyển sang HOSE
  giờ giao dịch khác mẫu HOSE, rồi vài phiên không có nến). Suy ra từ dữ liệu, chưa đối chiếu
  nguồn niêm yết.
- `known_anomalies`: ngày bất thường đã biết. Trường tuỳ chọn `exclude_from` (mặc định rỗng; giá
  trị hợp lệ duy nhất hiện nay `volume_minute_gt_daily`) loại ngày đó khỏi bộ số kết luận của kiểm
  tra cứng tương ứng; không có trường này thì chỉ gắn nhãn. Hiện có:
  - 2024-11-15: volume phút thấp hơn volume ngày trên toàn thị trường; chưa giải thích, không
    điều tra thêm; KHÔNG loại khỏi kiểm tra nào;
  - 2025-05-06 (21 mã), 2025-05-26 (7), 2025-11-20 (13), 2026-03-16 (5), 2026-11-23 (1, THG):
    nguồn có nến phút thừa, volume phút > volume ngày; `exclude_from: [volume_minute_gt_daily]`.

Trong JSON của job:

- `results`: các check kết luận (xem trên), kèm `reference_all_data` khi có.
- `notes`: nội dung ghi chú đã nạp (mỗi ngày bất thường có `exclude_from`).
- `excluding_known_periods`: `periods_excluded` và phân phối nến/ngày của bộ số loại trừ (ngày kỳ
  vọng); các metric của bộ số này nằm ở `results`.
- `known_period_stats`: mỗi giai đoạn — số ngày kỳ vọng (`expected_days`), số ngày lịch theo quy tắc
  cũ (`calendar_days`), `thin_days` (định nghĩa mới), `low_bar_days` (< 100 nến), phân phối
  nến/ngày, số nến ngoài 09:00–15:00 giờ VN (kèm trước 09:15, nghỉ trưa, sau 14:45).
- `expected_day_filter`: số ngày lịch trong [đầu, cuối], số ngày kỳ vọng, số ngày bị bỏ (kèm số
  ngày bị bỏ mà vẫn có nến phút, số ngày không có volume ngày), toàn bộ dữ liệu.
- `volume_minute_gt_daily_excluded`: các ngày đang được loại khỏi kiểm tra phút > ngày.
- `minute_gt_daily_days`: tối đa 50 (mã, ngày) volume phút > volume ngày, toàn bộ dữ liệu.
- `days_daily_volume_after_last_bar_by_symbol`: xem mục 1 ở trên.
- `worst_volume_days`, `thinnest_days` (ít nến nhất, kèm `thin` và `volume_gap`), `thin_days_top`
  (ngày thưa thiếu nhiều cổ phiếu nhất), `missing_days_by_symbol` (kèm `thin_days`,
  `low_bar_days`), `bars_per_day`: tính trên toàn bộ dữ liệu (phần đầy đủ ngày: trên ngày kỳ
  vọng); trường `note` ghi nhãn giai đoạn / ngày bất thường mà dòng đó rơi vào (null nếu không).
- `legacy_rules`: số theo quy tắc cũ để so — `day_coverage_ratio`, `symbol_coverage_ratio_min` (+
  `symbol_coverage_min_symbol`), `days_expected`, `days_missing`, `thin_days`/`thin_day_share`
  (< 100 nến) tính trên mọi ngày lịch trong [đầu, cuối], đã bỏ giai đoạn đã biết như bộ kết luận;
  `volume_match_ratio` (chỉ 1%), `volume_abs_diff_p95` (thô), `volume_days_minute_gt_daily`
  (không loại ngày nào) trên toàn bộ dữ liệu như check volume.

## Kết quả 28/11/2026

Quy tắc mới, kèm số theo quy tắc cũ (`legacy_rules`) để so. 382 mã = HOSE ngoài VN30; 412 mã = cả
hai. Trong ngoặc: ngưỡng đạt.

| Kiểm tra | VN30 mới | VN30 cũ | 382 mã mới | 382 mã cũ | 412 mã mới | 412 mã cũ |
|---|---|---|---|---|---|---|
| Độ đủ (>= 99,8%) | 100% | 100% | 99,87% | 93,27% | 99,88% | 93,74% |
| Mã thấp nhất (>= 98%) | 100% | 100% | 96,5% (DTT) | 17,4% (TTE) | 96,5% (DTT) | 17,4% (TTE) |
| Tỷ lệ ngày thưa (<= 1%) | 0,02% | 0,81% | 0,07% | 65,08% | 0,06% | 60,31% |
| Khớp khối lượng (>= 99%) | 99,84% | 99,64% | 99,86% | 94,15% | 99,86% | 94,57% |
| p95 lệch khối lượng (<= 1%) | 0,31% | 0,35% | 0,09% | 1,22% | 0,12% | 1,10% |
| Ngày phút > ngày (= 0) | 0 | 0 | 0 (47 trên toàn bộ dữ liệu) | 47 | 0 (47) | 47 |
| Kết luận | đạt | đạt | không đạt | không đạt | không đạt | không đạt |

Theo quy tắc mới chỉ còn một kiểm tra không đạt là độ đủ của mã thấp nhất (ngưỡng 98%). 7 mã dưới
98%: DTT 96,5%, COM 97,2%, SBV 97,2%, LGC 97,5%, SC5 97,6%, BTT 97,8%, CLW 97,9% (các ngày nến
ngày có từ 100 cổ phiếu nhưng không có nến phút, phần lớn rơi vào cuối năm 2023; riêng SBV chủ yếu
2024–2025). Ngưỡng chưa đổi; chờ quyết định.

## Hiệu năng

Bảng nến phút (26,8 triệu dòng) chỉ quét một lần thành bảng theo (mã, ngày) có số nến, nến
đầu/cuối và tổng volume (cache); độ đủ, volume, ngày ngoài lịch, ngày sau nến cuối đều dựng từ
bảng này. Riêng `known_period_stats` vẫn đọc nến của các mã có giai đoạn đã biết (như trước).
