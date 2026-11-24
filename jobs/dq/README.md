# jobs/dq

Chủ: Hồng Dương.

Điểm vào mỏng cho job kiểm tra chất lượng dữ liệu.

## Job hiện có

- `intraday_dq.py`: kiểm tra nến phút trên bảng clean — đầy đủ theo ngày giao dịch
  (lịch = VNINDEX trong clean daily) và đối chiếu tổng volume phút với volume ngày.
  Ghi `curated/dq_report` (trừ khi `--no-write`), in một dòng JSON gồm cả phân phối chênh
  lệch volume để chọn ngưỡng. Ngưỡng hiện tại là giá trị tạm, chưa hiệu chỉnh.
