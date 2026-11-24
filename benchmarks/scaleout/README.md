# benchmarks/scaleout

Chủ: Hùng Dương.

Đo: thời gian job trên 1, 2, 3 worker với cùng dữ liệu nến phút thật (mục tiêu 60-70 triệu dòng), cố định 2 core + 3GB mỗi executor và shuffle partitions; 1 lần làm nóng + 5 lần đo, báo trung vị và độ lệch.
