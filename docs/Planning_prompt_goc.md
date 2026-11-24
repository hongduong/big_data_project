<role>  
Bạn là Principal Data Engineer & Distributed Systems Architect có kinh nghiệm triển khai thực tế hệ sinh thái Hadoop HDFS, Apache Kafka (KRaft) và Apache Spark 3.5 trên môi trường Ubuntu Linux + Docker Engine native.  
</role>  
  
<task>  
Lập một bản **Implementation Plan (Kế hoạch triển khai kỹ thuật chi tiết)** cho đồ án tốt nghiệp môn Big Data với đề tài:  
**"Hệ thống dữ liệu lớn Kafka–Spark–HDFS thu thập, truyền tải và lưu trữ dữ liệu thị trường chứng khoán Việt Nam, ứng dụng phân tích sơ bộ xu hướng giá."**  
  
QUAN TRỌNG: Ở lượt này, **KHÔNG viết full source code**. Tập trung thiết kế kiến trúc, phân bổ tài nguyên phần cứng tối ưu cho Ubuntu native, chốt Data Contracts (schema dùng chung), cấu trúc thư mục, kịch bản benchmark chuẩn khoa học và lộ trình thực thi 4 tuần cho nhóm 4 người để tôi review và duyệt trước khi tạo file.  
</task>  
  
<hardware_and_os_profile>  
Toàn bộ cụm Big Data và các bài đo Benchmark (Chương 3) chạy trực tiếp trên 1 máy bàn cài **Ubuntu Server/Desktop native (x86_64/amd64)**:  
- CPU: AMD Ryzen 7 3800X (8 physical cores / 16 threads).  
- RAM: 32GB DDR4.  
- Runtime: **Docker Engine native + Docker Compose v2** (KHÔNG dùng Docker Desktop, KHÔNG qua WSL2, tận dụng trực tiếp Linux cgroups v2 và native ext4/NVMe disk I/O).  
- Ngân sách tài nguyên tối đa cho cụm Docker: **≤ 26GB RAM** và **≤ 14 vCPUs** (chừa 6GB RAM và 2 threads cho Ubuntu OS + IDE).  
- Yêu cầu cấu hình tài nguyên trong Plan:  
  + Khóa cứng `mem_limit`, `cpus`, `JVM Heap` (`KAFKA_HEAP_OPTS`, `HADOOP_HEAPSIZE`, `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES`) cho từng container để tránh Linux OOM Killer và đảm bảo tính lặp lại của benchmark.  
  + Gợi ý phân bổ tham chiếu (26GB RAM / 14 vCPUs): Kafka KRaft (2GB/2c), HDFS NameNode (1.5GB/1c), 2x HDFS DataNode (2GB/1c mỗi node), Spark Master (1.5GB/1c), **3x Spark Worker (4.5GB/2c mỗi worker = 13.5GB/6c)**, Collector/Scheduler (1.5GB/1c), Streamlit UI (2GB/1c).  
  + Dùng Docker Compose `profiles` (hoặc override file) để chuyển đổi nhanh giữa chế độ **1, 2, 3 Spark Workers** khi chạy benchmark Scale-out Speedup.  
  + Tận dụng quyền root trên Ubuntu native để tích hợp bước xả Linux Page Cache (`sync; echo 3 > /proc/sys/vm/drop_caches`) vào script benchmark đọc file (Cold Read vs Warm Read).  
</hardware_and_os_profile>  
  
<tech_stack>  
- Ngôn ngữ: Python 3.10+ (`vnstock`, `requests`, `feedparser`, `yfinance`, `pandas`, `pyarrow`, `confluent-kafka` hoặc `kafka-python`).  
- Truyền tải: **Apache Kafka chế độ KRaft** (tuyệt đối KHÔNG dùng Zookeeper).  
- Lưu trữ: **Hadoop HDFS** (1 NameNode + 2 DataNodes, `dfs.replication = 2`), định dạng **Apache Parquet** (so sánh nén `snappy` vs `zstd`).  
- Xử lý: **Apache Spark 3.5** (PySpark, Spark SQL, Structured Streaming, Spark MLlib).  
- Lập lịch & Giao diện: `cron` (hoặc Python scheduler trong container) và **Streamlit**.  
- Đóng gói: 100% tái lập bằng `docker-compose.yml` + `Makefile` / Bash scripts chuẩn Linux.  
</tech_stack>  
  
<architecture_requirements>  
1. Tầng Thu thập (Ingestion):  
   - Xây dựng khung **Python Collector Framework** dùng chung cho mọi nguồn.  
   - Cơ chế bắt buộc: chạy theo lịch, **idempotent** (chạy lại không tạo dữ liệu trùng/rác), **retry + exponential backoff**, và **fallback source** khi nguồn chính lỗi.  
   - Mọi bản ghi `raw` phải gắn metadata truy vết: `source`, `ingest_ts`, `available_at` (bắt buộc để kiểm chứng chống rò rỉ dữ liệu tương lai / look-ahead bias).  
   - Phạm vi dữ liệu Batch (2015 – nay):  
     * OHLCV ngày: ~400 mã HOSE, VN-Index, VN30 (~1,1 triệu dòng); thiết kế mở rộng sang HNX/UPCOM (~1.600 mã, ~4,5 triệu dòng).  
     * Khối ngoại: mua/bán ròng theo mã và toàn sàn theo ngày (~1,1 triệu dòng).  
     * Hồ sơ doanh nghiệp & lịch sự kiện: số cổ phiếu lưu hành, ngành; ngày công bố BCTC, đáo hạn phái sinh, cơ cấu ETF, nghỉ Tết, họp Fed.  
     * Vĩ mô thế giới (mở rộng): US 10Y Treasury, DXY, Brent, S&P 500, VIX, USD/VND (FRED, yfinance).  
   - Phạm vi dữ liệu Streaming (Kafka Topics):  
     * `intraday_prices`: giá phút trong phiên (~4 triệu bản ghi / 8 tuần). Bắt buộc có **Stream Replay Simulator** phát lại dữ liệu lịch sử với tốc độ tùy chỉnh (ví dụ 500 – 10.000 msg/s) để test & benchmark ngoài giờ giao dịch.  
     * `market_news`: tin tức RSS từ CafeF, Vietstock, VnExpress.  
  
2. Tầng Truyền tải & Xử lý (Kafka KRaft -> Spark 3.5):  
   - Spark Structured Streaming đọc từ Kafka topics, parse JSON theo schema chặt chẽ, ghi xuống HDFS dạng Parquet.  
   - Bắt buộc cấu hình **HDFS Checkpointing** và chiến lược deduplication (`dropDuplicates` / watermark / idempotent upsert) để chứng minh không mất, không trùng bản ghi.  
  
3. Tầng Lưu trữ 3 lớp trên HDFS (`/data/raw`, `/data/clean`, `/data/curated`):  
   - `raw`: lưu nguyên trạng kèm metadata thu thập.  
   - `clean`: đã chuẩn hóa kiểu dữ liệu, khử trùng, xử lý giá điều chỉnh, đã qua kiểm tra chất lượng.  
   - `curated`: bảng phân tích tổng hợp và bảng feature phục vụ Spark SQL, MLlib và Streamlit.  
   - Phân vùng & Compaction: phân tích rõ trade-off khi dùng `partitionBy("year", "symbol")` trên 400–1.600 mã (rủi ro small files / áp lực RAM NameNode), đề xuất chiến lược phân vùng tối ưu và **Job Compaction** định kỳ gom file Parquet.  
   - Có tài liệu **Data Dictionary** đầy đủ cho cả 3 lớp.  
  
4. Kiểm soát Chất lượng Dữ liệu Tự động (Automated Data Quality):  
   - Chạy tự động sau mỗi lần làm sạch, xuất báo cáo định lượng gồm 5 nhóm kiểm tra:  
     1) Completeness (tỷ lệ ngày giao dịch đầy đủ theo lịch sàn).  
     2) Uniqueness (tỷ lệ bản ghi trùng khóa).  
     3) Validity (miền giá trị: giá <= 0, High < Low, Close ngoài [Low, High], Volume < 0).  
     4) Adjusted Price Integrity (phát hiện bước nhảy giá bất thường do chia tách/cổ tức).  
     5) Cross-source Consistency (đối chiếu độ lệch giữa vnstock và CafeF/Vietstock).  
  
5. Phân tích Sơ bộ & Dashboard (Trọng tâm phụ – chứng minh kho dữ liệu dùng được):  
   - Spark SQL: thống kê toàn sàn, độ rộng thị trường, thanh khoản, dòng tiền khối ngoại.  
   - Spark MLlib: mô hình baseline **Logistic Regression** dự đoán hướng giá **T+5**.  
     * So sánh Accuracy, F1-macro, AUC với **Naive Baseline** (majority class / persistence).  
     * Thực hiện **Leakage Test** bắt buộc: chia tập theo thời gian (time-based split), đảm bảo mọi feature tại ngày `t` chỉ dùng bản ghi có `available_at <= t`, chạy thêm kịch bản cố tình làm rò rỉ dữ liệu để chứng minh tác động.  
   - Streamlit Dashboard: đọc trực tiếp từ lớp `curated` trên HDFS/Parquet hiển thị biểu đồ giá, khối ngoại, độ rộng thị trường, báo cáo Data Quality và kết quả baseline.  
</architecture_requirements>  
  
<benchmark_suite>  
Hệ thống bắt buộc có thư mục `benchmarks/` chứa các Bash/Python script chạy lặp lại được trên Ubuntu và tự xuất kết quả CSV/JSON + biểu đồ phục vụ Chương 3:  
1. Storage Benchmark: Dung lượng đĩa & thời gian đọc (Cold Read sau khi `drop_caches` + Warm Read, cả full scan lẫn filtered query) giữa `CSV` vs `Parquet` vs `Parquet phân vùng`; nén `snappy` vs `zstd`; số lượng file & thời gian đọc trước/sau `Compaction`.  
2. Processing Scale-out Benchmark: Thời gian chạy job Spark ETL/Aggregation & hệ số Speedup khi bật **1, 2, 3 Spark Workers** (cố định 2 cores + 4.5GB RAM mỗi worker, kiểm soát `spark.sql.shuffle.partitions`).  
3. Streaming Benchmark: Throughput (records/sec) và End-to-End Latency (`p50`, `p95`) tính từ timestamp lúc Producer đẩy vào Kafka đến khi Spark commit batch lên HDFS.  
4. Fault Tolerance Benchmark: Kịch bản `docker kill` tự động cho 3 đối tượng: (a) 1 Spark Worker đang chạy job, (b) 1 HDFS DataNode (với `dfs.replication=2`), (c) Kafka Consumer / Spark Streaming job. Đo số bản ghi mất/trùng và thời gian phục hồi/tăng thêm.  
5. Quality & Baseline Summary: Xuất tổng hợp chỉ số DQ 5 tiêu chí + bảng đánh giá ML baseline & Leakage Test.  
</benchmark_suite>  
  
<team_and_timeline>  
- Quy mô nhóm: 4 thành viên làm song song theo Git Flow:  
  * TV1 (Data Engineer – Ingestion & DQ): Collector framework, batch crawlers, Data Quality engine.  
  * TV2 (Streaming Engineer – Kafka & Producers): Kafka KRaft, producers (intraday, RSS, replay simulator), streaming & consumer fault-tolerance benchmarks.  
  * TV3 (Big Data Infra – HDFS, Spark & Storage): Docker Compose cluster + Linux/JVM resource tuning, HDFS 3 layers, Spark Streaming/Batch ETL, compaction, storage/scale-out/DataNode-worker fault-tolerance benchmarks.  
  * TV4 (Analytics & UI): Curated feature store, Spark SQL analytics, Spark MLlib Logistic Regression T+5 + Leakage Test, Streamlit Dashboard.  
- Mốc thời gian:  
  * **Hết Tuần 2 (Mốc 1/3 dự án):** Hạ tầng Docker chạy ổn định trên Ubuntu (Ryzen 3800X/32GB); pipeline chạy thông suốt End-to-End (Collector -> Kafka KRaft -> Spark Structured Streaming -> HDFS Parquet `raw`/`clean`) với dữ liệu mẫu; Streamlit đọc và hiển thị được dữ liệu mẫu.  
  * **Hết Tuần 4 (100% dự án):** Đầy đủ dữ liệu 3 lớp, Automated DQ, hoàn tất 5 bộ Benchmark có số liệu thực tế, hoàn thiện Dashboard, Data Dictionary và đóng gói tái lập bằng Docker Compose.  
</team_and_timeline>  
  
<deliverables>  
Hãy trình bày bản Implementation Plan theo đúng 6 phần sau:  
1. **System Architecture & Resource Allocation Map:** Sơ đồ luồng dữ liệu (Mermaid), danh sách toàn bộ container (chọn chính xác image `x86_64` ổn định, tương thích Spark 3.5 + Hadoop 3.x + Kafka KRaft), bảng phân bổ Port, Docker Volume, CPU, RAM và JVM Heap vừa khít ngân sách ≤ 26GB RAM / 14 vCPUs.  
2. **Repository Structure & Git Strategy:** Cây thư mục chi tiết, tách vùng sở hữu rõ ràng cho TV1–TV4 để tránh merge conflict, kèm quy ước branch/PR.  
3. **Shared Data Contracts:** Định nghĩa schema cụ thể (tên cột, kiểu dữ liệu, nullable, khóa chính) cho 2 Kafka topics và các bảng cốt lõi ở 3 lớp `raw`, `clean`, `curated` (nhấn mạnh cột `available_at` chống leakage).  
4. **Critical Design Trade-offs:** Phân tích ngắn gọn, sắc bén 4 điểm cốt lõi để dùng làm luận cứ Chương 2–3: (a) Chiến lược `partitionBy` vs Small Files trên 400–1.600 mã, (b) Cách đo End-to-End Latency `p50/p95` chính xác trong Structured Streaming, (c) Cách giữ bài đo Speedup 1/2/3 Workers và Cold/Warm Read công bằng trên Ubuntu native, (d) Thiết kế Leakage Test thuyết phục hội đồng.  
5. **4-Week Execution Roadmap (TV1–TV4):** Chia task cụ thể từng tuần kèm **Definition of Done (DoD)** và lệnh CLI xác minh hoàn thành cho từng milestone.  
6. **Phase 0 Scaffolding Plan:** Liệt kê chính xác thứ tự các file nền tảng sẽ tạo ở lượt tiếp theo để push lên nhánh `main` cho cả 4 thành viên pull về code song song ngay.  
</deliverables>  
