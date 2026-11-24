# dashboard/

Chủ: Thanh. Ứng dụng Streamlit (cổng 8501), chỉ đọc lớp `curated` qua WebHDFS (`fsspec`).

- Chỉ import `stocklake.contracts` và `stocklake.common`; không import Spark.
- `app.py`: trang chính (cấu hình kết nối, kiểm tra HDFS, bảng schema curated).
- `pages/`: các trang phụ.

Chạy: `streamlit run dashboard/app.py`
