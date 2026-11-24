# dashboard/

Chủ: Thanh. Ứng dụng Streamlit (cổng 8501), đọc Parquet trên HDFS qua WebHDFS (`fsspec` + `pyarrow`).

- Chỉ import `stocklake.contracts` và `stocklake.common`; không import Spark.
- `app.py`: trang chính (cấu hình kết nối, trạng thái dữ liệu clean, nút "Kiểm tra HDFS", schema curated).
- `data.py`: loader có cache 60 giây (`load_daily`, `load_intraday`, `list_symbols`, `table_status`) và các hàm thuần (bộ lọc pyarrow, giờ Việt Nam, lợi suất, MA20). Các trang import bằng `import data` (Streamlit thêm thư mục của `app.py` vào `sys.path`).
- `pages/1_Bieu_do_gia.py`: ba tab: đường đóng cửa nhiều mã; nến + khối lượng (+ MA20) cho một mã; nến phút 1 ngày. Mỗi tab có dòng tóm tắt số dòng/khoảng ngày và bảng 10 dòng cuối để đối chiếu dữ liệu thật.

## Nguồn dữ liệu

Lát 1: đọc tạm lớp `clean` (`/data/clean/daily_ohlcv`, `/data/clean/intraday_1m`) bằng `stocklake.common.hdfs.read_parquet_dir`. Từ Lát 2 chuyển sang `curated`. Giá là giá đã điều chỉnh; `bar_ts` lưu UTC và được đổi sang giờ Việt Nam (UTC+7) khi hiển thị. Thiếu dữ liệu thì trang hiện thông báo, không báo lỗi.

Biểu đồ nến dùng `altair` (đi kèm streamlit); nếu không import được thì dùng `st.line_chart` OHLC.

## Chạy

- Trong compose: mở `http://<IP Tailscale của PC>:8501` (cổng bind theo `HOST_BIND`).
- Cục bộ: `streamlit run dashboard/app.py`
