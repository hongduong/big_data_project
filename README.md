# StockLake — đồ án Công nghệ dữ liệu lớn

Hệ thống thu thập, lưu trữ và phân tích dữ liệu chứng khoán Việt Nam trên
Kafka (KRaft) + Spark 3.5 + HDFS. Package Python: `stocklake`.

## Cấu trúc

Cây thư mục, vùng sở hữu, data contracts và lộ trình nằm trong
[docs/Planning.md](docs/Planning.md) (mục 3.1, 4, 6).

## Chạy unit test trên máy bất kỳ

```bash
python3 -m venv .venv
.venv/bin/pip install -e . pytest ruff
.venv/bin/pytest tests/unit -m "not spark"
.venv/bin/ruff check . && .venv/bin/ruff format --check .
```

Test đánh dấu `spark` cần Java và `pyspark` (`pip install -e ".[dev]"`),
chạy trên PC chung. Test đánh dấu `integration` cần cụm đang chạy.

## Cụm

Kafka, HDFS, Spark chỉ chạy trên PC chung (docker compose, truy cập qua SSH).
Xem `Makefile` và `docker-compose.yml`.
