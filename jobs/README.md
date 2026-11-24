# jobs/

Điểm vào mỏng cho mọi job. Logic nằm trong `src/stocklake/`.

| Thư mục | Chủ | Module tương ứng |
|---|---|---|
| `ingest/` | Hồng Dương | `stocklake.ingestion` |
| `streaming/` | Tuyền | `stocklake.streaming` |
| `etl/` | Hùng Dương | `stocklake.etl` |
| `dq/` | Hồng Dương | `stocklake.dq` |
| `analytics/` | Thanh | `stocklake.analytics` |

## Quy tắc điểm vào mỏng

Mỗi file chỉ đọc tham số (argparse) rồi gọi đúng một hàm trong `stocklake`. Không đặt logic ở đây.

## Cách chạy

```bash
# Job Spark
docker compose exec spark-driver spark-submit /opt/stocklake/jobs/<thư_mục>/<file>.py

# Job collector / producer (không cần Spark)
docker compose exec collector python /opt/stocklake/jobs/<thư_mục>/<file>.py
```
