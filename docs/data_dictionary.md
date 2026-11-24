# Từ điển dữ liệu

Sinh tự động từ `src/stocklake/contracts/schemas.py` bằng `make docs`. Không sửa tay.

## topic.intraday_prices

Nến 1 phút realtime; key = symbol, 6 partition

- Khoá chính: `symbol`, `bar_ts`, `source`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `bar_ts` | long | không | Epoch ms, thời điểm mở nến |
| `open` | double | không |  |
| `high` | double | không |  |
| `low` | double | không |  |
| `close` | double | không |  |
| `volume` | long | không |  |
| `source` | string | không | vci, vndirect, kbs, replay |
| `produced_ts` | long | không | Epoch ms, lúc producer gửi |
| `available_at` | long | không | Epoch ms |
| `replay_run_id` | string | có | Chỉ có khi phát lại |

## topic.market_news

Tin tức RSS; key = news_id, 1 partition

- Khoá chính: `news_id`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `news_id` | string | không | SHA-1 của link |
| `feed` | string | không | cafef, vietstock, vnexpress |
| `title` | string | không |  |
| `link` | string | không |  |
| `summary` | string | có |  |
| `published_ts` | long | không | Epoch ms |
| `produced_ts` | long | không | Epoch ms |
| `available_at` | long | không | Epoch ms |

## raw.daily_ohlcv

Nến ngày theo đơn vị gốc của nguồn

- Khoá chính: `symbol`, `trade_date`, `source`
- Phân vùng: `source`, `year` (suy ra)

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `trade_date` | date | không |  |
| `open` | double | không |  |
| `high` | double | không |  |
| `low` | double | không |  |
| `close` | double | không |  |
| `volume` | long | không |  |
| `raw_close` | double | có | Null nếu nguồn không có |
| `price_unit` | string | không | Đơn vị giá gốc của nguồn |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |

## raw.foreign_flow

Giao dịch khối ngoại theo ngày

- Khoá chính: `symbol`, `trade_date`, `source`
- Phân vùng: `source`, `year` (suy ra)

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `trade_date` | date | không |  |
| `buy_vol` | long | không |  |
| `sell_vol` | long | không |  |
| `net_vol` | long | không |  |
| `buy_val` | double | không |  |
| `sell_val` | double | không |  |
| `net_val` | double | không |  |
| `room_left` | double | có |  |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |

## raw.company_profile

Ảnh chụp hồ sơ doanh nghiệp, không dùng làm feature lịch sử

- Khoá chính: `symbol`, `snapshot_date`
- Phân vùng: `snapshot_date`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `snapshot_date` | date | không |  |
| `exchange` | string | có |  |
| `company_name` | string | có |  |
| `icb_code` | string | có |  |
| `listed_date` | date | có |  |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |

## raw.macro_daily

Chuỗi vĩ mô hằng ngày (yfinance, FRED)

- Khoá chính: `series_id`, `obs_date`, `source`
- Phân vùng: `source`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `series_id` | string | không |  |
| `obs_date` | date | không |  |
| `value` | double | có |  |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |

## raw.intraday_history

Lịch sử nến 1 phút tải batch bởi collector (không qua Kafka); year theo giờ VN

- Khoá chính: `symbol`, `bar_ts`, `source`
- Phân vùng: `source`, `year` (suy ra)

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `bar_ts` | timestamp | không | Giờ mở nến (UTC) |
| `open` | double | không |  |
| `high` | double | không |  |
| `low` | double | không |  |
| `close` | double | không |  |
| `volume` | long | không |  |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |
| `ingest_run_id` | string | không | Id lượt chạy collector đã ghi dòng này |

## raw.intraday_prices

Nến 1 phút từ Kafka; cột thời gian đã đổi sang timestamp

- Khoá chính: `symbol`, `bar_ts`, `source`
- Phân vùng: `trade_date`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `bar_ts` | timestamp | không |  |
| `open` | double | không |  |
| `high` | double | không |  |
| `low` | double | không |  |
| `close` | double | không |  |
| `volume` | long | không |  |
| `produced_ts` | timestamp | không |  |
| `replay_run_id` | string | có |  |
| `trade_date` | date | không | Ngày theo giờ VN |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |
| `kafka_partition` | int | không |  |
| `kafka_offset` | long | không |  |
| `kafka_ts` | timestamp | không |  |
| `spark_batch_ts` | timestamp | không |  |

## raw.market_news

Tin tức từ Kafka; source = feed

- Khoá chính: `news_id`
- Phân vùng: `ingest_date`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `news_id` | string | không |  |
| `feed` | string | không |  |
| `title` | string | không |  |
| `link` | string | không |  |
| `summary` | string | có |  |
| `published_ts` | timestamp | không |  |
| `produced_ts` | timestamp | không |  |
| `ingest_date` | date | không |  |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |
| `kafka_partition` | int | không |  |
| `kafka_offset` | long | không |  |
| `kafka_ts` | timestamp | không |  |
| `spark_batch_ts` | timestamp | không |  |

## clean.daily_ohlcv

Giá đồng, đã điều chỉnh; sắp xếp theo symbol

- Khoá chính: `symbol`, `trade_date`
- Phân vùng: `year` (suy ra)

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `trade_date` | date | không |  |
| `open` | double | không |  |
| `high` | double | không |  |
| `low` | double | không |  |
| `close` | double | không |  |
| `raw_close` | double | có |  |
| `volume` | long | không |  |
| `source` | string | không | Nguồn được chọn |
| `available_at` | timestamp | không |  |
| `dq_flags` | array<string> | không |  |

## clean.foreign_flow

Như raw, đã chuẩn hoá kiểu

- Khoá chính: `symbol`, `trade_date`
- Phân vùng: `year` (suy ra)

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `trade_date` | date | không |  |
| `buy_vol` | long | không |  |
| `sell_vol` | long | không |  |
| `net_vol` | long | không |  |
| `buy_val` | double | không |  |
| `sell_val` | double | không |  |
| `net_val` | double | không |  |
| `room_left` | double | có |  |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |

## clean.company_profile

Bản mới nhất của từng mã

- Khoá chính: `symbol`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `snapshot_date` | date | không |  |
| `exchange` | string | có |  |
| `company_name` | string | có |  |
| `icb_code` | string | có |  |
| `listed_date` | date | có |  |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |

## clean.macro_daily

Chuỗi vĩ mô đã khử trùng

- Khoá chính: `series_id`, `obs_date`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `series_id` | string | không |  |
| `obs_date` | date | không |  |
| `value` | double | có |  |
| `available_at` | timestamp | không |  |

## clean.intraday_1m

Nến 1 phút đã khử trùng; sắp xếp theo symbol

- Khoá chính: `symbol`, `bar_ts`
- Phân vùng: `year` (suy ra), `month` (suy ra)

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `bar_ts` | timestamp | không |  |
| `open` | double | không |  |
| `high` | double | không |  |
| `low` | double | không |  |
| `close` | double | không |  |
| `volume` | long | không |  |
| `trade_date` | date | không |  |
| `available_at` | timestamp | không |  |

## clean.market_news

Tin tức đã khử trùng

- Khoá chính: `news_id`
- Phân vùng: `year` (suy ra), `month` (suy ra)

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `news_id` | string | không |  |
| `feed` | string | không |  |
| `title` | string | không |  |
| `link` | string | không |  |
| `summary` | string | có |  |
| `published_ts` | timestamp | không |  |
| `produced_ts` | timestamp | không |  |
| `source` | string | không | Nguồn dữ liệu |
| `ingest_ts` | timestamp | không | Lúc ghi vào hệ thống (UTC) |
| `available_at` | timestamp | không | Lúc dữ liệu được coi là đã biết (UTC) |

## clean.trading_calendar

Lịch giao dịch suy ra từ VNINDEX

- Khoá chính: `trade_date`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `trade_date` | date | không |  |
| `is_trading_day` | boolean | không |  |

## curated.market_daily

Tổng hợp thị trường theo ngày

- Khoá chính: `trade_date`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `trade_date` | date | không |  |
| `advancers` | int | không | Số mã tăng |
| `decliners` | int | không | Số mã giảm |
| `unchanged` | int | không | Số mã đứng giá |
| `total_volume` | long | không |  |
| `total_value` | double | không |  |
| `foreign_net_val` | double | có | Khối ngoại ròng toàn sàn |
| `vnindex_close` | double | có |  |

## curated.symbol_daily_features

Bảng feature cho mô hình

- Khoá chính: `symbol`, `trade_date`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `symbol` | string | không |  |
| `trade_date` | date | không |  |
| `ret_1d` | double | có |  |
| `ret_5d` | double | có |  |
| `ret_20d` | double | có |  |
| `vol_20d` | double | có | Độ biến động 20 ngày |
| `volume_ratio_20d` | double | có | KL / TB 20 ngày |
| `foreign_net_5d` | double | có | Khối ngoại ròng 5 ngày |
| `macro_vol_20d` | double | có | Biến động vĩ mô |
| `feature_available_at` | timestamp | không | Max available_at của đầu vào |
| `label_up_t5` | int | có | 1 nếu giá sau 5 phiên tăng |

## curated.dq_report

Kết quả kiểm tra chất lượng dữ liệu

- Khoá chính: `run_id`, `table_name`, `check_name`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `run_id` | string | không |  |
| `table_name` | string | không |  |
| `check_name` | string | không |  |
| `metric_value` | double | có |  |
| `threshold` | double | có |  |
| `passed` | boolean | không |  |
| `run_ts` | timestamp | không |  |

## curated.ml_results

Kết quả mô hình baseline và leakage test

- Khoá chính: `run_id`, `scenario`, `model`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `run_id` | string | không |  |
| `scenario` | string | không |  |
| `model` | string | không |  |
| `accuracy` | double | có |  |
| `f1_macro` | double | có |  |
| `auc` | double | có |  |
| `train_start` | date | không |  |
| `train_end` | date | không |  |
| `test_start` | date | không |  |
| `test_end` | date | không |  |

## curated.benchmark_results

Số đo benchmark

- Khoá chính: `suite`, `run_id`, `case`, `metric`

| Cột | Kiểu | Null | Mô tả |
|---|---|---|---|
| `suite` | string | không |  |
| `run_id` | string | không |  |
| `case` | string | không |  |
| `metric` | string | không |  |
| `value` | double | không |  |
| `unit` | string | có |  |
| `config` | string | có | Cấu hình đo, dạng JSON |
| `run_ts` | timestamp | không |  |

