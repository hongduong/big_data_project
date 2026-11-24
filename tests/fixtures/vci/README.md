# Fixtures VCI

Chụp ngày 24/11/2026 bằng `requests`, tổng 7 request (cách nhau >= 0,7 s), chỉ FPT, VNINDEX.
Endpoint: POST `https://trading.vietcap.com.vn/api/chart/OHLCChart/gap-chart`
và GET `https://trading.vietcap.com.vn/api/price/symbols/getAll`.

| File | Request |
|---|---|
| `fpt_daily.json` | `ONE_DAY`, FPT, `to` = now, `countBack` 10 (t là chuỗi epoch, tăng dần) |
| `vnindex_daily.json` | `ONE_DAY`, VNINDEX, `to` = now, `countBack` 5 |
| `fpt_1m_page1.json` | `ONE_MINUTE`, FPT, `to` = now, `countBack` 20 |
| `fpt_1m_page2.json` | `ONE_MINUTE`, FPT, `to` = (t nhỏ nhất của page1) - 1, `countBack` 20 |
| `empty.json` | `ONE_DAY`, FPT, `to` = 1000000 (1970) -> `[]` |
| `get_all_sample.json` | `getAll` rút gọn còn 13 mục: HSX, HNX, UPCOM, DELISTED (cổ phiếu) và vài loại khác (BOND, FU, ETF, UNIT_TRUST) |

Ngoài ra 1 request thử `countBack` = 6000 cho FPT ONE_DAY (không lưu).
Mỗi object trong response còn các khóa `accumulatedVolume`, `accumulatedValue`, `minBatchTruncTime` (parser bỏ qua).

`group_vn30.json`: GET `https://trading.vietcap.com.vn/api/price/symbols/getByGroup?group=VN30`, chụp 24/11/2026 (30 mã, cả 30 là cổ phiếu HSX theo getAll), nguyên văn.
