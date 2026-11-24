# Fixtures VNDirect

Chụp ngày 24/11/2026 bằng `requests`, tổng 5 request (cách nhau 0,8 s), chỉ FPT và VNINDEX.
Endpoint: GET `https://api-finfo.vndirect.com.vn/v4/stock_prices`, `sort=date:asc`,
header trình duyệt (User-Agent, Referer/Origin `https://dstock.vndirect.com.vn`).

| File | Tham số `q` | size, page |
|---|---|---|
| `fpt_daily_nov2026.json` | `code:FPT~date:gte:2026-11-10~date:lte:2026-11-23` | 100, 1 (10 dòng, đủ totalPages) |
| `fpt_2013_page1.json` | `code:FPT~date:gte:2013-01-01~date:lte:2013-01-31` | 5, 1 (totalElements 22, totalPages 5) |
| `fpt_2013_page2.json` | như trên | 5, 2 (chỉ có `currentPage`, `size`, không có tổng) |
| `vnindex_daily.json` | `code:VNINDEX~date:gte:2026-11-17~date:lte:2026-11-23` | 5, 1 -> data rỗng (không hỗ trợ chỉ số) |
| `empty.json` | `code:FPT~date:gte:2000-01-01~date:lte:2000-12-31` | 5, 1 -> data rỗng |

Đối chiếu với `tests/fixtures/vci/fpt_daily.json` (10 phiên 10-23/11/2026): giá điều chỉnh khớp,
volume VCI == `nmVolume` (không cộng `ptVolume`).
