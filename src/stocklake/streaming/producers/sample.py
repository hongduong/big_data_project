"""Producer mẫu cho smoke test: gửi vài nến 1 phút vào topic intraday_prices."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from stocklake.common.config import Settings
from stocklake.contracts.topics import INTRADAY_PRICES
from stocklake.streaming.producers.base import make_producer, serialize

_MINUTE_MS = 60_000


def build_sample_intraday(symbol: str = "FPT", now: datetime | None = None) -> dict[str, Any]:
    """Tạo một bản tin intraday_prices hợp lệ (source 'replay')."""
    now = now or datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    bar_ts = now_ms - now_ms % _MINUTE_MS
    return {
        "symbol": symbol,
        "bar_ts": bar_ts,
        "open": 100000.0,
        "high": 100500.0,
        "low": 99500.0,
        "close": 100200.0,
        "volume": 1000,
        "source": "replay",
        "produced_ts": now_ms,
        "available_at": bar_ts + _MINUTE_MS,
        "replay_run_id": None,
    }


def send_sample(n: int = 1, settings: Settings | None = None) -> int:
    """Gửi n bản tin mẫu, flush, trả số bản tin gửi thành công.

    Ném RuntimeError nếu có lỗi giao nhận hoặc flush chưa xong.
    """
    producer = make_producer(settings)
    delivered = 0
    errors: list[str] = []

    def on_delivery(err, _msg) -> None:
        nonlocal delivered
        if err is not None:
            errors.append(str(err))
        else:
            delivered += 1

    now = datetime.now(timezone.utc)
    for i in range(n):
        record = build_sample_intraday(now=now)
        # Mỗi bản tin một nến khác nhau để khoá khử trùng không trùng.
        record["bar_ts"] -= i * _MINUTE_MS
        record["available_at"] = record["bar_ts"] + _MINUTE_MS
        key, value = serialize(record, INTRADAY_PRICES)
        producer.produce(INTRADAY_PRICES.name, key=key, value=value, on_delivery=on_delivery)
    remaining = producer.flush(30)
    if errors:
        raise RuntimeError(f"Lỗi gửi Kafka: {errors[0]} (tổng {len(errors)})")
    if remaining:
        raise RuntimeError(f"Còn {remaining} bản tin chưa gửi xong")
    return delivered
