"""Producer tin tức RSS (Lát 2).

Đọc 3 feed (cafef, vietstock, vnexpress) qua stocklake.sources, tính news_id
= SHA-1 của link, available_at = pubDate, gửi vào topic market_news.
"""

from __future__ import annotations

from stocklake.common.config import Settings


def run_rss_producer(poll_seconds: float = 300.0, settings: Settings | None = None) -> None:
    """Vòng lặp đọc RSS và gửi Kafka cho tới khi bị dừng."""
    raise NotImplementedError("Lát 2")
