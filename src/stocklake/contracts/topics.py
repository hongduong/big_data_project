"""Khai báo Kafka topic."""

from __future__ import annotations

from dataclasses import dataclass

from stocklake.contracts.schemas import INTRADAY_PRICES_TOPIC, MARKET_NEWS_TOPIC, TableSchema


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    key_field: str
    schema: TableSchema
    dedup_key: tuple[str, ...]


INTRADAY_PRICES = TopicSpec(
    name="intraday_prices",
    partitions=6,
    key_field="symbol",
    schema=INTRADAY_PRICES_TOPIC,
    dedup_key=("symbol", "bar_ts", "source"),
)

MARKET_NEWS = TopicSpec(
    name="market_news",
    partitions=1,
    key_field="news_id",
    schema=MARKET_NEWS_TOPIC,
    dedup_key=("news_id",),
)

ALL_TOPICS: tuple[TopicSpec, ...] = (INTRADAY_PRICES, MARKET_NEWS)
