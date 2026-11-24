"""Tiện ích chung cho producer: tuần tự hoá bản tin và tạo Producer."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from stocklake.common.config import Settings, get_settings
from stocklake.contracts.topics import TopicSpec


def serialize(record: Mapping[str, Any], topic: TopicSpec) -> tuple[bytes, bytes]:
    """Trả (key, value) dạng bytes. Value là JSON UTF-8 gọn.

    Kiểm theo schema trung lập của topic: thiếu/null ở trường bắt buộc hoặc
    có trường lạ thì ném ValueError.
    """
    columns = {c.name: c for c in topic.schema.columns}
    unknown = sorted(set(record) - set(columns))
    if unknown:
        raise ValueError(f"{topic.name}: trường lạ {unknown}")
    missing = sorted(
        name for name, c in columns.items() if not c.nullable and record.get(name) is None
    )
    if missing:
        raise ValueError(f"{topic.name}: thiếu trường bắt buộc {missing}")
    key = str(record[topic.key_field]).encode("utf-8")
    value = json.dumps(dict(record), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return key, value


def make_producer(settings: Settings | None = None):
    """Tạo confluent_kafka.Producer (idempotent, acks=all)."""
    from confluent_kafka import Producer

    s = settings or get_settings()
    return Producer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
