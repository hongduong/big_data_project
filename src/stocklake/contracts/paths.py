"""Đường dẫn HDFS. Gốc truyền qua tham số, không đọc env."""

from __future__ import annotations

DATA_BASE = "/data"
CHECKPOINT_BASE = "/checkpoints"
DATA_LAYERS = ("raw", "clean", "curated")


def layer_root(layer: str, base: str = DATA_BASE) -> str:
    """Vd layer_root('raw') -> '/data/raw'."""
    if layer not in DATA_LAYERS:
        raise ValueError(f"Layer không hợp lệ: {layer!r}")
    return f"{base.rstrip('/')}/{layer}"


def table_path(layer: str, table: str, base: str = DATA_BASE) -> str:
    """Vd table_path('raw', 'daily_ohlcv') -> '/data/raw/daily_ohlcv'."""
    return f"{layer_root(layer, base)}/{table}"


def checkpoint_path(job_name: str, base: str = CHECKPOINT_BASE) -> str:
    """Vd checkpoint_path('kafka_to_raw') -> '/checkpoints/kafka_to_raw'."""
    return f"{base.rstrip('/')}/{job_name}"
