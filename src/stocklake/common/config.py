"""Cấu hình đọc từ biến môi trường; mặc định dùng trong mạng compose."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any


def _env(name: str, default: str):
    return field(default_factory=lambda: os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP", "kafka:9092")
    hdfs_uri: str = _env("HDFS_URI", "hdfs://namenode:8020")
    webhdfs_host: str = _env("WEBHDFS_HOST", "namenode")
    webhdfs_port: int = field(default_factory=lambda: int(os.environ.get("WEBHDFS_PORT", "9870")))
    spark_master: str = _env("SPARK_MASTER", "spark://spark-master:7077")
    app_conf_dir: str = _env("APP_CONF_DIR", "/opt/stocklake/conf/app")
    hdfs_user: str = _env("HDFS_USER", "stocklake")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def load_app_conf(name: str, settings: Settings | None = None) -> dict[str, Any]:
    """Đọc <APP_CONF_DIR>/<name>.yaml."""
    import yaml

    s = settings or get_settings()
    path = Path(s.app_conf_dir) / f"{name}.yaml"
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
