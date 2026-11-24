"""Khung collector: lớp cơ sở, nguồn dự phòng, trạng thái idempotent."""

from stocklake.ingestion.framework.base import BaseCollector, RunResult, WorkUnit
from stocklake.ingestion.framework.fallback import AllSourcesFailedError, fetch_with_fallback
from stocklake.ingestion.framework.state import JsonFileStateStore, StateStore

__all__ = [
    "AllSourcesFailedError",
    "BaseCollector",
    "JsonFileStateStore",
    "RunResult",
    "StateStore",
    "WorkUnit",
    "fetch_with_fallback",
]
