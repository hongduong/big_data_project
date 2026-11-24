"""Trạng thái chạy idempotent: đơn vị đã xong thì lần sau bỏ qua."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol


class StateStore(Protocol):
    def is_done(self, collector: str, key: str) -> bool: ...

    def mark_done(
        self, collector: str, key: str, meta: Mapping[str, Any] | None = None
    ) -> None: ...

    def mark_failed(
        self, collector: str, key: str, error: str, meta: Mapping[str, Any] | None = None
    ) -> None: ...


class JsonFileStateStore:
    """Lưu trạng thái ra một file JSON (volume `collector_state`), ghi nguyên tử."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, dict[str, Any]]] = {}
        if self.path.exists():
            with self.path.open(encoding="utf-8") as f:
                self._data = json.load(f)

    def is_done(self, collector: str, key: str) -> bool:
        return self._data.get(collector, {}).get(key, {}).get("status") == "done"

    def get(self, collector: str, key: str) -> dict[str, Any] | None:
        return self._data.get(collector, {}).get(key)

    def mark_done(self, collector: str, key: str, meta: Mapping[str, Any] | None = None) -> None:
        self._set(collector, key, {"status": "done", "meta": dict(meta or {})})

    def mark_failed(
        self, collector: str, key: str, error: str, meta: Mapping[str, Any] | None = None
    ) -> None:
        self._set(collector, key, {"status": "failed", "error": error, "meta": dict(meta or {})})

    def _set(self, collector: str, key: str, entry: dict[str, Any]) -> None:
        entry["updated_at"] = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._data.setdefault(collector, {})[key] = entry
            self._flush()

    def _flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=self.path.name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=1, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
