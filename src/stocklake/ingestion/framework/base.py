"""Lớp cơ sở cho collector batch."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from stocklake.common.http import SourceBlockedError
from stocklake.common.logging import get_logger
from stocklake.ingestion.framework.fallback import AllSourcesFailedError, fetch_with_fallback
from stocklake.ingestion.framework.state import StateStore
from stocklake.sources.base import SourceClient

log = get_logger(__name__)


@dataclass(frozen=True)
class WorkUnit:
    """Một phần việc độc lập (vd một mã, một khoảng ngày); `key` duy nhất trong collector."""

    key: str
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class RunResult:
    done: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    stopped_blocked: bool = False  # dừng vì mọi nguồn còn lại bị chặn


class BaseCollector(ABC):
    name: str = ""

    def __init__(self, sources: Sequence[SourceClient], state: StateStore):
        self.sources = list(sources)  # theo thứ tự ưu tiên
        self.state = state

    @abstractmethod
    def plan(self) -> Iterable[WorkUnit]: ...

    @abstractmethod
    def fetch(self, unit: WorkUnit, source: SourceClient) -> Any:
        """Lấy bản ghi của `unit` từ `source`; ném lỗi nếu không được."""

    @abstractmethod
    def write(self, unit: WorkUnit, records: Any, source_name: str) -> None:
        """Ghi bản ghi (idempotent: chạy lại ghi đè đúng phần của unit)."""

    def run(self) -> RunResult:
        res = RunResult()
        blocked: set[str] = set()  # nguồn bị chặn trong lượt chạy này, không gọi lại
        for unit in self.plan():
            if self.state.is_done(self.name, unit.key):
                res.skipped.append(unit.key)
                continue
            active = [s for s in self.sources if s.name not in blocked]
            if not active:
                res.stopped_blocked = True
                break
            try:
                records, used = fetch_with_fallback(
                    active,
                    lambda s, u=unit: self.fetch(u, s),
                    on_error=lambda n, e: (
                        blocked.add(n) if isinstance(e, SourceBlockedError) else None
                    ),
                )
                self.write(unit, records, used)
            except AllSourcesFailedError as exc:
                self.state.mark_failed(self.name, unit.key, str(exc))
                res.failed.append(unit.key)
                if all(s.name in blocked for s in self.sources):
                    res.stopped_blocked = True
                    log.error("[%s] mọi nguồn bị chặn, dừng lượt chạy", self.name)
                    break
                continue
            except Exception as exc:  # noqa: BLE001 - lỗi ghi: đánh dấu rồi làm tiếp unit khác
                log.error("[%s] ghi %s lỗi: %s", self.name, unit.key, exc)
                self.state.mark_failed(self.name, unit.key, f"write: {exc}")
                res.failed.append(unit.key)
                continue
            meta: dict[str, Any] = {"source": used}
            if hasattr(records, "__len__"):
                meta["records"] = len(records)
            self.state.mark_done(self.name, unit.key, meta)
            res.done.append(unit.key)
        return res
