"""Thử lần lượt các nguồn theo thứ tự ưu tiên."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TypeVar

from stocklake.common.http import SourceBlockedError
from stocklake.common.logging import get_logger
from stocklake.sources.base import SourceClient

log = get_logger(__name__)
T = TypeVar("T")


class AllSourcesFailedError(Exception):
    """Mọi nguồn đều lỗi; `errors` giữ (tên nguồn, lỗi) theo thứ tự thử."""

    def __init__(self, errors: list[tuple[str, Exception]]):
        self.errors = errors
        detail = "; ".join(f"{n}: {type(e).__name__}: {e}" for n, e in errors) or "không có nguồn"
        super().__init__(f"Mọi nguồn đều lỗi ({detail})")

    @property
    def blocked_sources(self) -> list[str]:
        return [n for n, e in self.errors if isinstance(e, SourceBlockedError)]


def fetch_with_fallback(
    sources: Sequence[SourceClient],
    call: Callable[[SourceClient], T],
    on_error: Callable[[str, Exception], None] | None = None,
) -> tuple[T, str]:
    """Gọi `call(source)` lần lượt; trả (kết quả, tên nguồn thành công).

    Nguồn bị chặn (429/403) cũng chuyển sang nguồn kế, nhưng được ghi lại trong lỗi.
    `on_error(tên, lỗi)` được gọi cho mỗi nguồn lỗi, kể cả khi nguồn sau đó thành công.
    """
    errors: list[tuple[str, Exception]] = []
    for src in sources:
        try:
            return call(src), src.name
        except Exception as exc:  # noqa: BLE001 - mọi lỗi nguồn đều cho phép chuyển nguồn
            level = log.error if isinstance(exc, SourceBlockedError) else log.warning
            level("nguồn %s lỗi: %s: %s", src.name, type(exc).__name__, exc)
            errors.append((src.name, exc))
            if on_error:
                on_error(src.name, exc)
    raise AllSourcesFailedError(errors)
