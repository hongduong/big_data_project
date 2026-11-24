"""HTTP client dùng chung: giới hạn tốc độ, retry/backoff, phân loại lỗi."""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from stocklake.common.logging import get_logger

log = get_logger(__name__)

BLOCK_STATUSES = (403, 429)


class HttpError(Exception):
    def __init__(self, message: str, status: int | None = None, url: str | None = None):
        super().__init__(message)
        self.status = status
        self.url = url


def parse_retry_after(raw: str | None, now: Callable[[], datetime] | None = None) -> float | None:
    """Đổi giá trị `Retry-After` (số giây hoặc HTTP-date) sang số giây, không âm."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        secs = float(text)
    except ValueError:
        try:
            when = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        current = now() if now is not None else datetime.now(timezone.utc)
        secs = (when - current).total_seconds()
    if secs != secs or secs in (float("inf"), float("-inf")):
        return None
    return max(0.0, secs)


def _get_header(resp: Any, name: str) -> str | None:
    """Đọc header không phân biệt hoa thường; response không có headers thì trả None."""
    headers = getattr(resp, "headers", None)
    if not headers:
        return None
    try:
        for key, value in headers.items():
            if str(key).lower() == name.lower():
                return None if value is None else str(value)
    except AttributeError:
        return None
    return None


class SourceBlockedError(HttpError):
    """Nguồn chặn (429/403): dừng, không retry, chờ rồi chạy lại sau.

    `retry_after_raw`: header Retry-After như nhận được; `retry_after_seconds`: đã đổi sang giây.
    """

    def __init__(
        self,
        message: str,
        status: int | None = None,
        url: str | None = None,
        retry_after_raw: str | None = None,
        retry_after_seconds: float | None = None,
    ):
        super().__init__(message, status, url)
        self.retry_after_raw = retry_after_raw
        self.retry_after_seconds = retry_after_seconds


class RetryExhaustedError(HttpError):
    """Hết số lần thử."""


class RateLimiter:
    """Giới hạn khoảng cách tối thiểu giữa các lần gọi; an toàn đa luồng."""

    def __init__(
        self,
        max_per_second: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if max_per_second <= 0:
            raise ValueError("max_per_second phải > 0")
        self.interval = 1.0 / max_per_second
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            start = max(now, self._next_allowed)
            self._next_allowed = start + self.interval
            wait = start - now
        if wait > 0:
            self._sleep(wait)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 4
    base_delay: float = 1.0
    max_delay: float = 30.0
    jitter: float = 0.2

    def delay(self, attempt: int, rand: Callable[[], float] = random.random) -> float:
        """Thời gian chờ sau lần thử thứ `attempt` (bắt đầu từ 1)."""
        d = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        return d * (1 + self.jitter * (2 * rand() - 1))


class HttpClient:
    def __init__(
        self,
        name: str,
        max_per_second: float = 2.0,
        retry: RetryPolicy = RetryPolicy(),  # noqa: B008 - dataclass frozen, an toàn
        default_headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        session: Any = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
    ):
        self.name = name
        self._now = now
        self.retry = retry
        self.default_headers = dict(default_headers or {})
        self.timeout = timeout
        self.session = session if session is not None else requests.Session()
        self._sleep = sleep
        self.limiter = RateLimiter(max_per_second, clock=clock, sleep=sleep)

    def request(self, method: str, url: str, **kw: Any):
        headers = {**self.default_headers, **(kw.pop("headers", None) or {})}
        kw.setdefault("timeout", self.timeout)
        last: str = ""
        for attempt in range(1, self.retry.max_attempts + 1):
            self.limiter.acquire()
            try:
                resp = self.session.request(method, url, headers=headers, **kw)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last = f"{type(exc).__name__}: {exc}"
            else:
                status = resp.status_code
                if status in BLOCK_STATUSES:
                    raw = _get_header(resp, "Retry-After")
                    secs = parse_retry_after(raw, self._now)
                    extra = ""
                    if raw is not None:
                        parsed = "không đọc được" if secs is None else f"~{secs:.0f}s"
                        extra = f", Retry-After={raw!r} ({parsed})"
                    log.error(
                        "[%s] %s %s bị chặn (HTTP %s%s)", self.name, method, url, status, extra
                    )
                    raise SourceBlockedError(
                        f"Nguồn {self.name} chặn: HTTP {status}{extra}", status, url, raw, secs
                    )
                if status >= 500:
                    last = f"HTTP {status}"
                elif status >= 400:
                    raise HttpError(f"HTTP {status} từ {url}", status, url)
                else:
                    return resp
            log.warning("[%s] lần %d/%d lỗi: %s", self.name, attempt, self.retry.max_attempts, last)
            if attempt < self.retry.max_attempts:
                self._sleep(self.retry.delay(attempt))
        raise RetryExhaustedError(
            f"Hết {self.retry.max_attempts} lần thử {method} {url}: {last}", url=url
        )

    def get_json(self, url: str, params: dict | None = None, headers: dict | None = None):
        resp = self.request("GET", url, params=params, headers=headers)
        return self._parse_json(resp, url)

    def post_json(self, url: str, json: Any = None, headers: dict | None = None):
        resp = self.request("POST", url, json=json, headers=headers)
        return self._parse_json(resp, url)

    def get_text(self, url: str, params: dict | None = None, headers: dict | None = None) -> str:
        return self.request("GET", url, params=params, headers=headers).text

    @staticmethod
    def _parse_json(resp: Any, url: str):
        try:
            return resp.json()
        except ValueError as exc:
            raise HttpError(f"JSON không hợp lệ từ {url}", resp.status_code, url) from exc
