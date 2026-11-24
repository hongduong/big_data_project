from datetime import datetime, timezone

import pytest
import requests

from stocklake.common.http import (
    HttpClient,
    HttpError,
    RateLimiter,
    RetryExhaustedError,
    RetryPolicy,
    SourceBlockedError,
)


class FakeResponse:
    def __init__(self, status=200, payload=None, text="", bad_json=False, headers=None):
        if headers is not None:
            self.headers = headers
        self.status_code = status
        self._payload = payload
        self.text = text
        self._bad = bad_json

    def json(self):
        if self._bad:
            raise ValueError("bad json")
        return self._payload


class FakeSession:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def request(self, method, url, **kw):
        self.calls.append((method, url, kw))
        out = self.outcomes.pop(0)
        if isinstance(out, Exception):
            raise out
        return out


class FakeTime:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, s):
        self.sleeps.append(s)
        self.now += s


def make(session, **kw):
    t = FakeTime()
    retry = kw.pop("retry", RetryPolicy(jitter=0.0))
    client = HttpClient(
        "test", max_per_second=1000, retry=retry, session=session,
        sleep=t.sleep, clock=t.clock, **kw,
    )  # fmt: skip
    return client, t


def test_success_and_headers():
    s = FakeSession(FakeResponse(payload={"a": 1}))
    c, _ = make(s, default_headers={"User-Agent": "x"})
    assert c.get_json("http://u", params={"q": 1}, headers={"H": "1"}) == {"a": 1}
    method, url, kw = s.calls[0]
    assert (method, url) == ("GET", "http://u")
    assert kw["headers"] == {"User-Agent": "x", "H": "1"}
    assert kw["params"] == {"q": 1} and kw["timeout"] == 30.0


def test_post_json_and_text():
    s = FakeSession(FakeResponse(payload=[1]), FakeResponse(text="hi"))
    c, _ = make(s)
    assert c.post_json("http://u", json={"k": 2}) == [1]
    assert s.calls[0][2]["json"] == {"k": 2}
    assert c.get_text("http://u") == "hi"


def test_retry_then_success_with_backoff():
    s = FakeSession(
        FakeResponse(500), requests.ConnectionError("x"), requests.Timeout("t"),
        FakeResponse(payload={"ok": True}),
    )  # fmt: skip
    c, t = make(s)
    assert c.get_json("http://u") == {"ok": True}
    assert len(s.calls) == 4
    backoff = [x for x in t.sleeps if x >= 1.0 - 1e-9]
    assert backoff == [1.0, 2.0, 4.0]


def test_backoff_capped():
    p = RetryPolicy(base_delay=1.0, max_delay=5.0, jitter=0.0)
    assert [p.delay(i) for i in (1, 2, 3, 4, 5)] == [1.0, 2.0, 4.0, 5.0, 5.0]
    p = RetryPolicy(jitter=0.2)
    assert p.delay(1, rand=lambda: 0.0) == pytest.approx(0.8)
    assert p.delay(1, rand=lambda: 1.0) == pytest.approx(1.2)


@pytest.mark.parametrize("status", [429, 403])
def test_blocked_no_retry(status):
    s = FakeSession(FakeResponse(status), FakeResponse(200))
    c, _ = make(s)
    with pytest.raises(SourceBlockedError) as ei:
        c.get_json("http://u")
    assert ei.value.status == status
    assert len(s.calls) == 1


def test_other_4xx_no_retry():
    s = FakeSession(FakeResponse(404), FakeResponse(200))
    c, _ = make(s)
    with pytest.raises(HttpError) as ei:
        c.get_text("http://u")
    assert not isinstance(ei.value, (SourceBlockedError, RetryExhaustedError))
    assert ei.value.status == 404 and len(s.calls) == 1


def test_retry_exhausted():
    s = FakeSession(*[FakeResponse(503) for _ in range(3)])
    c, t = make(s, retry=RetryPolicy(max_attempts=3, jitter=0.0))
    with pytest.raises(RetryExhaustedError):
        c.get_text("http://u")
    assert len(s.calls) == 3
    assert t.sleeps.count(1.0) == 1 and t.sleeps.count(2.0) == 1  # không chờ sau lần cuối


def test_invalid_json():
    s = FakeSession(FakeResponse(bad_json=True))
    c, _ = make(s)
    with pytest.raises(HttpError):
        c.get_json("http://u")


def test_rate_limiter_spacing():
    t = FakeTime()
    rl = RateLimiter(2.0, clock=t.clock, sleep=t.sleep)
    stamps = []
    for _ in range(4):
        rl.acquire()
        stamps.append(t.now)
    assert stamps == [0.0, 0.5, 1.0, 1.5]
    t.now += 10
    rl.acquire()  # đã trôi qua lâu thì không chờ
    assert t.sleeps == [0.5, 0.5, 0.5]
    with pytest.raises(ValueError):
        RateLimiter(0)


def test_every_attempt_rate_limited():
    s = FakeSession(FakeResponse(500), FakeResponse(payload=1))
    t = FakeTime()
    c = HttpClient(
        "t", max_per_second=1.0, retry=RetryPolicy(base_delay=0.1, jitter=0.0),
        session=s, sleep=t.sleep, clock=t.clock,
    )  # fmt: skip
    c.get_json("http://u")
    # lần 2 phải cách lần 1 ít nhất 1s dù backoff chỉ 0.1s
    assert t.now >= 1.0


NOW = datetime(2026, 11, 24, 10, 0, 0, tzinfo=timezone.utc)


def blocked(resp):
    c, _ = make(FakeSession(resp), now=lambda: NOW)
    with pytest.raises(SourceBlockedError) as ei:
        c.get_json("http://u")
    return ei.value


def test_retry_after_seconds():
    e = blocked(FakeResponse(429, headers={"retry-after": "120"}))  # khác hoa thường
    assert e.retry_after_raw == "120" and e.retry_after_seconds == 120.0
    assert "Retry-After" in str(e) and e.status == 429 and e.url == "http://u"
    assert blocked(FakeResponse(429, headers={"Retry-After": "1.5"})).retry_after_seconds == 1.5


def test_retry_after_http_date():
    e = blocked(FakeResponse(429, headers={"Retry-After": "Tue, 24 Nov 2026 10:05:00 GMT"}))
    assert e.retry_after_seconds == 300.0
    assert e.retry_after_raw == "Tue, 24 Nov 2026 10:05:00 GMT"
    past = blocked(FakeResponse(429, headers={"Retry-After": "Tue, 24 Nov 2026 09:00:00 GMT"}))
    assert past.retry_after_seconds == 0.0


def test_retry_after_absent_or_garbage():
    for resp in (FakeResponse(429), FakeResponse(429, headers={})):
        e = blocked(resp)
        assert e.retry_after_raw is None and e.retry_after_seconds is None
    e = blocked(FakeResponse(429, headers={"Retry-After": "soon-ish"}))
    assert e.retry_after_raw == "soon-ish" and e.retry_after_seconds is None
    assert blocked(FakeResponse(429, headers={"Retry-After": "-5"})).retry_after_seconds == 0.0


def test_retry_after_on_403():
    e = blocked(FakeResponse(403, headers={"Retry-After": "30"}))
    assert e.status == 403 and e.retry_after_seconds == 30.0


def test_blocked_error_compat():
    e = SourceBlockedError("m", 429, "http://u")
    assert e.retry_after_raw is None and e.retry_after_seconds is None
