"""Producer nến 1 phút realtime từ VCI vào topic intraday_prices.

Mỗi lần poll: lấy vài chục nến gần nhất của từng mã, chỉ gửi nến ĐÃ ĐÓNG
(bar_ts + 60 s <= now) và mới hơn nến cuối đã gửi (state JSON trên volume
collector_state, ghi nguyên tử). source = 'vci', replay_run_id = None.
State của một mã chỉ tiến khi mọi bản tin của mã đó đã giao thành công.

Giờ chạy: thứ 2–6, 09:00–11:30 và 13:00–15:00 giờ VN. Chưa có lịch nghỉ lễ.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
from pathlib import Path
from typing import Any

from stocklake.common.config import Settings
from stocklake.common.http import HttpError, SourceBlockedError
from stocklake.common.logging import get_logger
from stocklake.contracts.available_at import VN_TZ
from stocklake.contracts.topics import INTRADAY_PRICES
from stocklake.sources.base import SourceError
from stocklake.streaming.producers.replay import (
    ReplayBar,
    atomic_write_text,
    bar_to_message,
    encode_message,
    flush_all,
    produce_with_backpressure,
    state_dir,
)

log = get_logger(__name__)

UTC = timezone.utc
SOURCE = "vci"
MINUTE_MS = 60_000
STATE_FILE = "intraday_producer_state.json"
SESSIONS_VN = ((dtime(9, 0), dtime(11, 30)), (dtime(13, 0), dtime(15, 0)))


@dataclass
class IntradayState:
    """bar_ts (epoch ms) của nến cuối đã giao thành công, theo mã."""

    last_bar_ts: dict[str, int] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> IntradayState:
        if not path.exists():
            return cls()
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls({k: int(v) for k, v in data.get("last_bar_ts", {}).items()})

    def save(self, path: Path) -> None:
        atomic_write_text(path, json.dumps({"last_bar_ts": self.last_bar_ts}, sort_keys=True))


@dataclass(frozen=True)
class PollResult:
    sent: int
    delivered: int
    failed: int
    per_symbol: dict[str, int]  # số nến gửi theo mã
    errors: dict[str, str]  # mã -> lỗi nguồn (bỏ qua mã đó ở lần poll này)


def default_state_path() -> Path:
    return state_dir() / STATE_FILE


def is_trading_time(now: datetime, grace: timedelta = timedelta(minutes=2)) -> bool:
    """Trong phiên (giờ VN, thứ 2–6), cộng `grace` sau mỗi phiên để gửi nến cuối."""
    vn = now.astimezone(VN_TZ)
    if vn.weekday() >= 5:
        return False
    for start, end in SESSIONS_VN:
        lo = datetime.combine(vn.date(), start, tzinfo=VN_TZ)
        hi = datetime.combine(vn.date(), end, tzinfo=VN_TZ) + grace
        if lo <= vn < hi:
            return True
    return False


def select_new_bars(bars: Sequence[ReplayBar], last_ts: int | None, now_ms: int) -> list[ReplayBar]:
    """Nến đã đóng và mới hơn last_ts, tăng dần, không trùng bar_ts."""
    uniq = {b.bar_ts: b for b in bars}
    return [
        uniq[ts]
        for ts in sorted(uniq)
        if ts + MINUTE_MS <= now_ms and (last_ts is None or ts > last_ts)
    ]


def fetch_recent(client: Any, symbol: str, now: datetime, lookback_minutes: int) -> list[ReplayBar]:
    """Tối đa `lookback_minutes` nến gần nhất (1 request, countBack nhỏ)."""
    start = now - timedelta(minutes=lookback_minutes)
    out: list[ReplayBar] = []
    pages = client.minute_pages(symbol, start, now, max_pages=1, page_size=lookback_minutes)
    for page in pages:
        for b in page.bars:
            ts = int(b.bar_ts.timestamp() * 1000)
            out.append(ReplayBar(symbol, ts, b.open, b.high, b.low, b.close, int(b.volume)))
    return out


def poll_once(
    symbols: Sequence[str],
    state: IntradayState,
    *,
    client: Any,
    producer: Any,
    now: datetime | None = None,
    wall_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    lookback_minutes: int = 30,
    flush_timeout_s: float = 30.0,
    topic: str = INTRADAY_PRICES.name,
) -> PollResult:
    """Một vòng: lấy, lọc, gửi, flush; cập nhật `state` (trong bộ nhớ) cho mã giao đủ.

    SourceBlockedError nổi lên (dừng); lỗi nguồn khác chỉ bỏ qua mã đó.
    """
    now = now or datetime.now(UTC)
    now_ms = int(now.timestamp() * 1000)
    pending: dict[str, list[int]] = {}
    counts: dict[str, dict[str, int]] = {}
    errors: dict[str, str] = {}

    def make_cb(sym: str) -> Callable[[Any, Any], None]:
        def cb(err: Any, _msg: Any) -> None:
            counts[sym]["failed" if err is not None else "delivered"] += 1
            if err is not None:
                log.error("Intraday %s: lỗi giao nhận: %s", sym, err)

        return cb

    for sym in symbols:
        try:
            bars = fetch_recent(client, sym, now, lookback_minutes)
        except SourceBlockedError:
            raise
        except (HttpError, SourceError) as exc:
            errors[sym] = f"{type(exc).__name__}: {exc}"
            log.warning("Intraday %s: bỏ qua lần này: %s", sym, errors[sym])
            continue
        new = select_new_bars(bars, state.last_bar_ts.get(sym), now_ms)
        if not new:
            continue
        counts[sym] = {"delivered": 0, "failed": 0}
        cb = make_cb(sym)
        for bar in new:
            msg = bar_to_message(bar, source=SOURCE, produced_ts=wall_ms(), replay_run_id=None)
            key, value = encode_message(msg)
            produce_with_backpressure(producer, topic, key, value, cb)
        pending[sym] = [b.bar_ts for b in new]
        producer.poll(0)

    remaining = flush_all(producer, flush_timeout_s) if pending else 0
    if remaining:
        log.error("Intraday: %d bản tin chưa giao xong sau flush", remaining)
    for sym, ts_list in pending.items():
        c = counts[sym]
        if c["delivered"] == len(ts_list):
            state.last_bar_ts[sym] = max(ts_list)
    sent = sum(len(v) for v in pending.values())
    delivered = sum(c["delivered"] for c in counts.values())
    return PollResult(
        sent=sent,
        delivered=delivered,
        failed=sent - delivered,
        per_symbol={s: len(v) for s, v in pending.items()},
        errors=errors,
    )


def run_loop(
    symbols: Sequence[str],
    *,
    interval_s: float = 60.0,
    once: bool = False,
    state_path: Path | None = None,
    client: Any = None,
    producer: Any = None,
    settings: Settings | None = None,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleep: Callable[[float], None] = time.sleep,
    max_iterations: int | None = None,
) -> PollResult | None:
    """Poll mỗi `interval_s` giây trong giờ giao dịch, ngoài giờ thì ngủ.

    `once=True`: poll đúng một lần (bỏ qua giờ giao dịch) rồi trả kết quả.
    """
    state_path = state_path or default_state_path()
    state = IntradayState.load(state_path)
    if client is None:
        from stocklake.sources.vci import VciClient

        client = VciClient(max_per_second=2.0)
    if producer is None:
        from stocklake.streaming.producers.base import make_producer

        producer = make_producer(settings)

    def tick() -> PollResult:
        res = poll_once(symbols, state, client=client, producer=producer, now=now_fn())
        state.save(state_path)
        log.info("Intraday poll: gửi %d, giao %d, lỗi %d", res.sent, res.delivered, res.failed)
        return res

    if once:
        return tick()
    n = 0
    last: PollResult | None = None
    while max_iterations is None or n < max_iterations:
        n += 1
        t0 = time.monotonic()
        if is_trading_time(now_fn()):
            last = tick()
        sleep(max(1.0, interval_s - (time.monotonic() - t0)))
    return last


def run_intraday_producer(
    symbols: Sequence[str] | None = None,
    once: bool = False,
    interval_s: float = 60.0,
    settings: Settings | None = None,
) -> PollResult | None:
    """Điểm vào cho jobs/streaming/intraday_producer.py."""
    if not symbols:
        from stocklake.streaming.producers.replay import default_symbols

        symbols = default_symbols(settings)
    return run_loop(list(symbols), interval_s=interval_s, once=once, settings=settings)
