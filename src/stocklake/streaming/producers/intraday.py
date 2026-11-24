"""Producer nến 1 phút realtime (Lát 1/2).

Trong giờ giao dịch: gọi nguồn qua stocklake.sources (VCI, dự phòng VNDirect),
lấy nến phút mới của từng mã, gắn produced_ts/available_at và gửi vào topic
intraday_prices (key = symbol).
"""

from __future__ import annotations

from collections.abc import Sequence

from stocklake.common.config import Settings


def run_intraday_producer(
    symbols: Sequence[str],
    poll_seconds: float = 30.0,
    settings: Settings | None = None,
) -> None:
    """Vòng lặp lấy nến mới và gửi Kafka cho tới khi bị dừng."""
    raise NotImplementedError("Lát 1")
