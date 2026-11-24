"""Replay simulator (Lát 1/2).

Phát lại lịch sử nến phút (đã tải từ VCI) vào topic intraday_prices với tốc độ
cấu hình được (500, 2.000, 5.000, 10.000 msg/giây), source = 'replay',
gắn replay_run_id để đối chiếu số bản ghi gửi đi và số bản ghi ở raw.
"""

from __future__ import annotations

from collections.abc import Sequence

from stocklake.common.config import Settings


def run_replay(
    symbols: Sequence[str],
    rate_per_sec: int = 2000,
    replay_run_id: str | None = None,
    settings: Settings | None = None,
) -> int:
    """Phát lại dữ liệu, trả số bản tin đã gửi."""
    raise NotImplementedError("Lát 1")
