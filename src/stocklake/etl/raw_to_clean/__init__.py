"""Biến đổi raw -> clean: hàm nhận và trả DataFrame để test bằng Spark local.

Import lười (pyspark chỉ nạp khi gọi), nên `import stocklake.etl.raw_to_clean` không cần pyspark.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "clean_daily_ohlcv": "daily",
    "run_daily_raw_to_clean": "daily",
    "clean_intraday": "intraday",
    "run_intraday_raw_to_clean": "intraday",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        return getattr(import_module(f"{__name__}.{_EXPORTS[name]}"), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
