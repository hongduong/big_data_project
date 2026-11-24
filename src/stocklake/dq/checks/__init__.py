"""5 nhóm kiểm tra chất lượng và kiểu kết quả (khớp curated/dq_report)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class CheckGroup(str, Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    PRICE_JUMP = "price_jump"
    CROSS_SOURCE = "cross_source"


@dataclass(frozen=True)
class CheckResult:
    run_id: str
    table_name: str
    check_name: str
    metric_value: float | None
    threshold: float | None
    passed: bool
    run_ts: datetime
