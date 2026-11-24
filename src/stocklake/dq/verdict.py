"""Quy tắc đạt/không đạt của DQ nến phút (duyệt 24/11/2026). Python thuần, không import pyspark.

Check có hai bộ số (toàn bộ dữ liệu; bỏ giai đoạn đã biết) thì ĐẠT/KHÔNG ĐẠT tính trên bộ số
bỏ giai đoạn đã biết. Bộ số toàn bộ dữ liệu vẫn được tính và ghi, chỉ để tham khảo.

Đặt tên trong `dq_report.check_name` (từ 24/11/2026):
- `<check>`            : dòng KẾT LUẬN — metric và passed tính trên bộ số bỏ giai đoạn đã biết;
- `<check>__all_data`  : dòng THAM KHẢO — cùng ngưỡng, tính trên toàn bộ dữ liệu; `passed` chỉ là
                         so sánh ngưỡng, không quyết định kết luận;
- check không có biến thể (volume, ngày ngoài lịch): giữ tên `<check>`, tính trên toàn bộ dữ liệu.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any

from stocklake.dq.checks import CheckResult

ALL_DATA_SUFFIX = "__all_data"
BASIS_EXCL = "excluding_known_periods"
BASIS_ALL = "all_data"
VERDICT_RULE = (
    "Check có bộ số bỏ giai đoạn đã biết (conf/app/dq_notes.yaml): đạt/không đạt tính trên bộ số "
    "bỏ giai đoạn đã biết (basis=excluding_known_periods); bộ số toàn bộ dữ liệu "
    "(reference_all_data) chỉ để tham khảo. Check còn lại tính trên toàn bộ dữ liệu "
    "(basis=all_data). passed tổng = mọi check đều đạt."
)


@dataclass(frozen=True)
class Verdict:
    rows: list[CheckResult]  # ghi vào dq_report: kết luận + tham khảo + check không có biến thể
    checks: list[dict[str, Any]]  # dạng JSON của báo cáo
    passed: bool  # mọi dòng kết luận đều đạt


def _ref_json(r: CheckResult) -> dict[str, Any]:
    return {"metric": r.metric_value, "passed": bool(r.passed)}


def build_verdict(
    variant_all: Sequence[CheckResult],
    variant_excl: Sequence[CheckResult],
    single: Sequence[CheckResult],
    group_of: Callable[[str], str],
) -> Verdict:
    """Ghép các bộ số thành kết luận.

    `variant_all` / `variant_excl`: cùng danh sách check (tên thường, không hậu tố), lần lượt
    trên toàn bộ dữ liệu và bỏ giai đoạn đã biết (hai bộ giống hệt nhau khi không có giai đoạn
    nào). `single`: check không có biến thể. `group_of(check_name)` trả nhóm báo cáo.
    """
    excl = {r.check_name: r for r in variant_excl}
    missing = [r.check_name for r in variant_all if r.check_name not in excl]
    if missing:
        raise ValueError(f"Thiếu bộ số bỏ giai đoạn đã biết cho: {missing}")
    rows: list[CheckResult] = []
    checks: list[dict[str, Any]] = []
    for ref in variant_all:
        verdict = excl[ref.check_name]
        rows.append(verdict)
        checks.append(
            {
                "check_name": verdict.check_name,
                "group": group_of(verdict.check_name),
                "table_name": verdict.table_name,
                "metric": verdict.metric_value,
                "threshold": verdict.threshold,
                "passed": bool(verdict.passed),
                "basis": BASIS_EXCL,
                "reference_all_data": _ref_json(ref),
            }
        )
    for ref in variant_all:
        rows.append(replace(ref, check_name=ref.check_name + ALL_DATA_SUFFIX))
    for r in single:
        rows.append(r)
        checks.append(
            {
                "check_name": r.check_name,
                "group": group_of(r.check_name),
                "table_name": r.table_name,
                "metric": r.metric_value,
                "threshold": r.threshold,
                "passed": bool(r.passed),
                "basis": BASIS_ALL,
            }
        )
    return Verdict(rows=rows, checks=checks, passed=all(c["passed"] for c in checks))
