"""Quy tắc đạt/không đạt của DQ nến phút (duyệt 24/11/2026, mở rộng 28/11/2026).

Python thuần, không import pyspark.

Check có hai bộ số (toàn bộ dữ liệu; bộ loại trừ) thì ĐẠT/KHÔNG ĐẠT tính trên bộ loại trừ. Bộ số
toàn bộ dữ liệu vẫn được tính và ghi, chỉ để tham khảo. Bộ loại trừ tuỳ check (`basis`):
- check đầy đủ ngày: bỏ giai đoạn đã biết và khoảng trống nguồn đã biết (`known_source_gaps`,
  từ 28/11/2026) — tên basis vẫn là `excluding_known_periods`;
- `intraday_volume_days_minute_gt_daily` (từ 28/11/2026): bỏ các ngày bất thường đánh dấu
  `exclude_from: [volume_minute_gt_daily]` trong dq_notes (`excluding_known_anomalies`).

Đặt tên trong `dq_report.check_name` (từ 24/11/2026):
- `<check>`            : dòng KẾT LUẬN — metric và passed tính trên bộ loại trừ;
- `<check>__all_data`  : dòng THAM KHẢO — cùng ngưỡng, tính trên toàn bộ dữ liệu; `passed` chỉ là
                         so sánh ngưỡng, không quyết định kết luận;
- check không có biến thể (volume khác, ngày ngoài lịch, ngày sau nến cuối): giữ tên `<check>`,
  tính trên toàn bộ dữ liệu.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any

from stocklake.dq.checks import CheckResult

ALL_DATA_SUFFIX = "__all_data"
BASIS_EXCL = "excluding_known_periods"
BASIS_EXCL_ANOMALIES = "excluding_known_anomalies"
BASIS_ALL = "all_data"
VERDICT_RULE = (
    "Check có bộ số loại trừ (conf/app/dq_notes.yaml): đạt/không đạt tính trên bộ loại trừ — "
    "check đầy đủ ngày bỏ giai đoạn đã biết (known_periods) và các (mã, ngày) khoảng trống đã "
    "biết của nguồn (known_source_gaps) (basis=excluding_known_periods); "
    "intraday_volume_days_minute_gt_daily bỏ các ngày bất thường đánh dấu "
    "exclude_from: [volume_minute_gt_daily] (basis=excluding_known_anomalies). Bộ số toàn bộ dữ "
    "liệu (reference_all_data) chỉ để tham khảo. Check còn lại tính trên toàn bộ dữ liệu "
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
    basis_of: Callable[[str], str] | None = None,
) -> Verdict:
    """Ghép các bộ số thành kết luận.

    `variant_all` / `variant_excl`: cùng danh sách check (tên thường, không hậu tố), lần lượt
    trên toàn bộ dữ liệu và trên bộ loại trừ (hai bộ giống hệt nhau khi không có gì để loại).
    `single`: check không có biến thể. `group_of(check_name)` trả nhóm báo cáo.
    `basis_of(check_name)` trả tên bộ loại trừ của check có biến thể (mặc định BASIS_EXCL).
    """
    excl = {r.check_name: r for r in variant_excl}
    missing = [r.check_name for r in variant_all if r.check_name not in excl]
    if missing:
        raise ValueError(f"Thiếu bộ số loại trừ cho: {missing}")
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
                "basis": basis_of(verdict.check_name) if basis_of else BASIS_EXCL,
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
