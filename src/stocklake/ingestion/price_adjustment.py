"""Phát hiện nguồn điều chỉnh lại giá lịch sử (hàm thuần, chỉ thư viện chuẩn).

Dùng chung cho collector nến phút (`intraday_history`) và nến ngày (`daily_ohlcv`).

Bối cảnh: VCI trả giá ĐÃ ĐIỀU CHỈNH. Khi mã có sự kiện (cổ tức, tách...), nguồn nhân lại TOÀN BỘ
giá lịch sử của mã với một hệ số (vd FPT 11/2026: x0,9775 trên mọi nến; khối lượng không đổi).
Collector chỉ tải lại năm hiện tại, nên so giá các nến TRÙNG khoá giữa dữ liệu mới và file đang có
của năm hiện tại là cách duy nhất (không tốn thêm request) để biết các năm cũ đã lệch hệ số.

Phân loại (`compare_prices`), xét bốn giá open/high/low/close của mỗi nến trùng khoá:

- ``no_overlap``: không có nến trùng (file mới/rỗng) -> không kết luận gì.
- ``unchanged``: mọi nến trùng có cả bốn giá khớp cũ trong dung sai làm tròn (hệ số 1). Khác biệt
  CHỈ ở khối lượng được đếm trong `volume_changed` nhưng không coi là lệch giá: điều chỉnh giá
  của VCI không đổi khối lượng (đã thấy với FPT), còn khối lượng sửa lẻ không làm hỏng giá.
- ``scaled``: lệch theo MỘT tỷ lệ chung: ít nhất `MATCH_FRACTION` (99%) số nến trùng khớp
  ``mới ~= f x cũ`` trên cả bốn giá, hệ số 1 KHÔNG giải thích được (dưới 99% nến khớp với f=1),
  và có ít nhất `min_overlap` nến trùng. Cho phép tới 1% nến lệch để một vài nến nguồn sửa cùng
  lúc không làm hỏng kết luận.
- ``minor``: chỉ vài nến lệch giá (<= max(`MINOR_MIN_BARS`, `MINOR_FRACTION` x số nến trùng)),
  phần còn lại khớp hệ số 1: nguồn sửa lẻ vài nến (thường là nến gần nhất). Không cảnh báo ồn
  (log INFO), nhưng vẫn ghi vào kết quả JSON kèm ví dụ để không bị bỏ qua lặng lẽ.
- ``irregular``: mọi trường hợp lệch giá còn lại: lệch nhiều nến mà không theo một tỷ lệ chung,
  hoặc trông như một tỷ lệ chung nhưng quá ít nến trùng để kết luận (`reason="too_few_overlap"`).
  Collector chỉ CẢNH BÁO (WARNING + state + JSON), không tự tải lại năm cũ.

Dung sai (giá tuyệt đối, cùng đơn vị với nguồn). Giá VCI có 2 chữ số thập phân (vd 22309.84), tức
lượng tử q = 0,01 đồng; mỗi giá cũ/mới sai tối đa q/2 so với giá thực. Nếu giá thực đúng tỷ lệ F
(mới_thực = F x cũ_thực) thì ``|mới - f x cũ| <= |F - f| x cũ + (q/2) x (1 + f)``. Ta lấy dung sai
``tol = q x (1 + f)`` = gấp đôi biên làm tròn, nửa còn lại dành cho sai số ước lượng f (f là tổng
giá mới / tổng giá cũ trên các nến khớp sơ bộ, nên sai số làm tròn bị trung bình hoá, rất nhỏ).

Vì sao dùng dung sai TUYỆT ĐỐI: sai số tương đối do làm tròn tỷ lệ nghịch với giá.
- Giá 3.000 đồng: q/2 / 3000 ~= 1,7e-6 mỗi giá, tỷ lệ mới/cũ sai tới ~3,3e-6; tol(f~1) = 0,02 đồng
  tức ~6,7e-6 tương đối.
- Giá 100.000 đồng: 0,005 / 100000 = 5e-8; tol vẫn 0,02 đồng = 2e-7 tương đối.
Một dung sai tương đối cố định đủ rộng cho giá 3.000 (>= 6,7e-6) sẽ quá rộng ở giá cao và ngược
lại; dung sai tuyệt đối theo lượng tử đúng cho cả hai. Hệ số nhỏ nhất phân biệt được với 1 ở giá
3.000 là ~|f-1| > 6,7e-6 (0,0007%); sự kiện thật (cổ tức tiền 0,1%..., tách cổ phiếu) lớn hơn nhiều.
Nến ngày VNDirect (dự phòng) có đơn vị nghìn đồng; giả định 2 chữ số thập phân (q = 0,01 nghìn =
10 đồng; CHƯA kiểm trên dữ liệu thật), nên ở giá 3.000 đồng (3,00) dung sai tương đối tới ~0,7%:
điều chỉnh nhỏ hơn mức đó của mã giá thấp sẽ không thấy được qua VNDirect.

Vì sao dùng cả bốn giá chứ không chỉ close: một điều chỉnh thật nhân cả bốn giá cùng một hệ số;
kiểm cả bốn cho nhiều bằng chứng hơn trên mỗi nến và bắt được trường hợp nguồn chỉ sửa high/low.
Nến phút phẳng (o=h=l=c) chỉ cho một điểm dữ liệu, nên ngưỡng số nến trùng tối thiểu đặt theo
collector: nến phút 30 (nửa giờ giao dịch), nến ngày 5 phiên.
"""

from __future__ import annotations

import statistics
from collections.abc import Callable, Hashable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

PRICE_FIELDS = ("open", "high", "low", "close")
DEFAULT_QUANTUM = 0.01  # lượng tử giá của nguồn (VCI: 0,01 đồng)
MATCH_FRACTION = 0.99  # tỷ lệ nến trùng phải khớp một hệ số để kết luận "scaled"
MINOR_MIN_BARS = 3  # tối đa số nến lệch vẫn coi là "nguồn sửa lẻ"...
MINOR_FRACTION = 0.001  # ...hoặc 0,1% số nến trùng nếu lớn hơn
MAX_EXAMPLES = 5
_FLOAT_EPS = 1e-9  # sai số dấu phẩy động tương đối

UNCHANGED, SCALED, MINOR, IRREGULAR, NO_OVERLAP = (
    "unchanged",
    "scaled",
    "minor",
    "irregular",
    "no_overlap",
)


@dataclass(frozen=True)
class PriceComparison:
    kind: str  # unchanged | scaled | minor | irregular | no_overlap
    overlap: int  # số nến trùng khoá
    changed: int  # số nến lệch giá so với cũ (hệ số 1) quá dung sai
    volume_changed: int  # số nến trùng có khối lượng khác (chỉ để thông tin)
    factor: float | None  # hệ số ước lượng mới/cũ (None nếu không có nến trùng)
    matched: int  # số nến khớp hệ số `factor`
    reason: str | None = None
    examples: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "overlap": self.overlap,
            "changed": self.changed,
            "volume_changed": self.volume_changed,
            "factor": None if self.factor is None else round(self.factor, 8),
            "matched": self.matched,
            "reason": self.reason,
            "examples": list(self.examples),
        }


def tolerance(old: float, factor: float, quantum: float = DEFAULT_QUANTUM) -> float:
    """Sai lệch tuyệt đối cho phép giữa giá mới và `factor` x giá cũ (xem docstring module)."""
    return quantum * (1.0 + abs(factor)) + _FLOAT_EPS * abs(old)


def _fits(old: Mapping[str, Any], new: Mapping[str, Any], f: float, q: float, k: float = 1) -> bool:
    return all(
        abs(float(new[c]) - f * float(old[c])) <= k * tolerance(float(old[c]), f, q)
        for c in PRICE_FIELDS
    )


def _example(key: Hashable, old: Mapping[str, Any], new: Mapping[str, Any]) -> dict[str, Any]:
    oc, nc = float(old["close"]), float(new["close"])
    return {
        "key": str(key),
        "old": {c: old[c] for c in PRICE_FIELDS},
        "new": {c: new[c] for c in PRICE_FIELDS},
        "close_ratio": round(nc / oc, 8) if oc else None,
    }


def compare_prices(
    old_rows: Iterable[Mapping[str, Any]],
    new_rows: Iterable[Mapping[str, Any]],
    key: Callable[[Mapping[str, Any]], Hashable],
    *,
    quantum: float = DEFAULT_QUANTUM,
    min_overlap: int = 30,
    match_fraction: float = MATCH_FRACTION,
) -> PriceComparison:
    """So giá các nến trùng `key` giữa file đang có (`old_rows`) và dữ liệu mới (`new_rows`).

    Mỗi dòng cần open/high/low/close (và tuỳ chọn volume). Xem docstring module về phân loại.
    """
    old_by = {key(r): r for r in old_rows}
    pairs = [(k, old_by[k], n) for n in new_rows if (k := key(n)) in old_by]
    overlap = len(pairs)
    if not overlap:
        return PriceComparison(NO_OVERLAP, 0, 0, 0, None, 0)
    vol_changed = sum(1 for _, o, n in pairs if o.get("volume") != n.get("volume"))
    diff = [(k, o, n) for k, o, n in pairs if not _fits(o, n, 1.0, quantum)]
    changed = len(diff)
    if not changed:
        return PriceComparison(UNCHANGED, overlap, 0, vol_changed, 1.0, overlap)

    # Ước lượng hệ số: trung vị tỷ lệ (bền với vài nến lệch), rồi tinh lại bằng tổng mới / tổng
    # cũ trên các nến khớp sơ bộ (dung sai gấp đôi) để sai số làm tròn được trung bình hoá.
    ratios = [
        float(n[c]) / float(o[c]) for _, o, n in pairs for c in PRICE_FIELDS if float(o[c]) > 0
    ]
    f = statistics.median(ratios) if ratios else 1.0
    inl = [(o, n) for _, o, n in pairs if _fits(o, n, f, quantum, 2)]
    s_old = sum(float(o[c]) for o, _ in inl for c in PRICE_FIELDS)
    if s_old > 0:
        f = sum(float(n[c]) for _, n in inl for c in PRICE_FIELDS) / s_old
    matched = sum(1 for _, o, n in pairs if _fits(o, n, f, quantum))
    unity_matched = overlap - changed
    need = match_fraction * overlap

    def make(kind: str, reason: str | None, sample) -> PriceComparison:
        ex = tuple(_example(k, o, n) for k, o, n in sample[:MAX_EXAMPLES])
        return PriceComparison(kind, overlap, changed, vol_changed, f, matched, reason, ex)

    if matched >= need and unity_matched < need:
        if overlap >= min_overlap:
            return make(SCALED, None, diff)
        return make(IRREGULAR, "too_few_overlap", diff)
    if changed <= max(MINOR_MIN_BARS, MINOR_FRACTION * overlap):
        return make(MINOR, "few_bars_revised", diff)
    off = [(k, o, n) for k, o, n in diff if not _fits(o, n, f, quantum)] or diff
    return make(IRREGULAR, "not_single_ratio", off)


# ----------------------------------------------------------------- lịch sử trong state


def event_key(collector: str, symbol: str, run_id: str) -> str:
    """Khoá state của một sự kiện điều chỉnh/lệch giá: mỗi lượt một khoá -> giữ lịch sử."""
    return f"{collector}/{symbol}/_adjustment/{run_id}"


def adjustment_events(state: Any, collector: str, symbol: str | None = None) -> list[dict]:
    """Đọc lại các sự kiện đã ghi (cần store có `entries`, vd JsonFileStateStore), cũ -> mới.

    Mỗi phần tử: {"key", "status", "error"?, "meta", "updated_at"}.
    """
    entries = getattr(state, "entries", None)
    if entries is None:
        return []
    prefix = f"{collector}/{symbol}/_adjustment/" if symbol else f"{collector}/"
    out = [{"key": k, **v} for k, v in entries(collector, prefix).items() if "/_adjustment/" in k]
    return sorted(out, key=lambda e: (e.get("meta", {}).get("detected_at", ""), e["key"]))
