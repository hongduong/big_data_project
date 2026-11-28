"""Ghi chú riêng cho DQ nến phút (conf/app/dq_notes.yaml): giai đoạn, khoảng trống nguồn và ngày
bất thường đã biết.

Ghi chú là DỮ LIỆU, không lọc gì ở raw/clean. Runner dùng chúng để:
- tính bộ số bỏ các giai đoạn đã biết VÀ các (mã, ngày) khoảng trống đã biết của nguồn
  (`known_source_gaps`), dùng làm kết luận đạt/không đạt của phần đầy đủ ngày
  (stocklake.dq.verdict);
- tính bộ số bỏ các ngày bất thường được đánh dấu `exclude_from: [<kiểm tra>]` khỏi kiểm tra đó
  (hiện chỉ có `volume_minute_gt_daily`); ngày không có trường này không bị loại khỏi kiểm tra nào;
- gắn `note` vào các dòng xấu nhất rơi vào giai đoạn / khoảng trống nguồn / ngày đã biết.
Python thuần, không import pyspark.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any

from stocklake.common.logging import get_logger

log = get_logger(__name__)

CONF_NAME = "dq_notes"
# Giá trị hợp lệ của `known_anomalies[].exclude_from`: kiểm tra cứng mà ngày đó bị loại khỏi.
EXCLUDE_VOLUME_MINUTE_GT_DAILY = "volume_minute_gt_daily"
EXCLUDE_TARGETS = frozenset({EXCLUDE_VOLUME_MINUTE_GT_DAILY})


@dataclass(frozen=True)
class KnownPeriod:
    """Giai đoạn đã biết của một mã; `start` None = từ đầu dữ liệu, `end` None = tới cuối."""

    symbol: str
    start: date | None
    end: date | None
    label: str
    note: str = ""

    def contains(self, symbol: str, day: date) -> bool:
        if symbol != self.symbol:
            return False
        return (self.start is None or day >= self.start) and (self.end is None or day <= self.end)


@dataclass(frozen=True)
class KnownSourceGap:
    """Các ngày mà nguồn không có nến phút cho một mã (đã gọi lại nguồn kiểm chứng)."""

    symbol: str
    dates: tuple[date, ...]
    label: str
    note: str = ""

    def contains(self, symbol: str, day: date) -> bool:
        return symbol == self.symbol and day in self.dates


@dataclass(frozen=True)
class KnownAnomaly:
    """Ngày bất thường đã biết; `symbols` rỗng = toàn thị trường.

    `exclude_from`: các kiểm tra cứng (EXCLUDE_TARGETS) bỏ ngày này khỏi bộ số kết luận; rỗng =
    chỉ là ghi chú, không loại trừ gì.
    """

    date: date
    label: str
    note: str = ""
    status: str = ""
    symbols: tuple[str, ...] = ()
    exclude_from: tuple[str, ...] = ()

    def contains(self, symbol: str, day: date) -> bool:
        return day == self.date and (not self.symbols or symbol in self.symbols)


@dataclass(frozen=True)
class DqNotes:
    known_periods: tuple[KnownPeriod, ...] = ()
    known_anomalies: tuple[KnownAnomaly, ...] = ()
    source: str | None = field(default=None, compare=False)
    known_source_gaps: tuple[KnownSourceGap, ...] = ()

    def source_gap_pairs(self) -> tuple[tuple[str, date], ...]:
        """Mọi (mã, ngày) của `known_source_gaps`, theo thứ tự trong file."""
        return tuple((g.symbol, d) for g in self.known_source_gaps for d in g.dates)

    def anomalies_excluded_from(self, target: str) -> tuple[KnownAnomaly, ...]:
        """Các ngày bất thường đánh dấu `exclude_from` chứa `target`."""
        return tuple(a for a in self.known_anomalies if target in a.exclude_from)

    def labels_for(self, symbol: str | None, day: date | None) -> list[str]:
        """Nhãn của các giai đoạn / ngày bất thường chứa (symbol, day).

        `day` None: mọi giai đoạn và khoảng trống nguồn của mã (dùng cho danh sách theo mã).
        """
        if not symbol:
            return []
        if day is None:
            out = [p.label for p in self.known_periods if p.symbol == symbol]
            out += [g.label for g in self.known_source_gaps if g.symbol == symbol]
            return out
        out = [p.label for p in self.known_periods if p.contains(symbol, day)]
        out += [g.label for g in self.known_source_gaps if g.contains(symbol, day)]
        out += [a.label for a in self.known_anomalies if a.contains(symbol, day)]
        return out

    def to_json(self) -> dict[str, Any]:
        def enc(d: dict[str, Any]) -> dict[str, Any]:
            return {k: (v.isoformat() if isinstance(v, date) else v) for k, v in d.items()}

        return {
            "source": self.source,
            "known_periods": [enc(asdict(p)) for p in self.known_periods],
            "known_source_gaps": [
                {
                    "symbol": g.symbol,
                    "dates": [d.isoformat() for d in g.dates],
                    "label": g.label,
                    "note": g.note,
                }
                for g in self.known_source_gaps
            ],
            "known_anomalies": [
                enc({**asdict(a), "symbols": list(a.symbols), "exclude_from": list(a.exclude_from)})
                for a in self.known_anomalies
            ],
        }


def _date(v: Any, where: str, allow_none: bool = False) -> date | None:
    if v is None:
        if allow_none:
            return None
        raise ValueError(f"{where}: thiếu ngày")
    if isinstance(v, datetime):
        raise ValueError(f"{where}: cần ngày YYYY-MM-DD, không phải thời điểm ({v!r})")
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        try:
            return date.fromisoformat(v.strip())
        except ValueError:
            pass
    raise ValueError(f"{where}: ngày không hợp lệ {v!r} (cần YYYY-MM-DD)")


def _symbol(v: Any, where: str) -> str:
    if not isinstance(v, str) or not v.strip() or v != v.strip().upper():
        raise ValueError(f"{where}: mã phải là chuỗi viết hoa, không rỗng ({v!r})")
    return v


def _text(v: Any, where: str, required: bool = False) -> str:
    if v is None and not required:
        return ""
    if not isinstance(v, str) or (required and not v.strip()):
        raise ValueError(f"{where}: cần chuỗi không rỗng ({v!r})")
    return v.strip()


def _exclude_target(v: Any, where: str) -> str:
    if not isinstance(v, str) or v not in EXCLUDE_TARGETS:
        raise ValueError(f"{where}: {v!r} không hợp lệ (cần một trong {sorted(EXCLUDE_TARGETS)})")
    return v


def _list(v: Any, where: str) -> list[Any]:
    if v is None:
        return []
    if not isinstance(v, list):
        raise ValueError(f"{where}: phải là danh sách")
    return v


def _check_no_overlap(periods: Sequence[KnownPeriod]) -> None:
    """Hai giai đoạn của cùng mã không được chồng nhau (mỗi (mã, ngày) thuộc tối đa một)."""
    by_symbol: dict[str, list[tuple[int, KnownPeriod]]] = {}
    for i, p in enumerate(periods):
        by_symbol.setdefault(p.symbol, []).append((i, p))
    for sym, items in by_symbol.items():
        items.sort(key=lambda x: x[1].start or date.min)
        for (i, a), (j, b) in zip(items, items[1:], strict=False):
            # b.start >= a.start; chồng nhau khi a mở tới cuối hoặc b bắt đầu trước khi a kết thúc
            if a.end is None or b.start is None or b.start <= a.end:
                raise ValueError(
                    f"known_periods[{i}] và known_periods[{j}] ({sym}) chồng nhau: "
                    f"[{a.start}, {a.end}] và [{b.start}, {b.end}]"
                )


def parse_notes(conf: Mapping[str, Any] | None, source: str | None = None) -> DqNotes:
    """Kiểm và đổi nội dung YAML sang DqNotes; sai định dạng thì ValueError."""
    conf = conf or {}
    if not isinstance(conf, Mapping):
        raise ValueError("dq_notes: gốc phải là mapping")
    periods: list[KnownPeriod] = []
    for i, e in enumerate(_list(conf.get("known_periods"), "known_periods")):
        w = f"known_periods[{i}]"
        if not isinstance(e, Mapping):
            raise ValueError(f"{w}: phải là mapping")
        p = KnownPeriod(
            symbol=_symbol(e.get("symbol"), w),
            start=_date(e.get("start"), f"{w}.start", allow_none=True),
            end=_date(e.get("end"), f"{w}.end", allow_none=True),
            label=_text(e.get("label"), f"{w}.label", required=True),
            note=_text(e.get("note"), f"{w}.note"),
        )
        if p.start and p.end and p.start > p.end:
            raise ValueError(f"{w}: start {p.start} sau end {p.end}")
        periods.append(p)
    _check_no_overlap(periods)
    gaps: list[KnownSourceGap] = []
    seen: set[tuple[str, date]] = set()
    for i, e in enumerate(_list(conf.get("known_source_gaps"), "known_source_gaps")):
        w = f"known_source_gaps[{i}]"
        if not isinstance(e, Mapping):
            raise ValueError(f"{w}: phải là mapping")
        sym = _symbol(e.get("symbol"), w)
        raw_dates = _list(e.get("dates"), f"{w}.dates")
        if not raw_dates:
            raise ValueError(f"{w}.dates: cần ít nhất một ngày")
        days = tuple(_date(d, f"{w}.dates[{j}]") for j, d in enumerate(raw_dates))
        for d in days:
            if (sym, d) in seen:
                raise ValueError(f"{w}: ({sym}, {d}) đã có ở mục khác hoặc lặp lại")
            seen.add((sym, d))  # type: ignore[arg-type]
        gaps.append(
            KnownSourceGap(
                symbol=sym,
                dates=days,  # type: ignore[arg-type]
                label=_text(e.get("label"), f"{w}.label", required=True),
                note=_text(e.get("note"), f"{w}.note"),
            )
        )
    anomalies: list[KnownAnomaly] = []
    for i, e in enumerate(_list(conf.get("known_anomalies"), "known_anomalies")):
        w = f"known_anomalies[{i}]"
        if not isinstance(e, Mapping):
            raise ValueError(f"{w}: phải là mapping")
        syms = tuple(_symbol(s, f"{w}.symbols") for s in _list(e.get("symbols"), f"{w}.symbols"))
        excl = tuple(
            _exclude_target(t, f"{w}.exclude_from")
            for t in _list(e.get("exclude_from"), f"{w}.exclude_from")
        )
        anomalies.append(
            KnownAnomaly(
                date=_date(e.get("date"), f"{w}.date"),  # type: ignore[arg-type]
                label=_text(e.get("label"), f"{w}.label", required=True),
                note=_text(e.get("note"), f"{w}.note"),
                status=_text(e.get("status"), f"{w}.status"),
                symbols=syms,
                exclude_from=excl,
            )
        )
    return DqNotes(tuple(periods), tuple(anomalies), source, tuple(gaps))


def load_dq_notes(settings: Any = None) -> DqNotes:
    """Đọc conf/app/dq_notes.yaml qua load_app_conf; thiếu file thì trả ghi chú rỗng."""
    from stocklake.common.config import get_settings, load_app_conf

    s = settings or get_settings()
    try:
        conf = load_app_conf(CONF_NAME, s)
    except FileNotFoundError:
        log.warning("Không có %s/%s.yaml: chạy DQ không có ghi chú", s.app_conf_dir, CONF_NAME)
        return DqNotes()
    return parse_notes(conf, source=f"{s.app_conf_dir}/{CONF_NAME}.yaml")


def annotate(rows: Iterable[dict[str, Any]], notes: DqNotes) -> list[dict[str, Any]]:
    """Thêm khoá `note` (nhãn nối bằng ', ' hoặc None) vào từng dòng có symbol[, trade_date]."""
    out = []
    for r in rows:
        day = r.get("trade_date")
        if isinstance(day, str):
            day = date.fromisoformat(day[:10])
        labels = notes.labels_for(r.get("symbol"), day)
        out.append({**r, "note": ", ".join(labels) or None})
    return out
