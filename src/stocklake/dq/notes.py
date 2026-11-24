"""Ghi chú riêng cho DQ nến phút (conf/app/dq_notes.yaml): giai đoạn và ngày bất thường đã biết.

Ghi chú là DỮ LIỆU, không lọc gì ở raw/clean. Runner dùng chúng để:
- tính thêm khối metric `excluding_known_periods` (bỏ các giai đoạn đã biết);
- gắn `note` vào các dòng xấu nhất rơi vào giai đoạn / ngày đã biết.
Python thuần, không import pyspark.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any

from stocklake.common.logging import get_logger

log = get_logger(__name__)

CONF_NAME = "dq_notes"


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
class KnownAnomaly:
    """Ngày bất thường đã biết; `symbols` rỗng = toàn thị trường."""

    date: date
    label: str
    note: str = ""
    status: str = ""
    symbols: tuple[str, ...] = ()

    def contains(self, symbol: str, day: date) -> bool:
        return day == self.date and (not self.symbols or symbol in self.symbols)


@dataclass(frozen=True)
class DqNotes:
    known_periods: tuple[KnownPeriod, ...] = ()
    known_anomalies: tuple[KnownAnomaly, ...] = ()
    source: str | None = field(default=None, compare=False)

    def labels_for(self, symbol: str | None, day: date | None) -> list[str]:
        """Nhãn của các giai đoạn / ngày bất thường chứa (symbol, day).

        `day` None: mọi giai đoạn của mã (dùng cho danh sách theo mã).
        """
        if not symbol:
            return []
        if day is None:
            return [p.label for p in self.known_periods if p.symbol == symbol]
        out = [p.label for p in self.known_periods if p.contains(symbol, day)]
        out += [a.label for a in self.known_anomalies if a.contains(symbol, day)]
        return out

    def to_json(self) -> dict[str, Any]:
        def enc(d: dict[str, Any]) -> dict[str, Any]:
            return {k: (v.isoformat() if isinstance(v, date) else v) for k, v in d.items()}

        return {
            "source": self.source,
            "known_periods": [enc(asdict(p)) for p in self.known_periods],
            "known_anomalies": [
                enc({**asdict(a), "symbols": list(a.symbols)}) for a in self.known_anomalies
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


def _list(v: Any, where: str) -> list[Any]:
    if v is None:
        return []
    if not isinstance(v, list):
        raise ValueError(f"{where}: phải là danh sách")
    return v


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
    anomalies: list[KnownAnomaly] = []
    for i, e in enumerate(_list(conf.get("known_anomalies"), "known_anomalies")):
        w = f"known_anomalies[{i}]"
        if not isinstance(e, Mapping):
            raise ValueError(f"{w}: phải là mapping")
        syms = tuple(_symbol(s, f"{w}.symbols") for s in _list(e.get("symbols"), f"{w}.symbols"))
        anomalies.append(
            KnownAnomaly(
                date=_date(e.get("date"), f"{w}.date"),  # type: ignore[arg-type]
                label=_text(e.get("label"), f"{w}.label", required=True),
                note=_text(e.get("note"), f"{w}.note"),
                status=_text(e.get("status"), f"{w}.status"),
                symbols=syms,
            )
        )
    return DqNotes(tuple(periods), tuple(anomalies), source)


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
