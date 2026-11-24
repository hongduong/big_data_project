"""Dựng lại clean.intraday_1m theo tháng: trạng thái build và lập kế hoạch (Python thuần).

Không import pyspark; phần Spark (đọc raw, tính fingerprint, ghi) nằm ở `intraday.py`.

Trạng thái lưu ở `<clean>/intraday_1m/_build_state.json` (tên bắt đầu bằng `_` nên Spark và
reader pyarrow của dự án bỏ qua):

    {"version": 1, "built_at": ISO-UTC, "run_id": ..., "mode": ...,
     "probe_ms": thời điểm NameNode lúc bắt đầu lượt chạy,
     "horizon_ms": mọi thay đổi thư mục raw có mtime <= mốc này đã phản ánh vào clean,
     "inputs": {"history": ["source=vci/year=2026", ...], "stream": ["trade_date=...", ...]},
     "months": {"YYYY-MM": {"fingerprint": {...}, "clean_rows": n, "built_at": ...,
                            "run_id": ..., ["orphan": true]}}}

Phát hiện thay đổi (chi tiết ở `candidate_months` và `compute_horizon`):
1. Tháng ứng viên: thư mục lá của raw (`source=<s>/year=<Y>` của history, `trade_date=<D>` của
   streaming) có mtime > `horizon_ms`, thư mục mới, hoặc thư mục đã biến mất. HDFS cập nhật mtime
   của thư mục cha khi tạo/xoá/đổi tên file con, nên collector (ghi tmp rồi đổi tên) và file
   sink của streaming (tạo file trực tiếp) đều làm mtime thư mục lá tăng. year=Y -> 12 tháng của
   Y; trade_date=D -> tháng của D (sink tính trade_date đúng như clean: ngày VN của bar_ts).
2. Chỉ tính fingerprint cho tháng ứng viên rồi so với trạng thái.

Mọi mốc thời gian so sánh đều là mtime do NameNode gán (kể cả `probe_ms`: mtime của một file
thăm dò vừa tạo), nên không phụ thuộc đồng hồ của driver.
"""

from __future__ import annotations

import calendar
import datetime as dt
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

STATE_VERSION = 1
STATE_FILE = "_build_state.json"
HISTORY, STREAM = "history", "stream"
ORIGINS = (HISTORY, STREAM)
# Khoảng an toàn: file streaming được tạo (cập nhật mtime thư mục) trước khi batch commit vào
# `_spark_metadata`; file collector đóng trước khi đổi tên. Thay đổi mới hơn (probe - lag) chưa
# chắc đã "lắng" nên vẫn là ứng viên ở lượt sau. Phải lớn hơn thời lượng một micro-batch.
SAFETY_LAG_S = 900.0
VN_OFFSET_S = 7 * 3600

MODE_FULL, MODE_INCREMENTAL, MODE_DATES = "full", "incremental", "dates"

_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")
_HISTORY_DIR_RE = re.compile(r"(?:^|/)(source=[^/]+/year=(\d{4}))/?$")
_STREAM_DIR_RE = re.compile(r"(?:^|/)(trade_date=(\d{4})-(\d{2})-(\d{2}))/?$")
_CLEAN_FILE_RE = re.compile(r"(?:^|/)year=(\d{4})/month=(\d{1,2})/[^/]+$")


@dataclass(frozen=True)
class DirInfo:
    """Thư mục lá của raw: `name` tương đối so với gốc bảng, `mtime_ms` do NameNode gán."""

    name: str
    mtime_ms: int


# ---------------------------------------------------------------- tháng


def month_key(year: int, month: int) -> str:
    return f"{int(year):04d}-{int(month):02d}"


def normalise_months(values: Iterable[Any]) -> list[str]:
    """'YYYY-MM-DD' hoặc 'YYYY-MM' -> danh sách 'YYYY-MM' đã sắp xếp, không trùng."""
    out = set()
    for v in values:
        s = str(v).strip()[:7]
        m = _MONTH_RE.match(s)
        if not m or not 1 <= int(m.group(2)) <= 12:
            raise ValueError(f"Ngày/tháng không hợp lệ: {v!r} (cần YYYY-MM-DD hoặc YYYY-MM)")
        out.add(s)
    return sorted(out)


def _next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


def month_bounds_utc(month: str) -> tuple[int, int]:
    """[đầu, cuối) của tháng theo giờ VN, dạng epoch giây UTC."""
    y, m = int(month[:4]), int(month[5:7])
    ny, nm = _next_month(y, m)
    start = calendar.timegm((y, m, 1, 0, 0, 0)) - VN_OFFSET_S
    end = calendar.timegm((ny, nm, 1, 0, 0, 0)) - VN_OFFSET_S
    return start, end


def month_ranges(months: Iterable[str]) -> list[tuple[str, str]]:
    """Gộp các tháng liền nhau thành đoạn (tháng đầu, tháng cuối) để biểu thức lọc ngắn."""
    ms = normalise_months(months)
    out: list[tuple[str, str]] = []
    for m in ms:
        if out:
            y, mo = int(out[-1][1][:4]), int(out[-1][1][5:7])
            if month_key(*_next_month(y, mo)) == m:
                out[-1] = (out[-1][0], m)
                continue
        out.append((m, m))
    return out


def month_last_day(month: str) -> str:
    y, m = int(month[:4]), int(month[5:7])
    return f"{month}-{calendar.monthrange(y, m)[1]:02d}"


def months_of_dir(origin: str, name: str) -> list[str]:
    """Các tháng (giờ VN) mà thư mục raw có thể chứa."""
    if origin == HISTORY:
        m = _HISTORY_DIR_RE.search(name)
        return [month_key(int(m.group(2)), mo) for mo in range(1, 13)] if m else []
    if origin == STREAM:
        m = _STREAM_DIR_RE.search(name)
        if not m or not 1 <= int(m.group(3)) <= 12:
            return []
        return [month_key(int(m.group(2)), int(m.group(3)))]
    raise ValueError(f"origin không hợp lệ: {origin!r}")


def dir_name(origin: str, path: str) -> str | None:
    """Đường dẫn đầy đủ (vd hdfs://nn:8020/data/raw/...) -> tên tương đối của thư mục lá."""
    rx = _HISTORY_DIR_RE if origin == HISTORY else _STREAM_DIR_RE
    m = rx.search(path.rstrip("/"))
    return m.group(1) if m else None


def clean_months_from_files(paths: Iterable[str]) -> set[str]:
    """Đường dẫn file của clean (year=Y/month=M/part-*.parquet) -> tập tháng có dữ liệu."""
    out = set()
    for p in paths:
        m = _CLEAN_FILE_RE.search(p)
        if m and 1 <= int(m.group(2)) <= 12:
            out.add(month_key(int(m.group(1)), int(m.group(2))))
    return out


def possible_months(dirs: Mapping[str, Iterable[DirInfo]]) -> set[str]:
    """Tháng có thể còn dữ liệu đầu vào (theo thư mục đang có)."""
    out: set[str] = set()
    for origin, items in dirs.items():
        for d in items:
            out.update(months_of_dir(origin, d.name))
    return out


# ---------------------------------------------------------------- trạng thái


def parse_state(text: str | bytes | None) -> dict[str, Any] | None:
    """JSON -> dict trạng thái; None nếu thiếu, hỏng hoặc khác phiên bản (=> dựng lại toàn bộ)."""
    if not text:
        return None
    try:
        doc = json.loads(text)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(doc, dict) or doc.get("version") != STATE_VERSION:
        return None
    months = doc.get("months")
    inputs = doc.get("inputs")
    if not isinstance(months, dict) or not isinstance(inputs, dict):
        return None
    if not isinstance(doc.get("horizon_ms"), int):
        return None
    for k, v in months.items():
        if not _MONTH_RE.match(str(k)) or not isinstance(v, dict):
            return None
    for origin in ORIGINS:
        if not isinstance(inputs.get(origin, []), list):
            return None
    return doc


def dump_state(state: Mapping[str, Any]) -> str:
    """Dict -> JSON (ASCII, khoá sắp xếp: so sánh/diff được)."""
    return json.dumps(state, sort_keys=True, ensure_ascii=True, indent=1) + "\n"


def has_rows(fp: Mapping[str, Any] | None) -> bool:
    return bool(fp) and any(int((v or {}).get("rows", 0)) > 0 for v in fp.values())


def live_months(state: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Các tháng trong trạng thái không bị đánh dấu orphan."""
    if not state:
        return {}
    return {m: e for m, e in state["months"].items() if not e.get("orphan")}


def clean_rows_total(state: Mapping[str, Any] | None) -> int | None:
    if state is None:
        return None
    return sum(int(e.get("clean_rows") or 0) for e in live_months(state).values())


# ---------------------------------------------------------------- phát hiện


def choose_mode(full: bool, forced: list[str], state: Mapping[str, Any] | None) -> str:
    if full and forced:
        raise ValueError("Không dùng --full cùng --dates")
    if forced:
        return MODE_DATES
    if full or state is None:
        return MODE_FULL
    return MODE_INCREMENTAL


def compute_horizon(probe_ms: int, dirs: Mapping[str, Iterable[DirInfo]], lag_ms: int) -> int:
    """Mốc mà mọi thay đổi thư mục raw có mtime <= mốc chắc chắn đã phản ánh vào lượt này.

    = min(mtime lớn nhất đã thấy, probe - lag). Thay đổi cũ hơn (probe - lag) đã lắng: file
    streaming tạo trước đó đã commit vào `_spark_metadata` trước khi lượt này bắt đầu đọc.
    Thay đổi mới hơn có thể chưa được đọc nên giữ làm ứng viên cho lượt sau.
    """
    settled = int(probe_ms) - int(lag_ms)
    mtimes = [d.mtime_ms for items in dirs.values() for d in items]
    return min(max(mtimes), settled) if mtimes else settled


def candidate_months(
    dirs: Mapping[str, Iterable[DirInfo]], state: Mapping[str, Any]
) -> dict[str, list[str]]:
    """Tháng ứng viên -> lý do. Thư mục mtime > horizon, thư mục mới, thư mục đã mất."""
    horizon = int(state["horizon_ms"])
    known_all = state.get("inputs", {})
    out: dict[str, set[str]] = {}

    def add(origin: str, name: str, why: str) -> None:
        for m in months_of_dir(origin, name):
            out.setdefault(m, set()).add(f"{origin}:{why}")

    for origin in ORIGINS:
        known = set(known_all.get(origin, []))
        seen = set()
        for d in dirs.get(origin, ()):
            seen.add(d.name)
            if d.mtime_ms > horizon:
                add(origin, d.name, "modified")
            elif d.name not in known:
                add(origin, d.name, "new_dir")
        for name in known - seen:
            add(origin, name, "removed")
    return {m: sorted(v) for m, v in sorted(out.items())}


def months_to_fingerprint(
    mode: str,
    state: Mapping[str, Any] | None,
    candidates: Iterable[str],
    clean_months: set[str],
    forced: list[str],
) -> list[str] | None:
    """Tháng cần tính fingerprint; None = tất cả (full)."""
    if mode == MODE_FULL:
        return None
    if mode == MODE_DATES:
        return sorted(forced)
    missing = {m for m in live_months(state) if m not in clean_months}
    return sorted(set(candidates) | missing)


@dataclass
class Plan:
    mode: str
    rebuild: list[str] = field(default_factory=list)
    reasons: dict[str, str] = field(default_factory=dict)  # tháng -> new/changed/...
    compared: list[str] = field(default_factory=list)  # tháng đã tính fingerprint
    orphans: list[str] = field(default_factory=list)


def decide(
    mode: str,
    state: Mapping[str, Any] | None,
    compared: list[str] | None,
    fingerprints: Mapping[str, Mapping[str, Any]],
    clean_months: set[str],
    possible: set[str],
    forced: list[str] | None = None,
) -> Plan:
    """Quyết định tháng dựng lại và tháng mồ côi (có trong trạng thái/clean nhưng hết đầu vào).

    Không bao giờ xoá dữ liệu: tháng mồ côi chỉ được báo cáo.
    """
    with_rows = {m for m, fp in fingerprints.items() if has_rows(fp)}
    if mode == MODE_FULL:
        rebuild = sorted(with_rows)
        orphans = sorted((clean_months | set(live_months(state))) - with_rows)
        return Plan(mode, rebuild, {m: "full" for m in rebuild}, sorted(fingerprints), orphans)

    known = state["months"] if state else {}
    if mode == MODE_DATES:
        forced = sorted(forced or [])
        rebuild = [m for m in forced if m in with_rows]
        orphans = [m for m in forced if m not in with_rows and (m in known or m in clean_months)]
        return Plan(mode, rebuild, {m: "forced" for m in rebuild}, forced, orphans)

    compared = sorted(compared or [])
    reasons: dict[str, str] = {}
    orphans: set[str] = set()
    for m in compared:
        entry = known.get(m)
        fp = fingerprints.get(m)
        if not has_rows(fp):
            if entry is not None or m in clean_months:
                orphans.add(m)
            continue
        if entry is None:
            reasons[m] = "new"
        elif m not in clean_months:
            reasons[m] = "missing_partition"
        elif entry.get("orphan"):
            reasons[m] = "input_returned"
        elif entry.get("fingerprint") != fp:
            reasons[m] = "changed"
    # Tháng trong trạng thái mà không còn thư mục đầu vào nào có thể chứa nó.
    for m, entry in known.items():
        if m in reasons:
            continue
        if entry.get("orphan") or m not in possible:
            orphans.add(m)
    rebuild = sorted(reasons)
    return Plan(mode, rebuild, reasons, compared, sorted(orphans - set(rebuild)))


def _entry(fp: Mapping[str, Any], clean_rows: int, run_id: str, now_iso: str) -> dict[str, Any]:
    return {
        "fingerprint": dict(fp),
        "clean_rows": int(clean_rows),
        "built_at": now_iso,
        "run_id": run_id,
    }


def next_state(
    prev: Mapping[str, Any] | None,
    plan: Plan,
    fingerprints: Mapping[str, Mapping[str, Any]],
    clean_rows: Mapping[str, int],
    *,
    horizon_ms: int,
    probe_ms: int,
    dirs: Mapping[str, Iterable[DirInfo]],
    run_id: str,
    now_iso: str,
) -> dict[str, Any] | None:
    """Trạng thái sau lượt chạy thành công. None: không được ghi (dates khi chưa có trạng thái).

    - full: viết lại toàn bộ; tháng mồ côi giữ dạng {"orphan": true}.
    - incremental: cập nhật tháng dựng lại, đánh dấu mồ côi, dời horizon/inputs.
    - dates: chỉ cập nhật tháng đã ép; KHÔNG dời horizon (các tháng khác chưa được kiểm).
    """
    if plan.mode == MODE_DATES and prev is None:
        return None
    months: dict[str, dict[str, Any]] = {}
    if plan.mode != MODE_FULL and prev is not None:
        months = {m: dict(e) for m, e in prev["months"].items()}
    for m in plan.rebuild:
        months[m] = _entry(fingerprints[m], clean_rows.get(m, 0), run_id, now_iso)
    for m in plan.orphans:
        e = months.setdefault(m, {})
        e["orphan"] = True
    for m in plan.rebuild:
        months[m].pop("orphan", None)
    if plan.mode == MODE_DATES:
        horizon, probe, inputs = prev["horizon_ms"], prev.get("probe_ms"), prev["inputs"]
    else:
        horizon, probe = int(horizon_ms), int(probe_ms)
        inputs = {o: sorted(d.name for d in dirs.get(o, ())) for o in ORIGINS}
    return {
        "version": STATE_VERSION,
        "built_at": now_iso,
        "run_id": run_id,
        "mode": plan.mode,
        "probe_ms": probe,
        "horizon_ms": horizon,
        "inputs": inputs,
        "months": dict(sorted(months.items())),
    }


def merge_state(
    disk: Mapping[str, Any] | None, ours: Mapping[str, Any], plan: Plan
) -> dict[str, Any]:
    """Gộp với trạng thái đọc lại ngay trước khi ghi (một lượt khác có thể vừa cập nhật).

    Tháng lượt này dựng lại/đánh dấu mồ côi lấy bản của mình; tháng khác lấy bản trên đĩa.
    Horizon + inputs lấy từ bản có horizon lớn hơn. full thì ghi đè hẳn.
    Sai lệch có thể có của phép gộp chỉ làm fingerprint cũ hơn dữ liệu clean -> lượt sau dựng
    lại thừa, không bao giờ bỏ sót.
    """
    if disk is None or plan.mode == MODE_FULL:
        return dict(ours)
    touched = set(plan.rebuild) | set(plan.orphans)
    months = {m: dict(e) for m, e in disk["months"].items()}
    for m in touched:
        if m in ours["months"]:
            months[m] = dict(ours["months"][m])
    out = dict(ours)
    out["months"] = dict(sorted(months.items()))
    if int(disk["horizon_ms"]) > int(ours["horizon_ms"]):
        out["horizon_ms"] = disk["horizon_ms"]
        out["probe_ms"] = disk.get("probe_ms")
        out["inputs"] = disk["inputs"]
    return out


def needs_state_write(
    plan: Plan,
    prev: Mapping[str, Any] | None,
    candidates: Mapping[str, Any],
    dirs: Mapping[str, Iterable[DirInfo]],
) -> bool:
    """Khi không dựng lại tháng nào: có cần ghi trạng thái (dời horizon/đánh dấu mồ côi)?"""
    if plan.rebuild or plan.mode == MODE_FULL:
        return True
    if prev is None:
        return False
    if plan.mode == MODE_DATES:
        return any(not prev["months"].get(m, {}).get("orphan") for m in plan.orphans)
    if candidates:
        return True
    names = {o: sorted(d.name for d in dirs.get(o, ())) for o in ORIGINS}
    if any(sorted(prev["inputs"].get(o, [])) != names[o] for o in ORIGINS):
        return True
    return any(not prev["months"].get(m, {}).get("orphan") for m in plan.orphans)


def now_iso(now: dt.datetime | None = None) -> str:
    return (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc).isoformat()
