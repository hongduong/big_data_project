"""Khoá tư vấn (advisory) đọc/ghi lưu dạng file nhỏ trên HDFS.

Mục đích: job batch ghi đè phân vùng của một bảng không được chạy khi job khác đang đọc bảng đó.

Bố cục: `<base>/<name>/writer.lock` (một file cố định -> loại trừ giữa các writer) và
`<base>/<name>/reader-<id>.lock` (mỗi reader một file). Nội dung file là JSON
{kind, id, owner, acquired_at, expires_at, ttl_s}.

Hết hạn: file có `expires_at` đã qua coi như không ai giữ và bị xoá bởi bên phát hiện đầu tiên
(kèm cảnh báo). File hỏng/rỗng/không parse được: KHÔNG biết hạn nên coi là đang giữ trong
`CORRUPT_GRACE_S` giây kể từ lần đầu tiến trình này nhìn thấy (file mới tạo có thể đang được ghi
dở); quá hạn ân hạn mà vẫn hỏng thì coi là mồ côi và xoá. Việc xoá file hết hạn kiểm lại nội
dung ngay trước khi xoá nhưng vẫn còn cửa sổ race rất nhỏ (khoá hết hạn vốn là tình huống hiếm).

TTL ngắn + heartbeat: mặc định khoá ghi sống 5 phút, khoá đọc 10 phút. Khi giữ khoá, một luồng
daemon `lock-heartbeat-<name>` gọi `refresh()` mỗi `ttl_s / 3` giây nên bên giữ còn sống không
bao giờ mất khoá dù làm lâu; tiến trình chết (kể cả SIGKILL) thì heartbeat dừng và bảng được
giải phóng sau tối đa một TTL.

Chỉ dùng thư viện chuẩn; không import pyspark.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from stocklake.common.logging import get_logger

log = get_logger("stocklake.common.locks")

CORRUPT_GRACE_S = 60.0  # ân hạn cho file khoá hỏng (xem docstring module)
DEFAULT_WRITE_TTL_S = 300.0  # writer đo được giữ 6–26 s
DEFAULT_READ_TTL_S = 600.0  # reader (DQ) đo được giữ 30–40 s
# Phải lớn hơn cả hai TTL: bên chờ luôn sống lâu hơn một bên giữ đã chết.
DEFAULT_WAIT_TIMEOUT_S = 900.0
HEARTBEAT_JOIN_S = 5.0  # chờ tối đa luồng heartbeat khi thoát; không treo job
_NAME_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")


class LockTimeout(TimeoutError):
    """Chờ khoá quá `wait_timeout_s`."""


class LockLost(RuntimeError):
    """Khoá của mình đã bị xoá/chiếm (vd hết hạn) khi gọi refresh()."""


class LockFs(Protocol):
    """Giao diện filesystem tối thiểu cho khoá."""

    def mkdirs(self, path: str) -> None: ...

    def create_exclusive(self, path: str, data: bytes) -> bool:
        """Tạo file nguyên tử; đã có -> False, tạo được -> True."""
        ...

    def read(self, path: str) -> bytes | None: ...

    def list(self, dir_path: str) -> list[str]:
        """Đường dẫn đầy đủ các mục; [] nếu thư mục không có."""
        ...

    def delete(self, path: str) -> None: ...

    # Tuỳ chọn (không bắt buộc): overwrite(path, data) -> None, dùng cho refresh().


class MemoryLockFs:
    """LockFs trong bộ nhớ, an toàn đa luồng; dùng cho test."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self._mu = threading.Lock()

    def mkdirs(self, path: str) -> None:
        pass

    def create_exclusive(self, path: str, data: bytes) -> bool:
        with self._mu:
            if path in self.files:
                return False
            self.files[path] = data
            return True

    def overwrite(self, path: str, data: bytes) -> None:
        with self._mu:
            self.files[path] = data

    def read(self, path: str) -> bytes | None:
        with self._mu:
            return self.files.get(path)

    def list(self, dir_path: str) -> list[str]:
        prefix = dir_path.rstrip("/") + "/"
        with self._mu:
            return sorted(
                p for p in self.files if p.startswith(prefix) and "/" not in p[len(prefix) :]
            )

    def delete(self, path: str) -> None:
        with self._mu:
            self.files.pop(path, None)


class _HadoopLockFs:
    """Adapter LockFs trên Hadoop FileSystem của JVM (qua py4j của SparkSession)."""

    def __init__(self, spark: Any) -> None:
        self._jvm = spark._jvm
        self._conf = spark._jsc.hadoopConfiguration()

    def _path(self, p: str) -> Any:
        return self._jvm.org.apache.hadoop.fs.Path(p)

    def _fs(self, p: str) -> Any:
        return self._path(p).getFileSystem(self._conf)

    @staticmethod
    def _is(exc: Exception, *needles: str) -> bool:
        text = str(exc)
        return any(n in text for n in needles)

    def mkdirs(self, path: str) -> None:
        self._fs(path).mkdirs(self._path(path))

    def _write(self, path: str, data: bytes, overwrite: bool) -> None:
        out = self._fs(path).create(self._path(path), overwrite)
        try:
            out.write(bytearray(data))
        finally:
            out.close()

    def create_exclusive(self, path: str, data: bytes) -> bool:
        try:
            out = self._fs(path).create(self._path(path), False)
        except Exception as exc:  # noqa: BLE001 - py4j ném Py4JJavaError, không import được ở đây
            if self._is(exc, "FileAlreadyExistsException", "already exists"):
                return False
            raise
        try:
            out.write(bytearray(data))
        except BaseException:
            try:
                out.close()
            finally:
                self.delete(path)  # không để lại file rỗng/dở dang chặn người khác
            raise
        out.close()
        return True

    def overwrite(self, path: str, data: bytes) -> None:
        self._write(path, data, True)

    def read(self, path: str) -> bytes | None:
        fs = self._fs(path)
        p = self._path(path)
        try:
            if not fs.exists(p):
                return None
            stream = fs.open(p)
            try:
                return bytes(self._jvm.org.apache.hadoop.io.IOUtils.readFullyToByteArray(stream))
            finally:
                stream.close()
        except Exception as exc:  # noqa: BLE001
            if self._is(exc, "FileNotFoundException"):  # bị xoá giữa chừng
                return None
            raise

    def list(self, dir_path: str) -> list[str]:
        fs = self._fs(dir_path)
        p = self._path(dir_path)
        try:
            if not fs.exists(p):
                return []
            statuses = fs.listStatus(p)
        except Exception as exc:  # noqa: BLE001
            if self._is(exc, "FileNotFoundException"):
                return []
            raise
        return sorted(str(st.getPath().toUri().getPath()) for st in statuses)

    def delete(self, path: str) -> None:
        self._fs(path).delete(self._path(path), False)


def hadoop_fs(spark: Any) -> LockFs:
    """Adapter LockFs qua Hadoop FileSystem của SparkSession (không import pyspark)."""
    return _HadoopLockFs(spark)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _validate_name(name: str) -> None:
    if not name or name in {".", ".."} or any(c not in _NAME_CHARS for c in name):
        raise ValueError(f"Tên khoá không hợp lệ: {name!r} (chỉ chữ, số, '.', '_', '-')")


class LockHandle:
    """Tay cầm khoá đang giữ.

    `lost`: True khi một lần gia hạn thấy khoá đã mất/bị chiếm (thân `with` KHÔNG bị ngắt; bên
    gọi tự kiểm sau đoạn găng). `refreshes`: số lần gia hạn thành công. `last_refresh_error`:
    lỗi gần nhất khi gia hạn (None nếu chưa lỗi).
    """

    def __init__(
        self, fs: LockFs, kind: str, id: str, path: str, owner: str, ttl_s: float, clock: Callable
    ) -> None:
        self._fs = fs
        self._clock = clock
        self.kind = kind
        self.id = id
        self.path = path
        self.owner = owner
        self.ttl_s = ttl_s
        self.waited_s = 0.0
        self.acquired_at: datetime = clock()
        self.lost = False
        self.refreshes = 0
        self.last_refresh_error: str | None = None
        # refresh() và _release() loại trừ nhau (heartbeat chạy ở luồng khác)
        self._mu = threading.Lock()
        self._released = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _payload(self) -> bytes:
        return _payload(self.kind, self.id, self.owner, self.acquired_at, self.ttl_s)

    def refresh(self) -> None:
        """Gia hạn thêm `ttl_s` kể từ bây giờ. Ném LockLost nếu khoá không còn là của mình."""
        with self._mu:
            if self._released:  # đã bắt đầu nhả: không gia hạn, không tạo lại file
                raise LockLost(f"Khoá {self.path} đã nhả")
            info = _parse(self._fs.read(self.path))
            if info is None or info["id"] != self.id:
                self.lost = True
                raise LockLost(f"Khoá {self.path} không còn thuộc {self.id}")
            now = self._clock()
            data = _payload(self.kind, self.id, self.owner, self.acquired_at, self.ttl_s, now)
            overwrite = getattr(self._fs, "overwrite", None)
            if overwrite is not None:
                overwrite(self.path, data)
            else:
                # Fallback: xoá rồi tạo lại (có cửa sổ ngắn không có file)
                self._fs.delete(self.path)
                if not self._fs.create_exclusive(self.path, data):
                    self.lost = True
                    raise LockLost(f"Khoá {self.path} bị chiếm khi gia hạn")
            self.refreshes += 1

    def _heartbeat(self, interval_s: float) -> None:
        # Nhịp = ttl/3: lỡ một nhịp (lỗi tạm thời) vẫn còn hai lần thử trước khi khoá hết hạn.
        while not self._stop.wait(interval_s):
            try:
                self.refresh()
            except LockLost as exc:
                if self._released or self._stop.is_set():
                    return  # đang nhả: không phải mất khoá
                log.error("Mất khoá %s, dừng heartbeat: %s", self.path, exc)
                return
            except Exception as exc:  # noqa: BLE001 - lỗi IO tạm thời (NameNode chập chờn)
                self.last_refresh_error = f"{type(exc).__name__}: {exc}"
                log.warning("Gia hạn khoá %s lỗi, thử lại nhịp sau: %s", self.path, exc)

    def _start_heartbeat(self, name: str, interval_s: float) -> None:
        self._thread = threading.Thread(
            target=self._heartbeat, args=(interval_s,), name=f"lock-heartbeat-{name}", daemon=True
        )
        self._thread.start()

    def _stop_heartbeat(self) -> None:
        self._stop.set()
        th = self._thread
        if th is not None and th is not threading.current_thread():
            th.join(HEARTBEAT_JOIN_S)
            if th.is_alive():  # refresh đang treo ở fs; _release sẽ chờ nó qua _mu
                log.warning("Heartbeat khoá %s chưa dừng sau %.0fs", self.path, HEARTBEAT_JOIN_S)

    def _release(self) -> None:
        self._stop_heartbeat()  # dừng heartbeat TRƯỚC khi xoá file
        with self._mu:
            self._released = True
            info = _parse(self._fs.read(self.path))
            if info is not None and info["id"] == self.id:
                self._fs.delete(self.path)
            else:
                log.warning("Khoá %s không còn của %s, bỏ qua khi nhả", self.path, self.id)


def _payload(
    kind: str,
    id: str,
    owner: str,
    acquired_at: datetime,
    ttl_s: float,
    from_time: datetime | None = None,
) -> bytes:
    base = from_time or acquired_at
    doc = {
        "kind": kind,
        "id": id,
        "owner": owner,
        "acquired_at": _iso(acquired_at),
        "expires_at": _iso(base + timedelta(seconds=ttl_s)),
        "ttl_s": ttl_s,
    }
    return json.dumps(doc).encode()


def _parse(data: bytes | None) -> dict | None:
    """Parse nội dung khoá; None nếu thiếu/hỏng."""
    if not data:
        return None
    try:
        doc = json.loads(data.decode())
        if not isinstance(doc, dict) or not isinstance(doc.get("id"), str):
            return None
        exp = datetime.fromisoformat(doc["expires_at"])
        if exp.tzinfo is None:
            return None
        doc["_expires"] = exp
        return doc
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        return None


class _Waiter:
    """Trạng thái một lần chờ: hạn chót, dò file khoá, ngủ."""

    def __init__(self, fs, clock, sleep, wait_timeout_s, poll_s, label):
        self.fs = fs
        self.clock = clock
        self._sleep = sleep
        self.poll_s = poll_s
        self.label = label
        self.start = clock()
        self.deadline = self.start + timedelta(seconds=wait_timeout_s)
        self._corrupt_seen: dict[str, datetime] = {}

    def inspect(self, path: str) -> tuple[str, dict | None, bytes | None]:
        """Trả ("missing"|"live"|"stale", info, raw). File stale đã được xoá khi trả về."""
        raw = self.fs.read(path)
        if raw is None:
            self._corrupt_seen.pop(path, None)
            return "missing", None, None
        info = _parse(raw)
        now = self.clock()
        if info is None:
            first = self._corrupt_seen.setdefault(path, now)
            if (now - first).total_seconds() < CORRUPT_GRACE_S:
                return "live", {"owner": "?", "acquired_at": "?", "expires_at": "?"}, raw
            log.warning("Khoá hỏng %s quá %.0fs, coi là mồ côi và xoá", path, CORRUPT_GRACE_S)
            self._remove_if_same(path, raw)
            return "stale", None, raw
        self._corrupt_seen.pop(path, None)
        if info["_expires"] <= now:
            log.warning(
                "Khoá hết hạn %s (owner=%s, acquired_at=%s, expires_at=%s), xoá",
                path,
                info.get("owner"),
                info.get("acquired_at"),
                info.get("expires_at"),
            )
            self._remove_if_same(path, raw)
            return "stale", info, raw
        return "live", info, raw

    def _remove_if_same(self, path: str, raw: bytes) -> None:
        if self.fs.read(path) == raw:  # tránh xoá nhầm khoá mới của người khác
            self.fs.delete(path)

    def wait(self, holders: list[str]) -> None:
        """Ngủ một nhịp hoặc ném LockTimeout nếu hết hạn chót."""
        now = self.clock()
        if now >= self.deadline:
            waited = (now - self.start).total_seconds()
            raise LockTimeout(
                f"Hết {waited:.0f}s chờ {self.label}; đang bị giữ bởi: " + "; ".join(holders)
            )
        remaining = (self.deadline - now).total_seconds()
        self._sleep(min(self.poll_s, remaining))

    def waited_s(self) -> float:
        return (self.clock() - self.start).total_seconds()


def _fmt(info: dict | None, path: str) -> str:
    info = info or {}
    return (
        f"{path} (owner={info.get('owner', '?')}, acquired_at={info.get('acquired_at', '?')}, "
        f"expires_at={info.get('expires_at', '?')})"
    )


def _is_reader(path: str) -> bool:
    base = path.rsplit("/", 1)[-1]
    return base.startswith("reader-") and base.endswith(".lock")


def _default_owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _check_timing(
    label: str, ttl_s: float, wait_timeout_s: float, waits_for_ttl_s: float, hb: float | None
) -> float:
    """Kiểm tham số thời gian; trả nhịp heartbeat."""
    if ttl_s <= 0:
        raise ValueError(f"{label}: ttl_s phải > 0 (nhận {ttl_s})")
    if wait_timeout_s <= 0:
        raise ValueError(f"{label}: wait_timeout_s phải > 0 (nhận {wait_timeout_s})")
    if wait_timeout_s <= waits_for_ttl_s:
        log.warning(
            "%s: wait_timeout_s=%.0f <= TTL %.0f của khoá phải chờ; holder chết có thể làm hết giờ",
            label,
            wait_timeout_s,
            waits_for_ttl_s,
        )
    interval = ttl_s / 3 if hb is None else hb
    if interval <= 0:
        raise ValueError(f"{label}: heartbeat_interval_s phải > 0 (nhận {hb})")
    return interval


@contextmanager
def read_lock(
    fs: LockFs,
    name: str,
    *,
    owner: str | None = None,
    ttl_s: float = DEFAULT_READ_TTL_S,
    wait_timeout_s: float = DEFAULT_WAIT_TIMEOUT_S,
    poll_s: float = 2.0,
    base: str = "/locks",
    clock: Callable[[], datetime] | None = None,
    sleep: Callable[[float], None] | None = None,
    heartbeat: bool = True,
    heartbeat_interval_s: float | None = None,
) -> Iterator[LockHandle]:
    """Khoá đọc: nhiều reader cùng giữ; nhường writer đang giữ hoặc đang chờ.

    `heartbeat=True`: luồng nền gia hạn khoá mỗi `heartbeat_interval_s` (mặc định ttl_s/3).
    """
    _validate_name(name)
    # reader chỉ chờ writer
    hb_s = _check_timing(
        f"read_lock {name}", ttl_s, wait_timeout_s, DEFAULT_WRITE_TTL_S, heartbeat_interval_s
    )
    clock = clock or _now
    owner = owner or _default_owner()
    d = f"{base.rstrip('/')}/{name}"
    wpath = f"{d}/writer.lock"
    lid = uuid.uuid4().hex
    rpath = f"{d}/reader-{lid}.lock"
    w = _Waiter(fs, clock, sleep or time.sleep, wait_timeout_s, poll_s, f"read_lock {name}")
    fs.mkdirs(d)
    handle = LockHandle(fs, "reader", lid, rpath, owner, ttl_s, clock)
    waiting_logged = False
    while True:
        state, info, _ = w.inspect(wpath)
        if state != "live":
            handle.acquired_at = clock()
            if fs.create_exclusive(rpath, handle._payload()):
                state2, info2, _ = w.inspect(wpath)  # kiểm lại: writer có xuất hiện không?
                if state2 != "live":
                    break
                fs.delete(rpath)
                info = info2
                state = state2
            else:  # id là uuid nên gần như không xảy ra
                continue
        if not waiting_logged:
            log.info("read_lock %s: chờ writer %s", name, _fmt(info, wpath))
            waiting_logged = True
        w.wait([_fmt(info, wpath)])
    handle.waited_s = w.waited_s()
    log.info("read_lock %s: đã giữ %s sau %.1fs chờ", name, lid, handle.waited_s)
    if heartbeat:
        handle._start_heartbeat(name, hb_s)
    try:
        yield handle
    finally:
        handle._release()
        log.info("read_lock %s: đã nhả %s", name, lid)


@contextmanager
def write_lock(
    fs: LockFs,
    name: str,
    *,
    owner: str | None = None,
    ttl_s: float = DEFAULT_WRITE_TTL_S,
    wait_timeout_s: float = DEFAULT_WAIT_TIMEOUT_S,
    poll_s: float = 2.0,
    base: str = "/locks",
    clock: Callable[[], datetime] | None = None,
    sleep: Callable[[float], None] | None = None,
    heartbeat: bool = True,
    heartbeat_interval_s: float | None = None,
) -> Iterator[LockHandle]:
    """Khoá ghi: độc quyền; giành writer.lock trước (chặn reader mới) rồi chờ reader cũ xong.

    `heartbeat=True`: luồng nền gia hạn writer.lock mỗi `heartbeat_interval_s` (mặc định
    ttl_s/3), bắt đầu ngay khi giành được writer.lock (cả lúc còn chờ reader).
    """
    _validate_name(name)
    # writer chờ cả writer khác lẫn reader
    waits_for = max(ttl_s, DEFAULT_WRITE_TTL_S, DEFAULT_READ_TTL_S)
    hb_s = _check_timing(
        f"write_lock {name}", ttl_s, wait_timeout_s, waits_for, heartbeat_interval_s
    )
    clock = clock or _now
    owner = owner or _default_owner()
    d = f"{base.rstrip('/')}/{name}"
    wpath = f"{d}/writer.lock"
    lid = uuid.uuid4().hex
    w = _Waiter(fs, clock, sleep or time.sleep, wait_timeout_s, poll_s, f"write_lock {name}")
    fs.mkdirs(d)
    handle = LockHandle(fs, "writer", lid, wpath, owner, ttl_s, clock)
    # Pha 1: giành writer.lock
    waiting_logged = False
    while True:
        handle.acquired_at = clock()
        if fs.create_exclusive(wpath, handle._payload()):
            break
        state, info, _ = w.inspect(wpath)
        if state != "live":
            continue  # đã xoá khoá hết hạn hoặc vừa biến mất -> thử lại ngay
        if not waiting_logged:
            log.info("write_lock %s: chờ writer %s", name, _fmt(info, wpath))
            waiting_logged = True
        w.wait([_fmt(info, wpath)])  # chưa tạo file nên không cần dọn
    # Pha 2: chờ reader còn sống; heartbeat chạy luôn để writer.lock không hết hạn khi chờ lâu
    if heartbeat:
        handle._start_heartbeat(name, hb_s)
    try:
        waiting_logged = False
        while True:
            holders: list[str] = []
            for p in fs.list(d):
                if not _is_reader(p):
                    continue
                state, info, _ = w.inspect(p)
                if state == "live":
                    holders.append(_fmt(info, p))
            if not holders:
                break
            if not waiting_logged:
                log.info("write_lock %s: chờ %d reader: %s", name, len(holders), "; ".join(holders))
                waiting_logged = True
            w.wait(holders)
        if handle.lost:  # writer.lock bị chiếm khi đang chờ reader: không được ghi
            raise LockLost(f"write_lock {name}: mất {wpath} khi chờ reader")
    except BaseException:
        handle._release()
        raise
    handle.waited_s = w.waited_s()
    log.info("write_lock %s: đã giữ %s sau %.1fs chờ", name, lid, handle.waited_s)
    try:
        yield handle
    finally:
        handle._release()
        log.info("write_lock %s: đã nhả %s", name, lid)
