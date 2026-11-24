import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from stocklake.common import locks
from stocklake.common.locks import (
    LockTimeout,
    MemoryLockFs,
    read_lock,
    write_lock,
)

W = "/locks/t/writer.lock"
D = "/locks/t"


class FakeTime:
    """Đồng hồ giả; sleep chỉ nhích đồng hồ, có thể gắn hook chạy mỗi lần ngủ."""

    def __init__(self):
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.hooks = []
        self.sleeps = 0

    def clock(self):
        return self.now

    def sleep(self, s):
        self.sleeps += 1
        self.now += timedelta(seconds=s)
        if self.hooks:
            self.hooks.pop(0)()


def kw(t, **extra):
    return dict(clock=t.clock, sleep=t.sleep, poll_s=1.0, wait_timeout_s=10.0, **extra)


def lock_doc(kind, id_, now, ttl=100.0, owner="x"):
    return json.dumps(
        {
            "kind": kind,
            "id": id_,
            "owner": owner,
            "acquired_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=ttl)).isoformat(),
            "ttl_s": ttl,
        }
    ).encode()


def test_two_readers_together():
    fs, t = MemoryLockFs(), FakeTime()
    with read_lock(fs, "t", **kw(t)) as a, read_lock(fs, "t", **kw(t)) as b:
        assert a.id != b.id
        assert len(fs.list(D)) == 2
        assert a.waited_s == 0
    assert fs.list(D) == []


def test_invalid_name():
    fs, t = MemoryLockFs(), FakeTime()
    for bad in ["", "a/b", "a b", "..", "x*"]:
        with pytest.raises(ValueError), read_lock(fs, bad, **kw(t)):
            pass
        with pytest.raises(ValueError), write_lock(fs, bad, **kw(t)):
            pass
    with write_lock(fs, "clean.intraday_1m-x", **kw(t)):
        pass


def test_writer_waits_for_reader_then_proceeds():
    fs, t = MemoryLockFs(), FakeTime()
    with read_lock(fs, "t", **kw(t)) as r:
        t.hooks.append(lambda: None)
        t.hooks.append(lambda: fs.delete(r.path))  # reader nhả sau 2 nhịp
        with write_lock(fs, "t", **kw(t)) as w:
            assert w.waited_s == 2.0
            assert fs.read(W) is not None
    assert fs.list(D) == []


def test_reader_waits_while_writer_holds():
    fs, t = MemoryLockFs(), FakeTime()
    with write_lock(fs, "t", **kw(t)) as w:
        t.hooks.append(lambda: fs.delete(w.path))
        with read_lock(fs, "t", **kw(t)) as r:
            assert r.waited_s == 1.0
    assert fs.list(D) == []


def test_reader_blocked_by_waiting_writer_priority():
    fs, t = MemoryLockFs(), FakeTime()
    with read_lock(fs, "t", **kw(t)) as r1:
        seen = {}

        def writer_appears_then_new_reader_tries():
            # writer đang chờ r1 (đã tạo writer.lock) -> reader mới phải bị chặn
            assert fs.read(W) is not None
            with pytest.raises(LockTimeout):
                with read_lock(fs, "t", **kw(t, owner="r2")):
                    seen["got"] = True
            r1_release()

        def r1_release():
            fs.delete(r1.path)

        t.hooks.append(writer_appears_then_new_reader_tries)
        with write_lock(fs, "t", **kw(t)):
            pass
    assert "got" not in seen
    assert fs.list(D) == []


def test_second_writer_waits_for_first():
    fs, t = MemoryLockFs(), FakeTime()
    with write_lock(fs, "t", **kw(t)) as w1:
        t.hooks.append(lambda: fs.delete(w1.path))
        with write_lock(fs, "t", **kw(t)) as w2:
            assert w2.waited_s == 1.0
            assert fs.read(W) is not None
    assert fs.list(D) == []


def test_expired_writer_is_taken_over(caplog):
    fs, t = MemoryLockFs(), FakeTime()
    fs.files[W] = lock_doc("writer", "old", t.now - timedelta(seconds=500), ttl=100, owner="dead")
    with write_lock(fs, "t", **kw(t)) as w:
        assert json.loads(fs.read(W))["id"] == w.id
        assert w.waited_s == 0
    assert fs.list(D) == []


def test_expired_writer_does_not_block_reader():
    fs, t = MemoryLockFs(), FakeTime()
    fs.files[W] = lock_doc("writer", "old", t.now - timedelta(seconds=500), ttl=100)
    with read_lock(fs, "t", **kw(t)):
        assert fs.read(W) is None


def test_expired_reader_does_not_block_writer():
    fs, t = MemoryLockFs(), FakeTime()
    old = f"{D}/reader-old.lock"
    fs.files[old] = lock_doc("reader", "old", t.now - timedelta(seconds=500), ttl=100)
    with write_lock(fs, "t", **kw(t)):
        assert fs.read(old) is None


def test_timeout_leaves_no_own_file_and_lists_holders():
    fs, t = MemoryLockFs(), FakeTime()
    with read_lock(fs, "t", **kw(t, owner="reader-A")):
        before = set(fs.files)
        with pytest.raises(LockTimeout, match="reader-A"):
            with write_lock(fs, "t", **kw(t)):
                pass
        assert set(fs.files) == before  # writer.lock của ta đã bị xoá
    with write_lock(fs, "t", **kw(t, owner="writer-B")) as w:
        before = set(fs.files)
        with pytest.raises(LockTimeout, match="writer-B"):
            with write_lock(fs, "t", **kw(t)):
                pass
        with pytest.raises(LockTimeout, match="writer-B"):
            with read_lock(fs, "t", **kw(t)):
                pass
        assert set(fs.files) == before == {w.path}
    assert isinstance(LockTimeout("x"), TimeoutError)


def test_release_does_not_delete_takeover_file():
    fs, t = MemoryLockFs(), FakeTime()
    with write_lock(fs, "t", **kw(t)):
        # khoá của ta bị coi là hết hạn và người khác chiếm
        fs.files[W] = lock_doc("writer", "other", t.now, ttl=100)
    assert json.loads(fs.read(W))["id"] == "other"


def test_refresh_extends_expiry_and_detects_loss():
    fs, t = MemoryLockFs(), FakeTime()
    with write_lock(fs, "t", ttl_s=100.0, **kw(t)) as w:
        t.now += timedelta(seconds=60)
        w.refresh()
        exp = datetime.fromisoformat(json.loads(fs.read(W))["expires_at"])
        assert exp == t.now + timedelta(seconds=100)
        fs.files[W] = lock_doc("writer", "other", t.now)
        with pytest.raises(locks.LockLost):
            w.refresh()


def test_refresh_fallback_without_overwrite():
    class NoOverwrite(MemoryLockFs):
        overwrite = None

    fs, t = NoOverwrite(), FakeTime()
    with read_lock(fs, "t", ttl_s=100.0, **kw(t)) as r:
        t.now += timedelta(seconds=60)
        r.refresh()
        exp = datetime.fromisoformat(json.loads(fs.read(r.path))["expires_at"])
        assert exp == t.now + timedelta(seconds=100)


@pytest.mark.parametrize("junk", [b"", b"{not json", b'{"id": "x"}', b"[1]"])
def test_corrupt_writer_blocks_during_grace_then_removed(junk):
    fs, t = MemoryLockFs(), FakeTime()
    fs.files[W] = junk
    with pytest.raises(LockTimeout):  # trong ân hạn: coi là đang giữ
        with read_lock(fs, "t", **kw(t)):
            pass
    assert fs.read(W) == junk
    fs2, t2 = MemoryLockFs(), FakeTime()
    fs2.files[W] = junk
    with write_lock(fs2, "t", **dict(kw(t2), wait_timeout_s=locks.CORRUPT_GRACE_S + 10)):
        assert json.loads(fs2.read(W))["kind"] == "writer"


def test_corrupt_reader_removed_after_grace():
    fs, t = MemoryLockFs(), FakeTime()
    bad = f"{D}/reader-bad.lock"
    fs.files[bad] = b"garbage"
    with write_lock(fs, "t", **dict(kw(t), wait_timeout_s=200.0)) as w:
        assert fs.read(bad) is None
        assert w.waited_s >= locks.CORRUPT_GRACE_S


def test_reader_recheck_race_writer_appears_between_create_and_check():
    class Racy(MemoryLockFs):
        def __init__(self, t):
            super().__init__()
            self.t = t
            self.injected = False

        def create_exclusive(self, path, data):
            ok = super().create_exclusive(path, data)
            if ok and "reader-" in path and not self.injected:
                self.injected = True  # writer chen vào ngay sau khi reader tạo file
                self.files[W] = lock_doc("writer", "w1", self.t.now, ttl=100)
            return ok

    t = FakeTime()
    fs = Racy(t)
    t.hooks.append(lambda: fs.delete(W))
    with read_lock(fs, "t", **kw(t)) as r:
        assert r.waited_s == 1.0
        assert fs.read(W) is None
        assert fs.read(r.path) is not None
        assert len(fs.list(D)) == 1  # file reader lần đầu đã bị xoá
    assert fs.list(D) == []


def test_exception_in_body_releases():
    fs, t = MemoryLockFs(), FakeTime()
    with pytest.raises(RuntimeError):
        with write_lock(fs, "t", **kw(t)):
            raise RuntimeError("x")
    assert fs.list(D) == []


def test_memory_fs_basics():
    fs = MemoryLockFs()
    assert fs.list("/a") == []
    assert fs.create_exclusive("/a/x", b"1") is True
    assert fs.create_exclusive("/a/x", b"2") is False
    assert fs.read("/a/x") == b"1"
    assert fs.read("/a/y") is None
    assert fs.list("/a") == ["/a/x"]
    fs.delete("/a/x")
    fs.delete("/a/x")
    assert fs.list("/a") == []


def test_threads_writer_excludes_readers_real_sleep():
    fs = MemoryLockFs()
    state = {"readers": 0, "writing": False, "violation": False}
    mu = threading.Lock()
    real = dict(poll_s=0.002, wait_timeout_s=10.0)

    def reader():
        with read_lock(fs, "t", **real):
            with mu:
                state["readers"] += 1
                if state["writing"]:
                    state["violation"] = True
            time.sleep(0.01)
            with mu:
                state["readers"] -= 1

    def writer():
        with write_lock(fs, "t", **real):
            with mu:
                state["writing"] = True
                if state["readers"]:
                    state["violation"] = True
            time.sleep(0.01)
            with mu:
                state["writing"] = False

    ts = [threading.Thread(target=reader) for _ in range(4)]
    ts += [threading.Thread(target=writer) for _ in range(2)]
    for th in ts:
        th.start()
    for th in ts:
        th.join(20)
    assert not any(th.is_alive() for th in ts)
    assert not state["violation"]
    assert fs.list(D) == []


def test_threads_two_writers_mutual_exclusion():
    fs = MemoryLockFs()
    inside = []
    bad = []

    def w():
        with write_lock(fs, "t", poll_s=0.002, wait_timeout_s=10.0):
            inside.append(1)
            if len(inside) > 1:
                bad.append(1)
            time.sleep(0.01)
            inside.pop()

    ts = [threading.Thread(target=w) for _ in range(3)]
    for th in ts:
        th.start()
    for th in ts:
        th.join(20)
    assert not bad
    assert fs.list(D) == []


# --- TTL ngắn + heartbeat (luồng thật, nhịp vài chục ms; assert rộng để không flaky) ---


def hb_threads(name="t"):
    return [
        th for th in threading.enumerate() if th.name == f"lock-heartbeat-{name}" and th.is_alive()
    ]


def expires(fs, path):
    return datetime.fromisoformat(json.loads(fs.read(path))["expires_at"])


class Capture:
    """Bắt log của module (logger stocklake không propagate nên caplog không thấy)."""

    def __init__(self):
        import logging

        self.records = []
        self.h = logging.Handler()
        self.h.emit = self.records.append

    def __enter__(self):
        locks.log.addHandler(self.h)
        return self

    def __exit__(self, *a):
        locks.log.removeHandler(self.h)

    def levels(self, level):
        return [r.getMessage() for r in self.records if r.levelname == level]


def test_defaults_short_ttl():
    import inspect

    assert locks.DEFAULT_WRITE_TTL_S == 300.0
    assert locks.DEFAULT_READ_TTL_S == 600.0
    assert locks.DEFAULT_WAIT_TIMEOUT_S == 900.0
    assert locks.DEFAULT_WAIT_TIMEOUT_S > max(locks.DEFAULT_WRITE_TTL_S, locks.DEFAULT_READ_TTL_S)
    for fn, ttl in [(write_lock, 300.0), (read_lock, 600.0)]:
        p = inspect.signature(fn.__wrapped__).parameters
        assert p["ttl_s"].default == ttl
        assert p["wait_timeout_s"].default == 900.0
        assert p["heartbeat"].default is True
        assert p["heartbeat_interval_s"].default is None
    fs = MemoryLockFs()
    with write_lock(fs, "t") as w:
        assert w.ttl_s == 300.0
        exp = expires(fs, W) - datetime.fromisoformat(json.loads(fs.read(W))["acquired_at"])
        assert exp == timedelta(seconds=300)
    with read_lock(fs, "t") as r:
        assert r.ttl_s == 600.0
    assert not hb_threads()


def test_timing_validation_and_warning():
    fs = MemoryLockFs()
    with pytest.raises(ValueError):
        with write_lock(fs, "t", wait_timeout_s=0):
            pass
    with pytest.raises(ValueError):
        with read_lock(fs, "t", ttl_s=0):
            pass
    with pytest.raises(ValueError):
        with read_lock(fs, "t", heartbeat_interval_s=0):
            pass
    with Capture() as cap:
        with write_lock(fs, "t", wait_timeout_s=100.0):  # < TTL đọc/ghi mặc định -> chỉ cảnh báo
            pass
        with read_lock(fs, "t", wait_timeout_s=200.0):  # < TTL ghi 300
            pass
        with write_lock(fs, "t"), read_lock(fs, "u"):  # mặc định: không cảnh báo
            pass
    assert len([m for m in cap.levels("WARNING") if "wait_timeout_s" in m]) == 2
    assert fs.files == {}


def test_heartbeat_keeps_lock_beyond_ttl():
    fs = MemoryLockFs()
    got = {}

    def second():
        with write_lock(fs, "t", ttl_s=0.3, poll_s=0.01, wait_timeout_s=10.0, owner="w2"):
            got["at"] = time.monotonic()

    with write_lock(fs, "t", ttl_s=0.3, heartbeat_interval_s=0.1, owner="w1") as w1:
        exp0 = expires(fs, W)
        th = threading.Thread(target=second)
        th.start()
        time.sleep(1.2)  # 4 TTL
        assert "at" not in got  # w2 vẫn chờ
        assert json.loads(fs.read(W))["id"] == w1.id
        assert w1.refreshes >= 2
        assert w1.lost is False
        assert expires(fs, W) > exp0
        released = time.monotonic()
    th.join(10)
    assert not th.is_alive()
    assert got["at"] >= released
    assert fs.list(D) == []
    assert not hb_threads()


def test_dead_holder_taken_over_after_ttl():
    fs = MemoryLockFs()
    cm = write_lock(fs, "t", ttl_s=0.4, heartbeat=False, owner="dead")
    dead = cm.__enter__()  # "tiến trình chết": không bao giờ nhả, không heartbeat
    start = time.monotonic()
    with write_lock(fs, "t", poll_s=0.02, wait_timeout_s=10.0) as w:
        took = time.monotonic() - start
        assert json.loads(fs.read(W))["id"] == w.id != dead.id
        assert 0.25 <= w.waited_s <= 2.0
        assert took >= 0.25
    assert fs.list(D) == []


def test_without_heartbeat_waiter_takes_over_and_old_holder_cannot_refresh():
    fs = MemoryLockFs()
    with write_lock(fs, "t", ttl_s=0.2, heartbeat=False, owner="slow") as slow:
        with write_lock(fs, "t", ttl_s=5.0, poll_s=0.01, wait_timeout_s=10.0, owner="new") as new:
            assert new.waited_s > 0
            with pytest.raises(locks.LockLost):
                slow.refresh()
            assert slow.lost is True
            assert slow.refreshes == 0
        # new đã nhả; giả lập người khác lại chiếm để kiểm slow không xoá nhầm
        fs.files[W] = lock_doc("writer", "other", locks._now(), ttl=100)
    assert json.loads(fs.read(W))["id"] == "other"


def test_lost_detected_by_heartbeat_and_foreign_file_kept():
    fs = MemoryLockFs()
    with Capture() as cap:
        with write_lock(fs, "t", ttl_s=0.3, heartbeat_interval_s=0.02) as w:
            fs.files[W] = lock_doc("writer", "thief", locks._now(), ttl=100)
            deadline = time.monotonic() + 5
            while not w.lost and time.monotonic() < deadline:
                time.sleep(0.01)
            assert w.lost is True
            deadline = time.monotonic() + 5
            while hb_threads() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert not hb_threads()  # heartbeat tự dừng
    assert json.loads(fs.read(W))["id"] == "thief"
    assert len([m for m in cap.levels("ERROR") if "Mất khoá" in m]) == 1


def test_lost_when_file_deleted_reader():
    fs = MemoryLockFs()
    with read_lock(fs, "t", ttl_s=0.3, heartbeat_interval_s=0.02) as r:
        fs.delete(r.path)
        deadline = time.monotonic() + 5
        while not r.lost and time.monotonic() < deadline:
            time.sleep(0.01)
        assert r.lost
        time.sleep(0.1)
        assert fs.read(r.path) is None  # heartbeat không tạo lại file
    assert fs.list(D) == []


def test_transient_refresh_error_retried():
    class Flaky(MemoryLockFs):
        def __init__(self):
            super().__init__()
            self.fail = 0

        def overwrite(self, path, data):
            if self.fail:
                self.fail -= 1
                raise OSError("NameNode hiccup")
            super().overwrite(path, data)

    fs = Flaky()
    with Capture() as cap:
        with write_lock(fs, "t", ttl_s=0.6, heartbeat_interval_s=0.03) as w:
            fs.fail = 1
            deadline = time.monotonic() + 5
            while (w.last_refresh_error is None or w.refreshes < 2) and time.monotonic() < deadline:
                time.sleep(0.01)
            assert "NameNode hiccup" in w.last_refresh_error
            before = w.refreshes
            time.sleep(0.15)
            assert w.refreshes > before  # nhịp sau thành công
            assert not w.lost
            assert json.loads(fs.read(W))["id"] == w.id
    assert any("NameNode hiccup" in m for m in cap.levels("WARNING"))
    assert fs.list(D) == []


def test_no_heartbeat_thread_leak():
    fs = MemoryLockFs()
    fast = dict(heartbeat_interval_s=0.01)
    with write_lock(fs, "t", **fast), read_lock(fs, "u", **fast):
        assert len(hb_threads("t")) == 1 and len(hb_threads("u")) == 1
        assert all(th.daemon for th in hb_threads("t"))
    assert not hb_threads("t") and not hb_threads("u")
    with pytest.raises(RuntimeError):
        with read_lock(fs, "t", **fast):
            raise RuntimeError("x")
    assert not hb_threads()
    # LockTimeout ở pha 1 (writer khác giữ) và pha 2 (reader giữ)
    with write_lock(fs, "t", owner="holder", **fast):
        with pytest.raises(LockTimeout):
            with write_lock(fs, "t", poll_s=0.01, wait_timeout_s=0.05, **fast):
                pass
    with read_lock(fs, "t", heartbeat=False):
        with pytest.raises(LockTimeout):
            with write_lock(fs, "t", poll_s=0.01, wait_timeout_s=0.05, **fast):
                pass
        assert not hb_threads()
    assert not hb_threads()
    assert fs.list(D) == []


def test_release_refresh_race_leaves_no_file():
    class Slow(MemoryLockFs):
        def overwrite(self, path, data):
            time.sleep(0.0005)  # nới cửa sổ race
            super().overwrite(path, data)

    fs = Slow()

    def worker(i):
        for _ in range(40):
            with write_lock(fs, "t", heartbeat_interval_s=0.001, poll_s=0.001, wait_timeout_s=20):
                time.sleep(0.002)
            with read_lock(fs, "t", heartbeat_interval_s=0.001, poll_s=0.001, wait_timeout_s=20):
                time.sleep(0.002)

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for th in ts:
        th.start()
    for th in ts:
        th.join(60)
    assert not any(th.is_alive() for th in ts)
    time.sleep(0.05)
    assert fs.list(D) == []
    assert not hb_threads()


def test_refresh_after_release_raises_and_does_not_recreate():
    fs = MemoryLockFs()
    with write_lock(fs, "t", heartbeat=False) as w:
        pass
    with pytest.raises(locks.LockLost):
        w.refresh()
    assert fs.list(D) == []
    assert w.lost is False
