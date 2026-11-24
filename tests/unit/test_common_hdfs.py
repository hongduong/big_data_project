import io

import pytest

from stocklake.common import hdfs


class FakeFS:
    """fs giả trong bộ nhớ; rename (mv) KHÔNG ghi đè giống WebHDFS."""

    def __init__(self, fail_on_write=False, rename_noop=False):
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = set()
        self.fail_on_write = fail_on_write
        self.rename_noop = rename_noop

    def makedirs(self, path, exist_ok=False):
        self.dirs.add(path)

    def exists(self, path):
        return path in self.files or path in self.dirs

    def open(self, path, mode="rb"):
        fs = self
        buf = io.BytesIO()

        class _W:
            def __enter__(self_):
                return self_

            def write(self_, data):
                if fs.fail_on_write:
                    raise OSError("disk full")
                buf.write(data)

            def __exit__(self_, *exc):
                fs.files[path] = buf.getvalue()

        return _W()

    def rm(self, path):
        del self.files[path]

    def mv(self, src, dst):
        if self.rename_noop:
            return
        if dst in self.files:
            raise FileExistsError(dst)
        self.files[dst] = self.files.pop(src)

    def ls(self, path, detail=False):
        return [p for p in self.files if p.startswith(path.rstrip("/") + "/")]


def test_makedirs_uses_fs():
    fs = FakeFS()
    hdfs.makedirs("/data/raw/x", fs=fs)
    assert fs.exists("/data/raw/x")


def test_write_atomic_creates_parent_and_overwrites():
    fs = FakeFS()
    hdfs._write_atomic(fs, "/d/a/f.parquet", lambda f: f.write(b"one"))
    assert fs.files == {"/d/a/f.parquet": b"one"}
    assert "/d/a" in fs.dirs
    hdfs._write_atomic(fs, "/d/a/f.parquet", lambda f: f.write(b"two"))
    assert fs.files == {"/d/a/f.parquet": b"two"}  # đã ghi đè, không còn file tạm


def test_write_atomic_cleans_tmp_on_failure():
    fs = FakeFS()
    hdfs._write_atomic(fs, "/d/f.parquet", lambda f: f.write(b"old"))

    def boom(f):
        f.write(b"partial")
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        hdfs._write_atomic(fs, "/d/f.parquet", boom)
    assert fs.files == {"/d/f.parquet": b"old"}


def test_write_atomic_detects_failed_rename():
    fs = FakeFS(rename_noop=True)
    with pytest.raises(OSError):
        hdfs._write_atomic(fs, "/d/f.parquet", lambda f: f.write(b"x"))
    assert fs.files == {}


def test_is_data_file():
    assert hdfs._is_data_file(["year=2026", "FPT.parquet"])
    assert hdfs._is_data_file(["part-0.snappy.parquet"])
    assert not hdfs._is_data_file(["_SUCCESS"])
    assert not hdfs._is_data_file(["_spark_metadata", "0.parquet"])
    assert not hdfs._is_data_file([".part.parquet.crc"])
    assert not hdfs._is_data_file(["FPT.parquet.tmp-abc123"])


def test_read_missing_path_raises():
    pytest.importorskip("pyarrow")
    with pytest.raises(FileNotFoundError):
        hdfs.read_parquet_dir("/nope", fs=FakeFS())


def test_parquet_roundtrip_hive_and_ignored_entries(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    fsspec = pytest.importorskip("fsspec")
    fs = fsspec.filesystem("file")
    root = str(tmp_path / "t")
    t = pa.table({"symbol": ["FPT", "VNM"], "close": [1.0, 2.0]})
    hdfs.write_parquet_atomic(t, f"{root}/year=2026/FPT.parquet", fs=fs)
    hdfs.write_parquet_atomic(t, f"{root}/year=2026/FPT.parquet", fs=fs)  # ghi đè
    hdfs.write_parquet_atomic(t, f"{root}/year=2025/VNM.parquet", fs=fs)
    fs.makedirs(f"{root}/_spark_metadata", exist_ok=True)
    pq.write_table(t, f"{root}/_spark_metadata/0.parquet")
    fs.touch(f"{root}/_SUCCESS")
    fs.touch(f"{root}/year=2026/FPT.parquet.tmp-dead")

    out = hdfs.read_parquet_dir(root, fs=fs)
    assert out.num_rows == 4
    assert set(out.column_names) == {"symbol", "close", "year"}
    only = hdfs.read_parquet_dir(root, columns=["symbol"], filters=[("year", "=", 2026)], fs=fs)
    assert only.num_rows == 2
