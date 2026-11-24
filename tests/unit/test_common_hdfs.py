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


# --- row group / đọc danh sách file / footer (cần pyarrow, chạy trong image cụm) ---


def _ts_table(pa, start_minute, n, sym="FPT"):
    from datetime import datetime, timedelta

    base = datetime(2026, 1, 5, 9, 0) + timedelta(minutes=start_minute)
    return pa.table(
        {
            "ts": pa.array([base + timedelta(minutes=i) for i in range(n)], pa.timestamp("us")),
            # kiểu tường minh: list rỗng (n=0) sẽ bị suy ra kiểu null -> khác schema
            "symbol": pa.array([sym] * n, pa.string()),
            "close": pa.array([float(start_minute + i) for i in range(n)], pa.float64()),
        }
    )


def test_row_groups_validation_errors():
    pa = pytest.importorskip("pyarrow")
    fs = FakeFS()
    with pytest.raises(ValueError):
        hdfs.write_parquet_row_groups_atomic([], "/d/f.parquet", fs=fs)
    a = pa.table({"x": [1]})
    b = pa.table({"y": [1]})
    with pytest.raises(ValueError):
        hdfs.write_parquet_row_groups_atomic([a, b], "/d/f.parquet", fs=fs)
    assert fs.files == {}


def test_row_groups_one_per_table_and_info(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("pyarrow.parquet")
    fsspec = pytest.importorskip("fsspec")
    fs = fsspec.filesystem("file")
    path = str(tmp_path / "d" / "f.parquet")
    tables = [_ts_table(pa, 0, 3), _ts_table(pa, 10, 0), _ts_table(pa, 10, 5), _ts_table(pa, 20, 2)]
    hdfs.write_parquet_row_groups_atomic(tables, path, fs=fs)
    hdfs.write_parquet_row_groups_atomic(tables, path, fs=fs)  # ghi đè

    info = hdfs.parquet_file_info(path, fs=fs)
    assert info["num_rows"] == 10
    assert info["num_row_groups"] == 3  # bảng 0 dòng bị bỏ qua
    assert info["row_group_rows"] == [3, 5, 2]
    assert info["size_bytes"] > 0
    assert [p for p in fs.ls(str(tmp_path / "d"), detail=False)] == [path]  # không còn file tạm

    out = hdfs.read_parquet_files([path], fs=fs)
    assert out.num_rows == 10
    assert out.column("close").to_pylist() == [0.0, 1.0, 2.0] + [10.0 + i for i in range(5)] + [
        20.0,
        21.0,
    ]


def test_parquet_file_info_missing_is_none(tmp_path):
    pytest.importorskip("pyarrow.parquet")
    fsspec = pytest.importorskip("fsspec")
    assert (
        hdfs.parquet_file_info(str(tmp_path / "nope.parquet"), fs=fsspec.filesystem("file")) is None
    )


def test_read_parquet_files_filter_prunes_rows(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")
    pytest.importorskip("pyarrow.dataset")
    fsspec = pytest.importorskip("fsspec")
    from datetime import datetime

    fs = fsspec.filesystem("file")
    p1, p2 = str(tmp_path / "a.parquet"), str(tmp_path / "b.parquet")
    hdfs.write_parquet_row_groups_atomic([_ts_table(pa, 0, 3), _ts_table(pa, 100, 3)], p1, fs=fs)
    hdfs.write_parquet_row_groups_atomic([_ts_table(pa, 200, 3, sym="VNM")], p2, fs=fs)
    cut = datetime(2026, 1, 5, 9, 0) + __import__("datetime").timedelta(minutes=100)

    import pyarrow.dataset as ds

    expr = ds.field("ts") >= pa.scalar(cut, pa.timestamp("us"))
    out = hdfs.read_parquet_files([p1, p2], columns=["ts", "close"], filters=expr, fs=fs)
    assert out.column_names == ["ts", "close"]
    assert sorted(out.column("close").to_pylist()) == [100.0, 101.0, 102.0, 200.0, 201.0, 202.0]

    dnf = hdfs.read_parquet_files([p1, p2], filters=[("symbol", "=", "VNM")], fs=fs)
    assert dnf.num_rows == 3
    assert pc.all(pc.equal(dnf.column("symbol"), "VNM")).as_py()


def test_read_parquet_files_missing_handling(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("pyarrow.dataset")
    fsspec = pytest.importorskip("fsspec")
    fs = fsspec.filesystem("file")
    good = str(tmp_path / "g.parquet")
    missing = str(tmp_path / "m.parquet")
    hdfs.write_parquet_atomic(pa.table({"x": [1, 2]}), good, fs=fs)

    assert hdfs.read_parquet_files([missing, good], fs=fs).num_rows == 2
    empty = hdfs.read_parquet_files([missing], fs=fs)
    assert empty.num_rows == 0 and empty.num_columns == 0
    assert hdfs.read_parquet_files([], fs=fs).num_rows == 0
    with pytest.raises(FileNotFoundError):
        hdfs.read_parquet_files([good, missing], fs=fs, missing_ok=False)
