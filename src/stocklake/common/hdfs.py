"""Truy cập HDFS qua WebHDFS (fsspec). Dùng cho dashboard và collector."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from stocklake.common.config import Settings, get_settings

if TYPE_CHECKING:
    import pyarrow


def webhdfs_fs(settings: Settings | None = None):
    """Trả fsspec WebHDFS filesystem."""
    import fsspec

    s = settings or get_settings()
    return fsspec.filesystem("webhdfs", host=s.webhdfs_host, port=s.webhdfs_port, user=s.hdfs_user)


def exists(path: str, fs: Any = None) -> bool:
    return bool((fs or webhdfs_fs()).exists(path))


def ls(path: str, fs: Any = None) -> list[str]:
    """Liệt kê tên đầy đủ các mục trong thư mục."""
    return sorted((fs or webhdfs_fs()).ls(path, detail=False))


def makedirs(path: str, fs: Any = None) -> None:
    """Tạo thư mục (kèm thư mục cha); không lỗi nếu đã có."""
    (fs or webhdfs_fs()).makedirs(path, exist_ok=True)


def _parent(path: str) -> str:
    head = path.rstrip("/").rpartition("/")[0]
    return head


def _write_atomic(fs: Any, path: str, writer: Callable[[Any], None]) -> None:
    """Ghi file tạm rồi đổi tên đè lên `path`; dọn file tạm nếu lỗi.

    WebHDFS RENAME không ghi đè nên phải xoá đích trước (có một khoảng ngắn không có file).
    """
    parent = _parent(path)
    if parent:
        fs.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp-{uuid.uuid4().hex[:12]}"
    try:
        with fs.open(tmp, "wb") as f:
            writer(f)
        if fs.exists(path):
            fs.rm(path)
        fs.mv(tmp, path)
        if not fs.exists(path):  # RENAME của WebHDFS trả false thay vì ném lỗi
            raise OSError(f"Đổi tên {tmp} -> {path} thất bại")
    except BaseException:
        try:
            if fs.exists(tmp):
                fs.rm(tmp)
        except Exception:  # noqa: BLE001 - dọn rác không được che lỗi gốc
            pass
        raise


def write_parquet_atomic(table: pyarrow.Table, path: str, fs: Any = None) -> None:
    """Ghi bảng pyarrow thành 1 file Parquet tại `path`, ghi đè nguyên tử bằng file tạm."""
    import pyarrow.parquet as pq

    _write_atomic(fs or webhdfs_fs(), path, lambda f: pq.write_table(table, f))


def _is_data_file(rel_parts: list[str]) -> bool:
    if any(p.startswith(("_", ".")) for p in rel_parts):
        return False
    return rel_parts[-1].endswith(".parquet")


def read_parquet_dir(
    path: str, columns: list[str] | None = None, filters: Any = None, fs: Any = None
) -> pyarrow.Table:
    """Đọc thư mục Parquet (có thể phân vùng hive, vd `year=2026/`) thành bảng pyarrow.

    Bỏ qua mục bắt đầu bằng `_` hoặc `.` (`_SUCCESS`, `_spark_metadata/`) và file tạm không
    đuôi `.parquet`. `filters` là biểu thức pyarrow hoặc danh sách DNF.
    `path` không tồn tại -> FileNotFoundError; thư mục có nhưng chưa có file dữ liệu ->
    bảng rỗng không cột.
    """
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs

    fs = fs or webhdfs_fs()
    base = path.rstrip("/") or "/"
    if not fs.exists(base):
        raise FileNotFoundError(f"Không có đường dẫn HDFS: {path}")
    files = []
    for f in sorted(fs.find(base)):
        rel = f[len(base) :].strip("/").split("/") if f.startswith(base) else [f.rsplit("/", 1)[-1]]
        if _is_data_file(rel):
            files.append(f)
    if not files:
        return pa.table({})
    dataset = ds.dataset(
        files,
        filesystem=pafs.PyFileSystem(pafs.FSSpecHandler(fs)),
        format="parquet",
        partitioning="hive",
        partition_base_dir=base,
    )
    if isinstance(filters, list):
        import pyarrow.parquet as pq

        filters = pq.filters_to_expression(filters)
    return dataset.to_table(columns=columns, filter=filters)
