"""Truy cập HDFS qua WebHDFS (fsspec). Dùng cho dashboard và collector."""

from __future__ import annotations

from typing import Any

from stocklake.common.config import Settings, get_settings


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
