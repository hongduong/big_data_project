"""Hàm dùng chung cho job raw -> clean. pyspark chỉ import bên trong hàm."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from stocklake.common.logging import get_logger
from stocklake.contracts import TableSchema, to_spark

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = get_logger(__name__)

VN_TZ_OFFSET_SECONDS = 7 * 3600  # Việt Nam UTC+7, không có giờ mùa hè

# Khoá ghi bảng clean (tên dùng chung với dq/dashboard). TTL/thời gian chờ lấy mặc định của
# `stocklake.common.locks` (TTL ngắn + heartbeat gia hạn khi lệnh ghi còn chạy). Khoá chỉ giữ
# quanh lệnh ghi Spark (+ ghi trạng thái), không quanh phần lập kế hoạch.
LOCK_PREFIX = "clean."


def _contract_cols(df: DataFrame, schema: TableSchema, names: tuple[str, ...] | None = None):
    from pyspark.sql import functions as F

    types = {f.name: f.dataType for f in to_spark(schema).fields}
    cols = []
    for name in names or schema.column_names:
        if name in df.columns:
            cols.append(F.col(name).cast(types[name]).alias(name))
        else:
            cols.append(F.lit(None).cast(types[name]).alias(name))
    return cols


def conform(df: DataFrame, schema: TableSchema, names: tuple[str, ...] | None = None) -> DataFrame:
    """Chọn các cột theo contract, ép đúng kiểu; cột thiếu thành null."""
    return df.select(*_contract_cols(df, schema, names))


def select_contract(df: DataFrame, schema: TableSchema) -> DataFrame:
    """Cột theo contract (đúng kiểu) rồi tới các cột phân vùng suy ra (year, month)."""
    from pyspark.sql import functions as F

    # Cột phân vùng suy ra phải lấy từ `df` trước khi conform bỏ chúng đi.
    extra = [F.col(c).cast("int").alias(c) for c in schema.derived_partitions]
    return df.select(*_contract_cols(df, schema), *extra)


def vn_date(ts: Column) -> Column:
    """Ngày theo giờ VN của timestamp; không phụ thuộc timezone của session."""
    from pyspark.sql import functions as F

    days = F.floor((ts.cast("long") + VN_TZ_OFFSET_SECONDS) / 86400).cast("int")
    return F.date_add(F.lit("1970-01-01").cast("date"), days)


def path_exists(spark: SparkSession, path: str) -> bool:
    """Kiểm tra đường dẫn qua Hadoop FileSystem của Spark (HDFS hoặc local)."""
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return bool(p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p))


def is_empty_input_error(exc: Exception) -> bool:
    """Spark ném AnalysisException khi thư mục không có file Parquet nào."""
    return "infer schema" in str(exc) or "Path does not exist" in str(exc)


def write_partitioned(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    partition_cols: tuple[str, ...],
    files_per_partition: int = 1,
) -> None:
    """Ghi Parquet phân vùng, sắp xếp theo symbol, ghi đè động.

    `files_per_partition` > 1: chia mỗi phân vùng thành nhiều file theo hash(symbol)
    (tháng quá lớn không bị dồn vào một task); mặc định mỗi phân vùng một file.
    """
    from pyspark.sql import functions as F

    key = "spark.sql.sources.partitionOverwriteMode"
    old = spark.conf.get(key, None)
    spark.conf.set(key, "dynamic")
    try:
        if files_per_partition > 1:
            salt = F.pmod(F.hash("symbol"), F.lit(int(files_per_partition)))
            out = df.repartition(*partition_cols, salt).sortWithinPartitions("symbol")
        else:
            out = df.repartition(*partition_cols).sortWithinPartitions("symbol")
        out.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)
    finally:
        if old is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, old)


# ---------------------------------------------------------------- Hadoop FileSystem (driver)


class HadoopFiles:
    """Thao tác file nhỏ trên driver qua Hadoop FileSystem của JVM (HDFS hoặc file://).

    Không cần pyspark import ở đây: chỉ dùng `spark._jvm` / `spark._jsc` qua py4j.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._jvm = spark._jvm
        self._conf = spark._jsc.hadoopConfiguration()

    def _path(self, p: str) -> Any:
        return self._jvm.org.apache.hadoop.fs.Path(p)

    def _fs(self, p: str) -> Any:
        return self._path(p).getFileSystem(self._conf)

    def _glob(self, pattern: str) -> list[Any]:
        statuses = self._fs(pattern).globStatus(self._path(pattern))
        return list(statuses) if statuses is not None else []

    def glob_dirs(self, pattern: str) -> list[tuple[str, int]]:
        """(đường dẫn đầy đủ, mtime ms của NameNode) của các thư mục khớp glob."""
        out = []
        for st in self._glob(pattern):
            if st.isDirectory():
                out.append((str(st.getPath().toString()), int(st.getModificationTime())))
        return out

    def glob_files(self, pattern: str) -> list[str]:
        return [str(st.getPath().toString()) for st in self._glob(pattern) if st.isFile()]

    def read_text(self, path: str) -> str | None:
        fs, p = self._fs(path), self._path(path)
        try:
            if not fs.exists(p):
                return None
            stream = fs.open(p)
            try:
                data = self._jvm.org.apache.hadoop.io.IOUtils.readFullyToByteArray(stream)
            finally:
                stream.close()
        except Exception as exc:  # noqa: BLE001 - Py4JJavaError
            if "FileNotFoundException" in str(exc):
                return None
            raise
        return bytes(data).decode("utf-8")

    def write_text_atomic(self, path: str, text: str) -> None:
        """Ghi file tạm cùng thư mục rồi đổi tên. HDFS rename không ghi đè -> xoá đích trước.

        Chết giữa lúc xoá và đổi tên thì mất file đích (lượt sau dựng lại toàn bộ: an toàn).
        """
        fs, p = self._fs(path), self._path(path)
        tmp = self._path(f"{path}.tmp-{uuid.uuid4().hex[:12]}")
        out = fs.create(tmp, True)
        try:
            out.write(bytearray(text.encode("utf-8")))
        finally:
            out.close()
        try:
            if fs.exists(p):
                fs.delete(p, False)
            if not fs.rename(tmp, p):
                raise OSError(f"Không đổi tên được {tmp.toString()} -> {path}")
        except BaseException:
            fs.delete(tmp, False)
            raise

    def now_ms(self, dir_path: str) -> int:
        """Giờ của NameNode (ms): tạo file thăm dò `_probe-*`, đọc mtime, xoá.

        Dùng cùng miền đồng hồ với mtime của file raw, tránh lệch giờ driver/NameNode.
        """
        fs = self._fs(dir_path)
        fs.mkdirs(self._path(dir_path))
        p = self._path(f"{dir_path.rstrip('/')}/_probe-{uuid.uuid4().hex[:12]}")
        fs.create(p, True).close()
        try:
            return int(fs.getFileStatus(p).getModificationTime())
        finally:
            fs.delete(p, False)


# ---------------------------------------------------------------- khoá ghi


def job_owner(spark: Any) -> str:
    """Tên app + applicationId của Spark, ghi vào file khoá để biết ai đang giữ."""
    try:
        sc = spark.sparkContext
        return f"{sc.appName}:{sc.applicationId}"
    except Exception:  # noqa: BLE001
        return "unknown-spark-app"


class _NoLock:
    id = path = None
    waited_s = 0.0
    lost = False
    refreshes = 0
    last_refresh_error = None


@contextmanager
def clean_write_lock(
    spark: Any, table: str, lock_fs: Any = None, **lock_kwargs: Any
) -> Iterator[Any]:
    """Giữ khoá ghi `clean.<table>` quanh lệnh ghi (heartbeat gia hạn trong lúc giữ).

    `lock_fs`: None -> `hadoop_fs(spark)`; False -> không khoá (chỉ dùng trong test); còn lại là
    một LockFs (vd `MemoryLockFs`). `lock_kwargs` chuyển thẳng cho `write_lock` (mặc định của
    module khoá). Hết giờ chờ -> TimeoutError; writer.lock bị chiếm khi đang chờ reader ->
    RuntimeError. Cả hai: job thoát mã khác 0, chưa ghi gì.
    Sau khối `with`, gọi `report_lock_lost` để báo nếu khoá bị mất giữa chừng.
    """
    if lock_fs is False:
        yield _NoLock()
        return
    from stocklake.common.locks import (
        DEFAULT_WAIT_TIMEOUT_S,
        LockLost,
        LockTimeout,
        hadoop_fs,
        write_lock,
    )

    fs = hadoop_fs(spark) if lock_fs is None else lock_fs
    name = f"{LOCK_PREFIX}{table}"
    wait_s = float(lock_kwargs.get("wait_timeout_s", DEFAULT_WAIT_TIMEOUT_S))
    acquired = False
    try:
        with write_lock(fs, name, owner=job_owner(spark), **lock_kwargs) as handle:
            acquired = True
            yield handle
    except LockTimeout as exc:
        if acquired:
            raise
        msg = f"Không giành được khoá ghi {name} sau {wait_s:.0f}s; chưa ghi gì. {exc}"
        log.error(msg)
        raise TimeoutError(msg) from exc
    except LockLost as exc:
        if acquired:
            raise
        msg = f"Mất khoá ghi {name} khi đang chờ reader (bị chiếm); chưa ghi gì. {exc}"
        log.error(msg)
        raise RuntimeError(msg) from exc


def report_lock_lost(handle: Any, result: dict[str, Any]) -> bool:
    """Ghi `result["lock_lost"]`; khoá mất trong lúc ghi -> log ERROR (dữ liệu đã ghi, job không
    lỗi, nhưng có thể đã có người đọc/ghi chồng: cần kiểm tra)."""
    lost = bool(getattr(handle, "lost", False))
    result["lock_lost"] = lost
    if lost:
        log.error(
            "MẤT khoá ghi %s trong lúc ghi (gia hạn %s lần, lỗi cuối: %s). Dữ liệu đã ghi xong; "
            "kiểm tra job đọc/ghi chạy cùng lúc.",
            getattr(handle, "path", "?"),
            getattr(handle, "refreshes", "?"),
            getattr(handle, "last_refresh_error", None),
        )
    return lost
