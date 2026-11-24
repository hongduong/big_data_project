"""Nến 1 phút raw -> clean = hợp nhất lịch sử batch (raw/intraday_history) và streaming.

Quyết định:
- `trade_date` tính lại từ `bar_ts` theo giờ VN (UTC+7), không tin cột raw.
- Một dòng cho mỗi (symbol, bar_ts) kể cả khác nguồn. Ưu tiên (nhỏ = thắng):
  0 history của VCI; 1 history khác; 2 streaming nguồn thật (vci/vndirect/kbs);
  3 streaming replay có replay_run_id. Hoà trong cùng mức: `kafka_ts` mới nhất (chỉ
  streaming), rồi `ingest_ts` mới nhất, rồi tên nguồn.
- Streaming source = "replay" không có replay_run_id (bản tin thử) bị loại và đếm riêng.
- Cột `source` của clean = nguồn của dòng được chọn (chữ thường, bỏ khoảng trắng): history
  -> nguồn của file (vd "vci"), streaming nguồn thật -> tên nguồn, replay có run id -> "replay".
- Clean phân vùng (year, month) và ghi đè cả phân vùng, nên mỗi tháng phải dựng lại từ TOÀN BỘ
  raw của CẢ HAI bảng trong tháng đó (tháng theo giờ VN của bar_ts).
- Chạy tăng dần: chỉ dựng lại tháng có đầu vào thay đổi (trạng thái `_build_state.json` cạnh
  bảng, logic thuần ở `_incremental.py`); lệnh ghi giữ khoá ghi `clean.intraday_1m`.
- full/--dates (mọi tháng quét đều dựng lại): raw đọc MỘT lần (`_scan`, persist), fingerprint
  và clean cùng lấy từ lần quét đó.
"""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING, Any

from stocklake.common.logging import get_logger
from stocklake.contracts import get_schema, table_path
from stocklake.contracts.paths import DATA_BASE
from stocklake.etl.raw_to_clean import _incremental as inc
from stocklake.etl.raw_to_clean._common import (
    HadoopFiles,
    clean_write_lock,
    conform,
    is_empty_input_error,
    job_owner,
    path_exists,
    report_lock_lost,
    select_contract,
    vn_date,
    write_partitioned,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)

RAW = get_schema("raw", "intraday_prices")
HISTORY_TABLE = "intraday_history"  # schema lấy lười trong hàm (contract do agent khác thêm)
CLEAN = get_schema("clean", "intraday_1m")

# Nguồn thật (khớp topic intraday_prices); mọi nguồn khác, gồm "replay", xếp sau.
REAL_SOURCES = ("vci", "vndirect", "kbs")
_REQUIRED = ("symbol", "bar_ts", "open", "high", "low", "close", "volume", "available_at")
# Cột chung của hai đầu vào sau khi chiếu về một dạng.
_COMMON = (*_REQUIRED, "source", "ingest_ts", "kafka_ts")

# Mức ưu tiên (xem docstring module).
PRIO_HISTORY_VCI, PRIO_HISTORY_OTHER, PRIO_STREAM_REAL, PRIO_STREAM_REPLAY = 0, 1, 2, 3
# Lọc dòng dùng được (đúng bộ lọc của transform và fingerprint).
_KEEP = "is_valid AND NOT is_untagged_replay"
# Ước lượng ~128 MB Parquet mỗi file với dòng nến phút (~30 byte/dòng nén); cần đo lại.
ROWS_PER_FILE = 4_000_000


def _valid_expr():
    from pyspark.sql import functions as F

    valid = F.lit(True)
    for c in _REQUIRED:
        valid = valid & F.col(c).isNotNull()
    return valid


def _normalise(raw_df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    df = conform(raw_df, RAW)
    valid = _valid_expr()
    # Bản tin thử (vd. send_sample ở Lát 0): source = "replay" nhưng không có replay_run_id.
    run_id = F.trim(F.coalesce(F.col("replay_run_id"), F.lit("")))
    untagged = (F.lower(F.col("source")) == F.lit("replay")) & (run_id == F.lit(""))
    return df.withColumn("is_valid", valid).withColumn("is_untagged_replay", untagged)


def count_rejected(raw_df: DataFrame) -> int:
    """Số dòng raw bị loại do thiếu trường bắt buộc."""
    return _normalise(raw_df).filter("NOT is_valid").count()


def count_untagged_replay(raw_df: DataFrame) -> int:
    """Số dòng hợp lệ bị loại vì source = "replay" mà replay_run_id rỗng."""
    return _normalise(raw_df).filter("is_valid AND is_untagged_replay").count()


def _project_stream(stream_df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    df = _normalise(stream_df)
    real = F.lower(F.col("source")).isin(*REAL_SOURCES)
    prio = F.when(real, F.lit(PRIO_STREAM_REAL)).otherwise(F.lit(PRIO_STREAM_REPLAY))
    return df.select(
        *_COMMON,
        "replay_run_id",
        "is_valid",
        "is_untagged_replay",
        prio.alias("_prio"),
        F.lit(inc.STREAM).alias("_origin"),
    )


def _project_history(history_df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    df = conform(history_df, get_schema("raw", HISTORY_TABLE))
    df = df.withColumn("kafka_ts", F.lit(None).cast("timestamp"))
    is_vci = F.lower(F.col("source")) == F.lit("vci")
    prio = F.when(is_vci, F.lit(PRIO_HISTORY_VCI)).otherwise(F.lit(PRIO_HISTORY_OTHER))
    return df.select(
        *_COMMON,
        F.lit(None).cast("string").alias("replay_run_id"),
        _valid_expr().alias("is_valid"),
        F.lit(False).alias("is_untagged_replay"),
        prio.alias("_prio"),
        F.lit(inc.HISTORY).alias("_origin"),
    )


def _combine(stream_df: DataFrame | None, history_df: DataFrame | None) -> DataFrame:
    """Chiếu hai đầu vào về cùng dạng (+ replay_run_id, cờ hợp lệ, `_prio`, `_origin`) rồi union."""
    parts = []
    if stream_df is not None:
        parts.append(_project_stream(stream_df))
    if history_df is not None:
        parts.append(_project_history(history_df))
    if not parts:
        raise ValueError("Cần ít nhất một đầu vào (stream_df hoặc history_df)")
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def _dedupe(combined: DataFrame) -> DataFrame:
    """Lọc dòng hợp lệ, chọn một dòng mỗi (symbol, bar_ts); giữ `_prio`, thêm year/month."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = combined.filter(_KEEP)
    w = Window.partitionBy("symbol", "bar_ts").orderBy(
        F.col("_prio").asc(),
        F.col("kafka_ts").desc_nulls_last(),
        F.col("ingest_ts").desc_nulls_last(),
        F.col("source").asc_nulls_last(),
    )
    best = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    best = best.withColumn("source", F.lower(F.trim(F.col("source"))))
    best = best.withColumn("trade_date", vn_date(F.col("bar_ts")))
    return best.withColumn("year", F.year("trade_date")).withColumn("month", F.month("trade_date"))


def clean_intraday(
    stream_df: DataFrame | None = None, history_df: DataFrame | None = None
) -> DataFrame:
    """Raw -> clean: hợp nhất history + streaming, khử trùng theo (symbol, bar_ts).

    Trả đúng các cột contract clean.intraday_1m (gồm `source` của dòng được chọn) rồi tới
    `year`, `month`.
    Loại streaming source = "replay" không có replay_run_id (bản tin thử, không phải dữ liệu).
    """
    return select_contract(_dedupe(_combine(stream_df, history_df)), CLEAN)


def _read_hive_parquet(spark: SparkSession, root: str, **options: str) -> DataFrame | None:
    reader = spark.read.option("basePath", root)
    for k, v in options.items():
        reader = reader.option(k, v)
    try:
        return reader.parquet(root)
    except Exception as exc:  # noqa: BLE001
        if is_empty_input_error(exc):
            return None
        raise


def _bar_ts_in_months(months: list[str]):
    """bar_ts thuộc các tháng (giờ VN). So với hằng timestamp nên đẩy xuống Parquet được."""
    from pyspark.sql import functions as F

    cond = None
    for first, last in inc.month_ranges(months):
        lo, _ = inc.month_bounds_utc(first)
        _, hi = inc.month_bounds_utc(last)
        c = (F.col("bar_ts") >= F.timestamp_seconds(F.lit(lo))) & (
            F.col("bar_ts") < F.timestamp_seconds(F.lit(hi))
        )
        cond = c if cond is None else (cond | c)
    return cond


def read_raw_intraday(
    spark: SparkSession,
    dates: list[str] | None = None,
    *,
    months: list[str] | None = None,
    base: str = DATA_BASE,
) -> DataFrame | None:
    """Đọc raw intraday_prices (qua gốc bảng: tôn trọng `_spark_metadata`); None nếu chưa có.

    `dates`/`months`: chỉ giữ dòng có bar_ts thuộc các tháng (giờ VN) đó. Lọc thêm phân vùng
    trade_date theo cùng khoảng ngày để Spark tỉa phân vùng (sink tính trade_date = ngày VN của
    bar_ts nên hai điều kiện trùng nhau).
    """
    from pyspark.sql import functions as F

    root = table_path("raw", RAW.name, base)
    if not path_exists(spark, root):
        return None
    df = _read_hive_parquet(spark, root)
    if df is None:
        return None
    wanted = inc.normalise_months([*(dates or []), *(months or [])])
    if wanted:
        td = F.col("trade_date").cast("date")
        part = None
        for first, last in inc.month_ranges(wanted):
            c = td.between(
                F.to_date(F.lit(f"{first}-01")), F.to_date(F.lit(inc.month_last_day(last)))
            )
            part = c if part is None else (part | c)
        df = df.filter(part).filter(_bar_ts_in_months(wanted))
    return df


def read_raw_history(
    spark: SparkSession, months: list[str] | None = None, *, base: str = DATA_BASE
) -> DataFrame | None:
    """Đọc raw intraday_history (source, year lấy từ đường dẫn); None nếu chưa có bảng.

    `months` dạng 'YYYY-MM' (tháng theo giờ VN): file chia theo năm nên đọc các năm cần
    rồi lọc dòng theo khoảng bar_ts của tháng.
    """
    from pyspark.sql import functions as F

    root = table_path("raw", HISTORY_TABLE, base)
    if not path_exists(spark, root):
        return None
    df = _read_hive_parquet(
        spark,
        root,
        mergeSchema="true",
        pathGlobFilter="*.parquet",  # bỏ qua file tạm `.parquet.tmp-*`
    )
    if df is None:
        return None
    if months:
        months = inc.normalise_months(months)
        years = sorted({int(m[:4]) for m in months})
        df = df.filter(F.col("year").cast("int").isin(years)).filter(_bar_ts_in_months(months))
    return df


def _files_per_partition(clean_rows: int, partitions: int) -> int:
    """Số file mỗi phân vùng ~ ROWS_PER_FILE dòng/file; tháng nhỏ một file."""
    return max(1, math.ceil(clean_rows / max(partitions, 1) / ROWS_PER_FILE))


# ---------------------------------------------------------------- fingerprint

# Cột đi vào checksum: mọi thứ quyết định nội dung clean, TRỪ ingest_ts/ingest_run_id/cột Kafka
# (nạp lại y hệt các nến không được coi là thay đổi).
_FP_COLS = ("symbol", "bar_ts", "open", "high", "low", "close", "volume", "available_at", "source")
_FP_KEY = (*_FP_COLS, "replay_run_id", "_origin")


def month_fingerprints(
    stream_df: DataFrame | None, history_df: DataFrame | None
) -> dict[str, dict[str, dict[str, Any]]]:
    """{'YYYY-MM': {'history'|'stream': {rows, max_bar_us, sum_volume, checksum}}}.

    Tính trên các dòng KHÁC NHAU (theo cột checksum) còn lại sau đúng bộ lọc hợp lệ của transform
    (thiếu trường bắt buộc, replay không run id); `rows` là số dòng khác nhau đó.
    checksum = sum(xxhash64(cột)) cộng trong decimal(38,0): xxhash64 có |h| < 2^63 nên tràn
    chỉ khi > 1e19 dòng; tổng số nguyên không phụ thuộc thứ tự dòng/phân vùng.
    sum_volume, checksum trả dạng chuỗi (số nguyên lớn, JSON giữ chính xác).
    """
    from pyspark.sql import functions as F

    parts = []
    if stream_df is not None:
        s = _normalise(stream_df).filter("is_valid AND NOT is_untagged_replay")
        parts.append(
            s.select(*_FP_COLS, F.col("replay_run_id"), F.lit(inc.STREAM).alias("_origin"))
        )
    if history_df is not None:
        h = conform(history_df, get_schema("raw", HISTORY_TABLE)).filter(_valid_expr())
        parts.append(
            h.select(
                *_FP_COLS,
                F.lit(None).cast("string").alias("replay_run_id"),
                F.lit(inc.HISTORY).alias("_origin"),
            )
        )
    if not parts:
        return {}
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    return _fp_aggregate(df)


def scan_fingerprints(scan: DataFrame) -> dict[str, dict[str, dict[str, Any]]]:
    """Fingerprint từ khung đã quét (`_scan`), không đọc lại raw.

    Cùng định nghĩa với `month_fingerprints` (cùng cột, cùng bộ lọc, cùng khử bản sao, cùng tổng
    decimal) nên trạng thái do hai đường ghi so sánh được với nhau.
    """
    return _fp_aggregate(scan.filter(_KEEP).select(*_FP_KEY))


def _fp_aggregate(df: DataFrame) -> dict[str, dict[str, dict[str, Any]]]:
    """Dòng hợp lệ (cột `_FP_KEY`) -> {'YYYY-MM': {origin: {...}}}; một action (collect)."""
    from pyspark.sql import functions as F

    # Bản sao y hệt (Kafka giao lại, nạp lại cùng nến) không đổi clean -> không tính là thay đổi.
    df = df.dropDuplicates(list(_FP_KEY))
    row_hash = F.xxhash64(*[F.col(c) for c in (*_FP_COLS, "replay_run_id")])
    rows = (
        df.groupBy(F.date_format(vn_date(F.col("bar_ts")), "yyyy-MM").alias("_ym"), "_origin")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.expr("unix_micros(max(bar_ts))").alias("max_bar_us"),
            F.sum(F.col("volume").cast("decimal(38,0)")).alias("sum_volume"),
            F.sum(row_hash.cast("decimal(38,0)")).alias("checksum"),
        )
        .collect()
    )
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for r in rows:
        out.setdefault(r["_ym"], {})[r["_origin"]] = {
            "rows": int(r["rows"]),
            "max_bar_us": int(r["max_bar_us"]),
            "sum_volume": str(int(r["sum_volume"])),
            "checksum": str(int(r["checksum"])),
        }
    return out


def _read_inputs(
    spark: SparkSession, months: list[str] | None, base: str
) -> tuple[DataFrame | None, DataFrame | None]:
    """(stream, history) của các tháng; months None = toàn bộ."""
    if months is not None and not months:
        return None, None
    return (
        read_raw_intraday(spark, months=months, base=base),
        read_raw_history(spark, months, base=base),
    )


# ---------------------------------------------------------------- liệt kê thư mục


def list_input_dirs(files: Any, base: str = DATA_BASE) -> dict[str, list[inc.DirInfo]]:
    """Thư mục lá của raw kèm mtime: history `source=*/year=*`, streaming `trade_date=*`."""
    roots = {
        inc.HISTORY: f"{table_path('raw', HISTORY_TABLE, base)}/source=*/year=*",
        inc.STREAM: f"{table_path('raw', RAW.name, base)}/trade_date=*",
    }
    out: dict[str, list[inc.DirInfo]] = {}
    for origin, pattern in roots.items():
        items = []
        for path, mtime in files.glob_dirs(pattern):
            name = inc.dir_name(origin, path)
            if name is not None:
                items.append(inc.DirInfo(name, int(mtime)))
        out[origin] = sorted(items, key=lambda d: d.name)
    return out


def list_clean_months(files: Any, clean_root: str) -> set[str]:
    """Tháng đang có ít nhất một file Parquet trong clean."""
    return inc.clean_months_from_files(files.glob_files(f"{clean_root}/year=*/month=*/*.parquet"))


# ---------------------------------------------------------------- job


def _empty_result(mode: str) -> dict[str, Any]:
    return {
        "mode": mode,
        "raw_rows": 0,
        "history_rows": 0,
        "stream_rows": 0,
        "clean_rows": 0,
        "clean_from_history": 0,
        "clean_from_stream_real": 0,
        "clean_from_replay": 0,
        "rejected": 0,
        "dropped_untagged_replay": 0,
        "partitions": 0,
        "clean_by_source": {},
        "months_total": 0,
        "months_candidates": 0,
        "months_rebuilt": [],
        "months_skipped": 0,
        "orphan_months": [],
        "clean_rows_total": None,
        "state_written": False,
        "lock_waited_s": 0.0,
        "lock_lost": False,
        "detect_seconds": 0.0,
        "fingerprint_seconds": 0.0,
        "build_seconds": 0.0,
        "write_seconds": 0.0,
        "total_seconds": 0.0,
    }


def _scan(stream, history, result: dict[str, Any]) -> DataFrame | None:
    """Đọc hai đầu vào MỘT lần: chiếu + union, chia theo (symbol, bar_ts), persist.

    Action duy nhất ở đây (thống kê đầu vào) vật chất hoá cache. Sau đó fingerprint
    (dropDuplicates) và khử trùng (window theo symbol, bar_ts) đọc từ cache; dữ liệu đã chia theo
    (symbol, bar_ts) nên hai bước đó không cần shuffle toàn bộ lần nữa.
    MEMORY_AND_DISK: block không vừa bộ nhớ executor nằm ở đĩa cục bộ, không cần cả raw vừa RAM.
    Trả None (đã unpersist) khi không có dòng nào; còn lại người gọi phải unpersist.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    scan = _combine(stream, history).repartition("symbol", "bar_ts")
    scan = scan.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        s = scan.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col("_origin") == inc.HISTORY, 1).otherwise(0)).alias("history"),
            F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("rejected"),
            F.sum(F.when(F.col("is_valid") & F.col("is_untagged_replay"), 1).otherwise(0)).alias(
                "untagged"
            ),
        ).collect()[0]
    except BaseException:
        scan.unpersist()
        raise
    result["raw_rows"] = int(s["total"] or 0)
    result["history_rows"] = int(s["history"] or 0)
    result["stream_rows"] = result["raw_rows"] - result["history_rows"]
    result["rejected"] = int(s["rejected"] or 0)
    result["dropped_untagged_replay"] = int(s["untagged"] or 0)
    if result["raw_rows"] == 0:
        scan.unpersist()
        return None
    return scan


def _build(scan: DataFrame, result: dict[str, Any]):
    """Khử trùng từ khung đã quét. Trả (best đã persist | None, số dòng clean theo tháng).

    Một action: groupBy(year, month, _prio, source).count() vừa vật chất hoá best vừa cho mọi
    thống kê clean (theo nguồn, theo mức ưu tiên, theo tháng).
    """
    from pyspark import StorageLevel

    keep = (*CLEAN.column_names, "_prio", *CLEAN.derived_partitions)
    best = _dedupe(scan).select(*keep).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rows = best.groupBy("year", "month", "_prio", "source").count().collect()
    except BaseException:
        best.unpersist()
        raise
    by_prio: dict[int, int] = {}
    by_source: dict[str, int] = {}
    by_month: dict[str, int] = {}
    for r in rows:
        n = int(r["count"])
        by_prio[r["_prio"]] = by_prio.get(r["_prio"], 0) + n
        by_source[r["source"]] = by_source.get(r["source"], 0) + n
        key = inc.month_key(r["year"], r["month"])
        by_month[key] = by_month.get(key, 0) + n
    result["clean_from_history"] = by_prio.get(PRIO_HISTORY_VCI, 0) + by_prio.get(
        PRIO_HISTORY_OTHER, 0
    )
    result["clean_from_stream_real"] = by_prio.get(PRIO_STREAM_REAL, 0)
    result["clean_from_replay"] = by_prio.get(PRIO_STREAM_REPLAY, 0)
    result["clean_rows"] = sum(by_prio.values())
    result["clean_by_source"] = by_source
    result["partitions"] = len(by_month)
    if not result["clean_rows"]:
        best.unpersist()
        return None, {}
    return best, by_month


def run_intraday_raw_to_clean(
    spark: SparkSession,
    dates: list[str] | None = None,
    full: bool = False,
    lock_fs: Any = None,
    *,
    base: str = DATA_BASE,
    safety_lag_s: float = inc.SAFETY_LAG_S,
) -> dict[str, Any]:
    """Dựng clean.intraday_1m (phân vùng year, month) từ history + streaming raw.

    - Mặc định (incremental): chỉ dựng lại tháng có đầu vào thay đổi so với `_build_state.json`
      (xem `_incremental.py`). Không có gì đổi: không ghi Spark, không lấy khoá.
    - `full=True`, hoặc chưa có/hỏng/khác phiên bản trạng thái: dựng lại mọi tháng, viết lại
      trạng thái.
    - `dates`: ép dựng lại các tháng chứa những ngày này, bất kể fingerprint.
    full/dates: mọi tháng quét đều dựng lại nên raw chỉ đọc một lần; fingerprint (để ghi trạng
    thái) và clean cùng lấy từ lần quét đó. incremental: quét tháng ứng viên để so fingerprint,
    rồi chỉ đọc lại tháng đổi để dựng.
    Mỗi tháng dựng lại từ MỌI dòng raw của cả hai bảng trong tháng (ghi đè phân vùng động thay cả
    tháng). Tháng mồ côi (hết đầu vào) chỉ báo cáo, không xoá.
    `lock_fs`: None -> khoá trên HDFS; False -> không khoá (test); hoặc một LockFs.
    Trạng thái chỉ ghi SAU khi lệnh ghi Spark thành công. Khoá mất trong lúc ghi -> `lock_lost`.
    Thời gian: detect = tới khi có kế hoạch (full/dates: gồm cả lần quét chung); fingerprint =
    riêng bước tính fingerprint; build = khử trùng + đếm (incremental: gồm đọc tháng đổi);
    write = từ sau kế hoạch tới khi ghi xong trạng thái (gồm build).
    """
    t0 = time.monotonic()
    forced = inc.normalise_months(dates) if dates else []
    files = HadoopFiles(spark)
    clean_root = table_path("clean", CLEAN.name, base)
    state_path = f"{clean_root}/{inc.STATE_FILE}"
    run_id = job_owner(spark)

    # --- phát hiện (ngoài khoá)
    probe_ms = files.now_ms(clean_root)
    dirs = list_input_dirs(files, base)
    clean_months = list_clean_months(files, clean_root)
    prev = inc.parse_state(files.read_text(state_path))
    mode = inc.choose_mode(full, forced, prev)
    if prev is None and not full:
        log.warning("Không có trạng thái hợp lệ tại %s", state_path)
    result = _empty_result(mode)
    candidates = inc.candidate_months(dirs, prev) if mode == inc.MODE_INCREMENTAL else {}
    to_fp = inc.months_to_fingerprint(mode, prev, candidates, clean_months, forced)

    scan = best = None
    try:
        if mode == inc.MODE_INCREMENTAL:
            t_fp = time.monotonic()
            fps = month_fingerprints(*_read_inputs(spark, to_fp, base))
        else:
            # Mọi tháng quét đều dựng lại: một lần quét cho cả fingerprint lẫn clean.
            stream, history = _read_inputs(spark, to_fp, base)
            if stream is not None or history is not None:
                scan = _scan(stream, history, result)
            t_fp = time.monotonic()
            fps = scan_fingerprints(scan) if scan is not None else {}
        result["fingerprint_seconds"] = round(time.monotonic() - t_fp, 3)
        plan = inc.decide(mode, prev, to_fp, fps, clean_months, inc.possible_months(dirs), forced)
        horizon = inc.compute_horizon(probe_ms, dirs, int(safety_lag_s * 1000))
        result["months_candidates"] = len(plan.compared)
        result["orphan_months"] = plan.orphans
        result["detect_seconds"] = round(time.monotonic() - t0, 3)
        if candidates:
            log.info("tháng ứng viên: %s", candidates)
        log.info("kế hoạch: mode=%s dựng lại=%s mồ côi=%s", mode, plan.reasons, plan.orphans)

        def finish(state: dict[str, Any] | None) -> dict[str, Any]:
            known = state if state is not None else prev
            result["clean_rows_total"] = inc.clean_rows_total(known)
            live = set(inc.live_months(known)) | set(plan.rebuild)
            result["months_total"] = len(live)
            result["months_rebuilt"] = list(plan.rebuild)
            result["months_skipped"] = len(live - set(plan.rebuild))
            result["total_seconds"] = round(time.monotonic() - t0, 3)
            log.info("intraday raw->clean: %s", result)
            return result

        def new_state(by_month: dict[str, int]) -> dict[str, Any] | None:
            return inc.next_state(
                prev,
                plan,
                fps,
                by_month,
                horizon_ms=horizon,
                probe_ms=probe_ms,
                dirs=dirs,
                run_id=run_id,
                now_iso=inc.now_iso(),
            )

        def save(state: dict[str, Any] | None) -> dict[str, Any] | None:
            if state is None:
                log.warning(
                    "--dates khi chưa có trạng thái: không ghi trạng thái (lượt sau chạy full)"
                )
                return None
            if mode != inc.MODE_FULL:
                state = inc.merge_state(inc.parse_state(files.read_text(state_path)), state, plan)
            files.write_text_atomic(state_path, inc.dump_state(state))
            result["state_written"] = True
            return state

        if not plan.rebuild:
            state = None
            if inc.needs_state_write(plan, prev, candidates, dirs):
                # Chỉ dời horizon / đánh dấu mồ côi: file nhỏ, không đụng dữ liệu -> không lấy khoá.
                state = save(new_state({}))
            return finish(state)

        # --- dựng lại
        t1 = time.monotonic()
        by_month: dict[str, int] = {}
        if scan is None:  # incremental: chỉ đọc các tháng đổi
            stream, history = _read_inputs(spark, plan.rebuild, base)
            if stream is None and history is None:  # raw biến mất giữa chừng
                log.warning("Không còn raw cho các tháng %s", plan.rebuild)
                return finish(None)
            scan = _scan(stream, history, result)
        if scan is not None:
            best, by_month = _build(scan, result)
            scan.unpersist()  # best đã vật chất hoá: nhả bộ nhớ trước lệnh ghi
            scan = None
        result["build_seconds"] = round(time.monotonic() - t1, 3)
        state = None
        if best is not None:
            clean = select_contract(best, CLEAN)
            n_files = _files_per_partition(result["clean_rows"], result["partitions"])
            with clean_write_lock(spark, CLEAN.name, lock_fs) as handle:
                result["lock_waited_s"] = round(float(handle.waited_s), 3)
                write_partitioned(spark, clean, clean_root, CLEAN.partition_by, n_files)
                state = save(new_state(by_month))
            report_lock_lost(handle, result)
        result["write_seconds"] = round(time.monotonic() - t1, 3)
        return finish(state)
    finally:
        for df in (best, scan):
            if df is not None:
                df.unpersist()
