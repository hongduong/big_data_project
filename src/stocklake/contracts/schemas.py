"""Schema trung lập (Python thuần) cho mọi topic và bảng. Không import thư viện ngoài."""

from __future__ import annotations

from dataclasses import dataclass

TYPES = ("string", "int", "long", "double", "boolean", "date", "timestamp", "array<string>")
LAYERS = ("topic", "raw", "clean", "curated")


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    description: str = ""

    def __post_init__(self) -> None:
        if self.type not in TYPES:
            raise ValueError(f"Kiểu không hợp lệ: {self.type!r} (cột {self.name})")


@dataclass(frozen=True)
class TableSchema:
    name: str
    layer: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    partition_by: tuple[str, ...] = ()
    description: str = ""
    # Cột phân vùng suy ra khi ghi (vd year, month), không có trong columns.
    derived_partitions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.layer not in LAYERS:
            raise ValueError(f"Layer không hợp lệ: {self.layer!r}")
        names = [c.name for c in self.columns]
        if len(set(names)) != len(names):
            raise ValueError(f"{self.layer}.{self.name}: tên cột trùng")
        for k in self.primary_key:
            if k not in names:
                raise ValueError(f"{self.layer}.{self.name}: khoá {k!r} không phải cột")
        for d in self.derived_partitions:
            if d in names:
                raise ValueError(f"{self.layer}.{self.name}: {d!r} đã là cột thật")
        for p in self.partition_by:
            if p not in names and p not in self.derived_partitions:
                raise ValueError(f"{self.layer}.{self.name}: phân vùng {p!r} không tồn tại")

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


def _c(name: str, type_: str, nullable: bool = False, description: str = "") -> Column:
    # Mặc định not null cho gọn; truyền nullable=True khi cho phép null.
    return Column(name, type_, nullable, description)


_META_RAW = (
    _c("source", "string", description="Nguồn dữ liệu"),
    _c("ingest_ts", "timestamp", description="Lúc ghi vào hệ thống (UTC)"),
    _c("available_at", "timestamp", description="Lúc dữ liệu được coi là đã biết (UTC)"),
)

_OHLC = tuple(_c(n, "double") for n in ("open", "high", "low", "close"))

# ---------------------------------------------------------------- topics

INTRADAY_PRICES_TOPIC = TableSchema(
    name="intraday_prices",
    layer="topic",
    columns=(
        _c("symbol", "string"),
        _c("bar_ts", "long", description="Epoch ms, thời điểm mở nến"),
        *_OHLC,
        _c("volume", "long"),
        _c("source", "string", description="vci, vndirect, kbs, replay"),
        _c("produced_ts", "long", description="Epoch ms, lúc producer gửi"),
        _c("available_at", "long", description="Epoch ms"),
        _c("replay_run_id", "string", nullable=True, description="Chỉ có khi phát lại"),
    ),
    primary_key=("symbol", "bar_ts", "source"),
    description="Nến 1 phút realtime; key = symbol, 6 partition",
)

MARKET_NEWS_TOPIC = TableSchema(
    name="market_news",
    layer="topic",
    columns=(
        _c("news_id", "string", description="SHA-1 của link"),
        _c("feed", "string", description="cafef, vietstock, vnexpress"),
        _c("title", "string"),
        _c("link", "string"),
        _c("summary", "string", nullable=True),
        _c("published_ts", "long", description="Epoch ms"),
        _c("produced_ts", "long", description="Epoch ms"),
        _c("available_at", "long", description="Epoch ms"),
    ),
    primary_key=("news_id",),
    description="Tin tức RSS; key = news_id, 1 partition",
)

# ---------------------------------------------------------------- raw

_FLOW_COLS = (
    _c("buy_vol", "long"),
    _c("sell_vol", "long"),
    _c("net_vol", "long"),
    _c("buy_val", "double"),
    _c("sell_val", "double"),
    _c("net_val", "double"),
    _c("room_left", "double", nullable=True),
)

RAW_DAILY_OHLCV = TableSchema(
    name="daily_ohlcv",
    layer="raw",
    columns=(
        _c("symbol", "string"),
        _c("trade_date", "date"),
        *_OHLC,
        _c("volume", "long"),
        _c("raw_close", "double", nullable=True, description="Null nếu nguồn không có"),
        _c("price_unit", "string", description="Đơn vị giá gốc của nguồn"),
        *_META_RAW,
    ),
    primary_key=("symbol", "trade_date", "source"),
    partition_by=("source", "year"),
    derived_partitions=("year",),
    description="Nến ngày theo đơn vị gốc của nguồn",
)

RAW_FOREIGN_FLOW = TableSchema(
    name="foreign_flow",
    layer="raw",
    columns=(_c("symbol", "string"), _c("trade_date", "date"), *_FLOW_COLS, *_META_RAW),
    primary_key=("symbol", "trade_date", "source"),
    partition_by=("source", "year"),
    derived_partitions=("year",),
    description="Giao dịch khối ngoại theo ngày",
)

_PROFILE_COLS = (
    _c("symbol", "string"),
    _c("snapshot_date", "date"),
    _c("exchange", "string", nullable=True),
    _c("company_name", "string", nullable=True),
    _c("icb_code", "string", nullable=True),
    _c("listed_date", "date", nullable=True),
)

RAW_COMPANY_PROFILE = TableSchema(
    name="company_profile",
    layer="raw",
    columns=(*_PROFILE_COLS, *_META_RAW),
    primary_key=("symbol", "snapshot_date"),
    partition_by=("snapshot_date",),
    description="Ảnh chụp hồ sơ doanh nghiệp, không dùng làm feature lịch sử",
)

RAW_MACRO_DAILY = TableSchema(
    name="macro_daily",
    layer="raw",
    columns=(
        _c("series_id", "string"),
        _c("obs_date", "date"),
        _c("value", "double", nullable=True),
        *_META_RAW,
    ),
    primary_key=("series_id", "obs_date", "source"),
    partition_by=("source",),
    description="Chuỗi vĩ mô hằng ngày (yfinance, FRED)",
)

_KAFKA_COLS = (
    _c("kafka_partition", "int"),
    _c("kafka_offset", "long"),
    _c("kafka_ts", "timestamp"),
    _c("spark_batch_ts", "timestamp"),
)

RAW_INTRADAY_PRICES = TableSchema(
    name="intraday_prices",
    layer="raw",
    columns=(
        _c("symbol", "string"),
        _c("bar_ts", "timestamp"),
        *_OHLC,
        _c("volume", "long"),
        _c("produced_ts", "timestamp"),
        _c("replay_run_id", "string", nullable=True),
        _c("trade_date", "date", description="Ngày theo giờ VN"),
        *_META_RAW,
        *_KAFKA_COLS,
    ),
    primary_key=("symbol", "bar_ts", "source"),
    partition_by=("trade_date",),
    description="Nến 1 phút từ Kafka; cột thời gian đã đổi sang timestamp",
)

RAW_MARKET_NEWS = TableSchema(
    name="market_news",
    layer="raw",
    columns=(
        _c("news_id", "string"),
        _c("feed", "string"),
        _c("title", "string"),
        _c("link", "string"),
        _c("summary", "string", nullable=True),
        _c("published_ts", "timestamp"),
        _c("produced_ts", "timestamp"),
        _c("ingest_date", "date"),
        *_META_RAW,
        *_KAFKA_COLS,
    ),
    primary_key=("news_id",),
    partition_by=("ingest_date",),
    description="Tin tức từ Kafka; source = feed",
)

# ---------------------------------------------------------------- clean

CLEAN_DAILY_OHLCV = TableSchema(
    name="daily_ohlcv",
    layer="clean",
    columns=(
        _c("symbol", "string"),
        _c("trade_date", "date"),
        *_OHLC,
        _c("raw_close", "double", nullable=True),
        _c("volume", "long"),
        _c("source", "string", description="Nguồn được chọn"),
        _c("available_at", "timestamp"),
        _c("dq_flags", "array<string>"),
    ),
    primary_key=("symbol", "trade_date"),
    partition_by=("year",),
    derived_partitions=("year",),
    description="Giá đồng, đã điều chỉnh; sắp xếp theo symbol",
)

CLEAN_FOREIGN_FLOW = TableSchema(
    name="foreign_flow",
    layer="clean",
    columns=(_c("symbol", "string"), _c("trade_date", "date"), *_FLOW_COLS, *_META_RAW),
    primary_key=("symbol", "trade_date"),
    partition_by=("year",),
    derived_partitions=("year",),
    description="Như raw, đã chuẩn hoá kiểu",
)

CLEAN_COMPANY_PROFILE = TableSchema(
    name="company_profile",
    layer="clean",
    columns=(*_PROFILE_COLS, *_META_RAW),
    primary_key=("symbol",),
    description="Bản mới nhất của từng mã",
)

CLEAN_MACRO_DAILY = TableSchema(
    name="macro_daily",
    layer="clean",
    columns=(
        _c("series_id", "string"),
        _c("obs_date", "date"),
        _c("value", "double", nullable=True),
        _c("available_at", "timestamp"),
    ),
    primary_key=("series_id", "obs_date"),
    description="Chuỗi vĩ mô đã khử trùng",
)

CLEAN_INTRADAY_1M = TableSchema(
    name="intraday_1m",
    layer="clean",
    columns=(
        _c("symbol", "string"),
        _c("bar_ts", "timestamp"),
        *_OHLC,
        _c("volume", "long"),
        _c("trade_date", "date"),
        _c("available_at", "timestamp"),
    ),
    primary_key=("symbol", "bar_ts"),
    partition_by=("year", "month"),
    derived_partitions=("year", "month"),
    description="Nến 1 phút đã khử trùng; sắp xếp theo symbol",
)

CLEAN_MARKET_NEWS = TableSchema(
    name="market_news",
    layer="clean",
    columns=(
        _c("news_id", "string"),
        _c("feed", "string"),
        _c("title", "string"),
        _c("link", "string"),
        _c("summary", "string", nullable=True),
        _c("published_ts", "timestamp"),
        _c("produced_ts", "timestamp"),
        *_META_RAW,
    ),
    primary_key=("news_id",),
    partition_by=("year", "month"),
    derived_partitions=("year", "month"),
    description="Tin tức đã khử trùng",
)

CLEAN_TRADING_CALENDAR = TableSchema(
    name="trading_calendar",
    layer="clean",
    columns=(_c("trade_date", "date"), _c("is_trading_day", "boolean")),
    primary_key=("trade_date",),
    description="Lịch giao dịch suy ra từ VNINDEX",
)

# ---------------------------------------------------------------- curated

CURATED_MARKET_DAILY = TableSchema(
    name="market_daily",
    layer="curated",
    columns=(
        _c("trade_date", "date"),
        _c("advancers", "int", description="Số mã tăng"),
        _c("decliners", "int", description="Số mã giảm"),
        _c("unchanged", "int", description="Số mã đứng giá"),
        _c("total_volume", "long"),
        _c("total_value", "double"),
        _c("foreign_net_val", "double", nullable=True, description="Khối ngoại ròng toàn sàn"),
        _c("vnindex_close", "double", nullable=True),
    ),
    primary_key=("trade_date",),
    description="Tổng hợp thị trường theo ngày",
)

CURATED_SYMBOL_DAILY_FEATURES = TableSchema(
    name="symbol_daily_features",
    layer="curated",
    columns=(
        _c("symbol", "string"),
        _c("trade_date", "date"),
        _c("ret_1d", "double", nullable=True),
        _c("ret_5d", "double", nullable=True),
        _c("ret_20d", "double", nullable=True),
        _c("vol_20d", "double", nullable=True, description="Độ biến động 20 ngày"),
        _c("volume_ratio_20d", "double", nullable=True, description="KL / TB 20 ngày"),
        _c("foreign_net_5d", "double", nullable=True, description="Khối ngoại ròng 5 ngày"),
        _c("macro_vol_20d", "double", nullable=True, description="Biến động vĩ mô"),
        _c("feature_available_at", "timestamp", description="Max available_at của đầu vào"),
        _c("label_up_t5", "int", nullable=True, description="1 nếu giá sau 5 phiên tăng"),
    ),
    primary_key=("symbol", "trade_date"),
    description="Bảng feature cho mô hình",
)

CURATED_DQ_REPORT = TableSchema(
    name="dq_report",
    layer="curated",
    columns=(
        _c("run_id", "string"),
        _c("table_name", "string"),
        _c("check_name", "string"),
        _c("metric_value", "double", nullable=True),
        _c("threshold", "double", nullable=True),
        _c("passed", "boolean"),
        _c("run_ts", "timestamp"),
    ),
    primary_key=("run_id", "table_name", "check_name"),
    description="Kết quả kiểm tra chất lượng dữ liệu",
)

CURATED_ML_RESULTS = TableSchema(
    name="ml_results",
    layer="curated",
    columns=(
        _c("run_id", "string"),
        _c("scenario", "string"),
        _c("model", "string"),
        _c("accuracy", "double", nullable=True),
        _c("f1_macro", "double", nullable=True),
        _c("auc", "double", nullable=True),
        _c("train_start", "date"),
        _c("train_end", "date"),
        _c("test_start", "date"),
        _c("test_end", "date"),
    ),
    primary_key=("run_id", "scenario", "model"),
    description="Kết quả mô hình baseline và leakage test",
)

CURATED_BENCHMARK_RESULTS = TableSchema(
    name="benchmark_results",
    layer="curated",
    columns=(
        _c("suite", "string"),
        _c("run_id", "string"),
        _c("case", "string"),
        _c("metric", "string"),
        _c("value", "double"),
        _c("unit", "string", nullable=True),
        _c("config", "string", nullable=True, description="Cấu hình đo, dạng JSON"),
        _c("run_ts", "timestamp"),
    ),
    primary_key=("suite", "run_id", "case", "metric"),
    description="Số đo benchmark",
)

ALL_SCHEMAS: tuple[TableSchema, ...] = (
    INTRADAY_PRICES_TOPIC,
    MARKET_NEWS_TOPIC,
    RAW_DAILY_OHLCV,
    RAW_FOREIGN_FLOW,
    RAW_COMPANY_PROFILE,
    RAW_MACRO_DAILY,
    RAW_INTRADAY_PRICES,
    RAW_MARKET_NEWS,
    CLEAN_DAILY_OHLCV,
    CLEAN_FOREIGN_FLOW,
    CLEAN_COMPANY_PROFILE,
    CLEAN_MACRO_DAILY,
    CLEAN_INTRADAY_1M,
    CLEAN_MARKET_NEWS,
    CLEAN_TRADING_CALENDAR,
    CURATED_MARKET_DAILY,
    CURATED_SYMBOL_DAILY_FEATURES,
    CURATED_DQ_REPORT,
    CURATED_ML_RESULTS,
    CURATED_BENCHMARK_RESULTS,
)

_INDEX = {(s.layer, s.name): s for s in ALL_SCHEMAS}
if len(_INDEX) != len(ALL_SCHEMAS):
    raise RuntimeError("Trùng (layer, name) trong ALL_SCHEMAS")


def get_schema(layer: str, name: str) -> TableSchema:
    """Lấy schema theo (layer, name); KeyError nếu không có."""
    try:
        return _INDEX[(layer, name)]
    except KeyError:
        raise KeyError(f"Không có schema {layer}.{name}") from None
