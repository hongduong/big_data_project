"""Khung Streamlit: cấu hình kết nối, kiểm tra HDFS, schema curated. Không import Spark."""

import streamlit as st
from data import summary_line, table_status, to_vn_time

from stocklake.common import hdfs
from stocklake.common.config import get_settings
from stocklake.contracts import ALL_SCHEMAS, layer_root

st.set_page_config(page_title="Stocklake", layout="wide")
st.title("Stocklake - Dữ liệu chứng khoán Việt Nam")
st.caption("Đồ án Công nghệ dữ liệu lớn: Kafka + Spark + HDFS")

settings = get_settings()

st.header("Cấu hình kết nối")
st.json(
    {
        "kafka_bootstrap": settings.kafka_bootstrap,
        "hdfs_uri": settings.hdfs_uri,
        "webhdfs": f"{settings.webhdfs_host}:{settings.webhdfs_port}",
        "hdfs_user": settings.hdfs_user,
    }
)

st.header("Trạng thái dữ liệu (lớp clean)")
for _kind, _label in (("daily", "Nến ngày clean"), ("intraday", "Nến phút clean")):
    try:
        _s = table_status(_kind)
    except Exception as exc:  # HDFS chưa sẵn sàng hoặc thiếu thư viện
        st.error(f"{_label}: không đọc được ({exc})")
        continue
    if not _s["rows"]:
        st.info(f"{_label}: chưa có dữ liệu.")
        continue
    _first, _last = _s["first"], _s["last"]
    if _kind == "intraday":  # bar_ts là UTC -> giờ Việt Nam
        _first, _last = to_vn_time(_first), to_vn_time(_last)
    st.write(f"**{_label}**: " + summary_line(_s["rows"], _s["symbols"], _first, _last))

st.header("Kiểm tra HDFS")
if st.button("Kiểm tra HDFS"):
    for layer in ("curated", "clean", "raw"):
        path = layer_root(layer)
        st.subheader(path)
        try:
            if not hdfs.exists(path):
                st.warning(f"Đường dẫn chưa tồn tại: {path}")
                continue
            entries = hdfs.ls(path)
            if entries:
                st.write(entries)
            else:
                st.info("Thư mục rỗng.")
        except Exception as exc:  # NameNode không kết nối được, thiếu fsspec, ...
            st.error(f"Không đọc được {path}: {exc}")

st.header("Schema curated")
st.dataframe(
    [
        {
            "bảng": s.name,
            "cột": ", ".join(f"{c.name}:{c.type}" for c in s.columns),
            "khoá chính": ", ".join(s.primary_key),
            "mô tả": s.description,
        }
        for s in ALL_SCHEMAS
        if s.layer == "curated"
    ],
    width="stretch",
)
