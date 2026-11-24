"""Khung Streamlit: cấu hình kết nối, kiểm tra HDFS, schema curated. Không import Spark."""

import streamlit as st

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
    use_container_width=True,
)
