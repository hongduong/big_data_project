"""CafeF (endpoint JSON): khối ngoại theo mã theo ngày, có dữ liệu từ 2007; mỗi request
tối đa 30 dòng, cửa sổ khoảng 1 năm. Cách điều chỉnh giá khác VCI/VNDirect (dùng đối chiếu
chéo). RSS tin tức nằm ở `rss.py`. Planning mục 0.2/0.3."""

from __future__ import annotations

from stocklake.sources.base import SourceClient


class CafefClient(SourceClient):
    name = "cafef"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Lát 1/2")
