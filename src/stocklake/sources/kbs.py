"""KBS (kbbuddywts.kbsec.com.vn): nến ngày khoảng 10 năm gần nhất (FPT từ 09/2016),
giá điều chỉnh; nến 1 phút khoảng 3 tháng gần nhất (~14.460 dòng). Planning mục 0.2."""

from __future__ import annotations

from stocklake.sources.base import SourceClient


class KbsClient(SourceClient):
    name = "kbs"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Lát 1/2")
