"""VNDirect finfo (api-finfo.vndirect.com.vn): nến ngày từ 02/01/2013, có cả giá gốc
(`close`) và giá điều chỉnh (`adClose`), đơn vị nghìn đồng; khối ngoại `foreigns` từ 30/08/2018
(gồm VNINDEX); `stocks` (danh sách mã); `events` (sự kiện, có `disclosureDate`).
Nến phút dchart: FPT từ 11/2021, thưa hơn VCI. Planning mục 0.2."""

from __future__ import annotations

from stocklake.sources.base import SourceClient


class VndirectClient(SourceClient):
    name = "vndirect"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Lát 1/2")
