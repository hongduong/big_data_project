"""Yahoo Finance qua yfinance: vĩ mô thế giới `^TNX`, `DX-Y.NYB`, `BZ=F`, `^GSPC`, `^VIX`,
`VND=X`, có từ 01/2015 (~2.990 dòng/chuỗi). Import yfinance phải nằm trong hàm.
Planning mục 0.2."""

from __future__ import annotations

from stocklake.sources.base import SourceClient


class YahooClient(SourceClient):
    name = "yahoo"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Lát 1/2")
