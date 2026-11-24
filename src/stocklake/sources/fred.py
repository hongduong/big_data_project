"""FRED `fredgraph.csv` (không cần khoá API): vd DGS10. Lần thử đầu từng timeout 30 giây,
nên đặt timeout rộng và retry. Planning mục 0.2."""

from __future__ import annotations

from stocklake.sources.base import SourceClient


class FredClient(SourceClient):
    name = "fred"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Lát 1/2")
