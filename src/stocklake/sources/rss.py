"""RSS tin tức CafeF, Vietstock, VnExpress: mỗi feed 20-60 tin, có `pubDate`
(dùng làm `available_at`). Import feedparser phải nằm trong hàm. Planning mục 0.2."""

from __future__ import annotations

from stocklake.sources.base import SourceClient


class RssClient(SourceClient):
    name = "rss"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("Lát 1/2")
