"""Logger dùng chung, giờ UTC."""

from __future__ import annotations

import logging
import os
import time

_FORMAT = "%(asctime)sZ %(levelname)s %(name)s: %(message)s"
_configured = False


def _configure() -> None:
    global _configured
    if _configured:
        return
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(_FORMAT)
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    root = logging.getLogger("stocklake")
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Trả logger nằm dưới namespace 'stocklake'."""
    _configure()
    if not name.startswith("stocklake"):
        name = f"stocklake.{name}"
    return logging.getLogger(name)
