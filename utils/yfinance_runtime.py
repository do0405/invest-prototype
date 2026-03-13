from __future__ import annotations

import os

from config import EXTERNAL_DATA_DIR
from utils.io_utils import ensure_dir


def bootstrap_yfinance_cache() -> str:
    cache_dir = os.path.join(EXTERNAL_DATA_DIR, "yfinance_cache")
    ensure_dir(cache_dir)

    try:
        import yfinance as yf  # type: ignore

        if hasattr(yf, "set_tz_cache_location"):
            yf.set_tz_cache_location(cache_dir)
        else:
            from yfinance import cache as yf_cache  # type: ignore

            if hasattr(yf_cache, "set_cache_location"):
                yf_cache.set_cache_location(cache_dir)
    except Exception:
        pass

    return cache_dir
