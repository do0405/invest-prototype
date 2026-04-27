from __future__ import annotations

import os

from config import BASE_DIR, EXTERNAL_DATA_DIR
from utils.io_utils import ensure_dir


def _resolve_yfinance_cache_dir() -> str:
    explicit = str(os.environ.get("INVEST_PROTO_YFINANCE_CACHE_DIR") or "").strip()
    if explicit:
        if os.path.isabs(explicit):
            return explicit
        return os.path.abspath(os.path.join(BASE_DIR, explicit))

    results_override = str(os.environ.get("INVEST_PROTO_RESULTS_DIR") or "").strip()
    if results_override:
        base = results_override if os.path.isabs(results_override) else os.path.join(BASE_DIR, results_override)
        return os.path.abspath(os.path.join(base, "_cache", "yfinance"))

    return os.path.join(EXTERNAL_DATA_DIR, "yfinance_cache")


def bootstrap_yfinance_cache() -> str:
    cache_dir = _resolve_yfinance_cache_dir()
    try:
        ensure_dir(cache_dir)
    except OSError:
        # Cache bootstrap is an optimization; runtime should continue without it.
        return cache_dir

    try:
        import yfinance as yf  # type: ignore

        if hasattr(yf, "set_tz_cache_location"):
            yf.set_tz_cache_location(cache_dir)
        from yfinance import cache as yf_cache  # type: ignore

        if hasattr(yf_cache, "set_cache_location"):
            yf_cache.set_cache_location(cache_dir)
        if hasattr(yf_cache, "set_tz_cache_location"):
            yf_cache.set_tz_cache_location(cache_dir)
    except Exception:
        pass

    return cache_dir
