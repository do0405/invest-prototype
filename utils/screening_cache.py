from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Mapping

import numpy as np
import pandas as pd

from .io_utils import safe_filename
from .market_runtime import get_market_data_dir, require_market_key


FEATURE_ROW_CACHE_VERSION = "1"


def env_flag_enabled(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
            return None
        return value
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        casted = float(value)
        return None if np.isnan(casted) or np.isinf(casted) else casted
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def stable_payload_hash(payload: Any) -> str:
    encoded = json.dumps(
        _json_safe(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def source_file_signature(source_path: str | os.PathLike[str] | None) -> dict[str, Any]:
    if not source_path:
        return {"path": "", "mtime_ns": 0, "size": 0}
    path = Path(source_path)
    try:
        stat = path.stat()
    except OSError:
        return {"path": str(path), "mtime_ns": 0, "size": 0}
    return {
        "path": str(path),
        "mtime_ns": int(stat.st_mtime_ns),
        "size": int(stat.st_size),
    }


def resolve_ohlcv_source_path(market: str, symbol: str) -> str:
    market_dir = get_market_data_dir(require_market_key(market))
    symbol_key = str(symbol or "").strip().upper()
    candidates = [
        Path(market_dir) / f"{symbol_key}.csv",
        Path(market_dir) / f"{safe_filename(symbol_key)}.csv",
    ]
    for path in candidates:
        if path.exists():
            return str(path)
    return str(candidates[0])


def _feature_cache_root(market: str, namespace: str) -> Path:
    market_dir = Path(get_market_data_dir(require_market_key(market)))
    namespace_key = safe_filename(str(namespace or "default").strip() or "default")
    return market_dir / "cache" / "feature_rows" / namespace_key


def _feature_cache_identity(
    *,
    namespace: str,
    market: str,
    symbol: str,
    as_of: str,
    feature_version: str,
    source_path: str,
    extra_key: Any,
) -> dict[str, Any]:
    return {
        "schema_version": FEATURE_ROW_CACHE_VERSION,
        "namespace": str(namespace or ""),
        "market": require_market_key(market),
        "symbol": str(symbol or "").strip().upper(),
        "as_of": str(as_of or "").strip(),
        "feature_version": str(feature_version or ""),
        "source": source_file_signature(source_path),
        "extra_hash": stable_payload_hash(extra_key or {}),
    }


def feature_row_cache_get_or_compute(
    *,
    namespace: str,
    market: str,
    symbol: str,
    as_of: str,
    feature_version: str,
    source_path: str,
    compute_fn: Callable[[], dict[str, Any]],
    runtime_context: Any | None = None,
    extra_key: Any | None = None,
) -> dict[str, Any]:
    if not env_flag_enabled("INVEST_PROTO_FEATURE_ROW_CACHE", default=True):
        return compute_fn()

    started = time.perf_counter()
    market_key = require_market_key(market)
    symbol_key = str(symbol or "").strip().upper()
    identity = _feature_cache_identity(
        namespace=namespace,
        market=market_key,
        symbol=symbol_key,
        as_of=as_of,
        feature_version=feature_version,
        source_path=source_path,
        extra_key=extra_key,
    )
    cache_dir = _feature_cache_root(market_key, namespace)
    cache_path = cache_dir / f"{safe_filename(symbol_key)}.json"
    try:
        if cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and cached.get("identity") == identity and isinstance(cached.get("row"), dict):
                if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
                    runtime_context.add_runtime_metric("feature_analysis", "cache_hits", 1)
                    runtime_context.add_runtime_metric("feature_analysis", "cache_seconds", time.perf_counter() - started)
                return dict(cached["row"])
    except Exception as exc:
        print(f"[Cache] Feature row cache read skipped ({market_key}:{symbol_key}) - {exc}")

    row = compute_fn()
    if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
        runtime_context.add_runtime_metric("feature_analysis", "cache_misses", 1)
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(
                {"identity": identity, "row": _json_safe(row)},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        os.replace(tmp_path, cache_path)
    except Exception as exc:
        print(f"[Cache] Feature row cache write skipped ({market_key}:{symbol_key}) - {exc}")
    if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
        runtime_context.add_runtime_metric("feature_analysis", "cache_seconds", time.perf_counter() - started)
    return dict(row)
