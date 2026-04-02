from __future__ import annotations

import importlib
import os
from typing import Any, Callable, Sequence

import pandas as pd

from screeners.signals import source_registry as signal_source_registry
from screeners.source_contracts import (
    CANONICAL_SOURCE_SPECS,
    primary_source_style,
    source_engine_bonus,
    source_priority_score,
    source_style_tags,
    source_tag_priority,
    sorted_source_tags,
    stage_priority,
)
from utils.market_data_contract import load_local_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_augment_results_dir,
    get_market_screeners_root,
    market_key,
)
from utils.screener_utils import save_screening_results
from utils.symbol_normalization import (
    normalize_provider_symbol_value,
    normalize_symbol_columns,
    normalize_symbol_value,
)

from .chronos_rerank import generate_chronos_rerank_rows
from .stumpy_sidecar import generate_stumpy_summary_rows


def build_buy_eligible_source_specs():
    return [spec for spec in CANONICAL_SOURCE_SPECS if spec.buy_eligible]


def build_merged_candidate_pool_rows(
    registry: dict[str, dict[str, Any]],
    *,
    market: str,
) -> list[dict[str, Any]]:
    normalized_market = market_key(market).upper()
    rows: list[dict[str, Any]] = []
    for symbol in sorted(registry):
        entry = registry[symbol]
        rows.append(
            {
                "symbol": str(entry.get("symbol") or symbol),
                "market": str(entry.get("market") or normalized_market).upper(),
                "buy_eligible": bool(entry.get("buy_eligible")),
                "watch_only": bool(entry.get("watch_only")),
                "screen_stage": str(entry.get("screen_stage") or ""),
                "source_tags": list(entry.get("source_tags", [])),
                "primary_source_tag": str(entry.get("primary_source_tag") or ""),
                "primary_source_stage": str(entry.get("primary_source_stage") or ""),
                "primary_source_style": str(entry.get("primary_source_style") or ""),
                "source_style_tags": list(entry.get("source_style_tags", [])),
                "source_priority_score": float(entry.get("source_priority_score") or 0.0),
                "trend_source_bonus": float(entry.get("trend_source_bonus") or 0.0),
                "ug_source_bonus": float(entry.get("ug_source_bonus") or 0.0),
                "source_overlap_bonus": float(entry.get("source_overlap_bonus") or 0.0),
                "sector": str(entry.get("sector") or ""),
                "industry": str(entry.get("industry") or ""),
                "group_name": str(entry.get("group_name") or ""),
                "as_of_ts": entry.get("as_of_ts"),
            }
        )
    return rows


def _write_records(output_dir: str, filename_prefix: str, rows: list[dict[str, Any]]) -> None:
    save_screening_results(
        results=rows,
        output_dir=output_dir,
        filename_prefix=filename_prefix,
        include_timestamp=False,
        incremental_update=False,
    )


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _load_raw_source_rows(
    *,
    screeners_root: str,
    market: str,
    source_specs: Sequence[Any],
) -> list[dict[str, Any]]:
    normalized_market = market_key(market)
    payloads: list[dict[str, Any]] = []
    for spec in source_specs:
        path = os.path.join(screeners_root, spec.relative_path)
        if not os.path.exists(path):
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if frame.empty:
            continue
        frame.columns = [str(column).strip() for column in frame.columns]
        symbol_column = signal_source_registry.resolve_symbol_column(frame)
        if symbol_column is None:
            continue
        if symbol_column == "provider_symbol":
            frame = normalize_symbol_columns(
                frame,
                normalized_market,
                provider_columns=(symbol_column,),
            )
        else:
            frame = normalize_symbol_columns(
                frame,
                normalized_market,
                columns=(symbol_column,),
            )
        rows: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            raw_symbol = row.get(symbol_column)
            if symbol_column == "provider_symbol":
                provider_symbol = normalize_provider_symbol_value(raw_symbol)
                if normalized_market == "kr" and provider_symbol and "." in provider_symbol:
                    base, _suffix = provider_symbol.rsplit(".", 1)
                    symbol = normalize_symbol_value(base, normalized_market)
                else:
                    symbol = provider_symbol
            else:
                symbol = normalize_symbol_value(raw_symbol, normalized_market)
            if not symbol:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "market": normalized_market.upper(),
                    "source_tag": spec.source_tag,
                    "screen_stage": spec.screen_stage,
                    "relative_path": spec.relative_path,
                    "as_of_ts": _safe_text(row.get("as_of_ts") or row.get("date")) or None,
                }
            )
        if rows:
            payloads.append({"spec": spec, "path": path, "rows": rows})
    return payloads


def _validate_optional_dependencies() -> None:
    missing: list[str] = []
    for module_name in ("stumpy", "chronos", "torch", "transformers"):
        try:
            importlib.import_module(module_name)
        except Exception:
            missing.append(module_name)
    if missing:
        missing_csv = ", ".join(missing)
        raise RuntimeError(
            "Screening augment optional dependencies are missing "
            f"({missing_csv}). Install with `.\\.venv\\Scripts\\python -m pip install -r requirements-augment.txt`."
        )


def run_screening_augment(
    *,
    market: str,
    load_ohlcv_frame_fn: Callable[..., pd.DataFrame] = load_local_ohlcv_frame,
) -> dict[str, Any]:
    normalized_market = market_key(market)
    ensure_market_dirs(normalized_market)
    _validate_optional_dependencies()

    screeners_root = get_market_screeners_root(normalized_market)
    augment_dir = get_augment_results_dir(normalized_market)
    os.makedirs(augment_dir, exist_ok=True)

    buy_specs = build_buy_eligible_source_specs()
    raw_payloads = _load_raw_source_rows(
        screeners_root=screeners_root,
        market=normalized_market,
        source_specs=buy_specs,
    )

    registry = signal_source_registry.load_source_registry(
        screeners_root=screeners_root,
        market=normalized_market,
        source_specs=buy_specs,
        stage_priority=stage_priority,
        source_tag_priority=source_tag_priority,
        sorted_source_tags=sorted_source_tags,
        source_style_tags=source_style_tags,
        primary_source_style=primary_source_style,
        source_priority_score=source_priority_score,
        source_engine_bonus=source_engine_bonus,
        safe_text=_safe_text,
    )
    merged_rows = build_merged_candidate_pool_rows(registry, market=normalized_market)
    _write_records(augment_dir, "merged_candidate_pool", merged_rows)

    total_stumpy_rows = 0
    for payload in raw_payloads:
        spec = payload["spec"]
        source_rows = payload["rows"]
        stumpy_rows = generate_stumpy_summary_rows(
            source_rows=source_rows,
            source_tag=spec.source_tag,
            market=normalized_market,
            load_ohlcv_frame_fn=load_ohlcv_frame_fn,
        )
        total_stumpy_rows += len(stumpy_rows)
        source_dir = os.path.dirname(payload["path"])
        stem = os.path.splitext(os.path.basename(payload["path"]))[0]
        _write_records(source_dir, f"{stem}_stumpy_summary", stumpy_rows)

    rerank_rows = generate_chronos_rerank_rows(
        merged_candidate_rows=merged_rows,
        market=normalized_market,
        load_ohlcv_frame_fn=load_ohlcv_frame_fn,
    )
    _write_records(augment_dir, "chronos2_rerank", rerank_rows)

    return {
        "market": normalized_market,
        "ok": True,
        "raw_source_files": len(raw_payloads),
        "merged_candidates": len(merged_rows),
        "stumpy_rows": total_stumpy_rows,
        "chronos_rows": len(rerank_rows),
    }
