from __future__ import annotations

import os
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

from utils.io_utils import safe_filename
from utils.market_runtime import get_financial_cache_dir, get_stock_metadata_path, market_key
from utils.symbol_normalization import (
    normalize_provider_symbol_value,
    normalize_symbol_columns,
    normalize_symbol_value,
)
from utils.typing_utils import frame_keyed_records, row_to_record


def resolve_symbol_column(frame: pd.DataFrame) -> str | None:
    for candidate in ("symbol", "ticker", "provider_symbol"):
        if candidate in frame.columns:
            return candidate
    return None


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def _default_entry(symbol: str, screen_stage: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "buy_eligible": False,
        "watch_only": False,
        "screen_stage": screen_stage,
        "source_tags": [],
        "source_records": [],
        "sector": "",
        "industry": "",
        "group_name": "",
        "as_of_ts": None,
        "industry_key": "",
        "group_state": "",
        "leader_state": "",
        "breakdown_status": "",
        "group_strength_score": None,
        "leader_score": None,
        "breakdown_score": None,
        "market_condition_state": "",
        "market_condition_reason": "",
        "market_alignment_score": None,
        "breadth_support_score": None,
        "rotation_support_score": None,
        "leader_health_score": None,
        "regime_state": "",
        "top_state": "",
        "market_state": "",
        "breadth_state": "",
        "concentration_state": "",
        "leadership_state": "",
    }


def load_metadata_map(
    market: str,
    *,
    get_stock_metadata_path_fn: Callable[[str], str] = get_stock_metadata_path,
) -> dict[str, dict[str, Any]]:
    path = get_stock_metadata_path_fn(market)
    if not os.path.exists(path):
        return {}
    normalized_market = market_key(market)
    frame = pd.read_csv(path, dtype={"symbol": "string"})
    if frame.empty or "symbol" not in frame.columns:
        return {}
    frame = normalize_symbol_columns(frame, normalized_market, columns=("symbol",))
    return frame_keyed_records(
        frame, key_column="symbol", uppercase_keys=True, drop_na=True
    )


def financial_cache_lookup_paths(
    symbol: str,
    market: str,
    *,
    get_financial_cache_dir_fn: Callable[[str], str] = get_financial_cache_dir,
) -> list[str]:
    normalized_market = market_key(market)
    directory = get_financial_cache_dir_fn(normalized_market)
    normalized_symbol = normalize_symbol_value(symbol, normalized_market)
    raw_symbol = str(symbol or "").strip().upper()
    candidates: list[str] = []

    def _add(symbol_text: str) -> None:
        text = str(symbol_text or "").strip().upper()
        if not text:
            return
        path = os.path.join(directory, f"{safe_filename(text)}.csv")
        if path not in candidates:
            candidates.append(path)

    _add(normalized_symbol)
    _add(raw_symbol)
    if normalized_market == "kr" and normalized_symbol.isdigit():
        _add(normalized_symbol.lstrip("0") or "0")
    return candidates


def load_financial_map(
    market: str,
    symbols: Iterable[str] | None = None,
    *,
    get_financial_cache_dir_fn: Callable[[str], str] = get_financial_cache_dir,
) -> dict[str, dict[str, Any]]:
    normalized_market = market_key(market)
    directory = get_financial_cache_dir_fn(normalized_market)
    if not os.path.isdir(directory):
        return {}

    if symbols is None:
        candidate_paths = [
            os.path.join(directory, filename)
            for filename in sorted(os.listdir(directory))
            if filename.lower().endswith(".csv")
        ]
    else:
        candidate_paths = []
        seen_paths: set[str] = set()
        for symbol in sorted(
            {
                normalize_symbol_value(item, normalized_market)
                for item in symbols
                if str(item).strip()
            }
        ):
            if not symbol:
                continue
            resolved_path = next(
                (
                    path
                    for path in financial_cache_lookup_paths(
                        symbol,
                        normalized_market,
                        get_financial_cache_dir_fn=get_financial_cache_dir_fn,
                    )
                    if os.path.exists(path)
                ),
                None,
            )
            if resolved_path is None or resolved_path in seen_paths:
                continue
            candidate_paths.append(resolved_path)
            seen_paths.add(resolved_path)

    records: dict[str, dict[str, Any]] = {}
    for path in candidate_paths:
        try:
            frame = pd.read_csv(
                path, nrows=1, dtype={"symbol": "string", "provider_symbol": "string"}
            )
        except Exception:
            continue
        if frame.empty:
            continue
        row = row_to_record(frame.iloc[0])
        symbol = normalize_symbol_value(row.get("symbol"), normalized_market)
        if not symbol:
            symbol = normalize_symbol_value(
                os.path.splitext(os.path.basename(path))[0], normalized_market
            )
        if not symbol:
            continue
        provider_symbol = normalize_provider_symbol_value(row.get("provider_symbol"))
        if normalized_market == "kr" and provider_symbol and "." in provider_symbol:
            base, suffix = provider_symbol.rsplit(".", 1)
            provider_symbol = f"{normalize_symbol_value(base, normalized_market)}.{suffix}"
        row["symbol"] = symbol.upper()
        row["provider_symbol"] = provider_symbol or None
        records[symbol.upper()] = row
    return records


def _merge_registry_row(
    *,
    registry: dict[str, dict[str, Any]],
    symbol: str,
    incoming: Mapping[str, Any],
    stage_priority: Callable[[str], int],
    safe_text: Callable[[Any], str],
) -> None:
    screen_stage = str(incoming.get("screen_stage") or "UNIVERSE")
    entry = registry.setdefault(symbol, _default_entry(symbol, screen_stage))
    entry["buy_eligible"] = bool(entry["buy_eligible"] or incoming.get("buy_eligible"))
    entry["watch_only"] = bool(entry["watch_only"] or incoming.get("watch_only"))
    for tag in incoming.get("source_tags", []) or []:
        if str(tag).strip():
            entry["source_tags"].append(str(tag).strip())
    for record in incoming.get("source_records", []) or []:
        if isinstance(record, dict):
            entry["source_records"].append(dict(record))
    if stage_priority(screen_stage) > stage_priority(str(entry.get("screen_stage") or "")):
        entry["screen_stage"] = screen_stage
    if not entry.get("sector"):
        entry["sector"] = safe_text(incoming.get("sector"))
    if not entry.get("industry"):
        entry["industry"] = safe_text(incoming.get("industry"))
    if not entry.get("group_name"):
        entry["group_name"] = safe_text(
            incoming.get("group_name") or incoming.get("industry") or incoming.get("sector")
        )
    row_date = safe_text(incoming.get("as_of_ts") or incoming.get("date")) or None
    if row_date and (entry.get("as_of_ts") is None or row_date > str(entry.get("as_of_ts"))):
        entry["as_of_ts"] = row_date
    for text_key in ("industry_key", "group_state", "leader_state", "breakdown_status"):
        value = safe_text(incoming.get(text_key))
        if value:
            entry[text_key] = value
    for numeric_key in ("group_strength_score", "leader_score", "breakdown_score"):
        value = _safe_float(incoming.get(numeric_key))
        if value is not None:
            entry[numeric_key] = value



def merge_source_registry_entries(
    *,
    registry: dict[str, dict[str, Any]],
    entries: Iterable[Mapping[str, Any]],
    stage_priority: Callable[[str], int],
    source_tag_priority: Callable[[str], float],
    sorted_source_tags: Callable[[Iterable[str] | object | None], list[str]],
    source_style_tags: Callable[[Iterable[str]], list[str]],
    primary_source_style: Callable[[Iterable[str]], str],
    source_priority_score: Callable[[Iterable[str]], float],
    source_engine_bonus: Callable[..., float],
    safe_text: Callable[[Any], str],
) -> dict[str, dict[str, Any]]:
    for incoming in entries:
        if not isinstance(incoming, Mapping):
            continue
        symbol = safe_text(incoming.get("symbol")).upper()
        if not symbol:
            continue
        _merge_registry_row(
            registry=registry,
            symbol=symbol,
            incoming=incoming,
            stage_priority=stage_priority,
            safe_text=safe_text,
        )

    for entry in registry.values():
        unique_tags = sorted_source_tags(entry.get("source_tags", []))
        source_records = list(entry.get("source_records", []))
        best_record = max(
            source_records,
            key=lambda record: (
                stage_priority(safe_text(record.get("screen_stage"))),
                source_tag_priority(safe_text(record.get("source_tag"))),
            ),
            default={},
        )
        entry["source_tags"] = unique_tags
        entry["primary_source_tag"] = safe_text(best_record.get("source_tag")) or (unique_tags[0] if unique_tags else "")
        entry["primary_source_stage"] = safe_text(best_record.get("screen_stage")) or safe_text(entry.get("screen_stage"))
        entry["source_style_tags"] = source_style_tags(unique_tags)
        entry["primary_source_style"] = primary_source_style(unique_tags)
        entry["source_priority_score"] = source_priority_score(unique_tags)
        entry["trend_source_bonus"] = source_engine_bonus(unique_tags, engine="TREND")
        entry["ug_source_bonus"] = source_engine_bonus(unique_tags, engine="UG")
        entry["source_overlap_bonus"] = max(len(unique_tags) - 1, 0) * 5.0
    return registry


def load_source_registry(
    *,
    screeners_root: str,
    market: str,
    source_specs: Sequence[Any],
    stage_priority: Callable[[str], int],
    source_tag_priority: Callable[[str], float],
    sorted_source_tags: Callable[[Iterable[str] | object | None], list[str]],
    source_style_tags: Callable[[Iterable[str]], list[str]],
    primary_source_style: Callable[[Iterable[str]], str],
    source_priority_score: Callable[[Iterable[str]], float],
    source_engine_bonus: Callable[..., float],
    safe_text: Callable[[Any], str],
) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}
    normalized_market = market_key(market)
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
        symbol_column = resolve_symbol_column(frame)
        if symbol_column is None:
            continue
        if symbol_column == "provider_symbol":
            frame = normalize_symbol_columns(
                frame, normalized_market, provider_columns=(symbol_column,)
            )
        else:
            frame = normalize_symbol_columns(
                frame, normalized_market, columns=(symbol_column,)
            )
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
            _merge_registry_row(
                registry=registry,
                symbol=symbol,
                incoming={
                    "screen_stage": spec.screen_stage,
                    "buy_eligible": spec.buy_eligible,
                    "watch_only": not spec.buy_eligible,
                    "source_tags": [spec.source_tag],
                    "source_records": [
                        {
                            "source_tag": spec.source_tag,
                            "screen_stage": spec.screen_stage,
                            "buy_eligible": spec.buy_eligible,
                        }
                    ],
                    "sector": row.get("sector"),
                    "industry": row.get("industry"),
                    "group_name": row.get("group_name") or row.get("industry") or row.get("sector"),
                    "as_of_ts": row.get("as_of_ts") or row.get("date"),
                },
                stage_priority=stage_priority,
                safe_text=safe_text,
            )

    return merge_source_registry_entries(
        registry=registry,
        entries=[],
        stage_priority=stage_priority,
        source_tag_priority=source_tag_priority,
        sorted_source_tags=sorted_source_tags,
        source_style_tags=source_style_tags,
        primary_source_style=primary_source_style,
        source_priority_score=source_priority_score,
        source_engine_bonus=source_engine_bonus,
        safe_text=safe_text,
    )