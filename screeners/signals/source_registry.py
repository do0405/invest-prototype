from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

from utils.io_utils import write_json_with_fallback
from utils.io_utils import safe_filename
from utils.market_runtime import get_financial_cache_dir, get_stock_metadata_path, market_key
from utils.symbol_normalization import (
    normalize_provider_symbol_value,
    normalize_symbol_columns,
    normalize_symbol_value,
)
from utils.typing_utils import frame_keyed_records, row_to_record
from screeners.source_contracts import normalize_source_disposition


SOURCE_REGISTRY_SNAPSHOT_SCHEMA_VERSION = 3

LEADER_LAGGING_SOURCE_CONTEXT_FIELDS: tuple[str, ...] = (
    "source_evidence_tags",
    "link_evidence_tags",
    "weighted_rs_score",
    "rs_rank_true",
    "rs_rank_proxy",
    "rs_proxy_sample_count",
    "rs_proxy_component_coverage",
    "rs_proxy_confidence",
    "rs_quality_score",
    "leadership_freshness_score",
    "early_leader_score",
    "momentum_persistence_score",
    "near_high_leadership_score",
    "hidden_rs_score",
    "hidden_rs_weak_day_count",
    "hidden_rs_down_day_excess_return",
    "hidden_rs_drawdown_resilience",
    "hidden_rs_weak_window_excess_return",
    "hidden_rs_confidence",
    "leader_rs_state",
    "leader_tier",
    "entry_suitability",
    "legacy_label",
    "leader_confidence_score",
    "confidence_bucket",
    "low_confidence_reason_codes",
    "reject_reason_codes",
    "extended_reason_codes",
    "threshold_proximity_codes",
    "hybrid_gate_pass",
    "strict_rs_gate_pass",
    "fading_risk_score",
    "structure_readiness_score",
    "breakout_confirmation_score",
    "box_touch_count",
    "support_hold_count",
    "dry_volume_score",
    "failed_breakout_risk_score",
    "breakout_quality_score",
    "structure_confidence",
    "base_depth_pct",
    "loose_base_risk_score",
    "support_violation_count",
    "breakout_failure_count",
    "breakout_volume_quality_score",
    "structure_reject_reason_codes",
    "extension_risk_score",
    "peer_lead_score",
    "best_lag_days",
    "lagged_corr",
    "follower_confidence_score",
    "pair_evidence_confidence",
    "lag_profile_sample_count",
    "lag_profile_stability_score",
    "catchup_room_score",
    "propagation_state",
    "follower_reject_reason_codes",
    "propagation_ratio",
    "structure_preservation_score",
    "sympathy_freshness_score",
    "underreaction_score",
    "lead_lag_profile",
    "connection_type",
    "pair_confidence",
)


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
        "source_disposition": "",
        "source_buy_eligible": False,
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
        "market_truth_source": "",
        "core_overlay_applied": None,
    }


def _disposition_from_fields(
    fields: Mapping[str, Any],
    *,
    default: str = "",
) -> str:
    explicit = normalize_source_disposition(
        fields.get("source_disposition"),
        default="",
    )
    if explicit:
        return explicit
    if bool(fields.get("source_buy_eligible") or fields.get("buy_eligible")):
        return "buy_eligible"
    if bool(fields.get("watch_only")):
        return "watch_only"
    return normalize_source_disposition(
        default,
        default="",
    )


def _disposition_from_spec(spec: Any) -> str:
    return normalize_source_disposition(
        getattr(spec, "source_disposition", None),
        default=("buy_eligible" if bool(getattr(spec, "buy_eligible", False)) else ""),
    )


def _snapshot_as_of_date(
    as_of_date: str | None = None,
    *,
    fallback_dates: Iterable[Any] | None = None,
) -> str:
    text = str(as_of_date or "").strip()
    if text:
        return text
    resolved_fallbacks = sorted(
        {
            str(value or "").strip()
            for value in (fallback_dates or [])
            if str(value or "").strip()
        }
    )
    if resolved_fallbacks:
        return resolved_fallbacks[-1]
    return datetime.now().strftime("%Y-%m-%d")


def _present_source_context_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _leader_lagging_source_context(relative_path: str, row: Mapping[str, Any]) -> dict[str, Any]:
    normalized_path = str(relative_path or "").replace("\\", "/").lower()
    if not normalized_path.startswith("leader_lagging/"):
        return {}
    context: dict[str, Any] = {}
    for field in LEADER_LAGGING_SOURCE_CONTEXT_FIELDS:
        value = row.get(field)
        if _present_source_context_value(value):
            context[field] = value
    return context


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
    screen_stage = str(incoming.get("screen_stage") or "")
    entry = registry.setdefault(symbol, _default_entry(symbol, screen_stage))
    incoming_disposition = _disposition_from_fields(incoming)
    entry["source_buy_eligible"] = bool(
        entry.get("source_buy_eligible")
        or incoming.get("source_buy_eligible")
        or incoming.get("buy_eligible")
        or incoming_disposition == "buy_eligible"
    )
    entry["buy_eligible"] = bool(entry.get("source_buy_eligible"))
    for tag in incoming.get("source_tags", []) or []:
        if str(tag).strip():
            entry["source_tags"].append(str(tag).strip())
    for record in incoming.get("source_records", []) or []:
        if isinstance(record, dict):
            copied = dict(record)
            copied["source_disposition"] = normalize_source_disposition(
                copied.get("source_disposition"),
                default=incoming_disposition,
            )
            entry["source_records"].append(copied)
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
        entry["source_disposition"] = _disposition_from_fields(
            best_record,
            default=(
                "buy_eligible"
                if bool(entry.get("source_buy_eligible"))
                else ("watch_only" if bool(entry.get("watch_only")) else "")
            ),
        )
        entry["buy_eligible"] = bool(entry.get("source_buy_eligible"))
        entry["watch_only"] = entry["source_disposition"] == "watch_only"
        entry["source_style_tags"] = source_style_tags(unique_tags)
        entry["primary_source_style"] = primary_source_style(unique_tags)
        entry["source_priority_score"] = source_priority_score(unique_tags)
        entry["trend_source_bonus"] = source_engine_bonus(unique_tags, engine="TREND")
        entry["ug_source_bonus"] = source_engine_bonus(unique_tags, engine="UG")
        entry["source_overlap_bonus"] = max(len(unique_tags) - 1, 0) * 5.0
    return registry


def build_source_registry_snapshot(
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
    as_of_date: str | None = None,
) -> dict[str, Any]:
    registry: dict[str, dict[str, Any]] = {}
    source_rows: list[dict[str, Any]] = []
    observed_as_of_dates: list[str] = []
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
        spec_disposition = _disposition_from_spec(spec)

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
            source_record = {
                "source_tag": spec.source_tag,
                "screen_stage": spec.screen_stage,
                "source_buy_eligible": spec.buy_eligible,
                "source_disposition": spec_disposition,
            }
            source_record.update(_leader_lagging_source_context(spec.relative_path, row))
            _merge_registry_row(
                registry=registry,
                symbol=symbol,
                incoming={
                    "screen_stage": spec.screen_stage,
                    "source_buy_eligible": spec.buy_eligible,
                    "source_tags": [spec.source_tag],
                    "source_records": [source_record],
                    "source_disposition": spec_disposition,
                    "sector": row.get("sector"),
                    "industry": row.get("industry"),
                    "group_name": row.get("group_name") or row.get("industry") or row.get("sector"),
                    "as_of_ts": row.get("as_of_ts") or row.get("date"),
                },
                stage_priority=stage_priority,
                safe_text=safe_text,
            )
            row_as_of = safe_text(row.get("as_of_ts") or row.get("date"))
            if row_as_of:
                observed_as_of_dates.append(row_as_of)
            source_rows.append(
                {
                    "symbol": symbol.upper(),
                    "market": normalized_market.upper(),
                    "source_tag": spec.source_tag,
                    "screen_stage": spec.screen_stage,
                    "relative_path": spec.relative_path,
                    "as_of_ts": row.get("as_of_ts") or row.get("date"),
                    "source_buy_eligible": bool(spec.buy_eligible),
                    "buy_eligible": bool(spec.buy_eligible),
                    "watch_only": spec_disposition == "watch_only",
                    "source_disposition": spec_disposition,
                }
            )

    merged_registry = merge_source_registry_entries(
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
    snapshot_as_of = max(observed_as_of_dates) if observed_as_of_dates else None
    return {
        "schema_version": SOURCE_REGISTRY_SNAPSHOT_SCHEMA_VERSION,
        "market": normalized_market.upper(),
        "as_of_date": _snapshot_as_of_date(as_of_date or snapshot_as_of),
        "registry": merged_registry,
        "source_rows": source_rows,
    }


def expected_source_artifact_paths(
    *,
    screeners_root: str,
    source_specs: Sequence[Any],
) -> list[str]:
    expected: list[str] = []
    for spec in source_specs:
        relative_path = str(getattr(spec, "relative_path", "") or "").strip()
        if not relative_path:
            continue
        path = os.path.join(screeners_root, relative_path)
        if path not in expected:
            expected.append(path)
    return expected


def existing_source_artifact_paths(
    *,
    screeners_root: str,
    source_specs: Sequence[Any],
) -> list[str]:
    return [
        path
        for path in expected_source_artifact_paths(
            screeners_root=screeners_root,
            source_specs=source_specs,
        )
        if os.path.exists(path)
    ]


def snapshot_is_compatible(
    snapshot: Mapping[str, Any] | None,
    *,
    market: str,
    as_of_date: str | None = None,
) -> bool:
    if not isinstance(snapshot, Mapping):
        return False
    schema_version = int(snapshot.get("schema_version") or 0)
    if schema_version != SOURCE_REGISTRY_SNAPSHOT_SCHEMA_VERSION:
        return False
    if safe_market := str(snapshot.get("market") or "").strip().upper():
        if safe_market != market_key(market).upper():
            return False
    if as_of_date is not None:
        if str(snapshot.get("as_of_date") or "").strip() != str(as_of_date).strip():
            return False
    registry = snapshot.get("registry")
    return isinstance(registry, Mapping)


def registry_from_snapshot(
    snapshot: Mapping[str, Any],
    *,
    buy_eligible_only: bool = False,
) -> dict[str, dict[str, Any]]:
    registry = snapshot.get("registry")
    if not isinstance(registry, Mapping):
        return {}
    extracted: dict[str, dict[str, Any]] = {}
    for symbol, entry in registry.items():
        if not isinstance(entry, Mapping):
            continue
        copied = deepcopy(dict(entry))
        copied_symbol = str(copied.get("symbol") or symbol).strip().upper()
        if not copied_symbol:
            continue
        copied["source_disposition"] = _disposition_from_fields(
            copied,
            default="",
        )
        if "source_buy_eligible" not in copied:
            copied["source_buy_eligible"] = bool(
                copied.get("buy_eligible")
                or copied["source_disposition"] == "buy_eligible"
            )
        copied["buy_eligible"] = bool(copied.get("source_buy_eligible"))
        copied["watch_only"] = copied["source_disposition"] == "watch_only"
        if buy_eligible_only and not bool(copied.get("source_buy_eligible")):
            continue
        copied["symbol"] = copied_symbol
        extracted[copied_symbol] = copied
    return extracted


def source_rows_from_snapshot(
    snapshot: Mapping[str, Any],
    *,
    buy_eligible_only: bool = False,
) -> list[dict[str, Any]]:
    rows = snapshot.get("source_rows")
    if not isinstance(rows, list):
        return []
    extracted: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        copied = deepcopy(dict(row))
        copied["source_disposition"] = _disposition_from_fields(
            copied,
            default="",
        )
        if "source_buy_eligible" not in copied:
            copied["source_buy_eligible"] = bool(
                copied.get("buy_eligible")
                or copied["source_disposition"] == "buy_eligible"
            )
        copied["buy_eligible"] = bool(copied.get("source_buy_eligible"))
        copied["watch_only"] = copied["source_disposition"] == "watch_only"
        if buy_eligible_only and not bool(copied.get("source_buy_eligible")):
            continue
        symbol = str(copied.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        copied["symbol"] = symbol
        extracted.append(copied)
    return extracted


def source_payloads_from_snapshot(
    snapshot: Mapping[str, Any],
    *,
    screeners_root: str,
    source_specs: Sequence[Any],
    buy_eligible_only: bool = False,
) -> list[dict[str, Any]]:
    spec_by_path = {str(spec.relative_path): spec for spec in source_specs}
    grouped_rows: dict[str, list[dict[str, Any]]] = {}
    for row in source_rows_from_snapshot(
        snapshot,
        buy_eligible_only=buy_eligible_only,
    ):
        relative_path = str(row.get("relative_path") or "").strip()
        if not relative_path or relative_path not in spec_by_path:
            continue
        grouped_rows.setdefault(relative_path, []).append(row)
    payloads: list[dict[str, Any]] = []
    for relative_path in sorted(grouped_rows):
        payloads.append(
            {
                "spec": spec_by_path[relative_path],
                "path": os.path.join(screeners_root, relative_path),
                "rows": grouped_rows[relative_path],
            }
        )
    return payloads


def write_source_registry_snapshot(path: str, snapshot: Mapping[str, Any]) -> None:
    write_json_with_fallback(dict(snapshot), path, ensure_ascii=False, indent=2)


def read_source_registry_snapshot(
    path: str,
    *,
    market: str,
    as_of_date: str | None = None,
) -> dict[str, Any] | None:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8-sig") as handle:
            snapshot = json.load(handle)
    except Exception:
        return None
    if not snapshot_is_compatible(snapshot, market=market, as_of_date=as_of_date):
        return None
    return deepcopy(dict(snapshot))


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
    snapshot: Mapping[str, Any] | None = None,
    as_of_date: str | None = None,
) -> dict[str, dict[str, Any]]:
    if snapshot_is_compatible(snapshot, market=market, as_of_date=as_of_date):
        return registry_from_snapshot(snapshot or {})
    built_snapshot = build_source_registry_snapshot(
        screeners_root=screeners_root,
        market=market,
        source_specs=source_specs,
        stage_priority=stage_priority,
        source_tag_priority=source_tag_priority,
        sorted_source_tags=sorted_source_tags,
        source_style_tags=source_style_tags,
        primary_source_style=primary_source_style,
        source_priority_score=source_priority_score,
        source_engine_bonus=source_engine_bonus,
        safe_text=safe_text,
        as_of_date=as_of_date,
    )
    return registry_from_snapshot(built_snapshot)
