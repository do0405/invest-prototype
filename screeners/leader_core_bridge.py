from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from screeners.source_contracts import MIC_LEADER_CORE_SOURCE_TAG, MIC_LEADER_CORE_STAGE
from utils.market_runtime import get_market_intel_compat_root, market_key


LEADER_CORE_SCHEMA_VERSION = "leader_core_v1"
LEADER_CORE_ENGINE_VERSION = "leader_kernel_v1"
MARKET_CONTEXT_SCHEMA_VERSION = "market_context_v1"
_BUYABLE_LEADER_STATES = {"CONFIRMED", "EMERGING"}


@dataclass(frozen=True)
class LeaderCoreSnapshot:
    market: str
    as_of: str
    summary: dict[str, Any]
    groups_by_key: dict[str, dict[str, Any]]
    leaders_by_symbol: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class MarketTruthSnapshot:
    market: str
    as_of: str
    leader_core: LeaderCoreSnapshot
    market_context: dict[str, Any]
    market_alias: str
    regime_state: str
    top_state: str
    market_state: str
    breadth_state: str
    concentration_state: str
    leadership_state: str
    market_alignment_score: float | None
    breadth_support_score: float | None
    rotation_support_score: float | None
    leader_health_score: float | None
    leader_health_status: str


@dataclass(frozen=True)
class CompatAvailabilityProbeResult:
    market: str
    as_of_date: str
    status: str
    root: str
    summary_path: str
    market_context_path: str


def _safe_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if not text or text.lower() == "nan" else text


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def _weighted_mean(items: list[tuple[float | None, float]]) -> float | None:
    total_weight = 0.0
    total_value = 0.0
    for value, weight in items:
        if value is None:
            continue
        total_value += float(value) * float(weight)
        total_weight += float(weight)
    if total_weight <= 0.0:
        return None
    return total_value / total_weight


def _benchmark_close_series(benchmark_daily: pd.DataFrame) -> pd.Series:
    if benchmark_daily.empty:
        return pd.Series(dtype=float)
    for column in ("adj_close", "close"):
        if column in benchmark_daily.columns:
            return pd.to_numeric(benchmark_daily[column], errors="coerce").dropna().reset_index(drop=True)
    return pd.Series(dtype=float)


def _market_alias_from_score(score: float) -> str:
    if score >= 70.0:
        return "RISK_ON"
    if score >= 45.0:
        return "NEUTRAL"
    return "RISK_OFF"


def _leader_health_status(score: float) -> str:
    if score >= 70.0:
        return "HEALTHY"
    if score >= 45.0:
        return "MIXED"
    return "WEAK"


def _benchmark_trend_score(benchmark_daily: pd.DataFrame) -> float:
    closes = _benchmark_close_series(benchmark_daily)
    if closes.empty:
        return 55.0

    ma50 = closes.rolling(50, min_periods=20).mean()
    ma200 = closes.rolling(200, min_periods=80).mean()
    close = _safe_float(closes.iloc[-1])
    ma50_now = _safe_float(ma50.iloc[-1]) if not ma50.empty else None
    ma200_now = _safe_float(ma200.iloc[-1]) if not ma200.empty else None
    ma200_prev = _safe_float(ma200.iloc[-21]) if len(ma200) >= 21 else None
    slope_up = None
    if ma200_now is not None and ma200_prev is not None:
        slope_up = 100.0 if ma200_now > ma200_prev else 0.0

    score = _weighted_mean(
        [
            (100.0 if close is not None and ma50_now is not None and close > ma50_now else 0.0, 0.35),
            (100.0 if ma50_now is not None and ma200_now is not None and ma50_now > ma200_now else 0.0, 0.35),
            (slope_up, 0.30),
        ]
    )
    return round(float(score if score is not None else 55.0), 2)


def empty_leader_core_snapshot(
    market: str,
    as_of: str,
    *,
    leader_health_score: float | None = None,
    leader_health_status: str | None = None,
) -> LeaderCoreSnapshot:
    score = round(float(leader_health_score if leader_health_score is not None else 55.0), 2)
    status = _safe_text(leader_health_status).upper() or _leader_health_status(score)
    return LeaderCoreSnapshot(
        market=market_key(market),
        as_of=_safe_text(as_of),
        summary={
            "market": market_key(market),
            "as_of": _safe_text(as_of),
            "schema_version": LEADER_CORE_SCHEMA_VERSION,
            "engine_version": "local_standalone_v1",
            "leader_health_score": score,
            "leader_health_status": status,
            "confirmed_count": 0,
            "imminent_count": 0,
            "broken_count": 0,
            "market_truth_source": "local_standalone",
            "core_overlay_applied": False,
        },
        groups_by_key={},
        leaders_by_symbol={},
    )


def build_local_market_truth_snapshot(
    market: str,
    *,
    as_of_date: str,
    benchmark_symbol: str,
    benchmark_daily: pd.DataFrame,
) -> MarketTruthSnapshot:
    trend_score = _benchmark_trend_score(benchmark_daily)
    market_alias = _market_alias_from_score(trend_score)
    top_state = "risk_on" if market_alias == "RISK_ON" else "neutral" if market_alias == "NEUTRAL" else "risk_off"
    market_state = "uptrend" if market_alias == "RISK_ON" else "neutral" if market_alias == "NEUTRAL" else "downtrend"
    health_status = _leader_health_status(trend_score)
    reason_codes = ["LOCAL_STANDALONE", "BENCHMARK_ONLY", "NO_CORE_OVERLAY"]
    if market_alias == "RISK_ON":
        reason_codes.append("BENCHMARK_UPTREND")
    elif market_alias == "RISK_OFF":
        reason_codes.append("BENCHMARK_DOWNTREND")

    leader_core = empty_leader_core_snapshot(
        market,
        as_of_date,
        leader_health_score=trend_score,
        leader_health_status=health_status,
    )
    payload = {
        "market": market_key(market),
        "as_of": _safe_text(as_of_date),
        "schema_version": MARKET_CONTEXT_SCHEMA_VERSION,
        "engine_version": "local_standalone_v1",
        "benchmark_symbol": _safe_text(benchmark_symbol).upper(),
        "prototype_market_alias": market_alias,
        "regime_state": market_state,
        "top_state": top_state,
        "market_state": market_state,
        "breadth_state": "benchmark_only",
        "concentration_state": "local_unknown",
        "leadership_state": "local_screeners",
        "market_alignment_score": trend_score,
        "breadth_support_score": trend_score,
        "rotation_support_score": trend_score,
        "leader_health_score": trend_score,
        "leader_health_status": health_status,
        "reason_codes": list(reason_codes),
        "data_flags": ["LOCAL_STANDALONE", "BENCHMARK_ONLY", "NO_CORE_OVERLAY"],
        "market_truth_source": "local_standalone",
        "core_overlay_applied": False,
    }
    return MarketTruthSnapshot(
        market=market_key(market),
        as_of=_safe_text(as_of_date),
        leader_core=leader_core,
        market_context=payload,
        market_alias=market_alias,
        regime_state=market_state,
        top_state=top_state,
        market_state=market_state,
        breadth_state="benchmark_only",
        concentration_state="local_unknown",
        leadership_state="local_screeners",
        market_alignment_score=trend_score,
        breadth_support_score=trend_score,
        rotation_support_score=trend_score,
        leader_health_score=trend_score,
        leader_health_status=health_status,
    )


def build_industry_key(sector: Any, industry: Any) -> str:
    sector_text = _safe_text(sector).lower() or "unknown"
    industry_text = _safe_text(industry).lower() or "unknown"
    return f"{sector_text}__{industry_text}".replace(" ", "_")


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def _resolve_root(
    market: str,
    compat_root_resolver: Callable[[str], str],
) -> str:
    normalized_market = market_key(market)
    return compat_root_resolver(normalized_market)


def probe_market_intel_compat_availability(
    market: str,
    *,
    as_of_date: str,
    compat_root_resolver: Callable[[str], str] | None = None,
) -> CompatAvailabilityProbeResult:
    resolver = compat_root_resolver or get_market_intel_compat_root
    normalized_market = market_key(market)
    root = _resolve_root(normalized_market, resolver)
    summary_path = os.path.join(root, "leader_market_summary.json")
    market_context_path = os.path.join(root, "market_context.json")
    if not os.path.exists(summary_path) or not os.path.exists(market_context_path):
        return CompatAvailabilityProbeResult(
            market=normalized_market,
            as_of_date=_safe_text(as_of_date),
            status="missing",
            root=root,
            summary_path=summary_path,
            market_context_path=market_context_path,
        )

    try:
        summary_payload = _read_json(summary_path)
        market_context_payload = _read_json(market_context_path)
    except Exception:
        return CompatAvailabilityProbeResult(
            market=normalized_market,
            as_of_date=_safe_text(as_of_date),
            status="schema_mismatch",
            root=root,
            summary_path=summary_path,
            market_context_path=market_context_path,
        )

    if not isinstance(summary_payload, dict) or not isinstance(market_context_payload, dict):
        status = "schema_mismatch"
    elif (
        _safe_text(summary_payload.get("schema_version")) != LEADER_CORE_SCHEMA_VERSION
        or _safe_text(market_context_payload.get("schema_version")) != MARKET_CONTEXT_SCHEMA_VERSION
    ):
        status = "schema_mismatch"
    elif (
        _safe_text(summary_payload.get("as_of")) != _safe_text(as_of_date)
        or _safe_text(market_context_payload.get("as_of")) != _safe_text(as_of_date)
    ):
        status = "stale"
    else:
        status = "compat"

    return CompatAvailabilityProbeResult(
        market=normalized_market,
        as_of_date=_safe_text(as_of_date),
        status=status,
        root=root,
        summary_path=summary_path,
        market_context_path=market_context_path,
    )


def _require_summary(
    *,
    root: str,
    as_of_date: str,
) -> dict[str, Any]:
    summary_path = os.path.join(root, "leader_market_summary.json")
    if not os.path.exists(summary_path):
        raise ValueError(
            "missing leader core artifact summary: "
            f"{summary_path}. Generate market-intel-core compat artifacts, "
            "set MARKET_INTEL_COMPAT_RESULTS_ROOT to their root, or run this task with --standalone."
        )
    payload = _read_json(summary_path)
    if not isinstance(payload, dict):
        raise ValueError("leader core artifact summary payload is invalid")
    schema_version = _safe_text(payload.get("schema_version"))
    if schema_version != LEADER_CORE_SCHEMA_VERSION:
        raise ValueError(
            f"leader core artifact schema mismatch: expected {LEADER_CORE_SCHEMA_VERSION}, got {schema_version or 'missing'}"
        )
    payload_as_of = _safe_text(payload.get("as_of"))
    if payload_as_of != _safe_text(as_of_date):
        raise ValueError(
            f"leader core artifact is stale: expected {_safe_text(as_of_date)}, got {payload_as_of or 'missing'}"
        )
    return payload


def _load_leader_rows(*, root: str) -> list[dict[str, Any]]:
    leaders_path = os.path.join(root, "leaders.json")
    if not os.path.exists(leaders_path):
        return []
    payload = _read_json(leaders_path)
    if not isinstance(payload, list):
        raise ValueError("leader core leaders payload is invalid")
    return [row for row in payload if isinstance(row, dict)]


def _load_group_rows(*, root: str, as_of_date: str) -> list[dict[str, Any]]:
    groups_path = os.path.join(root, "industry_rotation.json")
    if not os.path.exists(groups_path):
        raise ValueError(
            "missing leader core group artifact: "
            f"{groups_path}. Generate market-intel-core compat artifacts, "
            "set MARKET_INTEL_COMPAT_RESULTS_ROOT to their root, or run this task with --standalone."
        )
    payload = _read_json(groups_path)
    if not isinstance(payload, list):
        raise ValueError("leader core industry rotation payload is invalid")
    rows = [row for row in payload if isinstance(row, dict)]
    filtered = [
        row
        for row in rows
        if not _safe_text(row.get("as_of")) or _safe_text(row.get("as_of")) == _safe_text(as_of_date)
    ]
    return filtered or rows


def _require_market_context(
    *,
    root: str,
    as_of_date: str,
) -> dict[str, Any]:
    context_path = os.path.join(root, "market_context.json")
    if not os.path.exists(context_path):
        raise ValueError(
            "missing market truth artifact: "
            f"{context_path}. Generate market-intel-core compat artifacts, "
            "set MARKET_INTEL_COMPAT_RESULTS_ROOT to their root, or run this task with --standalone."
        )
    payload = _read_json(context_path)
    if not isinstance(payload, dict):
        raise ValueError("market truth artifact payload is invalid")
    schema_version = _safe_text(payload.get("schema_version"))
    if schema_version != MARKET_CONTEXT_SCHEMA_VERSION:
        raise ValueError(
            f"market truth artifact schema mismatch: expected {MARKET_CONTEXT_SCHEMA_VERSION}, got {schema_version or 'missing'}"
        )
    payload_as_of = _safe_text(payload.get("as_of"))
    if payload_as_of != _safe_text(as_of_date):
        raise ValueError(
            f"market truth artifact is stale: expected {_safe_text(as_of_date)}, got {payload_as_of or 'missing'}"
        )
    return payload


def load_leader_core_snapshot(
    market: str,
    *,
    as_of_date: str,
    compat_root_resolver: Callable[[str], str] | None = None,
) -> LeaderCoreSnapshot:
    resolver = compat_root_resolver or get_market_intel_compat_root
    root = _resolve_root(market, resolver)
    summary = _require_summary(root=root, as_of_date=as_of_date)
    leader_rows = _load_leader_rows(root=root)
    group_rows = _load_group_rows(root=root, as_of_date=as_of_date)

    groups_by_key: dict[str, dict[str, Any]] = {}
    for row in group_rows:
        industry_key = _safe_text(row.get("industry_key"))
        if not industry_key:
            continue
        groups_by_key[industry_key] = dict(row)

    leaders_by_symbol: dict[str, dict[str, Any]] = {}
    for row in leader_rows:
        symbol = _safe_text(row.get("symbol")).upper()
        if not symbol:
            continue
        leaders_by_symbol[symbol] = dict(row)

    return LeaderCoreSnapshot(
        market=market_key(market),
        as_of=_safe_text(summary.get("as_of")) or _safe_text(as_of_date),
        summary=summary,
        groups_by_key=groups_by_key,
        leaders_by_symbol=leaders_by_symbol,
    )


def load_market_truth_snapshot(
    market: str,
    *,
    as_of_date: str,
    compat_root_resolver: Callable[[str], str] | None = None,
) -> MarketTruthSnapshot:
    resolver = compat_root_resolver or get_market_intel_compat_root
    leader_core = load_leader_core_snapshot(
        market,
        as_of_date=as_of_date,
        compat_root_resolver=resolver,
    )
    root = _resolve_root(market, resolver)
    payload = _require_market_context(root=root, as_of_date=as_of_date)
    return MarketTruthSnapshot(
        market=market_key(market),
        as_of=_safe_text(payload.get("as_of")) or leader_core.as_of,
        leader_core=leader_core,
        market_context=dict(payload),
        market_alias=_safe_text(payload.get("prototype_market_alias")).upper() or "NEUTRAL",
        regime_state=_safe_text(payload.get("regime_state")).lower(),
        top_state=_safe_text(payload.get("top_state")).lower(),
        market_state=_safe_text(payload.get("market_state")).lower(),
        breadth_state=_safe_text(payload.get("breadth_state")).lower(),
        concentration_state=_safe_text(payload.get("concentration_state")).lower(),
        leadership_state=_safe_text(payload.get("leadership_state")).lower(),
        market_alignment_score=_safe_float(payload.get("market_alignment_score")),
        breadth_support_score=_safe_float(payload.get("breadth_support_score")),
        rotation_support_score=_safe_float(payload.get("rotation_support_score")),
        leader_health_score=_safe_float(payload.get("leader_health_score")),
        leader_health_status=_safe_text(payload.get("leader_health_status")).upper(),
    )


def load_leader_core_registry_entries(
    market: str,
    *,
    as_of_date: str,
    compat_root_resolver: Callable[[str], str] | None = None,
) -> dict[str, dict[str, Any]]:
    resolver = compat_root_resolver or get_market_intel_compat_root
    root = _resolve_root(market, resolver)
    _require_summary(root=root, as_of_date=as_of_date)
    rows = _load_leader_rows(root=root)

    entries: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = _safe_text(row.get("symbol")).upper()
        if not symbol:
            continue
        leader_state = _safe_text(row.get("leader_state")).upper()
        breakdown_status = _safe_text(row.get("breakdown_status")).upper()
        if leader_state not in _BUYABLE_LEADER_STATES:
            continue
        source_buy_eligible = breakdown_status == "OK"
        source_disposition = (
            "buy_eligible" if breakdown_status == "OK" else "watch_only"
        )
        if breakdown_status not in {"OK", "IMMINENT"}:
            continue

        industry_key = _safe_text(row.get("industry_key"))
        entry = entries.setdefault(
            symbol,
            {
                "symbol": symbol,
                "source_disposition": source_disposition,
                "source_buy_eligible": False,
                "buy_eligible": False,
                "watch_only": False,
                "screen_stage": MIC_LEADER_CORE_STAGE,
                "source_tags": [MIC_LEADER_CORE_SOURCE_TAG],
                "source_records": [],
                "sector": "",
                "industry": industry_key,
                "group_name": industry_key,
                "as_of_ts": _safe_text(row.get("as_of")) or _safe_text(as_of_date),
            },
        )
        entry["source_buy_eligible"] = bool(
            entry.get("source_buy_eligible") or source_buy_eligible
        )
        entry["source_disposition"] = source_disposition
        entry["buy_eligible"] = bool(entry.get("source_buy_eligible"))
        entry["watch_only"] = source_disposition == "watch_only"
        entry["source_records"].append(
            {
                "source_tag": MIC_LEADER_CORE_SOURCE_TAG,
                "screen_stage": MIC_LEADER_CORE_STAGE,
                "source_buy_eligible": source_buy_eligible,
                "source_disposition": source_disposition,
            }
        )
        entry["industry_key"] = industry_key
        entry["group_state"] = _safe_text(row.get("group_state")).upper()
        entry["leader_state"] = leader_state
        entry["breakdown_status"] = breakdown_status
        entry["group_strength_score"] = _safe_float(row.get("group_strength_score"))
        entry["leader_score"] = _safe_float(row.get("leader_score"))
        entry["breakdown_score"] = _safe_float(row.get("breakdown_score"))

    return entries


def annotate_frame_with_leader_core(
    frame: pd.DataFrame,
    snapshot: LeaderCoreSnapshot,
    *,
    symbol_column: str = "symbol",
    sector_column: str = "sector",
    industry_column: str = "industry",
    industry_key_column: str = "industry_key",
) -> pd.DataFrame:
    if frame.empty:
        table = frame.copy()
        if industry_key_column not in table.columns:
            table[industry_key_column] = pd.Series(dtype="object")
        return table

    table = frame.copy()
    if symbol_column not in table.columns:
        table[symbol_column] = ""
    table[symbol_column] = table[symbol_column].astype(str).str.upper().str.strip()

    if industry_key_column not in table.columns:
        table[industry_key_column] = ""
    derived_key = table.apply(
        lambda row: build_industry_key(row.get(sector_column), row.get(industry_column)),
        axis=1,
    )
    existing_key = table[industry_key_column].astype(str).str.strip()
    table[industry_key_column] = existing_key.where(existing_key != "", derived_key)

    def _payload_map(payload: object) -> dict[str, object]:
        return payload if isinstance(payload, dict) else {}

    leader_payloads = table[symbol_column].map(snapshot.leaders_by_symbol)
    table["core_leader_state"] = leader_payloads.map(
        lambda payload: _safe_text(_payload_map(payload).get("leader_state")).upper()
    )
    table["core_breakdown_status"] = leader_payloads.map(
        lambda payload: _safe_text(_payload_map(payload).get("breakdown_status")).upper()
    )
    table["core_leader_score"] = leader_payloads.map(
        lambda payload: _safe_float(_payload_map(payload).get("leader_score"))
    )
    table["leader_core_buyable"] = (table["core_leader_state"].isin(sorted(_BUYABLE_LEADER_STATES))) & (
        table["core_breakdown_status"] == "OK"
    )
    table["leader_core_imminent"] = (table["core_leader_state"].isin(sorted(_BUYABLE_LEADER_STATES))) & (
        table["core_breakdown_status"] == "IMMINENT"
    )

    group_payloads = table[industry_key_column].map(snapshot.groups_by_key)
    table["core_group_state"] = group_payloads.map(
        lambda payload: _safe_text(_payload_map(payload).get("group_state")).upper()
    )
    table["core_group_strength_score"] = group_payloads.map(
        lambda payload: _safe_float(_payload_map(payload).get("group_strength_score"))
    )
    table["core_group_rank"] = group_payloads.map(lambda payload: _safe_float(_payload_map(payload).get("rank")))
    table["core_group_present"] = group_payloads.map(lambda payload: isinstance(payload, dict) and bool(payload))

    return table


def shared_market_alias_to_signal_state(alias: str) -> str:
    state = _safe_text(alias).upper()
    if state in {"RISK_ON", "NEUTRAL", "RISK_OFF"}:
        return state
    return "NEUTRAL"


def shared_market_alias_to_leader_lagging_state(alias: str) -> str:
    state = _safe_text(alias).upper()
    if state == "RISK_ON":
        return "Risk-On"
    if state == "RISK_OFF":
        return "Risk-Off"
    return "Neutral"


def shared_market_alias_to_weinstein_state(alias: str) -> str:
    state = _safe_text(alias).upper()
    if state == "RISK_ON":
        return "MARKET_STAGE2_FAVORABLE"
    if state == "RISK_OFF":
        return "MARKET_STAGE4_RISK"
    return "MARKET_NEUTRAL"


def shared_market_alias_to_qullamaggie_state(alias: str) -> str:
    state = _safe_text(alias).upper()
    if state == "RISK_ON":
        return "RISK_ON"
    if state == "RISK_OFF":
        return "RISK_OFF"
    return "RISK_NEUTRAL"


def market_truth_reason(snapshot: MarketTruthSnapshot) -> str:
    parts = [f"CORE_{shared_market_alias_to_signal_state(snapshot.market_alias)}"]
    if snapshot.top_state:
        parts.append(f"TOP_{snapshot.top_state.upper()}")
    if snapshot.market_state:
        parts.append(f"MARKET_{snapshot.market_state.upper()}")
    if snapshot.breadth_state:
        parts.append(f"BREADTH_{snapshot.breadth_state.upper()}")
    if snapshot.leadership_state:
        parts.append(f"LEADERSHIP_{snapshot.leadership_state.upper()}")
    return "|".join(parts)
