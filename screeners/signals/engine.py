from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
from datetime import datetime
from typing import Any, Callable, Iterable, Mapping, Sequence
import numpy as np
import pandas as pd

from utils.indicator_helpers import (
    adr_percent,
    atr_percent,
    normalize_indicator_frame,
    rolling_atr,
    rolling_average_volume,
    rolling_ema,
    rolling_max,
    rolling_min,
    rolling_sma,
)

from utils.io_utils import safe_filename
from utils.market_data_contract import PricePolicy, load_local_ohlcv_frame, load_local_ohlcv_frames_ordered
from utils.exchange_calendar import resolve_latest_completed_as_of

from utils.market_runtime import (
    ensure_market_dirs,
    get_benchmark_candidates,
    get_financial_cache_dir,
    get_market_data_dir,
    get_market_source_registry_snapshot_path,
    get_market_screeners_root,
    get_primary_benchmark_symbol,
    is_index_symbol,
    limit_runtime_symbols,
    results_root_override_active,
    get_signal_engine_results_dir,
    get_peg_imminent_results_dir,
    get_stock_metadata_path,
    market_key,
)
from utils.runtime_context import RuntimeContext
from utils.screener_utils import save_screening_results
from utils.symbol_normalization import (
    normalize_provider_symbol_value,
    normalize_symbol_columns,
    normalize_symbol_value,
)
from utils.typing_utils import frame_keyed_records, row_to_record

from . import cycle_store as _cycle_store
from . import metrics as _signal_metrics
from . import patterns as _signal_patterns
from . import source_registry as _source_registry
from screeners import leader_core_bridge as _market_intel_bridge
from . import writers as _signal_writers

from screeners.qullamaggie.core import QullamaggieAnalyzer, _safe_bool, _safe_float
from screeners.source_contracts import (
    CANONICAL_SOURCE_SPECS,
    normalize_source_disposition as _normalize_source_disposition,
    primary_source_style as _shared_primary_source_style,
    source_engine_bonus as _shared_source_engine_bonus,
    source_priority_score as _shared_source_priority_score,
    source_style_bonus as _shared_source_style_bonus,
    source_style_tags as _shared_source_style_tags,
    source_tag_priority as _shared_source_tag_priority,
    source_tag_style as _shared_source_tag_style,
    sorted_source_tags as _shared_sorted_source_tags,
    stage_priority as _shared_stage_priority,
)

from screeners.qullamaggie.earnings_data_collector import EarningsDataCollector

try:

    import yahoo_fin.stock_info as si

    _YAHOO_FIN_AVAILABLE = True

except Exception:
    _YAHOO_FIN_AVAILABLE = False

_ANALYZER = QullamaggieAnalyzer()


_SOURCE_SPECS = CANONICAL_SOURCE_SPECS
_ALL_SCOPE = "all"
_SCREENED_SCOPE = "screened"

_FAMILY_STYLE_FIT = {
    "TF_REGULAR_PULLBACK": {
        "PULLBACK": 100.0,
        "TREND": 92.0,
        "STRUCTURE": 84.0,
        "LEADERSHIP": 68.0,
        "BREAKOUT": 52.0,
        "VOLATILITY": 34.0,
        "WATCH": 0.0,
        "PEG": 28.0,
    },
    "TF_BREAKOUT": {
        "BREAKOUT": 100.0,
        "TREND": 88.0,
        "LEADERSHIP": 78.0,
        "STRUCTURE": 74.0,
        "PULLBACK": 56.0,
        "VOLATILITY": 44.0,
        "WATCH": 0.0,
        "PEG": 30.0,
    },
    "TF_MOMENTUM": {
        "LEADERSHIP": 100.0,
        "BREAKOUT": 86.0,
        "VOLATILITY": 82.0,
        "TREND": 72.0,
        "STRUCTURE": 60.0,
        "PULLBACK": 40.0,
        "WATCH": 0.0,
        "PEG": 46.0,
    },
    "TF_PEG": {
        "PEG": 100.0,
        "TREND": 42.0,
        "BREAKOUT": 36.0,
        "LEADERSHIP": 28.0,
        "STRUCTURE": 22.0,
        "PULLBACK": 22.0,
        "VOLATILITY": 18.0,
        "WATCH": 0.0,
    },
    "UG_BREAKOUT": {
        "BREAKOUT": 100.0,
        "LEADERSHIP": 88.0,
        "TREND": 74.0,
        "STRUCTURE": 64.0,
        "VOLATILITY": 46.0,
        "PULLBACK": 42.0,
        "WATCH": 0.0,
        "PEG": 24.0,
    },
    "UG_PULLBACK": {
        "PULLBACK": 100.0,
        "STRUCTURE": 80.0,
        "TREND": 66.0,
        "LEADERSHIP": 58.0,
        "BREAKOUT": 48.0,
        "VOLATILITY": 34.0,
        "WATCH": 0.0,
        "PEG": 20.0,
    },
    "UG_MEAN_REVERSION": {
        "VOLATILITY": 100.0,
        "LEADERSHIP": 70.0,
        "PULLBACK": 54.0,
        "STRUCTURE": 48.0,
        "TREND": 42.0,
        "BREAKOUT": 38.0,
        "WATCH": 0.0,
        "PEG": 18.0,
    },
}

_FAMILY_MIN_BUY_FIT = {
    "TF_REGULAR_PULLBACK": 45.0,
    "TF_BREAKOUT": 50.0,
    "TF_MOMENTUM": 55.0,
    "TF_PEG": 25.0,
    "UG_BREAKOUT": 55.0,
    "UG_PULLBACK": 45.0,
    "UG_MEAN_REVERSION": 40.0,
}

_FAMILY_MIN_WATCH_FIT = {
    "TF_REGULAR_PULLBACK": 20.0,
    "TF_BREAKOUT": 25.0,
    "TF_MOMENTUM": 30.0,
    "TF_PEG": 15.0,
    "UG_BREAKOUT": 25.0,
    "UG_PULLBACK": 20.0,
    "UG_MEAN_REVERSION": 20.0,
}


_TREND_FAMILIES = (
    "TF_REGULAR_PULLBACK",
    "TF_BREAKOUT",
    "TF_PEG",
    "TF_MOMENTUM",
)

_UG_FAMILIES = (
    "UG_BREAKOUT",
    "UG_PULLBACK",
    "UG_MEAN_REVERSION",
)

_UG_BUY_CODES = {
    "UG_BUY_BREAKOUT",
    "UG_BUY_SQUEEZE_BREAKOUT",
    "UG_BUY_PBB",
    "UG_BUY_MR_LONG",
}
_UG_COOLDOWN_BUSINESS_DAYS = 15
_TF_CHANNEL_LOOKBACK = 8
_PEG_FOLLOWUP_WINDOW = 10
_SIGNAL_HISTORY_PREFIX = "signal_event_history"
_STATE_HISTORY_PREFIX = "signal_state_history"
_PEG_EVENT_HISTORY_PREFIX = "peg_event_history"
_CONTRACT_VERSION_V2 = "v2"
_UG_TF_ONLY_FIELDS = (
    "break_even_level",
    "tp1_level",
    "tp2_level",
    "trailing_mode",
    "tp_plan",
    "risk_free_armed",
    "protected_stop_level",
    "add_on_count",
    "add_on_slot",
    "max_add_ons",
    "tranche_pct",
    "next_addon_allowed",
    "pyramid_state",
    "blended_entry_price",
    "last_trailing_confirmed_level",
    "last_protected_stop_level",
    "last_pyramid_reference_level",
)
_UG_CYCLE_TF_ONLY_FIELDS = (
    "break_even_level",
    "tp1_level",
    "tp2_level",
    "trailing_level",
    "trailing_mode",
    "tp_plan",
    "risk_free_armed",
    "protected_stop_level",
    "add_on_count",
    "add_on_slot",
    "max_add_ons",
    "tranche_pct",
    "next_addon_allowed",
    "last_addon_date",
    "pyramid_state",
    "blended_entry_price",
    "last_trailing_confirmed_level",
    "last_protected_stop_level",
    "last_pyramid_reference_level",
)
_LABELS = {
    "TF_SETUP_ACTIVE": "Setup Active | TF_SETUP_ACTIVE",
    "TF_VCP_ACTIVE": "VCP Active | TF_VCP_ACTIVE",
    "TF_BUILDUP_READY": "Build-up Ready | TF_BUILDUP_READY",
    "TF_AGGRESSIVE_ALERT": "Aggressive Alert | TF_AGGRESSIVE_ALERT",
    "TF_ADDON_READY": "Add-on Ready | TF_ADDON_READY",
    "TF_ADDON_SLOT1_READY": "Add-on Slot1 Ready | TF_ADDON_SLOT1_READY",
    "TF_ADDON_SLOT2_READY": "Add-on Slot2 Ready | TF_ADDON_SLOT2_READY",
    "TF_TRAILING_LEVEL": "Trailing Level | TF_TRAILING_LEVEL",
    "TF_PROTECTED_STOP_LEVEL": "Protected Stop Level | TF_PROTECTED_STOP_LEVEL",
    "TF_BREAKEVEN_LEVEL": "Breakeven Level | TF_BREAKEVEN_LEVEL",
    "TF_TP1_LEVEL": "TP1 Level | TF_TP1_LEVEL",
    "TF_TP2_LEVEL": "TP2 Level | TF_TP2_LEVEL",
    "TF_BUY_REGULAR": "Regular Buy | TF_BUY_REGULAR",
    "TF_BUY_BREAKOUT": "Breakout Buy | TF_BUY_BREAKOUT",
    "TF_BUY_PEG_PULLBACK": "PEG Pullback Buy | TF_BUY_PEG_PULLBACK",
    "TF_BUY_PEG_REBREAK": "PEG Rebreak Buy | TF_BUY_PEG_REBREAK",
    "TF_BUY_MOMENTUM": "Momentum Buy | TF_BUY_MOMENTUM",
    "TF_ADDON_PYRAMID": "Add-on Pyramid | TF_ADDON_PYRAMID",
    "TF_SELL_RESISTANCE_REJECT": "Resistance Reject Sell | TF_SELL_RESISTANCE_REJECT",
    "TF_SELL_BREAKDOWN": "Breakdown Sell | TF_SELL_BREAKDOWN",
    "TF_SELL_CHANNEL_BREAK": "Channel Break Sell | TF_SELL_CHANNEL_BREAK",
    "TF_SELL_TRAILING_BREAK": "Trailing Break Sell | TF_SELL_TRAILING_BREAK",
    "TF_SELL_TP1": "TP1 Sell | TF_SELL_TP1",
    "TF_SELL_TP2": "TP2 Sell | TF_SELL_TP2",
    "TF_SELL_MOMENTUM_END": "Momentum End Sell | TF_SELL_MOMENTUM_END",
    "TF_PEG_EVENT": "PEG Event | TF_PEG_EVENT",
    "UG_STATE_GREEN": "Green Light | UG_STATE_GREEN",
    "UG_STATE_ORANGE": "Orange Light | UG_STATE_ORANGE",
    "UG_STATE_RED": "Red Light | UG_STATE_RED",
    "UG_NH60": "NH60 | UG_NH60",
    "UG_VOL2X": "Vol 2x | UG_VOL2X",
    "UG_W": "W Base | UG_W",
    "UG_VCP": "VCP | UG_VCP",
    "UG_SQUEEZE": "Squeeze | UG_SQUEEZE",
    "UG_TIGHT": "Tight | UG_TIGHT",
    "UG_BUY_BREAKOUT": "UG Breakout Buy | UG_BUY_BREAKOUT",
    "UG_BUY_SQUEEZE_BREAKOUT": "UG Squeeze Breakout Buy | UG_BUY_SQUEEZE_BREAKOUT",
    "UG_BUY_PBB": "UG PBB Buy | UG_BUY_PBB",
    "UG_BUY_MR_LONG": "UG MR Long Buy | UG_BUY_MR_LONG",
    "UG_SELL_PBS": "UG PBS Sell | UG_SELL_PBS",
    "UG_SELL_BREAKDOWN": "UG Breakdown Sell | UG_SELL_BREAKDOWN",
    "UG_SELL_MR_SHORT": "UG MR Short Sell | UG_SELL_MR_SHORT",
    "UG_COMBO_TREND": "UG Combo Trend | UG_COMBO_TREND",
    "UG_COMBO_PULLBACK": "UG Combo Pullback | UG_COMBO_PULLBACK",
    "UG_COMBO_SQUEEZE": "UG Combo Squeeze | UG_COMBO_SQUEEZE",
}

_FINAL_CLOSE_SIGNAL_CODES = frozenset(
    {
        "TF_SELL_RESISTANCE_REJECT",
        "TF_SELL_BREAKDOWN",
        "TF_SELL_CHANNEL_BREAK",
        "TF_SELL_TRAILING_BREAK",
        "TF_SELL_MOMENTUM_END",
        "UG_SELL_PBS",
        "UG_SELL_BREAKDOWN",
    }
)

_TRIM_SIGNAL_CODES = frozenset(
    {
        "TF_SELL_TP1",
        "TF_SELL_TP2",
        "UG_SELL_MR_SHORT",
    }
)

_STATE_SIGNAL_CODES = frozenset(
    {
        "TF_SETUP_ACTIVE",
        "TF_VCP_ACTIVE",
        "TF_BUILDUP_READY",
        "TF_AGGRESSIVE_ALERT",
        "TF_ADDON_READY",
        "TF_ADDON_SLOT1_READY",
        "TF_ADDON_SLOT2_READY",
        "TF_TRAILING_LEVEL",
        "TF_PROTECTED_STOP_LEVEL",
        "TF_BREAKEVEN_LEVEL",
        "TF_TP1_LEVEL",
        "TF_TP2_LEVEL",
        "UG_STATE_GREEN",
        "UG_STATE_ORANGE",
        "UG_STATE_RED",
        "UG_NH60",
        "UG_VOL2X",
        "UG_W",
        "UG_VCP",
        "UG_SQUEEZE",
        "UG_TIGHT",
        "UG_COMBO_TREND",
        "UG_COMBO_PULLBACK",
        "UG_COMBO_SQUEEZE",
    }
)

_OPEN_SIGNAL_CODES = frozenset(
    {
        "TF_BUY_REGULAR",
        "TF_BUY_BREAKOUT",
        "TF_BUY_PEG_PULLBACK",
        "TF_BUY_PEG_REBREAK",
        "TF_BUY_MOMENTUM",
        "UG_BUY_BREAKOUT",
        "UG_BUY_SQUEEZE_BREAKOUT",
        "UG_BUY_PBB",
        "UG_BUY_MR_LONG",
    }
)

_INDICATOR_DOG_RULE_IDS = {
    "TF_BUY_REGULAR": ("IDOG.TF.REGULAR_PULLBACK",),
    "TF_BUY_BREAKOUT": ("IDOG.TF.BREAKOUT_BB_VOL",),
    "TF_BUY_PEG_PULLBACK": ("IDOG.TF.PEG_R50_REBREAK",),
    "TF_BUY_PEG_REBREAK": ("IDOG.TF.PEG_R50_REBREAK",),
    "TF_BUY_MOMENTUM": ("IDOG.TF.MOMENTUM_CHASE",),
    "TF_ADDON_PYRAMID": ("IDOG.TF.PYRAMID_WINNER",),
    "TF_SELL_BREAKDOWN": ("IDOG.TF.SUPPORT_FAIL_EXIT",),
    "TF_SELL_CHANNEL_BREAK": ("IDOG.TF.CHANNEL_BREAK_EXIT",),
    "TF_SELL_TRAILING_BREAK": ("IDOG.TF.TRAILING_RATCHET",),
    "TF_SELL_TP1": ("IDOG.TF.R_MULTIPLE_TP",),
    "TF_SELL_TP2": ("IDOG.TF.R_MULTIPLE_TP",),
    "TF_SELL_MOMENTUM_END": ("IDOG.TF.MOMENTUM_CHASE",),
    "TF_SELL_RESISTANCE_REJECT": ("IDOG.TF.FAILED_RECLAIM_EXIT",),
    "TF_SETUP_ACTIVE": ("IDOG.TF.BUILDUP_VCP",),
    "TF_BUILDUP_READY": ("IDOG.TF.BUILDUP_VCP",),
    "TF_VCP_ACTIVE": ("IDOG.TF.BUILDUP_VCP",),
    "TF_AGGRESSIVE_ALERT": ("IDOG.TF.AGGRESSIVE_ALERT",),
    "TF_ADDON_READY": ("IDOG.TF.ADDON_READY",),
    "TF_ADDON_SLOT1_READY": ("IDOG.TF.ADDON_READY",),
    "TF_ADDON_SLOT2_READY": ("IDOG.TF.ADDON_READY",),
    "TF_TRAILING_LEVEL": ("IDOG.TF.LEVEL_PROVENANCE",),
    "TF_PROTECTED_STOP_LEVEL": ("IDOG.TF.LEVEL_PROVENANCE",),
    "TF_BREAKEVEN_LEVEL": ("IDOG.TF.LEVEL_PROVENANCE",),
    "TF_TP1_LEVEL": ("IDOG.TF.LEVEL_PROVENANCE", "IDOG.TF.R_MULTIPLE_TP"),
    "TF_TP2_LEVEL": ("IDOG.TF.LEVEL_PROVENANCE", "IDOG.TF.R_MULTIPLE_TP"),
    "UG_STATE_GREEN": ("IDOG.UG.STATE_TRAFFIC_LIGHT", "IDOG.UG.VALIDATION_SCORE"),
    "UG_STATE_ORANGE": ("IDOG.UG.STATE_TRAFFIC_LIGHT", "IDOG.UG.VALIDATION_SCORE"),
    "UG_STATE_RED": ("IDOG.UG.STATE_TRAFFIC_LIGHT", "IDOG.UG.VALIDATION_SCORE"),
    "UG_NH60": ("IDOG.UG.GP_NH60", "IDOG.UG.VALIDATION_SCORE"),
    "UG_VOL2X": ("IDOG.UG.GP_VOL2X", "IDOG.UG.VALIDATION_SCORE"),
    "UG_W": ("IDOG.UG.GP_W", "IDOG.UG.VALIDATION_SCORE"),
    "UG_VCP": ("IDOG.UG.GP_VCP",),
    "UG_SQUEEZE": ("IDOG.UG.SIGMA_SQUEEZE",),
    "UG_TIGHT": ("IDOG.UG.SIGMA_SQUEEZE",),
    "UG_BUY_BREAKOUT": (
        "IDOG.UG.VALIDATION_SCORE",
        "IDOG.UG.GP_NH60",
        "IDOG.UG.SIGMA_BO",
    ),
    "UG_BUY_SQUEEZE_BREAKOUT": (
        "IDOG.UG.VALIDATION_SCORE",
        "IDOG.UG.GP_NH60",
        "IDOG.UG.SIGMA_BO",
        "IDOG.UG.SIGMA_SQUEEZE",
    ),
    "UG_BUY_PBB": ("IDOG.UG.VALIDATION_SCORE", "IDOG.UG.SIGMA_PBB"),
    "UG_BUY_MR_LONG": ("IDOG.UG.VALIDATION_SCORE", "IDOG.UG.SIGMA_MR_LONG"),
    "UG_SELL_PBS": ("IDOG.UG.SIGMA_PBS",),
    "UG_SELL_BREAKDOWN": ("IDOG.UG.VALIDATION_SCORE", "IDOG.UG.SIGMA_BREAKDOWN"),
    "UG_SELL_MR_SHORT": ("IDOG.UG.SIGMA_MR_SHORT",),
    "UG_COMBO_TREND": ("IDOG.UG.STATE_TRAFFIC_LIGHT", "IDOG.UG.SIGMA_BO"),
    "UG_COMBO_PULLBACK": ("IDOG.UG.SIGMA_PBB",),
    "UG_COMBO_SQUEEZE": ("IDOG.UG.SIGMA_SQUEEZE",),
}


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _default_cycle_effect(signal_code: Any, action_type: Any, signal_kind: Any) -> str:
    code = _safe_text(signal_code).upper()
    action = _safe_text(action_type).upper()
    kind = _safe_text(signal_kind).upper()
    if code in _TRIM_SIGNAL_CODES:
        return "TRIM"
    if code in _FINAL_CLOSE_SIGNAL_CODES:
        return "CLOSE"
    if code == "TF_ADDON_PYRAMID" and action == "BUY":
        return "ADD"
    if code in _OPEN_SIGNAL_CODES and action == "BUY":
        return "OPEN"
    if code in _STATE_SIGNAL_CODES or kind in {"STATE", "AUX"} or action == "STATE":
        return "STATE"
    return "NONE"


def _raise_isolated_results_prerequisite_error(
    *,
    phase: str,
    snapshot_path: str,
    expected_paths: Sequence[str],
) -> None:
    examples = ", ".join(str(path) for path in list(expected_paths)[:3])
    if len(expected_paths) > 3:
        examples = f"{examples}, ..."
    raise ValueError(
        f"{phase} prerequisite screening artifacts are missing under isolated results root. "
        f"Expected source registry snapshot at {snapshot_path} or at least one screening artifact such as {examples}"
    )


def _indicator_dog_rule_ids_for(signal_code: Any) -> list[str]:
    return list(_INDICATOR_DOG_RULE_IDS.get(_safe_text(signal_code).upper(), ()))


_PUBLIC_PROJECTION_INTERNAL_FIELDS = {
    "bb_percent_b",
    "bb_z_score",
    "pocket_pivot_score",
    "volume_quality_reason_codes",
    "ug_pbb_score",
    "ug_pbs_score",
    "ug_mr_long_score",
    "ug_mr_short_score",
    "band_reversion_reason_codes",
}


def _has_band_reversion_diagnostics(metrics: Mapping[str, Any]) -> bool:
    return any(
        key in metrics
        for key in (
            "bb_percent_b",
            "bb_z_score",
            "ug_pbb_score",
            "ug_pbs_score",
            "ug_mr_long_score",
            "ug_mr_short_score",
            "band_reversion_reason_codes",
        )
    )


def _band_reversion_from_metrics(metrics: Mapping[str, Any]) -> dict[str, Any]:
    return _signal_patterns.score_band_reversion(pd.DataFrame(), metrics)


def _never_down_level(
    candidate: float | None,
    *references: float | None,
) -> float | None:
    values = [value for value in [candidate, *references] if value is not None]
    if not values:
        return None
    return max(values)


def _record_position_effect(
    row: dict[str, Any],
    *,
    cycle_effect: str,
    before: float | None,
    after: float | None,
) -> None:
    row["cycle_effect"] = cycle_effect
    row["position_units_before"] = (
        None if before is None else round(float(before), 4)
    )
    row["position_units_after"] = None if after is None else round(float(after), 4)
    row["position_delta_units"] = (
        None
        if before is None or after is None
        else round(float(after) - float(before), 4)
    )


def _coerce_date(value: Any) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, errors="coerce")

    if pd.isna(parsed):

        return None

    return pd.Timestamp(parsed).normalize()


def _date_to_str(value: Any) -> str | None:

    parsed = _coerce_date(value)

    if parsed is None:

        return None

    return parsed.strftime("%Y-%m-%d")


def _resolve_runtime_as_of_date(market: str, as_of_date: Any) -> str:
    explicit = _date_to_str(as_of_date)
    if explicit:
        return explicit

    normalized_market = market_key(market)
    benchmark_as_of = None
    for candidate in get_benchmark_candidates(normalized_market):
        try:
            frame = load_local_ohlcv_frame(
                normalized_market,
                candidate,
                price_policy=PricePolicy.SPLIT_ADJUSTED,
            )
        except Exception:
            continue
        if frame.empty or "date" not in frame.columns:
            continue
        latest = _date_to_str(frame.iloc[-1]["date"])
        if latest:
            benchmark_as_of = latest
            break

    resolution = resolve_latest_completed_as_of(
        market=normalized_market,
        explicit_as_of=None,
        benchmark_as_of=benchmark_as_of,
    )
    if resolution.as_of_date:
        return resolution.as_of_date

    return datetime.now().strftime("%Y-%m-%d")


def _next_business_day(value: Any) -> str | None:

    parsed = _coerce_date(value)

    if parsed is None:

        return None

    return pd.bdate_range(parsed, periods=2)[-1].strftime("%Y-%m-%d")


def _business_days_between(left: Any, right: Any) -> int | None:

    left_ts = _coerce_date(left)

    right_ts = _coerce_date(right)

    if left_ts is None or right_ts is None:

        return None

    if right_ts < left_ts:

        return -1

    return max(len(pd.bdate_range(left_ts, right_ts)) - 1, 0)


def _to_list(value: Any) -> list[str]:

    if value is None:

        return []

    if isinstance(value, list):

        return [str(item) for item in value if str(item)]

    if isinstance(value, tuple):

        return [str(item) for item in value if str(item)]

    if isinstance(value, str):

        text = value.strip()

        if not text:

            return []

        if text.startswith("[") and text.endswith("]"):

            try:

                parsed = json.loads(text.replace("'", '"'))

                if isinstance(parsed, list):

                    return [str(item) for item in parsed if str(item)]

            except Exception:

                pass

        return [text]

    return [str(value)]


def _write_records(
    output_dir: str, filename_prefix: str, rows: list[dict[str, Any]]
) -> None:
    _signal_writers.write_records(
        output_dir,
        filename_prefix,
        rows,
        save_screening_results_fn=save_screening_results,
    )


def _load_metadata_map(market: str) -> dict[str, dict[str, Any]]:
    return _source_registry.load_metadata_map(
        market,
        get_stock_metadata_path_fn=get_stock_metadata_path,
    )


def _financial_cache_lookup_paths(symbol: str, market: str) -> list[str]:
    return _source_registry.financial_cache_lookup_paths(
        symbol,
        market,
        get_financial_cache_dir_fn=get_financial_cache_dir,
    )


def _load_financial_map(
    market: str, symbols: Iterable[str] | None = None
) -> dict[str, dict[str, Any]]:
    return _source_registry.load_financial_map(
        market,
        symbols=symbols,
        get_financial_cache_dir_fn=get_financial_cache_dir,
    )


def _resolve_symbol_column(frame: pd.DataFrame) -> str | None:
    for candidate in ("symbol", "ticker", "provider_symbol"):
        if candidate in frame.columns:
            return candidate
    return None


def _rolling_adx(frame: pd.DataFrame, window: int = 14) -> pd.Series:

    high = pd.to_numeric(frame["high"], errors="coerce")

    low = pd.to_numeric(frame["low"], errors="coerce")

    close = pd.to_numeric(frame["close"], errors="coerce")

    up_move = high.diff()

    down_move = low.shift(1) - low

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0),
        index=frame.index,
        dtype=float,
    )

    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0),
        index=frame.index,
        dtype=float,
    )

    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(window, min_periods=window).mean()

    plus_di = (
        100.0
        * plus_dm.rolling(window, min_periods=window).mean()
        / atr.replace({0.0: np.nan})
    )

    minus_di = (
        100.0
        * minus_dm.rolling(window, min_periods=window).mean()
        / atr.replace({0.0: np.nan})
    )

    dx = (
        (plus_di - minus_di).abs() / (plus_di + minus_di).replace({0.0: np.nan}) * 100.0
    )

    return dx.rolling(window, min_periods=window).mean()


def _rsi_series(series: pd.Series, window: int = 14) -> pd.Series:

    delta = pd.to_numeric(series, errors="coerce").diff()

    gain = delta.clip(lower=0.0)

    loss = (-delta).clip(lower=0.0)

    avg_gain = gain.ewm(alpha=1.0 / window, min_periods=window, adjust=False).mean()

    avg_loss = loss.ewm(alpha=1.0 / window, min_periods=window, adjust=False).mean()

    zero_loss = avg_loss == 0.0

    zero_gain = avg_gain == 0.0

    rs = avg_gain / avg_loss.replace({0.0: np.nan})

    rsi = 100.0 - (100.0 / (1.0 + rs))

    rsi = rsi.mask(zero_loss & (avg_gain > 0.0), 100.0)

    rsi = rsi.mask(zero_gain & (avg_loss > 0.0), 0.0)

    rsi = rsi.mask(zero_gain & zero_loss, 50.0)

    return rsi


def _macd(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:

    close = pd.to_numeric(frame["close"], errors="coerce")

    fast = rolling_ema(close, 12, adjust=False)

    slow = rolling_ema(close, 26, adjust=False)

    line = fast - slow

    signal = rolling_ema(line, 9, adjust=False)

    hist = line - signal

    return line, signal, hist


def _bollinger(
    frame: pd.DataFrame, window: int = 20, deviations: float = 2.0
) -> tuple[pd.Series, pd.Series, pd.Series]:

    close = pd.to_numeric(frame["close"], errors="coerce")

    mid = rolling_sma(close, window, min_periods=window)

    std = close.rolling(window, min_periods=window).std()

    upper = mid + (std * deviations)

    lower = mid - (std * deviations)

    return lower, mid, upper


def _detect_double_bottom(frame: pd.DataFrame) -> bool:

    if len(frame) < 40:

        return False

    lows = pd.to_numeric(frame["low"], errors="coerce").iloc[-60:]

    if len(lows.dropna()) < 30:

        return False

    left = lows.iloc[: len(lows) // 2]

    right = lows.iloc[len(lows) // 2 :]

    left_low = _safe_float(left.min())

    right_low = _safe_float(right.min())

    if left_low is None or right_low is None or left_low <= 0:

        return False

    return abs(left_low - right_low) / left_low <= 0.08


def _dry_volume(frame: pd.DataFrame) -> bool:

    if len(frame) < 30:

        return False

    volume = pd.to_numeric(frame["volume"], errors="coerce")

    recent = _safe_float(volume.iloc[-10:].mean())

    prior = _safe_float(volume.iloc[-30:-10].mean())

    if recent is None or prior is None or prior <= 0:

        return False

    return recent <= (prior * 0.8)


def _zone_bounds(
    anchor: float | None, width_pct: float | None
) -> tuple[float | None, float | None]:

    if anchor is None or width_pct is None:

        return None, None

    width = abs(anchor) * width_pct

    return float(anchor - width), float(anchor + width)


def _pct_distance(base: float | None, value: float | None) -> float | None:

    if base is None or value is None or base == 0:

        return None

    return ((value / base) - 1.0) * 100.0


def _is_rising(current: float | None, prior: float | None) -> bool:

    if current is None or prior is None:

        return False

    return current > prior


def _candle_close_position_pct(
    high: float | None, low: float | None, close: float | None
) -> float | None:

    if high is None or low is None or close is None:

        return None

    candle_range = high - low

    if candle_range <= 0:

        return None

    return (close - low) / candle_range


def _candle_body_strength_pct(
    open_value: float | None, high: float | None, low: float | None, close: float | None
) -> float | None:

    if open_value is None or high is None or low is None or close is None:

        return None

    candle_range = high - low

    if candle_range <= 0:

        return None

    return abs(close - open_value) / candle_range


def _pick_latest(*values: Any) -> float | None:
    for value in values:
        casted = _safe_float(value)
        if casted is not None:
            return casted
    return None


def _safe_int(value: Any) -> int | None:
    casted = _safe_float(value)
    if casted is None:
        return None
    return int(casted)


def _grade_from_score(score: float, hard_fail: bool) -> str:
    if hard_fail:
        return "D"
    if score >= 90.0:

        return "S"

    if score >= 80.0:

        return "A"

    if score >= 68.0:
        return "B"
    return "C"


def _shift_grade(grade: str, steps: int) -> str:
    scale = ["D", "C", "B", "A", "S"]
    normalized = _safe_text(grade).upper()
    if normalized not in scale or steps == 0:
        return normalized
    index = scale.index(normalized)
    shifted = min(max(index + int(steps), 0), len(scale) - 1)
    return scale[shifted]


def _normalize_alignment_state(state: Any) -> str:
    normalized = _safe_text(state).upper()
    if normalized in {"BULLISH", "MIXED", "BEARISH"}:
        return normalized
    if normalized == "RED":
        return "BEARISH"
    return "BEARISH"


def _alignment_state_from_refs(
    fast_ref: float | None,
    mid_ref: float | None,
    slow_ref: float | None,
    close_value: float | None,
) -> str:
    if fast_ref and mid_ref and slow_ref:
        if fast_ref > mid_ref > slow_ref:
            return "BULLISH"
        if fast_ref > mid_ref and close_value is not None and close_value > slow_ref:
            return "MIXED"
    return "BEARISH"


def _alignment_transition(previous_state: Any, current_state: Any) -> str:
    current = _normalize_alignment_state(current_state)
    previous = _normalize_alignment_state(previous_state)
    if previous == current:
        return f"{current}_HOLD"
    return f"{current}_FROM_{previous}"


def _stage_priority(stage: str) -> int:
    return _shared_stage_priority(stage)


def _source_tag_priority(tag: str) -> float:
    return _shared_source_tag_priority(tag)


def _source_tag_style(tag: str) -> str:
    return _shared_source_tag_style(tag)


def _source_style_bonus(style: str, *, engine: str) -> float:
    return _shared_source_style_bonus(style, engine=engine)


def _sorted_source_tags(tags: Iterable[str] | object | None) -> list[str]:
    return _shared_sorted_source_tags(tags)


def _source_priority_score(tags: Iterable[str]) -> float:
    return _shared_source_priority_score(tags)


def _source_engine_bonus(tags: Iterable[str], *, engine: str) -> float:
    return _shared_source_engine_bonus(tags, engine=engine)


def _source_style_tags(tags: Iterable[str]) -> list[str]:
    return _shared_source_style_tags(tags)


def _primary_source_style(tags: Iterable[str]) -> str:
    return _shared_primary_source_style(tags)


def _family_source_fit_score(family: str, styles: Iterable[str]) -> float:
    family_map = _FAMILY_STYLE_FIT.get(_safe_text(family), {})
    ordered_styles = list(dict.fromkeys(_to_list(styles)))
    if not family_map or not ordered_styles:
        return 0.0
    weights = (1.0, 0.35, 0.15)
    score = 0.0
    for index, style in enumerate(ordered_styles[: len(weights)]):
        score += float(family_map.get(_safe_text(style), 0.0)) * weights[index]
    return round(min(score, 100.0), 2)


def _family_source_fit_label(score: float) -> str:
    if score >= 85.0:
        return "PRIMARY"
    if score >= 65.0:
        return "SUPPORTIVE"
    if score >= 45.0:
        return "TACTICAL"
    if score >= 20.0:
        return "EARLY"
    if score > 0.0:
        return "OFFSTYLE"
    return "NONE"


def _is_fast_character(stock_character: Any) -> bool:
    normalized = _safe_text(stock_character).upper()
    return normalized in {"FAST", "PARABOLIC"}


def _compute_zone_width_pct(
    atr_pct_value: float | None, adr_pct_value: float | None
) -> float:

    atr_component = (atr_pct_value or 0.0) * 0.50

    adr_component = (adr_pct_value or 0.0) * 0.25

    return max(1.0, atr_component, adr_component) / 100.0


def _safe_csv_rows(path: str) -> list[dict[str, Any]]:

    if not os.path.exists(path):

        return []

    try:

        frame = pd.read_csv(path)

    except Exception:

        return []

    if frame.empty:

        return []

    return [row_to_record(row) for _, row in frame.iterrows()]


def _sorted_signal_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:

    def sort_key(row: Mapping[str, Any]) -> tuple[str, str, str, str, str, str, str]:

        return (
            _safe_text(row.get("signal_date")),
            _safe_text(row.get("symbol")).upper(),
            _safe_text(row.get("engine")),
            _safe_text(row.get("family")),
            _safe_text(row.get("signal_kind")),
            _safe_text(row.get("signal_code")),
            _safe_text(row.get("action_type")),
        )

    return [dict(row) for row in sorted(rows, key=sort_key)]


def _env_int_default(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _resolved_parallel_workers(
    total_items: int,
    cap: int = 8,
    *,
    env_var: str | None = None,
) -> int:
    if total_items <= 1:
        return 1
    resolved_cap = _env_int_default(env_var, cap) if env_var else cap
    return max(1, min(resolved_cap, total_items))


def _normalize_scope(value: Any, *, default: str = _SCREENED_SCOPE) -> str:
    text = _safe_text(value).lower()
    if text in {_ALL_SCOPE, _SCREENED_SCOPE}:
        return text
    return default


def _source_buy_eligible(
    source_entry: Mapping[str, Any],
    *,
    symbol: str,
    peg_ready_map: Mapping[str, Mapping[str, Any]],
    peg_event_history_map: Mapping[str, Mapping[str, Any]] | None = None,
) -> bool:
    peg_history = peg_event_history_map or {}
    return bool(
        _normalize_source_disposition(source_entry.get("source_disposition"))
        == "buy_eligible"
        or source_entry.get("source_buy_eligible")
        or source_entry.get("buy_eligible")
        or symbol in peg_ready_map
        or symbol in peg_history
    )


def _source_disposition(
    source_entry: Mapping[str, Any],
    *,
    symbol: str,
    peg_ready_map: Mapping[str, Mapping[str, Any]],
    peg_event_history_map: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    peg_history = peg_event_history_map or {}
    explicit = _normalize_source_disposition(source_entry.get("source_disposition"))
    if explicit in {"buy_eligible", "watch_only"}:
        return explicit
    if bool(source_entry.get("source_buy_eligible") or source_entry.get("buy_eligible")):
        return "buy_eligible"
    if bool(source_entry.get("watch_only")):
        return "watch_only"
    if symbol in peg_ready_map or symbol in peg_history:
        return "buy_eligible"
    return ""


def _public_source_disposition(
    source_entry: Mapping[str, Any],
    *,
    symbol: str,
    scope: str,
    scope_symbols: set[str],
    screened_symbols: set[str],
    peg_ready_map: Mapping[str, Mapping[str, Any]],
    peg_event_history_map: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    disposition = _source_disposition(
        source_entry,
        symbol=symbol,
        peg_ready_map=peg_ready_map,
        peg_event_history_map=peg_event_history_map,
    )
    if disposition:
        return disposition
    if (
        _normalize_scope(scope) == _ALL_SCOPE
        and symbol in scope_symbols
        and symbol not in screened_symbols
    ):
        return "discovery_only"
    return ""


def _scope_local_cycle_map(
    active_cycles: Mapping[tuple[Any, ...], Mapping[str, Any]],
    *,
    scope: str,
) -> dict[tuple[str, str, str], dict[str, Any]]:
    normalized_scope = _normalize_scope(scope)
    localized: dict[tuple[str, str, str], dict[str, Any]] = {}
    for key, cycle in active_cycles.items():
        if len(key) >= 4:
            cycle_scope = _normalize_scope(key[0])
            engine = _safe_text(key[1])
            family = _safe_text(key[2])
            symbol = _safe_text(key[3]).upper()
        else:
            cycle_scope = _normalize_scope(cycle.get("scope"))
            engine = _safe_text(key[0]) if len(key) >= 1 else ""
            family = _safe_text(key[1]) if len(key) >= 2 else ""
            symbol = _safe_text(key[2]).upper() if len(key) >= 3 else ""
        if cycle_scope != normalized_scope or not engine or not family or not symbol:
            continue
        localized[(engine, family, symbol)] = {**dict(cycle), "scope": cycle_scope}
    return localized


def _history_rows_for_scope(
    rows: Iterable[Mapping[str, Any]],
    *,
    scope: str,
) -> list[dict[str, Any]]:
    normalized_scope = _normalize_scope(scope)
    selected: list[dict[str, Any]] = []
    for row in rows:
        row_scope = _normalize_scope(row.get("scope"))
        if row_scope != normalized_scope:
            continue
        selected.append({**dict(row), "scope": row_scope})
    return selected


def _scope_signal_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    scope: str,
    scope_symbols: set[str],
    screened_symbols: set[str],
    source_registry: Mapping[str, Mapping[str, Any]],
    peg_ready_map: Mapping[str, Mapping[str, Any]],
    peg_event_history_map: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    scoped_rows: list[dict[str, Any]] = []
    normalized_scope = _normalize_scope(scope)
    for row in rows:
        updated = dict(row)
        symbol = _safe_text(updated.get("symbol")).upper()
        source_entry = source_registry.get(symbol, {})
        updated["scope"] = normalized_scope
        updated["is_screened"] = symbol in screened_symbols
        updated["buy_eligible"] = symbol in scope_symbols
        updated["source_buy_eligible"] = _source_buy_eligible(
            source_entry,
            symbol=symbol,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        updated["source_disposition"] = _public_source_disposition(
            source_entry,
            symbol=symbol,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbols,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        scoped_rows.append(updated)
    return _sorted_signal_rows(scoped_rows)


def _default_signal_source_entry(
    symbol: str,
    *,
    peg_ready_map: Mapping[str, Mapping[str, Any]],
    peg_event_history_map: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    peg_capable = symbol in peg_ready_map or symbol in peg_event_history_map
    return {
        "symbol": symbol,
        "source_disposition": "buy_eligible" if peg_capable else "",
        "source_buy_eligible": peg_capable,
        "buy_eligible": peg_capable,
        "watch_only": False,
        "screen_stage": "PEG_READY" if peg_capable else "",
        "source_tags": ["PEG_READY"] if peg_capable else [],
        "source_overlap_bonus": 0.0,
    }


def _annotate_buy_candidate_row(
    row: Mapping[str, Any],
    *,
    legacy_visible: bool,
    artifact_action_type: str,
) -> dict[str, Any]:
    updated = dict(row)
    updated["_legacy_visible"] = bool(legacy_visible)
    updated["_artifact_action_type"] = _safe_text(artifact_action_type).upper()
    return updated


def _history_merge_rows(
    existing_rows: Iterable[Mapping[str, Any]],
    new_rows: Iterable[Mapping[str, Any]],
    *,
    key_fields: tuple[str, ...],
) -> list[dict[str, Any]]:

    merged: dict[tuple[str, ...], dict[str, Any]] = {}

    for row in list(existing_rows) + list(new_rows):

        record = dict(row)

        key = tuple(_safe_text(record.get(field)) for field in key_fields)

        if any(part for part in key):

            merged[key] = record

    return _sorted_signal_rows(merged.values())


_ACTIVE_SIGNAL_CODES = frozenset(_LABELS)
_UG_PULLBACK_REFERENCE_EXIT_SIGNAL = "UG_SELL_MR_SHORT_OR_PBS"
_REFERENCE_EXIT_HELPER_LITERALS = frozenset({_UG_PULLBACK_REFERENCE_EXIT_SIGNAL})


def _is_active_signal_code(signal_code: Any) -> bool:
    return _safe_text(signal_code).upper() in _ACTIVE_SIGNAL_CODES



def _is_reference_exit_helper_literal(signal_code: Any) -> bool:
    return _safe_text(signal_code).upper() in _REFERENCE_EXIT_HELPER_LITERALS


def _signal_code_label(signal_code: str) -> str:
    normalized = _safe_text(signal_code).upper()
    if _is_active_signal_code(normalized):
        return _LABELS[normalized]
    return _safe_text(signal_code)


_REFERENCE_EXIT_SIGNAL_MAP: dict[str, tuple[str, ...]] = {
    _UG_PULLBACK_REFERENCE_EXIT_SIGNAL: ("UG_SELL_MR_SHORT", "UG_SELL_PBS"),
}


def _normalized_reference_exit_signal(
    *, engine: Any, family: Any, reference_exit_signal: Any
) -> str:
    if _safe_text(engine).upper() != "UG":
        return ""
    if _safe_text(family) != "UG_PULLBACK":
        return ""
    text = _safe_text(reference_exit_signal).upper()
    return _UG_PULLBACK_REFERENCE_EXIT_SIGNAL if _is_reference_exit_helper_literal(text) else ""


def _reference_exit_codes(reference_exit_signal: Any) -> tuple[str, ...]:

    text = _safe_text(reference_exit_signal).upper()
    if not text:
        return ()
    mapped = _REFERENCE_EXIT_SIGNAL_MAP.get(text)
    if mapped is not None:
        return mapped
    normalized = text.replace(',', '|')
    return tuple(
        dict.fromkeys(part.strip().upper() for part in normalized.split('|') if part.strip())
    )


def _select_ma_system(stock_character: str) -> str:

    return "FIBO" if stock_character in {"FAST", "PARABOLIC"} else "CLASSIC"


def _classify_stock_character(
    frame: pd.DataFrame, adr_pct_value: float | None, atr_pct_value: float | None
) -> str:

    close = pd.to_numeric(frame["close"], errors="coerce")

    if len(close) < 15:

        return "SLOW"

    gaps = (
        (
            (
                (pd.to_numeric(frame["open"], errors="coerce") / close.shift(1)) - 1.0
            ).abs()
            >= 0.05
        )
        .iloc[-20:]
        .fillna(False)
    )

    gap_count = int(gaps.sum())

    recent_extension = None

    if len(close) >= 20:

        ema20 = rolling_ema(close, 20, adjust=False)

        if _safe_float(ema20.iloc[-1]):

            recent_extension = (
                (close.iloc[-1] - ema20.iloc[-1]) / ema20.iloc[-1]
            ) * 100.0

    if (adr_pct_value or 0.0) >= 7.0 or (atr_pct_value or 0.0) >= 8.0 or gap_count >= 3:

        if (adr_pct_value or 0.0) >= 10.0 or (
            _safe_float(recent_extension) or 0.0
        ) >= 20.0:

            return "PARABOLIC"

        return "FAST"

    return "SLOW"


def _build_signal_row(
    *,
    signal_date: str | None,
    symbol: str,
    market: str,
    engine: str,
    family: str,
    signal_kind: str,
    signal_code: str,
    action_type: str,
    conviction_grade: str,
    screen_stage: str,
    signal_score: float | None = None,
    gp_score: float | None = None,
    gp_health: str = "",
    sigma_score: float | None = None,
    sigma_health: str = "",
    traffic_light: str = "",
    technical_light: str = "",
    growth_score: float | None = None,
    growth_health: str = "",
    eps_health: str = "",
    sales_health: str = "",
    growth_data_status: str = "",
    dashboard_score: float | None = None,
    dashboard_light: str = "",
    dashboard_position_bias: str = "",
    primary_source_tag: str = "",
    primary_source_stage: str = "",
    primary_source_style: str = "",
    source_priority_score: float | None = None,
    source_style_tags: Iterable[str] | None = None,
    trend_source_bonus: float | None = None,
    ug_source_bonus: float | None = None,
    source_fit_score: float | None = None,
    source_fit_label: str = "",
    family_cycle_id: str = "",
    cooldown_bucket: str = "",
    cooldown_blocked: bool = False,
    support_zone_low: float | None = None,
    support_zone_high: float | None = None,
    stop_level: float | None = None,
    break_even_level: float | None = None,
    tp1_level: float | None = None,
    tp2_level: float | None = None,
    trailing_mode: str = "",
    tp_plan: str = "",
    trim_count: int | None = None,
    risk_free_armed: bool | None = None,
    protected_stop_level: float | None = None,
    add_on_count: int | None = None,
    add_on_slot: int | None = None,
    max_add_ons: int | None = None,
    tranche_pct: float | None = None,
    next_addon_allowed: bool | None = None,
    pyramid_state: str = "",
    base_position_units: float | None = None,
    current_position_units: float | None = None,
    blended_entry_price: float | None = None,
    last_trailing_confirmed_level: float | None = None,
    last_protected_stop_level: float | None = None,
    last_pyramid_reference_level: float | None = None,
    strategy_combo: str = "",
    signal_phase: str = "",
    reference_target_level: float | None = None,
    reference_exit_signal: str = "",
    contract_version: str = _CONTRACT_VERSION_V2,
    cycle_effect: str = "",
    position_units_before: float | None = None,
    position_units_after: float | None = None,
    position_delta_units: float | None = None,
    indicator_dog_rule_ids: Iterable[str] | None = None,
    source_tags: Iterable[str] | None = None,
    reason_codes: Iterable[str] | None = None,
    quality_flags: Iterable[str] | None = None,
) -> dict[str, Any]:
    signal_day = signal_date or datetime.now().strftime("%Y-%m-%d")
    resolved_source_tags = list(dict.fromkeys(_to_list(source_tags)))
    resolved_primary_source_tag = _safe_text(primary_source_tag) or (
        resolved_source_tags[0] if resolved_source_tags else ""
    )
    resolved_source_style_tags = list(
        dict.fromkeys(_to_list(source_style_tags))
    ) or _source_style_tags(resolved_source_tags)
    resolved_primary_source_style = _safe_text(primary_source_style) or (
        resolved_source_style_tags[0] if resolved_source_style_tags else ""
    )
    resolved_source_priority_score = source_priority_score
    if resolved_source_priority_score is None:
        resolved_source_priority_score = _source_priority_score(resolved_source_tags)
    resolved_primary_source_stage = _safe_text(primary_source_stage) or screen_stage
    resolved_trend_source_bonus = trend_source_bonus
    if resolved_trend_source_bonus is None:
        resolved_trend_source_bonus = _source_engine_bonus(
            resolved_source_tags, engine="TREND"
        )
    resolved_ug_source_bonus = ug_source_bonus
    if resolved_ug_source_bonus is None:
        resolved_ug_source_bonus = _source_engine_bonus(
            resolved_source_tags, engine="UG"
        )
    resolved_source_fit_score = source_fit_score
    if resolved_source_fit_score is None:
        resolved_source_fit_score = _family_source_fit_score(
            family, resolved_source_style_tags
        )
    resolved_source_fit_label = _safe_text(
        source_fit_label
    ) or _family_source_fit_label(float(resolved_source_fit_score or 0.0))
    resolved_cycle_effect = _safe_text(cycle_effect).upper() or _default_cycle_effect(
        signal_code, action_type, signal_kind
    )
    resolved_indicator_dog_rule_ids = list(
        dict.fromkeys(
            _to_list(indicator_dog_rule_ids)
            or _indicator_dog_rule_ids_for(signal_code)
        )
    )
    return {
        "signal_date": signal_day,
        "intended_action_date": _next_business_day(signal_day),
        "symbol": symbol,
        "market": market.upper(),
        "engine": engine,
        "family": family,
        "family_cycle_id": family_cycle_id,
        "signal_kind": signal_kind,
        "signal_code": signal_code,
        "display_label": _signal_code_label(signal_code),
        "action_type": action_type,
        "conviction_grade": conviction_grade,
        "screen_stage": screen_stage,
        "signal_score": None if signal_score is None else round(float(signal_score), 2),
        "gp_score": None if gp_score is None else round(float(gp_score), 2),
        "gp_health": _safe_text(gp_health).upper(),
        "sigma_score": None if sigma_score is None else round(float(sigma_score), 2),
        "sigma_health": _safe_text(sigma_health).upper(),
        "traffic_light": _safe_text(traffic_light).upper(),
        "technical_light": _safe_text(technical_light).upper(),
        "growth_score": None if growth_score is None else round(float(growth_score), 2),
        "growth_health": _safe_text(growth_health).upper(),
        "eps_health": _safe_text(eps_health).upper(),
        "sales_health": _safe_text(sales_health).upper(),
        "growth_data_status": _safe_text(growth_data_status).upper(),
        "dashboard_score": (
            None if dashboard_score is None else round(float(dashboard_score), 2)
        ),
        "dashboard_light": _safe_text(dashboard_light).upper(),
        "dashboard_position_bias": _safe_text(dashboard_position_bias).upper(),
        "primary_source_tag": resolved_primary_source_tag,
        "primary_source_stage": resolved_primary_source_stage,
        "primary_source_style": resolved_primary_source_style,
        "source_priority_score": (
            None
            if resolved_source_priority_score is None
            else round(float(resolved_source_priority_score), 2)
        ),
        "source_style_tags": resolved_source_style_tags,
        "trend_source_bonus": (
            None
            if resolved_trend_source_bonus is None
            else round(float(resolved_trend_source_bonus), 2)
        ),
        "ug_source_bonus": (
            None
            if resolved_ug_source_bonus is None
            else round(float(resolved_ug_source_bonus), 2)
        ),
        "source_fit_score": (
            None
            if resolved_source_fit_score is None
            else round(float(resolved_source_fit_score), 2)
        ),
        "source_fit_label": resolved_source_fit_label,
        "cooldown_bucket": cooldown_bucket,
        "cooldown_blocked": bool(cooldown_blocked),
        "support_zone_low": (
            None if support_zone_low is None else round(float(support_zone_low), 4)
        ),
        "support_zone_high": (
            None if support_zone_high is None else round(float(support_zone_high), 4)
        ),
        "stop_level": None if stop_level is None else round(float(stop_level), 4),
        "break_even_level": (
            None if break_even_level is None else round(float(break_even_level), 4)
        ),
        "tp1_level": None if tp1_level is None else round(float(tp1_level), 4),
        "tp2_level": None if tp2_level is None else round(float(tp2_level), 4),
        "trailing_mode": _safe_text(trailing_mode),
        "tp_plan": _safe_text(tp_plan),
        "trim_count": None if trim_count is None else int(trim_count),
        "risk_free_armed": None if risk_free_armed is None else bool(risk_free_armed),
        "protected_stop_level": (
            None
            if protected_stop_level is None
            else round(float(protected_stop_level), 4)
        ),
        "add_on_count": None if add_on_count is None else int(add_on_count),
        "add_on_slot": None if add_on_slot is None else int(add_on_slot),
        "max_add_ons": None if max_add_ons is None else int(max_add_ons),
        "tranche_pct": None if tranche_pct is None else round(float(tranche_pct), 4),
        "next_addon_allowed": (
            None if next_addon_allowed is None else bool(next_addon_allowed)
        ),
        "pyramid_state": _safe_text(pyramid_state),
        "base_position_units": (
            None
            if base_position_units is None
            else round(float(base_position_units), 4)
        ),
        "current_position_units": (
            None
            if current_position_units is None
            else round(float(current_position_units), 4)
        ),
        "blended_entry_price": (
            None
            if blended_entry_price is None
            else round(float(blended_entry_price), 4)
        ),
        "last_trailing_confirmed_level": (
            None
            if last_trailing_confirmed_level is None
            else round(float(last_trailing_confirmed_level), 4)
        ),
        "last_protected_stop_level": (
            None
            if last_protected_stop_level is None
            else round(float(last_protected_stop_level), 4)
        ),
        "last_pyramid_reference_level": (
            None
            if last_pyramid_reference_level is None
            else round(float(last_pyramid_reference_level), 4)
        ),
        "strategy_combo": _safe_text(strategy_combo),
        "signal_phase": _safe_text(signal_phase).upper(),
        "reference_target_level": (
            None
            if reference_target_level is None
            else round(float(reference_target_level), 4)
        ),
        "reference_exit_signal": _safe_text(reference_exit_signal),
        "contract_version": _safe_text(contract_version) or _CONTRACT_VERSION_V2,
        "cycle_effect": resolved_cycle_effect,
        "position_units_before": (
            None
            if position_units_before is None
            else round(float(position_units_before), 4)
        ),
        "position_units_after": (
            None
            if position_units_after is None
            else round(float(position_units_after), 4)
        ),
        "position_delta_units": (
            None
            if position_delta_units is None
            else round(float(position_delta_units), 4)
        ),
        "indicator_dog_rule_ids": resolved_indicator_dog_rule_ids,
        "source_tags": resolved_source_tags,
        "reason_codes": list(dict.fromkeys(_to_list(reason_codes))),
        "quality_flags": list(dict.fromkeys(_to_list(quality_flags))),
    }


def _set_row_code(
    row: Mapping[str, Any],
    signal_code: str,
) -> dict[str, Any]:
    updated = dict(row)
    updated["signal_code"] = signal_code
    updated["display_label"] = _signal_code_label(signal_code)
    return updated


def _blank_ug_tf_fields(
    updated: dict[str, Any], *, cycle: bool = False
) -> dict[str, Any]:
    fields = _UG_CYCLE_TF_ONLY_FIELDS if cycle else _UG_TF_ONLY_FIELDS
    for field in fields:
        updated[field] = "" if field in {"trailing_mode", "tp_plan"} else None
    return updated


def _v2_signal_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    updated = dict(row)
    updated["contract_version"] = _CONTRACT_VERSION_V2
    if _safe_text(updated.get("engine")) == "UG":
        updated = _blank_ug_tf_fields(updated)
    return updated


def _strip_internal_signal_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in dict(row).items()
        if not key.startswith("_") and key not in _PUBLIC_PROJECTION_INTERNAL_FIELDS
    }


def _transform_signal_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    strip_internal: bool = True,
) -> list[dict[str, Any]]:
    transformed: list[dict[str, Any]] = []
    for row in rows:
        updated = _v2_signal_row(row)
        if updated is not None:
            if strip_internal:
                updated = _strip_internal_signal_fields(updated)
            transformed.append(updated)
    return _sorted_signal_rows(transformed)


def _sanitize_cycle_row(row: Mapping[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated["contract_version"] = _CONTRACT_VERSION_V2
    if _safe_text(updated.get("engine")) == "UG":
        updated = _blank_ug_tf_fields(updated, cycle=True)
    return updated


def _ug_cycle_zone(
    family: str,
    metrics: Mapping[str, Any],
    *,
    support_low: float | None = None,
    support_high: float | None = None,
    stop_level: float | None = None,
) -> tuple[float | None, float | None, float | None]:
    if family == "UG_BREAKOUT":
        low = _pick_latest(_safe_float(metrics.get("bb_mid")), support_low)
        high = _pick_latest(_safe_float(metrics.get("bb_upper")), support_high)
        stop = _pick_latest(_safe_float(metrics.get("bb_mid")), stop_level, low)
        return low, high, stop
    if family == "UG_PULLBACK":
        low = _pick_latest(_safe_float(metrics.get("bb_zone_low")), support_low)
        high = _pick_latest(_safe_float(metrics.get("bb_zone_high")), support_high)
        stop = _pick_latest(_safe_float(metrics.get("bb_zone_low")), stop_level, low)
        return low, high, stop
    if family == "UG_MEAN_REVERSION":
        low = _pick_latest(_safe_float(metrics.get("bb_lower")), support_low)
        high = _pick_latest(_safe_float(metrics.get("bb_mid")), support_high)
        stop = _pick_latest(_safe_float(metrics.get("bb_lower")), stop_level, low)
        return low, high, stop
    return support_low, support_high, stop_level


def _summary_text(items: Any) -> str:
    values = []
    for item in _to_list(items):
        text = _safe_text(item)
        if text and text not in values:
            values.append(text)
    return " ".join(values)


def _market_condition_is_weak(state: Any) -> bool:
    return _safe_text(state).upper() in {"RISK_OFF", "WEAK", "BEARISH", "RED"}


def _market_condition_is_strong(state: Any) -> bool:
    return _safe_text(state).upper() in {"BULLISH", "GREEN", "RISK_ON"}


def _apply_shared_market_truth_to_registry(
    registry: dict[str, dict[str, Any]],
    market_truth: _market_intel_bridge.MarketTruthSnapshot,
    *,
    market_truth_source: str,
    core_overlay_applied: bool,
) -> dict[str, dict[str, Any]]:
    market_state = _market_intel_bridge.shared_market_alias_to_signal_state(market_truth.market_alias)
    market_reason = _market_intel_bridge.market_truth_reason(market_truth)
    for entry in registry.values():
        entry["market_condition_state"] = market_state
        entry["market_condition_reason"] = market_reason
        entry["market_alignment_score"] = market_truth.market_alignment_score
        entry["breadth_support_score"] = market_truth.breadth_support_score
        entry["rotation_support_score"] = market_truth.rotation_support_score
        entry["leader_health_score"] = market_truth.leader_health_score
        entry["regime_state"] = market_truth.regime_state
        entry["top_state"] = market_truth.top_state
        entry["market_state"] = market_truth.market_state
        entry["breadth_state"] = market_truth.breadth_state
        entry["concentration_state"] = market_truth.concentration_state
        entry["leadership_state"] = market_truth.leadership_state
        entry["market_truth_source"] = market_truth_source
        entry["core_overlay_applied"] = core_overlay_applied
    return registry


def _buy_sizing_overlay(
    row: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sizing_entry_price": None,
        "sizing_stop_price": None,
        "sizing_stop_distance_pct": None,
        "sizing_risk_budget_pct": None,
        "sizing_position_cap_pct": None,
        "sizing_raw_position_pct": None,
        "sizing_recommended_position_pct": None,
        "sizing_tier": None,
        "sizing_reason": "",
        "sizing_status": "NON_BUY",
    }
    if _safe_text(row.get("action_type")).upper() != "BUY":
        payload["sizing_reason"] = "NON_BUY"
        return payload

    grade = _safe_text(row.get("conviction_grade")).upper()
    market_state = _safe_text(row.get("market_condition_state")).upper()
    if _market_condition_is_strong(market_state) and grade in {"A", "S"}:
        tier = "TOP"
        risk_budget_pct = 2.0
        position_cap_pct = 20.0
        tier_reason = f"GRADE_{grade}|MARKET_{market_state}"
    elif _market_condition_is_strong(market_state) and grade == "B":
        tier = "STRONG"
        risk_budget_pct = 1.5
        position_cap_pct = 10.0
        tier_reason = f"GRADE_B|MARKET_{market_state}"
    else:
        tier = "BASE"
        risk_budget_pct = 1.0
        position_cap_pct = 10.0
        tier_reason = "BASE_FALLBACK"

    payload["sizing_tier"] = tier
    payload["sizing_risk_budget_pct"] = risk_budget_pct
    payload["sizing_position_cap_pct"] = position_cap_pct

    entry_price = _pick_latest(
        _safe_float(row.get("blended_entry_price")),
        _safe_float(metrics.get("close")),
    )
    stop_price = _safe_float(row.get("stop_level"))
    payload["sizing_entry_price"] = entry_price
    payload["sizing_stop_price"] = stop_price

    if entry_price is None or entry_price <= 0 or stop_price is None:
        payload["sizing_status"] = "MISSING_PRICE"
        payload["sizing_reason"] = "MISSING_PRICE"
        return payload

    stop_distance_frac = (entry_price - stop_price) / entry_price
    if stop_distance_frac <= 0:
        payload["sizing_status"] = "INVALID_STOP"
        payload["sizing_reason"] = "INVALID_STOP"
        return payload

    raw_position_frac = (risk_budget_pct / 100.0) / stop_distance_frac
    recommended_position_frac = min(raw_position_frac, position_cap_pct / 100.0)

    payload["sizing_stop_distance_pct"] = stop_distance_frac * 100.0
    payload["sizing_raw_position_pct"] = raw_position_frac * 100.0
    payload["sizing_recommended_position_pct"] = recommended_position_frac * 100.0
    payload["sizing_status"] = "OK"
    payload["sizing_reason"] = tier_reason
    return payload

def _aux_signal_summary(engine: str, metrics: Mapping[str, Any]) -> str:
    engine_key = _safe_text(engine).upper()
    if engine_key == "TREND":
        candidates = [
            ("SETUP", metrics.get("setup_active")),
            ("VCP", metrics.get("vcp_active")),
            ("BUILDUP", metrics.get("build_up_ready")),
            ("DRY_PULLBACK", metrics.get("volume_dry")),
        ]
    elif engine_key == "UG":
        candidates = [
            ("NH60", metrics.get("nh60")),
            ("VOL2X", metrics.get("vol2x")),
            ("W", metrics.get("w_active")),
            ("PBB", metrics.get("ug_pbb_ready")),
            ("SQUEEZE", metrics.get("squeeze_active")),
            ("VCP", metrics.get("vcp_active")),
            ("TIGHT", metrics.get("tight_active")),
        ]
    else:
        candidates = [
            ("SETUP", metrics.get("setup_active")),
            ("VCP", metrics.get("vcp_active")),
            ("BUILDUP", metrics.get("build_up_ready")),
            ("NH60", metrics.get("nh60")),
            ("VOL2X", metrics.get("vol2x")),
            ("W", metrics.get("w_active")),
            ("SQUEEZE", metrics.get("squeeze_active")),
            ("TIGHT", metrics.get("tight_active")),
            ("DRY_PULLBACK", metrics.get("volume_dry")),
        ]
    return _summary_text([label for label, active in candidates if active])


def _buy_warning_summary(metrics: Mapping[str, Any]) -> str:
    alignment = _normalize_alignment_state(
        metrics.get("ema_alignment_state") or metrics.get("alignment_state")
    )
    warnings = []
    if not _safe_bool(metrics.get("above_200ma")):
        warnings.append("BELOW_200MA")
    if _safe_bool(metrics.get("ema_turn_down")):
        warnings.append("EMA_TURN_DOWN")
    if alignment == "BEARISH":
        warnings.append("BEARISH_ALIGNMENT")
    elif alignment == "MIXED":
        warnings.append("MIXED_ALIGNMENT")
    if _market_condition_is_weak(metrics.get("market_condition_state")):
        warnings.append("MARKET_WEAK")
    if not _safe_bool(metrics.get("liquidity_pass")):
        warnings.append("LIQUIDITY_FAIL")
    if _safe_bool(metrics.get("in_channel8")):
        warnings.append("CHANNEL_8_ACTIVE")
    return _summary_text(warnings)


def _conviction_reason(metrics: Mapping[str, Any], row: Mapping[str, Any]) -> str:
    alignment = _normalize_alignment_state(
        metrics.get("ema_alignment_state") or metrics.get("alignment_state")
    )
    action_type = _safe_text(row.get("action_type")).upper()
    signal_code = _safe_text(row.get("signal_code")).upper()
    if _safe_bool(metrics.get("ema_turn_down")):
        return "EMA_TURN_DOWN"
    if alignment == "BEARISH":
        return "BEARISH_ALIGNMENT"
    if _market_condition_is_weak(metrics.get("market_condition_state")):
        return f"MARKET_{_safe_text(metrics.get('market_condition_state')).upper()}"
    if not _safe_bool(metrics.get("liquidity_pass")):
        return "LIQUIDITY_FAIL"
    if action_type in {"BUY", "WATCH"} and not _safe_bool(metrics.get("above_200ma")):
        return "BELOW_200MA"
    if action_type in {"BUY", "WATCH"} and alignment == "MIXED":
        return "MIXED_ALIGNMENT"
    if signal_code in {"TF_BUY_REGULAR", "TF_BUY_PEG_PULLBACK", "UG_BUY_PBB"}:
        if not _safe_bool(metrics.get("pullback_profile_pass")):
            return "PULLBACK_NOT_CONFIRMED"
        if not _safe_bool(metrics.get("prior_expansion_ready")):
            return "NO_PRIOR_EXPANSION"
    if (
        signal_code in {"TF_BUY_BREAKOUT", "UG_BUY_BREAKOUT", "UG_BUY_SQUEEZE_BREAKOUT"}
        and alignment != "BULLISH"
    ):
        return "MIXED_ALIGNMENT" if alignment == "MIXED" else "STRUCTURE_WEAK"
    return ""


def _apply_update_overlay_rows(
    rows: Iterable[Mapping[str, Any]],
    metrics_map: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    updated_rows: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        symbol = _safe_text(updated.get("symbol")).upper()
        metrics = metrics_map.get(symbol, {})
        engine = _safe_text(updated.get("engine"))
        updated["ema_alignment_state"] = _normalize_alignment_state(
            metrics.get("ema_alignment_state") or metrics.get("alignment_state")
        )
        updated["ema_alignment_transition"] = _safe_text(
            metrics.get("ema_alignment_transition")
        )
        updated["market_condition_state"] = (
            _safe_text(metrics.get("market_condition_state")) or "UNKNOWN"
        )
        updated["market_condition_reason"] = _safe_text(
            metrics.get("market_condition_reason")
        )
        updated["pullback_quality"] = _safe_text(metrics.get("pullback_quality"))
        updated["pullback_context"] = _summary_text(metrics.get("pullback_context", []))
        updated["industry_key"] = _safe_text(metrics.get("industry_key"))
        updated["group_state"] = _safe_text(metrics.get("group_state")).upper()
        updated["leader_state"] = _safe_text(metrics.get("leader_state")).upper()
        updated["breakdown_status"] = _safe_text(metrics.get("breakdown_status")).upper()
        updated["group_strength_score"] = _safe_float(metrics.get("group_strength_score"))
        updated["leader_score"] = _safe_float(metrics.get("leader_score"))
        updated["breakdown_score"] = _safe_float(metrics.get("breakdown_score"))
        updated["buy_warning_summary"] = _buy_warning_summary(metrics)
        updated["aux_signal_summary"] = _aux_signal_summary(engine, metrics)
        updated["conviction_reason"] = _conviction_reason(metrics, updated)
        action_type = _safe_text(updated.get("action_type")).upper()
        grade = _safe_text(updated.get("conviction_grade")).upper()
        if action_type in {"BUY", "WATCH"} and grade:
            if (
                _safe_bool(metrics.get("ema_turn_down"))
                or _normalize_alignment_state(metrics.get("alignment_state"))
                == "BEARISH"
            ):
                updated["conviction_grade"] = "D"
            else:
                shifts = 0
                if not _safe_bool(metrics.get("above_200ma")):
                    shifts -= 1
                if (
                    _normalize_alignment_state(metrics.get("alignment_state"))
                    == "MIXED"
                ):
                    shifts -= 1
                if _market_condition_is_weak(metrics.get("market_condition_state")):
                    shifts -= 1
                if shifts:
                    updated["conviction_grade"] = _shift_grade(grade, shifts)
        updated.update(_buy_sizing_overlay(updated, metrics))
        updated_rows.append(updated)
    return updated_rows


def _apply_update_snapshot_rows(
    rows: Iterable[Mapping[str, Any]],
    metrics_map: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    updated_rows: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        symbol = _safe_text(updated.get("symbol")).upper()
        metrics = metrics_map.get(symbol, {})
        updated["ema_alignment_state"] = _normalize_alignment_state(
            metrics.get("ema_alignment_state") or metrics.get("alignment_state")
        )
        updated["ema_alignment_transition"] = _safe_text(
            metrics.get("ema_alignment_transition")
        )
        updated["market_condition_state"] = (
            _safe_text(metrics.get("market_condition_state")) or "UNKNOWN"
        )
        updated["market_condition_reason"] = _safe_text(
            metrics.get("market_condition_reason")
        )
        updated["pullback_quality"] = _safe_text(metrics.get("pullback_quality"))
        updated["pullback_context"] = _summary_text(metrics.get("pullback_context", []))
        updated["industry_key"] = _safe_text(metrics.get("industry_key"))
        updated["group_state"] = _safe_text(metrics.get("group_state")).upper()
        updated["leader_state"] = _safe_text(metrics.get("leader_state")).upper()
        updated["breakdown_status"] = _safe_text(metrics.get("breakdown_status")).upper()
        updated["group_strength_score"] = _safe_float(metrics.get("group_strength_score"))
        updated["leader_score"] = _safe_float(metrics.get("leader_score"))
        updated["breakdown_score"] = _safe_float(metrics.get("breakdown_score"))
        updated["buy_warning_summary"] = _buy_warning_summary(metrics)
        updated["aux_signal_summary"] = _aux_signal_summary("", metrics)
        updated["conviction_reason"] = _conviction_reason(metrics, updated)
        updated_rows.append(updated)
    return updated_rows


def _project_scoped_signal_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    as_of_date: str,
    scope_symbols: Iterable[str] | None = None,
    signal_side: str,
) -> list[dict[str, Any]]:
    normalized_scope = (
        {
            _safe_text(symbol).upper()
            for symbol in scope_symbols
            if _safe_text(symbol)
        }
        if scope_symbols is not None
        else None
    )
    normalized_side = _safe_text(signal_side).upper()
    projected: list[dict[str, Any]] = []

    for row in rows:
        symbol = _safe_text(row.get("symbol")).upper()
        if normalized_scope is not None and symbol not in normalized_scope:
            continue
        if _date_to_str(row.get("signal_date")) != as_of_date:
            continue

        updated = dict(row)
        if normalized_side == "BUY":
            if _safe_text(updated.get("_artifact_action_type")).upper() != "BUY":
                continue
            updated["action_type"] = "BUY"
        elif _safe_text(updated.get("action_type")).upper() not in {"SELL", "EXIT", "TRIM"}:
            continue

        projected.append(_strip_internal_signal_fields(updated))

    return _sorted_signal_rows(projected)


def _count_signal_rows_by(
    rows: Iterable[Mapping[str, Any]],
    field: str,
    *,
    default: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = _safe_text(row.get(field)).upper() or default
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _build_buy_signal_segment_summary(
    *,
    all_rows: Iterable[Mapping[str, Any]],
    screened_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    all_list = list(all_rows)
    screened_list = list(screened_rows)
    return {
        "all_total": len(all_list),
        "screened_total": len(screened_list),
        "all_only_discovery_total": sum(
            1
            for row in all_list
            if _normalize_scope(row.get("scope")) == _ALL_SCOPE
            and _safe_text(row.get("source_disposition")).lower() == "discovery_only"
        ),
        "all_source_disposition_counts": _count_signal_rows_by(
            all_list,
            "source_disposition",
            default="UNSPECIFIED",
        ),
        "all_source_fit_label_counts": _count_signal_rows_by(
            all_list,
            "source_fit_label",
            default="NONE",
        ),
        "screened_source_fit_label_counts": _count_signal_rows_by(
            screened_list,
            "source_fit_label",
            default="NONE",
        ),
        "all_signal_code_counts": _count_signal_rows_by(
            all_list,
            "signal_code",
            default="UNKNOWN",
        ),
        "screened_signal_code_counts": _count_signal_rows_by(
            screened_list,
            "signal_code",
            default="UNKNOWN",
        ),
    }


class PEGImminentScreener:

    def __init__(
        self,
        *,
        market: str = "us",
        as_of_date: str | None = None,
        upcoming_earnings_fetcher: (
            Callable[[str, str | None, int], pd.DataFrame] | None
        ) = None,
        metadata_map: Mapping[str, Mapping[str, Any]] | None = None,
        financial_map: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self.market = market_key(market)
        self.as_of_date = _date_to_str(as_of_date) or datetime.now().strftime(
            "%Y-%m-%d"
        )
        self.results_dir = get_peg_imminent_results_dir(self.market)
        self.upcoming_earnings_fetcher = (
            upcoming_earnings_fetcher or self._fetch_upcoming_earnings
        )
        self.metadata_map = (
            dict(metadata_map)
            if metadata_map is not None
            else _load_metadata_map(self.market)
        )
        self.financial_map: dict[str, dict[str, Any]] = {
            str(symbol).upper(): dict(row)
            for symbol, row in (financial_map or {}).items()
            if str(symbol).strip()
        }

    def _fetch_upcoming_earnings(
        self, market: str, as_of_date: str | None, business_days: int
    ) -> pd.DataFrame:

        if not _YAHOO_FIN_AVAILABLE:

            return pd.DataFrame()

        start = _date_to_str(as_of_date) or datetime.now().strftime("%Y-%m-%d")

        start_ts = _coerce_date(start)

        if start_ts is None:

            start_ts = pd.Timestamp(datetime.now().date())

        end_ts = pd.bdate_range(start_ts, periods=business_days + 1)[-1]

        try:

            payload = si.get_earnings_in_date_range(
                start_ts.strftime("%Y-%m-%d"), end_ts.strftime("%Y-%m-%d")
            )

        except Exception:

            return pd.DataFrame()

        if isinstance(payload, pd.DataFrame):

            frame = payload.copy()

        elif isinstance(payload, dict):

            if payload and all(isinstance(value, dict) for value in payload.values()):

                frame = pd.DataFrame(
                    [{**value, "symbol": key} for key, value in payload.items()]
                )

            else:

                frame = pd.DataFrame(payload)

        elif isinstance(payload, list):

            frame = pd.DataFrame(payload)

        else:

            frame = pd.DataFrame()

        return frame

    def _normalize_upcoming_earnings(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        normalized = frame.copy()
        normalized.columns = [
            str(column).strip().lower() for column in normalized.columns
        ]
        symbol_source = "symbol"
        if "symbol" not in normalized.columns:
            for candidate in ("ticker", "provider_symbol", "companyshortname"):
                if candidate in normalized.columns:
                    normalized["symbol"] = normalized[candidate]
                    symbol_source = candidate
                    break
        date_column = None
        for candidate in ("startdatetime", "date", "earnings_date", "startdate"):
            if candidate in normalized.columns:
                date_column = candidate
                break
        if "symbol" not in normalized.columns or date_column is None:
            return pd.DataFrame()
        if symbol_source == "provider_symbol":
            normalized["symbol"] = normalized["symbol"].map(
                lambda value: (
                    normalize_symbol_value(
                        normalize_provider_symbol_value(value).rsplit(".", 1)[0],
                        self.market,
                    )
                    if normalize_provider_symbol_value(value)
                    and "." in normalize_provider_symbol_value(value)
                    else normalize_symbol_value(value, self.market)
                )
            )
            normalized["symbol"] = (
                normalized["symbol"].replace("", pd.NA).astype("string")
            )
        else:
            normalized = normalize_symbol_columns(
                normalized, self.market, columns=("symbol",)
            )
        normalized["earnings_date"] = pd.to_datetime(
            normalized[date_column], errors="coerce"
        )
        normalized = normalized.dropna(subset=["symbol", "earnings_date"]).copy()
        normalized["earnings_date"] = normalized["earnings_date"].dt.strftime(
            "%Y-%m-%d"
        )
        return normalized.reset_index(drop=True)

    def _compute_peg_ready(
        self, raw_rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:

        rows: list[dict[str, Any]] = []

        for symbol in dict.fromkeys(
            [str(row.get("symbol") or "").upper() for row in raw_rows]
        ):

            if not symbol:

                continue

            frame = load_local_ohlcv_frame(
                self.market,
                symbol,
                as_of=self.as_of_date,
                price_policy=PricePolicy.SPLIT_ADJUSTED,
            )

            if frame.empty:

                continue

            metrics = _build_metrics(
                symbol=symbol,
                market=self.market,
                frame=frame,
                metadata=self.metadata_map.get(symbol, {}),
                financial_row=self.financial_map.get(symbol, {}),
                feature_row={},
                source_entry={
                    "screen_stage": "PEG_READY",
                    "source_tags": ["PEG_IMMINENT"],
                    "source_overlap_bonus": 0.0,
                },
            )
            setup_pass = any(
                [
                    metrics.get("build_up_ready"),
                    metrics.get("vcp_active"),
                    metrics.get("squeeze_active"),
                    metrics.get("alignment_state") == "BULLISH",
                ]
            )
            if not (
                metrics.get("liquidity_pass")
                and setup_pass
                and not metrics.get("ema_turn_down")
            ):
                continue
            raw = next(
                (
                    row
                    for row in raw_rows
                    if str(row.get("symbol") or "").upper() == symbol
                ),
                {},
            )
            rows.append(
                {
                    "as_of_ts": self.as_of_date,
                    "symbol": symbol,
                    "market": self.market.upper(),
                    "earnings_date": raw.get("earnings_date"),
                    "close": metrics.get("close"),
                    "sma200": metrics.get("sma200"),
                    "adr_pct": metrics.get("adr_pct"),
                    "atr_pct": metrics.get("atr_pct"),
                    "build_up_ready": bool(metrics.get("build_up_ready")),
                    "vcp_active": bool(metrics.get("vcp_active")),
                    "squeeze_active": bool(metrics.get("squeeze_active")),
                    "alignment_state": metrics.get("alignment_state"),
                    "reason_codes": [
                        reason
                        for reason, condition in (
                            ("ABOVE_200MA", metrics.get("above_200ma")),
                            ("BELOW_200MA", not metrics.get("above_200ma")),
                            ("LIQUIDITY_PASS", metrics.get("liquidity_pass")),
                            ("BUILD_UP", metrics.get("build_up_ready")),
                            ("VCP", metrics.get("vcp_active")),
                            ("SQUEEZE", metrics.get("squeeze_active")),
                            ("ALIGNMENT", metrics.get("alignment_state") == "BULLISH"),
                        )
                        if condition
                    ],
                }
            )
        return rows

    def run(self) -> dict[str, list[dict[str, Any]]]:
        ensure_market_dirs(self.market)
        os.makedirs(self.results_dir, exist_ok=True)
        raw_frame = self.upcoming_earnings_fetcher(self.market, self.as_of_date, 10)
        normalized = self._normalize_upcoming_earnings(raw_frame)
        raw_rows = (
            [
                {
                    "as_of_ts": self.as_of_date,
                    "symbol": row.get("symbol"),
                    "market": self.market.upper(),
                    "earnings_date": row.get("earnings_date"),
                }
                for _, row in normalized.iterrows()
            ]
            if not normalized.empty
            else []
        )

        missing_financial_symbols = [
            symbol
            for symbol in dict.fromkeys(
                _safe_text(row.get("symbol")).upper() for row in raw_rows
            )
            if symbol and symbol not in self.financial_map
        ]
        if missing_financial_symbols:
            self.financial_map.update(
                _load_financial_map(self.market, symbols=missing_financial_symbols)
            )

        ready_rows = self._compute_peg_ready(raw_rows) if raw_rows else []

        _write_records(self.results_dir, "peg_imminent_raw", raw_rows)

        _write_records(self.results_dir, "peg_ready", ready_rows)

        return {"peg_imminent_raw": raw_rows, "peg_ready": ready_rows}


def _build_metrics(
    *,
    symbol: str,
    market: str,
    frame: pd.DataFrame,
    metadata: Mapping[str, Any],
    financial_row: Mapping[str, Any],
    feature_row: Mapping[str, Any],
    source_entry: Mapping[str, Any],
) -> dict[str, Any]:
    if frame.empty:
        return {"symbol": symbol, "daily": pd.DataFrame()}

    daily = normalize_indicator_frame(
        frame, symbol=symbol, price_policy=PricePolicy.SPLIT_ADJUSTED
    )

    if daily.empty:

        return {"symbol": symbol, "daily": daily}

    if len(daily) < 2:
        close = pd.to_numeric(daily["close"], errors="coerce")
        high = pd.to_numeric(daily["high"], errors="coerce")
        low = pd.to_numeric(daily["low"], errors="coerce")
        open_ = pd.to_numeric(daily["open"], errors="coerce")
        volume = pd.to_numeric(daily["volume"], errors="coerce")
        return {
            "symbol": symbol,
            "market": market,
            "date": _date_to_str(daily.iloc[-1]["date"]),
            "daily": daily,
            "close": _safe_float(close.iloc[-1]),
            "open": _safe_float(open_.iloc[-1]),
            "high": _safe_float(high.iloc[-1]),
            "low": _safe_float(low.iloc[-1]),
            "volume": _safe_float(volume.iloc[-1]),
        }

    close = pd.to_numeric(daily["close"], errors="coerce")

    high = pd.to_numeric(daily["high"], errors="coerce")

    low = pd.to_numeric(daily["low"], errors="coerce")

    open_ = pd.to_numeric(daily["open"], errors="coerce")

    volume = pd.to_numeric(daily["volume"], errors="coerce")

    ema5 = rolling_ema(close, 5, adjust=False)

    ema8 = rolling_ema(close, 8, adjust=False)

    ema10 = rolling_ema(close, 10, adjust=False)

    ema14 = rolling_ema(close, 14, adjust=False)

    ema20 = rolling_ema(close, 20, adjust=False)

    ema21 = rolling_ema(close, 21, adjust=False)

    ema55 = rolling_ema(close, 55, adjust=False)

    sma50 = rolling_sma(close, 50, min_periods=25)

    sma100 = rolling_sma(close, 100, min_periods=50)

    sma200 = rolling_sma(close, 200, min_periods=120)

    atr14_series = rolling_atr(daily, 14, min_periods=14)

    adx14_series = _rolling_adx(daily, 14)

    vol_ma50_series = rolling_average_volume(daily, 50, min_periods=20)

    lower_band, mid_band, upper_band = _bollinger(daily)

    macd_line, macd_signal, macd_hist = _macd(daily)

    rsi14 = _rsi_series(close, 14)

    atr14 = _safe_float(atr14_series.iloc[-1])

    adr20 = adr_percent(daily, length=20)

    atr_pct_value = atr_percent(daily, length=14)

    rvol20 = None

    if len(volume.dropna()) >= 20:

        avg_volume_20 = _safe_float(volume.iloc[-20:].mean())

        latest_volume = _safe_float(volume.iloc[-1])

        if avg_volume_20 and latest_volume is not None:

            rvol20 = latest_volume / avg_volume_20

    bullish_rvol50 = _signal_patterns.bullish_rvol50(daily)

    prev_close = _safe_float(close.iloc[-2]) if len(close) >= 2 else None

    gap_pct = None

    daily_return_pct = None

    if prev_close and prev_close != 0:

        latest_open = _safe_float(open_.iloc[-1])

        latest_close = _safe_float(close.iloc[-1])

        if latest_open is not None:

            gap_pct = ((latest_open / prev_close) - 1.0) * 100.0

        if latest_close is not None:

            daily_return_pct = ((latest_close / prev_close) - 1.0) * 100.0

    donchian_high20 = _safe_float(
        rolling_max(high, 20, min_periods=10).shift(1).iloc[-1]
    )
    donchian_low20 = _safe_float(rolling_min(low, 20, min_periods=10).shift(1).iloc[-1])
    prior_high60 = _safe_float(rolling_max(high, 60, min_periods=20).shift(1).iloc[-1])
    prior_high15 = _safe_float(rolling_max(high, 15, min_periods=5).shift(1).iloc[-1])
    prior_low20 = _safe_float(rolling_min(low, 20, min_periods=10).shift(1).iloc[-1])
    channel_high8 = _safe_float(
        rolling_max(high, _TF_CHANNEL_LOOKBACK, min_periods=4).shift(1).iloc[-1]
    )
    channel_low8 = _safe_float(
        rolling_min(low, _TF_CHANNEL_LOOKBACK, min_periods=4).shift(1).iloc[-1]
    )
    recent_high15 = (
        _safe_float(high.iloc[-15:].max())
        if len(high) >= 15
        else _safe_float(high.max())
    )
    recent_low10 = (
        _safe_float(low.iloc[-10:].min()) if len(low) >= 10 else _safe_float(low.min())
    )

    stock_character = _classify_stock_character(daily, adr20, atr_pct_value)

    ma_system = _select_ma_system(stock_character)

    if ma_system == "FIBO":

        fast_ref = _safe_float(ema8.iloc[-1])

        mid_ref = _safe_float(ema21.iloc[-1])

        slow_ref = _safe_float(ema55.iloc[-1])

        fast_prev = (
            _safe_float(ema8.iloc[-3]) if len(ema8) >= 3 else _safe_float(ema8.iloc[-2])
        )

        mid_prev = (
            _safe_float(ema21.iloc[-3])
            if len(ema21) >= 3
            else _safe_float(ema21.iloc[-2])
        )

        slow_prev = (
            _safe_float(ema55.iloc[-3])
            if len(ema55) >= 3
            else _safe_float(ema55.iloc[-2])
        )

        ema_turn_down = (_safe_float(ema8.iloc[-1]) or 0.0) < (
            _safe_float(ema8.iloc[-2]) or 0.0
        )

    else:

        fast_ref = _safe_float(ema10.iloc[-1])

        mid_ref = _safe_float(ema20.iloc[-1])

        slow_ref = _safe_float(sma50.iloc[-1])

        fast_prev = (
            _safe_float(ema10.iloc[-3])
            if len(ema10) >= 3
            else _safe_float(ema10.iloc[-2])
        )

        mid_prev = (
            _safe_float(ema20.iloc[-3])
            if len(ema20) >= 3
            else _safe_float(ema20.iloc[-2])
        )

        slow_prev = (
            _safe_float(sma50.iloc[-3])
            if len(sma50) >= 3
            else _safe_float(sma50.iloc[-2])
        )

        ema_turn_down = (_safe_float(ema10.iloc[-1]) or 0.0) < (
            _safe_float(ema10.iloc[-2]) or 0.0
        )

    previous_close_value = (
        _safe_float(close.iloc[-2]) if len(close) >= 2 else _safe_float(close.iloc[-1])
    )

    alignment_state = _alignment_state_from_refs(
        fast_ref,
        mid_ref,
        slow_ref,
        _safe_float(close.iloc[-1]),
    )

    previous_alignment_state = _alignment_state_from_refs(
        fast_prev,
        mid_prev,
        slow_prev,
        previous_close_value,
    )

    ema_alignment_transition = _alignment_transition(
        previous_alignment_state, alignment_state
    )

    ma_gap_pct = None

    ma_gap_pass = False

    if fast_ref and mid_ref and mid_ref != 0:

        ma_gap_pct = abs(fast_ref - mid_ref) / abs(mid_ref) * 100.0

        ma_gap_pass = ma_gap_pct >= 2.0

    bb_width_pct = None

    mid_band_value = _safe_float(mid_band.iloc[-1])

    upper_band_value = _safe_float(upper_band.iloc[-1])

    lower_band_value = _safe_float(lower_band.iloc[-1])

    if mid_band_value:
        bb_width_pct = (
            ((upper_band_value or 0.0) - (lower_band_value or 0.0))
            / mid_band_value
            * 100.0
        )

    tight_active = _signal_patterns.detect_tight_range(daily)
    squeeze_active = bool(
        (bb_width_pct or 999.0) <= 12.0 and (atr_pct_value or 999.0) <= 2.5
    )
    volume_dry = _signal_patterns.detect_dry_volume(daily)

    vcp_active = False
    nh60 = bool(
        prior_high60 and (_safe_float(close.iloc[-1]) or 0.0) >= prior_high60 * 0.995
    )

    w_features = _signal_patterns.detect_w_pattern(daily)
    w_pending = bool(w_features.get("w_pending"))
    w_confirmed = bool(w_features.get("w_confirmed"))
    w_active = bool(w_pending or w_confirmed)

    above_200ma = bool(
        _safe_float(sma200.iloc[-1])
        and (_safe_float(close.iloc[-1]) or 0.0)
        >= (_safe_float(sma200.iloc[-1]) or 0.0)
    )

    liquidity_pass = bool(
        (_safe_float(vol_ma50_series.iloc[-1]) or 0.0)
        >= _ANALYZER.market_profile(market).min_adv20
    )

    risk_heat = bool(
        atr14
        and abs(
            (_safe_float(close.iloc[-1]) or 0.0) - (_safe_float(ema20.iloc[-1]) or 0.0)
        )
        / atr14
        >= 7.0
    )

    zone_width_pct = _compute_zone_width_pct(atr_pct_value, adr20)

    trend_support_anchor = _pick_latest(mid_ref, fast_ref, slow_ref)

    trend_zone_low, trend_zone_high = _zone_bounds(trend_support_anchor, zone_width_pct)

    bb_support_anchor = _pick_latest(mid_band_value, lower_band_value)

    bb_zone_low, bb_zone_high = _zone_bounds(bb_support_anchor, zone_width_pct)

    close_value = _safe_float(close.iloc[-1])

    open_value = _safe_float(open_.iloc[-1])

    low_value = _safe_float(low.iloc[-1])

    high_value = _safe_float(high.iloc[-1])

    close_position_pct = _candle_close_position_pct(high_value, low_value, close_value)
    body_strength_pct = _candle_body_strength_pct(
        open_value, high_value, low_value, close_value
    )
    channel_width_pct8 = None
    if channel_high8 is not None and channel_low8 is not None:
        channel_mid8 = (channel_high8 + channel_low8) / 2.0
        if channel_mid8 != 0:
            channel_width_pct8 = (
                (channel_high8 - channel_low8) / abs(channel_mid8)
            ) * 100.0
    in_channel8 = bool(
        channel_high8 is not None
        and channel_low8 is not None
        and high_value is not None
        and low_value is not None
        and low_value > channel_low8
        and high_value < channel_high8
    )
    channel_upper_break8 = bool(
        channel_high8 is not None
        and close_value is not None
        and close_value >= channel_high8
    )
    channel_lower_break8 = bool(
        channel_low8 is not None
        and close_value is not None
        and close_value <= channel_low8
    )
    vcp_features = _signal_patterns.detect_vcp_features(
        daily,
        volume_dry=volume_dry,
        tight_active=tight_active,
        bullish_rvol50_value=bullish_rvol50,
        close_position_pct=close_position_pct,
        risk_heat=risk_heat,
    )
    vcp_active = bool(vcp_features.get("vcp_setup_active"))
    pocket_pivot_features = _signal_patterns.detect_pocket_pivot(daily)
    atr_pct_series = pd.Series(index=close.index, dtype=float)
    close_abs = close.abs().replace(0, np.nan)
    if not atr14_series.empty:
        atr_pct_series = (atr14_series / close_abs) * 100.0
    bb_width_pct_series = pd.Series(index=close.index, dtype=float)
    if mid_band is not None:
        mid_abs = pd.to_numeric(mid_band, errors="coerce").abs().replace(0, np.nan)
        bb_width_pct_series = (
            (
                pd.to_numeric(upper_band, errors="coerce")
                - pd.to_numeric(lower_band, errors="coerce")
            )
            / mid_abs
        ) * 100.0
    tight_series = (atr_pct_series <= 2.5).fillna(False)
    squeeze_series = ((bb_width_pct_series <= 12.0) & (atr_pct_series <= 2.5)).fillna(
        False
    )
    recent_squeeze_ready10 = bool(
        squeeze_series.shift(1, fill_value=False).tail(10).any()
        or tight_series.shift(1, fill_value=False).tail(10).any()
        or vcp_active
    )
    fast_ref_rising = _is_rising(fast_ref, fast_prev)
    mid_ref_rising = _is_rising(mid_ref, mid_prev)
    slow_ref_rising = _is_rising(slow_ref, slow_prev)

    support_trend_rising = mid_ref_rising and slow_ref_rising

    recent_extension_pct = _pct_distance(mid_ref, recent_high15)

    recent_pullback_depth_pct = None

    if recent_high15 and close_value is not None and recent_high15 != 0:

        recent_pullback_depth_pct = (
            (recent_high15 - close_value) / recent_high15
        ) * 100.0

    zone_hold = bool(
        trend_zone_low is not None
        and trend_zone_high is not None
        and low_value is not None
        and close_value is not None
        and low_value <= trend_zone_high
        and close_value >= trend_zone_low
    )

    zone_reclaim = bool(
        trend_zone_low is not None
        and low_value is not None
        and close_value is not None
        and low_value < trend_zone_low
        and close_value >= trend_zone_low
    )

    bb_zone_hold = bool(
        bb_zone_low is not None
        and bb_zone_high is not None
        and low_value is not None
        and close_value is not None
        and low_value <= bb_zone_high
        and close_value >= bb_zone_low
    )

    bb_zone_reclaim = bool(
        bb_zone_low is not None
        and low_value is not None
        and close_value is not None
        and low_value < bb_zone_low
        and close_value >= bb_zone_low
    )

    bullish_reversal = bool(
        close_value is not None
        and open_value is not None
        and close_value > open_value
        and ((_safe_float(daily_return_pct) or 0.0) > 0.0)
        and (close_position_pct or 0.0) >= 0.60
    )

    prior_expansion_ready = bool(
        recent_extension_pct is not None
        and recent_extension_pct >= max(6.0, (adr20 or 0.0) * 1.25)
    )

    pullback_reset_ok = bool(
        recent_pullback_depth_pct is not None
        and 1.0 <= recent_pullback_depth_pct <= 12.0
        and volume_dry
        and not risk_heat
    )

    ema_zone_touch = bool(zone_hold or zone_reclaim or bb_zone_hold or bb_zone_reclaim)

    pullback_reversal_confirmed = bool(
        bullish_reversal or zone_reclaim or bb_zone_reclaim
    )

    pullback_profile_pass = bool(
        prior_expansion_ready
        and pullback_reset_ok
        and support_trend_rising
        and not ema_turn_down
        and ema_zone_touch
        and pullback_reversal_confirmed
    )

    pullback_context_codes = [
        *(["PRIOR_EXPANSION"] if prior_expansion_ready else []),
        *(["EMA_ZONE_TOUCH"] if ema_zone_touch else []),
        *(["DRY_PULLBACK"] if volume_dry else []),
        *(["EMA_RISING"] if support_trend_rising else []),
        *(["REVERSAL_CONFIRMED"] if pullback_reversal_confirmed else []),
    ]

    pullback_context = " ".join(pullback_context_codes)

    pullback_score = sum(
        1
        for condition in (
            prior_expansion_ready,
            ema_zone_touch,
            volume_dry,
            support_trend_rising,
            not ema_turn_down,
            pullback_reversal_confirmed,
        )
        if condition
    )

    if pullback_score >= 6:

        pullback_quality = "HIGH"

    elif pullback_score >= 4:

        pullback_quality = "MEDIUM"

    elif pullback_score >= 2:

        pullback_quality = "LOW"

    else:

        pullback_quality = "NONE"

    market_condition_state = (
        _safe_text(
            source_entry.get("market_condition_state")
            or metadata.get("market_condition_state")
        ).upper()
        or "UNKNOWN"
    )

    market_condition_reason = _safe_text(
        source_entry.get("market_condition_reason")
        or metadata.get("market_condition_reason")
    )

    near_high_ready = bool(
        (_safe_float(feature_row.get("high_52w_proximity")) or 0.0) >= 0.90
        or nh60
        or (
            prior_high60 is not None
            and close_value is not None
            and close_value >= prior_high60 * 0.94
        )
    )

    setup_active = bool(
        alignment_state == "BULLISH"
        and support_trend_rising
        and not ema_turn_down
        and (
            zone_hold
            or zone_reclaim
            or (
                close_value is not None
                and trend_support_anchor is not None
                and close_value >= trend_support_anchor
            )
        )
        and (
            tight_active
            or volume_dry
            or ((_safe_float(feature_row.get("compression_score")) or 0.0) >= 60.0)
        )
    )

    momentum_candle_strong = bool(
        (close_position_pct or 0.0) >= 0.65
        and ((_safe_float(daily_return_pct) or 0.0) >= max(1.5, (adr20 or 0.0) * 0.35))
    )

    breakout_anchor = _pick_latest(donchian_high20, prior_high60)

    breakout_anchor_clear = bool(
        breakout_anchor is not None
        and close_value is not None
        and close_value >= breakout_anchor
    )

    breakout_band_clear = bool(
        upper_band_value is not None
        and close_value is not None
        and close_value >= upper_band_value
    )

    breakout_close_strong = bool((close_position_pct or 0.0) >= 0.70)

    breakout_body_strong = bool((body_strength_pct or 0.0) >= 0.55)

    breakout_energy = bool(
        (_safe_float(daily_return_pct) or 0.0) >= max(2.0, (adr20 or 0.0) * 0.45)
    )

    breakout_ready = bool(
        liquidity_pass
        and support_trend_rising
        and not ema_turn_down
        and breakout_anchor_clear
        and breakout_band_clear
        and ((_safe_float(bullish_rvol50) or 0.0) >= 1.5)
        and breakout_close_strong
        and breakout_body_strong
        and breakout_energy
    )

    breakout_fakeout_risk = bool(
        breakout_anchor is not None
        and high_value is not None
        and high_value >= breakout_anchor
        and not breakout_ready
    )

    bb_percent_b = None
    bb_z_score = None
    if (
        close_value is not None
        and lower_band_value is not None
        and upper_band_value is not None
        and upper_band_value > lower_band_value
    ):
        bb_percent_b = (close_value - lower_band_value) / (
            upper_band_value - lower_band_value
        )
    if (
        close_value is not None
        and mid_band_value is not None
        and upper_band_value is not None
        and upper_band_value > mid_band_value
    ):
        bb_std_proxy = (upper_band_value - mid_band_value) / 2.0
        if bb_std_proxy > 0:
            bb_z_score = (close_value - mid_band_value) / bb_std_proxy
    band_reversion_features = _signal_patterns.score_band_reversion(
        daily,
        {
            "close": close_value,
            "open": open_value,
            "high": high_value,
            "low": low_value,
            "bb_lower": lower_band_value,
            "bb_mid": mid_band_value,
            "bb_upper": upper_band_value,
            "bb_zone_low": bb_zone_low,
            "bb_percent_b": bb_percent_b,
            "bb_z_score": bb_z_score,
            "rsi14": _safe_float(rsi14.iloc[-1]),
            "daily_return_pct": daily_return_pct,
            "close_position_pct": close_position_pct,
            "above_200ma": above_200ma,
            "alignment_state": alignment_state,
            "support_trend_rising": support_trend_rising,
            "ema_turn_down": ema_turn_down,
            "pullback_profile_pass": pullback_profile_pass,
            "risk_heat": risk_heat,
        },
    )
    ug_pbb_ready = bool(band_reversion_features.get("pbb_ready"))
    ug_mr_long_ready = bool(band_reversion_features.get("mr_long_ready"))
    ug_breakdown_risk = bool(
        band_reversion_features.get("breakdown_risk")
        or (
            bb_zone_low is not None
            and close_value is not None
            and close_value <= bb_zone_low
        )
    )
    ug_pbs_ready = bool(band_reversion_features.get("pbs_ready"))
    ug_mr_short_ready = bool(band_reversion_features.get("mr_short_ready"))

    momentum_ready = bool(
        liquidity_pass
        and support_trend_rising
        and not ema_turn_down
        and ((_safe_float(rsi14.iloc[-1]) or 0.0) >= 60.0)
        and (
            (_safe_float(macd_line.iloc[-1]) or -999.0)
            > (_safe_float(macd_signal.iloc[-1]) or 999.0)
        )
        and ((_safe_float(macd_hist.iloc[-1]) or -999.0) > 0.0)
        and close_value is not None
        and (donchian_high20 or 0.0) > 0.0
        and close_value >= (donchian_high20 or 0.0)
        and ((_safe_float(bullish_rvol50) or 0.0) >= 1.2)
        and momentum_candle_strong
        and not risk_heat
    )

    source_tags = [
        str(item) for item in source_entry.get("source_tags", []) if str(item)
    ]

    source_overlap_bonus = float(source_entry.get("source_overlap_bonus") or 0.0)

    build_up_ready = bool(
        setup_active
        and (vcp_active or squeeze_active or tight_active)
        and near_high_ready
    )
    recent_orange_ready10 = bool(
        recent_squeeze_ready10
        or build_up_ready
        or vcp_active
        or squeeze_active
        or tight_active
    )
    aggressive_ready = bool(
        not above_200ma
        and alignment_state in {"BULLISH", "MIXED"}
        and fast_ref_rising
        and mid_ref_rising
        and not ema_turn_down
        and ((_safe_float(adx14_series.iloc[-1]) or 0.0) >= 15.0)
        and close_value is not None
        and mid_ref is not None
        and close_value >= mid_ref
        and bullish_reversal
    )

    return {
        "symbol": symbol,
        "market": market,
        "date": _date_to_str(daily.iloc[-1]["date"]),
        "daily": daily,
        "close": _safe_float(close.iloc[-1]),
        "open": _safe_float(open_.iloc[-1]),
        "high": _safe_float(high.iloc[-1]),
        "low": _safe_float(low.iloc[-1]),
        "volume": _safe_float(volume.iloc[-1]),
        "prev_close": prev_close,
        "gap_pct": gap_pct,
        "daily_return_pct": daily_return_pct,
        "ema5": _safe_float(ema5.iloc[-1]),
        "ema8": _safe_float(ema8.iloc[-1]),
        "ema10": _safe_float(ema10.iloc[-1]),
        "ema14": _safe_float(ema14.iloc[-1]),
        "ema20": _safe_float(ema20.iloc[-1]),
        "ema21": _safe_float(ema21.iloc[-1]),
        "ema55": _safe_float(ema55.iloc[-1]),
        "sma50": _safe_float(sma50.iloc[-1]),
        "sma100": _safe_float(sma100.iloc[-1]),
        "sma200": _safe_float(sma200.iloc[-1]),
        "atr14": atr14,
        "adr_pct": adr20,
        "atr_pct": atr_pct_value,
        "adx14": _safe_float(adx14_series.iloc[-1]),
        "rsi14": _safe_float(rsi14.iloc[-1]),
        "macd_line": _safe_float(macd_line.iloc[-1]),
        "macd_signal": _safe_float(macd_signal.iloc[-1]),
        "macd_hist": _safe_float(macd_hist.iloc[-1]),
        "vol_ma50": _safe_float(vol_ma50_series.iloc[-1]),
        "volume_ma50": _safe_float(vol_ma50_series.iloc[-1]),
        "rvol20": rvol20,
        "bullish_rvol50": bullish_rvol50,
        "donchian_high20": donchian_high20,
        "donchian_low20": donchian_low20,
        "prior_high60": prior_high60,
        "prior_high15": prior_high15,
        "prior_low20": prior_low20,
        "channel_high8": channel_high8,
        "channel_low8": channel_low8,
        "channel_width_pct8": channel_width_pct8,
        "in_channel8": in_channel8,
        "channel_upper_break8": channel_upper_break8,
        "channel_lower_break8": channel_lower_break8,
        "recent_squeeze_ready10": recent_squeeze_ready10,
        "recent_orange_ready10": recent_orange_ready10,
        "bb_lower": lower_band_value,
        "bb_mid": mid_band_value,
        "bb_upper": upper_band_value,
        "bb_percent_b": _safe_float(band_reversion_features.get("bb_percent_b")),
        "bb_z_score": _safe_float(band_reversion_features.get("bb_z_score")),
        "bb_width_pct": bb_width_pct,
        "stock_character": stock_character,
        "ma_system": ma_system,
        "fast_ref": fast_ref,
        "mid_ref": mid_ref,
        "slow_ref": slow_ref,
        "fast_ref_rising": fast_ref_rising,
        "mid_ref_rising": mid_ref_rising,
        "slow_ref_rising": slow_ref_rising,
        "support_trend_rising": support_trend_rising,
        "alignment_state": alignment_state,
        "previous_alignment_state": previous_alignment_state,
        "ema_alignment_state": alignment_state,
        "ema_alignment_transition": ema_alignment_transition,
        "ema_turn_down": ema_turn_down,
        "ma_gap_pct": ma_gap_pct,
        "ma_gap_pass": ma_gap_pass,
        "squeeze_active": squeeze_active,
        "tight_active": tight_active,
        "vcp_active": vcp_active,
        "vcp_setup_active": bool(vcp_features.get("vcp_setup_active")),
        "vcp_pivot_level": _safe_float(vcp_features.get("vcp_pivot_level")),
        "vcp_pivot_breakout": bool(vcp_features.get("vcp_pivot_breakout")),
        "vcp_contraction_count": int(vcp_features.get("vcp_contraction_count") or 0),
        "nh60": nh60,
        "w_active": w_active,
        "w_pending": w_pending,
        "w_confirmed": w_confirmed,
        "w_neckline_level": _safe_float(w_features.get("w_neckline_level")),
        "above_200ma": above_200ma,
        "liquidity_pass": liquidity_pass,
        "risk_heat": risk_heat,
        "setup_active": setup_active,
        "build_up_ready": build_up_ready,
        "aggressive_ready": aggressive_ready,
        "volume_dry": volume_dry,
        "pocket_pivot": bool(pocket_pivot_features.get("pocket_pivot")),
        "pocket_pivot_ready": bool(pocket_pivot_features.get("pocket_pivot")),
        "pocket_pivot_signal": bool(pocket_pivot_features.get("pocket_pivot")),
        "pocket_pivot_down_volume_max": _safe_float(
            pocket_pivot_features.get("pocket_pivot_down_volume_max")
        ),
        "pocket_pivot_score": _safe_float(
            pocket_pivot_features.get("pocket_pivot_score")
        ),
        "volume_quality_reason_codes": list(
            pocket_pivot_features.get("reason_codes", [])
        ),
        "bullish_reversal": bullish_reversal,
        "close_position_pct": close_position_pct,
        "body_strength_pct": body_strength_pct,
        "zone_hold": zone_hold,
        "zone_reclaim": zone_reclaim,
        "bb_zone_hold": bb_zone_hold,
        "bb_zone_reclaim": bb_zone_reclaim,
        "ema_zone_touch": ema_zone_touch,
        "prior_expansion_ready": prior_expansion_ready,
        "recent_extension_pct": recent_extension_pct,
        "recent_pullback_depth_pct": recent_pullback_depth_pct,
        "pullback_reset_ok": pullback_reset_ok,
        "pullback_profile_pass": pullback_profile_pass,
        "pullback_quality": pullback_quality,
        "pullback_context": pullback_context,
        "pullback_reversal_confirmed": pullback_reversal_confirmed,
        "near_high_ready": near_high_ready,
        "momentum_candle_strong": momentum_candle_strong,
        "momentum_ready": momentum_ready,
        "breakout_anchor": breakout_anchor,
        "breakout_anchor_clear": breakout_anchor_clear,
        "breakout_band_clear": breakout_band_clear,
        "breakout_close_strong": breakout_close_strong,
        "breakout_body_strong": breakout_body_strong,
        "breakout_energy": breakout_energy,
        "breakout_ready": breakout_ready,
        "breakout_fakeout_risk": breakout_fakeout_risk,
        "ug_pbb_ready": ug_pbb_ready,
        "ug_pbb_score": _safe_float(band_reversion_features.get("pbb_score")),
        "ug_mr_long_ready": ug_mr_long_ready,
        "ug_mr_long_score": _safe_float(
            band_reversion_features.get("mr_long_score")
        ),
        "ug_breakdown_risk": ug_breakdown_risk,
        "ug_pbs_ready": ug_pbs_ready,
        "ug_pbs_score": _safe_float(band_reversion_features.get("pbs_score")),
        "ug_mr_short_ready": ug_mr_short_ready,
        "ug_mr_short_score": _safe_float(
            band_reversion_features.get("mr_short_score")
        ),
        "band_reversion_reason_codes": list(
            band_reversion_features.get("reason_codes", [])
        ),
        "recent_low10": recent_low10,
        "zone_width_pct": zone_width_pct,
        "trend_support_anchor": trend_support_anchor,
        "trend_zone_low": trend_zone_low,
        "trend_zone_high": trend_zone_high,
        "bb_support_anchor": bb_support_anchor,
        "bb_zone_low": bb_zone_low,
        "bb_zone_high": bb_zone_high,
        "market_condition_state": market_condition_state,
        "market_condition_reason": market_condition_reason,
        "market_alignment_score": _safe_float(source_entry.get("market_alignment_score")),
        "breadth_support_score": _safe_float(source_entry.get("breadth_support_score")),
        "rotation_support_score": _safe_float(source_entry.get("rotation_support_score")),
        "leader_health_score": _safe_float(source_entry.get("leader_health_score")),
        "regime_state": _safe_text(source_entry.get("regime_state")).lower(),
        "top_state": _safe_text(source_entry.get("top_state")).lower(),
        "market_state": _safe_text(source_entry.get("market_state")).lower(),
        "breadth_state": _safe_text(source_entry.get("breadth_state")).lower(),
        "concentration_state": _safe_text(source_entry.get("concentration_state")).lower(),
        "leadership_state": _safe_text(source_entry.get("leadership_state")).lower(),
        "sector": _safe_text(source_entry.get("sector") or metadata.get("sector")),
        "industry": _safe_text(
            source_entry.get("industry") or metadata.get("industry")
        ),
        "group_name": _safe_text(
            source_entry.get("group_name")
            or source_entry.get("industry")
            or metadata.get("industry")
            or metadata.get("sector")
        ),
        "industry_key": _safe_text(
            source_entry.get("industry_key")
            or source_entry.get("group_name")
            or source_entry.get("industry")
            or metadata.get("industry")
        ),
        "group_state": _safe_text(source_entry.get("group_state")).upper(),
        "leader_state": _safe_text(source_entry.get("leader_state")).upper(),
        "breakdown_status": _safe_text(source_entry.get("breakdown_status")).upper(),
        "group_strength_score": _safe_float(source_entry.get("group_strength_score")),
        "leader_score": _safe_float(source_entry.get("leader_score")),
        "breakdown_score": _safe_float(source_entry.get("breakdown_score")),
        "source_tags": source_tags,
        "primary_source_tag": _safe_text(source_entry.get("primary_source_tag")),
        "primary_source_stage": _safe_text(source_entry.get("primary_source_stage")),
        "primary_source_style": _safe_text(source_entry.get("primary_source_style")),
        "source_style_tags": list(source_entry.get("source_style_tags", [])),
        "source_priority_score": float(
            source_entry.get("source_priority_score") or 0.0
        ),
        "trend_source_bonus": float(source_entry.get("trend_source_bonus") or 0.0),
        "ug_source_bonus": float(source_entry.get("ug_source_bonus") or 0.0),
        "source_overlap_bonus": source_overlap_bonus,
        "screen_stage": source_entry.get("screen_stage") or "",
        "feature_row": dict(feature_row),
        "quarterly_eps_growth": _safe_float(financial_row.get("quarterly_eps_growth")),
        "annual_eps_growth": _safe_float(financial_row.get("annual_eps_growth")),
        "eps_growth_acceleration": financial_row.get("eps_growth_acceleration"),
        "eps_3q_accel": financial_row.get("eps_3q_accel"),
        "quarterly_revenue_growth": _safe_float(
            financial_row.get("quarterly_revenue_growth")
        ),
        "annual_revenue_growth": _safe_float(
            financial_row.get("annual_revenue_growth")
        ),
        "revenue_growth_acceleration": financial_row.get("revenue_growth_acceleration"),
        "sales_3q_accel": financial_row.get("sales_3q_accel"),
    }


class MultiScreenerSignalEngine:

    def __init__(
        self,
        *,
        market: str = "us",
        as_of_date: str | None = None,
        standalone: bool = False,
        upcoming_earnings_fetcher: (
            Callable[[str, str | None, int], pd.DataFrame] | None
        ) = None,
        earnings_collector: EarningsDataCollector | None = None,
        runtime_context: RuntimeContext | None = None,
        source_registry_snapshot: Mapping[str, Any] | None = None,
    ) -> None:
        self.market = market_key(market)
        resolved_as_of = (
            _date_to_str(as_of_date)
            or (
                _date_to_str(runtime_context.as_of_date)
                if runtime_context is not None
                else None
            )
        )
        self.as_of_date = _resolve_runtime_as_of_date(self.market, resolved_as_of)
        self.standalone = bool(standalone)
        self.results_dir = get_signal_engine_results_dir(self.market)
        self.screeners_root = get_market_screeners_root(self.market)
        self.runtime_context = runtime_context
        if self.runtime_context is not None:
            self.runtime_context.set_as_of_date(self.as_of_date)
        self.source_registry_snapshot = dict(source_registry_snapshot or {})
        cached_metadata_map = (
            dict(runtime_context.metadata_map)
            if runtime_context is not None and runtime_context.metadata_map
            else {}
        )
        cached_financial_map = (
            dict(runtime_context.financial_map)
            if runtime_context is not None and runtime_context.financial_map
            else {}
        )
        self.metadata_map = cached_metadata_map or _load_metadata_map(self.market)
        if runtime_context is not None and not runtime_context.metadata_map:
            runtime_context.metadata_map = dict(self.metadata_map)
        self.financial_map: dict[str, dict[str, Any]] = cached_financial_map
        self.upcoming_earnings_fetcher = upcoming_earnings_fetcher
        self.earnings_collector = earnings_collector or EarningsDataCollector(
            market=self.market
        )

    def _add_elapsed_timing(self, label: str, started_at: float) -> None:
        if self.runtime_context is not None:
            self.runtime_context.add_timing(label, time.perf_counter() - started_at)

    def _load_local_market_truth_snapshot(self) -> _market_intel_bridge.MarketTruthSnapshot:
        benchmark_symbol = get_primary_benchmark_symbol(self.market)
        benchmark_daily = pd.DataFrame()
        for candidate in get_benchmark_candidates(self.market):
            try:
                frame = load_local_ohlcv_frame(
                    self.market,
                    candidate,
                    as_of=self.as_of_date,
                    price_policy=PricePolicy.SPLIT_ADJUSTED,
                )
            except Exception:
                frame = pd.DataFrame()
            if not frame.empty:
                benchmark_symbol = candidate
                benchmark_daily = frame
                break
        return _market_intel_bridge.build_local_market_truth_snapshot(
            market=self.market,
            as_of_date=self.as_of_date,
            benchmark_symbol=benchmark_symbol,
            benchmark_daily=benchmark_daily,
        )

    def _load_source_registry(self) -> dict[str, dict[str, Any]]:
        active_snapshot: Mapping[str, Any] | None = None
        snapshot_path = get_market_source_registry_snapshot_path(self.market)
        if self.source_registry_snapshot:
            active_snapshot = self.source_registry_snapshot
        elif self.runtime_context is not None and self.runtime_context.source_registry_snapshot:
            active_snapshot = self.runtime_context.source_registry_snapshot
        else:
            active_snapshot = _source_registry.read_source_registry_snapshot(
                snapshot_path,
                market=self.market,
                as_of_date=self.as_of_date,
            )
            if active_snapshot is None and self.runtime_context is not None:
                self.runtime_context.record_cache_miss("source_registry_snapshot")
        if active_snapshot is not None:
            if not _source_registry.snapshot_is_compatible(
                active_snapshot,
                market=self.market,
                as_of_date=self.as_of_date,
            ):
                active_snapshot = None
                if self.runtime_context is not None:
                    self.runtime_context.source_registry_snapshot = None
                    self.runtime_context.record_cache_miss("source_registry_snapshot")
            elif self.runtime_context is not None:
                self.runtime_context.source_registry_snapshot = dict(active_snapshot)
                self.runtime_context.record_cache_hit("source_registry_snapshot")
        if active_snapshot is None and results_root_override_active():
            existing_paths = _source_registry.existing_source_artifact_paths(
                screeners_root=self.screeners_root,
                source_specs=_SOURCE_SPECS,
            )
            if not existing_paths:
                _raise_isolated_results_prerequisite_error(
                    phase="signals",
                    snapshot_path=snapshot_path,
                    expected_paths=_source_registry.expected_source_artifact_paths(
                        screeners_root=self.screeners_root,
                        source_specs=_SOURCE_SPECS,
                    ),
                )
        if active_snapshot is not None:
            snapshot_as_of = _date_to_str(active_snapshot.get("as_of_date"))
            if snapshot_as_of:
                self.as_of_date = snapshot_as_of
                if self.runtime_context is not None:
                    self.runtime_context.set_as_of_date(snapshot_as_of)
        registry = _source_registry.load_source_registry(
            screeners_root=self.screeners_root,
            market=self.market,
            source_specs=_SOURCE_SPECS,
            stage_priority=_stage_priority,
            source_tag_priority=_source_tag_priority,
            sorted_source_tags=_sorted_source_tags,
            source_style_tags=_source_style_tags,
            primary_source_style=_primary_source_style,
            source_priority_score=_source_priority_score,
            source_engine_bonus=_source_engine_bonus,
            safe_text=_safe_text,
            snapshot=active_snapshot,
            as_of_date=self.as_of_date,
        )
        if self.standalone:
            market_truth_snapshot = self._load_local_market_truth_snapshot()
            self.market_truth_snapshot = market_truth_snapshot
            return _apply_shared_market_truth_to_registry(
                registry,
                market_truth_snapshot,
                market_truth_source="local_standalone",
                core_overlay_applied=False,
            )

        market_truth_snapshot = _market_intel_bridge.load_market_truth_snapshot(
            self.market,
            as_of_date=self.as_of_date,
        )
        self.market_truth_snapshot = market_truth_snapshot
        market_intel_entries = _market_intel_bridge.load_leader_core_registry_entries(
            self.market,
            as_of_date=self.as_of_date,
        )
        merged = _source_registry.merge_source_registry_entries(
            registry=registry,
            entries=market_intel_entries.values(),
            stage_priority=_stage_priority,
            source_tag_priority=_source_tag_priority,
            sorted_source_tags=_sorted_source_tags,
            source_style_tags=_source_style_tags,
            primary_source_style=_primary_source_style,
            source_priority_score=_source_priority_score,
            source_engine_bonus=_source_engine_bonus,
            safe_text=_safe_text,
        )
        return _apply_shared_market_truth_to_registry(
            merged,
            market_truth_snapshot,
            market_truth_source="market_intel_compat",
            core_overlay_applied=True,
        )

    def _load_feature_map(
        self, frames: Mapping[str, pd.DataFrame]
    ) -> dict[str, dict[str, Any]]:
        return _signal_metrics.load_feature_map(
            frames,
            analyzer=_ANALYZER,
            market=self.market,
            metadata_map=self.metadata_map,
            frame_keyed_records_fn=frame_keyed_records,
            max_workers=_resolved_parallel_workers(len(frames)),
        )

    def _load_active_cycles(self) -> dict[tuple[str, str, str], dict[str, Any]]:
        return _cycle_store.load_active_cycles(
            self.results_dir,
            safe_csv_rows=_safe_csv_rows,
            safe_text=_safe_text,
            hydrate_loaded_cycle=self._hydrate_loaded_cycle,
        )

    def _load_signal_history(self) -> list[dict[str, Any]]:
        return _cycle_store.load_signal_history(
            self.results_dir,
            safe_csv_rows=_safe_csv_rows,
            safe_text=_safe_text,
            signal_history_prefix=_SIGNAL_HISTORY_PREFIX,
        )

    def _load_state_history(self) -> list[dict[str, Any]]:
        return _cycle_store.load_state_history(
            self.results_dir,
            safe_csv_rows=_safe_csv_rows,
            safe_text=_safe_text,
            state_history_prefix=_STATE_HISTORY_PREFIX,
        )

    def _persist_signal_history(
        self,
        existing_rows: Iterable[Mapping[str, Any]],
        new_event_rows: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        return _cycle_store.persist_signal_history(
            self.results_dir,
            existing_rows=existing_rows,
            new_event_rows=new_event_rows,
            history_merge_rows=_history_merge_rows,
            safe_text=_safe_text,
            write_records=_write_records,
            signal_history_prefix=_SIGNAL_HISTORY_PREFIX,
        )

    def _persist_state_history(
        self,
        existing_rows: Iterable[Mapping[str, Any]],
        new_state_rows: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        return _cycle_store.persist_state_history(
            self.results_dir,
            existing_rows=existing_rows,
            new_state_rows=new_state_rows,
            history_merge_rows=_history_merge_rows,
            safe_text=_safe_text,
            write_records=_write_records,
            state_history_prefix=_STATE_HISTORY_PREFIX,
        )

    def _load_peg_event_history(self) -> list[dict[str, Any]]:
        return _cycle_store.load_peg_event_history(
            self.results_dir,
            safe_csv_rows=_safe_csv_rows,
            peg_event_history_prefix=_PEG_EVENT_HISTORY_PREFIX,
        )

    def _latest_peg_event_map(
        self, rows: Iterable[Mapping[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        return _cycle_store.latest_peg_event_map(
            rows,
            safe_text=_safe_text,
            date_to_str=_date_to_str,
        )

    def _persist_peg_event_history(
        self,
        existing_rows: Iterable[Mapping[str, Any]],
        peg_context_map: Mapping[str, Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        return _cycle_store.persist_peg_event_history(
            self.results_dir,
            market=self.market,
            existing_rows=existing_rows,
            peg_context_map=peg_context_map,
            history_merge_rows=_history_merge_rows,
            write_records=_write_records,
            peg_event_history_prefix=_PEG_EVENT_HISTORY_PREFIX,
        )

    def _load_or_run_peg_screen(
        self,
    ) -> tuple[
        dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]
    ]:

        screener = PEGImminentScreener(
            market=self.market,
            as_of_date=self.as_of_date,
            upcoming_earnings_fetcher=self.upcoming_earnings_fetcher,
            metadata_map=self.metadata_map,
            financial_map=self.financial_map,
        )

        results = screener.run()

        raw_map = {
            str(row.get("symbol") or "").upper(): row
            for row in results.get("peg_imminent_raw", [])
            if str(row.get("symbol") or "").strip()
        }

        ready_map = {
            str(row.get("symbol") or "").upper(): row
            for row in results.get("peg_ready", [])
            if str(row.get("symbol") or "").strip()
        }

        return raw_map, ready_map, dict(screener.financial_map)

    def _load_all_symbol_universe(self) -> list[str]:
        data_dir = get_market_data_dir(self.market)
        if not os.path.isdir(data_dir):
            return []
        symbols = sorted(
            {
                os.path.splitext(name)[0].upper()
                for name in os.listdir(data_dir)
                if name.endswith(".csv")
                and not is_index_symbol(self.market, os.path.splitext(name)[0].upper())
            }
        )
        return limit_runtime_symbols(symbols)

    def _build_buy_candidate_bundle(
        self,
        *,
        symbol: str,
        metrics: Mapping[str, Any],
        source_entry: Mapping[str, Any],
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
        peg_ready_map: Mapping[str, Any],
        raw_peg_map: Mapping[str, Mapping[str, Any]],
        peg_event_history_map: Mapping[str, Mapping[str, Any]],
        signal_history: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        peg_context = self._current_peg_context(
            symbol, metrics, raw_peg_map, peg_event_history_map
        )
        return {
            "peg_context": peg_context,
            "trend_rows": self._trend_buy_events(
                symbol=symbol,
                metrics=metrics,
                source_entry=source_entry,
                active_cycles=active_cycles,
                peg_ready_map=peg_ready_map,
                peg_context=peg_context,
            ),
            "ug_rows": self._ug_buy_events(
                symbol=symbol,
                metrics=metrics,
                source_entry=source_entry,
                active_cycles=active_cycles,
                signal_history=signal_history,
            ),
        }

    def _load_frames(self, symbols: Iterable[str]) -> dict[str, pd.DataFrame]:

        frames: dict[str, pd.DataFrame] = {}

        normalized_symbols = sorted(
            {str(item).upper() for item in symbols if str(item).strip()}
        )
        frame_map = load_local_ohlcv_frames_ordered(
            self.market,
            normalized_symbols,
            as_of=self.as_of_date,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
            runtime_context=self.runtime_context,
            max_workers=_resolved_parallel_workers(len(normalized_symbols)),
            load_frame_fn=load_local_ohlcv_frame,
        )
        for index, symbol in enumerate(normalized_symbols, start=1):
            frame = frame_map.get(symbol, pd.DataFrame())
            if not frame.empty:
                frames[symbol] = frame
            if self.runtime_context is not None:
                self.runtime_context.update_runtime_state(
                    current_stage="Multi-screener signal engine",
                    current_symbol=symbol,
                    current_chunk=f"frame_load:{index}/{len(normalized_symbols)}",
                    status="running",
                )

        return frames

    def _trend_conviction(self, metrics: Mapping[str, Any]) -> str:
        score = 45.0
        score += min(float(metrics.get("source_overlap_bonus") or 0.0), 10.0)
        score += min(float(metrics.get("source_priority_score") or 0.0) * 0.35, 6.0)
        score += min(float(metrics.get("trend_source_bonus") or 0.0), 6.0)
        if metrics.get("above_200ma"):
            score += 8.0
        if metrics.get("alignment_state") == "BULLISH":
            score += 15.0
        if metrics.get("ma_gap_pass"):
            score += 8.0
        if (_safe_float(metrics.get("adx14")) or 0.0) >= 20.0:
            score += 8.0
        if metrics.get("setup_active"):
            score += 7.0
        if metrics.get("vcp_active"):
            score += 6.0
        if metrics.get("prior_expansion_ready"):
            score += 5.0
        if metrics.get("build_up_ready"):
            score += 8.0
        market_state = _safe_text(metrics.get("market_condition_state")).upper()
        hard_fail = bool(
            metrics.get("ema_turn_down")
            or _normalize_alignment_state(metrics.get("alignment_state")) == "BEARISH"
            or market_state in {"RISK_OFF", "WEAK", "BEARISH", "RED"}
            or not metrics.get("liquidity_pass")
        )
        return _grade_from_score(score, hard_fail)

    def _ug_growth_metric_points(
        self,
        value: Any,
        *,
        strong: float,
        good: float,
        positive: float,
        mild_negative: float,
        max_points: float,
        negative_penalty: float,
    ) -> float | None:
        numeric = _safe_float(value)
        if numeric is None:
            return None
        if numeric >= strong:
            return max_points
        if numeric >= good:
            return max_points * 0.75
        if numeric > positive:
            return max_points * 0.5
        if numeric >= mild_negative:
            return max_points * 0.1
        return -abs(negative_penalty)

    def _ug_acceleration_state(self, value: Any) -> int | None:
        if value in {None, ""}:
            return None
        if isinstance(value, (bool, np.bool_)):
            return 1 if bool(value) else -1
        text = _safe_text(value).lower()
        if text in {"true", "yes", "y"}:
            return 1
        if text in {"false", "no", "n"}:
            return -1
        numeric = _safe_float(value)
        if numeric is None:
            return None
        if numeric > 0:
            return 1
        if numeric < 0:
            return -1
        return 0

    def _ug_axis_profile(
        self,
        *,
        quarterly_growth: Any,
        annual_growth: Any,
        growth_acceleration: Any,
        three_q_accel: Any,
        positive_reason: str,
        accel_reason: str,
    ) -> dict[str, Any]:
        present = 0
        score = 0.0
        reasons: list[str] = []

        quarterly_points = self._ug_growth_metric_points(
            quarterly_growth,
            strong=25.0,
            good=10.0,
            positive=0.0,
            mild_negative=-10.0,
            max_points=20.0,
            negative_penalty=8.0,
        )
        if quarterly_points is not None:
            present += 1
            score += quarterly_points

        annual_points = self._ug_growth_metric_points(
            annual_growth,
            strong=20.0,
            good=10.0,
            positive=0.0,
            mild_negative=-5.0,
            max_points=10.0,
            negative_penalty=4.0,
        )
        if annual_points is not None:
            present += 1
            score += annual_points

        accel_state = self._ug_acceleration_state(growth_acceleration)
        if accel_state is not None:
            present += 1
            score += 10.0 if accel_state > 0 else -5.0 if accel_state < 0 else 0.0

        seq_accel_state = self._ug_acceleration_state(three_q_accel)
        if seq_accel_state is not None:
            present += 1
            score += (
                10.0 if seq_accel_state > 0 else -5.0 if seq_accel_state < 0 else 0.0
            )

        axis_score = max(0.0, min(score, 50.0))
        if present == 0:
            health = "UNKNOWN"
        elif axis_score >= 30.0:
            health = "GREEN"
        elif axis_score >= 15.0:
            health = "ORANGE"
        else:
            health = "RED"

        if ((_safe_float(quarterly_growth) or 0.0) > 0.0) or (
            (_safe_float(annual_growth) or 0.0) > 0.0
        ):
            reasons.append(positive_reason)
        if (accel_state or 0) > 0 or (seq_accel_state or 0) > 0:
            reasons.append(accel_reason)

        return {
            "score": round(axis_score, 2),
            "health": health,
            "present_count": present,
            "reason_codes": reasons,
        }

    def _ug_growth_profile(self, metrics: Mapping[str, Any]) -> dict[str, Any]:
        eps_profile = self._ug_axis_profile(
            quarterly_growth=metrics.get("quarterly_eps_growth"),
            annual_growth=metrics.get("annual_eps_growth"),
            growth_acceleration=metrics.get("eps_growth_acceleration"),
            three_q_accel=metrics.get("eps_3q_accel"),
            positive_reason="UG_GROWTH_EPS_POSITIVE",
            accel_reason="UG_GROWTH_EPS_ACCEL",
        )
        sales_profile = self._ug_axis_profile(
            quarterly_growth=metrics.get("quarterly_revenue_growth"),
            annual_growth=metrics.get("annual_revenue_growth"),
            growth_acceleration=metrics.get("revenue_growth_acceleration"),
            three_q_accel=metrics.get("sales_3q_accel"),
            positive_reason="UG_GROWTH_SALES_POSITIVE",
            accel_reason="UG_GROWTH_SALES_ACCEL",
        )
        present_count = int(eps_profile["present_count"]) + int(
            sales_profile["present_count"]
        )
        if present_count == 0:
            growth_data_status = "MISSING"
            growth_score = None
            growth_health = "UNKNOWN"
        else:
            growth_data_status = "COMPLETE" if present_count >= 6 else "PARTIAL"
            growth_score = round(
                float(eps_profile["score"]) + float(sales_profile["score"]), 2
            )
            if growth_score >= 60.0:
                growth_health = "GREEN"
            elif growth_score >= 30.0:
                growth_health = "ORANGE"
            else:
                growth_health = "RED"

        reason_codes = [*eps_profile["reason_codes"], *sales_profile["reason_codes"]]
        if growth_data_status != "COMPLETE":
            reason_codes.append("UG_GROWTH_DATA_MISSING")

        return {
            "growth_score": growth_score,
            "growth_health": growth_health,
            "eps_health": eps_profile["health"],
            "sales_health": sales_profile["health"],
            "growth_data_status": growth_data_status,
            "reason_codes": list(dict.fromkeys(reason_codes)),
        }

    def _ug_gp_profile(self, metrics: Mapping[str, Any]) -> dict[str, Any]:
        score = 0.0
        reasons: list[str] = []
        volume_rvol = _safe_float(metrics.get("bullish_rvol50"))
        if volume_rvol is None:
            volume_rvol = _safe_float(metrics.get("rvol20")) or 0.0
        w_score_active = (
            bool(metrics.get("w_confirmed"))
            if "w_confirmed" in metrics
            else bool(metrics.get("w_active"))
        )
        if metrics.get("nh60"):
            score += 15.0
            reasons.append("GP_NH60")
        if volume_rvol >= 2.0:
            score += 30.0
            reasons.append("GP_VOL2X")
        if w_score_active:
            score += 25.0
            reasons.append("GP_W")
        active_components = sum(
            1
            for active in (
                metrics.get("nh60"),
                volume_rvol >= 2.0,
                w_score_active,
            )
            if active
        )
        if active_components >= 2:
            health = "GREEN"
        elif active_components == 1:
            health = "ORANGE"
        else:
            health = "RED"
        bullish = bool(
            metrics.get("alignment_state") == "BULLISH"
            and metrics.get("support_trend_rising")
            and not metrics.get("ema_turn_down")
        )
        return {
            "score": round(score, 2),
            "health": health,
            "bullish": bullish,
            "reason_codes": reasons,
        }

    def _ug_sigma_profile(self, metrics: Mapping[str, Any]) -> dict[str, Any]:
        score = 0.0
        reasons: list[str] = []
        breakout_signal = bool(
            metrics.get("breakout_ready") or metrics.get("vcp_pivot_breakout")
        )
        pbb_signal = bool(metrics.get("ug_pbb_ready"))
        mr_long_signal = bool(metrics.get("ug_mr_long_ready"))
        pbs_risk = bool(metrics.get("ug_pbs_ready"))
        mr_short_signal = bool(metrics.get("ug_mr_short_ready"))
        breakdown_risk = bool(metrics.get("ug_breakdown_risk"))
        squeeze_ready = bool(
            metrics.get("recent_orange_ready10")
            and metrics.get("recent_squeeze_ready10")
        )

        if breakout_signal:
            score += 10.0
            reasons.append("SIGMA_BO")
        if pbb_signal:
            score += 15.0
            reasons.append("SIGMA_PBB")
        if mr_long_signal:
            score += 5.0
            reasons.append("SIGMA_MR_LONG")
        if squeeze_ready:
            reasons.append("SIGMA_SQUEEZE_READY")
        if mr_short_signal:
            reasons.append("SIGMA_MR_SHORT")
        if pbs_risk:
            reasons.append("SIGMA_PBS")
        if breakdown_risk:
            score -= 20.0
            reasons.extend(["SIGMA_BREAKDOWN_RISK", "SIGMA_BREAKDOWN_PENALTY"])

        if pbs_risk or breakdown_risk:
            health = "RED"
        elif breakout_signal or pbb_signal or mr_long_signal:
            health = "GREEN"
        else:
            health = "ORANGE"
        return {
            "score": round(score, 2),
            "health": health,
            "breakout_signal": breakout_signal,
            "pbb_signal": pbb_signal,
            "mr_long_signal": mr_long_signal,
            "mr_short_signal": mr_short_signal,
            "pbs_risk": pbs_risk,
            "breakdown_risk": breakdown_risk,
            "squeeze_ready": squeeze_ready,
            "entry_signal": bool(breakout_signal or pbb_signal),
            "reason_codes": reasons,
        }

    def _ug_validation_score(self, metrics: Mapping[str, Any]) -> float:
        gp_profile = self._ug_gp_profile(metrics)
        sigma_profile = self._ug_sigma_profile(metrics)
        score = float(gp_profile["score"]) + float(sigma_profile["score"])
        return round(max(0.0, min(score, 100.0)), 2)

    def _ug_validation_reason_codes(self, metrics: Mapping[str, Any]) -> list[str]:
        gp_profile = self._ug_gp_profile(metrics)
        sigma_profile = self._ug_sigma_profile(metrics)
        return list(
            dict.fromkeys([*gp_profile["reason_codes"], *sigma_profile["reason_codes"]])
        )

    def _ug_dashboard_profile(self, metrics: Mapping[str, Any]) -> dict[str, Any]:
        gp_profile = self._ug_gp_profile(metrics)
        sigma_profile = self._ug_sigma_profile(metrics)
        validation_score = self._ug_validation_score(metrics)
        technical_light = self._ug_traffic_light(
            metrics,
            validation_score,
            gp_profile=gp_profile,
            sigma_profile=sigma_profile,
        )
        growth_profile = self._ug_growth_profile(metrics)
        growth_score = growth_profile.get("growth_score")
        effective_growth_score = 50.0 if growth_score is None else float(growth_score)
        dashboard_score = round(
            (validation_score * 0.70)
            + (effective_growth_score * 0.30),
            2,
        )
        if dashboard_score >= 60.0:
            dashboard_light = "GREEN"
        elif dashboard_score >= 30.0:
            dashboard_light = "ORANGE"
        else:
            dashboard_light = "RED"
        growth_health = _safe_text(growth_profile.get("growth_health")).upper()
        if technical_light == "GREEN" and growth_health == "RED":
            dashboard_position_bias = "REDUCED"
        elif technical_light == "GREEN" and dashboard_light == "GREEN":
            dashboard_position_bias = "FULL"
        elif technical_light == "GREEN":
            dashboard_position_bias = "NORMAL"
        elif technical_light == "ORANGE":
            dashboard_position_bias = "WATCH"
        else:
            dashboard_position_bias = "AVOID"
        dashboard_reason_codes = list(growth_profile.get("reason_codes", []))
        if dashboard_score >= 60.0:
            dashboard_reason_codes.append("UG_DASHBOARD_SCORE_60PLUS")
        elif dashboard_score < 30.0:
            dashboard_reason_codes.append("UG_DASHBOARD_SCORE_UNDER30")
        return {
            "validation_score": round(validation_score, 2),
            "gp_score": gp_profile["score"],
            "gp_health": gp_profile["health"],
            "sigma_score": sigma_profile["score"],
            "sigma_health": sigma_profile["health"],
            "technical_light": technical_light,
            "dashboard_score": dashboard_score,
            "dashboard_light": dashboard_light,
            "dashboard_position_bias": dashboard_position_bias,
            "growth_score": growth_score,
            "growth_health": growth_health,
            "eps_health": growth_profile.get("eps_health", ""),
            "sales_health": growth_profile.get("sales_health", ""),
            "growth_data_status": growth_profile.get("growth_data_status", ""),
            "reason_codes": list(
                dict.fromkeys(
                    [
                        *gp_profile["reason_codes"],
                        *sigma_profile["reason_codes"],
                        *dashboard_reason_codes,
                    ]
                )
            ),
        }

    def _ug_traffic_light(
        self,
        metrics: Mapping[str, Any],
        validation_score: float,
        *,
        gp_profile: Mapping[str, Any] | None = None,
        sigma_profile: Mapping[str, Any] | None = None,
    ) -> str:
        _ = gp_profile
        sigma_profile = sigma_profile or self._ug_sigma_profile(metrics)
        if bool(metrics.get("ug_breakdown_risk")) or bool(
            sigma_profile.get("breakdown_risk")
        ):
            return "RED"
        if validation_score >= 60.0:
            return "GREEN"
        if validation_score >= 30.0:
            return "ORANGE"
        return "RED"

    def _ug_conviction(
        self,
        metrics: Mapping[str, Any],
        state_code: str,
        dashboard_profile: Mapping[str, Any] | None = None,
    ) -> str:
        profile = dashboard_profile or self._ug_dashboard_profile(metrics)
        validation_score = _safe_float(profile.get("validation_score")) or 0.0
        dashboard_score = (
            _safe_float(profile.get("dashboard_score")) or validation_score
        )
        score = 26.0 + (validation_score * 0.35) + (dashboard_score * 0.15)
        score += min(float(metrics.get("source_overlap_bonus") or 0.0), 10.0)
        score += min(float(metrics.get("source_priority_score") or 0.0) * 0.30, 5.0)
        score += min(float(metrics.get("ug_source_bonus") or 0.0), 6.0)
        if metrics.get("above_200ma"):
            score += 6.0
        if state_code == "UG_STATE_GREEN":
            score += 12.0
        elif state_code == "UG_STATE_ORANGE":
            score += 5.0
        market_state = _safe_text(metrics.get("market_condition_state")).upper()
        hard_fail = bool(
            metrics.get("ema_turn_down")
            or _normalize_alignment_state(metrics.get("alignment_state")) == "BEARISH"
            or market_state in {"RISK_OFF", "WEAK", "BEARISH", "RED"}
            or not metrics.get("liquidity_pass")
        )
        grade = _grade_from_score(score, hard_fail)
        if hard_fail:
            return grade
        growth_health = _safe_text(profile.get("growth_health")).upper()
        if growth_health == "GREEN":
            return _shift_grade(grade, 1)
        if growth_health == "RED":
            return _shift_grade(grade, -1)
        return grade

    def _family_source_profile(
        self,
        symbol: str,
        family: str,
        source_entry: Mapping[str, Any],
        peg_ready_map: Mapping[str, Any],
    ) -> dict[str, Any]:
        source_tags = _sorted_source_tags(source_entry.get("source_tags", []))
        source_styles = list(
            dict.fromkeys(_to_list(source_entry.get("source_style_tags")))
        ) or _source_style_tags(source_tags)
        peg_capable = family == "TF_PEG" and (
            symbol in peg_ready_map
            or "PEG_READY" in source_tags
        )
        if peg_capable and "PEG" not in source_styles:
            source_styles = ["PEG", *source_styles]
        fit_score = _family_source_fit_score(family, source_styles)
        if peg_capable:
            fit_score = max(fit_score, 95.0)
        fit_label = _family_source_fit_label(fit_score)
        primary_style = source_styles[0] if source_styles else ""
        source_disposition = _source_disposition(
            source_entry,
            symbol=symbol,
            peg_ready_map=peg_ready_map,
        )
        buy_capable = source_disposition == "buy_eligible" or peg_capable
        watch_capable = source_disposition in {"buy_eligible", "watch_only"} or peg_capable
        return {
            "buy_allowed": buy_capable,
            "watch_allowed": watch_capable,
            "fit_score": fit_score,
            "fit_label": fit_label,
            "primary_style": primary_style,
            "reason_codes": [
                *(
                    [
                        f"SOURCE_PRIMARY_{_safe_text(source_entry.get('primary_source_tag'))}"
                    ]
                    if _safe_text(source_entry.get("primary_source_tag"))
                    else []
                ),
                *([f"SOURCE_STYLE_{primary_style}"] if primary_style else []),
                f"SOURCE_FIT_{fit_label}",
            ],
        }

    def _family_buy_allowed(
        self,
        symbol: str,
        family: str,
        source_entry: Mapping[str, Any],
        peg_ready_map: Mapping[str, Any],
    ) -> bool:
        profile = self._family_source_profile(
            symbol, family, source_entry, peg_ready_map
        )
        return bool(profile.get("buy_allowed") or profile.get("watch_allowed"))

    def _base_quality_flags(self, metrics: Mapping[str, Any]) -> list[str]:
        flags: list[str] = []
        if not metrics.get("above_200ma"):
            flags.append("BELOW_200MA")
        if not metrics.get("liquidity_pass"):

            flags.append("LIQUIDITY_FAIL")

        if metrics.get("ema_turn_down"):

            flags.append("EMA_TURN_DOWN")

        if not metrics.get("support_trend_rising"):

            flags.append("SUPPORT_SLOPE_WEAK")

        if not metrics.get("prior_expansion_ready"):

            flags.append("NO_PRIOR_EXPANSION")

        if metrics.get("risk_heat"):
            flags.append("ATR_X7_HEAT")
        if metrics.get("breakout_fakeout_risk"):
            flags.append("BREAKOUT_FAKEOUT_RISK")
        if metrics.get("in_channel8"):
            flags.append("CHANNEL_8_ACTIVE")
        return flags

    def _hydrate_loaded_cycle(self, row: Mapping[str, Any]) -> dict[str, Any]:
        hydrated = dict(row)
        engine = _safe_text(hydrated.get("engine"))
        if engine == "UG":
            family = _safe_text(hydrated.get("family"))
            hydrated = _blank_ug_tf_fields(hydrated, cycle=True)
            hydrated["trim_count"] = max(_safe_int(hydrated.get("trim_count")) or 0, 0)
            hydrated["partial_exit_active"] = _safe_bool(
                hydrated.get("partial_exit_active")
            )
            hydrated["last_trim_date"] = _safe_text(hydrated.get("last_trim_date"))
            hydrated["reference_exit_signal"] = _normalized_reference_exit_signal(
                engine=engine,
                family=family,
                reference_exit_signal=hydrated.get("reference_exit_signal"),
            )
            hydrated["base_position_units"] = (
                _safe_float(hydrated.get("base_position_units")) or 1.0
            )
            hydrated["current_position_units"] = (
                _safe_float(hydrated.get("current_position_units")) or 1.0
            )
            return hydrated

        entry_price = _safe_float(hydrated.get("entry_price"))
        add_on_count = max(_safe_int(hydrated.get("add_on_count")) or 0, 0)
        base_position_units = _safe_float(hydrated.get("base_position_units")) or 1.0
        current_position_units = _safe_float(hydrated.get("current_position_units"))
        if current_position_units is None:
            current_position_units = self._trend_total_position_units(
                add_on_count, base_position_units=base_position_units
            )
        blended_entry_price = _safe_float(hydrated.get("blended_entry_price"))
        if blended_entry_price is None:
            blended_entry_price = entry_price
        break_even_level = _safe_float(hydrated.get("break_even_level"))
        if break_even_level is None:
            break_even_level = blended_entry_price or entry_price
        trailing_level = _safe_float(hydrated.get("trailing_level"))
        protected_stop_level = _safe_float(hydrated.get("protected_stop_level"))
        if protected_stop_level is None:
            protected_stop_level = _pick_latest(
                max(
                    value
                    for value in [trailing_level, break_even_level, blended_entry_price]
                    if value is not None
                )
                if trailing_level is not None
                or break_even_level is not None
                or blended_entry_price is not None
                else None
            )
        hydrated["base_position_units"] = base_position_units
        hydrated["current_position_units"] = current_position_units
        hydrated["blended_entry_price"] = blended_entry_price
        hydrated["break_even_level"] = break_even_level
        hydrated["protected_stop_level"] = protected_stop_level
        hydrated["last_trailing_confirmed_level"] = (
            _safe_float(hydrated.get("last_trailing_confirmed_level")) or trailing_level
        )
        hydrated["last_protected_stop_level"] = (
            _safe_float(hydrated.get("last_protected_stop_level"))
            or protected_stop_level
        )
        hydrated["last_pyramid_reference_level"] = (
            _safe_float(hydrated.get("last_pyramid_reference_level"))
            or _safe_float(hydrated.get("last_protected_stop_level"))
            or protected_stop_level
            or trailing_level
        )
        return hydrated

    def _trend_addon_slot(self, cycle: Mapping[str, Any]) -> int | None:
        add_on_count = max(_safe_int(cycle.get("add_on_count")) or 0, 0)
        max_add_ons = max(_safe_int(cycle.get("max_add_ons")) or 2, 0)
        if add_on_count >= max_add_ons:
            return None
        return add_on_count + 1

    def _trend_addon_tranche_pct(self, slot: int | None) -> float | None:
        if slot == 1:
            return 0.50
        if slot == 2:
            return 0.30
        return None

    def _trend_total_position_units(
        self, add_on_count: int, *, base_position_units: float = 1.0
    ) -> float:
        total = float(base_position_units)
        for slot in range(1, max(add_on_count, 0) + 1):
            tranche_pct = self._trend_addon_tranche_pct(slot)
            if tranche_pct is not None:
                total += float(tranche_pct)
        return total

    def _trend_weighted_entry(
        self,
        *,
        current_units: float | None,
        current_entry: float | None,
        add_on_units: float | None,
        add_on_price: float | None,
    ) -> float | None:
        if current_entry is None:
            return add_on_price
        if current_units is None or add_on_units is None or add_on_price is None:
            return current_entry
        total_units = current_units + add_on_units
        if total_units <= 0:
            return current_entry
        return (
            (current_units * current_entry) + (add_on_units * add_on_price)
        ) / total_units

    def _trend_protected_stop_level(self, cycle: Mapping[str, Any]) -> float | None:
        trailing_level = _safe_float(cycle.get("trailing_level"))
        break_even_level = _safe_float(cycle.get("break_even_level"))
        blended_entry_price = _safe_float(cycle.get("blended_entry_price"))
        add_on_count = max(_safe_int(cycle.get("add_on_count")) or 0, 0)
        if _safe_bool(cycle.get("risk_free_armed")) or add_on_count > 0:
            return _pick_latest(
                max(
                    value
                    for value in [trailing_level, break_even_level, blended_entry_price]
                    if value is not None
                )
                if trailing_level is not None
                or break_even_level is not None
                or blended_entry_price is not None
                else None
            )
        return trailing_level

    def _trend_addon_context(
        self,
        *,
        family: str,
        metrics: Mapping[str, Any],
        cycle: Mapping[str, Any],
        peg_context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        peg_context = peg_context or {}
        close = _safe_float(metrics.get("close"))
        low = _safe_float(metrics.get("low"))
        entry_price = _safe_float(cycle.get("entry_price"))
        base_position_units = _safe_float(cycle.get("base_position_units")) or 1.0
        current_position_units = _safe_float(cycle.get("current_position_units"))
        if current_position_units is None:
            current_position_units = self._trend_total_position_units(
                max(_safe_int(cycle.get("add_on_count")) or 0, 0),
                base_position_units=base_position_units,
            )
        blended_entry_price = (
            _safe_float(cycle.get("blended_entry_price")) or entry_price
        )
        break_even_level = _safe_float(cycle.get("break_even_level"))
        if break_even_level is None:
            break_even_level = blended_entry_price or entry_price
        next_slot = self._trend_addon_slot(cycle)
        tranche_pct = self._trend_addon_tranche_pct(next_slot)
        projected_position_units = current_position_units
        projected_blended_entry_price = blended_entry_price
        if close is not None and tranche_pct is not None:
            projected_position_units = (current_position_units or 0.0) + tranche_pct
            projected_blended_entry_price = self._trend_weighted_entry(
                current_units=current_position_units,
                current_entry=blended_entry_price,
                add_on_units=tranche_pct,
                add_on_price=close,
            )
        primary_source_style = _safe_text(cycle.get("primary_source_style"))
        projected_trailing_level, projected_trailing_mode = self._trailing_profile(
            family,
            metrics,
            _safe_float(cycle.get("stop_level")),
            primary_source_style=primary_source_style,
        )
        projected_protected_stop = projected_trailing_level
        if (
            close is not None
            and projected_blended_entry_price is not None
            and close > projected_blended_entry_price
        ):
            projected_protected_stop = _pick_latest(
                max(
                    value
                    for value in [
                        projected_trailing_level,
                        break_even_level,
                        projected_blended_entry_price,
                    ]
                    if value is not None
                )
                if projected_trailing_level is not None
                or break_even_level is not None
                or projected_blended_entry_price is not None
                else None
            )
        trailing_reference = _pick_latest(
            _safe_float(cycle.get("last_trailing_confirmed_level")),
            _safe_float(cycle.get("trailing_level")),
        )
        protected_reference = _pick_latest(
            _safe_float(cycle.get("last_protected_stop_level")),
            _safe_float(cycle.get("protected_stop_level")),
            trailing_reference,
        )
        pyramid_reference = _pick_latest(
            _safe_float(cycle.get("last_pyramid_reference_level")),
            protected_reference,
            trailing_reference,
        )

        trend_reversal_ready = bool(
            metrics.get("bullish_reversal")
            and (metrics.get("zone_hold") or metrics.get("zone_reclaim"))
        )
        family_ready = False
        family_reason = ""
        if family == "TF_REGULAR_PULLBACK":
            family_ready = bool(
                metrics.get("pullback_reset_ok")
                and trend_reversal_ready
                and metrics.get("support_trend_rising")
            )
            family_reason = "PULLBACK_RELOAD"
        elif family == "TF_BREAKOUT":
            family_ready = bool(
                metrics.get("build_up_ready")
                or metrics.get("vcp_active")
                or (metrics.get("breakout_ready") and metrics.get("setup_active"))
            )
            family_reason = "BREAKOUT_RELOAD"
        elif family == "TF_MOMENTUM":
            family_ready = bool(metrics.get("momentum_ready"))
            family_reason = "MOMENTUM_CONTINUE"
        elif family == "TF_PEG":
            peg_low = _safe_float(peg_context.get("gap_low"))
            peg_half = _safe_float(peg_context.get("half_gap"))
            peg_high = _safe_float(peg_context.get("event_high"))
            pullback_ready = bool(
                peg_context.get("peg_active")
                and peg_low is not None
                and peg_half is not None
                and low is not None
                and close is not None
                and peg_low <= low <= peg_half
                and close >= peg_half
            )
            rebreak_ready = bool(
                peg_context.get("peg_active")
                and peg_high is not None
                and close is not None
                and close >= peg_high
            )
            family_ready = bool(pullback_ready or rebreak_ready)
            family_reason = (
                "PEG_PULLBACK_READY"
                if pullback_ready
                else "PEG_REBREAK_READY" if rebreak_ready else "PEG_FOLLOWUP"
            )

        channel_blocked = bool(family != "TF_PEG" and metrics.get("in_channel8"))
        profit_zone = bool(
            projected_blended_entry_price is not None
            and close is not None
            and close > projected_blended_entry_price
        )
        blended_entry_protected = bool(
            projected_blended_entry_price is not None
            and projected_protected_stop is not None
            and projected_protected_stop >= projected_blended_entry_price
        )
        trailing_ratcheted = bool(
            projected_trailing_level is not None
            and trailing_reference is not None
            and projected_trailing_level > trailing_reference
        )
        protection_improved = bool(
            projected_protected_stop is not None
            and pyramid_reference is not None
            and projected_protected_stop > pyramid_reference
        )
        ready = bool(
            next_slot is not None
            and family_ready
            and not channel_blocked
            and profit_zone
            and blended_entry_protected
            and trailing_ratcheted
            and protection_improved
        )
        if next_slot is None:
            block_reason = "PYRAMID_MAXED"
        elif channel_blocked:
            block_reason = "ENTRY_BLOCKED_BY_CHANNEL"
        elif not family_ready:
            block_reason = "PYRAMID_SIGNAL_UNCONFIRMED"
        elif not profit_zone:
            block_reason = "AVERAGING_DOWN_BLOCKED"
        elif not trailing_ratcheted:
            block_reason = "TRAILING_RATCHET_MISSING"
        elif not blended_entry_protected:
            block_reason = "BLENDED_ENTRY_UNPROTECTED"
        elif not protection_improved:
            block_reason = "PROTECTED_STOP_NOT_RATCHETED"
        else:
            block_reason = ""
        reason_codes = [
            f"PYRAMID_SLOT_{next_slot}" if next_slot is not None else "PYRAMID_MAXED",
        ]
        if family_reason:
            reason_codes.append(family_reason)
            reason_codes.append("PYRAMID_SIGNAL_CONFIRMED")
        if channel_blocked:
            reason_codes.extend(["CHANNEL_8_ACTIVE", "ENTRY_BLOCKED_BY_CHANNEL"])
        if profit_zone:
            reason_codes.append("PROFIT_ZONE")
        else:
            reason_codes.append("AVERAGING_DOWN_BLOCKED")
        if trailing_ratcheted:
            reason_codes.append("TRAILING_RATCHET_CONFIRMED")
        if blended_entry_protected:
            reason_codes.append("BLENDED_ENTRY_PROTECTED")
        if protection_improved:
            reason_codes.append("PROTECTED_STOP_RATCHETED")
        if projected_trailing_mode:
            reason_codes.append(f"TRAIL_MODE_{projected_trailing_mode}")
        return {
            "ready": ready,
            "next_slot": next_slot,
            "tranche_pct": tranche_pct,
            "max_add_ons": max(_safe_int(cycle.get("max_add_ons")) or 2, 0),
            "projected_trailing_level": projected_trailing_level,
            "projected_protected_stop": projected_protected_stop,
            "projected_trailing_mode": projected_trailing_mode,
            "profit_zone": profit_zone,
            "family_ready": family_ready,
            "block_reason": block_reason,
            "trailing_ratcheted": trailing_ratcheted,
            "blended_entry_protected": blended_entry_protected,
            "protection_improved": protection_improved,
            "projected_blended_entry_price": projected_blended_entry_price,
            "base_position_units": base_position_units,
            "current_position_units": current_position_units,
            "projected_position_units": projected_position_units,
            "trailing_reference": trailing_reference,
            "protected_reference": protected_reference,
            "pyramid_reference": pyramid_reference,
            "reason_codes": reason_codes,
        }

    def _current_peg_context(
        self,
        symbol: str,
        metrics: Mapping[str, Any],
        raw_peg_map: Mapping[str, Mapping[str, Any]],
        peg_event_history: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:

        latest_date = _date_to_str(metrics.get("date")) or self.as_of_date

        if symbol not in raw_peg_map:

            payload = (
                self.earnings_collector.get_earnings_surprise(symbol)
                if self.earnings_collector
                else None
            )

            earnings_date = (
                _date_to_str(payload.get("earnings_date")) if payload else None
            )

        else:

            payload = None

            earnings_date = _date_to_str(raw_peg_map[symbol].get("earnings_date"))

        if earnings_date is not None:

            expected_event_date = _next_business_day(earnings_date)

            if latest_date == expected_event_date:

                gap_pct = _safe_float(metrics.get("gap_pct")) or 0.0

                if gap_pct < 8.0:

                    return {
                        "earnings_date": earnings_date,
                        "expected_event_date": expected_event_date,
                        "event_date": latest_date,
                        "event_day": True,
                        "missed": True,
                    }

                high = _safe_float(metrics.get("high"))

                low = _safe_float(metrics.get("low"))

                prev_close = _safe_float(metrics.get("prev_close"))

                if high is None or low is None or prev_close is None:

                    return {
                        "earnings_date": earnings_date,
                        "expected_event_date": expected_event_date,
                    }

                half_gap = prev_close + ((high - prev_close) * 0.50)

                return {
                    "earnings_date": earnings_date,
                    "expected_event_date": expected_event_date,
                    "event_date": latest_date,
                    "event_day": True,
                    "event_confirmed": True,
                    "peg_active": True,
                    "gap_pct": gap_pct,
                    "event_high": high,
                    "gap_low": low,
                    "half_gap": half_gap,
                }

        prior_event = peg_event_history.get(symbol)

        if prior_event:

            event_date = _date_to_str(prior_event.get("event_date"))

            followup_bars = _business_days_between(event_date, latest_date)

            if followup_bars is not None and 1 <= followup_bars <= _PEG_FOLLOWUP_WINDOW:

                return {
                    "earnings_date": prior_event.get("earnings_date"),
                    "event_date": event_date,
                    "event_day": False,
                    "event_confirmed": False,
                    "peg_active": True,
                    "followup_active": True,
                    "followup_bars": followup_bars,
                    "gap_pct": _safe_float(prior_event.get("gap_pct")),
                    "event_high": _safe_float(prior_event.get("event_high")),
                    "gap_low": _safe_float(prior_event.get("gap_low")),
                    "half_gap": _safe_float(prior_event.get("half_gap")),
                }

        if earnings_date is None:

            return {}

        return {
            "earnings_date": earnings_date,
            "expected_event_date": _next_business_day(earnings_date),
        }

    def _trend_buy_events(
        self,
        *,
        symbol: str,
        metrics: Mapping[str, Any],
        source_entry: Mapping[str, Any],
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
        peg_ready_map: Mapping[str, Any],
        peg_context: Mapping[str, Any],
    ) -> list[dict[str, Any]]:

        events: list[dict[str, Any]] = []

        quality_flags = self._base_quality_flags(metrics)

        screen_stage = str(
            source_entry.get("screen_stage")
            or metrics.get("screen_stage")
            or ""
        )

        conviction = self._trend_conviction(metrics)
        rvol20 = _safe_float(metrics.get("rvol20")) or 0.0
        close = _safe_float(metrics.get("close"))
        low = _safe_float(metrics.get("low"))
        channel_active = bool(metrics.get("in_channel8"))
        trend_reversal_ready = bool(
            metrics.get("bullish_reversal")
            and (metrics.get("zone_hold") or metrics.get("zone_reclaim"))
        )
        common_trend = bool(
            metrics.get("liquidity_pass")
            and not metrics.get("ema_turn_down")
            and metrics.get("alignment_state") == "BULLISH"
            and ((_safe_float(metrics.get("adx14")) or 0.0) >= 20.0)
            and metrics.get("ma_gap_pass")
            and metrics.get("support_trend_rising")
        )

        breakout_context = bool(
            metrics.get("liquidity_pass")
            and not metrics.get("ema_turn_down")
            and metrics.get("alignment_state") == "BULLISH"
            and ((_safe_float(metrics.get("adx14")) or 0.0) >= 20.0)
            and metrics.get("support_trend_rising")
            and (
                metrics.get("ma_gap_pass")
                or metrics.get("build_up_ready")
                or metrics.get("vcp_active")
            )
        )

        family = "TF_REGULAR_PULLBACK"
        trend_zone_low = _safe_float(metrics.get("trend_zone_low"))
        trend_zone_high = _safe_float(metrics.get("trend_zone_high"))
        family_profile = self._family_source_profile(
            symbol, family, source_entry, peg_ready_map
        )
        pullback_trigger = bool(
            common_trend
            and metrics.get("setup_active")
            and metrics.get("pullback_profile_pass")
            and trend_reversal_ready
            and not channel_active
        )
        if pullback_trigger:
            active = active_cycles.get(("TREND", family, symbol))
            addon_context = (
                self._trend_addon_context(
                    family=family, metrics=metrics, cycle=active
                )
                if active
                else {}
            )
            if active and not addon_context.get("ready"):
                pass
            else:
                action_type = (
                    "BUY" if family_profile.get("buy_allowed") else "WATCH"
                )
                events.append(
                    _annotate_buy_candidate_row(
                        _build_signal_row(
                            signal_date=metrics.get("date"),
                            symbol=symbol,
                            market=self.market,
                            engine="TREND",
                            family=family,
                            signal_kind="EVENT",
                            signal_code=(
                                "TF_ADDON_PYRAMID" if active else "TF_BUY_REGULAR"
                            ),
                            action_type=action_type,
                            conviction_grade=conviction,
                            screen_stage=screen_stage,
                            cooldown_bucket=family,
                            primary_source_style=family_profile.get(
                                "primary_style", ""
                            ),
                            source_fit_score=family_profile.get("fit_score"),
                            source_fit_label=family_profile.get("fit_label", ""),
                            support_zone_low=trend_zone_low,
                            support_zone_high=trend_zone_high,
                            stop_level=trend_zone_low,
                            protected_stop_level=addon_context.get(
                                "projected_protected_stop"
                            ),
                            add_on_slot=addon_context.get("next_slot"),
                            max_add_ons=addon_context.get("max_add_ons"),
                            tranche_pct=addon_context.get("tranche_pct"),
                            base_position_units=(
                                addon_context.get("base_position_units")
                                if active
                                else 1.0
                            ),
                            current_position_units=(
                                addon_context.get("projected_position_units")
                                if active
                                else 1.0
                            ),
                            blended_entry_price=(
                                addon_context.get("projected_blended_entry_price")
                                if active
                                else close
                            ),
                            last_trailing_confirmed_level=addon_context.get(
                                "trailing_reference"
                            ),
                            last_protected_stop_level=addon_context.get(
                                "protected_reference"
                            ),
                            last_pyramid_reference_level=addon_context.get(
                                "pyramid_reference"
                            ),
                            source_tags=metrics.get("source_tags"),
                            reason_codes=[
                                "PULLBACK",
                                "PRIOR_EXPANSION",
                                (
                                    "ZONE_HOLD"
                                    if metrics.get("zone_hold")
                                    else "ZONE_RECLAIM"
                                ),
                                "DRY_PULLBACK",
                                "BULLISH_REVERSAL",
                                *addon_context.get("reason_codes", []),
                                *family_profile.get("reason_codes", []),
                            ],
                            quality_flags=quality_flags,
                        ),
                        legacy_visible=bool(family_profile.get("watch_allowed")),
                        artifact_action_type="BUY",
                    )
                )

        family = "TF_BREAKOUT"
        family_profile = self._family_source_profile(
            symbol, family, source_entry, peg_ready_map
        )
        breakout_anchor = _safe_float(
            metrics.get("breakout_anchor")
        ) or _pick_latest(
            _safe_float(metrics.get("donchian_high20")),
            _safe_float(metrics.get("prior_high60")),
        )
        breakout_low, breakout_high = _zone_bounds(
            breakout_anchor, float(metrics.get("zone_width_pct") or 0.01)
        )
        breakout_signal_ready = bool(
            metrics.get("breakout_ready") or metrics.get("vcp_pivot_breakout")
        )
        breakout_trigger = bool(
            breakout_context
            and breakout_signal_ready
            and (metrics.get("setup_active") or metrics.get("build_up_ready"))
            and (
                metrics.get("vcp_active")
                or metrics.get("build_up_ready")
                or metrics.get("nh60")
            )
            and not channel_active
        )
        if breakout_trigger:
            active = active_cycles.get(("TREND", family, symbol))
            addon_context = (
                self._trend_addon_context(
                    family=family, metrics=metrics, cycle=active
                )
                if active
                else {}
            )
            if active and not addon_context.get("ready"):
                pass
            else:
                action_type = (
                    "BUY" if family_profile.get("buy_allowed") else "WATCH"
                )
                reason_codes = [
                    "BREAKOUT",
                    "SETUP",
                    "BB_UPPER_CLEAR",
                    "RVOL_PASS",
                    "BODY_STRENGTH",
                    "CLOSE_STRENGTH",
                ]
                if metrics.get("vcp_active"):
                    reason_codes.append("VCP")
                if metrics.get("build_up_ready"):
                    reason_codes.append("BUILDUP")
                if metrics.get("nh60"):
                    reason_codes.append("NEW_HIGH_CONTEXT")
                if metrics.get("vcp_pivot_breakout"):
                    reason_codes.append("VCP_PIVOT_BREAKOUT")
                reason_codes.extend(addon_context.get("reason_codes", []))
                reason_codes.extend(family_profile.get("reason_codes", []))
                events.append(
                    _annotate_buy_candidate_row(
                        _build_signal_row(
                            signal_date=metrics.get("date"),
                            symbol=symbol,
                            market=self.market,
                            engine="TREND",
                            family=family,
                            signal_kind="EVENT",
                            signal_code=(
                                "TF_ADDON_PYRAMID" if active else "TF_BUY_BREAKOUT"
                            ),
                            action_type=action_type,
                            conviction_grade=conviction,
                            screen_stage=screen_stage,
                            cooldown_bucket=family,
                            primary_source_style=family_profile.get(
                                "primary_style", ""
                            ),
                            source_fit_score=family_profile.get("fit_score"),
                            source_fit_label=family_profile.get("fit_label", ""),
                            support_zone_low=breakout_low,
                            support_zone_high=breakout_high,
                            stop_level=breakout_low,
                            protected_stop_level=addon_context.get(
                                "projected_protected_stop"
                            ),
                            add_on_slot=addon_context.get("next_slot"),
                            max_add_ons=addon_context.get("max_add_ons"),
                            tranche_pct=addon_context.get("tranche_pct"),
                            base_position_units=(
                                addon_context.get("base_position_units")
                                if active
                                else 1.0
                            ),
                            current_position_units=(
                                addon_context.get("projected_position_units")
                                if active
                                else 1.0
                            ),
                            blended_entry_price=(
                                addon_context.get("projected_blended_entry_price")
                                if active
                                else close
                            ),
                            last_trailing_confirmed_level=addon_context.get(
                                "trailing_reference"
                            ),
                            last_protected_stop_level=addon_context.get(
                                "protected_reference"
                            ),
                            last_pyramid_reference_level=addon_context.get(
                                "pyramid_reference"
                            ),
                            source_tags=metrics.get("source_tags"),
                            reason_codes=reason_codes,
                            quality_flags=quality_flags,
                        ),
                        legacy_visible=bool(family_profile.get("watch_allowed")),
                        artifact_action_type="BUY",
                    )
                )

        family = "TF_MOMENTUM"
        family_profile = self._family_source_profile(
            symbol, family, source_entry, peg_ready_map
        )
        momentum_trigger = bool(
            metrics.get("momentum_ready") and not channel_active
        )
        if momentum_trigger:
            active = active_cycles.get(("TREND", family, symbol))
            addon_context = (
                self._trend_addon_context(
                    family=family, metrics=metrics, cycle=active
                )
                if active
                else {}
            )
            if active and not addon_context.get("ready"):
                pass
            else:
                action_type = (
                    "BUY" if family_profile.get("buy_allowed") else "WATCH"
                )
                events.append(
                    _annotate_buy_candidate_row(
                        _build_signal_row(
                            signal_date=metrics.get("date"),
                            symbol=symbol,
                            market=self.market,
                            engine="TREND",
                            family=family,
                            signal_kind="EVENT",
                            signal_code=(
                                "TF_ADDON_PYRAMID" if active else "TF_BUY_MOMENTUM"
                            ),
                            action_type=action_type,
                            conviction_grade=conviction,
                            screen_stage=screen_stage,
                            cooldown_bucket=family,
                            primary_source_style=family_profile.get(
                                "primary_style", ""
                            ),
                            source_fit_score=family_profile.get("fit_score"),
                            source_fit_label=family_profile.get("fit_label", ""),
                            support_zone_low=_safe_float(metrics.get("donchian_low20")),
                            support_zone_high=_safe_float(metrics.get("fast_ref")),
                            stop_level=_safe_float(metrics.get("donchian_low20")),
                            protected_stop_level=addon_context.get(
                                "projected_protected_stop"
                            ),
                            add_on_slot=addon_context.get("next_slot"),
                            max_add_ons=addon_context.get("max_add_ons"),
                            tranche_pct=addon_context.get("tranche_pct"),
                            base_position_units=(
                                addon_context.get("base_position_units")
                                if active
                                else 1.0
                            ),
                            current_position_units=(
                                addon_context.get("projected_position_units")
                                if active
                                else 1.0
                            ),
                            blended_entry_price=(
                                addon_context.get("projected_blended_entry_price")
                                if active
                                else close
                            ),
                            last_trailing_confirmed_level=addon_context.get(
                                "trailing_reference"
                            ),
                            last_protected_stop_level=addon_context.get(
                                "protected_reference"
                            ),
                            last_pyramid_reference_level=addon_context.get(
                                "pyramid_reference"
                            ),
                            source_tags=metrics.get("source_tags"),
                            reason_codes=[
                                "RSI_STRONG",
                                "MACD_UP",
                                "DONCHIAN_HIGH",
                                "STRONG_CLOSE",
                                "TACTICAL_SWING",
                                *addon_context.get("reason_codes", []),
                                *family_profile.get("reason_codes", []),
                            ],
                            quality_flags=quality_flags,
                        ),
                        legacy_visible=bool(family_profile.get("watch_allowed")),
                        artifact_action_type="BUY",
                    )
                )

        family = "TF_PEG"
        family_profile = self._family_source_profile(
            symbol, family, source_entry, peg_ready_map
        )
        if family_profile.get("watch_allowed"):
            if peg_context.get("event_day") and peg_context.get("event_confirmed"):
                events.append(
                    _build_signal_row(
                        signal_date=metrics.get("date"),
                        symbol=symbol,
                        market=self.market,
                        engine="TREND",
                        family=family,
                        signal_kind="EVENT",
                        signal_code="TF_PEG_EVENT",
                        action_type="ALERT",
                        conviction_grade=conviction,
                        screen_stage=screen_stage,
                        cooldown_bucket=family,
                        primary_source_style=family_profile.get("primary_style", ""),
                        source_fit_score=family_profile.get("fit_score"),
                        source_fit_label=family_profile.get("fit_label", ""),
                        support_zone_low=_safe_float(peg_context.get("gap_low")),
                        support_zone_high=_safe_float(peg_context.get("half_gap")),
                        stop_level=_safe_float(peg_context.get("gap_low")),
                        source_tags=metrics.get("source_tags"),
                        reason_codes=[
                            "PEG_EVENT_CONFIRMED",
                            *family_profile.get("reason_codes", []),
                        ],
                        quality_flags=quality_flags,
                    )
                )
            if peg_context.get("event_day") and peg_context.get("missed"):
                events.append(
                    _build_signal_row(
                        signal_date=metrics.get("date"),
                        symbol=symbol,
                        market=self.market,
                        engine="TREND",
                        family=family,
                        signal_kind="EVENT",
                        signal_code="TF_PEG_EVENT",
                        action_type="WATCH",
                        conviction_grade=conviction,
                        screen_stage=screen_stage,
                        cooldown_bucket=family,
                        primary_source_style=family_profile.get("primary_style", ""),
                        source_fit_score=family_profile.get("fit_score"),
                        source_fit_label=family_profile.get("fit_label", ""),
                        source_tags=metrics.get("source_tags"),
                        reason_codes=[
                            "PEG_MISS",
                            *family_profile.get("reason_codes", []),
                        ],
                        quality_flags=quality_flags,
                    )
                )
        if peg_context.get("peg_active"):
            peg_low = _safe_float(peg_context.get("gap_low"))
            peg_half = _safe_float(peg_context.get("half_gap"))
            peg_high = _safe_float(peg_context.get("event_high"))

            pullback_trigger = bool(
                not metrics.get("ema_turn_down")
                and metrics.get("alignment_state") == "BULLISH"
                and peg_low is not None
                and peg_half is not None
                and low is not None
                and close is not None
                and peg_low <= low <= peg_half
                and close >= peg_half
            )

            rebreak_trigger = bool(
                not metrics.get("ema_turn_down")
                and metrics.get("alignment_state") == "BULLISH"
                and peg_high is not None
                and close is not None
                and close >= peg_high
            )
            active = active_cycles.get(("TREND", family, symbol))
            addon_context = (
                self._trend_addon_context(
                    family=family,
                    metrics=metrics,
                    cycle=active,
                    peg_context=peg_context,
                )
                if active
                else {}
            )
            if pullback_trigger:
                if not active or addon_context.get("ready"):
                    action_type = (
                        "BUY" if family_profile.get("buy_allowed") else "WATCH"
                    )
                    events.append(
                        _annotate_buy_candidate_row(
                            _build_signal_row(
                                signal_date=metrics.get("date"),
                                symbol=symbol,
                                market=self.market,
                                engine="TREND",
                                family=family,
                                signal_kind="EVENT",
                                signal_code=(
                                    "TF_ADDON_PYRAMID"
                                    if active
                                    else "TF_BUY_PEG_PULLBACK"
                                ),
                                action_type=action_type,
                                conviction_grade=conviction,
                                screen_stage=screen_stage,
                                cooldown_bucket=family,
                                primary_source_style=family_profile.get(
                                    "primary_style", ""
                                ),
                                source_fit_score=family_profile.get("fit_score"),
                                source_fit_label=family_profile.get("fit_label", ""),
                                support_zone_low=peg_low,
                                support_zone_high=peg_half,
                                stop_level=peg_low,
                                protected_stop_level=addon_context.get(
                                    "projected_protected_stop"
                                ),
                                add_on_slot=addon_context.get("next_slot"),
                                max_add_ons=addon_context.get("max_add_ons"),
                                tranche_pct=addon_context.get("tranche_pct"),
                                base_position_units=(
                                    addon_context.get("base_position_units")
                                    if active
                                    else 1.0
                                ),
                                current_position_units=(
                                    addon_context.get("projected_position_units")
                                    if active
                                    else 1.0
                                ),
                                blended_entry_price=(
                                    addon_context.get("projected_blended_entry_price")
                                    if active
                                    else close
                                ),
                                last_trailing_confirmed_level=addon_context.get(
                                    "trailing_reference"
                                ),
                                last_protected_stop_level=addon_context.get(
                                    "protected_reference"
                                ),
                                last_pyramid_reference_level=addon_context.get(
                                    "pyramid_reference"
                                ),
                                source_tags=metrics.get("source_tags"),
                                reason_codes=[
                                    "PEG_PULLBACK",
                                    "R50",
                                    *addon_context.get("reason_codes", []),
                                    *family_profile.get("reason_codes", []),
                                ],
                                quality_flags=quality_flags,
                            ),
                            legacy_visible=bool(family_profile.get("watch_allowed")),
                            artifact_action_type="BUY",
                        )
                    )
            if rebreak_trigger:
                if not active or addon_context.get("ready"):
                    action_type = (
                        "BUY" if family_profile.get("buy_allowed") else "WATCH"
                    )
                    events.append(
                        _annotate_buy_candidate_row(
                            _build_signal_row(
                                signal_date=metrics.get("date"),
                                symbol=symbol,
                                market=self.market,
                                engine="TREND",
                                family=family,
                                signal_kind="EVENT",
                                signal_code=(
                                    "TF_ADDON_PYRAMID"
                                    if active
                                    else "TF_BUY_PEG_REBREAK"
                                ),
                                action_type=action_type,
                                conviction_grade=conviction,
                                screen_stage=screen_stage,
                                cooldown_bucket=family,
                                primary_source_style=family_profile.get(
                                    "primary_style", ""
                                ),
                                source_fit_score=family_profile.get("fit_score"),
                                source_fit_label=family_profile.get("fit_label", ""),
                                support_zone_low=peg_half,
                                support_zone_high=peg_high,
                                stop_level=peg_low,
                                protected_stop_level=addon_context.get(
                                    "projected_protected_stop"
                                ),
                                add_on_slot=addon_context.get("next_slot"),
                                max_add_ons=addon_context.get("max_add_ons"),
                                tranche_pct=addon_context.get("tranche_pct"),
                                base_position_units=(
                                    addon_context.get("base_position_units")
                                    if active
                                    else 1.0
                                ),
                                current_position_units=(
                                    addon_context.get("projected_position_units")
                                    if active
                                    else 1.0
                                ),
                                blended_entry_price=(
                                    addon_context.get("projected_blended_entry_price")
                                    if active
                                    else close
                                ),
                                last_trailing_confirmed_level=addon_context.get(
                                    "trailing_reference"
                                ),
                                last_protected_stop_level=addon_context.get(
                                    "protected_reference"
                                ),
                                last_pyramid_reference_level=addon_context.get(
                                    "pyramid_reference"
                                ),
                                source_tags=metrics.get("source_tags"),
                                reason_codes=[
                                    "PEG_REBREAK",
                                    *addon_context.get("reason_codes", []),
                                    *family_profile.get("reason_codes", []),
                                ],
                                quality_flags=quality_flags,
                            ),
                            legacy_visible=bool(family_profile.get("watch_allowed")),
                            artifact_action_type="BUY",
                        )
                    )
        return events

    def _trend_state_rows(
        self,
        *,
        symbol: str,
        metrics: Mapping[str, Any],
        source_entry: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        conviction = self._trend_conviction(metrics)
        screen_stage = str(
            source_entry.get("screen_stage")
            or metrics.get("screen_stage")
            or ""
        )
        family_profile = self._family_source_profile(
            symbol, "TF_BREAKOUT", source_entry, peg_ready_map={}
        )
        source_reason_codes = list(family_profile.get("reason_codes", []))
        if metrics.get("setup_active"):
            setup_reason_codes = ["SETUP", "EMA_ALIGNED", "ZONE_SUPPORT"]
            if metrics.get("in_channel8"):
                setup_reason_codes.append("CHANNEL_8_ACTIVE")
            rows.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family="TF_BREAKOUT",
                    signal_kind="AUX",
                    signal_code="TF_SETUP_ACTIVE",
                    action_type="STATE",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    primary_source_style=family_profile.get("primary_style", ""),
                    source_fit_score=family_profile.get("fit_score"),
                    source_fit_label=family_profile.get("fit_label", ""),
                    support_zone_low=_safe_float(metrics.get("trend_zone_low")),
                    support_zone_high=_safe_float(metrics.get("trend_zone_high")),
                    source_tags=metrics.get("source_tags"),
                    reason_codes=[*setup_reason_codes, *source_reason_codes],
                    quality_flags=self._base_quality_flags(metrics),
                )
            )
        if metrics.get("vcp_active"):

            rows.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family="TF_BREAKOUT",
                    signal_kind="AUX",
                    signal_code="TF_VCP_ACTIVE",
                    action_type="STATE",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    primary_source_style=family_profile.get("primary_style", ""),
                    source_fit_score=family_profile.get("fit_score"),
                    source_fit_label=family_profile.get("fit_label", ""),
                    support_zone_low=_safe_float(metrics.get("trend_zone_low")),
                    support_zone_high=_safe_float(metrics.get("trend_zone_high")),
                    source_tags=metrics.get("source_tags"),
                    reason_codes=[
                        "VCP",
                        "DRY_VOLUME",
                        "TIGHT_RANGE",
                        *source_reason_codes,
                    ],
                    quality_flags=self._base_quality_flags(metrics),
                )
            )
        if metrics.get("setup_active") or metrics.get("build_up_ready"):

            reason_codes = ["SETUP"]

            if metrics.get("build_up_ready"):

                reason_codes.append("BUILD_UP_READY")

            if metrics.get("vcp_active"):

                reason_codes.append("VCP")

            if metrics.get("squeeze_active"):

                reason_codes.append("SQUEEZE")

            if metrics.get("tight_active"):

                reason_codes.append("TIGHT")

            if metrics.get("volume_dry"):
                reason_codes.append("DRY_VOLUME")
            if metrics.get("near_high_ready"):
                reason_codes.append("NEAR_HIGH")
            if metrics.get("in_channel8"):
                reason_codes.append("CHANNEL_8_ACTIVE")
            rows.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family="TF_BREAKOUT",
                    signal_kind="STATE",
                    signal_code="TF_BUILDUP_READY",
                    action_type="STATE",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    primary_source_style=family_profile.get("primary_style", ""),
                    source_fit_score=family_profile.get("fit_score"),
                    source_fit_label=family_profile.get("fit_label", ""),
                    support_zone_low=_safe_float(metrics.get("trend_zone_low")),
                    support_zone_high=_safe_float(metrics.get("trend_zone_high")),
                    source_tags=metrics.get("source_tags"),
                    reason_codes=reason_codes + source_reason_codes,
                    quality_flags=self._base_quality_flags(metrics),
                )
            )
        if metrics.get("aggressive_ready"):

            rows.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family="TF_BREAKOUT",
                    signal_kind="STATE",
                    signal_code="TF_AGGRESSIVE_ALERT",
                    action_type="ALERT",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    primary_source_style=family_profile.get("primary_style", ""),
                    source_fit_score=family_profile.get("fit_score"),
                    source_fit_label=family_profile.get("fit_label", ""),
                    support_zone_low=_safe_float(metrics.get("trend_zone_low")),
                    support_zone_high=_safe_float(metrics.get("trend_zone_high")),
                    source_tags=metrics.get("source_tags"),
                    reason_codes=[
                        "BELOW_200MA_ALERT",
                        "BULLISH_REVERSAL",
                        "RISING_EMA",
                        *source_reason_codes,
                    ],
                    quality_flags=self._base_quality_flags(metrics),
                )
            )
        return rows

    def _trend_sell_events(
        self,
        *,
        symbol: str,
        metrics: Mapping[str, Any],
        cycle: Mapping[str, Any],
    ) -> list[dict[str, Any]]:

        family = _safe_text(cycle.get("family"))

        conviction = self._trend_conviction(metrics)

        screen_stage = str(
            metrics.get("screen_stage") or cycle.get("screen_stage") or "ACTIVE_CYCLE"
        )

        close = _safe_float(metrics.get("close")) or 0.0

        high = _safe_float(metrics.get("high")) or 0.0

        events: list[dict[str, Any]] = []
        trailing_level = _safe_float(cycle.get("trailing_level"))
        protected_stop_level = _safe_float(cycle.get("protected_stop_level"))
        effective_trailing_level = _pick_latest(
            max(
                value
                for value in [trailing_level, protected_stop_level]
                if value is not None
            )
            if trailing_level is not None or protected_stop_level is not None
            else None
        )
        daily_frame = (
            metrics.get("daily")
            if isinstance(metrics.get("daily"), pd.DataFrame)
            else pd.DataFrame()
        )
        exit_pressure_profile = _signal_patterns.score_exit_pressure(
            daily_frame,
            metrics,
            cycle,
        )
        effective_trailing_level = _pick_latest(
            _safe_float(exit_pressure_profile.get("effective_trailing_level")),
            effective_trailing_level,
        )
        exit_pressure_reason_codes = list(
            exit_pressure_profile.get("reason_codes", [])
        )
        tp1_level = _safe_float(cycle.get("tp1_level"))
        tp2_level = _safe_float(cycle.get("tp2_level"))
        support_low = _safe_float(cycle.get("support_zone_low"))
        support_high = _safe_float(cycle.get("support_zone_high"))
        quality_flags = self._base_quality_flags(metrics)
        days_open = _business_days_between(cycle.get("opened_on"), metrics.get("date"))
        trailing_mode = _safe_text(cycle.get("trailing_mode"))
        tp_plan = _safe_text(cycle.get("tp_plan"))
        trim_count = _safe_int(cycle.get("trim_count"))
        risk_free_armed = _safe_bool(cycle.get("risk_free_armed"))
        break_even_level = _safe_float(cycle.get("break_even_level"))
        blended_entry_price = _safe_float(cycle.get("blended_entry_price"))
        base_position_units = _safe_float(cycle.get("base_position_units"))
        current_position_units = _safe_float(cycle.get("current_position_units"))
        primary_source_style = _safe_text(cycle.get("primary_source_style"))
        source_fit_label = _safe_text(cycle.get("source_fit_label"))
        source_fit_score = _safe_float(cycle.get("source_fit_score"))
        exit_context_codes = []
        if primary_source_style:
            exit_context_codes.append(f"SOURCE_STYLE_{primary_source_style}")
        if source_fit_label:
            exit_context_codes.append(f"SOURCE_FIT_{source_fit_label}")
        if trailing_mode:
            exit_context_codes.append(f"TRAIL_MODE_{trailing_mode}")
        if tp_plan:
            exit_context_codes.append(tp_plan)
        if trim_count:
            exit_context_codes.append(f"TRIM_COUNT_{trim_count}")
        if risk_free_armed:
            exit_context_codes.append("RISK_FREE_ARMED")
        channel_active = bool(metrics.get("in_channel8"))

        if support_low is not None and close <= support_low:

            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="TF_SELL_BREAKDOWN",
                    action_type="SELL",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=support_low,
                    break_even_level=break_even_level,
                    blended_entry_price=blended_entry_price,
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    trailing_mode=trailing_mode,
                    tp_plan=tp_plan,
                    trim_count=trim_count,
                    risk_free_armed=risk_free_armed,
                    protected_stop_level=protected_stop_level,
                    base_position_units=base_position_units,
                    current_position_units=current_position_units,
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=[
                        "SUPPORT_FAIL",
                        *exit_pressure_reason_codes,
                        *exit_context_codes,
                    ],
                    quality_flags=quality_flags,
                )
            )
        channel_floor = _safe_float(metrics.get("channel_low8"))
        if channel_floor is not None and close <= channel_floor:
            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="TF_SELL_CHANNEL_BREAK",
                    action_type="SELL",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=channel_floor,
                    break_even_level=break_even_level,
                    blended_entry_price=blended_entry_price,
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    trailing_mode=trailing_mode,
                    tp_plan=tp_plan,
                    trim_count=trim_count,
                    risk_free_armed=risk_free_armed,
                    protected_stop_level=protected_stop_level,
                    base_position_units=base_position_units,
                    current_position_units=current_position_units,
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=["CHANNEL_BREAK_8", *exit_context_codes],
                    quality_flags=quality_flags,
                )
            )
        if (
            not channel_active
            and effective_trailing_level is not None
            and close <= effective_trailing_level
        ):
            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="TF_SELL_TRAILING_BREAK",
                    action_type="SELL",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=effective_trailing_level,
                    break_even_level=break_even_level,
                    blended_entry_price=blended_entry_price,
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    trailing_mode=trailing_mode,
                    tp_plan=tp_plan,
                    trim_count=trim_count,
                    risk_free_armed=risk_free_armed,
                    protected_stop_level=protected_stop_level,
                    base_position_units=base_position_units,
                    current_position_units=current_position_units,
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=[
                        "TRAILING_BREAK",
                        *exit_pressure_reason_codes,
                        *exit_context_codes,
                    ],
                    quality_flags=quality_flags,
                )
            )
        if (
            tp1_level is not None
            and high >= tp1_level
            and not _safe_bool(cycle.get("tp1_hit"))
        ):

            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="TF_SELL_TP1",
                    action_type="SELL",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=effective_trailing_level,
                    break_even_level=break_even_level,
                    blended_entry_price=blended_entry_price,
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    trailing_mode=trailing_mode,
                    tp_plan=tp_plan,
                    trim_count=trim_count,
                    risk_free_armed=risk_free_armed,
                    protected_stop_level=protected_stop_level,
                    base_position_units=base_position_units,
                    current_position_units=current_position_units,
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=["TP1_HIT", *exit_context_codes],
                    quality_flags=quality_flags,
                )
            )
        if (
            tp2_level is not None
            and high >= tp2_level
            and not _safe_bool(cycle.get("tp2_hit"))
        ):

            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="TF_SELL_TP2",
                    action_type="SELL",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=effective_trailing_level,
                    break_even_level=break_even_level,
                    blended_entry_price=blended_entry_price,
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    trailing_mode=trailing_mode,
                    tp_plan=tp_plan,
                    trim_count=trim_count,
                    risk_free_armed=risk_free_armed,
                    protected_stop_level=protected_stop_level,
                    base_position_units=base_position_units,
                    current_position_units=current_position_units,
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=["TP2_HIT", *exit_context_codes],
                    quality_flags=quality_flags,
                )
            )
        if family == "TF_MOMENTUM":

            momentum_end = bool(
                ((_safe_float(metrics.get("rsi14")) or 100.0) < 55.0)
                or ((_safe_float(metrics.get("macd_hist")) or 1.0) < 0.0)
                or (close <= (_safe_float(metrics.get("ema10")) or close))
                or (
                    days_open is not None
                    and days_open >= 10
                    and close < (_safe_float(metrics.get("fast_ref")) or close)
                )
            )

            if momentum_end:

                reason_codes = ["MOMENTUM_END"]

                if (_safe_float(metrics.get("rsi14")) or 100.0) < 55.0:

                    reason_codes.append("RSI_FADE")

                if (_safe_float(metrics.get("macd_hist")) or 1.0) < 0.0:

                    reason_codes.append("MACD_FADE")

                if close <= (_safe_float(metrics.get("ema10")) or close):

                    reason_codes.append("EMA10_LOSS")

                if (
                    days_open is not None
                    and days_open >= 10
                    and close < (_safe_float(metrics.get("fast_ref")) or close)
                ):

                    reason_codes.append("SWING_WINDOW_EXPIRED")

                events.append(
                    _build_signal_row(
                        signal_date=metrics.get("date"),
                        symbol=symbol,
                        market=self.market,
                        engine="TREND",
                        family=family,
                        family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                        signal_kind="EVENT",
                        signal_code="TF_SELL_MOMENTUM_END",
                        action_type="SELL",
                        conviction_grade=conviction,
                        screen_stage=screen_stage,
                        support_zone_low=support_low,
                        support_zone_high=support_high,
                        stop_level=trailing_level,
                        break_even_level=break_even_level,
                        tp1_level=tp1_level,
                        tp2_level=tp2_level,
                        trailing_mode=trailing_mode,
                        tp_plan=tp_plan,
                        trim_count=trim_count,
                        risk_free_armed=risk_free_armed,
                        primary_source_style=primary_source_style,
                        source_fit_score=source_fit_score,
                        source_fit_label=source_fit_label,
                        source_tags=metrics.get("source_tags"),
                        reason_codes=reason_codes + exit_context_codes,
                        quality_flags=quality_flags,
                    )
                )
        rejection_zone = _safe_float(
            exit_pressure_profile.get("resistance_reject_level")
        ) or _safe_float(metrics.get("bb_mid"))
        if bool(exit_pressure_profile.get("resistance_reject")):
            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="TREND",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="TF_SELL_RESISTANCE_REJECT",
                    action_type="SELL",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=trailing_level,
                    break_even_level=break_even_level,
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    trailing_mode=trailing_mode,
                    tp_plan=tp_plan,
                    trim_count=trim_count,
                    risk_free_armed=risk_free_armed,
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=[
                        "PULLBACK_SELL",
                        *exit_pressure_reason_codes,
                        *exit_context_codes,
                    ],
                    quality_flags=quality_flags,
                )
            )
        return events

    def _ug_state_code(
        self,
        metrics: Mapping[str, Any],
        dashboard_profile: Mapping[str, Any] | None = None,
    ) -> str:
        if dashboard_profile is None:
            dashboard_profile = self._ug_dashboard_profile(metrics)
        light = _safe_text(
            dashboard_profile.get("technical_light")
        ) or self._ug_traffic_light(
            metrics,
            self._ug_validation_score(metrics),
        )
        return f"UG_STATE_{light}"

    def _ug_state_rows(
        self,
        *,
        symbol: str,
        metrics: Mapping[str, Any],
        source_entry: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        dashboard_profile = self._ug_dashboard_profile(metrics)
        validation_score = _safe_float(dashboard_profile.get("validation_score")) or 0.0
        state_code = self._ug_state_code(metrics, dashboard_profile)
        traffic_light = _safe_text(
            dashboard_profile.get("technical_light")
        ) or state_code.removeprefix("UG_STATE_")
        conviction = self._ug_conviction(metrics, state_code, dashboard_profile)
        screen_stage = str(
            source_entry.get("screen_stage")
            or metrics.get("screen_stage")
            or ""
        )
        family_profile = self._family_source_profile(
            symbol, "UG_BREAKOUT", source_entry, peg_ready_map={}
        )
        source_reason_codes = list(family_profile.get("reason_codes", []))
        state_reasons = [
            traffic_light,
            f"VALIDATION_{int(round(validation_score))}",
            *list(dashboard_profile.get("reason_codes", [])),
            *source_reason_codes,
        ]
        rows.append(
            _build_signal_row(
                signal_date=metrics.get("date"),
                symbol=symbol,
                market=self.market,
                engine="UG",
                family="UG_STATE",
                signal_kind="STATE",
                signal_code=state_code,
                action_type="STATE",
                conviction_grade=conviction,
                screen_stage=screen_stage,
                signal_score=validation_score,
                gp_score=_safe_float(dashboard_profile.get("gp_score")),
                gp_health=_safe_text(dashboard_profile.get("gp_health")),
                sigma_score=_safe_float(dashboard_profile.get("sigma_score")),
                sigma_health=_safe_text(dashboard_profile.get("sigma_health")),
                traffic_light=traffic_light,
                technical_light=traffic_light,
                growth_score=_safe_float(dashboard_profile.get("growth_score")),
                growth_health=_safe_text(dashboard_profile.get("growth_health")),
                eps_health=_safe_text(dashboard_profile.get("eps_health")),
                sales_health=_safe_text(dashboard_profile.get("sales_health")),
                growth_data_status=_safe_text(
                    dashboard_profile.get("growth_data_status")
                ),
                dashboard_score=_safe_float(dashboard_profile.get("dashboard_score")),
                dashboard_light=_safe_text(dashboard_profile.get("dashboard_light")),
                dashboard_position_bias=_safe_text(
                    dashboard_profile.get("dashboard_position_bias")
                ),
                signal_phase="STATE",
                primary_source_style=family_profile.get("primary_style", ""),
                source_fit_score=family_profile.get("fit_score"),
                source_fit_label=family_profile.get("fit_label", ""),
                support_zone_low=_safe_float(metrics.get("bb_zone_low")),
                support_zone_high=_safe_float(metrics.get("bb_zone_high")),
                source_tags=metrics.get("source_tags"),
                reason_codes=state_reasons,
                quality_flags=self._base_quality_flags(metrics),
            )
        )

        state_volume_rvol = _safe_float(metrics.get("bullish_rvol50"))
        if state_volume_rvol is None:
            state_volume_rvol = _safe_float(metrics.get("rvol20")) or 0.0

        for active, code in (
            (metrics.get("nh60"), "UG_NH60"),
            (state_volume_rvol >= 2.0, "UG_VOL2X"),
            (metrics.get("w_active"), "UG_W"),
            (metrics.get("vcp_active"), "UG_VCP"),
            (metrics.get("squeeze_active"), "UG_SQUEEZE"),
            (metrics.get("tight_active"), "UG_TIGHT"),
        ):

            if active:

                rows.append(
                    _build_signal_row(
                        signal_date=metrics.get("date"),
                        symbol=symbol,
                        market=self.market,
                        engine="UG",
                        family="UG_STATE",
                        signal_kind="AUX",
                        signal_code=code,
                        action_type="STATE",
                        conviction_grade=conviction,
                        screen_stage=screen_stage,
                        signal_score=validation_score,
                        gp_score=_safe_float(dashboard_profile.get("gp_score")),
                        gp_health=_safe_text(dashboard_profile.get("gp_health")),
                        sigma_score=_safe_float(dashboard_profile.get("sigma_score")),
                        sigma_health=_safe_text(dashboard_profile.get("sigma_health")),
                        traffic_light=traffic_light,
                        technical_light=traffic_light,
                        growth_score=_safe_float(dashboard_profile.get("growth_score")),
                        growth_health=_safe_text(
                            dashboard_profile.get("growth_health")
                        ),
                        eps_health=_safe_text(dashboard_profile.get("eps_health")),
                        sales_health=_safe_text(dashboard_profile.get("sales_health")),
                        growth_data_status=_safe_text(
                            dashboard_profile.get("growth_data_status")
                        ),
                        dashboard_score=_safe_float(
                            dashboard_profile.get("dashboard_score")
                        ),
                        dashboard_light=_safe_text(
                            dashboard_profile.get("dashboard_light")
                        ),
                        dashboard_position_bias=_safe_text(
                            dashboard_profile.get("dashboard_position_bias")
                        ),
                        signal_phase="AUX",
                        primary_source_style=family_profile.get("primary_style", ""),
                        source_fit_score=family_profile.get("fit_score"),
                        source_fit_label=family_profile.get("fit_label", ""),
                        source_tags=metrics.get("source_tags"),
                        reason_codes=[
                            {
                                "UG_NH60": "GP_NH60",
                                "UG_VOL2X": "GP_VOL2X",
                                "UG_W": "GP_W",
                                "UG_VCP": "SIGMA_VCP_READY",
                                "UG_SQUEEZE": "SIGMA_SQUEEZE_READY",
                                "UG_TIGHT": "SIGMA_TIGHT_READY",
                            }.get(code, code.removeprefix("UG_")),
                            *list(dashboard_profile.get("reason_codes", [])),
                            *source_reason_codes,
                        ],
                        quality_flags=self._base_quality_flags(metrics),
                    )
                )
        return rows

    def _is_ug_cooldown_blocked(
        self,
        symbol: str,
        signal_code: str,
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
        signal_history: Iterable[Mapping[str, Any]],
    ) -> bool:
        _ = signal_code
        for cycle in active_cycles.values():
            if _safe_text(cycle.get("engine")) != "UG":
                continue
            if _safe_text(cycle.get("symbol")).upper() != symbol:
                continue
            if _safe_text(cycle.get("buy_signal_code")) not in _UG_BUY_CODES:
                continue
            bars = _business_days_between(cycle.get("opened_on"), self.as_of_date)
            if bars is not None and 1 <= bars < _UG_COOLDOWN_BUSINESS_DAYS:
                return True
        for row in signal_history:
            if _safe_text(row.get("engine")) != "UG":
                continue
            if _safe_text(row.get("symbol")).upper() != symbol:
                continue
            if _safe_text(row.get("signal_code")) not in _UG_BUY_CODES:
                continue
            if _safe_text(row.get("action_type")) != "BUY":
                continue
            bars = _business_days_between(row.get("signal_date"), self.as_of_date)
            if bars is not None and 1 <= bars < _UG_COOLDOWN_BUSINESS_DAYS:
                return True

        return False

    def _ug_buy_events(
        self,
        *,
        symbol: str,
        metrics: Mapping[str, Any],
        source_entry: Mapping[str, Any],
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
        signal_history: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        screen_stage = str(
            source_entry.get("screen_stage")
            or metrics.get("screen_stage")
            or ""
        )
        dashboard_profile = self._ug_dashboard_profile(metrics)
        validation_score = _safe_float(dashboard_profile.get("validation_score")) or 0.0
        gp_score = _safe_float(dashboard_profile.get("gp_score"))
        sigma_score = _safe_float(dashboard_profile.get("sigma_score"))
        state_code = self._ug_state_code(metrics, dashboard_profile)
        traffic_light = _safe_text(
            dashboard_profile.get("technical_light")
        ) or state_code.removeprefix("UG_STATE_")
        conviction = self._ug_conviction(metrics, state_code, dashboard_profile)
        quality_flags = self._base_quality_flags(metrics)
        recent_squeeze_context = bool(metrics.get("recent_squeeze_ready10"))
        recent_orange_context = bool(metrics.get("recent_orange_ready10"))
        buy_volume_rvol = _safe_float(metrics.get("bullish_rvol50"))
        if buy_volume_rvol is None:
            buy_volume_rvol = _safe_float(metrics.get("rvol20")) or 0.0
        breakout_signal_ready = bool(
            metrics.get("breakout_ready") or metrics.get("vcp_pivot_breakout")
        )
        breakout_condition = bool(
            traffic_light == "GREEN"
            and metrics.get("nh60")
            and breakout_signal_ready
            and not metrics.get("ema_turn_down")
        )
        squeeze_breakout_condition = bool(
            breakout_condition
            and recent_orange_context
            and recent_squeeze_context
            and buy_volume_rvol >= 2.0
        )
        band_reversion_profile = _band_reversion_from_metrics(metrics)
        refined_band_gate_present = _has_band_reversion_diagnostics(metrics)
        pbb_gate_pass = bool(
            metrics.get("ug_pbb_ready")
            and (
                not refined_band_gate_present
                or band_reversion_profile.get("pbb_ready")
            )
        )
        mr_long_gate_pass = bool(
            metrics.get("ug_mr_long_ready")
            and (
                not refined_band_gate_present
                or band_reversion_profile.get("mr_long_ready")
            )
        )
        band_reason_codes = list(
            dict.fromkeys(
                list(metrics.get("band_reversion_reason_codes", []))
                + list(band_reversion_profile.get("reason_codes", []))
            )
        )
        pullback_condition = bool(
            pbb_gate_pass
            and metrics.get("pullback_profile_pass")
            and not metrics.get("ema_turn_down")
        )
        mr_long_condition = bool(
            mr_long_gate_pass
            and metrics.get("alignment_state") == "BULLISH"
            and not metrics.get("ema_turn_down")
        )
        reference_target_level = _pick_latest(
            _safe_float(metrics.get("prior_high15")),
            _safe_float(metrics.get("prior_high60")),
            _safe_float(metrics.get("breakout_anchor")),
        )

        for (
            family,
            code,
            condition,
            support_low,
            support_high,
            stop_level,
            reasons,
            signal_phase,
            reference_exit_signal,
        ) in (
            (
                "UG_BREAKOUT",
                (
                    "UG_BUY_SQUEEZE_BREAKOUT"
                    if squeeze_breakout_condition
                    else "UG_BUY_BREAKOUT"
                ),
                breakout_condition,
                _safe_float(metrics.get("bb_mid")),
                _safe_float(metrics.get("bb_upper")),
                _safe_float(metrics.get("bb_mid")),
                [
                    "GP_NH60",
                    "SIGMA_BO",
                    (
                        "VOL2X_CONFIRM"
                        if buy_volume_rvol >= 2.0
                        else "RVOL_PASS"
                    ),
                    *(["VCP_PIVOT_BREAKOUT"] if metrics.get("vcp_pivot_breakout") else []),
                    *(
                        ["SIGMA_SQUEEZE_READY", "UG_ORANGE_CONTEXT"]
                        if squeeze_breakout_condition
                        else []
                    ),
                ],
                "ENTRY",
                "",
            ),
            (
                "UG_PULLBACK",
                "UG_BUY_PBB",
                pullback_condition,
                _safe_float(metrics.get("bb_lower")),
                _safe_float(metrics.get("bb_mid")),
                _safe_float(metrics.get("bb_lower")),
                ["SIGMA_PBB", "SIGMA_LOWER_BAND_SUPPORT", *band_reason_codes],
                "ENTRY",
                "UG_SELL_MR_SHORT_OR_PBS",
            ),
            (
                "UG_MEAN_REVERSION",
                "UG_BUY_MR_LONG",
                mr_long_condition,
                _safe_float(metrics.get("bb_lower")),
                _safe_float(metrics.get("bb_mid")),
                _safe_float(metrics.get("bb_lower")),
                ["SIGMA_MR_LONG", *band_reason_codes],
                "ENTRY_SHORT_SWING",
                "",
            ),
        ):
            if not condition:
                continue
            family_profile = self._family_source_profile(
                symbol, family, source_entry, peg_ready_map={}
            )
            legacy_visible = bool(family_profile.get("watch_allowed"))
            cooldown_blocked = self._is_ug_cooldown_blocked(
                symbol, code, active_cycles, signal_history
            )
            if code in {"UG_BUY_BREAKOUT", "UG_BUY_SQUEEZE_BREAKOUT"}:
                legacy_action = (
                    "BUY"
                    if traffic_light == "GREEN" and family_profile.get("buy_allowed")
                    else "WATCH"
                )
                artifact_base_action = "BUY" if traffic_light == "GREEN" else "WATCH"
            elif code == "UG_BUY_PBB":
                legacy_action = (
                    "BUY"
                    if traffic_light == "GREEN" and family_profile.get("buy_allowed")
                    else "WATCH"
                )
                artifact_base_action = "BUY" if traffic_light == "GREEN" else "WATCH"
            else:
                legacy_action = (
                    "BUY"
                    if traffic_light in {"GREEN", "ORANGE"}
                    and family_profile.get("buy_allowed")
                    else "WATCH"
                )
                artifact_base_action = (
                    "BUY" if traffic_light in {"GREEN", "ORANGE"} else "WATCH"
                )
            row_conviction = (
                _shift_grade(conviction, -1) if code == "UG_BUY_MR_LONG" else conviction
            )
            events.append(
                _annotate_buy_candidate_row(
                    _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="UG",
                    family=family,
                    signal_kind="EVENT",
                    signal_code=code,
                    action_type="WATCH" if cooldown_blocked else legacy_action,
                    conviction_grade=row_conviction,
                    screen_stage=screen_stage,
                    signal_score=validation_score,
                    gp_score=gp_score,
                    gp_health=_safe_text(dashboard_profile.get("gp_health")),
                    sigma_score=sigma_score,
                    sigma_health=_safe_text(dashboard_profile.get("sigma_health")),
                    traffic_light=traffic_light,
                    technical_light=traffic_light,
                    growth_score=_safe_float(dashboard_profile.get("growth_score")),
                    growth_health=_safe_text(dashboard_profile.get("growth_health")),
                    eps_health=_safe_text(dashboard_profile.get("eps_health")),
                    sales_health=_safe_text(dashboard_profile.get("sales_health")),
                    growth_data_status=_safe_text(
                        dashboard_profile.get("growth_data_status")
                    ),
                    dashboard_score=_safe_float(
                        dashboard_profile.get("dashboard_score")
                    ),
                    dashboard_light=_safe_text(
                        dashboard_profile.get("dashboard_light")
                    ),
                    dashboard_position_bias=_safe_text(
                        dashboard_profile.get("dashboard_position_bias")
                    ),
                    primary_source_style=family_profile.get("primary_style", ""),
                    source_fit_score=family_profile.get("fit_score"),
                    source_fit_label=family_profile.get("fit_label", ""),
                    cooldown_bucket=code,
                    cooldown_blocked=cooldown_blocked,
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=stop_level,
                    signal_phase=signal_phase,
                    reference_target_level=(
                        reference_target_level if code == "UG_BUY_PBB" else None
                    ),
                    reference_exit_signal=reference_exit_signal,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=reasons
                    + [f"VALIDATION_{int(round(validation_score))}"]
                    + list(dashboard_profile.get("reason_codes", []))
                    + list(family_profile.get("reason_codes", [])),
                    quality_flags=quality_flags,
                    ),
                    legacy_visible=legacy_visible,
                    artifact_action_type=(
                        "WATCH" if cooldown_blocked else artifact_base_action
                    ),
                )
            )
        return events

    def _ug_sell_events(
        self,
        *,
        symbol: str,
        metrics: Mapping[str, Any],
        cycle: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        family = _safe_text(cycle.get("family"))
        dashboard_profile = self._ug_dashboard_profile(metrics)
        state_code = self._ug_state_code(metrics, dashboard_profile)
        validation_score = _safe_float(dashboard_profile.get("validation_score")) or 0.0
        gp_score = _safe_float(dashboard_profile.get("gp_score"))
        sigma_score = _safe_float(dashboard_profile.get("sigma_score"))
        traffic_light = _safe_text(
            dashboard_profile.get("technical_light")
        ) or state_code.removeprefix("UG_STATE_")
        conviction = self._ug_conviction(metrics, state_code, dashboard_profile)
        screen_stage = str(
            metrics.get("screen_stage") or cycle.get("screen_stage") or "ACTIVE_CYCLE"
        )
        close = _safe_float(metrics.get("close")) or 0.0
        high = _safe_float(metrics.get("high")) or 0.0
        support_low = _safe_float(cycle.get("support_zone_low"))
        support_high = _safe_float(cycle.get("support_zone_high"))
        events: list[dict[str, Any]] = []
        primary_source_style = _safe_text(cycle.get("primary_source_style"))
        source_fit_label = _safe_text(cycle.get("source_fit_label"))
        source_fit_score = _safe_float(cycle.get("source_fit_score"))
        reference_exit_signal = _normalized_reference_exit_signal(engine="UG", family=family, reference_exit_signal=cycle.get("reference_exit_signal"))
        reference_exit_codes = set(_reference_exit_codes(reference_exit_signal))
        trim_count = max(_safe_int(cycle.get("trim_count")) or 0, 0)
        mr_short_allowed = (
            not reference_exit_codes or "UG_SELL_MR_SHORT" in reference_exit_codes
        )
        pbs_allowed = not reference_exit_codes or "UG_SELL_PBS" in reference_exit_codes
        exit_context_codes = []
        if primary_source_style:
            exit_context_codes.append(f"SOURCE_STYLE_{primary_source_style}")
        if source_fit_label:
            exit_context_codes.append(f"SOURCE_FIT_{source_fit_label}")

        band_reversion_profile = _band_reversion_from_metrics(metrics)
        refined_band_gate_present = _has_band_reversion_diagnostics(metrics)
        pbs_gate_pass = bool(
            metrics.get("ug_pbs_ready")
            and (
                not refined_band_gate_present
                or band_reversion_profile.get("pbs_ready")
            )
        )
        mr_short_gate_pass = bool(
            metrics.get("ug_mr_short_ready")
            and (
                not refined_band_gate_present
                or band_reversion_profile.get("mr_short_ready")
            )
        )
        band_reason_codes = list(
            dict.fromkeys(
                list(metrics.get("band_reversion_reason_codes", []))
                + list(band_reversion_profile.get("reason_codes", []))
            )
        )

        if support_low is not None and close <= support_low:
            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="UG",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="UG_SELL_BREAKDOWN",
                    action_type="EXIT",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    signal_score=validation_score,
                    gp_score=gp_score,
                    gp_health=_safe_text(dashboard_profile.get("gp_health")),
                    sigma_score=sigma_score,
                    sigma_health=_safe_text(dashboard_profile.get("sigma_health")),
                    traffic_light=traffic_light,
                    technical_light=traffic_light,
                    growth_score=_safe_float(dashboard_profile.get("growth_score")),
                    growth_health=_safe_text(dashboard_profile.get("growth_health")),
                    eps_health=_safe_text(dashboard_profile.get("eps_health")),
                    sales_health=_safe_text(dashboard_profile.get("sales_health")),
                    growth_data_status=_safe_text(
                        dashboard_profile.get("growth_data_status")
                    ),
                    dashboard_score=_safe_float(
                        dashboard_profile.get("dashboard_score")
                    ),
                    dashboard_light=_safe_text(
                        dashboard_profile.get("dashboard_light")
                    ),
                    dashboard_position_bias=_safe_text(
                        dashboard_profile.get("dashboard_position_bias")
                    ),
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=support_low,
                    signal_phase="EXIT",
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=[
                        "SIGMA_BREAKDOWN",
                        *list(dashboard_profile.get("reason_codes", [])),
                        *exit_context_codes,
                    ],
                    quality_flags=self._base_quality_flags(metrics),
                )
            )
        bb_mid = _safe_float(metrics.get("bb_mid"))
        if pbs_gate_pass and bb_mid is not None and pbs_allowed:
            reason_codes = [
                "SIGMA_PBS",
                *band_reason_codes,
                *list(dashboard_profile.get("reason_codes", [])),
                *exit_context_codes,
            ]
            if "UG_SELL_PBS" in reference_exit_codes:
                reason_codes.append("REFERENCE_EXIT_MATCH")
            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="UG",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="UG_SELL_PBS",
                    action_type="EXIT",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    signal_score=validation_score,
                    gp_score=gp_score,
                    gp_health=_safe_text(dashboard_profile.get("gp_health")),
                    sigma_score=sigma_score,
                    sigma_health=_safe_text(dashboard_profile.get("sigma_health")),
                    traffic_light=traffic_light,
                    technical_light=traffic_light,
                    growth_score=_safe_float(dashboard_profile.get("growth_score")),
                    growth_health=_safe_text(dashboard_profile.get("growth_health")),
                    eps_health=_safe_text(dashboard_profile.get("eps_health")),
                    sales_health=_safe_text(dashboard_profile.get("sales_health")),
                    growth_data_status=_safe_text(
                        dashboard_profile.get("growth_data_status")
                    ),
                    dashboard_score=_safe_float(
                        dashboard_profile.get("dashboard_score")
                    ),
                    dashboard_light=_safe_text(
                        dashboard_profile.get("dashboard_light")
                    ),
                    dashboard_position_bias=_safe_text(
                        dashboard_profile.get("dashboard_position_bias")
                    ),
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=bb_mid,
                    signal_phase="EXIT",
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=reason_codes,
                    quality_flags=self._base_quality_flags(metrics),
                )
            )
        if mr_short_gate_pass and mr_short_allowed and trim_count < 2:
            action_type = "TRIM"
            signal_phase = "TRIM"
            reason_codes = [
                "SIGMA_MR_SHORT",
                *band_reason_codes,
                *list(dashboard_profile.get("reason_codes", [])),
                *exit_context_codes,
            ]
            if "UG_SELL_MR_SHORT" in reference_exit_codes:
                reason_codes.append("REFERENCE_EXIT_MATCH")
            events.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="UG",
                    family=family,
                    family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                    signal_kind="EVENT",
                    signal_code="UG_SELL_MR_SHORT",
                    action_type=action_type,
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    signal_score=validation_score,
                    gp_score=gp_score,
                    gp_health=_safe_text(dashboard_profile.get("gp_health")),
                    sigma_score=sigma_score,
                    sigma_health=_safe_text(dashboard_profile.get("sigma_health")),
                    traffic_light=traffic_light,
                    technical_light=traffic_light,
                    growth_score=_safe_float(dashboard_profile.get("growth_score")),
                    growth_health=_safe_text(dashboard_profile.get("growth_health")),
                    eps_health=_safe_text(dashboard_profile.get("eps_health")),
                    sales_health=_safe_text(dashboard_profile.get("sales_health")),
                    growth_data_status=_safe_text(
                        dashboard_profile.get("growth_data_status")
                    ),
                    dashboard_score=_safe_float(
                        dashboard_profile.get("dashboard_score")
                    ),
                    dashboard_light=_safe_text(
                        dashboard_profile.get("dashboard_light")
                    ),
                    dashboard_position_bias=_safe_text(
                        dashboard_profile.get("dashboard_position_bias")
                    ),
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    stop_level=bb_mid,
                    signal_phase=signal_phase,
                    primary_source_style=primary_source_style,
                    source_fit_score=source_fit_score,
                    source_fit_label=source_fit_label,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=reason_codes,
                    quality_flags=self._base_quality_flags(metrics),
                )
            )
        return events

    def _trailing_profile(
        self,
        family: str,
        metrics: Mapping[str, Any],
        stop_level: float | None,
        *,
        primary_source_style: str = "",
    ) -> tuple[float | None, str]:
        family = _safe_text(family)
        style = _safe_text(primary_source_style).upper()
        fast_character = _is_fast_character(metrics.get("stock_character"))
        level: float | None
        mode: str
        if family == "TF_MOMENTUM":
            if style in {"VOLATILITY", "LEADERSHIP"} or fast_character:
                level, mode = (
                    _pick_latest(
                        _safe_float(metrics.get("ema10")),
                        _safe_float(metrics.get("donchian_low20")),
                        stop_level,
                    ),
                    "MOMENTUM_RATCHET",
                )
                return self._with_atr_trailing_candidate(metrics, level, stop_level, mode)
            level, mode = (
                _pick_latest(
                    _safe_float(metrics.get("fast_ref")),
                    _safe_float(metrics.get("donchian_low20")),
                    stop_level,
                ),
                "MOMENTUM_SWING",
            )
            return self._with_atr_trailing_candidate(metrics, level, stop_level, mode)
        if family == "TF_PEG":
            level, mode = (
                _pick_latest(
                    _safe_float(metrics.get("ema10")),
                    _safe_float(metrics.get("ema20")),
                    stop_level,
                ),
                "PEG_GAP_HOLD",
            )
            return self._with_atr_trailing_candidate(metrics, level, stop_level, mode)
        if family == "TF_REGULAR_PULLBACK":
            if style in {"PULLBACK", "STRUCTURE"} and not fast_character:
                level, mode = (
                    _pick_latest(
                        _safe_float(metrics.get("mid_ref")),
                        _safe_float(metrics.get("slow_ref")),
                        stop_level,
                    ),
                    "PULLBACK_CLASSIC",
                )
                return self._with_atr_trailing_candidate(metrics, level, stop_level, mode)
            level, mode = (
                _pick_latest(
                    _safe_float(metrics.get("fast_ref")),
                    _safe_float(metrics.get("mid_ref")),
                    stop_level,
                ),
                "PULLBACK_FAST",
            )
            return self._with_atr_trailing_candidate(metrics, level, stop_level, mode)
        if family == "TF_BREAKOUT":
            if style in {"BREAKOUT", "LEADERSHIP", "TREND"} and fast_character:
                level, mode = (
                    _pick_latest(
                        _safe_float(metrics.get("fast_ref")),
                        _safe_float(metrics.get("mid_ref")),
                        stop_level,
                    ),
                    "BREAKOUT_FAST",
                )
                return self._with_atr_trailing_candidate(metrics, level, stop_level, mode)
            level, mode = (
                _pick_latest(
                    _safe_float(metrics.get("mid_ref")),
                    _safe_float(metrics.get("slow_ref")),
                    stop_level,
                ),
                "BREAKOUT_CLASSIC",
            )
            return self._with_atr_trailing_candidate(metrics, level, stop_level, mode)
        if family.startswith("TF_"):
            level, mode = (
                _pick_latest(
                    _safe_float(metrics.get("fast_ref")),
                    _safe_float(metrics.get("mid_ref")),
                    stop_level,
                ),
                "TREND_GENERIC",
            )
            return self._with_atr_trailing_candidate(metrics, level, stop_level, mode)
        return stop_level, ""

    def _with_atr_trailing_candidate(
        self,
        metrics: Mapping[str, Any],
        level: float | None,
        stop_level: float | None,
        mode: str,
    ) -> tuple[float | None, str]:
        daily_frame = (
            metrics.get("daily")
            if isinstance(metrics.get("daily"), pd.DataFrame)
            else pd.DataFrame()
        )
        exit_profile = _signal_patterns.score_exit_pressure(
            daily_frame,
            metrics,
            {
                "trailing_level": level,
                "protected_stop_level": stop_level,
            },
        )
        effective_level = _safe_float(exit_profile.get("effective_trailing_level"))
        chandelier_level = _safe_float(exit_profile.get("chandelier_long_stop"))
        if (
            effective_level is not None
            and chandelier_level is not None
            and (level is None or effective_level > level)
        ):
            return effective_level, f"{mode}_ATR_CHANDELIER"
        return level, mode

    def _compute_trailing_level(
        self,
        family: str,
        metrics: Mapping[str, Any],
        stop_level: float | None,
        *,
        primary_source_style: str = "",
    ) -> float | None:
        level, _ = self._trailing_profile(
            family, metrics, stop_level, primary_source_style=primary_source_style
        )
        return level

    def _cycle_tp_plan(
        self, family: str, metrics: Mapping[str, Any], *, primary_source_style: str = ""
    ) -> str:
        family = _safe_text(family)
        style = _safe_text(primary_source_style).upper()
        fast_character = _is_fast_character(metrics.get("stock_character"))
        if family == "TF_MOMENTUM":
            if style in {"VOLATILITY", "LEADERSHIP"} or fast_character:
                return "TP_MOMENTUM_1P5R_2P5R"
            return "TP_MOMENTUM_2R_3R"
        if family == "TF_PEG":
            return "TP_PEG_1P5R_3R"
        if family == "TF_BREAKOUT":
            if style in {"BREAKOUT", "LEADERSHIP"} or fast_character:
                return "TP_BREAKOUT_2R_3R"
            return "TP_BREAKOUT_2R_4R"
        if family == "TF_REGULAR_PULLBACK":
            if style in {"PULLBACK", "STRUCTURE"} and not fast_character:
                return "TP_PULLBACK_2P5R_4R"
            return "TP_PULLBACK_2R_3R"
        if family.startswith("TF_"):
            return "TP_RMULTIPLE_2R_3R"
        return ""

    def _tp_multipliers(self, tp_plan: str) -> tuple[float, float]:
        normalized = _safe_text(tp_plan).upper()
        mapping = {
            "TP_MOMENTUM_1P5R_2P5R": (1.5, 2.5),
            "TP_MOMENTUM_2R_3R": (2.0, 3.0),
            "TP_PEG_1P5R_3R": (1.5, 3.0),
            "TP_BREAKOUT_2R_3R": (2.0, 3.0),
            "TP_BREAKOUT_2R_4R": (2.0, 4.0),
            "TP_PULLBACK_2P5R_4R": (2.5, 4.0),
            "TP_PULLBACK_2R_3R": (2.0, 3.0),
            "TP_RMULTIPLE_2R_3R": (2.0, 3.0),
        }
        return mapping.get(normalized, (2.0, 3.0))

    def _apply_break_even_floor(
        self,
        level: float | None,
        *,
        break_even_level: float | None,
        risk_free_armed: bool,
    ) -> float | None:
        if not risk_free_armed:
            return level
        if break_even_level is None:
            return level
        if level is None:
            return break_even_level
        return max(level, break_even_level)

    def _update_cycles(
        self,
        events: list[dict[str, Any]],
        active_cycles: dict[tuple[str, str, str], dict[str, Any]],
        metrics_map: Mapping[str, Mapping[str, Any]],
        peg_context_map: Mapping[str, Mapping[str, Any]] | None = None,
        *,
        scope: str = _SCREENED_SCOPE,
    ) -> dict[tuple[str, str, str], dict[str, Any]]:
        peg_context_map = peg_context_map or {}
        normalized_scope = _normalize_scope(scope)
        updated = {
            key: {**dict(value), "scope": _normalize_scope(value.get("scope"), default=normalized_scope)}
            for key, value in active_cycles.items()
        }
        buy_codes = {
            "TF_BUY_REGULAR",
            "TF_BUY_BREAKOUT",
            "TF_BUY_PEG_PULLBACK",
            "TF_BUY_PEG_REBREAK",
            "TF_BUY_MOMENTUM",
            "TF_ADDON_PYRAMID",
            *list(_UG_BUY_CODES),
        }

        close_codes = set(_FINAL_CLOSE_SIGNAL_CODES)
        for row in events:
            symbol = _safe_text(row.get("symbol")).upper()
            engine = _safe_text(row.get("engine"))
            family = _safe_text(row.get("family"))
            key = (engine, family, symbol)
            metrics = metrics_map.get(symbol, {})
            if row.get("signal_code") in buy_codes and row.get("action_type") == "BUY":
                cycle = updated.get(key)
                if cycle is None:
                    entry_price = _safe_float(metrics.get("close"))
                    stop_level = (
                        _safe_float(row.get("stop_level"))
                        or _safe_float(metrics.get("trend_zone_low"))
                        or _safe_float(metrics.get("bb_zone_low"))
                    )
                    primary_source_style = _safe_text(row.get("primary_source_style"))
                    if engine == "UG":
                        support_low, support_high, stop_level = _ug_cycle_zone(
                            family,
                            metrics,
                            support_low=_safe_float(row.get("support_zone_low")),
                            support_high=_safe_float(row.get("support_zone_high")),
                            stop_level=stop_level,
                        )
                        cycle = {
                            "family_cycle_id": f"{engine}:{family}:{symbol}:{row.get('signal_date')}",
                            "scope": normalized_scope,
                            "engine": engine,
                            "family": family,
                            "symbol": symbol,
                            "opened_on": row.get("signal_date"),
                            "last_signal_date": row.get("signal_date"),
                            "buy_signal_code": row.get("signal_code"),
                            "screen_stage": row.get("screen_stage"),
                            "entry_price": entry_price,
                            "support_zone_low": support_low,
                            "support_zone_high": support_high,
                            "stop_level": stop_level,
                            "source_tags": row.get("source_tags", []),
                            "primary_source_style": primary_source_style,
                            "source_fit_score": row.get("source_fit_score"),
                            "source_fit_label": row.get("source_fit_label"),
                            "reference_exit_signal": _normalized_reference_exit_signal(
                                engine=engine,
                                family=family,
                                reference_exit_signal=row.get("reference_exit_signal"),
                            ),
                            "trim_count": 0,
                            "last_trim_date": None,
                            "partial_exit_active": False,
                            "base_position_units": 1.0,
                            "current_position_units": 1.0,
                        }
                    else:
                        break_even_level = entry_price
                        tp_plan = self._cycle_tp_plan(
                            family, metrics, primary_source_style=primary_source_style
                        )
                        tp1_multiple, tp2_multiple = self._tp_multipliers(tp_plan)
                        trailing_level, trailing_mode = self._trailing_profile(
                            family,
                            metrics,
                            stop_level,
                            primary_source_style=primary_source_style,
                        )
                        trailing_level = self._apply_break_even_floor(
                            trailing_level,
                            break_even_level=break_even_level,
                            risk_free_armed=False,
                        )
                        risk_unit = None
                        if (
                            entry_price is not None
                            and stop_level is not None
                            and entry_price > stop_level
                        ):
                            risk_unit = entry_price - stop_level
                        tp1_level = (
                            entry_price + (tp1_multiple * risk_unit)
                            if entry_price is not None and risk_unit is not None
                            else None
                        )
                        tp2_level = (
                            entry_price + (tp2_multiple * risk_unit)
                            if entry_price is not None and risk_unit is not None
                            else None
                        )
                        cycle = {
                            "family_cycle_id": f"{engine}:{family}:{symbol}:{row.get('signal_date')}",
                            "scope": normalized_scope,
                            "engine": engine,
                            "family": family,
                            "symbol": symbol,
                            "opened_on": row.get("signal_date"),
                            "last_signal_date": row.get("signal_date"),
                            "buy_signal_code": row.get("signal_code"),
                            "screen_stage": row.get("screen_stage"),
                            "entry_price": entry_price,
                            "break_even_level": break_even_level,
                            "support_zone_low": row.get("support_zone_low"),
                            "support_zone_high": row.get("support_zone_high"),
                            "stop_level": stop_level,
                            "trailing_level": trailing_level,
                            "trailing_mode": trailing_mode,
                            "tp1_level": tp1_level,
                            "tp2_level": tp2_level,
                            "tp_plan": tp_plan,
                            "tp1_hit": False,
                            "tp2_hit": False,
                            "trim_count": 0,
                            "last_trim_date": None,
                            "partial_exit_active": False,
                            "risk_free_armed": False,
                            "add_on_count": 0,
                            "add_on_slot": 0,
                            "max_add_ons": 2,
                            "tranche_pct": None,
                            "next_addon_allowed": False,
                            "last_addon_date": None,
                            "pyramid_state": "INITIAL",
                            "protected_stop_level": trailing_level,
                            "base_position_units": 1.0,
                            "current_position_units": 1.0,
                            "blended_entry_price": entry_price,
                            "last_trailing_confirmed_level": trailing_level,
                            "last_protected_stop_level": trailing_level,
                            "last_pyramid_reference_level": trailing_level,
                            "source_tags": row.get("source_tags", []),
                            "primary_source_style": primary_source_style,
                            "source_fit_score": row.get("source_fit_score"),
                            "source_fit_label": row.get("source_fit_label"),
                        }
                    updated[key] = cycle
                    _record_position_effect(
                        row,
                        cycle_effect="OPEN",
                        before=0.0,
                        after=_safe_float(cycle.get("current_position_units")) or 1.0,
                    )
                else:
                    cycle["last_signal_date"] = row.get("signal_date")
                    cycle["scope"] = _normalize_scope(
                        cycle.get("scope"), default=normalized_scope
                    )
                    primary_source_style = _safe_text(
                        cycle.get("primary_source_style")
                    ) or _safe_text(row.get("primary_source_style"))
                    if engine == "UG":
                        support_low, support_high, stop_level = _ug_cycle_zone(
                            family,
                            metrics,
                            support_low=_safe_float(cycle.get("support_zone_low")),
                            support_high=_safe_float(cycle.get("support_zone_high")),
                            stop_level=_safe_float(cycle.get("stop_level")),
                        )
                        cycle["support_zone_low"] = support_low
                        cycle["support_zone_high"] = support_high
                        cycle["stop_level"] = stop_level
                        cycle["reference_exit_signal"] = _normalized_reference_exit_signal(
                            engine=engine,
                            family=family,
                            reference_exit_signal=_safe_text(
                                cycle.get("reference_exit_signal")
                            )
                            or row.get("reference_exit_signal"),
                        )
                    else:
                        cycle["tp_plan"] = _safe_text(
                            cycle.get("tp_plan")
                        ) or self._cycle_tp_plan(
                            family, metrics, primary_source_style=primary_source_style
                        )
                        cycle["break_even_level"] = _safe_float(
                            cycle.get("break_even_level")
                        ) or _safe_float(cycle.get("entry_price"))
                        trailing_level, trailing_mode = self._trailing_profile(
                            family,
                            metrics,
                            _safe_float(cycle.get("stop_level")),
                            primary_source_style=primary_source_style,
                        )
                        cycle["trailing_level"] = self._apply_break_even_floor(
                            trailing_level,
                            break_even_level=_safe_float(cycle.get("break_even_level")),
                            risk_free_armed=_safe_bool(cycle.get("risk_free_armed")),
                        )
                        cycle["trailing_level"] = _never_down_level(
                            _safe_float(cycle.get("trailing_level")),
                            _safe_float(cycle.get("last_trailing_confirmed_level")),
                        )
                        cycle["trailing_mode"] = trailing_mode
                        if row.get("signal_code") == "TF_ADDON_PYRAMID":
                            next_slot = self._trend_addon_slot(cycle)
                            if next_slot is not None:
                                position_before = _safe_float(
                                    cycle.get("current_position_units")
                                )
                                add_on_units = (
                                    self._trend_addon_tranche_pct(next_slot) or 0.0
                                )
                                current_units = _safe_float(
                                    cycle.get("current_position_units")
                                ) or self._trend_total_position_units(
                                    max(_safe_int(cycle.get("add_on_count")) or 0, 0),
                                    base_position_units=_safe_float(
                                        cycle.get("base_position_units")
                                    )
                                    or 1.0,
                                )
                                blended_entry_price = self._trend_weighted_entry(
                                    current_units=current_units,
                                    current_entry=_safe_float(
                                        cycle.get("blended_entry_price")
                                    )
                                    or _safe_float(cycle.get("entry_price")),
                                    add_on_units=add_on_units,
                                    add_on_price=_safe_float(
                                        row.get("blended_entry_price")
                                    )
                                    or _safe_float(metrics.get("close")),
                                )
                                cycle["add_on_count"] = next_slot
                                cycle["add_on_slot"] = next_slot
                                cycle["max_add_ons"] = max(
                                    _safe_int(cycle.get("max_add_ons")) or 2, 2
                                )
                                cycle["tranche_pct"] = add_on_units
                                cycle["last_addon_date"] = row.get("signal_date")
                                cycle["pyramid_state"] = f"SLOT{next_slot}_FILLED"
                                cycle["base_position_units"] = (
                                    _safe_float(cycle.get("base_position_units")) or 1.0
                                )
                                cycle["current_position_units"] = (
                                    current_units or 0.0
                                ) + add_on_units
                                cycle["blended_entry_price"] = blended_entry_price
                                cycle["break_even_level"] = _pick_latest(
                                    max(
                                        value
                                        for value in [
                                            _safe_float(cycle.get("break_even_level")),
                                            blended_entry_price,
                                        ]
                                        if value is not None
                                    )
                                    if _safe_float(cycle.get("break_even_level"))
                                    is not None
                                    or blended_entry_price is not None
                                    else None
                                )
                                cycle["protected_stop_level"] = (
                                    self._trend_protected_stop_level(cycle)
                                )
                                cycle["protected_stop_level"] = _never_down_level(
                                    _safe_float(cycle.get("protected_stop_level")),
                                    _safe_float(
                                        cycle.get("last_protected_stop_level")
                                    ),
                                )
                                cycle["last_trailing_confirmed_level"] = _safe_float(
                                    cycle.get("trailing_level")
                                )
                                cycle["last_protected_stop_level"] = _safe_float(
                                    cycle.get("protected_stop_level")
                                )
                                cycle["last_pyramid_reference_level"] = _safe_float(
                                    cycle.get("protected_stop_level")
                                )
                                _record_position_effect(
                                    row,
                                    cycle_effect="ADD",
                                    before=position_before,
                                    after=_safe_float(
                                        cycle.get("current_position_units")
                                    ),
                                )
                row["family_cycle_id"] = cycle["family_cycle_id"]
                if engine == "TREND":
                    row["trailing_mode"] = cycle.get("trailing_mode")
                    row["tp_plan"] = cycle.get("tp_plan")
                    row["trim_count"] = cycle.get("trim_count")
                    row["risk_free_armed"] = cycle.get("risk_free_armed")
                    row["break_even_level"] = cycle.get("break_even_level")
                    row["tp1_level"] = cycle.get("tp1_level")
                    row["tp2_level"] = cycle.get("tp2_level")
                    row["protected_stop_level"] = cycle.get("protected_stop_level")
                    row["add_on_count"] = cycle.get("add_on_count")
                    row["add_on_slot"] = cycle.get("add_on_slot")
                    row["max_add_ons"] = cycle.get("max_add_ons")
                    row["tranche_pct"] = cycle.get("tranche_pct")
                    row["next_addon_allowed"] = cycle.get("next_addon_allowed")
                    row["pyramid_state"] = cycle.get("pyramid_state")
                    row["base_position_units"] = cycle.get("base_position_units")
                    row["current_position_units"] = cycle.get("current_position_units")
                    row["blended_entry_price"] = cycle.get("blended_entry_price")
                    row["last_trailing_confirmed_level"] = cycle.get(
                        "last_trailing_confirmed_level"
                    )
                    row["last_protected_stop_level"] = cycle.get(
                        "last_protected_stop_level"
                    )
                    row["last_pyramid_reference_level"] = cycle.get(
                        "last_pyramid_reference_level"
                    )
            elif row.get("signal_code") == "UG_SELL_MR_SHORT":
                cycle = updated.get(key)
                if cycle:
                    row["family_cycle_id"] = cycle.get("family_cycle_id", "")
                    if row.get("action_type") == "TRIM":
                        next_trim_count = min(
                            max(_safe_int(cycle.get("trim_count")) or 0, 0) + 1,
                            2,
                        )
                        trim_fraction = 0.5 if next_trim_count == 1 else 0.25
                        cycle["trim_count"] = next_trim_count
                        cycle["partial_exit_active"] = True
                        cycle["last_trim_date"] = row.get("signal_date")
                        base_position_units = (
                            _safe_float(cycle.get("base_position_units")) or 1.0
                        )
                        current_position_units = _safe_float(
                            cycle.get("current_position_units")
                        )
                        if current_position_units is None:
                            current_position_units = base_position_units
                        position_before = current_position_units
                        cycle["base_position_units"] = base_position_units
                        cycle["current_position_units"] = max(
                            current_position_units - (base_position_units * trim_fraction),
                            0.0,
                        )
                        row["trim_count"] = cycle.get("trim_count")
                        row["base_position_units"] = cycle.get("base_position_units")
                        row["current_position_units"] = cycle.get("current_position_units")
                        _record_position_effect(
                            row,
                            cycle_effect="TRIM",
                            before=position_before,
                            after=_safe_float(cycle.get("current_position_units")),
                        )
                    else:
                        updated.pop(key, None)
            elif row.get("signal_code") in {"TF_SELL_TP1", "TF_SELL_TP2"}:
                cycle = updated.get(key)
                if cycle:
                    position_before = _safe_float(cycle.get("current_position_units"))
                    if position_before is None:
                        position_before = _safe_float(cycle.get("base_position_units")) or 1.0
                    position_after = max(position_before * 0.5, 0.0)
                    cycle["current_position_units"] = position_after
                    if row.get("signal_code") == "TF_SELL_TP1":
                        cycle["tp1_hit"] = True
                        cycle["trim_count"] = max(
                            _safe_int(cycle.get("trim_count")) or 0, 1
                        )
                        cycle["partial_exit_active"] = True
                        cycle["risk_free_armed"] = True
                        cycle["last_trim_date"] = row.get("signal_date")
                    if row.get("signal_code") == "TF_SELL_TP2":
                        cycle["tp2_hit"] = True
                        cycle["trim_count"] = max(
                            _safe_int(cycle.get("trim_count")) or 0, 2
                        )
                        cycle["partial_exit_active"] = True
                        cycle["last_trim_date"] = row.get("signal_date")
                    cycle["break_even_level"] = _safe_float(
                        cycle.get("break_even_level")
                    ) or _safe_float(cycle.get("entry_price"))
                    row["family_cycle_id"] = cycle.get("family_cycle_id", "")
                    row["trim_count"] = cycle.get("trim_count")
                    row["risk_free_armed"] = cycle.get("risk_free_armed")
                    row["break_even_level"] = cycle.get("break_even_level")
                    row["tp_plan"] = cycle.get("tp_plan")
                    cycle["protected_stop_level"] = self._trend_protected_stop_level(
                        cycle
                    )
                    cycle["protected_stop_level"] = _never_down_level(
                        _safe_float(cycle.get("protected_stop_level")),
                        _safe_float(cycle.get("last_protected_stop_level")),
                    )
                    row["base_position_units"] = cycle.get("base_position_units")
                    row["current_position_units"] = cycle.get("current_position_units")
                    row["protected_stop_level"] = cycle.get("protected_stop_level")
                    _record_position_effect(
                        row,
                        cycle_effect="TRIM",
                        before=position_before,
                        after=position_after,
                    )
            elif row.get("signal_code") in close_codes:
                cycle = updated.get(key)
                if cycle:
                    row["family_cycle_id"] = cycle.get("family_cycle_id", "")
                    position_before = _safe_float(cycle.get("current_position_units"))
                    if position_before is None:
                        position_before = _safe_float(cycle.get("base_position_units"))
                    _record_position_effect(
                        row,
                        cycle_effect="CLOSE",
                        before=position_before,
                        after=0.0 if position_before is not None else None,
                    )
                    updated.pop(key, None)

        for key, cycle in list(updated.items()):
            symbol = _safe_text(cycle.get("symbol")).upper()
            metrics = metrics_map.get(symbol, {})
            family = _safe_text(cycle.get("family"))
            primary_source_style = _safe_text(cycle.get("primary_source_style"))
            if _safe_text(cycle.get("engine")) == "UG":
                support_low, support_high, stop_level = _ug_cycle_zone(
                    family,
                    metrics,
                    support_low=_safe_float(cycle.get("support_zone_low")),
                    support_high=_safe_float(cycle.get("support_zone_high")),
                    stop_level=_safe_float(cycle.get("stop_level")),
                )
                cycle["support_zone_low"] = support_low
                cycle["support_zone_high"] = support_high
                cycle["stop_level"] = stop_level
                continue
            cycle["tp_plan"] = _safe_text(cycle.get("tp_plan")) or self._cycle_tp_plan(
                family, metrics, primary_source_style=primary_source_style
            )
            entry_price = _safe_float(cycle.get("entry_price"))
            stop_level = _safe_float(cycle.get("stop_level"))
            cycle["base_position_units"] = (
                _safe_float(cycle.get("base_position_units")) or 1.0
            )
            cycle["add_on_count"] = max(_safe_int(cycle.get("add_on_count")) or 0, 0)
            cycle["current_position_units"] = _safe_float(
                cycle.get("current_position_units")
            ) or self._trend_total_position_units(
                cycle["add_on_count"],
                base_position_units=_safe_float(cycle.get("base_position_units"))
                or 1.0,
            )
            cycle["blended_entry_price"] = (
                _safe_float(cycle.get("blended_entry_price")) or entry_price
            )
            cycle["break_even_level"] = _pick_latest(
                max(
                    value
                    for value in [
                        _safe_float(cycle.get("break_even_level")),
                        _safe_float(cycle.get("blended_entry_price")),
                        entry_price,
                    ]
                    if value is not None
                )
                if _safe_float(cycle.get("break_even_level")) is not None
                or _safe_float(cycle.get("blended_entry_price")) is not None
                or entry_price is not None
                else None
            )
            addon_context = self._trend_addon_context(
                family=family,
                metrics=metrics,
                cycle=cycle,
                peg_context=peg_context_map.get(symbol, {}),
            )
            if (
                entry_price is not None
                and stop_level is not None
                and entry_price > stop_level
            ):
                risk_unit = entry_price - stop_level
                tp1_multiple, tp2_multiple = self._tp_multipliers(
                    _safe_text(cycle.get("tp_plan"))
                )
                cycle["tp1_level"] = entry_price + (tp1_multiple * risk_unit)
                cycle["tp2_level"] = entry_price + (tp2_multiple * risk_unit)
            trailing_level, trailing_mode = self._trailing_profile(
                family,
                metrics,
                stop_level,
                primary_source_style=primary_source_style,
            )
            cycle["trailing_level"] = self._apply_break_even_floor(
                trailing_level,
                break_even_level=_safe_float(cycle.get("break_even_level")),
                risk_free_armed=_safe_bool(cycle.get("risk_free_armed")),
            )
            cycle["trailing_level"] = _never_down_level(
                _safe_float(cycle.get("trailing_level")),
                _safe_float(cycle.get("last_trailing_confirmed_level")),
            )
            cycle["trailing_mode"] = trailing_mode
            cycle["max_add_ons"] = max(_safe_int(cycle.get("max_add_ons")) or 2, 2)
            cycle["add_on_slot"] = max(_safe_int(cycle.get("add_on_slot")) or 0, 0)
            cycle["protected_stop_level"] = self._trend_protected_stop_level(cycle)
            cycle["protected_stop_level"] = _never_down_level(
                _safe_float(cycle.get("protected_stop_level")),
                _safe_float(cycle.get("last_protected_stop_level")),
            )
            cycle["next_addon_allowed"] = bool(addon_context.get("ready"))
            cycle["addon_block_reason"] = addon_context.get("block_reason")
            cycle["trailing_ratcheted"] = addon_context.get("trailing_ratcheted")
            cycle["blended_entry_protected"] = addon_context.get(
                "blended_entry_protected"
            )
            cycle["addon_reason_codes"] = addon_context.get("reason_codes", [])
            cycle["addon_next_slot"] = addon_context.get("next_slot")
            cycle["addon_tranche_pct"] = addon_context.get("tranche_pct")
            if _safe_text(cycle.get("pyramid_state")) == "":
                cycle["pyramid_state"] = "INITIAL"
            if cycle["add_on_count"] >= cycle["max_add_ons"]:
                cycle["pyramid_state"] = "MAXED_OUT"
            cycle["last_trailing_confirmed_level"] = _safe_float(
                cycle.get("trailing_level")
            )
            cycle["last_protected_stop_level"] = _safe_float(
                cycle.get("protected_stop_level")
            )
            cycle["last_pyramid_reference_level"] = _pick_latest(
                _safe_float(cycle.get("last_pyramid_reference_level")),
                _safe_float(cycle.get("protected_stop_level")),
            )
        return updated

    def _build_level_state_rows(
        self,
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
        metrics_map: Mapping[str, Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for cycle in active_cycles.values():
            engine = _safe_text(cycle.get("engine"))
            if engine not in {"TREND", "UG"}:
                continue
            if engine == "UG":
                continue
            symbol = _safe_text(cycle.get("symbol")).upper()
            metrics = metrics_map.get(symbol, {})
            conviction = self._trend_conviction(metrics) if metrics else "C"
            screen_stage = str(
                metrics.get("screen_stage")
                or cycle.get("screen_stage")
                or "ACTIVE_CYCLE"
            )
            trailing_level = _safe_float(cycle.get("trailing_level"))
            tp1_level = _safe_float(cycle.get("tp1_level"))
            tp2_level = _safe_float(cycle.get("tp2_level"))
            primary_source_style = _safe_text(cycle.get("primary_source_style"))
            source_fit_label = _safe_text(cycle.get("source_fit_label"))
            source_fit_score = _safe_float(cycle.get("source_fit_score"))
            trailing_mode = _safe_text(cycle.get("trailing_mode"))
            tp_plan = _safe_text(cycle.get("tp_plan"))
            trim_count = _safe_int(cycle.get("trim_count"))
            risk_free_armed = _safe_bool(cycle.get("risk_free_armed"))
            break_even_level = _safe_float(cycle.get("break_even_level"))
            protected_stop_level = _safe_float(cycle.get("protected_stop_level"))
            add_on_count = max(_safe_int(cycle.get("add_on_count")) or 0, 0)
            add_on_slot = max(_safe_int(cycle.get("add_on_slot")) or 0, 0)
            max_add_ons = max(_safe_int(cycle.get("max_add_ons")) or 2, 2)
            tranche_pct = _safe_float(cycle.get("tranche_pct"))
            next_addon_allowed = _safe_bool(cycle.get("next_addon_allowed"))
            pyramid_state = _safe_text(cycle.get("pyramid_state"))
            base_position_units = _safe_float(cycle.get("base_position_units"))
            current_position_units = _safe_float(cycle.get("current_position_units"))
            blended_entry_price = _safe_float(cycle.get("blended_entry_price"))
            last_trailing_confirmed_level = _safe_float(
                cycle.get("last_trailing_confirmed_level")
            )
            last_protected_stop_level = _safe_float(
                cycle.get("last_protected_stop_level")
            )
            last_pyramid_reference_level = _safe_float(
                cycle.get("last_pyramid_reference_level")
            )
            level_specs = (
                ("TF_TRAILING_LEVEL", trailing_level),
                (
                    "TF_PROTECTED_STOP_LEVEL",
                    (
                        protected_stop_level
                        if (risk_free_armed or add_on_count > 0)
                        else None
                    ),
                ),
                ("TF_BREAKEVEN_LEVEL", break_even_level if risk_free_armed else None),
                ("TF_TP1_LEVEL", tp1_level),
                ("TF_TP2_LEVEL", tp2_level),
            )
            for code, level in level_specs:
                if level is None:
                    continue
                prefix = "TF_"
                reason_codes = [code.removeprefix(prefix)]
                if primary_source_style:
                    reason_codes.append(f"SOURCE_STYLE_{primary_source_style}")
                if source_fit_label:
                    reason_codes.append(f"SOURCE_FIT_{source_fit_label}")
                if code.endswith("TRAILING_LEVEL") and trailing_mode:
                    reason_codes.append(f"TRAIL_MODE_{trailing_mode}")
                if code.endswith("BREAKEVEN_LEVEL"):
                    reason_codes.extend(["RISK_FREE_ARMED", "BREAK_EVEN_FLOOR"])
                if code.endswith("PROTECTED_STOP_LEVEL"):
                    reason_codes.append("PROTECTED_STOP_ACTIVE")
                if code.endswith("TP1_LEVEL") or code.endswith("TP2_LEVEL"):
                    if tp_plan:
                        reason_codes.append(tp_plan)
                if trim_count:
                    reason_codes.append(f"TRIM_COUNT_{trim_count}")
                if add_on_count:
                    reason_codes.append(f"ADDON_COUNT_{add_on_count}")
                if add_on_slot:
                    reason_codes.append(f"ADDON_SLOT_{add_on_slot}")
                if pyramid_state:
                    reason_codes.append(f"PYRAMID_STATE_{pyramid_state}")
                rows.append(
                    _build_signal_row(
                        signal_date=metrics.get("date") or self.as_of_date,
                        symbol=symbol,
                        market=self.market,
                        engine=engine,
                        family=_safe_text(cycle.get("family")),
                        family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                        signal_kind="STATE",
                        signal_code=code,
                        action_type="STATE",
                        conviction_grade=conviction,
                        screen_stage=screen_stage,
                        primary_source_style=primary_source_style,
                        source_fit_score=source_fit_score,
                        source_fit_label=source_fit_label,
                        support_zone_low=_safe_float(cycle.get("support_zone_low")),
                        support_zone_high=_safe_float(cycle.get("support_zone_high")),
                        stop_level=trailing_level,
                        break_even_level=break_even_level,
                        blended_entry_price=blended_entry_price,
                        tp1_level=tp1_level,
                        tp2_level=tp2_level,
                        trailing_mode=trailing_mode,
                        tp_plan=tp_plan,
                        trim_count=trim_count,
                        risk_free_armed=risk_free_armed,
                        protected_stop_level=protected_stop_level,
                        add_on_count=add_on_count,
                        add_on_slot=add_on_slot,
                        max_add_ons=max_add_ons,
                        tranche_pct=tranche_pct,
                        next_addon_allowed=next_addon_allowed,
                        pyramid_state=pyramid_state,
                        base_position_units=base_position_units,
                        current_position_units=current_position_units,
                        last_trailing_confirmed_level=last_trailing_confirmed_level,
                        last_protected_stop_level=last_protected_stop_level,
                        last_pyramid_reference_level=last_pyramid_reference_level,
                        source_tags=cycle.get("source_tags", []),
                        reason_codes=reason_codes,
                        quality_flags=self._base_quality_flags(metrics),
                    )
                )
        return rows

    def _build_trend_addon_state_rows(
        self,
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
        metrics_map: Mapping[str, Mapping[str, Any]],
        peg_context_map: Mapping[str, Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for cycle in active_cycles.values():
            if _safe_text(cycle.get("engine")) != "TREND":
                continue
            symbol = _safe_text(cycle.get("symbol")).upper()
            family = _safe_text(cycle.get("family"))
            metrics = metrics_map.get(symbol, {})
            if not _safe_bool(cycle.get("next_addon_allowed")):
                continue
            conviction = self._trend_conviction(metrics) if metrics else "C"
            screen_stage = str(
                metrics.get("screen_stage")
                or cycle.get("screen_stage")
                or "ACTIVE_CYCLE"
            )
            add_on_count = max(_safe_int(cycle.get("add_on_count")) or 0, 0)
            next_slot = _safe_int(cycle.get("addon_next_slot"))
            tranche_pct = _safe_float(cycle.get("addon_tranche_pct"))
            common_reason_codes = list(_to_list(cycle.get("addon_reason_codes")))
            common_reason_codes.append(f"ADDON_COUNT_{add_on_count}")
            slot_code = (
                "TF_ADDON_SLOT1_READY"
                if next_slot == 1
                else "TF_ADDON_SLOT2_READY" if next_slot == 2 else ""
            )
            for code in ("TF_ADDON_READY", slot_code):
                if not code:
                    continue
                rows.append(
                    _build_signal_row(
                        signal_date=metrics.get("date") or self.as_of_date,
                        symbol=symbol,
                        market=self.market,
                        engine="TREND",
                        family=family,
                        family_cycle_id=_safe_text(cycle.get("family_cycle_id")),
                        signal_kind="STATE",
                        signal_code=code,
                        action_type="STATE",
                        conviction_grade=conviction,
                        screen_stage=screen_stage,
                        primary_source_style=_safe_text(
                            cycle.get("primary_source_style")
                        ),
                        source_fit_score=_safe_float(cycle.get("source_fit_score")),
                        source_fit_label=_safe_text(cycle.get("source_fit_label")),
                        support_zone_low=_safe_float(cycle.get("support_zone_low")),
                        support_zone_high=_safe_float(cycle.get("support_zone_high")),
                        stop_level=_safe_float(cycle.get("trailing_level")),
                        break_even_level=_safe_float(cycle.get("break_even_level")),
                        blended_entry_price=_safe_float(
                            cycle.get("blended_entry_price")
                        ),
                        protected_stop_level=_safe_float(
                            cycle.get("protected_stop_level")
                        ),
                        add_on_count=add_on_count,
                        add_on_slot=next_slot,
                        max_add_ons=_safe_int(cycle.get("max_add_ons")),
                        tranche_pct=tranche_pct,
                        next_addon_allowed=True,
                        pyramid_state=_safe_text(cycle.get("pyramid_state")),
                        base_position_units=_safe_float(
                            cycle.get("base_position_units")
                        ),
                        current_position_units=_safe_float(
                            cycle.get("current_position_units")
                        ),
                        last_trailing_confirmed_level=_safe_float(
                            cycle.get("last_trailing_confirmed_level")
                        ),
                        last_protected_stop_level=_safe_float(
                            cycle.get("last_protected_stop_level")
                        ),
                        last_pyramid_reference_level=_safe_float(
                            cycle.get("last_pyramid_reference_level")
                        ),
                        source_tags=cycle.get("source_tags", []),
                        reason_codes=common_reason_codes,
                        quality_flags=self._base_quality_flags(metrics),
                    )
                )
        return rows

    def _ug_strategy_combo_rows(
        self,
        *,
        symbol: str,
        metrics: Mapping[str, Any],
        source_entry: Mapping[str, Any],
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        dashboard_profile = self._ug_dashboard_profile(metrics)
        validation_score = _safe_float(dashboard_profile.get("validation_score")) or 0.0
        state_code = self._ug_state_code(metrics, dashboard_profile)
        traffic_light = _safe_text(
            dashboard_profile.get("technical_light")
        ) or state_code.removeprefix("UG_STATE_")
        conviction = self._ug_conviction(metrics, state_code, dashboard_profile)
        screen_stage = str(
            source_entry.get("screen_stage")
            or metrics.get("screen_stage")
            or ""
        )
        family_profile = self._family_source_profile(
            symbol, "UG_BREAKOUT", source_entry, peg_ready_map={}
        )
        source_reason_codes = list(family_profile.get("reason_codes", []))
        combo_trend_reasons = ["GREEN", "GP_NH60", "SIGMA_BO", "TREND_STRATEGY"]
        active_breakout_cycle = next(
            (
                cycle
                for key, cycle in active_cycles.items()
                if key[0] == "UG"
                and key[1] == "UG_BREAKOUT"
                and key[2] == symbol
                and _safe_text(cycle.get("buy_signal_code"))
                in {"UG_BUY_BREAKOUT", "UG_BUY_SQUEEZE_BREAKOUT"}
            ),
            None,
        )
        breakout_anchor = _pick_latest(
            (
                _safe_float(active_breakout_cycle.get("entry_price"))
                if active_breakout_cycle
                else None
            ),
            _safe_float(metrics.get("breakout_anchor")),
            _safe_float(metrics.get("donchian_high20")),
            _safe_float(metrics.get("prior_high60")),
        )
        close_value = _safe_float(metrics.get("close"))
        breakout_advance_pct = _pct_distance(breakout_anchor, close_value)
        if (
            active_breakout_cycle
            and breakout_advance_pct is not None
            and 3.0 <= breakout_advance_pct <= 5.5
        ):
            combo_trend_reasons.append("UG_PYRAMID_3TO5_ADVANCE")
        if active_breakout_cycle and metrics.get("ug_pbb_ready"):
            combo_trend_reasons.append("UG_PYRAMID_PBB_RETEST")
        combo_specs = (
            (
                "UG_COMBO_TREND",
                bool(
                    traffic_light == "GREEN"
                    and metrics.get("nh60")
                    and metrics.get("breakout_ready")
                ),
                combo_trend_reasons,
                _safe_float(metrics.get("bb_mid")),
                _safe_float(metrics.get("bb_upper")),
            ),
            (
                "UG_COMBO_PULLBACK",
                bool(metrics.get("ug_pbb_ready")),
                [
                    *(["GP_ABOVE_200MA"] if metrics.get("above_200ma") else []),
                    "SIGMA_PBB",
                    "PULLBACK_STRATEGY",
                ],
                _safe_float(metrics.get("bb_zone_low")),
                _safe_float(metrics.get("bb_zone_high")),
            ),
            (
                "UG_COMBO_SQUEEZE",
                bool(
                    traffic_light == "ORANGE"
                    and (
                        metrics.get("squeeze_active")
                        or metrics.get("vcp_active")
                        or metrics.get("tight_active")
                    )
                ),
                [
                    "ORANGE",
                    *(["SIGMA_SQUEEZE_READY"] if metrics.get("squeeze_active") else []),
                    *(["SIGMA_VCP_READY"] if metrics.get("vcp_active") else []),
                    *(["SIGMA_TIGHT_READY"] if metrics.get("tight_active") else []),
                    "SQUEEZE_STRATEGY",
                ],
                _safe_float(metrics.get("bb_zone_low")),
                _safe_float(metrics.get("bb_zone_high")),
            ),
        )
        for code, active, reasons, support_low, support_high in combo_specs:
            if not active:
                continue
            rows.append(
                _build_signal_row(
                    signal_date=metrics.get("date"),
                    symbol=symbol,
                    market=self.market,
                    engine="UG",
                    family="UG_STRATEGY",
                    signal_kind="STATE",
                    signal_code=code,
                    action_type="STATE",
                    conviction_grade=conviction,
                    screen_stage=screen_stage,
                    signal_score=validation_score,
                    gp_score=_safe_float(dashboard_profile.get("gp_score")),
                    gp_health=_safe_text(dashboard_profile.get("gp_health")),
                    sigma_score=_safe_float(dashboard_profile.get("sigma_score")),
                    sigma_health=_safe_text(dashboard_profile.get("sigma_health")),
                    traffic_light=traffic_light,
                    technical_light=traffic_light,
                    growth_score=_safe_float(dashboard_profile.get("growth_score")),
                    growth_health=_safe_text(dashboard_profile.get("growth_health")),
                    eps_health=_safe_text(dashboard_profile.get("eps_health")),
                    sales_health=_safe_text(dashboard_profile.get("sales_health")),
                    growth_data_status=_safe_text(
                        dashboard_profile.get("growth_data_status")
                    ),
                    dashboard_score=_safe_float(
                        dashboard_profile.get("dashboard_score")
                    ),
                    dashboard_light=_safe_text(
                        dashboard_profile.get("dashboard_light")
                    ),
                    dashboard_position_bias=_safe_text(
                        dashboard_profile.get("dashboard_position_bias")
                    ),
                    strategy_combo=code,
                    signal_phase="STATE",
                    primary_source_style=family_profile.get("primary_style", ""),
                    source_fit_score=family_profile.get("fit_score"),
                    source_fit_label=family_profile.get("fit_label", ""),
                    support_zone_low=support_low,
                    support_zone_high=support_high,
                    source_tags=metrics.get("source_tags"),
                    reason_codes=reasons
                    + [f"VALIDATION_{int(round(validation_score))}"]
                    + list(dashboard_profile.get("reason_codes", []))
                    + source_reason_codes,
                    quality_flags=self._base_quality_flags(metrics),
                )
            )
        return rows

    def _diagnostic_addon_state(
        self,
        symbol: str,
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
    ) -> dict[str, Any]:
        trend_cycles = [
            cycle
            for key, cycle in active_cycles.items()
            if key[0] == "TREND" and key[2] == symbol
        ]
        if not trend_cycles:
            return {
                "addon_ready": False,
                "addon_block_reason": None,
                "trailing_ratcheted": None,
                "blended_entry_protected": None,
            }
        selected_cycle = max(
            trend_cycles,
            key=lambda cycle: (
                int(bool(cycle.get("next_addon_allowed"))),
                _safe_text(cycle.get("last_signal_date")),
                _safe_text(cycle.get("opened_on")),
                _safe_text(cycle.get("family")),
            ),
        )
        addon_ready = _safe_bool(selected_cycle.get("next_addon_allowed"))
        return {
            "addon_ready": addon_ready,
            "addon_block_reason": (
                None
                if addon_ready
                else _safe_text(selected_cycle.get("addon_block_reason")) or None
            ),
            "trailing_ratcheted": (
                None
                if selected_cycle.get("trailing_ratcheted") in {None, ""}
                else _safe_bool(selected_cycle.get("trailing_ratcheted"))
            ),
            "blended_entry_protected": (
                None
                if selected_cycle.get("blended_entry_protected") in {None, ""}
                else _safe_bool(selected_cycle.get("blended_entry_protected"))
            ),
        }

    def _build_signal_universe_rows(
        self,
        source_registry: Mapping[str, Mapping[str, Any]],
        peg_ready_map: Mapping[str, Mapping[str, Any]],
        active_cycles: Mapping[tuple[str, str, str], Mapping[str, Any]],
        metrics_map: Mapping[str, Mapping[str, Any]],
        *,
        scope: str = _SCREENED_SCOPE,
        scope_symbols: Iterable[str] | None = None,
        screened_symbols: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        normalized_scope = _normalize_scope(scope)
        scope_symbol_set = {
            _safe_text(symbol).upper()
            for symbol in (
                scope_symbols
                if scope_symbols is not None
                else (set(source_registry.keys()) | set(peg_ready_map.keys()))
            )
            if _safe_text(symbol)
        }
        screened_symbol_set = {
            _safe_text(symbol).upper()
            for symbol in (
                screened_symbols
                if screened_symbols is not None
                else (set(source_registry.keys()) | set(peg_ready_map.keys()))
            )
            if _safe_text(symbol)
        }
        open_cycle_symbols = {symbol for (_, _, symbol) in active_cycles.keys()}
        public_symbol_set = set(screened_symbol_set)
        all_symbols = sorted(public_symbol_set)
        for symbol in all_symbols:
            source_entry = source_registry.get(symbol, {})
            metrics = metrics_map.get(symbol, {})
            dashboard_profile = self._ug_dashboard_profile(metrics) if metrics else {}
            source_disposition = _source_disposition(
                source_entry,
                symbol=symbol,
                peg_ready_map=peg_ready_map,
            )
            source_tags = _sorted_source_tags(source_entry.get("source_tags"))
            if symbol in peg_ready_map and "PEG_READY" not in source_tags:
                source_tags.append("PEG_READY")
            source_tags = _sorted_source_tags(source_tags)
            source_styles = list(
                dict.fromkeys(
                    _to_list(source_entry.get("source_style_tags"))
                    + _source_style_tags(source_tags)
                )
            )
            rows.append(
                {
                    "as_of_ts": self.as_of_date,
                    "scope": normalized_scope,
                    "symbol": symbol,
                    "market": self.market.upper(),
                    "screen_stage": source_entry.get("screen_stage")
                    or ("PEG_READY" if symbol in peg_ready_map else ""),
                    "source_disposition": source_disposition,
                    "buy_eligible": symbol in scope_symbol_set,
                    "source_buy_eligible": _source_buy_eligible(
                        source_entry,
                        symbol=symbol,
                        peg_ready_map=peg_ready_map,
                    ),
                    "watch_only": source_disposition == "watch_only",
                    "is_screened": symbol in screened_symbol_set,
                    "peg_ready": symbol in peg_ready_map,
                    "open_cycle": symbol in open_cycle_symbols,
                    "source_count": len(source_tags),
                    "source_tags": source_tags,
                    "primary_source_tag": _safe_text(
                        source_entry.get("primary_source_tag")
                    )
                    or (source_tags[0] if source_tags else ""),
                    "primary_source_stage": _safe_text(
                        source_entry.get("primary_source_stage")
                    )
                    or _safe_text(source_entry.get("screen_stage")),
                    "primary_source_style": _safe_text(
                        source_entry.get("primary_source_style")
                    )
                    or (source_styles[0] if source_styles else ""),
                    "source_style_tags": source_styles,
                    "source_overlap_bonus": float(
                        source_entry.get("source_overlap_bonus") or 0.0
                    ),
                    "source_priority_score": float(
                        source_entry.get("source_priority_score") or 0.0
                    ),
                    "trend_source_bonus": float(
                        source_entry.get("trend_source_bonus") or 0.0
                    ),
                    "ug_source_bonus": float(
                        source_entry.get("ug_source_bonus") or 0.0
                    ),
                    "sector": _safe_text(source_entry.get("sector") or metrics.get("sector")),
                    "industry": _safe_text(source_entry.get("industry") or metrics.get("industry")),
                    "group_name": _safe_text(source_entry.get("group_name") or metrics.get("group_name")),
                    "industry_key": _safe_text(source_entry.get("industry_key") or metrics.get("industry_key")),
                    "group_state": _safe_text(source_entry.get("group_state") or metrics.get("group_state")).upper(),
                    "leader_state": _safe_text(source_entry.get("leader_state") or metrics.get("leader_state")).upper(),
                    "breakdown_status": _safe_text(source_entry.get("breakdown_status") or metrics.get("breakdown_status")).upper(),
                    "group_strength_score": _safe_float(source_entry.get("group_strength_score") if source_entry.get("group_strength_score") is not None else metrics.get("group_strength_score")),
                    "leader_score": _safe_float(source_entry.get("leader_score") if source_entry.get("leader_score") is not None else metrics.get("leader_score")),
                    "breakdown_score": _safe_float(source_entry.get("breakdown_score") if source_entry.get("breakdown_score") is not None else metrics.get("breakdown_score")),
                    "ug_signal_score": _safe_float(
                        dashboard_profile.get("validation_score")
                    ),
                    "gp_score": _safe_float(dashboard_profile.get("gp_score")),
                    "gp_health": _safe_text(dashboard_profile.get("gp_health")),
                    "sigma_score": _safe_float(dashboard_profile.get("sigma_score")),
                    "sigma_health": _safe_text(dashboard_profile.get("sigma_health")),
                    "technical_light": _safe_text(
                        dashboard_profile.get("technical_light")
                    ),
                    "growth_score": _safe_float(dashboard_profile.get("growth_score")),
                    "growth_health": _safe_text(dashboard_profile.get("growth_health")),
                    "eps_health": _safe_text(dashboard_profile.get("eps_health")),
                    "sales_health": _safe_text(dashboard_profile.get("sales_health")),
                    "growth_data_status": _safe_text(
                        dashboard_profile.get("growth_data_status")
                    ),
                    "dashboard_score": _safe_float(
                        dashboard_profile.get("dashboard_score")
                    ),
                    "dashboard_light": _safe_text(
                        dashboard_profile.get("dashboard_light")
                    ),
                    "dashboard_position_bias": _safe_text(
                        dashboard_profile.get("dashboard_position_bias")
                    ),
                    "registry_as_of_ts": source_entry.get("as_of_ts"),
                }
            )
        return rows

    def _build_source_registry_summary(
        self,
        source_registry: Mapping[str, Mapping[str, Any]],
        signal_universe_rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        stage_counts: dict[str, int] = {}
        source_tag_counts: dict[str, int] = {}
        primary_source_tag_counts: dict[str, int] = {}
        primary_source_style_counts: dict[str, int] = {}
        source_style_counts: dict[str, int] = {}
        disposition_counts: dict[str, int] = {}
        buy_eligible_count = 0
        market_truth_source = ""
        core_overlay_applied: bool | None = None
        runtime_state = (
            dict(self.runtime_context.runtime_state)
            if self.runtime_context is not None and isinstance(self.runtime_context.runtime_state, dict)
            else {}
        )
        market_truth_mode = str(runtime_state.get("market_truth_mode") or "").strip()
        if not market_truth_mode:
            market_truth_mode = "standalone_manual" if self.standalone else "compat"
        fallback_reason = str(runtime_state.get("fallback_reason") or "").strip()
        for row in signal_universe_rows:
            stage = _safe_text(row.get("screen_stage"))
            if stage:
                stage_counts[stage] = stage_counts.get(stage, 0) + 1
            primary_tag = _safe_text(row.get("primary_source_tag"))
            if primary_tag:
                primary_source_tag_counts[primary_tag] = (
                    primary_source_tag_counts.get(primary_tag, 0) + 1
                )
            primary_style = _safe_text(row.get("primary_source_style"))
            if primary_style:
                primary_source_style_counts[primary_style] = (
                    primary_source_style_counts.get(primary_style, 0) + 1
                )
            disposition = _normalize_source_disposition(row.get("source_disposition"))
            if disposition:
                disposition_counts[disposition] = disposition_counts.get(disposition, 0) + 1
            if _safe_bool(row.get("buy_eligible")):
                buy_eligible_count += 1
            if not market_truth_source:
                market_truth_source = _safe_text(row.get("market_truth_source"))
            if core_overlay_applied is None and row.get("core_overlay_applied") is not None:
                core_overlay_applied = bool(row.get("core_overlay_applied"))
            for tag in _to_list(row.get("source_tags")):
                source_tag_counts[tag] = source_tag_counts.get(tag, 0) + 1
            for style in _to_list(row.get("source_style_tags")):
                source_style_counts[style] = source_style_counts.get(style, 0) + 1
        return {
            "market": self.market.upper(),
            "as_of_date": self.as_of_date,
            "market_truth_source": market_truth_source or ("local_standalone" if self.standalone else "market_intel_compat"),
            "core_overlay_applied": (not self.standalone) if core_overlay_applied is None else core_overlay_applied,
            "market_truth_mode": market_truth_mode,
            "fallback_reason": fallback_reason,
            "registry_symbols": len(source_registry),
            "signal_universe_symbols": len(signal_universe_rows),
            "buy_eligible_symbols": buy_eligible_count,
            "disposition_counts": dict(sorted(disposition_counts.items())),
            "stage_counts": dict(sorted(stage_counts.items())),
            "source_tag_counts": dict(sorted(source_tag_counts.items())),
            "primary_source_tag_counts": dict(
                sorted(primary_source_tag_counts.items())
            ),
            "primary_source_style_counts": dict(
                sorted(primary_source_style_counts.items())
            ),
            "source_style_counts": dict(sorted(source_style_counts.items())),
        }

    def _run_scope_scan(
        self,
        *,
        scope: str,
        scope_symbols: set[str],
        screened_symbol_scope: set[str],
        source_registry: Mapping[str, Mapping[str, Any]],
        raw_peg_map: Mapping[str, Mapping[str, Any]],
        peg_ready_map: Mapping[str, Mapping[str, Any]],
        peg_event_history_map: Mapping[str, Mapping[str, Any]],
        active_cycles: Mapping[tuple[Any, ...], Mapping[str, Any]],
        signal_history: Sequence[Mapping[str, Any]],
        metrics_map: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        normalized_scope = _normalize_scope(scope)
        scope_active_cycles = _scope_local_cycle_map(
            active_cycles,
            scope=normalized_scope,
        )
        scope_signal_history = _history_rows_for_scope(
            signal_history,
            scope=normalized_scope,
        )
        open_cycle_symbols = {symbol for (_, _, symbol) in scope_active_cycles.keys()}
        evaluation_symbols = sorted(scope_symbols | open_cycle_symbols)

        def _scoped_source_entry(symbol: str) -> dict[str, Any]:
            base_entry = source_registry.get(
                symbol,
                _default_signal_source_entry(
                    symbol=symbol,
                    peg_ready_map=peg_ready_map,
                    peg_event_history_map=peg_event_history_map,
                ),
            )
            scoped_entry = dict(base_entry)
            scoped_entry["source_disposition"] = _source_disposition(
                scoped_entry,
                symbol=symbol,
                peg_ready_map=peg_ready_map,
                peg_event_history_map=peg_event_history_map,
            )
            scoped_entry["source_buy_eligible"] = _source_buy_eligible(
                scoped_entry,
                symbol=symbol,
                peg_ready_map=peg_ready_map,
                peg_event_history_map=peg_event_history_map,
            )
            scoped_entry["buy_eligible"] = symbol in scope_symbols
            scoped_entry["watch_only"] = (
                scoped_entry["source_disposition"] == "watch_only"
            )
            scoped_entry["is_screened"] = symbol in screened_symbol_scope
            if not _safe_text(scoped_entry.get("screen_stage")):
                scoped_entry["screen_stage"] = (
                    "PEG_READY" if symbol in peg_ready_map else ""
                )
            return scoped_entry

        trend_buy_candidate_rows: list[dict[str, Any]] = []
        trend_state_rows: list[dict[str, Any]] = []
        ug_buy_candidate_rows: list[dict[str, Any]] = []
        ug_state_rows: list[dict[str, Any]] = []
        ug_combo_rows: list[dict[str, Any]] = []
        peg_context_map: dict[str, dict[str, Any]] = {}
        trend_sell_rows: list[dict[str, Any]] = []
        ug_sell_rows: list[dict[str, Any]] = []

        def _build_state_bundle(symbol: str) -> dict[str, list[dict[str, Any]]]:
            metrics = metrics_map.get(symbol)
            if not metrics:
                return {
                    "trend_state_rows": [],
                    "ug_state_rows": [],
                    "ug_combo_rows": [],
                }
            scoped_source_entry = _scoped_source_entry(symbol)
            return {
                "trend_state_rows": self._trend_state_rows(
                    symbol=symbol,
                    metrics=metrics,
                    source_entry=scoped_source_entry,
                ),
                "ug_state_rows": self._ug_state_rows(
                    symbol=symbol,
                    metrics=metrics,
                    source_entry=scoped_source_entry,
                ),
                "ug_combo_rows": self._ug_strategy_combo_rows(
                    symbol=symbol,
                    metrics=metrics,
                    source_entry=scoped_source_entry,
                    active_cycles=scope_active_cycles,
                ),
            }

        state_bundle_map: dict[str, dict[str, list[dict[str, Any]]]] = {}
        state_worker_count = _resolved_parallel_workers(
            len(evaluation_symbols),
            env_var="INVEST_PROTO_SIGNAL_EVENT_WORKERS",
        )
        if state_worker_count == 1:
            for symbol in evaluation_symbols:
                state_bundle_map[symbol] = _build_state_bundle(symbol)
        else:
            with ThreadPoolExecutor(max_workers=state_worker_count) as executor:
                future_map = {
                    symbol: executor.submit(_build_state_bundle, symbol)
                    for symbol in evaluation_symbols
                }
                for symbol in evaluation_symbols:
                    state_bundle_map[symbol] = future_map[symbol].result()

        for symbol in evaluation_symbols:
            bundle = state_bundle_map[symbol]
            trend_state_rows.extend(bundle.get("trend_state_rows", []))
            ug_state_rows.extend(bundle.get("ug_state_rows", []))
            ug_combo_rows.extend(bundle.get("ug_combo_rows", []))

        buy_candidate_symbols = sorted(
            symbol for symbol in scope_symbols if symbol in metrics_map
        )
        buy_bundle_map: dict[str, dict[str, Any]] = {}
        buy_worker_count = _resolved_parallel_workers(len(buy_candidate_symbols))

        def _build_buy_bundle(symbol: str) -> dict[str, Any]:
            metrics = metrics_map[symbol]
            return self._build_buy_candidate_bundle(
                symbol=symbol,
                metrics=metrics,
                source_entry=_scoped_source_entry(symbol),
                active_cycles=scope_active_cycles,
                peg_ready_map=peg_ready_map,
                raw_peg_map=raw_peg_map,
                peg_event_history_map=peg_event_history_map,
                signal_history=scope_signal_history,
            )

        if buy_worker_count == 1:
            for symbol in buy_candidate_symbols:
                buy_bundle_map[symbol] = _build_buy_bundle(symbol)
        else:
            with ThreadPoolExecutor(max_workers=buy_worker_count) as executor:
                future_map = {
                    symbol: executor.submit(_build_buy_bundle, symbol)
                    for symbol in buy_candidate_symbols
                }
                for symbol in buy_candidate_symbols:
                    buy_bundle_map[symbol] = future_map[symbol].result()

        for symbol in buy_candidate_symbols:
            bundle = buy_bundle_map[symbol]
            peg_context_map[symbol] = dict(bundle.get("peg_context", {}))
            trend_buy_candidate_rows.extend(bundle.get("trend_rows", []))
            ug_buy_candidate_rows.extend(bundle.get("ug_rows", []))

        cycle_items = list(scope_active_cycles.items())

        def _build_sell_bundle(
            item: tuple[tuple[Any, ...], Mapping[str, Any]],
        ) -> dict[str, list[dict[str, Any]]]:
            key, cycle = item
            symbol = key[2]
            metrics = metrics_map.get(symbol)
            if not metrics:
                return {"trend_sell_rows": [], "ug_sell_rows": []}
            if key[0] == "TREND":
                return {
                    "trend_sell_rows": self._trend_sell_events(
                        symbol=symbol,
                        metrics=metrics,
                        cycle=cycle,
                    ),
                    "ug_sell_rows": [],
                }
            elif key[0] == "UG":
                return {
                    "trend_sell_rows": [],
                    "ug_sell_rows": self._ug_sell_events(
                        symbol=symbol,
                        metrics=metrics,
                        cycle=cycle,
                    ),
                }
            return {"trend_sell_rows": [], "ug_sell_rows": []}

        sell_bundle_map: dict[int, dict[str, list[dict[str, Any]]]] = {}
        sell_worker_count = _resolved_parallel_workers(
            len(cycle_items),
            env_var="INVEST_PROTO_SIGNAL_EVENT_WORKERS",
        )
        if sell_worker_count == 1:
            for index, item in enumerate(cycle_items):
                sell_bundle_map[index] = _build_sell_bundle(item)
        else:
            with ThreadPoolExecutor(max_workers=sell_worker_count) as executor:
                future_map = {
                    index: executor.submit(_build_sell_bundle, item)
                    for index, item in enumerate(cycle_items)
                }
                for index in range(len(cycle_items)):
                    sell_bundle_map[index] = future_map[index].result()

        for index in range(len(cycle_items)):
            bundle = sell_bundle_map[index]
            trend_sell_rows.extend(bundle.get("trend_sell_rows", []))
            ug_sell_rows.extend(bundle.get("ug_sell_rows", []))

        trend_buy_candidate_rows = _sorted_signal_rows(trend_buy_candidate_rows)
        ug_buy_candidate_rows = _sorted_signal_rows(ug_buy_candidate_rows)
        trend_sell_rows = _sorted_signal_rows(trend_sell_rows)
        ug_sell_rows = _sorted_signal_rows(ug_sell_rows)
        trend_event_rows = _sorted_signal_rows(
            [
                row
                for row in trend_buy_candidate_rows
                if _safe_bool(row.get("_legacy_visible"))
            ]
            + trend_sell_rows
        )
        trend_state_rows = _sorted_signal_rows(trend_state_rows)
        ug_event_rows = _sorted_signal_rows(
            [
                row
                for row in ug_buy_candidate_rows
                if _safe_bool(row.get("_legacy_visible"))
            ]
            + ug_sell_rows
        )
        ug_state_rows = _sorted_signal_rows(ug_state_rows)
        ug_combo_rows = _sorted_signal_rows(ug_combo_rows)

        all_events = trend_event_rows + ug_event_rows
        updated_cycles = self._update_cycles(
            all_events,
            scope_active_cycles,
            metrics_map,
            peg_context_map,
            scope=normalized_scope,
        )
        level_state_rows = _sorted_signal_rows(
            self._build_level_state_rows(updated_cycles, metrics_map)
        )
        addon_state_rows = _sorted_signal_rows(
            self._build_trend_addon_state_rows(
                updated_cycles,
                metrics_map,
                peg_context_map,
            )
        )
        trend_state_rows = _sorted_signal_rows(
            trend_state_rows
            + [
                row
                for row in level_state_rows
                if _safe_text(row.get("engine")) == "TREND"
            ]
            + addon_state_rows
        )
        ug_state_rows = _sorted_signal_rows(
            ug_state_rows
            + [
                row
                for row in level_state_rows
                if _safe_text(row.get("engine")) == "UG"
            ]
        )

        diagnostics = []
        for symbol in evaluation_symbols:
            if symbol not in metrics_map:
                continue
            scoped_source_entry = _scoped_source_entry(symbol)
            diagnostics.append(
                {
                    "screen_date": self.as_of_date,
                    "scope": normalized_scope,
                    "symbol": symbol,
                    "market": self.market.upper(),
                    "screen_stage": scoped_source_entry.get("screen_stage"),
                    "buy_eligible": symbol in scope_symbols,
                    "source_buy_eligible": scoped_source_entry.get(
                        "source_buy_eligible"
                    ),
                    "is_screened": symbol in screened_symbol_scope,
                    "source_tags": list(scoped_source_entry.get("source_tags", [])),
                    **self._diagnostic_addon_state(symbol, updated_cycles),
                }
            )
        diagnostics = sorted(
            diagnostics,
            key=lambda row: (
                _safe_text(row.get("scope")),
                _safe_text(row.get("screen_date")),
                _safe_text(row.get("symbol")).upper(),
            ),
        )

        trend_event_rows = _scope_signal_rows(
            trend_event_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        trend_state_rows = _scope_signal_rows(
            trend_state_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        ug_event_rows = _scope_signal_rows(
            ug_event_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        ug_state_rows = _scope_signal_rows(
            ug_state_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        ug_combo_rows = _scope_signal_rows(
            ug_combo_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        trend_buy_candidate_rows = _scope_signal_rows(
            trend_buy_candidate_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        ug_buy_candidate_rows = _scope_signal_rows(
            ug_buy_candidate_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        trend_sell_rows = _scope_signal_rows(
            trend_sell_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )
        ug_sell_rows = _scope_signal_rows(
            ug_sell_rows,
            scope=normalized_scope,
            scope_symbols=scope_symbols,
            screened_symbols=screened_symbol_scope,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
        )

        all_state_rows = _sorted_signal_rows(
            trend_state_rows + ug_state_rows + ug_combo_rows
        )
        trend_event_rows_v2 = _apply_update_overlay_rows(
            _transform_signal_rows(trend_event_rows),
            metrics_map,
        )
        trend_state_rows_v2 = _apply_update_overlay_rows(
            _transform_signal_rows(trend_state_rows),
            metrics_map,
        )
        ug_event_rows_v2 = _apply_update_overlay_rows(
            _transform_signal_rows(ug_event_rows),
            metrics_map,
        )
        ug_state_rows_v2 = _apply_update_overlay_rows(
            _transform_signal_rows(ug_state_rows),
            metrics_map,
        )
        ug_combo_rows_v2 = _apply_update_overlay_rows(
            _transform_signal_rows(ug_combo_rows),
            metrics_map,
        )
        buy_projection_rows_v2 = _apply_update_overlay_rows(
            _transform_signal_rows(
                trend_buy_candidate_rows + ug_buy_candidate_rows,
                strip_internal=False,
            ),
            metrics_map,
        )
        sell_projection_rows_v2 = _apply_update_overlay_rows(
            _transform_signal_rows(
                trend_sell_rows + ug_sell_rows,
                strip_internal=False,
            ),
            metrics_map,
        )
        all_signal_rows_v2 = _sorted_signal_rows(
            trend_event_rows_v2
            + trend_state_rows_v2
            + ug_event_rows_v2
            + ug_state_rows_v2
            + ug_combo_rows_v2
        )
        buy_rows_v1 = _project_scoped_signal_rows(
            buy_projection_rows_v2,
            as_of_date=self.as_of_date,
            signal_side="BUY",
        )
        sell_rows_v1 = _project_scoped_signal_rows(
            sell_projection_rows_v2,
            as_of_date=self.as_of_date,
            signal_side="SELL",
        )
        open_cycle_rows = _sorted_signal_rows(
            _sanitize_cycle_row(row) for row in updated_cycles.values()
        )
        signal_universe_rows = _apply_update_snapshot_rows(
            self._build_signal_universe_rows(
                source_registry,
                peg_ready_map,
                updated_cycles,
                metrics_map,
                scope=normalized_scope,
                scope_symbols=scope_symbols,
                screened_symbols=screened_symbol_scope,
            ),
            metrics_map,
        )

        return {
            "trend_event_rows_raw": trend_event_rows,
            "trend_state_rows_raw": trend_state_rows,
            "ug_event_rows_raw": ug_event_rows,
            "ug_state_rows_raw": ug_state_rows,
            "ug_combo_rows_raw": ug_combo_rows,
            "all_event_rows_raw": _sorted_signal_rows(trend_event_rows + ug_event_rows),
            "all_state_rows_raw": all_state_rows,
            "trend_event_rows_v2": trend_event_rows_v2,
            "trend_state_rows_v2": trend_state_rows_v2,
            "ug_event_rows_v2": ug_event_rows_v2,
            "ug_state_rows_v2": ug_state_rows_v2,
            "ug_combo_rows_v2": ug_combo_rows_v2,
            "all_signal_rows_v2": all_signal_rows_v2,
            "buy_rows_v1": buy_rows_v1,
            "sell_rows_v1": sell_rows_v1,
            "open_cycle_rows": open_cycle_rows,
            "diagnostics": diagnostics,
            "signal_universe_rows": signal_universe_rows,
            "peg_context_map": peg_context_map,
        }

    def _run_scope_scans(
        self,
        *,
        all_scope_symbols: set[str],
        screened_scope_symbols: set[str],
        source_registry: Mapping[str, Mapping[str, Any]],
        raw_peg_map: Mapping[str, Mapping[str, Any]],
        peg_ready_map: Mapping[str, Mapping[str, Any]],
        peg_event_history_map: Mapping[str, Mapping[str, Any]],
        active_cycles: Mapping[tuple[Any, ...], Mapping[str, Any]],
        signal_history: Sequence[Mapping[str, Any]],
        metrics_map: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        scope_inputs = {
            _ALL_SCOPE: set(all_scope_symbols),
            _SCREENED_SCOPE: set(screened_scope_symbols),
        }
        worker_count = _resolved_parallel_workers(
            len(scope_inputs),
            cap=2,
            env_var="INVEST_PROTO_SIGNAL_SCOPE_WORKERS",
        )

        def _scan(scope: str) -> tuple[dict[str, Any], float]:
            started = time.perf_counter()
            result = self._run_scope_scan(
                scope=scope,
                scope_symbols=scope_inputs[scope],
                screened_symbol_scope=screened_scope_symbols,
                source_registry=source_registry,
                raw_peg_map=raw_peg_map,
                peg_ready_map=peg_ready_map,
                peg_event_history_map=peg_event_history_map,
                active_cycles=active_cycles,
                signal_history=signal_history,
                metrics_map=metrics_map,
            )
            return result, time.perf_counter() - started

        scopes = [_ALL_SCOPE, _SCREENED_SCOPE]
        parallel_started = time.perf_counter()
        if worker_count == 1:
            scan_results = {scope: _scan(scope) for scope in scopes}
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_map = {
                    scope: executor.submit(_scan, scope)
                    for scope in scopes
                }
                scan_results = {
                    scope: future_map[scope].result()
                    for scope in scopes
                }

        if self.runtime_context is not None:
            all_elapsed = scan_results[_ALL_SCOPE][1]
            screened_elapsed = scan_results[_SCREENED_SCOPE][1]
            self.runtime_context.add_timing(
                "signals.scope_scan_all_seconds",
                all_elapsed,
            )
            self.runtime_context.add_timing(
                "signals.scope_scan_screened_seconds",
                screened_elapsed,
            )
            self.runtime_context.add_timing(
                "signals.scope_scan_parallel_seconds",
                time.perf_counter() - parallel_started,
            )

        return {
            _ALL_SCOPE: scan_results[_ALL_SCOPE][0],
            _SCREENED_SCOPE: scan_results[_SCREENED_SCOPE][0],
        }

    def run(self) -> dict[str, Any]:
        ensure_market_dirs(self.market)

        os.makedirs(self.results_dir, exist_ok=True)
        if self.runtime_context is not None:
            self.runtime_context.update_runtime_state(
                current_stage="Multi-screener signal engine",
                current_symbol="",
                current_chunk="source_registry",
                status="running",
            )

        phase_started = time.perf_counter()
        source_registry = self._load_source_registry()
        self._add_elapsed_timing("signals.source_registry_seconds", phase_started)

        phase_started = time.perf_counter()
        raw_peg_map, peg_ready_map, peg_financial_map = self._load_or_run_peg_screen()
        self._add_elapsed_timing("signals.peg_screen_seconds", phase_started)

        phase_started = time.perf_counter()
        active_cycles = self._load_active_cycles()

        signal_history = self._load_signal_history()
        state_history = self._load_state_history()

        peg_event_history_rows = self._load_peg_event_history()
        self._add_elapsed_timing("signals.history_load_seconds", phase_started)

        peg_event_history_map = self._latest_peg_event_map(peg_event_history_rows)

        all_symbol_scope = set(self._load_all_symbol_universe())
        screened_symbol_scope = (
            set(source_registry.keys()) | set(peg_ready_map.keys())
        ) & all_symbol_scope
        active_cycle_symbols = {
            _safe_text(key[-1]).upper()
            for key in active_cycles.keys()
            if len(key) >= 3
        }

        symbols_to_load = set(all_symbol_scope)
        symbols_to_load.update(active_cycle_symbols)
        symbols_to_load.update(peg_event_history_map.keys())

        phase_started = time.perf_counter()
        self.financial_map = dict(peg_financial_map)
        missing_financial_symbols = sorted(symbols_to_load - set(self.financial_map))
        if missing_financial_symbols:
            self.financial_map.update(
                _load_financial_map(self.market, symbols=missing_financial_symbols)
            )
        self._add_elapsed_timing("signals.financial_load_seconds", phase_started)
        if self.runtime_context is not None:
            self.runtime_context.financial_map = dict(self.financial_map)
            self.runtime_context.update_runtime_state(
                current_stage="Multi-screener signal engine",
                current_symbol="",
                current_chunk=f"metrics_inputs:{len(symbols_to_load)}",
                status="running",
            )

        phase_started = time.perf_counter()
        frames = self._load_frames(symbols_to_load)
        self._add_elapsed_timing("signals.frame_load_seconds", phase_started)

        phase_started = time.perf_counter()
        feature_map = self._load_feature_map(frames)
        self._add_elapsed_timing("signals.feature_map_seconds", phase_started)

        phase_started = time.perf_counter()
        metrics_map = _signal_metrics.build_metrics_map(
            frames=frames,
            market=self.market,
            metadata_map=self.metadata_map,
            financial_map=self.financial_map,
            feature_map=feature_map,
            source_registry=source_registry,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
            build_metrics_fn=_build_metrics,
            max_workers=_resolved_parallel_workers(len(frames)),
        )
        self._add_elapsed_timing("signals.metrics_map_seconds", phase_started)

        scope_results = self._run_scope_scans(
            all_scope_symbols=all_symbol_scope,
            screened_scope_symbols=screened_symbol_scope,
            source_registry=source_registry,
            raw_peg_map=raw_peg_map,
            peg_ready_map=peg_ready_map,
            peg_event_history_map=peg_event_history_map,
            active_cycles=active_cycles,
            signal_history=signal_history,
            metrics_map=metrics_map,
        )
        if self.runtime_context is not None:
            self.runtime_context.update_runtime_state(
                current_stage="Multi-screener signal engine",
                current_symbol="",
                current_chunk="persist_outputs",
                status="running",
            )

        trend_event_rows_v2 = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["trend_event_rows_v2"]
            + scope_results[_SCREENED_SCOPE]["trend_event_rows_v2"]
        )
        trend_state_rows_v2 = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["trend_state_rows_v2"]
            + scope_results[_SCREENED_SCOPE]["trend_state_rows_v2"]
        )
        ug_event_rows_v2 = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["ug_event_rows_v2"]
            + scope_results[_SCREENED_SCOPE]["ug_event_rows_v2"]
        )
        ug_state_rows_v2 = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["ug_state_rows_v2"]
            + scope_results[_SCREENED_SCOPE]["ug_state_rows_v2"]
        )
        ug_combo_rows_v2 = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["ug_combo_rows_v2"]
            + scope_results[_SCREENED_SCOPE]["ug_combo_rows_v2"]
        )
        all_signal_rows_v2 = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["all_signal_rows_v2"]
            + scope_results[_SCREENED_SCOPE]["all_signal_rows_v2"]
        )
        buy_signals_all_rows_v1 = scope_results[_ALL_SCOPE]["buy_rows_v1"]
        sell_signals_all_rows_v1 = scope_results[_ALL_SCOPE]["sell_rows_v1"]
        buy_signals_screened_rows_v1 = scope_results[_SCREENED_SCOPE]["buy_rows_v1"]
        sell_signals_screened_rows_v1 = scope_results[_SCREENED_SCOPE]["sell_rows_v1"]
        open_cycle_rows_v2 = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["open_cycle_rows"]
            + scope_results[_SCREENED_SCOPE]["open_cycle_rows"]
        )
        diagnostics = sorted(
            scope_results[_ALL_SCOPE]["diagnostics"]
            + scope_results[_SCREENED_SCOPE]["diagnostics"],
            key=lambda row: (
                _safe_text(row.get("scope")),
                _safe_text(row.get("screen_date")),
                _safe_text(row.get("symbol")).upper(),
            ),
        )
        signal_universe_rows = sorted(
            scope_results[_ALL_SCOPE]["signal_universe_rows"]
            + scope_results[_SCREENED_SCOPE]["signal_universe_rows"],
            key=lambda row: (
                _safe_text(row.get("scope")),
                _safe_text(row.get("symbol")).upper(),
            ),
        )

        all_state_rows = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["all_state_rows_raw"]
            + scope_results[_SCREENED_SCOPE]["all_state_rows_raw"]
        )
        all_events = _sorted_signal_rows(
            scope_results[_ALL_SCOPE]["all_event_rows_raw"]
            + scope_results[_SCREENED_SCOPE]["all_event_rows_raw"]
        )
        phase_started = time.perf_counter()
        state_history = self._persist_state_history(state_history, all_state_rows)
        signal_history = self._persist_signal_history(signal_history, all_events)
        peg_context_map: dict[str, dict[str, Any]] = {}
        peg_context_map.update(scope_results[_ALL_SCOPE]["peg_context_map"])
        peg_context_map.update(scope_results[_SCREENED_SCOPE]["peg_context_map"])
        peg_event_history_rows = self._persist_peg_event_history(
            peg_event_history_rows,
            peg_context_map,
        )
        self._add_elapsed_timing("signals.history_persist_seconds", phase_started)
        earnings_provider_diagnostics = (
            self.earnings_collector.provider_diagnostics_rows()
            if self.earnings_collector
            else []
        )
        source_registry_summary = self._build_source_registry_summary(
            source_registry, signal_universe_rows
        )

        summary = {
            "market": self.market.upper(),
            "as_of_date": self.as_of_date,
            "market_truth_source": "local_standalone" if self.standalone else "market_intel_compat",
            "core_overlay_applied": not self.standalone,
            "market_truth_mode": (
                str(self.runtime_context.runtime_state.get("market_truth_mode") or "").strip()
                if self.runtime_context is not None and isinstance(self.runtime_context.runtime_state, dict)
                else ""
            ) or ("standalone_manual" if self.standalone else "compat"),
            "fallback_reason": (
                str(self.runtime_context.runtime_state.get("fallback_reason") or "").strip()
                if self.runtime_context is not None and isinstance(self.runtime_context.runtime_state, dict)
                else ""
            ),
            "counts": {
                "trend_events_v2": len(trend_event_rows_v2),
                "trend_states_v2": len(trend_state_rows_v2),
                "ug_events_v2": len(ug_event_rows_v2),
                "ug_states_v2": len(ug_state_rows_v2),
                "ug_strategy_combos_v2": len(ug_combo_rows_v2),
                "all_signals_v2": len(all_signal_rows_v2),
                "buy_signals_all_symbols_v1": len(buy_signals_all_rows_v1),
                "sell_signals_all_symbols_v1": len(sell_signals_all_rows_v1),
                "buy_signals_screened_symbols_v1": len(
                    buy_signals_screened_rows_v1
                ),
                "sell_signals_screened_symbols_v1": len(
                    sell_signals_screened_rows_v1
                ),
                "open_cycles": len(open_cycle_rows_v2),
                "diagnostics": len(diagnostics),
                "earnings_provider_diagnostics": len(earnings_provider_diagnostics),
                "signal_history": len(signal_history),
                "signal_state_history": len(state_history),
                "peg_event_history": len(peg_event_history_rows),
                "signal_universe": len(signal_universe_rows),
            },
            "buy_signal_segments": _build_buy_signal_segment_summary(
                all_rows=buy_signals_all_rows_v1,
                screened_rows=buy_signals_screened_rows_v1,
            ),
        }
        phase_started = time.perf_counter()
        _signal_writers.write_signal_outputs(
            self.results_dir,
            trend_event_rows=trend_event_rows_v2,
            trend_state_rows=trend_state_rows_v2,
            ug_event_rows=ug_event_rows_v2,
            ug_state_rows=ug_state_rows_v2,
            ug_combo_rows=ug_combo_rows_v2,
            all_signal_rows=all_signal_rows_v2,
            buy_signals_all_rows=buy_signals_all_rows_v1,
            sell_signals_all_rows=sell_signals_all_rows_v1,
            buy_signals_screened_rows=buy_signals_screened_rows_v1,
            sell_signals_screened_rows=sell_signals_screened_rows_v1,
            open_cycle_rows=open_cycle_rows_v2,
            diagnostics=diagnostics,
            earnings_provider_diagnostics=earnings_provider_diagnostics,
            signal_universe_rows=signal_universe_rows,
            source_registry_summary=source_registry_summary,
            signal_summary=summary,
            write_records_fn=_write_records,
        )
        self._add_elapsed_timing("signals.persist_outputs_seconds", phase_started)
        if self.earnings_collector:
            self.earnings_collector.log_provider_summary()
        return {
            "trend_following_events_v2": trend_event_rows_v2,
            "trend_following_states_v2": trend_state_rows_v2,
            "ultimate_growth_events_v2": ug_event_rows_v2,
            "ultimate_growth_states_v2": ug_state_rows_v2,
            "ug_strategy_combos_v2": ug_combo_rows_v2,
            "all_signals_v2": all_signal_rows_v2,
            "buy_signals_all_symbols_v1": buy_signals_all_rows_v1,
            "sell_signals_all_symbols_v1": sell_signals_all_rows_v1,
            "buy_signals_screened_symbols_v1": buy_signals_screened_rows_v1,
            "sell_signals_screened_symbols_v1": sell_signals_screened_rows_v1,
            "open_family_cycles": open_cycle_rows_v2,
            "screen_signal_diagnostics": diagnostics,
            "earnings_provider_diagnostics": earnings_provider_diagnostics,
            "signal_universe_snapshot": signal_universe_rows,
            "source_registry_summary": source_registry_summary,
            "signal_summary": summary,
        }


def run_multi_screener_signal_scan(
    market: str = "us",
    as_of_date: str | None = None,
    *,
    standalone: bool = False,
    upcoming_earnings_fetcher: (
        Callable[[str, str | None, int], pd.DataFrame] | None
    ) = None,
    earnings_collector: EarningsDataCollector | None = None,
    runtime_context: RuntimeContext | None = None,
    source_registry_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:

    engine = MultiScreenerSignalEngine(
        market=market,
        as_of_date=as_of_date,
        standalone=standalone,
        upcoming_earnings_fetcher=upcoming_earnings_fetcher,
        earnings_collector=earnings_collector,
        runtime_context=runtime_context,
        source_registry_snapshot=source_registry_snapshot,
    )
    return engine.run()


SignalEngine = MultiScreenerSignalEngine
QullamaggieSignalEngine = MultiScreenerSignalEngine
run_signal_scan = run_multi_screener_signal_scan


def run_peg_imminent_screen(
    market: str = "us",
    as_of_date: str | None = None,
    *,
    upcoming_earnings_fetcher: (
        Callable[[str, str | None, int], pd.DataFrame] | None
    ) = None,
) -> dict[str, list[dict[str, Any]]]:
    screener = PEGImminentScreener(
        market=market,
        as_of_date=as_of_date,
        upcoming_earnings_fetcher=upcoming_earnings_fetcher,
    )
    return screener.run()


def run_qullamaggie_signal_scan(
    market: str = "us",
    as_of_date: str | None = None,
    *,
    standalone: bool = False,
    upcoming_earnings_fetcher: (
        Callable[[str, str | None, int], pd.DataFrame] | None
    ) = None,
    earnings_collector: EarningsDataCollector | None = None,
    runtime_context: RuntimeContext | None = None,
    source_registry_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return run_multi_screener_signal_scan(
        market=market,
        as_of_date=as_of_date,
        standalone=standalone,
        upcoming_earnings_fetcher=upcoming_earnings_fetcher,
        earnings_collector=earnings_collector,
        runtime_context=runtime_context,
        source_registry_snapshot=source_registry_snapshot,
    )
