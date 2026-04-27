from __future__ import annotations

from functools import lru_cache
import os

import pandas as pd

from config import (
    BASE_DIR,
    DATA_DIR,
    DATA_KR_DIR,
    DATA_US_DIR,
    EXTERNAL_DATA_DIR,
    RESULTS_DIR,
    STOCK_METADATA_PATH,
)
from .symbol_normalization import normalize_provider_symbol_value, normalize_symbol_value


_MARKET_DATA_DIRS = {
    "us": DATA_US_DIR,
    "kr": DATA_KR_DIR,
}

_PRIMARY_BENCHMARK = {
    "us": "SPY",
    "kr": "KOSPI",
}

_BENCHMARK_CANDIDATES = {
    "us": ["SPY", "QQQ", "DIA", "IWM"],
    "kr": ["KOSPI", "^KS11", "069500"],
}

_INDEX_SYMBOLS = {
    "us": {"SPY", "QQQ", "DIA", "IWM", "^GSPC", "^IXIC", "^DJI", "^RUT", "^VIX", "^VVIX", "^SKEW"},
    "kr": {"KOSPI", "KOSDAQ", "^KS11", "^KQ11", "069500", "102110"},
}



def require_market_key(market: str) -> str:
    normalized = str(market or "").strip().lower()
    if not normalized:
        raise ValueError(f"Unsupported market: {market}")
    if any(not (char.isalnum() or char in {"_", "-"}) for char in normalized):
        raise ValueError(f"Unsupported market: {market}")
    if normalized not in _MARKET_DATA_DIRS:
        raise ValueError(f"Unsupported market: {market}")
    return normalized



def market_key(market: str) -> str:
    return require_market_key(market)



def get_market_data_dir(market: str) -> str:
    return _MARKET_DATA_DIRS[market_key(market)]



def get_stock_metadata_path(market: str) -> str:
    normalized_market = market_key(market)
    if normalized_market == "us":
        return STOCK_METADATA_PATH
    return os.path.join(os.path.dirname(STOCK_METADATA_PATH), f"stock_metadata_{normalized_market}.csv")


@lru_cache(maxsize=4)
def _load_provider_symbol_map(market: str) -> dict[str, str]:
    metadata_path = get_stock_metadata_path(market)
    if not os.path.exists(metadata_path):
        return {}

    try:
        frame = pd.read_csv(
            metadata_path,
            dtype={"symbol": "string", "provider_symbol": "string"},
            encoding="utf-8-sig",
        )
    except Exception:
        return {}
    if frame is None or frame.empty or "symbol" not in frame.columns or "provider_symbol" not in frame.columns:
        return {}

    normalized_market = market_key(market)
    mapping: dict[str, str] = {}
    for _, row in frame.iterrows():
        symbol = normalize_symbol_value(row.get("symbol"), normalized_market)
        provider_symbol = normalize_provider_symbol_value(row.get("provider_symbol"))
        if normalized_market == "kr" and provider_symbol and "." in provider_symbol:
            base, suffix = provider_symbol.rsplit(".", 1)
            provider_symbol = f"{normalize_symbol_value(base, normalized_market)}.{suffix.upper()}"
        if symbol and provider_symbol:
            mapping[symbol] = provider_symbol
    return mapping


def get_preferred_provider_symbol(symbol: str, market: str) -> str | None:
    symbol_key = normalize_symbol_value(symbol, market)
    if not symbol_key:
        return None
    provider_symbol = _load_provider_symbol_map(market).get(symbol_key)
    return provider_symbol or None


def iter_preferred_provider_symbols(
    symbol: str,
    market: str,
    *,
    strict: bool = False,
) -> list[str]:
    preferred = get_preferred_provider_symbol(symbol, market)
    candidates: list[str] = []
    if preferred:
        candidates.append(preferred)
    if strict and candidates:
        return candidates
    for candidate in iter_provider_symbols(symbol, market):
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates



def get_market_results_root(market: str) -> str:
    return os.path.join(get_results_root(), market_key(market))


def results_root_override_active() -> bool:
    return bool(str(os.environ.get("INVEST_PROTO_RESULTS_DIR") or "").strip())


def get_results_root() -> str:
    override = str(os.environ.get("INVEST_PROTO_RESULTS_DIR") or "").strip()
    if not override:
        return os.path.abspath(RESULTS_DIR)
    if os.path.isabs(override):
        return os.path.abspath(override)
    return os.path.abspath(os.path.join(BASE_DIR, override))



def get_market_screeners_root(market: str) -> str:
    return os.path.join(get_market_results_root(market), "screeners")


def get_market_source_registry_snapshot_path(market: str) -> str:
    return os.path.join(
        get_market_screeners_root(market), "source_registry_snapshot.json"
    )


def get_market_intel_compat_root(market: str) -> str:
    base_root = os.environ.get("MARKET_INTEL_COMPAT_RESULTS_ROOT") or os.path.join(
        BASE_DIR, "..", "market-intel-core", "results", "compat", "invest_prototype"
    )
    return os.path.join(os.path.abspath(base_root), market_key(market))


def get_augment_results_dir(market: str) -> str:
    return os.path.join(get_market_screeners_root(market), "augment")



def get_market_signals_root(market: str) -> str:
    return os.path.join(get_market_results_root(market), "signals")


def get_market_runtime_root(market: str) -> str:
    return os.path.join(get_market_results_root(market), "runtime")


def get_runtime_profile_path(market: str) -> str:
    return os.path.join(get_market_runtime_root(market), "runtime_profile.json")


def get_full_run_summary_path(market: str) -> str:
    return os.path.join(get_market_runtime_root(market), "full_run_summary.json")


def get_runtime_state_path(market: str) -> str:
    return os.path.join(get_market_runtime_root(market), "runtime_state.json")


def _results_root_from_data_dir(data_dir: str | None = None) -> str:
    base_data_dir = os.path.abspath(str(data_dir or DATA_DIR))
    if os.path.basename(base_data_dir).lower() == "data":
        project_root = os.path.dirname(base_data_dir)
    else:
        project_root = base_data_dir
    return os.path.join(project_root, "results")


def get_collector_run_state_path(market: str, *, data_dir: str | None = None) -> str:
    if str(os.environ.get("INVEST_PROTO_RESULTS_DIR") or "").strip():
        return os.path.join(
            get_market_runtime_root(market),
            "collector_run_state.json",
        )
    return os.path.join(
        _results_root_from_data_dir(data_dir),
        market_key(market),
        "runtime",
        "collector_run_state.json",
    )



def get_markminervini_results_dir(market: str) -> str:
    return os.path.join(get_market_screeners_root(market), "markminervini")



def get_markminervini_with_rs_path(market: str) -> str:
    return os.path.join(get_markminervini_results_dir(market), "with_rs.csv")



def get_markminervini_advanced_financial_results_path(market: str) -> str:
    return os.path.join(get_markminervini_results_dir(market), "advanced_financial_results.csv")



def get_markminervini_integrated_results_path(market: str) -> str:
    return os.path.join(get_markminervini_results_dir(market), "integrated_results.csv")



def get_markminervini_integrated_pattern_results_path(market: str) -> str:
    return os.path.join(get_markminervini_results_dir(market), "integrated_pattern_results.csv")



def get_markminervini_new_tickers_path(market: str) -> str:
    return os.path.join(get_markminervini_results_dir(market), "new_tickers.csv")



def get_markminervini_previous_with_rs_path(market: str) -> str:
    return os.path.join(get_markminervini_results_dir(market), "previous_with_rs.csv")



def get_qullamaggie_results_dir(market: str) -> str:
    return os.path.join(get_market_screeners_root(market), "qullamaggie")



def get_leader_lagging_results_dir(market: str) -> str:
    return os.path.join(get_market_screeners_root(market), "leader_lagging")



def get_tradingview_results_dir(market: str) -> str:
    return os.path.join(get_market_screeners_root(market), "tradingview")



def get_weinstein_stage2_results_dir(market: str) -> str:
    return os.path.join(get_market_screeners_root(market), "weinstein_stage2")



def get_peg_imminent_results_dir(market: str) -> str:
    return os.path.join(get_market_screeners_root(market), "peg_imminent")



def get_multi_screener_signals_results_dir(market: str) -> str:
    return os.path.join(get_market_signals_root(market), "multi_screener")



def get_signal_engine_results_dir(market: str) -> str:
    return get_multi_screener_signals_results_dir(market)



def get_earnings_cache_dir(market: str) -> str:
    return os.path.join(EXTERNAL_DATA_DIR, "earnings", market_key(market))



def get_financial_cache_dir(market: str) -> str:
    return os.path.join(EXTERNAL_DATA_DIR, "financials", market_key(market))



def get_primary_benchmark_symbol(market: str) -> str:
    return _PRIMARY_BENCHMARK[market_key(market)]


def get_benchmark_candidates(market: str) -> list[str]:
    return list(_BENCHMARK_CANDIDATES[market_key(market)])


def get_runtime_symbol_limit() -> int | None:
    raw_value = str(os.environ.get("INVEST_PROTO_RUNTIME_SYMBOL_LIMIT") or "").strip()
    if not raw_value:
        return None
    try:
        limit = int(raw_value)
    except ValueError:
        return None
    return limit if limit > 0 else None


def limit_runtime_symbols(symbols: list[str]) -> list[str]:
    limit = get_runtime_symbol_limit()
    if limit is None:
        return list(symbols)
    return list(symbols[:limit])


def get_index_symbols(market: str) -> set[str]:
    return set(_INDEX_SYMBOLS[market_key(market)])



def is_index_symbol(market: str, symbol: str) -> bool:
    return str(symbol or "").strip().upper() in get_index_symbols(market)



def iter_provider_symbols(symbol: str, market: str) -> list[str]:
    symbol_key = str(symbol or "").strip().upper()
    if not symbol_key:
        return []

    normalized_market = market_key(market)
    if normalized_market == "us":
        return [symbol_key]

    if symbol_key in {"KOSPI", "^KS11"}:
        return ["^KS11"]
    if symbol_key in {"KOSDAQ", "^KQ11"}:
        return ["^KQ11"]
    if symbol_key.startswith("^") or "." in symbol_key:
        return [symbol_key]

    return [f"{symbol_key}.KS", f"{symbol_key}.KQ"]



def ensure_market_dirs(market: str, *, include_signal_dirs: bool = False) -> None:
    directories: list[str] = [
        get_market_results_root(market),
        get_market_screeners_root(market),
        get_market_runtime_root(market),
        get_augment_results_dir(market),
        get_markminervini_results_dir(market),
        get_qullamaggie_results_dir(market),
        get_leader_lagging_results_dir(market),
        get_tradingview_results_dir(market),
        get_weinstein_stage2_results_dir(market),
        get_earnings_cache_dir(market),
        get_financial_cache_dir(market),
    ]
    if include_signal_dirs:
        directories.extend(
            [
                get_market_signals_root(market),
                get_peg_imminent_results_dir(market),
                get_multi_screener_signals_results_dir(market),
            ]
        )
    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def _probe_writable_directory(directory: str) -> None:
    os.makedirs(directory, exist_ok=True)
    probe_path = os.path.join(directory, ".invest_proto_write_probe")
    with open(probe_path, "w", encoding="utf-8") as handle:
        handle.write("ok")
    try:
        os.remove(probe_path)
    except OSError:
        pass


def preflight_market_output_dirs(
    market: str,
    *,
    include_signal_dirs: bool = False,
    include_augment_dirs: bool = False,
) -> list[str]:
    normalized_market = require_market_key(market)
    directories: list[str] = [
        get_market_runtime_root(normalized_market),
        get_markminervini_results_dir(normalized_market),
        get_qullamaggie_results_dir(normalized_market),
        get_leader_lagging_results_dir(normalized_market),
        get_tradingview_results_dir(normalized_market),
        get_weinstein_stage2_results_dir(normalized_market),
    ]
    if include_augment_dirs:
        directories.append(get_augment_results_dir(normalized_market))
    if include_signal_dirs:
        directories.extend(
            [
                get_peg_imminent_results_dir(normalized_market),
                get_multi_screener_signals_results_dir(normalized_market),
            ]
        )
    checked: list[str] = []
    for directory in directories:
        try:
            _probe_writable_directory(directory)
        except OSError as exc:
            raise PermissionError(
                "Output preflight failed: "
                f"market={normalized_market}, directory={directory}, error={exc}. "
                "If the default results directory is locked, set INVEST_PROTO_RESULTS_DIR "
                "to a writable runtime output root."
            ) from exc
        checked.append(directory)
    return checked
