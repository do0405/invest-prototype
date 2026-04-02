from __future__ import annotations

import os

from config import (
    BASE_DIR,
    DATA_KR_DIR,
    DATA_US_DIR,
    EXTERNAL_DATA_DIR,
    RESULTS_DIR,
    STOCK_METADATA_PATH,
)
from .market_data_contract import normalize_market


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
    normalized = normalize_market(market)
    if normalized not in _MARKET_DATA_DIRS:
        raise ValueError(f"Unsupported market: {market}")
    return normalized



def market_key(market: str) -> str:
    normalized = normalize_market(market)
    if normalized not in _MARKET_DATA_DIRS:
        return "us"
    return normalized



def get_market_data_dir(market: str) -> str:
    return _MARKET_DATA_DIRS[market_key(market)]



def get_stock_metadata_path(market: str) -> str:
    normalized_market = market_key(market)
    if normalized_market == "us":
        return STOCK_METADATA_PATH
    return os.path.join(os.path.dirname(STOCK_METADATA_PATH), f"stock_metadata_{normalized_market}.csv")



def get_market_results_root(market: str) -> str:
    return os.path.join(RESULTS_DIR, market_key(market))



def get_market_screeners_root(market: str) -> str:
    return os.path.join(get_market_results_root(market), "screeners")


def get_market_intel_compat_root(market: str) -> str:
    base_root = os.environ.get("MARKET_INTEL_COMPAT_RESULTS_ROOT") or os.path.join(
        BASE_DIR, "..", "market-intel-core", "results", "compat", "invest_prototype"
    )
    return os.path.join(os.path.abspath(base_root), market_key(market))


def get_augment_results_dir(market: str) -> str:
    return os.path.join(get_market_screeners_root(market), "augment")



def get_market_signals_root(market: str) -> str:
    return os.path.join(get_market_results_root(market), "signals")



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
