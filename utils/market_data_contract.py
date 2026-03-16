from __future__ import annotations

import os
import re
from enum import Enum
from typing import Iterable, Optional

import pandas as pd

from config import DATA_KR_DIR, DATA_US_DIR
from .io_utils import safe_filename
from .typing_utils import is_na_like


class PricePolicy(str, Enum):
    RAW = "raw"
    SPLIT_ADJUSTED = "split_adjusted"
    TOTAL_RETURN_ADJUSTED = "total_return_adjusted"


CANONICAL_OHLCV_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

AUGMENTED_OHLCV_COLUMNS: tuple[str, ...] = (
    *CANONICAL_OHLCV_COLUMNS,
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "adj_close",
    "dividends",
    "stock_splits",
    "split_factor",
    "price_adjustment_factor",
    "price_adjustment_source",
    "price_policy",
)

OHLCV_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "date": ("date", "Date", "timestamp", "Timestamp", "datetime", "Datetime"),
    "open": ("open", "Open"),
    "high": ("high", "High"),
    "low": ("low", "Low"),
    "close": ("close", "Close"),
    "adj_close": ("adj_close", "Adj Close", "adj close", "adjusted_close", "adjusted close"),
    "volume": ("volume", "Volume"),
    "dividends": ("dividends", "Dividends", "dividend", "Dividend"),
    "stock_splits": ("stock_splits", "Stock Splits", "stock splits", "split", "splits"),
    "split_factor": ("split_factor", "Split Factor", "split factor"),
}

LEGACY_MOJIBAKE_ALIASES: dict[str, tuple[str, ...]] = {}


def normalize_market(market: str) -> str:
    market_key = str(market or "us").strip().lower()
    if not market_key:
        return "us"
    if not re.match(r"^[a-z0-9_-]+$", market_key):
        return "us"
    return market_key


def resolve_price_policy(price_policy: PricePolicy | str | None) -> PricePolicy:
    if isinstance(price_policy, PricePolicy):
        return price_policy
    normalized = str(price_policy or PricePolicy.SPLIT_ADJUSTED.value).strip().lower()
    for policy in PricePolicy:
        if normalized == policy.value:
            return policy
    return PricePolicy.SPLIT_ADJUSTED


def _normalize_trading_date(value: object) -> str | pd.NA:
    if is_na_like(value):
        return pd.NA

    parsed = None
    for kwargs in ({"format": "mixed"}, {}):
        try:
            parsed = pd.to_datetime(value, errors="raise", **kwargs)
            break
        except Exception:
            continue

    if parsed is None or is_na_like(parsed):
        return pd.NA
    return pd.Timestamp(parsed).date().isoformat()


def normalize_ohlcv_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame

    renamed = frame.copy()
    col_lookup = {str(col).strip().lower(): col for col in renamed.columns}

    for canonical, aliases in OHLCV_COLUMN_ALIASES.items():
        if canonical in renamed.columns:
            continue
        for alias in aliases:
            alias_key = str(alias).strip().lower()
            original = col_lookup.get(alias_key)
            if original is not None:
                renamed = renamed.rename(columns={original: canonical})
                col_lookup = {str(col).strip().lower(): col for col in renamed.columns}
                break

    return renamed


def _derive_split_factor(normalized: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    split_factor = pd.to_numeric(normalized.get("split_factor"), errors="coerce")

    factor = pd.Series(1.0, index=normalized.index, dtype=float)
    source = pd.Series("raw", index=normalized.index, dtype=object)

    valid_explicit = split_factor.notna() & (split_factor > 0)
    if valid_explicit.any():
        factor.loc[valid_explicit] = split_factor.loc[valid_explicit].astype(float)
        source.loc[valid_explicit] = "split_factor"

    return factor, source


def normalize_ohlcv_frame(
    frame: pd.DataFrame,
    symbol: str,
    *,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    normalized = normalize_ohlcv_columns(frame.copy())
    has_input_adj_close = "adj_close" in normalized.columns
    if "date" not in normalized.columns:
        normalized = normalized.reset_index()
        if len(normalized.columns) > 0:
            normalized = normalized.rename(columns={normalized.columns[0]: "date"})

    if "date" not in normalized.columns:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    normalized["date"] = normalized["date"].map(_normalize_trading_date)
    normalized["symbol"] = symbol

    raw_close_source = normalized["close"] if "close" in normalized.columns else pd.Series(0.0, index=normalized.index)
    for column in ("open", "high", "low", "close"):
        if column not in normalized.columns:
            normalized[column] = raw_close_source
    if "adj_close" not in normalized.columns:
        normalized["adj_close"] = raw_close_source
    if "volume" not in normalized.columns:
        normalized["volume"] = 0.0
    if "dividends" not in normalized.columns:
        normalized["dividends"] = 0.0
    if "stock_splits" not in normalized.columns:
        normalized["stock_splits"] = 0.0
    if "split_factor" not in normalized.columns:
        normalized["split_factor"] = pd.NA

    for column in ("open", "high", "low", "close", "adj_close", "volume", "dividends", "stock_splits", "split_factor"):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["raw_open"] = pd.to_numeric(normalized["open"], errors="coerce")
    normalized["raw_high"] = pd.to_numeric(normalized["high"], errors="coerce")
    normalized["raw_low"] = pd.to_numeric(normalized["low"], errors="coerce")
    normalized["raw_close"] = pd.to_numeric(normalized["close"], errors="coerce")
    normalized["adj_close"] = pd.to_numeric(normalized["adj_close"], errors="coerce")
    normalized["dividends"] = pd.to_numeric(normalized["dividends"], errors="coerce").fillna(0.0)
    normalized["stock_splits"] = pd.to_numeric(normalized["stock_splits"], errors="coerce").fillna(0.0)

    policy = resolve_price_policy(price_policy)
    split_factor, split_source = _derive_split_factor(normalized)
    adjustment_factor = pd.Series(1.0, index=normalized.index, dtype=float)
    adjustment_source = pd.Series("raw", index=normalized.index, dtype=object)
    valid_adj = normalized["adj_close"].notna() & normalized["raw_close"].notna() & (normalized["raw_close"] != 0)
    if policy == PricePolicy.SPLIT_ADJUSTED:
        valid_split = split_factor.notna() & (split_factor > 0)
        if valid_split.any():
            adjustment_factor.loc[valid_split] = split_factor.loc[valid_split].astype(float)
            adjustment_source.loc[valid_split] = split_source.loc[valid_split]
    elif policy == PricePolicy.TOTAL_RETURN_ADJUSTED:
        if has_input_adj_close and valid_adj.any():
            candidate = (normalized["adj_close"] / normalized["raw_close"]).replace([float("inf"), float("-inf")], pd.NA)
            candidate = pd.to_numeric(candidate, errors="coerce")
            valid_factor = valid_adj & candidate.notna() & (candidate > 0)
            adjustment_factor.loc[valid_factor] = candidate.loc[valid_factor].astype(float)
            adjustment_source.loc[valid_factor] = "adj_close_proxy"
        valid_split_fallback = (adjustment_source == "raw") & split_factor.notna() & (split_factor > 0)
        if valid_split_fallback.any():
            adjustment_factor.loc[valid_split_fallback] = split_factor.loc[valid_split_fallback].astype(float)
            adjustment_source.loc[valid_split_fallback] = split_source.loc[valid_split_fallback]

    normalized["split_factor"] = split_factor
    normalized["price_adjustment_factor"] = adjustment_factor
    normalized["price_adjustment_source"] = adjustment_source
    normalized["price_policy"] = policy.value

    if policy == PricePolicy.RAW:
        normalized["open"] = normalized["raw_open"]
        normalized["high"] = normalized["raw_high"]
        normalized["low"] = normalized["raw_low"]
        normalized["close"] = normalized["raw_close"]
    else:
        normalized["open"] = normalized["raw_open"] * normalized["price_adjustment_factor"]
        normalized["high"] = normalized["raw_high"] * normalized["price_adjustment_factor"]
        normalized["low"] = normalized["raw_low"] * normalized["price_adjustment_factor"]
        normalized["close"] = normalized["raw_close"] * normalized["price_adjustment_factor"]

    normalized = normalized[list(AUGMENTED_OHLCV_COLUMNS)]
    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    return normalized


def _market_dir(market: str) -> str:
    return DATA_KR_DIR if normalize_market(market) == "kr" else DATA_US_DIR


def _filter_as_of(frame: pd.DataFrame, as_of: str | None) -> pd.DataFrame:
    if frame.empty or not as_of:
        return frame
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    if is_na_like(as_of_ts):
        return frame
    scoped = frame.copy()
    scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
    scoped = scoped.dropna(subset=["date"])
    scoped = scoped[scoped["date"] <= as_of_ts]
    if scoped.empty:
        return pd.DataFrame(columns=frame.columns)
    scoped["date"] = scoped["date"].dt.strftime("%Y-%m-%d")
    return scoped.reset_index(drop=True)


def _candidate_paths(market: str, symbol: str) -> list[str]:
    symbol_key = str(symbol or "").strip()
    safe_symbol = safe_filename(symbol_key)
    data_dir = _market_dir(market)
    candidates = [
        os.path.join(data_dir, f"{symbol_key}.csv"),
        os.path.join(data_dir, f"{safe_symbol}.csv"),
    ]
    return list(dict.fromkeys(candidates))


def load_local_ohlcv_frame(
    market: str,
    symbol: str,
    as_of: str | None = None,
    *,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> pd.DataFrame:
    symbol_key = str(symbol or "").strip().upper()
    for path in _candidate_paths(market, symbol_key):
        if not os.path.exists(path):
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        normalized = normalize_ohlcv_frame(frame, symbol=symbol_key, price_policy=price_policy)
        if normalized.empty:
            continue
        return _filter_as_of(normalized, as_of)
    return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)


def _download_yfinance_ohlcv(
    symbol: str,
    as_of: str | None = None,
    *,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> pd.DataFrame:
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    symbol_key = str(symbol or "").strip().upper()
    if not symbol_key:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    try:
        ticker = yf.Ticker(symbol_key)
        history = ticker.history(period="3y", interval="1d", auto_adjust=False, actions=True)
    except Exception:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    normalized = normalize_ohlcv_frame(history, symbol=symbol_key, price_policy=price_policy)
    if normalized.empty:
        return normalized
    return _filter_as_of(normalized, as_of)


def load_market_ohlcv_frames(
    market: str,
    symbols: Iterable[str],
    as_of: str | None = None,
    *,
    allow_yfinance_fallback: bool = True,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for raw_symbol in symbols:
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol:
            continue
        frame = load_local_ohlcv_frame(market, symbol, as_of=as_of, price_policy=price_policy)
        if frame.empty and allow_yfinance_fallback:
            frame = _download_yfinance_ohlcv(symbol, as_of=as_of, price_policy=price_policy)
        if not frame.empty:
            frames[symbol] = frame
    return frames


def load_benchmark_data(
    market: str,
    candidates: list[str],
    as_of: str | None = None,
    *,
    allow_yfinance_fallback: bool = True,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> tuple[Optional[str], pd.DataFrame]:
    for candidate in candidates:
        symbol = str(candidate or "").strip().upper()
        if not symbol:
            continue
        frame = load_local_ohlcv_frame(market, symbol, as_of=as_of, price_policy=price_policy)
        if frame.empty and allow_yfinance_fallback:
            frame = _download_yfinance_ohlcv(symbol, as_of=as_of, price_policy=price_policy)
        if not frame.empty:
            return symbol, frame
    return None, pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)
