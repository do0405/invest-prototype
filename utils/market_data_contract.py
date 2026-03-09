from __future__ import annotations

import os
import re
from typing import Iterable, Optional

import pandas as pd

from config import DATA_KR_DIR, DATA_US_DIR
from .io_utils import safe_filename


CANONICAL_OHLCV_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

OHLCV_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "date": ("date", "Date", "timestamp", "Timestamp", "datetime", "Datetime", "일자", "날짜"),
    "open": ("open", "Open", "시가"),
    "high": ("high", "High", "고가"),
    "low": ("low", "Low", "저가"),
    "close": ("close", "Close", "종가", "adj close", "Adj Close"),
    "volume": ("volume", "Volume", "거래량"),
}

LEGACY_MOJIBAKE_ALIASES: dict[str, tuple[str, ...]] = {
    "date": ("??깆쁽", "?醫롮?"),
    "open": ("???",),
    "high": ("?⑥쥒?",),
    "low": ("??揶쎛",),
    "close": ("?ル굛?",),
    "volume": ("椰꾧퀡???",),
}


def normalize_market(market: str) -> str:
    market_key = str(market or "us").strip().lower()
    if not market_key:
        return "us"
    if not re.match(r"^[a-z0-9_-]+$", market_key):
        return "us"
    return market_key


def normalize_ohlcv_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame

    renamed = frame.copy()
    col_lookup = {str(col).strip().lower(): col for col in renamed.columns}

    for canonical, aliases in OHLCV_COLUMN_ALIASES.items():
        if canonical in renamed.columns:
            continue
        all_aliases = tuple(aliases) + tuple(LEGACY_MOJIBAKE_ALIASES.get(canonical, ()))
        for alias in all_aliases:
            alias_key = str(alias).strip().lower()
            original = col_lookup.get(alias_key)
            if original is not None:
                renamed = renamed.rename(columns={original: canonical})
                col_lookup = {str(col).strip().lower(): col for col in renamed.columns}
                break

    return renamed


def normalize_ohlcv_frame(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=CANONICAL_OHLCV_COLUMNS)

    normalized = normalize_ohlcv_columns(frame.copy())
    if "date" not in normalized.columns:
        normalized = normalized.reset_index()
        if len(normalized.columns) > 0:
            normalized = normalized.rename(columns={normalized.columns[0]: "date"})

    if "date" not in normalized.columns:
        return pd.DataFrame(columns=CANONICAL_OHLCV_COLUMNS)

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized["symbol"] = symbol

    for col in ("open", "high", "low", "close", "volume"):
        if col not in normalized.columns:
            if col == "volume":
                normalized[col] = 0
            elif "close" in normalized.columns:
                normalized[col] = normalized["close"]
            else:
                normalized[col] = 0

    normalized = normalized[list(CANONICAL_OHLCV_COLUMNS)]
    for col in ("open", "high", "low", "close", "volume"):
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")

    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    return normalized


def _market_dir(market: str) -> str:
    return DATA_KR_DIR if normalize_market(market) == "kr" else DATA_US_DIR


def _filter_as_of(frame: pd.DataFrame, as_of: str | None) -> pd.DataFrame:
    if frame.empty or not as_of:
        return frame
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(as_of_ts):
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


def load_local_ohlcv_frame(market: str, symbol: str, as_of: str | None = None) -> pd.DataFrame:
    symbol_key = str(symbol or "").strip().upper()
    for path in _candidate_paths(market, symbol_key):
        if not os.path.exists(path):
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        normalized = normalize_ohlcv_frame(frame, symbol=symbol_key)
        if normalized.empty:
            continue
        return _filter_as_of(normalized, as_of)
    return pd.DataFrame(columns=CANONICAL_OHLCV_COLUMNS)


def _download_yfinance_ohlcv(symbol: str, as_of: str | None = None) -> pd.DataFrame:
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return pd.DataFrame(columns=CANONICAL_OHLCV_COLUMNS)

    symbol_key = str(symbol or "").strip().upper()
    if not symbol_key:
        return pd.DataFrame(columns=CANONICAL_OHLCV_COLUMNS)

    try:
        ticker = yf.Ticker(symbol_key)
        history = ticker.history(period="3y", interval="1d", auto_adjust=False, actions=False)
    except Exception:
        return pd.DataFrame(columns=CANONICAL_OHLCV_COLUMNS)

    normalized = normalize_ohlcv_frame(history, symbol=symbol_key)
    if normalized.empty:
        return normalized
    return _filter_as_of(normalized, as_of)


def load_market_ohlcv_frames(
    market: str,
    symbols: Iterable[str],
    as_of: str | None = None,
    *,
    allow_yfinance_fallback: bool = True,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for raw_symbol in symbols:
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol:
            continue
        frame = load_local_ohlcv_frame(market, symbol, as_of=as_of)
        if frame.empty and allow_yfinance_fallback:
            frame = _download_yfinance_ohlcv(symbol, as_of=as_of)
        if not frame.empty:
            frames[symbol] = frame
    return frames


def load_benchmark_data(
    market: str,
    candidates: list[str],
    as_of: str | None = None,
    *,
    allow_yfinance_fallback: bool = True,
) -> tuple[Optional[str], pd.DataFrame]:
    for candidate in candidates:
        symbol = str(candidate or "").strip().upper()
        if not symbol:
            continue
        frame = load_local_ohlcv_frame(market, symbol, as_of=as_of)
        if frame.empty and allow_yfinance_fallback:
            frame = _download_yfinance_ohlcv(symbol, as_of=as_of)
        if not frame.empty:
            return symbol, frame
    return None, pd.DataFrame(columns=CANONICAL_OHLCV_COLUMNS)
