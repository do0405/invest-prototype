"""Helper functions wrapping yfinance to avoid duplication."""

from __future__ import annotations

import yfinance as yf

__all__ = ["fetch_market_cap", "fetch_quarterly_eps_growth"]


def fetch_market_cap(symbol: str) -> int:
    """Return market cap of symbol in USD, 0 if unavailable."""
    try:
        info = yf.Ticker(symbol).info
        return int(info.get("marketCap", 0) or 0)
    except Exception:
        return 0


def fetch_quarterly_eps_growth(symbol: str) -> float:
    """Return quarter-over-quarter EPS growth percent."""
    try:
        ticker = yf.Ticker(symbol)
        q_earnings = ticker.quarterly_earnings
        if q_earnings is None or len(q_earnings) < 2:
            return 0.0
        recent_eps = q_earnings.iloc[-1]["Earnings"]
        prev_eps = q_earnings.iloc[-2]["Earnings"]
        if prev_eps == 0:
            return 0.0
        return (recent_eps - prev_eps) / abs(prev_eps) * 100
    except Exception:
        return 0.0
