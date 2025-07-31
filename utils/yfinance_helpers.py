"""Helper functions wrapping yfinance to avoid duplication."""

from __future__ import annotations

import pandas as pd
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
        # Use quarterly_income_stmt instead of deprecated quarterly_earnings
        q_income = ticker.quarterly_income_stmt
        if q_income is None or q_income.empty or len(q_income.columns) < 2:
            return 0.0
        
        # Get Net Income row (which represents earnings)
        if "Net Income" not in q_income.index:
            return 0.0
            
        net_income_row = q_income.loc["Net Income"]
        # Get the two most recent quarters
        recent_earnings = net_income_row.iloc[0]  # Most recent quarter
        prev_earnings = net_income_row.iloc[1]    # Previous quarter
        
        if prev_earnings == 0 or pd.isna(recent_earnings) or pd.isna(prev_earnings):
            return 0.0
        
        if prev_earnings > 0:
            return (recent_earnings - prev_earnings) / prev_earnings * 100
        else:
            if recent_earnings >= 0:
                return 200  # 흑자 전환
            else:
                return (recent_earnings - prev_earnings) / abs(prev_earnings) * 100
    except Exception:
        return 0.0
