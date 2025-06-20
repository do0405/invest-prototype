"""Common market utilities for screeners."""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Dict

import numpy as np
import pandas as pd

from config import DATA_US_DIR

# Sector ETF mapping used across screeners
SECTOR_ETFS = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Consumer Discretionary": "XLY",
    "Financials": "XLF",
    "Communication Services": "XLC",
    "Industrials": "XLI",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Materials": "XLB",
}

__all__ = ["get_vix_value", "calculate_sector_rs", "SECTOR_ETFS"]



def get_vix_value(data_dir: str = DATA_US_DIR) -> float:
    """Return latest VIX value from csv or 20.0 if unavailable."""
    vix_path = os.path.join(data_dir, "VIX.csv")
    if os.path.exists(vix_path):
        try:
            vix = pd.read_csv(vix_path)
            vix["date"] = pd.to_datetime(vix["date"])
            vix = vix.sort_values("date")
            if not vix.empty:
                return float(vix.iloc[-1]["close"])
        except Exception:
            pass
    return 20.0


def calculate_sector_rs(
    sector_etfs: Dict[str, str], data_dir: str = DATA_US_DIR
) -> Dict[str, Dict[str, float]]:
    """Calculate relative strength for each sector ETF."""
    rs: Dict[str, Dict[str, float]] = {}

    sp500_path = os.path.join(data_dir, "SPY.csv")
    if not os.path.exists(sp500_path):
        return rs
    try:
        sp500 = pd.read_csv(sp500_path)
        sp500["date"] = pd.to_datetime(sp500["date"])
        sp500 = sp500.sort_values("date")
        last_date = sp500["date"].max()
        three_months_ago = last_date - timedelta(days=90)
        sp500_3m = sp500[sp500["date"] >= three_months_ago]
        if sp500_3m.empty or len(sp500_3m) < 2:
            return rs
        sp500_return = (
            sp500_3m["close"].iloc[-1] / sp500_3m["close"].iloc[0] - 1
        ) * 100

        for sector, etf in sector_etfs.items():
            etf_path = os.path.join(data_dir, f"{etf}.csv")
            if not os.path.exists(etf_path):
                continue
            etf_data = pd.read_csv(etf_path)
            etf_data["date"] = pd.to_datetime(etf_data["date"])
            etf_data = etf_data.sort_values("date")
            etf_3m = etf_data[etf_data["date"] >= three_months_ago]
            if etf_3m.empty or len(etf_3m) < 2:
                continue
            etf_return = (
                etf_3m["close"].iloc[-1] / etf_3m["close"].iloc[0] - 1
            ) * 100
            if sp500_return > 0:
                rs_score = (etf_return / sp500_return) * 100
            else:
                rs_score = (
                    1 - (etf_return / sp500_return)
                ) * 100 if etf_return < 0 else 100
            rs[sector] = {"rs_score": rs_score, "return": etf_return}

        if rs:
            values = [v["rs_score"] for v in rs.values()]
            for sector in rs:
                percentile = np.percentile(
                    values,
                    np.searchsorted(np.sort(values), rs[sector]["rs_score"]) / len(values) * 100,
                )
                rs[sector]["percentile"] = percentile
        return rs
    except Exception:
        return rs
