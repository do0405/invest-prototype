"""Common market utilities for screeners."""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Dict

import numpy as np
import pandas as pd

from config import DATA_US_DIR, OPTION_DATA_DIR

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



def get_vix_value(data_dir: str = OPTION_DATA_DIR) -> float | None:
    """Return latest VIX value from csv.``None`` if unavailable."""
    vix_path = os.path.join(data_dir, "vix.csv")
    if os.path.exists(vix_path):
        try:
            vix = pd.read_csv(vix_path)
            if vix.empty:
                return None
                
            vix.columns = [c.lower() for c in vix.columns]
            vix["date"] = pd.to_datetime(vix["date"], utc=True)
            vix = vix.sort_values("date")
            
            # VIX close 컬럼 찾기 (vix_close, close, adj_close 등)
            close_col = None
            for col in vix.columns:
                if "close" in col:
                    close_col = col
                    break
                    
            if close_col and not vix.empty:
                return float(vix.iloc[-1][close_col])
        except Exception as e:
            print(f"❌ VIX 데이터 로드 오류: {e}")
    return None


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
        
        # 컬럼명을 소문자로 변환
        sp500.columns = [c.lower() for c in sp500.columns]
        
        # 필수 컬럼 확인
        if "close" not in sp500.columns or "date" not in sp500.columns:
            return rs
            
        sp500["date"] = pd.to_datetime(sp500["date"], utc=True)
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
            
            # 컬럼명을 소문자로 변환
            etf_data.columns = [c.lower() for c in etf_data.columns]
            
            # 필수 컬럼 확인
            if "close" not in etf_data.columns or "date" not in etf_data.columns:
                continue
                
            etf_data["date"] = pd.to_datetime(etf_data["date"], utc=True)
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
