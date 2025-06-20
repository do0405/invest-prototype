#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Collect sector and key financial metadata for US stocks.

The output ``data/stock_metadata.csv`` contains the following columns:
``symbol``, ``sector``, ``pe_ratio``, ``revenue_growth``, and ``market_cap``.
Symbols are derived from existing OHLCV files under ``data/us``.
"""

import os
from typing import Dict, List

import pandas as pd
import yfinance as yf

from config import DATA_US_DIR, STOCK_METADATA_PATH
from utils import ensure_dir


def fetch_metadata(symbol: str) -> Dict[str, object]:
    """Return metadata for a given symbol using yfinance."""
    try:
        tkr = yf.Ticker(symbol)
        info = tkr.info
        sector = info.get("sector", "") or ""
        pe = info.get("trailingPE") or info.get("forwardPE")
        market_cap = info.get("marketCap")

        revenue_growth = None
        try:
            fin = tkr.financials
            if fin is not None and not fin.empty and "Total Revenue" in fin.index:
                revs = fin.loc["Total Revenue"].dropna()
                if len(revs) >= 2 and revs.iloc[1] != 0:
                    revenue_growth = (revs.iloc[0] - revs.iloc[1]) / abs(revs.iloc[1]) * 100
        except Exception:
            revenue_growth = None

        return {
            "symbol": symbol,
            "sector": sector,
            "pe_ratio": pe,
            "revenue_growth": revenue_growth,
            "market_cap": market_cap,
        }
    except Exception:
        return {
            "symbol": symbol,
            "sector": "",
            "pe_ratio": None,
            "revenue_growth": None,
            "market_cap": None,
        }


def collect_stock_metadata(symbols: List[str]) -> pd.DataFrame:
    """Collect metadata for a list of symbols."""
    records = []
    for sym in symbols:
        records.append(fetch_metadata(sym))
    return pd.DataFrame(records)


def get_symbols() -> List[str]:
    """Return list of tickers based on files under DATA_US_DIR."""
    files = [f for f in os.listdir(DATA_US_DIR) if f.endswith(".csv")]
    return [os.path.splitext(f)[0].split("_")[0] for f in files]


def main() -> None:
    ensure_dir(os.path.dirname(STOCK_METADATA_PATH))
    symbols = get_symbols()
    df = collect_stock_metadata(symbols)
    df.to_csv(STOCK_METADATA_PATH, index=False)
    print(f"✅ Metadata saved to {STOCK_METADATA_PATH} ({len(df)} tickers)")


if __name__ == "__main__":
    main()
