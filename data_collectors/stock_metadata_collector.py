#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Collect sector and key financial metadata for US stocks.

The output ``data/stock_metadata.csv`` contains the following columns:
``symbol``, ``sector``, ``pe_ratio``, ``revenue_growth``, and ``market_cap``.
Symbols are derived from existing OHLCV files under ``data/us``.
"""

import os
import logging
import time
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

from config import (
    DATA_US_DIR,
    STOCK_METADATA_PATH,
    YAHOO_FINANCE_MAX_RETRIES,
    YAHOO_FINANCE_DELAY,
)
from utils import ensure_dir


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_metadata(symbol: str,
                   max_retries: int = YAHOO_FINANCE_MAX_RETRIES,
                   delay: int = YAHOO_FINANCE_DELAY) -> Dict[str, object]:
    """Return metadata for a given symbol using yfinance with retries."""
    for attempt in range(max_retries):
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
        except Exception as e:
            logger.warning(f"{symbol} 메타데이터 수집 실패 (시도 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))

    return {
        "symbol": symbol,
        "sector": "",
        "pe_ratio": None,
        "revenue_growth": None,
        "market_cap": None,
    }


def collect_stock_metadata(symbols: List[str], max_workers: int = 5) -> pd.DataFrame:
    """Collect metadata for a list of symbols concurrently."""
    records: List[Dict[str, object]] = []
    failures: List[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_metadata, sym): sym for sym in symbols}
        for future in as_completed(future_to_symbol):
            sym = future_to_symbol[future]
            try:
                data = future.result()
                if data["sector"] == "" and data["pe_ratio"] is None and data["market_cap"] is None:
                    failures.append(sym)
                records.append(data)
            except Exception as e:  # pragma: no cover - unexpected
                logger.error(f"{sym} 메타데이터 처리 오류: {e}")
                failures.append(sym)
    if failures:
        logger.warning(f"메타데이터 수집 실패 종목 {len(failures)}개: {', '.join(failures)}")
    return pd.DataFrame(records)


def get_symbols() -> List[str]:
    """Return list of tickers based on files under DATA_US_DIR."""
    files = [f for f in os.listdir(DATA_US_DIR) if f.endswith(".csv")]
    return [os.path.splitext(f)[0].split("_")[0] for f in files]


def main() -> None:
    ensure_dir(os.path.dirname(STOCK_METADATA_PATH))
    symbols = get_symbols()
    logger.info("메타데이터 수집 대상 종목 수: %d", len(symbols))
    df = collect_stock_metadata(symbols)
    df.to_csv(STOCK_METADATA_PATH, index=False)
    logger.info("✅ 메타데이터 저장 완료: %s (%d tickers)", STOCK_METADATA_PATH, len(df))


if __name__ == "__main__":
    main()
