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
import requests
from yahooquery import Ticker

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    DATA_US_DIR,
    STOCK_METADATA_PATH,
    YAHOO_FINANCE_MAX_RETRIES,
    YAHOO_FINANCE_DELAY,
)
from utils import ensure_dir

# Set user agent to avoid 401 errors
requests.packages.urllib3.disable_warnings()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_metadata_yahooquery(symbol: str, delay: float = 1.0) -> Dict[str, object]:
    """Fetch metadata using yahooquery as backup."""
    try:
        ticker = Ticker(symbol)
        time.sleep(delay)
        
        # Get summary detail and key stats
        summary = ticker.summary_detail
        key_stats = ticker.key_stats
        profile = ticker.summary_profile
        
        sector = ""
        pe_ratio = None
        market_cap = None
        revenue_growth = None
        
        # Extract sector from profile
        if isinstance(profile, dict) and symbol in profile:
            sector = profile[symbol].get('sector', '') or ''
        
        # Extract PE ratio from summary
        if isinstance(summary, dict) and symbol in summary:
            pe_ratio = summary[symbol].get('trailingPE', None)
            market_cap = summary[symbol].get('marketCap', None)
        
        # Extract revenue growth from key stats
        if isinstance(key_stats, dict) and symbol in key_stats:
            revenue_growth = key_stats[symbol].get('revenueQuarterlyGrowth', None)
        
        return {
            "symbol": symbol,
            "sector": sector,
            "pe_ratio": pe_ratio,
            "market_cap": market_cap,
            "revenue_growth": revenue_growth,
        }
    except Exception as e:
        logger.debug(f"{symbol} yahooquery 메타데이터 수집 실패: {str(e)[:100]}")
        return {
            "symbol": symbol,
            "sector": "",
            "pe_ratio": None,
            "market_cap": None,
            "revenue_growth": None,
        }


def fetch_metadata(symbol: str, max_retries: int = 1, delay: float = 2.0) -> Dict[str, object]:
    """Fetch metadata for a single symbol with yfinance first, then yahooquery backup."""
    # First try yfinance
    try:
        ticker = yf.Ticker(symbol)
        time.sleep(delay)
        info = ticker.info
        
        # Extract metadata with safe defaults
        sector = info.get("sector", "") or ""
        pe_ratio = info.get("trailingPE", None)
        market_cap = info.get("marketCap", None)
        revenue_growth = info.get("revenueGrowth", None)

        yf_data = {
            "symbol": symbol,
            "sector": sector,
            "pe_ratio": pe_ratio,
            "market_cap": market_cap,
            "revenue_growth": revenue_growth,
        }
        
        # Check if we got meaningful data from yfinance
        has_meaningful_data = (sector != "" or pe_ratio is not None or 
                             market_cap is not None or revenue_growth is not None)
        
        if has_meaningful_data:
            return yf_data
        else:
            # If yfinance data is empty, try yahooquery
            logger.debug(f"{symbol} yfinance 데이터 부족, yahooquery로 보완 시도")
            yq_data = fetch_metadata_yahooquery(symbol, delay=1.0)
            
            # Merge data, preferring non-empty values
            merged_data = {
                "symbol": symbol,
                "sector": yq_data["sector"] if yq_data["sector"] != "" else yf_data["sector"],
                "pe_ratio": yq_data["pe_ratio"] if yq_data["pe_ratio"] is not None else yf_data["pe_ratio"],
                "market_cap": yq_data["market_cap"] if yq_data["market_cap"] is not None else yf_data["market_cap"],
                "revenue_growth": yq_data["revenue_growth"] if yq_data["revenue_growth"] is not None else yf_data["revenue_growth"],
            }
            return merged_data
            
    except Exception as e:
        error_msg = str(e)
        if "401" in error_msg or "Unauthorized" in error_msg:
            logger.debug(f"{symbol} yfinance API 제한, yahooquery로 시도")
        else:
            logger.debug(f"{symbol} yfinance 실패: {error_msg[:50]}, yahooquery로 시도")
        
        # If yfinance fails, try yahooquery
        yq_data = fetch_metadata_yahooquery(symbol, delay=1.0)
        return yq_data


def collect_stock_metadata(symbols: List[str], max_workers: int = 2) -> pd.DataFrame:
    """Collect metadata for a list of symbols with reduced concurrency."""
    records: List[Dict[str, object]] = []
    successful_count = 0
    
    logger.info(f"메타데이터 수집 시작: {len(symbols)}개 종목")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_metadata, sym): sym for sym in symbols}
        for i, future in enumerate(as_completed(future_to_symbol)):
            sym = future_to_symbol[future]
            try:
                data = future.result()
                records.append(data)
                # Count successful data collection
                if data["sector"] != "" or data["pe_ratio"] is not None or data["market_cap"] is not None:
                    successful_count += 1
                
                # Progress logging every 100 symbols
                if (i + 1) % 100 == 0:
                    logger.info(f"진행률: {i + 1}/{len(symbols)} ({successful_count}개 성공)")
                    
            except Exception as e:
                logger.error(f"{sym} 메타데이터 처리 오류: {e}")
                # Add empty record for failed symbol
                records.append({
                    "symbol": sym,
                    "sector": "",
                    "pe_ratio": None,
                    "market_cap": None,
                    "revenue_growth": None,
                })
    
    logger.info(f"메타데이터 수집 완료: {successful_count}/{len(symbols)}개 성공")
    return pd.DataFrame(records)


def get_symbols() -> List[str]:
    """Return list of tickers based on files under DATA_US_DIR, filtering out derivatives."""
    files = [f for f in os.listdir(DATA_US_DIR) if f.endswith(".csv")]
    symbols = [os.path.splitext(f)[0].split("_")[0] for f in files]
    
    # Filter out derivatives and problematic symbols
    filtered_symbols = []
    for symbol in symbols:
        # Skip warrants (.W), units (.U), rights (.RT), preferred stocks (.PR)
        if any(symbol.endswith(suffix) for suffix in ['.W', '.U', '.RT', '.PR', '.WS']):
            continue
        # Skip symbols with $ (currency symbols)
        if '$' in symbol:
            continue
        # Skip very short symbols (likely ETFs or special cases)
        if len(symbol) < 2:
            continue
        # Skip symbols with numbers (often temporary or special listings)
        if any(char.isdigit() for char in symbol):
            continue
        filtered_symbols.append(symbol)
    
    logger.info(f"필터링 전 종목 수: {len(symbols)}, 필터링 후: {len(filtered_symbols)}")
    return filtered_symbols


def main() -> None:
    ensure_dir(os.path.dirname(STOCK_METADATA_PATH))
    symbols = get_symbols()
    logger.info("메타데이터 수집 대상 종목 수: %d", len(symbols))
    
    df = collect_stock_metadata(symbols)
    df.to_csv(STOCK_METADATA_PATH, index=False)
    logger.info("✅ 메타데이터 저장 완료: %s (%d tickers)", STOCK_METADATA_PATH, len(df))
    
    # Show summary of collected data
    successful_data = df[(df['sector'] != '') | (df['pe_ratio'].notna()) | (df['market_cap'].notna())]
    logger.info("성공적으로 수집된 데이터: %d/%d (%.1f%%)", len(successful_data), len(df), len(successful_data)/len(df)*100)
    
    # Show sector distribution
    sector_counts = df[df['sector'] != '']['sector'].value_counts().head(10)
    if len(sector_counts) > 0:
        logger.info("주요 섹터 분포:\n%s", sector_counts.to_string())



if __name__ == "__main__":
    main()
