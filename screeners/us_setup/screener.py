# -*- coding: utf-8 -*-
"""US Setup Formation Screener.

This module screens US stocks based on setup formation conditions.
It uses existing collected price data from ``data/us``.
"""

import os
from typing import List, Dict

import pandas as pd
import yfinance as yf

from config import DATA_US_DIR, US_SETUP_RESULTS_DIR
from utils import ensure_dir

US_SETUP_RESULTS_PATH = os.path.join(US_SETUP_RESULTS_DIR, 'us_setup_results.csv')


def _calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['bb_mid'] = df['close'].rolling(window=20).mean()
    df['bb_std'] = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['bb_mid'] + 2 * df['bb_std']
    return df


def _fetch_market_cap(symbol: str) -> int:
    try:
        info = yf.Ticker(symbol).info
        return int(info.get('marketCap', 0) or 0)
    except Exception:
        return 0


def screen_us_setup() -> pd.DataFrame:
    results: List[Dict[str, float]] = []

    for file in os.listdir(DATA_US_DIR):
        if not file.endswith('.csv'):
            continue
        file_path = os.path.join(DATA_US_DIR, file)
        try:
            df = pd.read_csv(file_path)
        except Exception:
            continue

        df.columns = [c.lower() for c in df.columns]
        if len(df) < 60:
            continue

        symbol = os.path.splitext(file)[0]
        df = _calculate_indicators(df)

        last = df.iloc[-1]
        price = last['close']
        if price <= 3:
            continue

        volume = last['volume']
        if volume <= 100_000:
            continue
        avg_volume60 = df['volume'].tail(60).mean()
        if avg_volume60 <= 300_000:
            continue

        market_cap = _fetch_market_cap(symbol)
        if market_cap < 500_000_000:
            continue

        adr = (last['high'] - last['low']) / price * 100
        if adr <= 3:
            continue

        if len(df) < 21:
            continue
        perf_1w = (price / df['close'].iloc[-5] - 1) * 100
        if not -25 <= perf_1w <= 25:
            continue
        perf_1m = (price / df['close'].iloc[-21] - 1) * 100
        if perf_1m <= 10:
            continue

        if not (last['ema50'] < last['ema20'] < last['ema10']):
            continue

        if not last['bb_upper'] > price:
            continue

        results.append({
            'symbol': symbol,
            'price': price,
            'market_cap': market_cap,
            'adr_percent': adr,
            'perf_1w_pct': perf_1w,
            'perf_1m_pct': perf_1m,
            'volume': volume,
            'avg_volume60': avg_volume60,
        })

    if results:
        df_res = pd.DataFrame(results)
        ensure_dir(US_SETUP_RESULTS_DIR)
        df_res.to_csv(US_SETUP_RESULTS_PATH, index=False)
        df_res.to_json(US_SETUP_RESULTS_PATH.replace('.csv', '.json'),
                       orient='records', indent=2)
        return df_res

    return pd.DataFrame()


if __name__ == '__main__':
    screen_us_setup()
