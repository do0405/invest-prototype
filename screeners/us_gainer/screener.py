# -*- coding: utf-8 -*-
"""US Gainers Screener.

This module screens US stocks for strong gains with high relative volume and earnings growth.
"""

import os
from typing import List, Dict

import pandas as pd

from config import DATA_US_DIR, US_GAINER_RESULTS_DIR
from utils import ensure_dir, fetch_market_cap, fetch_quarterly_eps_growth

US_GAINERS_RESULTS_PATH = os.path.join(US_GAINER_RESULTS_DIR, 'us_gainers_results.csv')




def screen_us_gainers() -> pd.DataFrame:
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
        if len(df) < 5:
            continue

        symbol = os.path.splitext(file)[0]

        last = df.iloc[-1]
        prev = df.iloc[-2]
        price = last['close']
        change_pct = (price / prev['close'] - 1) * 100
        if change_pct <= 2:
            continue

        volume = last['volume']
        avg_volume = df['volume'].tail(60).mean()
        rel_volume = volume / avg_volume if avg_volume else 0
        if rel_volume <= 2:
            continue

        market_cap = fetch_market_cap(symbol)
        if market_cap < 1_000_000_000:
            continue

        eps_growth = fetch_quarterly_eps_growth(symbol)
        if eps_growth <= 10:
            continue

        results.append({
            'symbol': symbol,
            'price': price,
            'change_pct': change_pct,
            'volume': volume,
            'relative_volume': rel_volume,
            'market_cap': market_cap,
            'eps_growth_qoq': eps_growth,
        })

    if results:
        df_res = pd.DataFrame(results)
        ensure_dir(US_GAINER_RESULTS_DIR)
        df_res.to_csv(US_GAINERS_RESULTS_PATH, index=False)
        df_res.to_json(US_GAINERS_RESULTS_PATH.replace('.csv', '.json'),
                       orient='records', indent=2)
        return df_res

    return pd.DataFrame()


if __name__ == '__main__':
    screen_us_gainers()
