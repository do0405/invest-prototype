"""Pattern detection for VCP and Cup-with-Handle.

This module implements simple rule based detectors for
Volatility Contraction Pattern (VCP) and Cup-with-Handle
patterns as described in ``VCP & Cup handle.md``.
It replaces the previous contraction score approach.
"""

from __future__ import annotations

import os
from typing import Dict, List

import pandas as pd
import numpy as np
from scipy.signal import find_peaks, argrelextrema


# -----------------------------------------------------
# Detection helpers
# -----------------------------------------------------

def detect_vcp(df: pd.DataFrame) -> bool:
    """Return True if VCP conditions are satisfied."""
    if df is None or len(df) < 60:
        return False

    recent = df.tail(84).copy()  # up to 12 weeks
    recent['adr'] = (recent['high'] - recent['low']) / recent['close'] * 100
    recent['ma20'] = recent['close'].rolling(window=20).mean()
    recent['correction'] = recent['close'] < recent['ma20']

    correction_periods: List[List[int]] = []
    current: List[int] = []
    in_corr = False
    for i, row in recent.iterrows():
        if row['correction'] and not in_corr:
            in_corr = True
            current = [i]
        elif row['correction'] and in_corr:
            current.append(i)
        elif not row['correction'] and in_corr:
            in_corr = False
            if len(current) >= 5:
                correction_periods.append(current)
            current = []
    if in_corr and len(current) >= 5:
        correction_periods.append(current)

    if len(correction_periods) < 3:
        return False

    correction_periods = correction_periods[-3:]
    adr_vals = []
    lows = []
    vols = []
    for p in correction_periods:
        seg = recent.loc[p]
        adr_vals.append(seg['adr'].mean())
        lows.append(seg['low'].min())
        vols.append(seg['volume'].mean())

    if not (adr_vals[1] < adr_vals[0] * 0.8 and adr_vals[2] < adr_vals[1] * 0.8):
        return False
    if not (lows[1] > lows[0] and lows[2] > lows[1]):
        return False
    if not (vols[1] < vols[0] * 0.7 and vols[2] < vols[1] * 0.7):
        return False

    vol_ma20 = recent['volume'].rolling(window=20).mean().iloc[-1]
    if vols[-1] > vol_ma20 * 0.7:
        return False

    last_corr_len = len(correction_periods[-1])
    if not (5 <= last_corr_len <= 15):
        return False

    return True


def detect_cup_and_handle(df: pd.DataFrame, window: int = 180) -> bool:
    """Return True if cup-with-handle conditions are satisfied."""
    if len(df) < window:
        return False
    data = df.tail(window)
    prices = data['close'].values
    volumes = data['volume'].values
    peaks, _ = find_peaks(prices)
    troughs, _ = find_peaks(-prices)
    if len(peaks) < 2 or len(troughs) == 0:
        return False
    left = peaks[0]
    right_candidates = peaks[peaks > left]
    if len(right_candidates) == 0:
        return False
    right = right_candidates[-1]
    bottom_candidates = troughs[(troughs > left) & (troughs < right)]
    if len(bottom_candidates) == 0:
        return False
    bottom = bottom_candidates[prices[bottom_candidates].argmin()]

    if right - left < 35:  # cup duration at least 7 weeks
        return False

    left_high = prices[left]
    right_high = prices[right]
    bottom_low = prices[bottom]
    depth = min(left_high, right_high) - bottom_low
    if depth <= 0:
        return False
    depth_pct = depth / min(left_high, right_high) * 100
    if not (12 <= depth_pct <= 50):
        return False
    if abs(left_high - right_high) / min(left_high, right_high) * 100 > 5:
        return False

    handle_low = prices[right:].min()
    handle_depth = (right_high - handle_low) / depth * 100
    handle_len = len(prices) - right
    if not (3 <= handle_depth <= 33):
        return False
    if not (7 <= handle_len <= 28):
        return False

    avg_volume = data['volume'].rolling(window=20).mean().iloc[-1]
    bottom_vol = volumes[bottom-2:bottom+3].mean() if bottom >=2 else volumes[bottom]
    handle_vol = volumes[right:right+handle_len].mean() if handle_len >0 else volumes[right]
    if bottom_vol >= avg_volume * 0.5:
        return False
    if handle_vol >= bottom_vol:
        return False
    if volumes[-1] < avg_volume * 1.5:
        return False
    return True


# -----------------------------------------------------
# Batch analysis
# -----------------------------------------------------

def analyze_tickers_from_results(results_dir: str, data_dir: str, output_dir: str = "../results2") -> pd.DataFrame:
    """Analyze tickers from a csv and detect patterns."""
    os.makedirs(output_dir, exist_ok=True)
    results_file = os.path.join(results_dir, "advanced_financial_results.csv")
    if not os.path.exists(results_file):
        raise FileNotFoundError(f"결과 파일을 찾을 수 없습니다: {results_file}")

    results_df = pd.read_csv(results_file)
    analysis = []

    for _, row in results_df.iterrows():
        symbol = row['symbol']
        fin_met_count = row.get('fin_met_count', 0)
        if fin_met_count < 5:
            continue
        file_path = os.path.join(data_dir, f"{symbol}.csv")
        if not os.path.exists(file_path):
            continue
        df = pd.read_csv(file_path)
        date_col = next((c for c in df.columns if c.lower() in ['date', '날짜', '일자']), None)
        if not date_col:
            continue
        df[date_col] = pd.to_datetime(df[date_col])
        df.set_index(date_col, inplace=True)
        col_map = {'high': ['high', 'High', '고가'], 'low': ['low', 'Low', '저가'], 'close': ['close', 'Close', '종가'], 'volume': ['volume', 'Volume', '거래량']}
        found = {}
        for k, names in col_map.items():
            for c in df.columns:
                if c.lower() in [n.lower() for n in names]:
                    found[k] = c
                    break
        if len(found) < 4:
            continue
        df = df.rename(columns={v: k for k, v in found.items()})

        vcp = detect_vcp(df)
        cup = detect_cup_and_handle(df)
        if not vcp and not cup:
            continue

        analysis.append({
            'symbol': symbol,
            'fin_met_count': fin_met_count,
            'vcp': vcp,
            'cup_handle': cup,
        })

    if not analysis:
        return pd.DataFrame()

    out_df = pd.DataFrame(analysis)
    out_df = out_df.sort_values(['vcp', 'cup_handle'], ascending=False)
    out_file = os.path.join(output_dir, 'pattern_analysis_results.csv')
    out_df.to_csv(out_file, index=False, encoding='utf-8-sig')
    out_df.to_json(out_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
    return out_df

