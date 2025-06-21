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
    """`VCP & Cup handle.md` 기준 VCP 패턴 판별"""
    if df is None or len(df) < 60:
        return False

    recent = df.tail(90).copy()
    recent["ma20"] = recent["close"].rolling(20).mean()
    recent["above_ma"] = recent["close"] >= recent["ma20"]

    periods: List[Tuple[int, int]] = []
    start = None
    for i, above in enumerate(recent["above_ma"]):
        if not above and start is None:
            start = i
        elif above and start is not None:
            if i - start >= 5:
                periods.append((start, i - 1))
            start = None
    if start is not None and len(recent) - start >= 5:
        periods.append((start, len(recent) - 1))

    if len(periods) < 3:
        return False

    periods = periods[-4:]
    if not (42 <= periods[-1][1] - periods[0][0] + 1 <= 84):
        return False

    depth_ranges = [(15, 25), (8, 15), (3, 8), (1, 5)]
    volumes = []
    highs = recent["high"].values
    lows = recent["low"].values
    closes = recent["close"].values
    vol = recent["volume"].values
    ma20_vol = recent["volume"].rolling(20).mean().values

    prev_peak = highs[periods[0][0] - 1] if periods[0][0] > 0 else highs[periods[0][0]]
    for idx, (start_idx, end_idx) in enumerate(periods):
        if idx >= len(depth_ranges):
            break
        if not (5 <= end_idx - start_idx + 1 <= 21):
            return False
        low = lows[start_idx:end_idx + 1].min()
        depth = (prev_peak - low) / prev_peak * 100
        min_d, max_d = depth_ranges[idx]
        if not (min_d <= depth <= max_d):
            return False
        avg_vol = vol[start_idx:end_idx + 1].mean()
        volumes.append(avg_vol)
        prev_peak = highs[end_idx]

    for i in range(1, len(volumes)):
        ratio = volumes[i] / volumes[i - 1]
        if not (0.5 <= ratio <= 0.8):
            return False

    if volumes[-1] > ma20_vol[periods[-1][1]] * 0.7:
        return False

    pivot_high = prev_peak
    last_close = closes[-1]
    if last_close < pivot_high * 0.95:
        return False
    if (pivot_high - lows[periods[-1][1]:].min()) / pivot_high * 100 > 8:
        return False

    side = closes[-15:]
    if side.max() / side.min() - 1 > 0.05:
        return False
    if vol[-1] < ma20_vol[-1] * 1.5:
        return False

    return True


def detect_cup_and_handle(df: pd.DataFrame, window: int = 180) -> bool:
    """`VCP & Cup handle.md` 기준 Cup-with-Handle 패턴 판별"""
    if len(df) < window:
        return False

    data = df.tail(window).copy()
    prices = data["close"].values
    volumes = data["volume"].values
    avg_volume = data["volume"].rolling(20).mean().values

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

    if right - left < 35:
        return False
    if bottom - left < 10 or right - bottom < 10:
        return False

    left_high = prices[left]
    right_high = prices[right]
    if abs(left_high - right_high) / min(left_high, right_high) * 100 > 5:
        return False

    bottom_low = prices[bottom]
    depth = (min(left_high, right_high) - bottom_low) / min(left_high, right_high) * 100
    if not (12 <= depth <= 50):
        return False

    handle_low = prices[right:].min()
    handle_depth = (right_high - handle_low) / right_high * 100
    handle_len = len(prices) - right
    if not (3 <= handle_depth <= 33):
        return False
    if not (7 <= handle_len <= 28):
        return False
    if len(prices) - left < 49:
        return False

    bottom_vol = volumes[max(0, bottom - 2): bottom + 3].mean()
    handle_vol = volumes[right: right + handle_len].mean() if handle_len > 0 else volumes[right]

    if bottom_vol >= avg_volume[bottom] * 0.5:
        return False
    if handle_vol >= bottom_vol:
        return False
    if volumes[-1] < avg_volume[-1] * 1.5:
        return False

    cup_volume_trend = pd.Series(volumes[left:right]).rolling(5).mean()
    if cup_volume_trend.iloc[-1] >= cup_volume_trend.iloc[0]:
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

