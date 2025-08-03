# -*- coding: utf-8 -*-
"""US Setup Formation Screener.

This module screens US stocks based on setup formation conditions.
It uses existing collected price data from ``data/us``.
"""

import os
from typing import List, Dict

import pandas as pd

from config import DATA_US_DIR, US_SETUP_RESULTS_DIR
from utils import ensure_dir, fetch_market_cap
from utils.screener_utils import save_screening_results, track_new_tickers, create_screener_summary, read_csv_flexible

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

def screen_us_setup() -> pd.DataFrame:
    results: List[Dict[str, float]] = []
    
    files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
    total_files = len(files)
    print(f"📊 총 {total_files}개 파일 처리 시작...")
    
    processed = 0
    for file in files:
        processed += 1
        if processed % 100 == 0:
            print(f"📈 진행률: {processed}/{total_files} ({processed/total_files*100:.1f}%)")
        file_path = os.path.join(DATA_US_DIR, file)
        try:
            df = read_csv_flexible(file_path, required_columns=['close', 'volume', 'date'])
            if df is None:
                continue
        except Exception:
            continue

        df.columns = [c.lower() for c in df.columns]
        
        # 필수 컬럼 존재 여부 확인
        required_columns = ['close', 'volume', 'date']
        if not all(col in df.columns for col in required_columns):
            continue
            
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

        market_cap = fetch_market_cap(symbol)
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

    print(f"✅ 처리 완료: {processed}개 파일, {len(results)}개 종목 발견")
    
    ensure_dir(US_SETUP_RESULTS_DIR)
    
    if results:
        df_res = pd.DataFrame(results)
        
        # 결과 저장 (JSON + CSV)
        results_paths = save_screening_results(
            results=results,
            output_dir=US_SETUP_RESULTS_DIR,
            filename_prefix="us_setup_results",
            include_timestamp=True,
            incremental_update=True
        )
        
        print(f"💾 결과 저장 완료: {results_paths['csv_path']}")
        
        # 새로운 티커 추적
        tracker_file = os.path.join(US_SETUP_RESULTS_DIR, "new_us_setup_tickers.csv")
        new_tickers = track_new_tickers(
            current_results=results,
            tracker_file=tracker_file,
            symbol_key='symbol',
            retention_days=14
        )
        
        # 요약 정보 생성
        summary = create_screener_summary(
            screener_name="US Setup",
            total_candidates=len(results),
            new_tickers=len(new_tickers),
            results_paths=results_paths
        )
        
        print(f"✅ US 셋업 스크리닝 완료: {len(df_res)}개 종목, 신규 {len(new_tickers)}개")
        return df_res
    else:
        # 빈 결과일 때도 칼럼명이 있는 빈 파일 생성
        empty_df = pd.DataFrame(columns=['symbol', 'price', 'market_cap', 'adr_percent', 'perf_1w_pct', 'perf_1m_pct', 'volume', 'avg_volume60'])
        
        # 빈 결과 저장
        results_paths = save_screening_results(
            results=[],
            output_dir=US_SETUP_RESULTS_DIR,
            filename_prefix="us_setup_results",
            include_timestamp=True,
            incremental_update=True
        )
        
        print(f"⚠️ 조건을 만족하는 종목이 없습니다. 빈 파일 생성: {results_paths['csv_path']}")
        return empty_df


