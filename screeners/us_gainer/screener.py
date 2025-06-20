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
    print("📊 US Gainers 스크리너 시작...")
    print(f"📁 데이터 디렉토리: {DATA_US_DIR}")
    
    results: List[Dict[str, float]] = []
    total_files = len([f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')])
    processed_files = 0
    qualified_stocks = 0
    
    print(f"📈 총 {total_files}개 종목 분석 시작...")

    for file in os.listdir(DATA_US_DIR):
        if not file.endswith('.csv'):
            continue
            
        processed_files += 1
        if processed_files % 100 == 0:
            print(f"⏳ 진행률: {processed_files}/{total_files} ({processed_files/total_files*100:.1f}%) - 조건 만족: {qualified_stocks}개")
            
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

        print(f"🔍 {symbol}: 가격변동 {change_pct:.1f}%, 상대거래량 {rel_volume:.1f}x - 시가총액 및 EPS 성장률 확인 중...")
        
        market_cap = fetch_market_cap(symbol)
        if market_cap < 1_000_000_000:
            print(f"❌ {symbol}: 시가총액 부족 (${market_cap:,.0f})")
            continue

        eps_growth = fetch_quarterly_eps_growth(symbol)
        if eps_growth <= 10:
            print(f"❌ {symbol}: EPS 성장률 부족 ({eps_growth:.1f}%)")
            continue

        qualified_stocks += 1
        print(f"✅ {symbol}: 모든 조건 만족! (가격변동: {change_pct:.1f}%, 상대거래량: {rel_volume:.1f}x, 시가총액: ${market_cap:,.0f}, EPS성장: {eps_growth:.1f}%)")
        
        results.append({
            'symbol': symbol,
            'price': price,
            'change_pct': change_pct,
            'volume': volume,
            'relative_volume': rel_volume,
            'market_cap': market_cap,
            'eps_growth_qoq': eps_growth,
        })

    print(f"\n📊 스크리닝 완료: {processed_files}개 종목 분석, {qualified_stocks}개 종목이 조건 만족")
    
    if results:
        df_res = pd.DataFrame(results)
        ensure_dir(US_GAINER_RESULTS_DIR)
        print(f"💾 결과 저장 중: {US_GAINERS_RESULTS_PATH}")
        df_res.to_csv(US_GAINERS_RESULTS_PATH, index=False)
        df_res.to_json(US_GAINERS_RESULTS_PATH.replace('.csv', '.json'),
                       orient='records', indent=2)
        print(f"✅ 결과 저장 완료: {len(df_res)}개 종목")
        return df_res
    else:
        print("⚠️ 조건을 만족하는 종목이 없습니다.")

    return pd.DataFrame()


if __name__ == '__main__':
    screen_us_gainers()
