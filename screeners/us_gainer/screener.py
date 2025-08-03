# -*- coding: utf-8 -*-
"""US Gainers Screener.

This module screens US stocks for strong gains with high relative volume and earnings growth.
"""

import os
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from config import DATA_US_DIR, US_GAINER_RESULTS_DIR
from utils import ensure_dir, fetch_market_cap, fetch_quarterly_eps_growth
from utils.screener_utils import save_screening_results, track_new_tickers, create_screener_summary, read_csv_flexible

US_GAINERS_RESULTS_PATH = os.path.join(US_GAINER_RESULTS_DIR, 'us_gainers_results.csv')




def screen_us_gainers() -> pd.DataFrame:
    print("📊 US Gainers 스크리너 시작...")
    print(f"📁 데이터 디렉토리: {DATA_US_DIR}")
    
    results: List[Dict[str, float]] = []
    total_files = len([f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')])
    processed_files = 0
    qualified_stocks = 0
    
    print(f"📈 총 {total_files}개 종목 분석 시작...")

    def process_file(file):
        """개별 파일 처리 함수"""
        if not file.endswith('.csv'):
            return None
            
        file_path = os.path.join(DATA_US_DIR, file)
        try:
            df = read_csv_flexible(file_path, required_columns=['close', 'volume', 'date'])
            if df is None:
                return None
        except Exception:
            return None

        df.columns = [c.lower() for c in df.columns]
        if len(df) < 5:
            return None

        symbol = os.path.splitext(file)[0]

        last = df.iloc[-1]
        prev = df.iloc[-2]
        price = last['close']
        change_pct = (price / prev['close'] - 1) * 100
        if change_pct <= 2:
            return None

        volume = last['volume']
        avg_volume = df['volume'].tail(60).mean()
        rel_volume = volume / avg_volume if avg_volume else 0
        if rel_volume <= 2:
            return None

        print(f"🔍 {symbol}: 가격변동 {change_pct:.1f}%, 상대거래량 {rel_volume:.1f}x - 시가총액 및 EPS 성장률 확인 중...")
        
        market_cap = fetch_market_cap(symbol)
        if market_cap < 1_000_000_000:
            print(f"❌ {symbol}: 시가총액 부족 (${market_cap:,.0f})")
            return None

        eps_growth = fetch_quarterly_eps_growth(symbol)
        if eps_growth <= 10:
            print(f"❌ {symbol}: EPS 성장률 부족 ({eps_growth:.1f}%)")
            return None

        print(f"✅ {symbol}: 모든 조건 만족! (가격변동: {change_pct:.1f}%, 상대거래량: {rel_volume:.1f}x, 시가총액: ${market_cap:,.0f}, EPS성장: {eps_growth:.1f}%)")
        
        return {
            'symbol': symbol,
            'price': price,
            'change_pct': change_pct,
            'volume': volume,
            'relative_volume': rel_volume,
            'market_cap': market_cap,
            'eps_growth_qoq': eps_growth,
        }
    
    # 병렬 처리 실행 (스레드 안전성 보장)
    csv_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
    max_workers = min(4, len(csv_files))  # 최대 4개 워커
    all_results = []  # 모든 결과를 임시로 저장
    temp_processed_files = 0
    temp_qualified_stocks = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 작업 제출
        future_to_file = {executor.submit(process_file, file): file for file in csv_files}
        
        # 결과 수집 (스레드 안전)
        for future in as_completed(future_to_file):
            temp_processed_files += 1
            
            if temp_processed_files % 100 == 0:
                print(f"⏳ 진행률: {temp_processed_files}/{total_files} ({temp_processed_files/total_files*100:.1f}%) - 조건 만족: {temp_qualified_stocks}개")
            
            try:
                result = future.result()
                if result is not None:
                    all_results.append(result)
                    temp_qualified_stocks += 1
            except Exception as e:
                file_name = future_to_file[future]
                print(f"⚠️ {file_name} 처리 중 오류: {e}")
    
    # 결과 병합 (메인 스레드에서 안전하게 처리)
    results.extend(all_results)
    processed_files = temp_processed_files
    qualified_stocks = temp_qualified_stocks

    print(f"\n📊 스크리닝 완료: {processed_files}개 종목 분석, {qualified_stocks}개 종목이 조건 만족")
    
    ensure_dir(US_GAINER_RESULTS_DIR)
    
    if results:
        df_res = pd.DataFrame(results)
        
        # 결과 저장 (JSON + CSV)
        results_paths = save_screening_results(
            results=results,
            output_dir=US_GAINER_RESULTS_DIR,
            filename_prefix="us_gainers_results",
            include_timestamp=True,
            incremental_update=True
        )
        
        # 새로운 티커 추적
        tracker_file = os.path.join(US_GAINER_RESULTS_DIR, "new_us_gainer_tickers.csv")
        new_tickers = track_new_tickers(
            current_results=results,
            tracker_file=tracker_file,
            symbol_key='symbol',
            retention_days=14
        )
        
        # 요약 정보 생성
        summary = create_screener_summary(
            screener_name="US Gainers",
            total_candidates=len(results),
            new_tickers=len(new_tickers),
            results_paths=results_paths
        )
        
        print(f"✅ US 게이너 스크리닝 완료: {len(df_res)}개 종목, 신규 {len(new_tickers)}개")
        return df_res
    else:
        # 빈 결과일 때도 칼럼명이 있는 빈 파일 생성
        empty_df = pd.DataFrame(columns=['symbol', 'price', 'change_pct', 'volume', 'relative_volume', 'market_cap', 'eps_growth_qoq'])
        empty_df.to_csv(US_GAINERS_RESULTS_PATH, index=False)
        empty_df.to_json(US_GAINERS_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2)
        print(f"⚠️ 조건을 만족하는 종목이 없습니다. 빈 파일 생성: {US_GAINERS_RESULTS_PATH}")
        return empty_df


