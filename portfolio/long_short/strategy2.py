# -*- coding: utf-8 -*-
# 전략 2: 평균회귀 단일 숏 (Mean Reversion Short Single)

import os
import traceback
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 파일 임포트
from config import (
    DATA_DIR, DATA_US_DIR, 
    RESULTS_DIR, RESULTS_VER2_DIR
)

# 유틸리티 함수 임포트
from utils import (
    ensure_dir, extract_ticker_from_filename, 
    calculate_atr, calculate_historical_volatility,
    calculate_rsi, calculate_adx,
    check_sp500_condition, process_stock_data
)


def run_strategy2_screening(total_capital=100000, update_existing=False):
    """
    전략 2: 평균회귀 단일 숏 스크리닝
    
    Args:
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전략 2: 평균회귀 단일 숏 스크리닝 시작...")
    
    # 결과 파일 경로 - sell 폴더로 변경
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
    ensure_dir(sell_dir)
    result_file = os.path.join(sell_dir, 'strategy2_results.csv')
    
    try:
        # S&P 500 조건 확인
        sp500_condition = check_sp500_condition(DATA_US_DIR)
        if not sp500_condition:
            print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성 (첫 번째 위치)
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
            # JSON 파일 생성 추가
            json_file = result_file.replace('.csv', '.json')
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_json(json_file, orient='records', indent=2, force_ascii=False)
            
            return
            
        # 개별 CSV 파일 로드
        us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        if not us_files:
            print("❌ 미국 주식 데이터 파일이 없습니다.")
            return
            
        print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")
        
        # 개별 종목 처리
        results = []
        for i, file in enumerate(us_files):
            if i % 100 == 0 and i > 0:
                print(f"⏳ 진행 중: {i}/{len(us_files)} 종목 처리됨")
                
            # 데이터 처리
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=20, recent_days=20)
            if symbol is None or df is None or recent_data is None:
                continue
                
            # 스크리닝 조건들
            # 1. 10일 평균 종가가 5달러 이상
            if len(recent_data) < 10:
                continue
            avg_close_10 = recent_data['close'].tail(10).mean()
            if avg_close_10 < 5:
                continue
            
            # 2. 20일 평균 일일 거래대금이 2500만 달러 이상
            if len(recent_data) < 20:
                continue
            recent_data_copy = recent_data.copy()
            recent_data_copy['daily_value'] = recent_data_copy['close'] * recent_data_copy['volume']
            avg_daily_value_20 = recent_data_copy['daily_value'].tail(20).mean()
            if avg_daily_value_20 < 25_000_000:
                continue
            
            # 3. 10일 ATR이 종가의 13% 이상
            atr_10 = calculate_atr(recent_data, window=10)
            if len(atr_10) == 0 or pd.isna(atr_10.iloc[-1]) or atr_10.iloc[-1] < (recent_data['close'].iloc[-1] * 0.13):
                continue
            
            # 4. 3일 RSI가 90 이상
            if len(recent_data) < 3:
                continue
            rsi_3_series = calculate_rsi(recent_data, window=3)
            if rsi_3_series.empty or pd.isna(rsi_3_series.iloc[-1]):
                continue
            rsi_3 = rsi_3_series.iloc[-1]
            if rsi_3 < 90:
                continue
            
            # 5. 최근 2일간 종가가 연속으로 전일보다 높음
            if len(recent_data) < 3:
                continue
            recent_closes = recent_data['close'].tail(3).values
            if len(recent_closes) < 3 or recent_closes[-1] <= recent_closes[-2] or recent_closes[-2] <= recent_closes[-3]:
                continue
            
            # 조건 6: 7일 ADX 계산
            if len(recent_data) < 7:
                continue
            adx_7d_df = calculate_adx(recent_data, window=7)
            if 'adx' not in adx_7d_df.columns or pd.isna(adx_7d_df['adx'].iloc[-1]):
                adx_7d = 0
            else:
                adx_7d = adx_7d_df['adx'].iloc[-1]
            
            # 매매 정보 계산
            if len(recent_data) < 2:
                continue
            entry_price = recent_data['close'].iloc[-2] * 1.04  # 전일 종가의 4% 위
            stop_loss = entry_price + (atr_10.iloc[-1] * 3)  # 진입가 + (10일 ATR * 3)
            profit_target = entry_price * 0.96  # 진입가의 4% 아래
            
            # 포지션 크기 계산 (2% 리스크 기준, 총 자산의 10% 제한)
            total_capital = 100000  # 10만 달러 기준
            risk_amount = total_capital * 0.02  # 총 자본의 2%
            risk_per_share = stop_loss - entry_price
            if risk_per_share > 0:
                position_size = min(risk_amount / risk_per_share, total_capital * 0.1 / entry_price)
            else:
                position_size = total_capital * 0.1 / entry_price
             
             # 결과 저장
            results.append({
                 'symbol': ticker,
                 'entry_price': round(entry_price, 2),
                 'stop_loss': round(stop_loss, 2),
                 'profit_target': round(profit_target, 2),
                 'position_size': int(position_size),
                 'adx_7': round(adx_7d, 2),
                 'rsi_3': round(rsi_3, 2),
                 'atr_10': round(atr_10.iloc[-1], 4),
                 'avg_close_10': round(avg_close_10, 2),
                 'avg_daily_value_20': round(avg_daily_value_20, 0),
                 'adx_7d': adx_7d  # 정렬용
             })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            empty_columns = ['symbol', 'entry_price', 'stop_loss', 'profit_target', 'position_size', 'adx_7', 'rsi_3', 'atr_10', 'avg_close_10', 'avg_daily_value_20']
            pd.DataFrame(columns=empty_columns).to_csv(result_file, index=False, encoding='utf-8-sig')
            # JSON 파일 생성
            json_file = result_file.replace('.csv', '.json')
            pd.DataFrame(columns=empty_columns).to_json(json_file, orient='records', indent=2, force_ascii=False)
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # ADX 기준으로 내림차순 정렬
        result_df = result_df.sort_values('adx_7d', ascending=False)
        
        # 상위 10개 종목만 선택
        result_df = result_df.head(10)
        
        # 결과 CSV에 포함할 컬럼 선택
        columns_to_save = ['symbol', 'entry_price', 'stop_loss', 'profit_target', 'position_size', 'adx_7', 'rsi_3', 'atr_10', 'avg_close_10', 'avg_daily_value_20']
        result_df_to_save = result_df[columns_to_save]

        # 결과 저장
        result_df_to_save.to_csv(result_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = result_file.replace('.csv', '.json')
        result_df_to_save.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        print(f"✅ 전략 2 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        # 상위 종목 출력
        print("\n🏆 전략 2 상위 종목 (스크리닝 결과):")
        print(result_df_to_save)
        
        
    except Exception as e:
        print(f"❌ 전략 2 스크리닝 오류: {e}")
        print(traceback.format_exc())







# 메인 실행 부분

def run_strategy(total_capital=100000):
    """Wrapper function for main.py compatibility"""
    return run_strategy2_screening(total_capital=total_capital, update_existing=False)
