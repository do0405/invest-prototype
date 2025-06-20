# -*- coding: utf-8 -*-
# 전략 1: 트렌드 하이 모멘텀 롱 (Long Trend High Momentum)

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
    check_sp500_condition, process_stock_data
)


def run_strategy1_screening(total_capital=100000, update_existing=False):
    """
    전략 1: 트렌드 하이 모멘텀 롱 스크리닝
    
    Args:
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전략 1: 트렌드 하이 모멘텀 롱 스크리닝 시작...")
    
    # 결과 파일 경로 - buy 폴더로 변경
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy1_results.csv')
    
    try:
        # SPY 데이터 로드 및 조건 확인
        spy_condition = check_sp500_condition(DATA_US_DIR)
        if not spy_condition:
            print("❌ SPY 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, mode='w', encoding='utf-8-sig')
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
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=200, recent_days=200)
            if symbol is None or df is None:
                continue
                
            # 최근 데이터 추출
            recent_data = df.iloc[-200:].copy()
            
            # 조건 1: 최근 10일 평균 종가가 5달러 이상
            recent_10d = recent_data.iloc[-10:]
            avg_price_10d = recent_10d['close'].mean()
            if avg_price_10d < 5.0:
                continue
            
            # 조건 2: 직전 20일 기준 일 평균 거래 금액이 5000만 달러 초과
            recent_20d = recent_data.iloc[-20:]
            avg_volume_value = (recent_20d['close'] * recent_20d['volume']).mean()
            if avg_volume_value <= 50000000:  # 5000만 달러
                continue
            
            # 조건 3: 25일 이동평균 > 50일 이동평균
            recent_data['ma25'] = recent_data['close'].rolling(window=25).mean()
            recent_data['ma50'] = recent_data['close'].rolling(window=50).mean()
            latest = recent_data.iloc[-1]
            if latest['ma25'] <= latest['ma50']:
                continue
            
            # 변동성 계산 (200일 기준)
            volatility = calculate_historical_volatility(recent_data, window=200).iloc[-1]
            
            # 200일 상승률 계산
            price_change_200d = ((recent_data['close'].iloc[-1] - recent_data['close'].iloc[0]) / recent_data['close'].iloc[0]) * 100
            
            # ATR 계산 (직전 20일 기준)
            atr_20d = calculate_atr(recent_data.iloc[-20:], window=20).iloc[-1]
            
            # 매수가 (시가) 설정
            entry_price = recent_data.iloc[-1]['open']
            
            # 손절매: 매수가 기준 직전 20일 ATR의 5배 위 지점
            stop_loss = entry_price - (atr_20d * 5)
            
            # 수익보호: 매수가 기준 25%의 trailing stop loss
            profit_protection_trailing_stop = entry_price * 0.75  # 매수가의 75% 지점 (25% 하락)
            
            # 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 10% 중 min 값
            risk_amount = entry_price - stop_loss
            if risk_amount <= 0:  # 위험 금액이 0 이하인 경우 처리
                position_size = 0  # 0%
            else:
                position_size_by_risk = 0.02 / (risk_amount / entry_price)  # 2% 위험 비율
                position_size = min(position_size_by_risk, 0.1)  # 10%와 비교하여 작은 값 선택
            
            # 모든 조건을 충족하는 종목 결과에 추가
            results.append({
                '종목명': symbol,
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': '시장가',  # 시장가 매수, 추후 다음날 시가로 업데이트
                '비중(%)': round(position_size * 100, 2), # % 기호 없이 숫자만 저장
                '수익률': 0.0, # 초기 수익률
                '차익실현': '없음',  # 목표 수익 없음
                '손절매': round(stop_loss, 2), # 계산된 손절매 가격
                '수익보호': round(profit_protection_trailing_stop, 2),  # 25% 트레일링 스톱 가격
                '롱여부': True,
                'volatility': volatility,  # 정렬용
                'price_change_200d': price_change_200d  # 정렬용
            })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # 변동성 기준으로 내림차순 정렬 후, 같은 변동성은 200일 상승률 기준으로 정렬
        result_df = result_df.sort_values(['volatility', 'price_change_200d'], ascending=[False, False])
        
        # 상위 10개 종목만 선택
        result_df = result_df.head(10)
        
        # 결과 CSV에 포함할 컬럼 선택
        strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
        result_df_to_save = result_df[strategy_result_columns]

        # 결과 저장
        result_df_to_save.to_csv(result_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = result_file.replace('.csv', '.json')
        result_df_to_save.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        print(f"✅ 전략 1 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        # 상위 종목 출력
        print("\n🏆 전략 1 상위 종목 (스크리닝 결과):")
        print(result_df_to_save)
        
        
    except Exception as e:
        print(f"❌ 전략 1 스크리닝 오류: {e}")
        print(traceback.format_exc())







# 메인 실행 부분


def run_strategy(total_capital=100000):
    """Wrapper function for main.py compatibility"""
    return run_strategy1_screening(total_capital=total_capital, update_existing=False)
