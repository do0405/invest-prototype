# -*- coding: utf-8 -*-
# 전랙 3: 평균회귀 셀오프 롱 (Long Mean Reversion Selloff)

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


def run_strategy3_screening(total_capital=100000, update_existing=False):
    """
    전랙 3: 평균회귀 셀오프 롱 스크리닝
    
    Args:
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전랙 3: 평균회귀 셀오프 롱 스크리닝 시작...")
    
    # 결과 파일 경로 - buy 폴더로 변경
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy3_results.csv')
    
    try:
        # strategy3.md에는 S&P500 조건이 명시되어 있지 않으므로 개별 종목 조건만 확인
            
        us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        if not us_files:
            print("❌ 미국 주식 데이터 파일이 없습니다.")
            return
            
        print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")
        
        results = []
        for i, file in enumerate(us_files):
            if i % 100 == 0 and i > 0:
                print(f"⏳ 진행 중: {i}/{len(us_files)} 종목 처리됨")
                
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=150, recent_days=150) # 150일 MA 필요
            if symbol is None or df is None or recent_data is None or len(recent_data) < 50: # 50일 평균 거래량, 150일 MA 계산 위해 충분한 데이터 필요
                continue
            
            latest_close = recent_data.iloc[-1]['close']
            if latest_close == 0: continue

            # 필터 1: 주가 최소 $1 이상
            if latest_close < 1.0:
                continue
            
            # 필터 2: 최근 50일 평균 거래량 100만 주 이상
            avg_volume_50d = recent_data.iloc[-50:]['volume'].mean()
            if avg_volume_50d < 1000000:
                continue

            # 필터 3: 최근 10일 ATR >= 5%
            atr_10d_series = calculate_atr(recent_data.iloc[-20:], window=10) # ATR 계산을 위해 최소 20일 데이터 전달
            if atr_10d_series.empty or pd.isna(atr_10d_series.iloc[-1]):
                continue
            atr_10d = atr_10d_series.iloc[-1]
            atr_percentage = (atr_10d / latest_close) * 100
            if atr_percentage < 5.0:
                continue

            # 설정 1: 종가가 150일 이동평균선 위
            ma_150d = recent_data['close'].rolling(window=150).mean().iloc[-1]
            if pd.isna(ma_150d) or latest_close <= ma_150d:
                continue

            # 설정 2: 최근 3일간 12.5% 이상 하락
            if len(recent_data) < 4: # 3일간 하락률 계산 위해 최소 4일 데이터 필요 (오늘, 3일 전)
                continue
            price_3days_ago = recent_data['close'].iloc[-4]
            if price_3days_ago == 0: continue
            price_change_3d = ((latest_close / price_3days_ago) - 1) * 100
            if price_change_3d > -12.5:
                continue
            
            # 순위용: 최근 3일간 하락폭
            # price_change_3d는 음수이므로, 가장 작은 값(가장 큰 하락)이 우선순위 높음

            # 시장 진입: 직전 종가보다 7% 낮은 가격에 지정가 매수
            entry_price = latest_close * 0.93
            
            # 손절매: 체결가 기준, 최근 10일 ATR의 2.5배 아래
            stop_loss_price = entry_price - (atr_10d * 2.5)
            
            # 포지션 크기
            risk_per_share = entry_price - stop_loss_price
            if risk_per_share <= 0: # 손절가가 진입가보다 높거나 같으면 투자 불가
                position_allocation = 0 # 기본값
            else:
                risk_ratio_per_share = risk_per_share / entry_price
                position_allocation_by_risk = 0.02 / risk_ratio_per_share # 총 자산의 2% 리스크
                position_allocation = min(position_allocation_by_risk, 0.1) # 최대 10% 배분
            
            results.append({
                '종목명': symbol,
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': round(entry_price, 2), # 지정가 매수
                '비중(%)': round(position_allocation * 100, 2), # % 기호 없이 숫자만 저장
                '수익률': 0.0, # 초기 수익률
                '차익실현': f'{round(entry_price * 1.04, 2)} (4% 수익) 또는 3일 후 청산',
                '손절매': round(stop_loss_price, 2), # 계산된 손절매 가격
                '수익보호': '없음', # 이 전랙에서는 수익보호 없음
                '롱여부': True,
                'price_drop_3d': price_change_3d # 정렬용
            })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
            # JSON 파일 생성 추가
            json_file = result_file.replace('.csv', '.json')
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_json(json_file, orient='records', indent=2, force_ascii=False)
            
            return
        
        result_df = pd.DataFrame(results)
        result_df = result_df.sort_values('price_drop_3d', ascending=True) # 가장 큰 하락폭 순
        result_df = result_df.head(10) # 최대 10개 포지션
        
        strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
        result_df_to_save = result_df[strategy_result_columns]

        result_df_to_save.to_csv(result_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = result_file.replace('.csv', '.json')
        result_df_to_save.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        print(f"✅ 전랙 3 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        print("\n🏆 전랙 3 상위 종목 (스크리닝 결과):")
        print(result_df_to_save)
        
        
    except Exception as e:
        print(f"❌ 전랙 3 스크리닝 오류: {e}")
        print(traceback.format_exc())





def run_strategy(total_capital=100000):
    """Wrapper function for main.py compatibility"""
    return run_strategy3_screening(total_capital=total_capital, update_existing=False)

if __name__ == "__main__":
    ensure_dir(RESULTS_VER2_DIR)
    ensure_dir(os.path.join(RESULTS_VER2_DIR, 'results')) # 통합 results 디렉토리
    
    print("\n📊 전략 3 스크리닝을 실행합니다. 개별 포트폴리오 관리는 portfolio_managing 모듈을 이용해주세요.")
    run_strategy()