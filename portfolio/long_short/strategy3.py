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


def run_strategy3_screening():
    """
    전랙 3: 평균회귀 셀오프 롱 스크리닝
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
        
        # 기존 포트폴리오 로드 및 병합 (원칙 3: 이미 채워진 종목은 매수일을 업데이트하지 않음)
        existing_portfolio = pd.DataFrame()
        if os.path.exists(result_file):
            try:
                existing_portfolio = pd.read_csv(result_file, encoding='utf-8-sig')
                print(f"📂 기존 포트폴리오 로드: {len(existing_portfolio)}개 종목")
            except Exception as e:
                print(f"⚠️ 기존 포트폴리오 로드 실패: {e}")
                existing_portfolio = pd.DataFrame()
        
        # 새로운 후보 종목들 (기존 종목 제외)
        if not existing_portfolio.empty:
            existing_symbols = set(existing_portfolio['종목명'].tolist())
            new_candidates = result_df[~result_df['종목명'].isin(existing_symbols)]
        else:
            new_candidates = result_df
        
        # 포트폴리오 구성 (원칙 1: 항상 최대한 만족하는 10개의 종목을 채우려 노력)
        final_portfolio = existing_portfolio.copy()
        
        # 10개까지 채우기 위해 새로운 종목 추가 (조건에 맞는 종목이 있을 때만)
        needed_count = max(0, 10 - len(final_portfolio))
        if needed_count > 0:
            if not new_candidates.empty:
                additional_stocks = new_candidates.head(needed_count)
                # 결과 CSV에 포함할 컬럼 선택 (표준 컬럼)
                strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
                additional_stocks_filtered = additional_stocks[strategy_result_columns]
                final_portfolio = pd.concat([final_portfolio, additional_stocks_filtered], ignore_index=True)
                print(f"➕ {len(additional_stocks_filtered)}개 새로운 종목 추가")
            else:
                print(f"⚠️ 조건에 맞는 새로운 종목이 없어 {len(final_portfolio)}개로 유지합니다.")
        
        # 원칙 2: 실행해서 csv파일이 일부라도 비어있을 경우(10개 미만일 경우) 종목을 찾는다
        if len(final_portfolio) < 10 and not result_df.empty:
            remaining_needed = 10 - len(final_portfolio)
            print(f"📋 포트폴리오가 {len(final_portfolio)}개로 부족하여 {remaining_needed}개 더 채웁니다.")
            strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
            additional_fill = result_df.head(remaining_needed)[strategy_result_columns]
            final_portfolio = pd.concat([final_portfolio, additional_fill], ignore_index=True)
        
        # 최종 포트폴리오가 비어있는 경우 빈 파일 생성
        if final_portfolio.empty:
            strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
            final_portfolio = pd.DataFrame(columns=strategy_result_columns)
        
        # 결과 저장
        final_portfolio.to_csv(result_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = result_file.replace('.csv', '.json')
        final_portfolio.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        print(f"✅ 전략 3 스크리닝 결과 저장 완료: {len(final_portfolio)}개 종목, 경로: {result_file}")
        print("\n🏆 전략 3 상위 종목 (스크리닝 결과):")
        print(final_portfolio)
        
        
    except Exception as e:
        print(f"❌ 전랙 3 스크리닝 오류: {e}")
        print(traceback.format_exc())





def run_strategy():
    """Wrapper function for main.py compatibility"""
    return run_strategy3_screening()

