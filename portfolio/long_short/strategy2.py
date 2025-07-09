# -*- coding: utf-8 -*-
# 전략 2: 평균회귀 단일 숏 (Mean Reversion Short Single)

import os
import traceback
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.path_utils import add_project_root

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

# 설정 파일 임포트
from config import (
    DATA_DIR, DATA_US_DIR,
    RESULTS_DIR, PORTFOLIO_RESULTS_DIR
)

# 유틸리티 함수 임포트
from utils import (
    ensure_dir, extract_ticker_from_filename, 
    calculate_atr, calculate_historical_volatility,
    calculate_rsi, calculate_adx,
    check_sp500_condition, process_stock_data
)


def run_strategy2_screening():
    """
    전략 2: 평균회귀 단일 숏 스크리닝
    """
    print("\n🔍 전략 2: 평균회귀 단일 숏 스크리닝 시작...")
    
    # 결과 파일 경로 - sell 폴더로 변경
    sell_dir = os.path.join(PORTFOLIO_RESULTS_DIR, 'sell')
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
            rsi_3_df = calculate_rsi(recent_data, window=3)
            if 'rsi_3' not in rsi_3_df.columns or pd.isna(rsi_3_df['rsi_3'].iloc[-1]):
                continue
            rsi_3 = rsi_3_df['rsi_3'].iloc[-1]
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

            # 자산 비중(%) 계산
            weight_pct = round((position_size * entry_price / total_capital) * 100, 2)

            # 결과 저장 (표준 컬럼 사용)
            results.append({
                '종목명': symbol,
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': round(entry_price, 2),
                '비중(%)': weight_pct,
                '수익률': 0.0,
                '차익실현': round(profit_target, 2),
                '손절매': round(stop_loss, 2),
                '수익보호': '없음',
                '롱여부': False,
                # 정렬 및 분석용 부가 정보
                'adx_7': round(adx_7d, 2),
                'rsi_3': round(rsi_3, 2),
                'atr_10': round(atr_10.iloc[-1], 4),
                'avg_close_10': round(avg_close_10, 2),
                'avg_daily_value_20': round(avg_daily_value_20, 0),
                'adx_7d': adx_7d  # 정렬용
            })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성 - 표준 컬럼 사용
            empty_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
            pd.DataFrame(columns=empty_columns).to_csv(result_file, index=False, encoding='utf-8-sig')
            # JSON 파일 생성
            json_file = result_file.replace('.csv', '.json')
            pd.DataFrame(columns=empty_columns).to_json(json_file, orient='records', indent=2, force_ascii=False)
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # ADX 기준으로 내림차순 정렬
        result_df = result_df.sort_values('adx_7d', ascending=False)
        
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
        
        print(f"✅ 전략 2 스크리닝 결과 저장 완료: {len(final_portfolio)}개 종목, 경로: {result_file}")
        # 상위 종목 출력
        print("\n🏆 전략 2 상위 종목 (스크리닝 결과):")
        print(final_portfolio)
        
        
    except Exception as e:
        print(f"❌ 전략 2 스크리닝 오류: {e}")
        print(traceback.format_exc())







# 메인 실행 부분

def run_strategy():
    """Wrapper function for main.py compatibility"""
    return run_strategy2_screening()
