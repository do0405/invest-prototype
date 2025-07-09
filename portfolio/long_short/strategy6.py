# -*- coding: utf-8 -*-
# 전략 6: 평균회귀 6일 급등 숏 (Mean Reversion 6-Day Surge Short)

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


def run_strategy6_screening():
    print("\n🔍 전략 6: 평균회귀 6일 급등 숏 스크리닝 시작...")

    # 결과 파일 경로 - sell 폴더로 변경
    sell_dir = os.path.join(PORTFOLIO_RESULTS_DIR, 'sell')
    ensure_dir(sell_dir)
    result_file = os.path.join(sell_dir, 'strategy6_results.csv')

    try:
        # S&P 500 조건 확인 (이 전략에서는 S&P500 조건이 명시되지 않았으므로 생략)
        # sp500_condition = check_sp500_condition(DATA_US_DIR)
        # if not sp500_condition:
        #     print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
        #     pd.DataFrame(columns=['종목명', '매수일', '시장 진입가', '비중(%)', '수익률(%)', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
        #     return

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
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=50, recent_days=50) # 50일 데이터 필요
            if symbol is None or df is None or recent_data is None or len(recent_data) < 6: # 최소 6일 데이터 필요
                continue
            latest_close = recent_data.iloc[-1]['close']

            # 필터 1: 최소 주가 $5 이상
            if latest_close < 5.0:
                continue

            # 필터 2: 최근 50일 기준 일평균 거래금액 ≥ 1,000만 달러
            avg_volume_value_50d = (recent_data['close'] * recent_data['volume']).mean()
            if avg_volume_value_50d < 10000000: # 1,000만 달러
                continue

            # 설정 1: 최근 6거래일 동안 20% 이상 상승한 종목
            
            close_6_days_ago = recent_data.iloc[-6]['close']
            if close_6_days_ago == 0: continue # 0으로 나누기 방지
            price_increase_6d = (latest_close - close_6_days_ago) / close_6_days_ago
            if price_increase_6d < 0.20:
                continue

            # 설정 2: 최근 2거래일 연속 상승한 종목
            if len(recent_data) < 3: # 최소 3일 데이터 필요 (오늘, 어제, 그제)
                continue
            today_close = recent_data.iloc[-1]['close']
            yesterday_close = recent_data.iloc[-2]['close']
            day_before_yesterday_close = recent_data.iloc[-3]['close']
            if not (today_close > yesterday_close and yesterday_close > day_before_yesterday_close):
                continue

            # 순위: 6일간 상승률
            rank_metric = price_increase_6d # 6일간 상승률로 순위 결정

            # ATR 계산 (10일)
            atr_10d_series = calculate_atr(recent_data, window=10)
            if atr_10d_series.empty or pd.isna(atr_10d_series.iloc[-1]):
                continue
            atr_10d = atr_10d_series.iloc[-1]

            # 시장 진입: 직전 종가보다 최대 5% 높은 가격에 지정가 공매도
            entry_price = latest_close * 1.05

            # 손절매: 체결가 기준 최근 10일 ATR의 3배 위에 손절가 설정
            stop_loss = entry_price + (atr_10d * 3)

            # 차익 실현: 수익률 5% 도달 시 또는 3거래일 후 청산
            profit_target_price = entry_price * 0.95 # 5% 수익
            profit_target_condition = f'{round(profit_target_price, 2)} (5% 수익) 또는 3일 후 청산'

            # 포지션 크기: 포지션당 총자산 대비 2% 리스크, 시스템 전체 자산 대비 최대 10% 배분
            risk_per_share = stop_loss - entry_price
            if risk_per_share <= 0: # 위험 금액이 0 이하인 경우 처리
                position_size_pct = 0.1 # 기본값 10%
            else:
                position_size_by_risk = 0.02 / (risk_per_share / entry_price) # 2% 리스크
                position_size_pct = min(position_size_by_risk, 0.1) # 최대 10% 배분

            results.append({
                '종목명': symbol,
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': round(entry_price, 2),
                '비중(%)': round(position_size_pct * 100, 2),
                '수익률': 0.0,
                '차익실현': profit_target_condition, # 이미 계산된 profit_target_price를 사용
                '손절매': round(stop_loss, 2),
                '수익보호': '없음',
                '롱여부': False,
                'rank_metric': rank_metric # 정렬용
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
        result_df = result_df.sort_values('rank_metric', ascending=False) # 6일 상승률 높은 순
        
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
        
        print(f"✅ 전략 6 스크리닝 결과 저장 완료: {len(final_portfolio)}개 종목, 경로: {result_file}")
        
        print("\n🏆 전략 6 상위 종목 (스크리닝 결과):")
        print(final_portfolio)


    except Exception as e:
        print(f"❌ 전략 6 스크리닝 오류: {e}")
        print(traceback.format_exc())









def run_strategy():
    """Wrapper function for main.py compatibility"""
    return run_strategy6_screening()
