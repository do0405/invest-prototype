# -*- coding: utf-8 -*-
# 전략 6: 평균회귀 6일 급등 숏 (Mean Reversion 6-Day Surge Short)

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


def run_strategy6_screening():
    print("\n🔍 전략 6: 평균회귀 6일 급등 숏 스크리닝 시작...")

    # 결과 파일 경로 - sell 폴더로 변경
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
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
        result_df = result_df.head(10) # 최대 10개 포지션

        strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
        result_df_to_save = result_df[strategy_result_columns]

        result_df_to_save.to_csv(result_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = result_file.replace('.csv', '.json')
        result_df_to_save.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        print(f"✅ 전략 6 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        print("\n🏆 전략 6 상위 종목 (스크리닝 결과):")
        print(result_df_to_save)


    except Exception as e:
        print(f"❌ 전략 6 스크리닝 오류: {e}")
        print(traceback.format_exc())





# 최신 가격 데이터 가져오기 함수 (고가 포함)
def get_latest_price_data_high(symbol):
    """특정 종목의 최신 가격 데이터를 가져오는 함수 (고가 포함)
    
    Args:
        symbol: 종목 심볼
        
    Returns:
        tuple: (현재가, 당일 고가) 또는 데이터가 없는 경우 (None, None)
    """
    try:
        # 종목 데이터 파일 경로
        file_path = os.path.join(DATA_US_DIR, f'{symbol}.csv')
        
        if not os.path.exists(file_path):
            print(f"⚠️ {symbol} 데이터 파일을 찾을 수 없습니다.")
            return None, None
        
        # 데이터 로드
        df = pd.read_csv(file_path)
        df.columns = [col.lower() for col in df.columns]
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')
        else:
            print(f"⚠️ {symbol} 데이터에 날짜 컬럼이 없습니다.")
            return None, None
        
        # 최신 데이터 확인
        if df.empty:
            return None, None
        
        latest = df.iloc[-1]
        
        return latest['close'], latest['high']
        
    except Exception as e:
        print(f"❌ {symbol} 가격 데이터 가져오기 오류: {e}")
        return None, None


if __name__ == '__main__':
    # 결과 디렉토리 생성
    ensure_dir(RESULTS_VER2_DIR) # RESULTS_DIR 대신 RESULTS_VER2_DIR 사용
    ensure_dir(os.path.join(RESULTS_VER2_DIR, 'results')) # 통합 results 디렉토리

    # 총 자산 설정
    CAPITAL = 100000

    print("🚀 전략 6 스크리닝을 실행합니다. 포트폴리오 관리는 run_integrated_portfolio.py를 이용해주세요.")
    try:
        run_strategy(total_capital=CAPITAL)
    except Exception as e:
        print(f"❌ 전략 6 실행 중 오류 발생: {e}")
        print(traceback.format_exc())

    print("\n🎉 전략 6 실행 완료.")


def run_strategy(total_capital=100000):
    """Wrapper function for main.py compatibility"""
    return run_strategy6_screening()