# -*- coding: utf-8 -*-
# 전략 4: 트렌드 저변동성 롱 (Long Trend Low Volatility)

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
    calculate_atr, calculate_historical_volatility, calculate_rsi,
    check_sp500_condition, process_stock_data
)


def run_strategy(total_capital=100000, update_existing=False):
    """
    전략 4: 트렌드 저변동성 롱 (Long Trend Low Volatility)

    필터:
    - 최근 50일 평균 일일 거래 금액이 1억 달러 이상.
    - 최근 120일간의 종가를 기준으로 계산한 연환산 변동성이 10%에서 40% 사이에 있는 종목만 선별

    설정:
    - S&P500 지수가 200일 이동평균 위에 있을 것 (시장 전체가 상승장일 때만 투자).
    - 개별 주가 역시 200일 이동평균 위에 있어야 함.

    순위:
    - 최근 4일간 RSI가 가장 낮은 순서

    시장 진입:
    - 장 시작 시 시장가 매수 (슬리피지와 상관없이 반드시 매수).

    손절매:
    - 최근 40일 ATR의 1.5배 아래에 손절라인 설정.

    시장 재진입:
    - 가능. 같은 조건 다시 충족되면 재진입.

    수익 보호:
    - 20%의 추격 역지정가 주문 설정 → 상승 지속 시 수익 확보.

    차익 실현:
    - 추세가 지속되는 한 차익 실현 없음, 계속 보유.

    포지션 크기:
    - 포지션별 자산 대비 2% 리스크.
    - 포지션당 최대 10% 자산 배분, 최대 10개 포지션.

    Args:
        total_capital: 총 자산 (기본값: 100000)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전략 4: 트렌드 저변동성 롱 스크리닝 시작...")

    # 결과 파일 경로
    ensure_dir(RESULTS_DIR)
    result_file = os.path.join(RESULTS_DIR, 'strategy4_results.csv')

    try:
        # 설정 1: S&P 500 지수가 200일 이동평균 위에 있을 것
        sp500_ok = check_sp500_condition(DATA_US_DIR, ma_days=200)
        if not sp500_ok:
            print("❌ S&P 500 조건을 충족하지 않습니다 (200일 MA 하회). 스크리닝을 중단합니다.")
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
            return

        us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        if not us_files:
            print("❌ 미국 주식 데이터 파일이 없습니다.")
            return

        print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")

        results = []
        for i, file in enumerate(us_files):
            if i % 100 == 0 and i > 0:
                print(f"⏳ 진행 중: {i}/{len(us_files)} 종목 처리됨")

            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=200, recent_days=200) # 200일 MA, 60일 변동성, 50일 거래대금, 40일 ATR
            if symbol is None or df is None or recent_data is None or len(recent_data) < 60: # 최소 60일 데이터 필요
                continue

            latest_close = recent_data.iloc[-1]['close']
            if latest_close == 0: continue

            # 필터 1: 최근 50일 평균 일일 거래 금액이 1억 달러 이상
            avg_daily_value_50d = (recent_data.iloc[-50:]['close'] * recent_data.iloc[-50:]['volume']).mean()
            if avg_daily_value_50d < 100000000: # 1억 달러
                continue

            # 필터 2: 최근 120일간의 종가를 기준으로 계산한 연환산 변동성이 10%에서 40% 사이에 있는 종목만 선별
            # 연환산 변동성 계산 (120일 기준)
            if len(recent_data) < 120:
                # logger.debug(f"{symbol}: Not enough data for 120-day volatility calculation (need 120, got {len(recent_data)})")
                continue
            # 연간 거래일 수를 252일로 가정
            volatility_120d = calculate_historical_volatility(recent_data.iloc[-120:], window=120).iloc[-1] # trading_days 인수 제거
            if not (0.10 <= volatility_120d <= 0.40):
                # logger.debug(f"{symbol}: 120-day annualized volatility {volatility_120d:.2%} out of range (10%-40%)")
                continue

            # 설정 2: 개별 주가 역시 200일 이동평균 위에 있어야 함
            ma_200d = recent_data['close'].rolling(window=200).mean().iloc[-1]
            if pd.isna(ma_200d) or latest_close <= ma_200d:
                continue

            # 순위: 최근 4일간 RSI가 가장 낮은 순서
            if len(recent_data) < 18: # RSI(14) 계산 위해 최소 14 + 4일 데이터 필요
                continue
            rsi_series = calculate_rsi(recent_data['close'], window=14)
            if rsi_series.empty or len(rsi_series) < 4:
                continue
            rsi_4d_avg = rsi_series.iloc[-4:].mean() # 최근 4일 RSI 평균 (또는 마지막 값, 여기서는 평균으로 해석)
            if pd.isna(rsi_4d_avg):
                continue

            # 시장 진입: 장 시작 시 시장가 매수 (최신 시가 사용)
            entry_price = recent_data.iloc[-1]['open']
            if entry_price == 0: continue # 시가가 0이면 거래 불가

            # 손절매: 최근 40일 ATR의 1.5배 아래
            atr_40d_series = calculate_atr(recent_data.iloc[-50:], window=40) # ATR 계산 위해 충분한 데이터 전달
            if atr_40d_series.empty or pd.isna(atr_40d_series.iloc[-1]):
                continue
            atr_40d = atr_40d_series.iloc[-1]
            stop_loss_price = entry_price - (atr_40d * 1.5)

            # 수익보호: 20% 추격 역지정가
            profit_protection = entry_price * 0.80  # 매수가의 80% 지점 (20% 하락)
            
            # 포지션 크기
            risk_per_share = entry_price - stop_loss_price
            if risk_per_share <= 0:
                position_allocation = 0.1 # 기본값
            else:
                risk_ratio_per_share = risk_per_share / entry_price
                position_allocation_by_risk = 0.02 / risk_ratio_per_share # 총 자산의 2% 리스크
                position_allocation = min(position_allocation_by_risk, 0.1) # 최대 10% 배분

            results.append({
                '종목명': symbol,
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': round(entry_price, 2), # 시장가, 추후 다음날 시가로 업데이트될 수 있음
                '비중': round(position_allocation * 100, 2), # % 기호 없이 숫자만 저장
                '수익률': 0.0, # 초기 수익률
                '차익실현': '없음 (추세 지속 시 보유)', # 조건부 문자열 유지
                '손절매': round(stop_loss_price, 2), # 계산된 손절매 가격
                '수익보호': round(profit_protection, 2), # 계산된 수익보호 가격
                '롱여부': True,
                'rsi_4d_avg': rsi_4d_avg # 정렬용
            })

        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
            return

        result_df = pd.DataFrame(results)
        result_df = result_df.sort_values('rsi_4d_avg', ascending=True) # RSI 낮은 순
        result_df = result_df.head(10) # 최대 10개 포지션

        strategy_result_columns = ['종목명', '매수일', '매수가', '비중', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
        result_df_to_save = result_df[strategy_result_columns]

        result_df_to_save.to_csv(result_file, index=False, encoding='utf-8-sig')
        print(f"✅ 전략 4 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")

        print("\n🏆 전략 4 상위 종목 (스크리닝 결과):")
        print(result_df_to_save)


    except Exception as e:
        print(f"❌ 전략 4 스크리닝 오류: {e}")
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


if __name__ == "__main__":
    ensure_dir(RESULTS_VER2_DIR) # RESULTS_DIR 대신 RESULTS_VER2_DIR 사용
    ensure_dir(os.path.join(RESULTS_VER2_DIR, 'results')) # 통합 results 디렉토리
    print("\n📊 전략 4 스크리닝을 실행합니다. 포트폴리오 관리는 run_integrated_portfolio.py를 이용해주세요.")
    run_strategy()