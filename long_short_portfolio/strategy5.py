# -*- coding: utf-8 -*-
# 전략 5: 평균회귀 하이 ADX 리버설 롱 (Mean Reversion High ADX Reversal Long)

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
    calculate_atr, calculate_rsi, calculate_adx,
    check_sp500_condition, process_stock_data
)


def run_strategy5_screening():
    print("\n🔍 전략 5: 평균회귀 하이 ADX 리버설 롱 스크리닝 시작...")

    # 결과 파일 경로 - buy 폴더로 변경
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy5_results.csv')

    try:
        # S&P500 조건은 명시되어 있지 않으므로, 개별 종목 조건만 확인
        # check_sp500_condition 함수는 사용하지 않음

        us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        if not us_files:
            print("❌ 미국 주식 데이터 파일이 없습니다.")
            return

        print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")

        results = []
        for i, file in enumerate(us_files):
            if i % 100 == 0 and i > 0:
                print(f"⏳ 진행 중: {i}/{len(us_files)} 종목 처리됨")

            # 100일 MA, 50일 거래량/금액, 10일 ATR, 7일 ADX, 3일 RSI 필요
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=100, recent_days=100)
            if symbol is None or df is None or recent_data is None or len(recent_data) < 50:
                continue

            latest_close = recent_data.iloc[-1]['close']
            if latest_close == 0: continue

            # 필터 1: 50일 기준 평균 거래량 ≥ 50만 주
            avg_volume_50d = recent_data.iloc[-50:]['volume'].mean()
            if avg_volume_50d < 500000:
                continue

            # 필터 2: 50일 기준 평균 거래금액 ≥ 250만 달러
            avg_value_50d = (recent_data.iloc[-50:]['close'] * recent_data.iloc[-50:]['volume']).mean()
            if avg_value_50d < 2500000:  # 250만 달러
                continue

            # 필터 3: ATR ≥ 4 (최근 10일 ATR 기준)
            atr_10d_series = calculate_atr(recent_data.iloc[-20:], window=10)
            if atr_10d_series.empty or pd.isna(atr_10d_series.iloc[-1]) or atr_10d_series.iloc[-1] < 4:
                continue
            atr_10d = atr_10d_series.iloc[-1]

            # 설정 조건 1: 종가 > 100일 이동평균, 최근 10일 1ATR보다 높은 것
            # (해석: 종가가 (100일 MA + 10일 ATR) 보다 높아야 함)
            ma_100d = recent_data['close'].rolling(window=100).mean().iloc[-1]
            if pd.isna(ma_100d) or latest_close <= (ma_100d + atr_10d):
                continue

            # 설정 조건 2: 7일 ADX ≥ 55
            # ADX 계산을 위해 high, low, close 데이터 필요
            # ADX 계산 (7일)
            adx_7d = pd.NA # Initialize adx_7d
            if len(recent_data) >= 20: # ADX 계산에 충분한 데이터가 있는지 확인 (일반적으로 ADX는 최소 14일 필요, 여유있게 20일)
                # logger.debug(f"{ticker}: Calculating ADX as data length {len(recent_data)} >= 20")
                adx_7d = calculate_adx(recent_data, window=7).iloc[-1]
            # else:
                # logger.debug(f"{ticker}: Not enough data for ADX calculation (need 20, got {len(recent_data)})")
            # logger.debug(f"{ticker}: 7-day ADX: {adx_7d}")
            if pd.isna(adx_7d) or adx_7d < 55:
                continue

            # 설정 조건 3: 3일 RSI ≤ 50
            rsi_3d_series = calculate_rsi(recent_data[['close']], window=3)
            if rsi_3d_series.empty or pd.isna(rsi_3d_series.iloc[-1]) or rsi_3d_series.iloc[-1] > 50:
                continue
            rsi_3d = rsi_3d_series.iloc[-1]

            # 시장 진입: 직전 종가보다 최대 3% 낮은 가격에 지정가 매수
            entry_price = latest_close * 0.97

            # 손절매: 체결가 기준 10일 ATR의 3배 아래
            stop_loss_price = entry_price - (atr_10d * 3)

            # 수익보호: 없음 (마크다운 문서에 따라)
            profit_protection = '없음'
            
            # 포지션 크기
            risk_per_share = entry_price - stop_loss_price
            if risk_per_share <= 0:
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
                '차익실현': f'{round(ma_100d + atr_10d, 2)} (10일 ATR 상단) 또는 6일 후 강제매도',
                '손절매': round(stop_loss_price, 2), # 계산된 손절매 가격
                '수익보호': profit_protection, # '없음'으로 설정됨
                '롱여부': True,
                'adx_7d': adx_7d, # 정렬용
                'rsi_3d': rsi_3d # 정렬용
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
        result_df = result_df.sort_values(['adx_7d', 'rsi_3d'], ascending=[False, True]) # ADX 높은 순, RSI 낮은 순
        result_df = result_df.head(10) # 최대 10개 포지션

        strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
        result_df_to_save = result_df[strategy_result_columns]

        result_df_to_save.to_csv(result_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = result_file.replace('.csv', '.json')
        result_df_to_save.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        print(f"✅ 전략 4 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        print("\n🏆 전략 5 상위 종목 (스크리닝 결과):")
        print(result_df_to_save)


    except Exception as e:
        print(f"❌ 전략 5 스크리닝 오류: {e}")
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


def run_strategy(total_capital=100000):
    """Wrapper function for main.py compatibility"""
    return run_strategy5_screening()

if __name__ == "__main__":
    ensure_dir(RESULTS_VER2_DIR)
    ensure_dir(os.path.join(RESULTS_VER2_DIR, 'results'))
    print("\n📊 전략 5 스크리닝을 실행합니다. 포트폴리오 관리는 run_integrated_portfolio.py를 이용해주세요.")