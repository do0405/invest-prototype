# -*- coding: utf-8 -*-
# 투자 스크리너 Ver4 - 추가 스크리닝 전략 모듈 (전략 4)

import os
import sys
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from scipy.stats import rankdata

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 파일 임포트
from config import (
    DATA_DIR, DATA_US_DIR, 
    RESULTS_DIR, RESULTS_VER2_DIR
)

# 유틸리티 함수 임포트
from utils import (ensure_dir, load_csvs_parallel, extract_ticker_from_filename,
                  calculate_historical_volatility, calculate_rsi, check_sp500_condition,
                  process_stock_data)

# 필요한 디렉토리 생성 함수
def create_required_dirs():
    """필요한 디렉토리를 생성하는 함수"""
    # RESULTS_VER2_DIR은 config.py에서 루트 디렉토리의 'results_ver2'로 설정됨
    ensure_dir(RESULTS_VER2_DIR)
    # 매수/매도 결과 저장 디렉토리 생성
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
    ensure_dir(buy_dir)
    ensure_dir(sell_dir)

# 참고: 역사적 변동성(HV) 계산 함수와 RSI 계산 함수는 utils.py로 이동됨

# 참고: S&P 500 조건 확인 함수는 utils.py의 check_sp500_condition으로 이동됨

# 전략 4: 낮은 변동성 주식 스크리닝
def run_strategy4(total_assets=100000, update_existing=False):
    """네 번째 전략 실행 함수 - 낮은 변동성 주식 식별
    
    조건:
    1. 일평균 거래 금액이 지난 50일 동안 1억 달러 이상
    2. 60일에 대한 역사적 변동성(HV)가 낮은 것을 기준으로 했을 때, 상위 10~40% (낮은 것 상위 0%가 제일 변동성 낮은 것을 의미)
    3. S&P 500 지수 종가가 200일 단순이동평균보다 위에 있어야 한다.
    4. 해당 주식 종가가 200일 단순 이동 평균보다 위에 있어야 한다.
    5. 4일간 RSI가 가장 낮은순위로 순위를 매겨 csv에 기록한다.
    """
    print("\n🔍 전략 4: 낮은 변동성 주식 스크리닝 시작...")
    
    # 결과 파일 경로 (매수 전략이므로 buy 폴더에 저장)
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy4_results.csv')
    
    try:
        # S&P 500 조건 확인
        sp500_condition = check_sp500_condition(DATA_US_DIR)
        if not sp500_condition:
            print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'price', 'avg_volume_value', 'hv_60d', 'rsi_4d']).to_csv(result_file, index=False, mode='w')
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
                
            try:
                # 데이터 처리
                symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=200, recent_days=200)
                if symbol is None or df is None or recent_data is None:
                    continue
                
                # 조건 1: 일평균 거래 금액이 지난 50일 동안 1억 달러 이상
                recent_50d = recent_data.iloc[-50:]
                avg_volume_value = (recent_50d['close'] * recent_50d['volume']).mean()
                if avg_volume_value < 100000000:  # 1억 달러
                    continue
                
                # 조건 2: 60일에 대한 역사적 변동성(HV) 계산
                hv_60d = calculate_historical_volatility(recent_data, window=60).iloc[-1]
                if hv_60d == 0.0:  # 변동성 계산 오류 시 건너뛰기
                    continue
                
                # 조건 4: 해당 주식 종가가 200일 단순 이동 평균보다 위에 있어야 한다.
                recent_data['ma200'] = recent_data['close'].rolling(window=200).mean()
                latest = recent_data.iloc[-1]
                latest_close = float(latest['close'])
                latest_ma200 = float(latest['ma200'])
                if latest_close <= latest_ma200:
                    continue
                
                # 조건 5: 4일간 RSI 계산
                rsi_4d_series = calculate_rsi(recent_data, window=4)
                if rsi_4d_series is None or rsi_4d_series.empty:
                    continue # RSI 계산 불가 시 건너뛰기
                rsi_4d = rsi_4d_series.iloc[-1]
                
                # 모든 조건을 충족하는 종목 결과에 추가
                results.append({
                    'symbol': symbol,
                    'price': latest['close'],
                    'avg_volume_value': avg_volume_value,
                    'hv_60d': hv_60d,
                    'rsi_4d': rsi_4d
                })
                
            except Exception as e:
                import traceback
                print(f"❌ {file} 처리 오류: {e}")
                print(traceback.format_exc())
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'price', 'avg_volume_value', 'hv_60d', 'rsi_4d']).to_csv(result_file, index=False, mode='w')
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # 역사적 변동성(HV) 기준으로 정렬하여 백분위 계산
        result_df = result_df.sort_values('hv_60d')
        total_stocks = len(result_df)
        
        # 각 종목의 변동성 백분위 계산 (0%가 가장 낮은 변동성)
        result_df['hv_percentile'] = [i / total_stocks * 100 for i in range(total_stocks)]
        
        # 조건 2: 변동성 상위 10~40% 필터링 (낮은 변동성 기준)
        filtered_df = result_df[(result_df['hv_percentile'] >= 10) & (result_df['hv_percentile'] <= 40)]
        
        if filtered_df.empty:
            print("❌ 변동성 필터링 후 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'price', 'avg_volume_value', 'hv_60d', 'rsi_4d', 'hv_percentile']).to_csv(result_file, index=False)
            return
        
        # 조건 5: 4일간 RSI가 가장 낮은 순으로 정렬
        final_df = filtered_df.sort_values('rsi_4d')
        
        # 상위 20개 종목만 선택
        final_df = final_df.head(20)
        
        # 결과 저장
        final_df.to_csv(result_file, index=False, mode='w')
        print(f"✅ 결과 저장 완료: {len(final_df)}개 종목, 경로: {result_file}")
        
        # 상위 10개 종목 출력
        print("\n🏆 전략 4 상위 10개 종목 (RSI 낮은 순):")
        print(final_df[['symbol', 'price', 'hv_60d', 'hv_percentile', 'rsi_4d']].head(10))

        # 포트폴리오 생성/업데이트
        create_portfolio_strategy4(final_df, total_assets=total_assets, update_existing=update_existing)
        
    except Exception as e:
        import traceback
        print(f"❌ 전략 4 스크리닝 오류: {e}")
        print(traceback.format_exc())

# 포트폴리오 생성 함수 (전략 4)
def create_portfolio_strategy4(screened_stocks_df, total_assets=100000, update_existing=False):
    """전략 4의 스크리닝 결과를 바탕으로 포트폴리오를 생성하거나 업데이트합니다.

    Args:
        screened_stocks_df (pd.DataFrame): 스크리닝된 종목 정보 (strategy4_results.csv 내용).
        total_assets (float): 총 투자 금액.
        update_existing (bool): 기존 포트폴리오 파일 업데이트 여부.
    """
    portfolio_dir = os.path.join(RESULTS_VER2_DIR, 'portfolios')
    ensure_dir(portfolio_dir)
    portfolio_file = os.path.join(portfolio_dir, 'portfolio_strategy4.csv')

    if screened_stocks_df.empty:
        print("⚠️ 전략 4: 스크리닝된 종목이 없어 포트폴리오를 생성/업데이트할 수 없습니다.")
        # 빈 포트폴리오 파일 생성 또는 유지 (기존 파일이 있다면 덮어쓰지 않음)
        if not os.path.exists(portfolio_file):
            pd.DataFrame(columns=[
                '종목명', '매수일', '매수가', '수량', '투자금액', '현재가', '수익률(%)', 
                '목표가', '손절가', '비중(%)', '전략명'
            ]).to_csv(portfolio_file, index=False, encoding='utf-8-sig')
        return

    # 상위 10개 종목만 포트폴리오에 포함 (스크리닝 결과가 이미 20개로 제한되어 있음)
    # 실제로는 스크리닝 결과 전체를 사용하거나, 여기서 추가로 필터링 가능
    target_stocks = screened_stocks_df.head(10) 
    num_stocks = len(target_stocks)
    if num_stocks == 0:
        print("⚠️ 전략 4: 포트폴리오에 추가할 종목이 없습니다.")
        if not os.path.exists(portfolio_file):
            pd.DataFrame(columns=[
                '종목명', '매수일', '매수가', '수량', '투자금액', '현재가', '수익률(%)', 
                '목표가', '손절가', '비중(%)', '전략명'
            ]).to_csv(portfolio_file, index=False, encoding='utf-8-sig')
        return

    investment_per_stock = total_assets / num_stocks

    new_portfolio_data = []
    for _, row in target_stocks.iterrows():
        symbol = row['symbol']
        entry_price = row['price'] # 스크리닝 시점의 가격을 매수가로 사용
        position_size = investment_per_stock / entry_price
        investment_amount = position_size * entry_price # 실제 투자금액
        
        new_portfolio_data.append({
            '종목명': symbol,
            '매수일': datetime.now().strftime('%Y-%m-%d'),
            '매수가': entry_price,
            '수량': round(position_size, 4),
            '투자금액': round(investment_amount, 2),
            '현재가': entry_price, # 초기 현재가는 매수가와 동일
            '수익률(%)': 0.0,
            '목표가': np.nan, # 전략 4는 명시적인 목표가/손절가 없음
            '손절가': np.nan,
            '비중(%)': round((investment_amount / total_assets) * 100, 2) if total_assets > 0 else 0,
            '전략명': 'Strategy4_LowVolatility'
        })

    new_portfolio_df = pd.DataFrame(new_portfolio_data)

    if update_existing and os.path.exists(portfolio_file):
        try:
            existing_portfolio_df = pd.read_csv(portfolio_file)
            # 기존 포트폴리오에 새 종목 추가 (중복 방지 로직은 필요에 따라 추가)
            # 여기서는 단순히 합치지만, 실제로는 종목별 업데이트 로직이 필요할 수 있음
            updated_portfolio_df = pd.concat([existing_portfolio_df, new_portfolio_df], ignore_index=True)
            # 중복된 종목명 처리 (가장 최근 데이터 유지 또는 다른 로직)
            updated_portfolio_df = updated_portfolio_df.drop_duplicates(subset=['종목명', '전략명'], keep='last')
        except pd.errors.EmptyDataError:
            print(f"⚠️ 기존 포트폴리오 파일 '{portfolio_file}'이 비어있습니다. 새로 생성합니다.")
            updated_portfolio_df = new_portfolio_df
    else:
        updated_portfolio_df = new_portfolio_df

    updated_portfolio_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')
    print(f"✅ 전략 4 포트폴리오 저장 완료: {len(updated_portfolio_df)}개 종목, 경로: {portfolio_file}")


# 메인 실행 함수
if __name__ == "__main__":
    # 필요한 디렉토리 생성
    create_required_dirs()
    
    # 전략 4 실행 (포트폴리오 생성 포함)
    run_strategy4(total_assets=100000, update_existing=False)
    
    print("\n실행이 완료되었습니다. 터미널에서 실행하려면 'py screener_ver4.py' 명령어를 사용하세요.")