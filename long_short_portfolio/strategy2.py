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


def run_strategy(total_capital=100000, update_existing=False):
    """
    전략 2: 평균회귀 단일 숏 스크리닝 시작...
    
    필터:
    - 주가 최소 $5 이상
    - 20일 평균 거래금액 2,500만 달러 이상
    - 최근 10일간 ATR이 주가의 13% 이상 (변동성 충분해야 함)
    
    설정:
    - 3일 RSI가 90 이상이어야 함 → '탐욕'을 의미
    - 최근 2일 종가가 각각 전일보다 높아야 함 → 과열심리 확인
    
    순위:
    - 7일 ADX가 가장 높은 종목 순으로 정렬 → 강한 추세 역매매
    
    시장 진입:
    - 전일 종가보다 4% 높은 가격에 지정가 공매도 주문
    - 이 가격에 도달하면 '탐욕이 더 커졌음'을 의미해 포지션 추가 가능
    
    손절매:
    - 매수(환매) 역지정가를 최근 10일 ATR의 3배 위에 설정
    - 시장이 계속 상승할 경우에도 여유를 두고 손실 제한
    
    시장 재진입:
    - 손절 후에도 진입 조건 충족 시 다음 날 재진입
    
    수익 보호:
    - 수익 보호 없음
    - 초단기 매매이며 추격 역지정가 주문은 설정하지 않음
    
    차익 실현:
    - 수익이 4% 이상이면 다음 날 종가에 청산
    - 또는 매수 2일 후에도 목표 도달 못 하면 정리 (시간 기반 전략)
    
    포지션 크기:
    - 최대 10개 포지션, 포지션당 자산 대비 2% 위험
    - 포지션당 최대 자산의 10% 배분
    
    Args:
        total_capital: 총 자산 (기본값: 100000)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전략 2: 평균회귀 단일 숏 스크리닝 시작...")
    
    # 결과 파일 경로
    results_output_dir = os.path.join(RESULTS_VER2_DIR, 'results') # 통합 results 디렉토리
    ensure_dir(results_output_dir)
    result_file = os.path.join(results_output_dir, 'strategy2_results.csv')
    
    try:
        # S&P 500 조건 확인
        sp500_condition = check_sp500_condition(DATA_US_DIR)
        if not sp500_condition:
            print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성 (첫 번째 위치)
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
            # JSON 파일 생성 추가
            json_file = result_file.replace('.csv', '.json')
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_json(json_file, orient='records', indent=2, force_ascii=False)
            
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
                
            # 조건 1: 최근 10일 평균 종가가 5달러 이상
            recent_10d = recent_data.iloc[-10:]
            avg_price_10d = recent_10d['close'].mean()
            if avg_price_10d < 5.0:
                continue
            
            # 조건 2: 최근 20일간 거래대금이 2500만 달러 이상
            avg_volume_value = (recent_data['close'] * recent_data['volume']).mean()
            if avg_volume_value <= 25000000:  # 2500만 달러
                continue
            
            # 조건 3: 지난 10일 동안의 ATR은 주식 종가의 13% 이상
            atr_10d_series = calculate_atr(recent_data, window=10)
            if atr_10d_series.empty or pd.isna(atr_10d_series.iloc[-1]):
                continue
            atr_10d = atr_10d_series.iloc[-1]
            
            latest_close = recent_data.iloc[-1]['close']
            if latest_close == 0: # 0으로 나누기 방지
                continue
            atr_percentage = (atr_10d / latest_close) * 100
            if atr_percentage < 13.0:
                continue
            
            # 조건 4: 3일 RSI는 90 이상
            rsi_3d_series = calculate_rsi(recent_data, window=3)
            if rsi_3d_series.empty or pd.isna(rsi_3d_series.iloc[-1]):
                continue
            rsi_3d = rsi_3d_series.iloc[-1]
            if rsi_3d < 90.0:
                continue
            
            # 조건 5: 최근 2일간 종가는 직전일 종가보다 높아야 함
            if len(recent_data) < 3:  # 최소 3일 데이터 필요 (오늘, 어제, 그제)
                continue
                
            today_close = recent_data.iloc[-1]['close']
            yesterday_close = recent_data.iloc[-2]['close']
            day_before_yesterday_close = recent_data.iloc[-3]['close']
            
            if not (today_close > yesterday_close and yesterday_close > day_before_yesterday_close):
                continue
            
            # 조건 6: 7일 ADX 계산
            adx_7d_series = calculate_adx(recent_data, window=7)
            if adx_7d_series.empty or pd.isna(adx_7d_series.iloc[-1]):
                continue # ADX is crucial for sorting, skip if not available
            adx_7d = adx_7d_series.iloc[-1]
            
            # 매도가 설정 (전일 종가 대비 4% 높은 가격)
            entry_price = today_close * 1.04
            
            # 손절매: 매도가 기준 직전 10일 ATR의 3배 위 지점
            stop_loss = entry_price + (atr_10d * 3)
            
            # 수익실현: 매도가 대비 4% 하락 시
            profit_target = entry_price * 0.96
            
            # 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 최대 10개 포지션
            risk_amount = stop_loss - entry_price
            if risk_amount <= 0:  # 위험 금액이 0 이하인 경우 처리
                position_size = 0  # 0%
            else:
                position_size_by_risk = 0.02 / (risk_amount / entry_price)  # 2% 위험 비율
                position_size = min(position_size_by_risk, 0.1)  # 10%와 비교하여 작은 값 선택
            
            # 모든 조건을 충족하는 종목 결과에 추가
            results.append({
                '종목명': symbol,
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': round(entry_price, 2), # 지정가 공매도
                '비중': round(position_size * 100, 2), # % 기호 없이 숫자만 저장
                '수익률': 0.0, # 초기 수익률
                '차익실현': round(profit_target, 2), # 계산된 차익실현 가격 (4% 하락)
                '손절매': round(stop_loss, 2), # 계산된 손절매 가격 (ATR 3배)
                '수익보호': '없음', # 이 전략에서는 수익보호 없음
                '롱여부': False,
                'adx_7d': adx_7d  # 정렬용 (결과에는 포함되지 않음)
            })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성 (두 번째 위치)
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
            # JSON 파일 생성 추가
            json_file = result_file.replace('.csv', '.json')
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_json(json_file, orient='records', indent=2, force_ascii=False)
            
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # ADX 기준으로 내림차순 정렬
        result_df = result_df.sort_values('adx_7d', ascending=False)
        
        # 상위 10개 종목만 선택
        result_df = result_df.head(10)
        
        # 결과 CSV에 포함할 컬럼 선택
        strategy_result_columns = ['종목명', '매수일', '매수가', '비중', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
        result_df_to_save = result_df[strategy_result_columns]

        # 결과 저장
        result_df_to_save.to_csv(result_file, index=False, encoding='utf-8-sig')
        print(f"✅ 전략 2 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        # 상위 종목 출력
        print("\n🏆 전략 2 상위 종목 (스크리닝 결과):")
        print(result_df_to_save)
        
        
    except Exception as e:
        print(f"❌ 전략 2 스크리닝 오류: {e}")
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





# 메인 실행 부분
if __name__ == "__main__":
    # 필요한 디렉토리 생성
    ensure_dir(RESULTS_VER2_DIR)
    ensure_dir(os.path.join(RESULTS_VER2_DIR, 'results')) # 통합 results 디렉토리

    print("\n📊 전략 2 스크리닝을 실행합니다. (결과 파일 생성)")
    run_strategy(total_capital=100000, update_existing=False)
    print("\n💡 포트폴리오 통합 관리는 'run_integrated_portfolio.py'를 사용하세요.")