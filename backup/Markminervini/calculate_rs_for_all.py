#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 모든 티커에 대한 RS 점수 계산 스크립트

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import traceback

# 디버깅을 위한 출력 설정
print("스크립트 시작...")
sys.stdout.flush()

try:
    # 설정 파일 임포트
    from config import (
        DATA_DIR, DATA_US_DIR, RESULTS_DIR, US_WITH_RS_PATH
    )
    print("설정 파일 임포트 성공")
    sys.stdout.flush()

    # 유틸리티 함수 임포트
    from utils import ensure_dir, load_csvs_parallel
    print("유틸리티 함수 임포트 성공")
    sys.stdout.flush()
except Exception as e:
    print(f"임포트 오류: {e}")
    traceback.print_exc()
    sys.exit(1)

# 파일명에서 원래 티커 추출 함수
def extract_ticker_from_filename(filename):
    """
    파일명에서 원래 티커 심볼을 추출하는 함수
    Windows 예약 파일명 처리를 위해 추가된 'STOCK_' 접두사 제거
    """
    # 파일 확장자 제거
    ticker = os.path.splitext(filename)[0]
    
    # 'STOCK_' 접두사가 있으면 제거
    if ticker.startswith('STOCK_'):
        ticker = ticker[6:]  # 'STOCK_' 길이(6)만큼 제거
    
    return ticker

# NaN 값 확인 함수
def is_valid_ticker(ticker):
    """
    티커가 유효한지 확인하는 함수
    NaN 값이나 None 값은 False 반환
    """
    if ticker is None:
        return False
    if isinstance(ticker, float) and np.isnan(ticker):
        return False
    if not isinstance(ticker, str) and not isinstance(ticker, int):
        return False
    return True

# 스크리너 함수 임포트
from Markminervini.screener import calculate_rs_score

def calculate_rs_for_all_tickers(window=126, min_data_points=200):
    """모든 티커에 대해 RS 점수를 계산하는 함수
    
    Args:
        window: RS 점수 계산에 사용할 기간 (기본값: 126일)
        min_data_points: 최소 데이터 포인트 수 (기본값: 200)
        
    Returns:
        DataFrame: RS 점수가 포함된 데이터프레임
    """
    print("\n🔍 모든 티커에 대한 RS 점수 계산 시작...")
    
    # 필요한 디렉토리 확인
    ensure_dir(RESULTS_DIR)
    
    # 미국 주식 데이터 파일 목록 가져오기
    us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
    if not us_files:
        print("❌ 미국 주식 데이터 파일이 없습니다.")
        return None
    
    print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")
    
    # 모든 종목의 종가 데이터를 하나의 데이터프레임으로 통합
    all_data = []
    processed_count = 0
    
    for file in us_files:
        try:
            file_path = os.path.join(DATA_US_DIR, file)
            # Windows 예약 파일명 처리 - 파일명에서 원래 티커 추출
            symbol = extract_ticker_from_filename(file)
            
            # 티커 유효성 검사 - NaN 값 처리
            if not is_valid_ticker(symbol):
                print(f"⚠️ 유효하지 않은 티커 건너뜀: {file}")
                continue
            
            # 개별 파일 로드
            df = pd.read_csv(file_path)
            
            # 컬럼명 소문자로 변환
            df.columns = [col.lower() for col in df.columns]
            
            # 최소 데이터 길이 확인
            if len(df) < min_data_points:
                continue
            
            if 'date' in df.columns and 'close' in df.columns:
                # 날짜 변환 및 정렬
                df['date'] = pd.to_datetime(df['date'], utc=True)
                df = df.sort_values('date')
                
                # 필요한 컬럼만 선택
                df_selected = df[['date', 'close']].copy()
                df_selected['symbol'] = symbol
                
                # 데이터 추가
                all_data.append(df_selected)
                processed_count += 1
                
                if processed_count % 100 == 0:
                    print(f"⏳ 진행 중: {processed_count}/{len(us_files)} 종목 처리됨")
        except Exception as e:
            print(f"❌ {file} 처리 오류: {e}")
    
    if not all_data:
        print("❌ 처리할 데이터가 없습니다.")
        return None
    
    # 데이터 통합
    print(f"✅ {processed_count}개 종목 데이터 로드 완료")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # 날짜-심볼 멀티인덱스 설정
    combined_df = combined_df.set_index(['date', 'symbol'])
    
    # RS 점수 계산
    print("\n💹 RS 점수 계산 중...")
    rs_scores = calculate_rs_score(combined_df, price_col='close', window=window)
    
    if rs_scores.empty:
        print("❌ RS 점수 계산 실패")
        return None
    
    # 결과 데이터프레임 생성
    result_df = pd.DataFrame({'symbol': rs_scores.index, 'rs_score': rs_scores.values})
    
    # 결과 저장
    result_df.to_csv(US_WITH_RS_PATH, index=False)
    print(f"✅ RS 점수 계산 완료: {len(result_df)}개 종목")
    
    # 상위 10개 종목 출력
    top_10 = result_df.sort_values('rs_score', ascending=False).head(10)
    print(f"\n🏆 RS 점수 상위 10개 종목:\n{top_10}")
    
    return result_df

def main():
    parser = argparse.ArgumentParser(description="모든 티커에 대한 RS 점수 계산")
    parser.add_argument("--window", type=int, default=126, help="RS 점수 계산에 사용할 기간 (기본값: 126일)")
    parser.add_argument("--min-data", type=int, default=200, help="최소 데이터 포인트 수 (기본값: 200)")
    
    args = parser.parse_args()
    
    # RS 점수 계산 실행
    calculate_rs_for_all_tickers(window=args.window, min_data_points=args.min_data)

if __name__ == "__main__":
    # 필요한 모듈 임포트
    import sys
    import traceback
    
    try:
        # 스크립트 실행 방식 출력
        print(f"\n🚀 실행 환경: {sys.executable}")
        sys.stdout.flush()
        
        # 명령줄 인수 처리 및 RS 점수 계산 실행
        main()
        
        print("\n✅ 모든 작업이 완료되었습니다.")
        sys.stdout.flush()
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        traceback.print_exc()
        sys.exit(1)