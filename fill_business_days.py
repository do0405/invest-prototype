# -*- coding: utf-8 -*-
# 영업일 데이터 채우기 모듈 (yfinance 사용 버전)

import os
import pandas as pd
import numpy as np
import glob
import time
import yfinance as yf
from datetime import datetime, timedelta
from pytz import timezone
from concurrent.futures import ThreadPoolExecutor

# 설정 임포트
from config import DATA_DIR, DATA_US_DIR

# 유틸리티 함수 임포트
from utils import ensure_dir, safe_filename

def fill_missing_business_days_with_yfinance(csv_file, output_file=None):
    """
    CSV 파일에서 빠진 영업일을 yfinance에서 실제 데이터를 가져와 채우는 함수
    
    Args:
        csv_file: 입력 CSV 파일 경로
        output_file: 출력 CSV 파일 경로 (기본값: None, 입력 파일을 덮어씀)
        
    Returns:
        bool: 성공 여부
    """
    try:
        # 파일이 존재하는지 확인
        if not os.path.exists(csv_file):
            print(f"❌ 파일을 찾을 수 없음: {csv_file}")
            return False
            
        # CSV 파일 로드
        df = pd.read_csv(csv_file)
        
        # 날짜 컬럼이 있는지 확인
        if 'date' not in df.columns:
            print(f"❌ 날짜 컬럼이 없음: {csv_file}")
            return False
            
        # 빈 데이터프레임인 경우 (상장 폐지 종목)
        if len(df) == 0:
            print(f"⚠️ 빈 데이터프레임 (상장 폐지 종목): {csv_file}")
            return False
            
        # 날짜 컬럼을 datetime 형식으로 변환
        df['date'] = pd.to_datetime(df['date'], utc=True)
        
        # 심볼 컬럼이 있는지 확인
        symbol = None
        if 'symbol' in df.columns:
            symbol = df['symbol'].iloc[0]
        else:
            # 파일명에서 심볼 추출
            symbol = os.path.splitext(os.path.basename(csv_file))[0]
        
        # 날짜로 정렬
        df = df.sort_values('date')
        
        # 시작일과 종료일 가져오기
        start_date = df['date'].min().date()
        end_date = df['date'].max().date()
        today = datetime.now(timezone('UTC')).date()
        
        # 영업일 날짜 범위 생성 (주말 제외)
        business_days = pd.date_range(start=start_date, end=end_date, freq='B')
        
        # 기존 데이터의 날짜를 문자열로 변환하여 비교용 집합 생성
        existing_dates = set(df['date'].dt.strftime('%Y-%m-%d'))
        
        # 누락된 날짜 찾기
        missing_dates = []
        for date in business_days:
            date_str = date.strftime('%Y-%m-%d')
            if date_str not in existing_dates:
                missing_dates.append(date.date())
        
        if not missing_dates:
            print(f"✅ 누락된 영업일 없음: {os.path.basename(csv_file)}")
            return True
        
        print(f"🔍 누락된 영업일 발견: {os.path.basename(csv_file)} ({len(missing_dates)}개)")
        
        # 누락된 날짜가 있는 경우 yfinance에서 데이터 가져오기
        # 연속된 날짜 범위로 묶기
        date_ranges = []
        if missing_dates:
            current_range = [missing_dates[0]]
            for i in range(1, len(missing_dates)):
                if (missing_dates[i] - missing_dates[i-1]).days <= 5:  # 5일 이내면 같은 범위로 간주
                    current_range.append(missing_dates[i])
                else:
                    date_ranges.append((current_range[0], current_range[-1]))
                    current_range = [missing_dates[i]]
            date_ranges.append((current_range[0], current_range[-1]))
        
        # 각 날짜 범위에 대해 yfinance에서 데이터 가져오기
        new_data_frames = []
        for start, end in date_ranges:
            # 종료일은 다음 날짜로 설정 (yfinance는 종료일을 포함하지 않음)
            end_plus_one = end + timedelta(days=1)
            
            # 시도 횟수 설정
            max_retries = 3
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    print(f"[YF] 📊 데이터 요청 중: {symbol} ({start} ~ {end})")
                    ticker_obj = yf.Ticker(symbol)
                    new_df = ticker_obj.history(start=start, end=end_plus_one, interval="1d",
                                              auto_adjust=False, actions=False, timeout=10)
                    
                    if not new_df.empty:
                        new_df = new_df.rename_axis("date").reset_index()
                        new_df["symbol"] = symbol
                        new_data_frames.append(new_df)
                        print(f"[YF] ✅ 데이터 수신 성공: {symbol} ({start} ~ {end}, {len(new_df)} 행)")
                        success = True
                    else:
                        print(f"[YF] ⚠️ 빈 데이터 반환됨: {symbol} ({start} ~ {end})")
                        retry_count += 1
                        time.sleep(2)
                except Exception as e:
                    print(f"[YF] ❌ 오류 발생: {symbol} ({start} ~ {end}) - {str(e)[:100]}")
                    retry_count += 1
                    time.sleep(2)
        
        if not new_data_frames:
            print(f"⚠️ 누락된 날짜에 대한 데이터를 가져오지 못함: {os.path.basename(csv_file)}")
            return False
        
        # 새로운 데이터 병합
        if new_data_frames:
            new_data = pd.concat(new_data_frames, ignore_index=True)
            
            # 컬럼명 소문자로 변환
            new_data.columns = [col.lower() for col in new_data.columns]
            
            # 필요한 컬럼만 선택
            if all(col in new_data.columns for col in ['open', 'high', 'low', 'close', 'volume']):
                new_data = new_data[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            
            # 날짜 형식 통일 (모든 날짜를 UTC 시간대로 변환)
            if not pd.api.types.is_datetime64tz_dtype(new_data["date"]):
                new_data["date"] = pd.to_datetime(new_data["date"], utc=True)
            
            # 날짜 문자열 형식으로 변환하여 중복 제거 (시간대 문제 해결)
            df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")
            new_data["date_str"] = new_data["date"].dt.strftime("%Y-%m-%d")
            
            # 기존 데이터에서 새 데이터와 중복되는 날짜 제거
            df_filtered = df[~df["date_str"].isin(new_data["date_str"])]
            
            # 데이터 병합
            df_combined = pd.concat([df_filtered, new_data], ignore_index=True)
            
            # 임시 컬럼 제거
            df_combined.drop("date_str", axis=1, inplace=True)
            
            # 날짜로 정렬
            df_combined = df_combined.sort_values('date')
            
            # 결과 저장
            if output_file is None:
                output_file = csv_file
                
            # 결합된 데이터 저장
            df_combined.to_csv(output_file, index=False)
            # JSON 파일 생성 추가
            json_file = output_file.replace('.csv', '.json')
            df_combined.to_json(json_file, orient='records', indent=2, force_ascii=False)
            print(f"✅ 영업일 데이터 채움 완료: {os.path.basename(csv_file)} ({len(df)} → {len(df_combined)} 행)")
            return True
        else:
            print(f"⚠️ 새로운 데이터가 없음: {os.path.basename(csv_file)}")
            return False
        
    except Exception as e:
        print(f"❌ 영업일 데이터 채우기 오류: {csv_file} - {e}")
        return False

def process_all_csv_files(directory=DATA_US_DIR, max_workers=4):
    """
    디렉토리 내의 모든 CSV 파일에 대해 영업일 데이터 채우기 함수를 실행
    
    Args:
        directory: CSV 파일이 있는 디렉토리 경로
        max_workers: 최대 병렬 작업자 수
        
    Returns:
        tuple: (성공 수, 실패 수)
    """
    # 디렉토리 내의 모든 CSV 파일 찾기
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    
    if not csv_files:
        print(f"⚠️ CSV 파일을 찾을 수 없음: {directory}")
        return 0, 0
    
    print(f"🔍 총 {len(csv_files)}개의 CSV 파일 처리 중...")
    
    success_count = 0
    failure_count = 0
    
    # 병렬 처리
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(fill_missing_business_days_with_yfinance, csv_files))
    
    # 결과 집계
    success_count = sum(1 for result in results if result)
    failure_count = sum(1 for result in results if not result)
    
    print(f"\n📊 처리 결과: 성공 {success_count}, 실패 {failure_count}")
    return success_count, failure_count

# 메인 함수
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="영업일 데이터 채우기 도구 (yfinance 사용 버전)")
    parser.add_argument("--dir", type=str, default=DATA_US_DIR, help="CSV 파일이 있는 디렉토리 경로")
    parser.add_argument("--workers", type=int, default=4, help="병렬 작업자 수")
    parser.add_argument("--file", type=str, help="단일 CSV 파일 처리 (선택 사항)")
    
    args = parser.parse_args()
    
    if args.file:
        # 단일 파일 처리
        success = fill_missing_business_days_with_yfinance(args.file)
        if success:
            print("✅ 단일 파일 처리 완료")
        else:
            print("❌ 단일 파일 처리 실패")
    else:
        # 디렉토리 내 모든 파일 처리
        success_count, failure_count = process_all_csv_files(args.dir, args.workers)
        if success_count > 0:
            print(f"✅ {success_count}개 파일 처리 완료, {failure_count}개 실패")
        else:
            print(f"❌ 모든 파일 처리 실패 ({failure_count}개)")

if __name__ == "__main__":
    main()