# -*- coding: utf-8 -*-

import sys
sys.path.append("..")
# 투자 스크리너 - 기술적 스크리닝 모듈

import os
import pandas as pd
import numpy as np
try:
    import schedule
except ImportError:  # pragma: no cover - optional dependency
    schedule = None
import time
import argparse
from datetime import datetime, timedelta
from scipy.stats import rankdata

# 설정 파일 임포트
from config import (
    DATA_DIR, DATA_US_DIR, RESULTS_DIR,
    US_WITH_RS_PATH
)

# 유틸리티 함수 임포트
from utils import ensure_dir, load_csvs_parallel
from utils import calculate_rs_score, calculate_rs_score_enhanced

# 데이터 수집 함수 임포트
from data_collector import collect_data

# 트렌드 조건 계산 함수
def calculate_trend_template(df) -> pd.Series:
    # 기본 결과값 정의 (모든 조건 False)
    default_result = pd.Series({f'cond{i}': False for i in range(1,8)} | {'met_count': 0})
    
    # 입력 데이터가 None인 경우 처리
    if df is None:
        print("⚠️ 입력 데이터가 None입니다.")
        return default_result
    
    # 입력 데이터가 DataFrame이 아닌 경우 처리
    if not isinstance(df, pd.DataFrame):
        try:
            if isinstance(df, pd.Series):
                try:
                    df = df.to_frame().reset_index()
                except Exception as e:
                    print(f"❌ Series를 DataFrame으로 변환 오류: {e}")
                    return default_result
            else:
                # 빈 데이터 확인
                if df is None or (hasattr(df, '__len__') and len(df) == 0):
                    print("⚠️ 입력 데이터가 비어 있습니다.")
                    return default_result
                # 리스트나 다른 형식의 데이터인 경우
                try:
                    df = pd.DataFrame(df)
                except Exception as e:
                    print(f"❌ 데이터를 DataFrame으로 변환 오류: {e}")
                    return default_result
        except Exception as e:
            print(f"❌ 데이터 변환 오류: {e}")
            return default_result
    
    # 빈 데이터프레임 확인
    if df is None or df.empty:
        print("⚠️ 데이터프레임이 비어 있습니다.")
        return default_result
    
    # 안전한 복사본 생성
    try:
        df = df.copy()
    except Exception as e:
        print(f"❌ 데이터프레임 복사 오류: {e}")
        return default_result
    
    # 컬럼명이 있는 경우에만 소문자 변환 시도
    if hasattr(df, 'columns') and len(df.columns) > 0:
        try:
            # 컬럼명이 문자열인지 확인
            if all(isinstance(col, str) for col in df.columns):
                df.columns = df.columns.str.lower()
            else:
                # 문자열이 아닌 컬럼명이 있는 경우 처리
                new_columns = []
                for col in df.columns:
                    if isinstance(col, str):
                        new_columns.append(col.lower())
                    else:
                        new_columns.append(str(col).lower())
                df.columns = new_columns
        except Exception as e:
            print(f"❌ 컬럼명 변환 오류: {e}")
            # 오류 발생 시 원본 컬럼명 유지
    
    # 필요한 컬럼이 있는지 확인
    required_cols = ['open', 'high', 'low', 'close']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"⚠️ 필수 컬럼이 누락되었습니다: {missing_cols}")
        return default_result
    
    # 데이터 타입 변환 시도
    for col in required_cols:
        try:
            if not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        except Exception as e:
            print(f"❌ 컬럼 '{col}' 변환 오류: {e}")
            return default_result
    
    # 결측치 처리
    df = df.dropna(subset=required_cols)
    if df.empty:
        print("⚠️ 결측치 제거 후 데이터가 비어 있습니다.")
        return default_result
    
    # 날짜 정렬 (인덱스가 날짜인 경우)
    try:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.sort_index()
        elif 'date' in df.columns and pd.api.types.is_datetime64_dtype(df['date']):
            df = df.sort_values('date')
    except Exception as e:
        print(f"❌ 날짜 정렬 오류: {e}")
        # 정렬 오류 시 계속 진행
    
    # 이동평균 계산
    try:
        # 단기 이동평균
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma50'] = df['close'].rolling(window=50).mean()
        df['ma150'] = df['close'].rolling(window=150).mean()
        df['ma200'] = df['close'].rolling(window=200).mean()
        
        # 추가 지표
        df['vol_avg'] = df['volume'].rolling(window=50).mean()
        
        # 52주 고가/저가
        df['high_52w'] = df['high'].rolling(window=252).max()
        df['low_52w'] = df['low'].rolling(window=252).min()
    except Exception as e:
        print(f"❌ 이동평균 계산 오류: {e}")
        return default_result
    
    # 최신 데이터 추출
    try:
        latest = df.iloc[-1].copy()
    except Exception as e:
        print(f"❌ 최신 데이터 추출 오류: {e}")
        return default_result
    
    # 조건 계산
    result = pd.Series(dtype='object')
    
    # 조건 1: 현재 주가 > 150일 이동평균 > 200일 이동평균
    try:
        result['cond1'] = (
            latest['close'] > latest['ma150'] > latest['ma200']
        )
    except Exception as e:
        print(f"❌ 조건 1 계산 오류: {e}")
        result['cond1'] = False
    
    # 조건 2: 150일 이동평균이 상승 추세 (3개월 전보다 높음)
    try:
        days_ago_60 = max(0, len(df) - 60)
        if days_ago_60 > 0 and len(df) > days_ago_60:
            result['cond2'] = latest['ma150'] > df.iloc[days_ago_60]['ma150']
        else:
            result['cond2'] = False
    except Exception as e:
        print(f"❌ 조건 2 계산 오류: {e}")
        result['cond2'] = False
    
    # 조건 3: 200일 이동평균이 상승 추세 (1개월 전보다 높음)
    try:
        days_ago_20 = max(0, len(df) - 20)
        if days_ago_20 > 0 and len(df) > days_ago_20:
            result['cond3'] = latest['ma200'] > df.iloc[days_ago_20]['ma200']
        else:
            result['cond3'] = False
    except Exception as e:
        print(f"❌ 조건 3 계산 오류: {e}")
        result['cond3'] = False
    
    # 조건 4: 현재 주가 > 50일 이동평균
    try:
        result['cond4'] = latest['close'] > latest['ma50']
    except Exception as e:
        print(f"❌ 조건 4 계산 오류: {e}")
        result['cond4'] = False
    
    # 조건 5: 현재 주가가 52주 최저가보다 30% 이상 높음
    try:
        result['cond5'] = latest['close'] >= latest['low_52w'] * 1.3
    except Exception as e:
        print(f"❌ 조건 5 계산 오류: {e}")
        result['cond5'] = False
    
    # 조건 6: 현재 주가가 52주 최고가의 75% 이상
    try:
        result['cond6'] = latest['close'] >= latest['high_52w'] * 0.75
    except Exception as e:
        print(f"❌ 조건 6 계산 오류: {e}")
        result['cond6'] = False
    
    # 조건 7: 현재 주가가 20일 이동평균보다 높음
    try:
        result['cond7'] = latest['close'] > latest['ma20']
    except Exception as e:
        print(f"❌ 조건 7 계산 오류: {e}")
        result['cond7'] = False
    
    # 충족된 조건 수 계산
    try:
        condition_cols = [col for col in result.index if col.startswith('cond')]
        # 수정: Series의 불리언 값을 직접 합산하는 대신 .sum() 메서드 사용
        result['met_count'] = sum(result[col].astype(int) for col in condition_cols)
    except Exception as e:
        print(f"❌ 충족 조건 수 계산 오류: {e}")
        result['met_count'] = 0
    
    return result

# 상대 강도 계산 함수 (고도화된 버전)


# 미국 주식 스크리닝 실행 함수
def run_us_screening():
    """미국 주식 스크리닝을 실행하는 함수
    
    기술적 지표와 상대 강도(RS) 점수를 계산하여 스크리닝 결과를 생성합니다.
    """
    print("\n🇺🇸 미국 주식 스크리닝 시작...")
    try:
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
                file_path = os.path.join(DATA_US_DIR, file)
                # Windows 예약 파일명 처리 - 파일명에서 원래 티커 추출
                from utils import extract_ticker_from_filename
                symbol = extract_ticker_from_filename(file)
                
                # 개별 파일 로드
                df = pd.read_csv(file_path)
                
                # 컬럼명 소문자로 변환
                df.columns = [col.lower() for col in df.columns]
                
                # 날짜 컬럼 처리
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'], utc=True)
                    df = df.sort_values('date')
                else:
                    continue
                    
                # 최소 데이터 길이 확인
                if len(df) < 200:  # 최소 200일 데이터 필요
                    continue
                    
                # 트렌드 조건 계산
                trend_result = calculate_trend_template(df)
                trend_result.name = symbol
                results.append(trend_result)
            except Exception as e:
                print(f"❌ {file} 처리 오류: {e}")
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        result_df.index.name = 'symbol'
        
        # 상대 강도 계산을 위한 통합 데이터프레임 생성
        try:
            # 모든 종목의 종가 데이터를 하나의 데이터프레임으로 통합
            all_data = []
            processed_count = 0
            
            for file in us_files:
                try:
                    file_path = os.path.join(DATA_US_DIR, file)
                    symbol = os.path.splitext(file)[0]
                    df = pd.read_csv(file_path)
                    
                    # 컬럼명 소문자로 변환
                    df.columns = [col.lower() for col in df.columns]
                    
                    if 'date' in df.columns and 'close' in df.columns:
                        # 날짜 변환 및 정렬
                        df['date'] = pd.to_datetime(df['date'], utc=True)
                        df = df.sort_values('date')
                        
                        # 최소 데이터 길이 확인 (RS 계산에 필요한 최소 데이터)
                        if len(df) >= 126:  # RS 계산에 필요한 최소 기간
                            df['symbol'] = symbol  # 문자열 타입으로 심볼 추가
                            all_data.append(df[['date', 'symbol', 'close']])
                            processed_count += 1
                            
                            # 진행 상황 출력 (100개 단위)
                            if processed_count % 100 == 0:
                                print(f"⏳ RS 데이터 준비 중: {processed_count}개 종목 처리됨")
                except Exception as e:
                    continue
            
            print(f"ℹ️ RS 계산을 위해 {processed_count}개 종목 데이터 준비 완료")
            
            # RS 점수 계산
            rs_scores = pd.Series(dtype=float)
            if all_data:
                # 데이터 통합
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # 중복 데이터 제거
                combined_df = combined_df.drop_duplicates(subset=['date', 'symbol'])
                
                # 인덱스 설정
                combined_df = combined_df.set_index(['date', 'symbol'])
                
                # RS 점수 계산 (고도화된 버전 사용)
                print("📊 고도화된 RS 점수 계산 중...")
                rs_scores = calculate_rs_score(combined_df, price_col='close', use_enhanced=True)
                print(f"✅ RS 점수 계산 완료: {len(rs_scores)}개 종목")
        except Exception as e:
            print(f"❌ RS 점수 계산 오류: {e}")
            rs_scores = pd.Series(dtype=float)
        
        # RS 점수 병합
        try:
            # 인덱스 타입 일관성 확보
            result_df.index = result_df.index.astype(str)
            if len(rs_scores) > 0:
                rs_scores.index = rs_scores.index.astype(str)
                
                # 직접 매핑 방식으로 RS 점수 할당
                rs_dict = rs_scores.to_dict()
                rs_values = []
                
                for symbol in result_df.index:
                    rs_values.append(rs_dict.get(symbol, 50))  # 매칭되지 않으면 기본값 50 사용
                
                # RS 점수 할당
                result_df['rs_score'] = rs_values
            else:
                # RS 점수가 없는 경우 기본값 할당
                result_df['rs_score'] = 0
        except Exception as e:
            print(f"⚠️ RS 점수 병합 오류: {e}")
            # 기본값으로 채우기
            result_df['rs_score'] = 0
            
        # 조건 8 및 총 충족 조건 수 계산
        try:
            # rs_score 컬럼 데이터 타입 확인 및 변환
            result_df['rs_score'] = pd.to_numeric(result_df['rs_score'], errors='coerce').fillna(50)
            
            # RS 점수 80 이상인 경우 조건 8 충족
            result_df['cond8'] = result_df['rs_score'] >= 85
            
            # 조건 컬럼 확인
            condition_cols = [f'cond{i}' for i in range(1, 9) if f'cond{i}' in result_df.columns]
            
            # 충족 조건 수 업데이트 (RS 점수 포함)
            # 수정: DataFrame의 불리언 값을 직접 합산하는 대신 .sum(axis=1) 메서드 사용
            result_df['met_count'] = result_df[condition_cols].astype(int).sum(axis=1)
        except Exception as e:
            print(f"❌ 조건 8 계산 오류: {e}")
        
        # 결과 저장
        ensure_dir(os.path.dirname(US_WITH_RS_PATH))
        result_df.to_csv(US_WITH_RS_PATH)
        # JSON 파일 생성 추가
        json_path = US_WITH_RS_PATH.replace('.csv', '.json')
        result_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
        print(f"✅ 결과 저장 완료: {len(result_df)}개 종목, 경로: {US_WITH_RS_PATH}")
        
        # 상위 10개 종목 출력
        top_10 = result_df.sort_values('met_count', ascending=False).head(10)
        print("\n🏆 미국 주식 상위 10개 종목:")
        print(top_10[[f'cond{i}' for i in range(1, 9)] + ['rs_score', 'met_count']])
    except Exception as e:
        print(f"❌ 미국 주식 스크리닝 오류: {e}")

# 크립토 스크리닝 함수 제거됨

# 스크리닝 실행 함수
def run_screening():
    run_us_screening()

# 스케줄러 설정 함수
def setup_scheduler(collect_hour=1, screen_hour=2):
    if schedule is None:
        raise ImportError("schedule 패키지가 필요합니다")

    schedule.every().day.at(f"{collect_hour:02d}:00").do(collect_data)
    schedule.every().day.at(f"{screen_hour:02d}:00").do(run_screening)

    print(f"\n⏰ 스케줄러 설정 완료:")
    print(f"  - 데이터 수집: 매일 {collect_hour:02d}:00")
    print(f"  - 스크리닝: 매일 {screen_hour:02d}:00")

    while True:
        schedule.run_pending()
        time.sleep(60)
