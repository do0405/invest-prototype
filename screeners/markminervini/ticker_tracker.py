# -*- coding: utf-8 -*-
# 새로 추가된 티커 추적 모듈

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import csv

import sys
sys.path.append("..")
from config import MARKMINERVINI_RESULTS_DIR, US_WITH_RS_PATH

# 새로 추가된 티커를 저장할 파일 경로
NEW_TICKERS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'new_tickers.csv')
# 이전 us_with_rs.csv 파일 백업 경로
PREVIOUS_US_WITH_RS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'previous_us_with_rs.csv')

def track_new_tickers(advanced_financial_results_path):
    """
    us_with_rs.csv 파일을 분석하여 새로 추가된 티커를 추적하고 관리합니다.
    새로 추가된 티커는 new_tickers.csv 파일에 저장되며, 2주 이상 지난 데이터는 자동으로 삭제됩니다.
    
    Args:
        advanced_financial_results_path (str): 고급 재무제표 분석 결과 파일 경로
    """
    print("\n🔍 새로 추가된 티커를 추적합니다...")
    
    # 현재 날짜
    today = datetime.now().date()
    
    # us_with_rs.csv 파일 로드
    try:
        current_us_with_rs = pd.read_csv(US_WITH_RS_PATH)
        
        # symbol 컬럼이 이미 존재하는지 확인
        if 'symbol' not in current_us_with_rs.columns:
            print(f"경고: {US_WITH_RS_PATH} 파일에 symbol 컬럼이 없습니다.")
            return
                
    except FileNotFoundError:
        print(f"경고: {US_WITH_RS_PATH} 파일을 찾을 수 없습니다.")
        return
    except Exception as e:
        print(f"오류: us_with_rs.csv 파일을 로드하는 중 오류가 발생했습니다: {e}")
        return
    
    # 고급 재무제표 분석 결과 파일 로드
    try:
        financial_results = pd.read_csv(advanced_financial_results_path)
    except FileNotFoundError:
        print(f"경고: {advanced_financial_results_path} 파일을 찾을 수 없습니다.")
        return
    except Exception as e:
        print(f"오류: 고급 재무제표 분석 결과 파일을 로드하는 중 오류가 발생했습니다: {e}")
        return
    
    # 이전 us_with_rs.csv 파일 로드 또는 생성
    if os.path.exists(PREVIOUS_US_WITH_RS_PATH):
        try:
            previous_us_with_rs = pd.read_csv(PREVIOUS_US_WITH_RS_PATH)
            
            # symbol 컬럼이 이미 존재하는지 확인
            if 'symbol' not in previous_us_with_rs.columns:
                print(f"경고: {PREVIOUS_US_WITH_RS_PATH} 파일에 symbol 컬럼이 없습니다.")
                previous_us_with_rs = pd.DataFrame()  # 빈 DataFrame으로 설정
                    
            previous_symbols = set(previous_us_with_rs['symbol'].tolist())
        except Exception as e:
            print(f"경고: 이전 us_with_rs.csv 파일을 로드하는 중 오류가 발생했습니다: {e}")
            previous_symbols = set()
    else:
        previous_symbols = set()
    
    # 현재 us_with_rs.csv의 심볼 목록 추출
    current_symbols = set(current_us_with_rs['symbol'].tolist())
    
    # 새로 추가된 티커 찾기 (현재 - 이전)
    new_symbols = current_symbols - previous_symbols
    
    # 새로 추가된 티커 파일 로드 또는 생성
    if os.path.exists(NEW_TICKERS_PATH):
        try:
            new_tickers_df = pd.read_csv(NEW_TICKERS_PATH)
        except Exception as e:
            print(f"오류: 새로 추가된 티커 파일을 로드하는 중 오류가 발생했습니다: {e}")
            new_tickers_df = pd.DataFrame(columns=['symbol', 'fin_met_count', 'rs_score', 'met_count', 'total_met_count', 'added_date'])
    else:
        new_tickers_df = pd.DataFrame(columns=['symbol', 'fin_met_count', 'rs_score', 'met_count', 'total_met_count', 'added_date'])
    
    # 새로 추가된 티커가 있으면 처리
    if new_symbols:
        print(f"새로 추가된 티커 {len(new_symbols)}개를 발견했습니다.")
        
        # 새로 추가된 티커 정보 추출
        new_tickers_info = []
        for symbol in new_symbols:
            # us_with_rs.csv에서 RS 점수 가져오기
            us_data = current_us_with_rs[current_us_with_rs['symbol'] == symbol]
            if us_data.empty:
                continue
                
            rs_score = us_data.iloc[0].get('rs_score', 0)
            
            # 재무제표 분석 결과에서 정보 가져오기
            fin_data = financial_results[financial_results['symbol'] == symbol]
            fin_met_count = 0
            met_count = 0
            total_met_count = 0
            
            if not fin_data.empty:
                fin_met_count = fin_data.iloc[0].get('fin_met_count', 0)
                
                # 기술적 지표 충족 개수 (8개 중)
                # 먼저 us_with_rs.csv에서 met_count 값을 가져옵니다
                us_data_met_count = us_data.iloc[0].get('met_count', 0)
                if us_data_met_count > 0:
                    met_count = us_data_met_count
                else:
                    # tech_ 컬럼이 없는 경우 대체 방법으로 시도
                    tech_columns = [col for col in fin_data.columns if col.startswith('tech_')]
                    if tech_columns:
                        met_count = fin_data.iloc[0][tech_columns].sum()
                    else:
                        # integrated_results.csv 파일에서 met_count 값을 가져오는 로직 추가
                        integrated_results_path = os.path.join(MARKMINERVINI_RESULTS_DIR, 'integrated_results.csv')
                        if os.path.exists(integrated_results_path):
                            try:
                                integrated_df = pd.read_csv(integrated_results_path)
                                integrated_data = integrated_df[integrated_df['symbol'] == symbol]
                                if not integrated_data.empty:
                                    met_count = integrated_data.iloc[0].get('met_count', 0)
                            except Exception as e:
                                print(f"통합 결과 파일 로드 중 오류: {e}")
                
                # 총 충족 지표 개수
                total_met_count = fin_met_count + met_count
            
            # 새로운 티커 정보 추가
            new_tickers_info.append({
                'symbol': symbol,
                'fin_met_count': fin_met_count,
                'rs_score': rs_score,
                'met_count': met_count,
                'total_met_count': total_met_count,
                'added_date': today.strftime('%Y-%m-%d')
            })
        
        # 새로운 티커 정보를 DataFrame으로 변환
        new_tickers_df_to_add = pd.DataFrame(new_tickers_info)
        
        # 기존 데이터와 새로운 데이터 병합 (빈 DataFrame 체크)
        if not new_tickers_df_to_add.empty:
            if new_tickers_df.empty:
                new_tickers_df = new_tickers_df_to_add.copy()
            else:
                new_tickers_df = pd.concat([new_tickers_df, new_tickers_df_to_add], ignore_index=True)
    else:
        print("새로 추가된 티커가 없습니다.")
        
    # 현재 us_with_rs.csv를 이전 파일로 백업
    try:
        # 이전 결과 백업
        current_us_with_rs.to_csv(PREVIOUS_US_WITH_RS_PATH, index=False)
        # JSON 파일 생성 추가
        json_path = PREVIOUS_US_WITH_RS_PATH.replace('.csv', '.json')
        current_us_with_rs.to_json(json_path, orient='records', indent=2, force_ascii=False)
        
    except Exception as e:
        print(f"경고: 현재 us_with_rs.csv 파일을 백업하는 중 오류가 발생했습니다: {e}")
    
    # 2주 이상 지난 데이터 삭제
    two_weeks_ago = today - timedelta(days=14)
    new_tickers_df['added_date'] = pd.to_datetime(new_tickers_df['added_date'], utc=True).dt.date
    new_tickers_df = new_tickers_df[new_tickers_df['added_date'] > two_weeks_ago]
    
    # 추가된 날짜 기준으로 내림차순 정렬 (최신 데이터가 위로)
    new_tickers_df = new_tickers_df.sort_values(by='added_date', ascending=False)
    
    # 결과 저장
    try:
        new_tickers_df.to_csv(NEW_TICKERS_PATH, index=False)
        json_path = NEW_TICKERS_PATH.replace('.csv', '.json')
        new_tickers_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
        print(f"새로 추가된 티커 정보를 {NEW_TICKERS_PATH}에 저장했습니다.")
        print(f"현재 추적 중인 티커 수: {len(new_tickers_df)}")
        
        # 새로 추가된 티커에 대해 통합 스크리너 실행 (패턴 감지 포함)
        if new_symbols and len(new_symbols) > 0:
            try:
                print("\n🔍 새로 추가된 티커에 대한 통합 패턴 감지 스크리너 실행 중...")
                from .integrated_screener import run_integrated_screening
                
                # 새로 추가된 심볼만 패턴 감지
                new_symbols_list = list(new_symbols)
                if new_symbols_list:
                    pattern_results = run_integrated_screening(max_symbols=len(new_symbols_list))
                    print(f"✅ 새 티커 패턴 감지 완료: {len(pattern_results)}개 심볼 처리")
                else:
                    print("⚠️ 패턴 감지할 새 심볼이 없습니다.")
            except Exception as e:
                print(f"⚠️ 새 티커 통합 패턴 감지 오류: {e}")
                
    except Exception as e:
        print(f"오류: 새로 추가된 티커 정보를 저장하는 중 오류가 발생했습니다: {e}")
