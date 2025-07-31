# -*- coding: utf-8 -*-
# 투자 스크리너 - 고급 재무제표 스크리닝 모듈

import os
import sys
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import time
import traceback
from config import (
    BASE_DIR, DATA_DIR, RESULTS_DIR,
    US_WITH_RS_PATH, ADVANCED_FINANCIAL_RESULTS_PATH, INTEGRATED_RESULTS_PATH,
    ADVANCED_FINANCIAL_CRITERIA,
    YAHOO_FINANCE_MAX_RETRIES, YAHOO_FINANCE_DELAY
)

# 유틸리티 함수 임포트
from utils import ensure_dir
from .financial_utils import (
    collect_financial_data,
    collect_financial_data_yahooquery,
    collect_financial_data_hybrid,
    screen_advanced_financials,
    calculate_percentile_rank,
)

# 필요한 디렉토리 생성
ensure_dir(RESULTS_DIR)


def run_advanced_financial_screening(force_update=False, skip_data=False):
    """고급 재무 분석 실행"""
    if skip_data:
        print("⏭️ OHLCV 업데이트 없이 기존 데이터로 재무 분석 진행")
    print("\n=== 고급 재무 분석 시작 ===")
    
    # results2 디렉토리가 없으면 생성
    # results 디렉토리는 이미 생성되어 있음
    
    try:
        print("\n📊 고급 재무제표 스크리닝 시작...")
        
        # US 주식 데이터 로드
        if not os.path.exists(US_WITH_RS_PATH):
            print(f"❌ US 주식 데이터 파일이 없습니다: {US_WITH_RS_PATH}")
            return

        us_df = pd.read_csv(US_WITH_RS_PATH)
        print(f"✅ US 주식 데이터 로드 완료: {len(us_df)}개 종목")
        
        # 심볼 목록 추출
        if 'symbol' not in us_df.columns:
            print(f"⚠️ 'symbol' 컬럼이 없습니다. 사용 가능한 컬럼: {', '.join(us_df.columns.tolist())}")
            return
        
        # us_with_rs.csv에 있는 종목만 처리
        symbols = us_df['symbol'].tolist()
        
        if not symbols:
            print("❌ 분석할 심볼이 없습니다.")
            return
        
        print(f"📈 분석할 종목 수: {len(symbols)}")
        
        # 재무제표 데이터 수집 (yfinance + yahooquery)
        print("\n💡 하이브리드 방식으로 재무 데이터를 수집합니다 (yfinance → yahooquery)")
        financial_data = collect_financial_data_hybrid(symbols, max_retries=2, delay=1.0)
        
        # 재무제표 스크리닝
        if not financial_data.empty:
            # 재무제표 스크리닝 실행
            result_df = screen_advanced_financials(financial_data)
            
            if not result_df.empty:
                # RS 점수 데이터 병합
                if 'rs_score' in us_df.columns:
                    rs_data = us_df[['symbol', 'rs_score']]
                    final_df = pd.merge(result_df, rs_data, on='symbol', how='right')  # right join으로 변경
                    
                    # 각 지표의 하위 백분위 계산 (us_with_rs.csv의 종목들끼리 비교)
                    # RS 점수 백분위 계산
                    rs_percentiles = calculate_percentile_rank(us_df['rs_score'])
                    rs_percentile_dict = dict(zip(us_df['symbol'], rs_percentiles))
                    final_df['rs_percentile'] = final_df['symbol'].map(rs_percentile_dict)
                    
                    # 재무 지표 백분위 계산 (us_with_rs.csv의 종목들끼리 비교)
                    fin_percentiles = calculate_percentile_rank(result_df['fin_met_count'])
                    fin_percentile_dict = dict(zip(result_df['symbol'], fin_percentiles))
                    final_df['fin_percentile'] = final_df['symbol'].map(fin_percentile_dict)
                    
                    # 누락된 값 처리 (FutureWarning 방지)
                    final_df['fin_met_count'] = final_df['fin_met_count'].fillna(0).infer_objects(copy=False)
                    final_df['has_error'] = final_df['has_error'].fillna(True).infer_objects(copy=False)
                    final_df['fin_percentile'] = final_df['fin_percentile'].fillna(0).infer_objects(copy=False)
                    
                    # 백분위 합계 계산
                    final_df['total_percentile'] = final_df['rs_percentile'] + final_df['fin_percentile']
                    
                    # 정렬 기준:
                    # 1. fin_met_count가 9인 종목 우선
                    # 2. total_percentile (내림차순)
                    # 3. rs_score (내림차순)
                    final_df['is_perfect'] = final_df['fin_met_count'] == 9
                    final_df = final_df.sort_values(
                        ['is_perfect', 'total_percentile', 'rs_score'],
                        ascending=[False, False, False]
                    )
                    final_df = final_df.drop('is_perfect', axis=1)  # 임시 컬럼 제거
                    
                    # 결과 저장 (간소화된 컬럼만)
                    final_df.to_csv(ADVANCED_FINANCIAL_RESULTS_PATH, index=False, mode='w')
                    # JSON 파일 생성 추가
                    json_path = ADVANCED_FINANCIAL_RESULTS_PATH.replace('.csv', '.json')
                    final_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
                    
                    # integrated_results 저장
                    final_df.to_csv(INTEGRATED_RESULTS_PATH, index=False, mode='w')
                    integrated_json_path = INTEGRATED_RESULTS_PATH.replace('.csv', '.json')
                    final_df.to_json(integrated_json_path, orient='records', indent=2, force_ascii=False)
                    
                    # 통합 스크리너 실행 (패턴 감지 포함)
                    print("\n🔍 통합 패턴 감지 스크리너 실행 중...")
                    try:
                        from .integrated_screener import run_integrated_screening
                        
                        # 상위 50개 심볼만 패턴 감지
                        top_symbols = final_df.head(50)['symbol'].tolist()
                        if top_symbols:
                            pattern_results = run_integrated_screening(max_symbols=len(top_symbols))
                            print(f"✅ 패턴 감지 완료: {len(pattern_results)}개 심볼 처리")
                        else:
                            print("⚠️ 패턴 감지할 심볼이 없습니다.")
                    except Exception as e:
                        print(f"⚠️ 통합 패턴 감지 오류: {e}")
                    
                    # 에러가 있는 종목 출력
                    error_df = final_df[final_df['has_error'] == True]
                    if not error_df.empty:
                        print("\n⚠️ 데이터 수집 또는 계산 중 오류가 발생한 종목:")
                        for _, row in error_df.iterrows():
                            print(f"- {row['symbol']}")
                    
                    # 상위 10개 종목 출력
                    top_10 = final_df.head(10)
                    print("\n🏆 상위 10개 종목:")
                    print(top_10[['symbol', 'fin_met_count', 'rs_score', 'total_percentile', 'has_error']])
                else:
                    # RS 점수가 없는 경우
                    result_df.to_csv(ADVANCED_FINANCIAL_RESULTS_PATH, index=False, mode='w')
                    # JSON 파일 생성 추가
                    json_path = ADVANCED_FINANCIAL_RESULTS_PATH.replace('.csv', '.json')
                    result_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
                    
                    # 에러가 있는 종목 출력
                    error_df = result_df[result_df['has_error'] == True]
                    if not error_df.empty:
                        print("\n⚠️ 데이터 수집 또는 계산 중 오류가 발생한 종목:")
                        for _, row in error_df.iterrows():
                            print(f"- {row['symbol']}")
                    
                    # 상위 10개 종목 출력
                    top_10 = result_df.sort_values('fin_met_count', ascending=False).head(10)
                    print("\n🏆 고급 재무제표 스크리닝 상위 10개 종목:")
                    print(top_10[['symbol', 'fin_met_count', 'has_error']])
            else:
                print("❌ 스크리닝 결과가 비어 있습니다.")
        else:
            print("❌ 재무제표 데이터가 없어 스크리닝을 진행할 수 없습니다.")
    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

# 직접 실행 시
