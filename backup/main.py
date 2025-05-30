# -*- coding: utf-8 -*-
# 투자 스크리너 - 메인 실행 파일

import os
import sys
import argparse
import pandas as pd
import numpy as np
from datetime import datetime

# 데이터 수집 및 스크리닝 모듈 임포트
from data_collector import collect_data
from utils import create_required_dirs
# Markminervini 폴더의 모듈 임포트
from Markminervini.screener import run_screening, setup_scheduler
from Markminervini.filter_stock import run_integrated_screening, filter_us
from Markminervini.advanced_financial import run_advanced_financial_screening
from Markminervini.ticker_tracker import track_new_tickers
from Markminervini.filter_tickers import filter_new_tickers
from config import (
    BASE_DIR, DATA_DIR, RESULTS_DIR, RESULTS_VER2_DIR,
    US_WITH_RS_PATH, ADVANCED_FINANCIAL_RESULTS_PATH
)

# 옵션 분석 모듈 제거됨

# 명령행 인터페이스
def main():
    parser = argparse.ArgumentParser(description="투자 스크리너 - 통합 실행 프로그램")
    parser.add_argument("--integrated", action="store_true", help="통합 스크리닝 실행 (기술적 + 재무제표)")
    parser.add_argument("--process-only", action="store_true", help="데이터 수집을 제외한 모든 과정 순차 실행")
    parser.add_argument("--collect-hour", type=int, default=1, help="데이터 수집 시간 (24시간제)")
    parser.add_argument("--screen-hour", type=int, default=2, help="스크리닝 시간 (24시간제)")
    # 옵션 분석 관련 인자 제거됨
    
    args = parser.parse_args()
    
    # 필요한 디렉토리 생성
    create_required_dirs()
    
    # 실행 모드 결정
    if args.integrated:
        # 통합 스크리닝 실행
        print("\n🔍 통합 스크리닝 모드로 실행합니다...")
        run_integrated_screening()
        # Long-Short Portfolio 전략 실행
        run_long_short_portfolio()
        
        # 옵션 분석 관련 코드 제거됨
    elif args.process_only:
        # 데이터 수집을 제외한 모든 과정 순차 실행
        print("\n🔄 데이터 수집을 제외한 모든 과정을 순차적으로 실행합니다...")
        
        # 1. 기술적 스크리닝
        print("\n🔍 기술적 스크리닝을 실행합니다...")
        run_screening()
        
        # 2. 조건 필터링 및 RS 점수 정렬
        print("\n🔍 조건 필터링 및 RS 점수 정렬을 실행합니다...")
        filter_us()
        
        # 3. 재무제표 스크리닝
        print("\n💰 재무제표 스크리닝을 실행합니다...")
        run_advanced_financial_screening(force_update=False)
        
        # 4. 새로 추가된 티커 추적
        print("\n🔎 새로 추가된 티커를 추적합니다...")
        track_new_tickers(ADVANCED_FINANCIAL_RESULTS_PATH)
        
        # 5-1. new_tickers.csv에서 유효하지 않은 티커 필터링
        print("\n🧹 new_tickers.csv에서 유효하지 않은 티커를 필터링합니다...")
        filter_new_tickers()
        
        # 5. 통합 스크리닝
        print("\n🔍 통합 스크리닝을 실행합니다...")
        run_integrated_screening()
        
        # 6. Long-Short Portfolio 전략 실행
        print("\n🔍 Long-Short Portfolio 전략을 실행합니다...")
        run_long_short_portfolio()
        
        # 옵션 분석 관련 코드 제거됨
    else:
        # 기본 모드: 전체 프로세스 실행
        print("\n🚀 전체 프로세스 모드로 실행합니다...")
        
        # 1. 데이터 수집
        print("\n📊 데이터 수집을 실행합니다...")
        collect_data()
        
        # 2. 기술적 스크리닝
        print("\n🔍 기술적 스크리닝을 실행합니다...")
        run_screening()
        
        # 3. 조건 필터링 및 RS 점수 정렬
        print("\n🔍 조건 필터링 및 RS 점수 정렬을 실행합니다...")
        filter_us()
        
        # 4. 재무제표 스크리닝
        print("\n💰 재무제표 스크리닝을 실행합니다...")
        run_advanced_financial_screening(force_update=False)
        
        # 5. 새로 추가된 티커 추적
        print("\n🔎 새로 추가된 티커를 추적합니다...")
        track_new_tickers(ADVANCED_FINANCIAL_RESULTS_PATH)
        
        # 5-1. new_tickers.csv에서 유효하지 않은 티커 필터링
        print("\n🧹 new_tickers.csv에서 유효하지 않은 티커를 필터링합니다...")
        filter_new_tickers()
        
        # 6. 통합 스크리닝
        print("\n🔍 통합 스크리닝을 실행합니다...")
        run_integrated_screening()
        
        # 7. Long-Short Portfolio 전략 실행
        print("\n🔍 Long-Short Portfolio 전략을 실행합니다...")
        run_long_short_portfolio()
        
        # 옵션 분석 관련 코드 제거됨

# 통합 스크리닝 함수는 Markminervini.filter_stock으로 이동했습니다

# long_short_portfolio 모듈 연결
from long_short_portfolio.screener_ver2 import run_strategy1, run_strategy2, track_portfolio_strategy1, track_portfolio_strategy2
from long_short_portfolio.screener_ver3 import run_strategy3, run_strategy5
from long_short_portfolio.screener_ver4 import run_strategy4
from long_short_portfolio.screener_ver6 import run_strategy6

def run_long_short_portfolio():
    """long_short_portfolio 모듈의 전략을 실행하는 함수"""
    print("\n🔍 Long-Short Portfolio 전략을 실행합니다...")
    run_strategy1(total_assets=100000, update_existing=False) # 전략 1 실행
    run_strategy2(total_assets=100000, update_existing=False) # 전략 2 실행
    run_strategy3() # run_strategy3는 내부적으로 포트폴리오 생성을 처리합니다.
    run_strategy4(total_assets=100000, update_existing=False)
    run_strategy5(total_assets=100000, update_existing=False) # create_portfolio 파라미터 제거
    run_strategy6()


# 옵션 분석 래퍼 함수 제거됨

if __name__ == "__main__":
    main()