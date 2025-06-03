#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 투자 스크리너 - 메인 실행 파일

import os
import sys
import argparse
import pandas as pd
import traceback

# 프로젝트 루트 디렉토리를 Python 경로에 추가 (최우선)
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'long_short_portfolio'))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'portfolio_managing'))

# 데이터 수집 및 스크리닝 모듈 임포트
from data_collector import collect_data
from utils import ensure_dir
# Markminervini 폴더의 모듈 임포트
from Markminervini.filter_stock import run_integrated_screening
from Markminervini.advanced_financial import run_advanced_financial_screening
from Markminervini.pattern_detection import analyze_tickers_from_results
from config import (
    DATA_US_DIR, RESULTS_DIR, RESULTS_VER2_DIR, OPTION_VOLATILITY_DIR,
    US_WITH_RS_PATH, ADVANCED_FINANCIAL_RESULTS_PATH, ALPHA_VANTAGE_API_KEY
)
# ticker_tracker import 추가
from Markminervini.ticker_tracker import track_new_tickers

# 기존 포트폴리오 관리 모듈 직접 활용
try:
    from portfolio_managing.core.portfolio_manager import (
        PortfolioManager, 
        run_integrated_portfolio_management,
        create_portfolio_manager
    )
    print("✅ 포트폴리오 관리 모듈 임포트 성공")
except ImportError as e:
    print(f"⚠️ 포트폴리오 관리 모듈 임포트 실패: {e}")
    PortfolioManager = None
    run_integrated_portfolio_management = None

# 변동성 스큐 스크리너 임포트
try:
    from option_data_based_strategy.volatility_skew_screener import VolatilitySkewScreener
except ImportError as e:
    print(f"⚠️ 변동성 스큐 모듈 임포트 실패: {e}")
    VolatilitySkewScreener = None


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="투자 스크리너 및 포트폴리오 관리 시스템")
    parser.add_argument('--run-all', action='store_true', help='데이터 수집, 스크리닝, 포트폴리오 관리를 모두 실행합니다.')
    parser.add_argument('--skip-data-collection', action='store_true', help='데이터 수집을 제외하고 스크리닝과 포트폴리오 관리를 실행합니다.')
    parser.add_argument('--portfolio-only', action='store_true', help='포트폴리오 관리만 실행합니다.')
    parser.add_argument('--volatility-skew', action='store_true', help='변동성 스큐 역전 전략 스크리닝을 실행합니다.')
    parser.add_argument('--strategies', action='store_true', help='6개 전략 스크리닝을 실행합니다.')

    args = parser.parse_args()

    # 필요한 디렉토리 생성
    ensure_directories()
    
    if args.run_all:
        print("🚀 전체 프로세스 실행: 데이터 수집, 스크리닝, 포트폴리오 관리")
        collect_data_main()
        run_all_screening_processes()
        run_pattern_analysis()
        run_six_strategies_screening()
        run_volatility_skew_screening()
        run_portfolio_management_main()
    elif args.skip_data_collection:
        print("🚀 데이터 수집 제외 실행: 스크리닝, 포트폴리오 관리")
        run_all_screening_processes()
        run_pattern_analysis()
        run_six_strategies_screening()
        run_volatility_skew_screening()
        run_portfolio_management_main()
    elif args.portfolio_only:
        print("🚀 포트폴리오 관리만 실행")
        run_portfolio_management_main()
    elif args.volatility_skew:
        print("🚀 변동성 스큐 역전 전략 스크리닝 실행")
        run_volatility_skew_screening()
    elif args.strategies:
        print("🚀 6개 전략 스크리닝 실행")
        run_six_strategies_screening()
    else:
        # 기본 실행 - 통합 포트폴리오 관리
        print("🚀 통합 포트폴리오 관리 시스템 시작 (기본 실행)")
        run_portfolio_management_main()


def ensure_directories():
    """필요한 디렉토리들을 생성합니다."""
    directories = [
        RESULTS_DIR, RESULTS_VER2_DIR, DATA_US_DIR,
        os.path.join(RESULTS_VER2_DIR, 'buy'),
        os.path.join(RESULTS_VER2_DIR, 'sell'),
        os.path.join(RESULTS_VER2_DIR, 'reports'),
        os.path.join(RESULTS_VER2_DIR, 'portfolio_management'),
        OPTION_VOLATILITY_DIR
    ]
    
    for directory in directories:
        ensure_dir(directory)


def run_six_strategies_screening():
    """6개 전략 스크리닝을 실행합니다."""
    try:
        print("\n📊 6개 전략 스크리닝 시작...")
        
        # 전략 모듈들 동적 import
        strategy_modules = {}
        for i in range(1, 7):
            try:
                module_name = f'strategy{i}'
                if os.path.exists(f'long_short_portfolio/{module_name}.py'):
                    exec(f'from long_short_portfolio import {module_name}')
                    strategy_modules[module_name] = eval(module_name)
                    print(f"✅ {module_name} 모듈 로드 성공")
            except Exception as e:
                print(f"⚠️ {module_name} 모듈 로드 실패: {e}")
        
        # 각 전략 실행
        for strategy_name, module in strategy_modules.items():
            try:
                print(f"\n🔄 {strategy_name} 실행 중...")
                
                # 전략별 실행 함수 호출
                if hasattr(module, 'run_strategy'):
                    module.run_strategy()
                elif hasattr(module, f'run_{strategy_name}_screening'):
                    getattr(module, f'run_{strategy_name}_screening')()
                else:
                    print(f"⚠️ {strategy_name}: 실행 함수를 찾을 수 없습니다.")
                    continue
                
                print(f"✅ {strategy_name} 실행 완료")
                
            except Exception as e:
                print(f"❌ {strategy_name} 실행 중 오류: {e}")
                print(traceback.format_exc())
        
        print("\n✅ 6개 전략 스크리닝 완료")
        
    except Exception as e:
        print(f"❌ 전략 스크리닝 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_pattern_analysis():
    """패턴 분석을 실행합니다."""
    try:
        print("\n📊 패턴 분석 시작...")
        
        results_dir = RESULTS_DIR
        data_dir = DATA_US_DIR
        output_dir = os.path.join(RESULTS_DIR, 'results2')
        
        # 패턴 분석 실행
        analyze_tickers_from_results(results_dir, data_dir, output_dir)
        
        print("✅ 패턴 분석 완료.")
        
    except Exception as e:
        print(f"❌ 패턴 분석 중 오류 발생: {e}")
        print(traceback.format_exc())


def collect_data_main():
    """데이터 수집 실행"""
    print("\n💾 데이터 수집 시작...")
    try:
        collect_data()
        print("✅ 데이터 수집 완료.")
    except Exception as e:
        print(f"❌ 데이터 수집 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_all_screening_processes():
    """모든 스크리닝 프로세스 실행"""
    print("\n⚙️ Mark Minervini 스크리닝 프로세스 시작...")
    try:
        # 1. 기본 스크리닝
        print("\n⏳ 1단계: 통합 스크리닝 실행 중...")
        run_integrated_screening()
        print("✅ 1단계: 통합 스크리닝 완료.")

        # 2. 고급 재무 스크리닝
        print("\n⏳ 2단계: 고급 재무 스크리닝 실행 중...")
        run_advanced_financial_screening()
        print("✅ 2단계: 고급 재무 스크리닝 완료.")

        # 3. 새로운 티커 추적
        print("\n⏳ 3단계: 새로운 티커 추적 실행 중...")
        track_new_tickers(ADVANCED_FINANCIAL_RESULTS_PATH)
        print("✅ 3단계: 새로운 티커 추적 완료.")

        print("\n✅ 모든 스크리닝 프로세스 완료.")
    except Exception as e:
        print(f"❌ 스크리닝 프로세스 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_volatility_skew_screening():
    """변동성 스큐 역전 전략 스크리닝을 실행합니다."""
    if not VolatilitySkewScreener:
        print("⚠️ VolatilitySkewScreener를 사용할 수 없습니다.")
        return
        
    try:
        print("\n📊 변동성 스큐 역전 전략 스크리닝 시작...")
        
        # Alpha Vantage API 키 설정
        api_key = ALPHA_VANTAGE_API_KEY if ALPHA_VANTAGE_API_KEY != "YOUR_ALPHA_VANTAGE_KEY" else None
        
        screener = VolatilitySkewScreener(alpha_vantage_key=api_key)
        results, filepath = screener.run_screening()
        
        if results:
            print(f"✅ 변동성 스큐 역전 전략 스크리닝 완료: {len(results)}개 종목 발견")
            print(f"📁 결과 파일: {filepath}")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
            
    except Exception as e:
        print(f"❌ 변동성 스큐 스크리닝 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_portfolio_management_main():
    """포트폴리오 관리 메인 실행 함수 - 기존 portfolio_manager.py 활용"""
    if not run_integrated_portfolio_management:
        print("⚠️ 포트폴리오 관리 모듈을 사용할 수 없습니다.")
        return
        
    try:
        print("\n🔄 통합 포트폴리오 관리 시스템 시작...")
        
        # 필요한 디렉토리 생성
        ensure_directories()
        
        # 기존 portfolio_manager.py의 통합 함수 직접 호출
        success = run_integrated_portfolio_management("main_portfolio")
        
        if success:
            print("✅ 통합 포트폴리오 관리 시스템 완료")
        else:
            print("⚠️ 포트폴리오 관리 중 일부 문제가 발생했습니다.")
        
    except Exception as e:
        print(f"❌ 포트폴리오 관리 시스템 오류: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()