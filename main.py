#!/usr/bin/env python3
# 투자 스크리너 - 메인 실행 파일

import os
import sys
import argparse
import pandas as pd
import traceback

# 프로젝트 루트 디렉토리를 Python 경로에 추가 (최우선)
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 데이터 수집 및 스크리닝 모듈 임포트
from data_collector import collect_data
from utils import ensure_dir
# Markminervini 폴더의 모듈 임포트
from Markminervini.filter_stock import run_integrated_screening
from Markminervini.advanced_financial import run_advanced_financial_screening
from Markminervini.pattern_detection import analyze_tickers_from_results
from config import (
    DATA_US_DIR, RESULTS_DIR, RESULTS_VER2_DIR,
    US_WITH_RS_PATH, ADVANCED_FINANCIAL_RESULTS_PATH
)
# ticker_tracker import 추가
from Markminervini.ticker_tracker import track_new_tickers

# 모듈 임포트 - try-except로 안전하게 처리
try:
    from long_short_portfolio.portfolio_integration import StrategyPortfolioIntegrator
except ImportError as e:
    print(f"⚠️ StrategyPortfolioIntegrator import 오류: {e}")
    print("📝 대체 import 시도 중...")
    try:
        # 대체 import 방법
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "portfolio_integration", 
            os.path.join(os.path.dirname(__file__), "long_short_portfolio", "portfolio_integration.py")
        )
        portfolio_integration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(portfolio_integration)
        StrategyPortfolioIntegrator = portfolio_integration.StrategyPortfolioIntegrator
        print("✅ 대체 import 성공")
    except Exception as e2:
        print(f"❌ 대체 import도 실패: {e2}")
        StrategyPortfolioIntegrator = None

# 변동성 스큐 스크리너 임포트
try:
    from option_data_based_strategy.volatility_skew_screener import VolatilitySkewScreener
except ImportError:
    VolatilitySkewScreener = None
    print("⚠️ VolatilitySkewScreener import 실패 - 옵션 전략 기능 비활성화")


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="투자 스크리너 및 포트폴리오 관리 시스템")
    parser.add_argument('--run-all', action='store_true', help='데이터 수집, 스크리닝, 포트폴리오 관리를 모두 실행합니다.')
    parser.add_argument('--skip-data-collection', action='store_true', help='데이터 수집을 제외하고 스크리닝과 포트폴리오 관리를 실행합니다.')
    parser.add_argument('--portfolio-only', action='store_true', help='포트폴리오 관리만 실행합니다.')
    # 새로운 옵션 추가
    parser.add_argument('--volatility-skew', action='store_true', help='변동성 스큐 역전 전략 스크리닝을 실행합니다.')

    args = parser.parse_args()

    # 필요한 디렉토리 생성
    ensure_dir(RESULTS_VER2_DIR)
    # 다음 두 줄 제거
    # ensure_dir(os.path.join(RESULTS_VER2_DIR, 'pattern_analysis_results'))
    # ensure_dir(os.path.join(RESULTS_VER2_DIR, 'option_volatility'))  # 새 디렉토리 추가
    
    if args.run_all:
        print("🚀 전체 프로세스 실행: 데이터 수집, 스크리닝, 포트폴리오 관리")
        collect_data_main()
        run_all_screening_processes()
        run_pattern_analysis()  # 이제 정의된 함수 호출
        run_volatility_skew_screening()
        run_portfolio_management_main()
    elif args.skip_data_collection:
        print("🚀 데이터 수집 제외 실행: 스크리닝, 포트폴리오 관리")
        run_all_screening_processes()
        run_pattern_analysis()
        run_volatility_skew_screening()  # 새로운 스크리닝 추가
        run_portfolio_management_main()
    elif args.portfolio_only:
        print("🚀 포트폴리오 관리만 실행")
        run_portfolio_management_main()
    elif args.volatility_skew:
        print("🚀 변동성 스큐 역전 전략 스크리닝 실행")
        run_volatility_skew_screening()
    else:
        # 기본 실행
        print("� 전략 포트폴리오 통합 시스템 시작 (기본 실행)")
        integrator = StrategyPortfolioIntegrator(initial_capital=100000)
        integrator.run_daily_cycle()


def run_strategies_if_needed(integrator: StrategyPortfolioIntegrator):
    """필요한 경우에만 전략을 실행합니다."""
    print("\n🔄 전략 실행 필요 여부 확인 중...")
    strategies_to_run = []

    # 각 전략에 대해 결과 파일 확인 (StrategyPortfolioIntegrator의 설정 사용)
    for strategy_name, config in integrator.strategies.items():
        file_path = config['result_file']
        run_this_strategy = False
        if not os.path.exists(file_path):
            print(f"⚠️ {strategy_name}: 결과 파일 없음 ({file_path}). 실행 필요.")
            run_this_strategy = True
        else:
            try:
                df = pd.read_csv(file_path)
                if len(df) < 10:
                    print(f"⚠️ {strategy_name}: 결과 파일에 {len(df)}개 항목만 존재 (10개 미만). 실행 필요.")
                    run_this_strategy = True
            except pd.errors.EmptyDataError:
                print(f"⚠️ {strategy_name}: 결과 파일이 비어 있음. 실행 필요.")
                run_this_strategy = True
            except Exception as e:
                print(f"⚠️ {strategy_name}: 결과 파일 읽기 오류 ({e}). 실행 필요.")
                run_this_strategy = True
        
        if run_this_strategy:
            strategies_to_run.append(strategy_name)

    if not strategies_to_run:
        print("✅ 모든 전략 결과가 최신 상태입니다. 추가 실행이 필요하지 않습니다.")
        return

    print(f"\n🚀 다음 전략 실행 예정: {', '.join(strategies_to_run)}")
    for strategy_name in strategies_to_run:
        try:
            print(f"\n📊 {strategy_name} 실행 중...")
            strategy_module = integrator.strategies[strategy_name]['module']
            strategy_module.run_strategy(total_capital=integrator.initial_capital)
            print(f"✅ {strategy_name} 실행 완료")
        except Exception as e:
            print(f"❌ {strategy_name} 실행 중 오류 발생: {e}")
            print(traceback.format_exc())


def ensure_directories():
    """필요한 디렉토리들을 생성합니다."""
    directories = [
        RESULTS_DIR, RESULTS_VER2_DIR, DATA_US_DIR, DATA_KR_DIR,
        os.path.join(RESULTS_VER2_DIR, 'buy'),
        os.path.join(RESULTS_VER2_DIR, 'sell'),
        os.path.join(RESULTS_VER2_DIR, 'performance'),
        os.path.join(RESULTS_VER2_DIR, 'reports'),
        # option_volatility 디렉토리 생성 제거 (이미 config.py에서 BASE_DIR 하위로 설정됨)
    ]
    
    for directory in directories:
        ensure_dir(directory)

def run_pattern_analysis():
    try:
        print("\n📊 패턴 분석 시작...")
        
        results_dir = RESULTS_DIR
        data_dir = DATA_US_DIR
        output_dir = os.path.join(RESULTS_DIR, 'results2')  # 경로 변경: results_ver2/pattern_analysis_results → results/results2
        
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

        # 2. 고급 재무 스크리닝 - 매개변수 제거
        print("\n⏳ 2단계: 고급 재무 스크리닝 실행 중...")
        run_advanced_financial_screening()  # 매개변수 제거
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
    try:
        print("\n📊 변동성 스큐 역전 전략 스크리닝 시작...")
        
        # Alpha Vantage API 키 설정 (config.py에서 가져오기)
        from config import ALPHA_VANTAGE_API_KEY
        api_key = ALPHA_VANTAGE_API_KEY if ALPHA_VANTAGE_API_KEY != "YOUR_ALPHA_VANTAGE_KEY" else None
        
        screener = VolatilitySkewScreener(alpha_vantage_key=api_key)
        results, filepath = screener.run_screening()  # run_full_screening() → run_screening()으로 수정
        
        if results:
            print(f"✅ 변동성 스큐 역전 전략 스크리닝 완료: {len(results)}개 종목 발견")
            print(f"📁 결과 파일: {filepath}")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
            
    except Exception as e:
        print(f"❌ 변동성 스큐 스크리닝 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_portfolio_management_main():
    """포트폴리오 관리 실행"""
    print("\n📊 포트폴리오 관리 시작")
    
    if StrategyPortfolioIntegrator is None:
        print("❌ StrategyPortfolioIntegrator를 import할 수 없어 포트폴리오 관리를 실행할 수 없습니다.")
        return
    
    try:
        # 객체 생성 전에 필요한 디렉토리 확인
        ensure_dir(RESULTS_VER2_DIR)
        ensure_dir(os.path.join(RESULTS_VER2_DIR, 'buy'))
        ensure_dir(os.path.join(RESULTS_VER2_DIR, 'sell'))
        
        print("🔄 StrategyPortfolioIntegrator 객체 생성 중...")
        integrator = StrategyPortfolioIntegrator(initial_capital=100000)
        
        # 객체가 제대로 생성되었는지 확인
        if not hasattr(integrator, 'manage_strategy_portfolio'):
            print("❌ manage_strategy_portfolio 메서드가 없습니다.")
            print(f"📝 사용 가능한 메서드: {[method for method in dir(integrator) if not method.startswith('_')]}")
            return
            
        print("✅ StrategyPortfolioIntegrator 객체 생성 완료")
        
        # 전략 실행 필요 여부 확인 및 실행
        run_strategies_if_needed(integrator)
        
        for strategy_name, strategy_config in integrator.strategies.items():
            portfolio_file_path = strategy_config['result_file']
            # 파일 존재 여부에 따라 is_initial_run 설정
            is_initial_run = not os.path.exists(portfolio_file_path) 
            
            print(f"\n▶️ {strategy_name} 관리 시작 (파일: {portfolio_file_path}, 초기 실행: {is_initial_run})")
            integrator.manage_strategy_portfolio(
                strategy_name,
                portfolio_file_path,
                is_initial_run=is_initial_run
            )
        
        # 일일 리포트 생성
        print("\n📝 일일 리포트 생성 중...")
        portfolio_summary = integrator._get_active_positions_summary_for_report()
        integrator.generate_daily_report(portfolio_summary)
        
        print("\n✅ 포트폴리오 관리 완료")
        
    except Exception as e:
        print(f"❌ 포트폴리오 관리 오류: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()