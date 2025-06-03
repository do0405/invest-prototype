#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 투자 스크리너 - 메인 실행 파일

import os
import sys
import argparse
import traceback
import pandas as pd
import importlib.util

from portfolio_managing import create_portfolio_manager

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'long_short_portfolio'))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'portfolio_managing'))

# 데이터 수집 및 스크리닝 모듈 임포트
from data_collector import collect_data
from utils import ensure_dir
from Markminervini.filter_stock import run_integrated_screening
from Markminervini.advanced_financial import run_advanced_financial_screening
from Markminervini.pattern_detection import analyze_tickers_from_results
from config import (
    DATA_US_DIR, RESULTS_DIR, RESULTS_VER2_DIR, OPTION_VOLATILITY_DIR,
    ADVANCED_FINANCIAL_RESULTS_PATH, ALPHA_VANTAGE_API_KEY
)
from Markminervini.ticker_tracker import track_new_tickers

# 옵션 기반 전략 모듈 임포트 (선택적)
try:
    from option_data_based_strategy.volatility_skew_reversal import VolatilitySkewScreener
except ImportError:
    VolatilitySkewScreener = None

# 포트폴리오 관리 모듈 임포트
try:
    from portfolio_managing.core.portfolio_manager import PortfolioManager
    from portfolio_managing.core.strategy_config import StrategyConfig
    print("✅ 포트폴리오 관리 모듈 임포트 성공")
except ImportError as e:
    print(f"⚠️ 포트폴리오 관리 모듈 임포트 실패: {e}")
    PortfolioManager = None
    StrategyConfig = None


def check_strategy_files_and_run_screening():
    """전략 결과 파일을 확인하고 필요시 스크리닝을 실행합니다."""
    strategy_files = {
        'strategy1': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy1_results.csv'),
        'strategy2': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy2_results.csv'),
        'strategy3': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy3_results.csv'),
        'strategy4': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy4_results.csv'),
        'strategy5': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy5_results.csv'),
        'strategy6': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy6_results.csv')
    }
    
    missing_files = []
    insufficient_files = []
    
    print("\n🔍 전략 결과 파일 상태 확인 중...")
    
    for strategy_name, file_path in strategy_files.items():
        if not os.path.exists(file_path):
            missing_files.append(strategy_name)
            print(f"❌ {strategy_name}: 파일 없음 - {file_path}")
        else:
            try:
                df = pd.read_csv(file_path)
                if len(df) < 10:  # 10개 미만 종목
                    insufficient_files.append(strategy_name)
                    print(f"⚠️ {strategy_name}: 종목 수 부족 ({len(df)}개) - {file_path}")
                else:
                    print(f"✅ {strategy_name}: 충분한 종목 수 ({len(df)}개)")
            except Exception as e:
                missing_files.append(strategy_name)
                print(f"❌ {strategy_name}: 파일 읽기 오류 - {e}")
    
    # 스크리닝이 필요한 경우
    strategies_need_screening = missing_files + insufficient_files
    
    if strategies_need_screening:
        print(f"\n🚨 스크리닝이 필요한 전략: {', '.join(strategies_need_screening)}")
        print("\n🔄 자동 스크리닝 시작...")
        
        # 기본 스크리닝 프로세스 실행
        run_all_screening_processes()
        
        # 필요한 전략만 실행
        execute_strategies(strategies_need_screening)
        
        print("\n✅ 자동 스크리닝 완료")
        return True
    else:
        print("\n✅ 모든 전략 파일이 충분한 종목을 포함하고 있습니다.")
        return False


def execute_strategies(strategy_list=None):
    """통합된 전략 실행 함수"""
    if strategy_list is None:
        strategy_list = [f'strategy{i}' for i in range(1, 7)]
    
    try:
        print(f"\n📊 전략 실행 시작: {strategy_list}")
        
        # 전략 모듈들 동적 로드
        strategy_modules = {}
        for strategy_name in strategy_list:
            module = load_strategy_module(strategy_name)
            if module:
                strategy_modules[strategy_name] = module
        
        # 각 전략 실행
        success_count = 0
        for strategy_name, module in strategy_modules.items():
            try:
                print(f"\n🔄 {strategy_name} 실행 중...")
                
                # 전략별 실행 함수 호출 (우선순위 순서)
                if hasattr(module, 'run_strategy'):
                    module.run_strategy()
                elif hasattr(module, f'run_{strategy_name}_screening'):
                    getattr(module, f'run_{strategy_name}_screening')()
                elif hasattr(module, 'main'):
                    module.main()
                else:
                    print(f"⚠️ {strategy_name}: 실행 함수를 찾을 수 없습니다.")
                    continue
                
                print(f"✅ {strategy_name} 실행 완료")
                success_count += 1
                
            except Exception as e:
                print(f"❌ {strategy_name} 실행 중 오류: {e}")
                print(traceback.format_exc())
        
        print(f"\n✅ 전략 실행 완료: {success_count}/{len(strategy_list)}개 성공")
        return success_count > 0
        
    except Exception as e:
        print(f"❌ 전략 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_portfolio_management_main():
    """포트폴리오 관리 메인 함수 - 기존 리소스 활용"""
    try:
        print("\n🚀 포트폴리오 관리 시작")
        
        # 포트폴리오 매니저 생성 (기존 함수 활용)
        portfolio_manager = create_portfolio_manager()
        
        # 1. 전략 결과 파일 처리 및 업데이트 (기존 메서드 활용)
        portfolio_manager.process_and_update_strategy_files()
        
        # 2. 매매 신호 모니터링 및 처리 (기존 메서드 활용)
        portfolio_manager.monitor_and_process_trading_signals()
        
        # 3. 모든 전략에 대한 포트폴리오 처리
        if hasattr(StrategyConfig, 'get_all_strategies'):
            for strategy_name in StrategyConfig.get_all_strategies():
                print(f"\n📊 {strategy_name} 처리 중...")
                
                # 전략 결과 로드
                strategy_results = portfolio_manager.load_strategy_results(strategy_name)
                
                if strategy_results is not None and not strategy_results.empty:
                    # 전략 신호 처리 (기존 메서드 활용)
                    added_count = portfolio_manager.process_strategy_signals(strategy_name, strategy_results)
                    print(f"✅ {strategy_name}: {added_count}개 포지션 추가")
                else:
                    print(f"⚠️ {strategy_name}: 처리할 결과 없음")
        
        # 4. 포지션 업데이트 및 리스크 체크
        portfolio_manager.position_tracker.update_positions()
        
        # 5. 청산 조건 확인 및 처리
        portfolio_manager.check_and_process_exit_conditions()
        
        # 6. 포트폴리오 리포트 생성
        portfolio_manager.generate_report()
        
        print("✅ 포트폴리오 관리 완료")
        
    except Exception as e:
        print(f"❌ 포트폴리오 관리 실패: {e}")
        import traceback
        traceback.print_exc()


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


def load_strategy_module(strategy_name):
    """전략 모듈을 동적으로 로드합니다."""
    try:
        strategy_path = os.path.join('long_short_portfolio', f'{strategy_name}.py')
        if not os.path.exists(strategy_path):
            print(f"⚠️ {strategy_name}: 파일이 존재하지 않습니다 - {strategy_path}")
            return None
            
        spec = importlib.util.spec_from_file_location(strategy_name, strategy_path)
        if spec is None:
            print(f"⚠️ {strategy_name}: 모듈 스펙을 생성할 수 없습니다")
            return None
            
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        print(f"✅ {strategy_name} 모듈 로드 성공")
        return module
        
    except Exception as e:
        print(f"⚠️ {strategy_name} 모듈 로드 실패: {e}")
        return None


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='투자 스크리너 및 포트폴리오 관리 시스템')
    parser.add_argument('--skip-data', action='store_true', help='데이터 수집 건너뛰기')
    parser.add_argument('--portfolio-only', action='store_true', help='포트폴리오 관리만 실행')
    parser.add_argument('--force-screening', action='store_true', help='강제 스크리닝 실행')
    parser.add_argument('--volatility-skew', action='store_true', help='변동성 스큐 역전 전략만 실행')
    parser.add_argument('--strategies', action='store_true', help='6개 전략 스크리닝만 실행')
    
    args = parser.parse_args()
    
    try:
        print("🚀 투자 스크리너 및 포트폴리오 관리 시스템 시작")
        
        # 필요한 디렉토리 생성
        ensure_directories()
        
        # 변동성 스큐 역전 전략만 실행
        if args.volatility_skew:
            run_volatility_skew_screening()
            return
        
        # 6개 전략 스크리닝만 실행
        if args.strategies:
            execute_strategies()
            return
        
        # 포트폴리오 관리만 실행
        if args.portfolio_only:
            run_portfolio_management_main()
            return
        
        # 전체 프로세스 실행
        if not args.skip_data:
            collect_data_main()
        
        # 강제 스크리닝 또는 전략 파일 상태 확인
        if args.force_screening:
            print("\n🔄 강제 스크리닝 모드...")
            run_all_screening_processes()
            run_pattern_analysis()
            execute_strategies()
            run_volatility_skew_screening()
        else:
            # 전략 파일 상태 확인 후 필요시에만 스크리닝
            screening_needed = check_strategy_files_and_run_screening()
            
            if not screening_needed:
                print("\n📊 패턴 분석 실행...")
                run_pattern_analysis()
        
        # 포트폴리오 관리 실행
        run_portfolio_management_main()
        
        print("\n🎉 모든 프로세스 완료!")
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 시스템 오류 발생: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()