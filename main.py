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
# 포트폴리오 관리 모듈 임포트
try:
    from portfolio_managing.core.portfolio_manager import PortfolioManager
    from portfolio_managing.core.strategy_config import StrategyConfig
    print("✅ 포트폴리오 관리 모듈 임포트 성공")
except ImportError as e:
    print(f"⚠️ 포트폴리오 관리 모듈 임포트 실패: {e}")
    PortfolioManager = None
    StrategyConfig = None


def execute_strategies(strategy_list=None, monitoring_only=False, screening_mode=False):
    """통합된 전략 실행 함수
    
    Args:
        strategy_list: 실행할 전략 리스트
        monitoring_only: True면 모니터링만 수행
        screening_mode: True면 스크리닝 모드로 실행
    """
    if strategy_list is None:
        strategy_list = [f'strategy{i}' for i in range(1, 7)]
    
    try:
        if monitoring_only:
            action_type = "모니터링"
        elif screening_mode:
            action_type = "스크리닝"
        else:
            action_type = "실행"
            
        print(f"\n📊 전략 {action_type} 시작: {strategy_list}")
        
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
                print(f"\n🔄 {strategy_name} {action_type} 중...")
                
                if monitoring_only:
                    # 모니터링 전용: 기존 포지션 추적/업데이트만
                    if hasattr(module, 'monitor_positions'):
                        module.monitor_positions()
                    elif hasattr(module, 'update_positions'):
                        module.update_positions()
                    elif hasattr(module, 'track_existing_positions'):
                        module.track_existing_positions()
                    else:
                        print(f"⚠️ {strategy_name}: 모니터링 함수를 찾을 수 없습니다. 스킵합니다.")
                        continue
                else:
                    # 스크리닝 또는 일반 실행 모드
                    if hasattr(module, 'run_strategy'):
                        module.run_strategy()
                    elif hasattr(module, f'run_{strategy_name}_screening'):
                        getattr(module, f'run_{strategy_name}_screening')()
                    elif hasattr(module, 'main'):
                        module.main()
                    else:
                        print(f"⚠️ {strategy_name}: 실행 함수를 찾을 수 없습니다.")
                        continue
                
                print(f"✅ {strategy_name} {action_type} 완료")
                success_count += 1
                
            except Exception as e:
                print(f"❌ {strategy_name} {action_type} 중 오류: {e}")
                # os 관련 오류는 상세 정보 출력하지 않음
                if "name 'os' is not defined" not in str(e):
                    print(traceback.format_exc())
        
        print(f"\n✅ 전략 {action_type} 완료: {success_count}/{len(strategy_list)}개 성공")
        return success_count > 0
        
    except Exception as e:
        print(f"❌ 전략 {action_type} 중 오류 발생: {e}")
        print(traceback.format_exc())

def check_strategy_file_status():
    """전략 결과 파일 상태만 확인하고 부족한 전략 리스트 반환"""
    strategy_files = {
        'strategy1': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy1_results.csv'),
        'strategy2': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy2_results.csv'),
        'strategy3': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy3_results.csv'),
        'strategy4': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy4_results.csv'),
        'strategy5': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy5_results.csv'),
        'strategy6': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy6_results.csv')
    }
    
    strategies_need_screening = []
    
    print("\n🔍 전략 결과 파일 상태 확인 중...")
    
    for strategy_name, file_path in strategy_files.items():
        if not os.path.exists(file_path):
            strategies_need_screening.append(strategy_name)
            print(f"❌ {strategy_name}: 파일 없음")
        else:
            try:
                df = pd.read_csv(file_path)
                if len(df) < 10:  # 10개 미만 종목
                    strategies_need_screening.append(strategy_name)
                    print(f"⚠️ {strategy_name}: 종목 수 부족 ({len(df)}개)")
                else:
                    print(f"✅ {strategy_name}: 충분한 종목 수 ({len(df)}개)")
            except Exception as e:
                strategies_need_screening.append(strategy_name)
                print(f"❌ {strategy_name}: 파일 읽기 오류")
    
    return strategies_need_screening

def ensure_directories():
    """필요한 디렉토리들을 생성합니다."""
    directories = [
        RESULTS_DIR, RESULTS_VER2_DIR, DATA_US_DIR, OPTION_VOLATILITY_DIR,
        os.path.join(RESULTS_VER2_DIR, 'buy'),
        os.path.join(RESULTS_VER2_DIR, 'sell'),
        os.path.join(RESULTS_VER2_DIR, 'reports'),
        os.path.join(RESULTS_VER2_DIR, 'portfolio_management')
    ]
    
    for directory in directories:
        ensure_dir(directory)

def run_pattern_analysis():
    """패턴 분석을 실행합니다."""
    try:
        print("\n📊 패턴 분석 시작...")
        
        output_dir = os.path.join(RESULTS_DIR, 'results2')
        analyze_tickers_from_results(RESULTS_DIR, DATA_US_DIR, output_dir)
        
        print("✅ 패턴 분석 완료")
        
    except Exception as e:
        print(f"❌ 패턴 분석 중 오류 발생: {e}")
        print(traceback.format_exc())

def collect_data_main():
    """데이터 수집 실행"""
    print("\n💾 데이터 수집 시작...")
    try:
        collect_data()
        print("✅ 데이터 수집 완료")
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
        
        # os 모듈을 전략 모듈에 주입
        module.os = os
        
        spec.loader.exec_module(module)
        
        print(f"✅ {strategy_name} 모듈 로드 성공")
        return module
        
    except Exception as e:
        if "name 'os' is not defined" in str(e):
            print(f"⚠️ {strategy_name}: os 모듈 오류 - 스킵합니다")
        else:
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
            create_portfolio_manager()
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
    # 전략 파일 상태 확인 및 필요시 스크리닝
            strategies_need_screening = check_strategy_file_status()
    
            if strategies_need_screening:
                print(f"\n🚨 스크리닝이 필요한 전략: {', '.join(strategies_need_screening)}")
                run_all_screening_processes()
                execute_strategies(strategies_need_screening)
            else:
                print("\n📊 패턴 분석 실행...")
                run_pattern_analysis()

# 포트폴리오 관리 실행
        create_portfolio_manager()
       
        print("\n🎉 모든 프로세스 완료!")
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 시스템 오류 발생: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
