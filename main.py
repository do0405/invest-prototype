#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 투자 스크리너 - 메인 실행 파일

import os
import sys
import argparse
import traceback
import pandas as pd
import importlib.util
try:
    import schedule
except ImportError:
    schedule = None
import time
from datetime import datetime

from portfolio.manager import create_portfolio_manager

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'portfolio', 'long_short'))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'portfolio', 'manager'))

# 데이터 수집 및 스크리닝 모듈 임포트
from data_collector import collect_data
from utils import ensure_dir
from screeners.markminervini.filter_stock import run_integrated_screening
from screeners.markminervini.advanced_financial import run_advanced_financial_screening
from screeners.markminervini.pattern_detection import analyze_tickers_from_results
from screeners.us_setup.screener import screen_us_setup
from screeners.us_gainer.screener import screen_us_gainers
from config import (
    DATA_US_DIR,
    RESULTS_DIR,
    SCREENER_RESULTS_DIR,
    PORTFOLIO_BUY_DIR,
    PORTFOLIO_SELL_DIR,
    OPTION_VOLATILITY_DIR,
    ADVANCED_FINANCIAL_RESULTS_PATH,
    ALPHA_VANTAGE_API_KEY,
)
from screeners.markminervini.ticker_tracker import track_new_tickers
# 포트폴리오 관리 모듈 임포트
try:
    from portfolio.manager.core.portfolio_manager import PortfolioManager
    from portfolio.manager.core.strategy_config import StrategyConfig
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
        if StrategyConfig is not None:
            strategy_list = StrategyConfig.get_all_strategies()
        else:
            strategy_list = [f'strategy{i}' for i in range(1, 7)]
    
    try:
        if monitoring_only:
            action_type = "모니터링"
        elif screening_mode:
            action_type = "스크리닝"
        else:
            action_type = "실행"
            
        print(f"\n📊 전략 {action_type} 시작: {strategy_list}")
        print(f"🔍 총 {len(strategy_list)}개 전략을 처리합니다.")
        
        # 전략 모듈들 동적 로드
        strategy_modules = {}
        print(f"\n📦 전략 모듈 로딩 시작...")
        for i, strategy_name in enumerate(strategy_list, 1):
            print(f"  [{i}/{len(strategy_list)}] {strategy_name} 모듈 로딩 중...")
            module = load_strategy_module(strategy_name)
            if module:
                strategy_modules[strategy_name] = module
                print(f"  ✅ {strategy_name} 모듈 로딩 성공")
            else:
                print(f"  ❌ {strategy_name} 모듈 로딩 실패")
        
        print(f"\n📊 로딩된 모듈: {len(strategy_modules)}/{len(strategy_list)}개")
        
        # 각 전략 실행
        success_count = 0
        for i, (strategy_name, module) in enumerate(strategy_modules.items(), 1):
            try:
                print(f"\n🔄 [{i}/{len(strategy_modules)}] {strategy_name} {action_type} 시작...")
                print(f"⏰ 현재 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                if monitoring_only:
                    # 모니터링 전용: 기존 포지션 추적/업데이트만
                    if hasattr(module, 'monitor_positions'):
                        print(f"  📊 {strategy_name}: monitor_positions() 실행 중...")
                        module.monitor_positions()
                    elif hasattr(module, 'update_positions'):
                        print(f"  📊 {strategy_name}: update_positions() 실행 중...")
                        module.update_positions()
                    elif hasattr(module, 'track_existing_positions'):
                        print(f"  📊 {strategy_name}: track_existing_positions() 실행 중...")
                        module.track_existing_positions()
                    else:
                        print(f"⚠️ {strategy_name}: 모니터링 함수를 찾을 수 없습니다. 스킵합니다.")
                        continue
                else:
                    # 스크리닝 또는 일반 실행 모드
                    if hasattr(module, 'run_strategy'):
                        print(f"  🚀 {strategy_name}: run_strategy() 실행 중...")
                        module.run_strategy()
                    elif hasattr(module, f'run_{strategy_name}_screening'):
                        print(f"  🚀 {strategy_name}: run_{strategy_name}_screening() 실행 중...")
                        getattr(module, f'run_{strategy_name}_screening')()
                    elif hasattr(module, 'main'):
                        print(f"  🚀 {strategy_name}: main() 실행 중...")
                        module.main()
                    else:
                        print(f"⚠️ {strategy_name}: 실행 함수를 찾을 수 없습니다.")
                        continue
                
                print(f"✅ {strategy_name} {action_type} 완료")
                success_count += 1
                print(f"📈 진행률: {success_count}/{len(strategy_modules)} ({success_count/len(strategy_modules)*100:.1f}%)")
                
            except Exception as e:
                print(f"❌ {strategy_name} {action_type} 중 오류: {e}")
                print(f"🔍 오류 발생 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
                # os 관련 오류는 상세 정보 출력하지 않음
                if "name 'os' is not defined" not in str(e):
                    print(traceback.format_exc())
        
        print(f"\n✅ 전략 {action_type} 완료: {success_count}/{len(strategy_list)}개 성공")
        print(f"📊 성공률: {success_count/len(strategy_list)*100:.1f}%")
        return success_count > 0
        
    except Exception as e:
        print(f"❌ 전략 {action_type} 중 오류 발생: {e}")
        print(f"🔍 오류 발생 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(traceback.format_exc())

def check_strategy_file_status():
    """전략 결과 파일 상태만 확인하고 부족한 전략 리스트 반환"""
    strategy_files = {
        'strategy1': os.path.join(PORTFOLIO_BUY_DIR, 'strategy1_results.csv'),
        'strategy2': os.path.join(PORTFOLIO_SELL_DIR, 'strategy2_results.csv'),
        'strategy3': os.path.join(PORTFOLIO_BUY_DIR, 'strategy3_results.csv'),
        'strategy4': os.path.join(PORTFOLIO_BUY_DIR, 'strategy4_results.csv'),
        'strategy5': os.path.join(PORTFOLIO_BUY_DIR, 'strategy5_results.csv'),
        'strategy6': os.path.join(PORTFOLIO_SELL_DIR, 'strategy6_results.csv'),
        'volatility_skew': os.path.join(PORTFOLIO_BUY_DIR, 'volatility_skew_results.csv'),
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
        RESULTS_DIR,
        SCREENER_RESULTS_DIR,
        PORTFOLIO_BUY_DIR,
        PORTFOLIO_SELL_DIR,
        DATA_US_DIR,
        OPTION_VOLATILITY_DIR,
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

        # 4. 변동성 스큐 스크리닝
        print("\n⏳ 4단계: 변동성 스큐 스크리닝 실행 중...")
        run_volatility_skew_portfolio()
        print("✅ 4단계: 변동성 스큐 스크리닝 완료.")

        # 5. US Setup 스크리닝
        print("\n⏳ 5단계: US Setup 스크리닝 실행 중...")
        run_setup_screener()
        print("✅ 5단계: US Setup 스크리닝 완료.")

        # 6. US Gainers 스크리닝
        print("\n⏳ 6단계: US Gainers 스크리닝 실행 중...")
        run_gainers_screener()
        print("✅ 6단계: US Gainers 스크리닝 완료.")

        print("\n✅ 모든 스크리닝 프로세스 완료.")
    except Exception as e:
        print(f"❌ 스크리닝 프로세스 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_volatility_skew_portfolio():
    """변동성 스큐 전략을 실행해 포트폴리오 신호를 생성합니다."""
    try:
        from portfolio.manager.strategies import VolatilitySkewPortfolioStrategy
    except Exception as e:
        print(f"⚠️ VolatilitySkewPortfolioStrategy 로드 실패: {e}")
        return

    try:
        print("\n📊 변동성 스큐 포트폴리오 생성 시작...")

        api_key = ALPHA_VANTAGE_API_KEY if ALPHA_VANTAGE_API_KEY != "YOUR_ALPHA_VANTAGE_KEY" else None

        strategy = VolatilitySkewPortfolioStrategy(alpha_vantage_key=api_key)
        signals, filepath = strategy.run_screening_and_portfolio_creation()

        if signals:
            print(f"✅ 변동성 스큐 포트폴리오 신호 생성: {len(signals)}개")
            print(f"📁 결과 파일: {filepath}")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")

    except Exception as e:
        print(f"❌ 변동성 스큐 포트폴리오 생성 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_setup_screener():
    """US Setup Screener 실행"""
    try:
        print("\n📊 US Setup Screener 시작...")
        df = screen_us_setup()
        if not df.empty:
            print(f"✅ US Setup 결과 저장 완료: {len(df)}개 종목")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:
        print(f"❌ US Setup Screener 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_gainers_screener():
    """US Gainers Screener 실행"""
    try:
        print("\n📊 US Gainers Screener 시작...")
        df = screen_us_gainers()
        if not df.empty:
            print(f"✅ US Gainers 결과 저장 완료: {len(df)}개 종목")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:
        print(f"❌ US Gainers Screener 실행 중 오류 발생: {e}")
        print(traceback.format_exc())



def load_strategy_module(strategy_name):
    """전략 모듈을 동적으로 로드합니다."""
    try:
        strategy_path = os.path.join('portfolio', 'long_short', f'{strategy_name}.py')
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

def run_after_market_close():
    """장 마감 후 포트폴리오 업데이트 실행"""
    try:
        print(f"\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 자동 포트폴리오 업데이트 시작")
        
        # 포트폴리오만 실행
        create_portfolio_manager()
        
        print(f"✅ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 자동 포트폴리오 업데이트 완료")
        
    except Exception as e:
        print(f"❌ 자동 포트폴리오 업데이트 실패: {e}")

def setup_scheduler():
    """스케줄러 설정 - 매일 오후 4시 30분에 실행"""
    if schedule is None:
        raise ImportError("schedule 패키지가 설치되어 있지 않습니다.")
    schedule.every().day.at("16:30").do(run_after_market_close)
    print("📅 스케줄러 설정 완료: 매일 오후 4시 30분에 포트폴리오 업데이트 실행")

def run_scheduler():
    """스케줄러 실행"""
    if schedule is None:
        raise ImportError("schedule 패키지가 설치되어 있지 않습니다.")
    setup_scheduler()
    print("🔄 스케줄러 시작... (Ctrl+C로 종료)")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 1분마다 확인
    except KeyboardInterrupt:
        print("\n⏹️ 스케줄러 종료")


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='투자 스크리너 및 포트폴리오 관리 시스템')
    parser.add_argument('--skip-data', action='store_true', help='데이터 수집 건너뛰기')
    parser.add_argument('--force-screening', action='store_true', help='강제 스크리닝 모드')
    parser.add_argument('--strategies', action='store_true', help='6개 전략 스크리닝만 실행')
    parser.add_argument('--volatility-skew', action='store_true', help='변동성 스큐 역전 전략만 실행')
    parser.add_argument('--qullamaggie', action='store_true', help='쿨라매기 전략 실행')
    parser.add_argument('--qullamaggie-breakout', action='store_true', help='쿨라매기 브레이크아웃 셋업만 실행')
    parser.add_argument('--qullamaggie-episode-pivot', action='store_true', help='쿨라매기 에피소드 피봇 셋업만 실행')
    parser.add_argument('--qullamaggie-parabolic-short', action='store_true', help='쿨라매기 파라볼릭 숏 셋업만 실행')
    parser.add_argument('--setup', action='store_true', help='US Setup 스크리너만 실행')
    parser.add_argument('--gainers', action='store_true', help='US Gainers 스크리너만 실행')
    parser.add_argument('--portfolio-only', action='store_true', help='포트폴리오 관리만 실행')
    parser.add_argument('--schedule', action='store_true', help='스케줄링 모드로 실행 (매일 오후 4시 30분)')
    
    args = parser.parse_args()
    
    try:
        print(f"🚀 투자 스크리너 및 포트폴리오 관리 시스템 시작")
        print(f"⏰ 시작 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 필요한 디렉토리 생성
        print(f"\n📁 디렉토리 생성 중...")
        ensure_directories()
        print(f"✅ 디렉토리 생성 완료")
        
        # 스케줄러 모드
        if args.schedule:
            print(f"\n🕐 스케줄러 모드 시작")
            setup_scheduler()
            run_scheduler()
            return
        
        # 변동성 스큐 역전 전략만 실행
        if args.volatility_skew:
            print(f"\n🎯 변동성 스큐 역전 전략 전용 모드")
            run_volatility_skew_portfolio()
            return

        if args.setup:
            print(f"\n🎯 US Setup 스크리너 전용 모드")
            run_setup_screener()
            return

        if args.gainers:
            print(f"\n🎯 US Gainers 스크리너 전용 모드")
            run_gainers_screener()
            return
        
        # 쿨라매기 전략 실행
        if args.qullamaggie or args.qullamaggie_breakout or args.qullamaggie_episode_pivot or args.qullamaggie_parabolic_short:
            print(f"\n🎯 쿨라매기 전략 실행 모드")
            try:
                from qullamaggie import run_qullamaggie_strategy
                
                # 실행할 셋업 결정
                setups = []
                if args.qullamaggie:  # 모든 셋업 실행
                    setups = ['breakout', 'episode_pivot', 'parabolic_short']
                else:
                    if args.qullamaggie_breakout:
                        setups.append('breakout')
                    if args.qullamaggie_episode_pivot:
                        setups.append('episode_pivot')
                    if args.qullamaggie_parabolic_short:
                        setups.append('parabolic_short')
                
                # 쿨라매기 전략 실행
                run_qullamaggie_strategy(setups)
                print(f"✅ 쿨라매기 전략 실행 완료: {', '.join(setups)}")
            except Exception as e:
                print(f"❌ 쿨라매기 전략 실행 중 오류: {str(e)}")
                traceback.print_exc()
            return
        
        # 6개 전략 스크리닝만 실행
        if args.strategies:
            print(f"\n🎯 6개 전략 스크리닝 전용 모드")
            execute_strategies()
            return
        
        # 포트폴리오 관리만 실행
        if args.schedule:
            print("📅 스케줄링 모드로 실행합니다.")
            run_scheduler()
        elif args.portfolio_only:
            print("🎯 포트폴리오 관리만 실행합니다.")
            os.environ["USE_LOCAL_DATA_ONLY"] = "1"
            create_portfolio_manager()
        else:
    # 기존 전체 실행 로직        
        
        # 전체 프로세스 실행
            print(f"\n🎯 전체 프로세스 실행 모드")
        
        if not args.skip_data:
            print(f"\n📊 1단계: 데이터 수집")
            collect_data_main()
        else:
            print(f"\n⏭️ 데이터 수집 건너뛰기")
        
        # 강제 스크리닝 또는 전략 파일 상태 확인
        if args.force_screening:
            print("\n🔄 2단계: 강제 스크리닝 모드...")
            print("  📊 2-1: 모든 스크리닝 프로세스 실행")
            run_all_screening_processes()
            print("  📊 2-2: 패턴 분석 실행")
            run_pattern_analysis()
            print("  📊 2-3: 전략 실행")
            execute_strategies()
            print("  📊 2-4: 변동성 스큐 스크리닝 실행")
            run_volatility_skew_portfolio()
        else:
            print("\n🔍 2단계: 전략 파일 상태 확인 및 조건부 스크리닝")
            # 전략 파일 상태 확인 및 필요시 스크리닝
            strategies_need_screening = check_strategy_file_status()
    
            if strategies_need_screening:
                print(f"\n🚨 스크리닝이 필요한 전략: {', '.join(strategies_need_screening)}")
                print("  📊 2-1: 모든 스크리닝 프로세스 실행")
                run_all_screening_processes()
                print("  📊 2-2: 필요한 전략들 실행")
                execute_strategies(strategies_need_screening)
            else:
                print("\n📊 2단계: 패턴 분석만 실행...")
                run_pattern_analysis()

        # 포트폴리오 관리 실행
        print("\n🏦 3단계: 포트폴리오 관리 실행")
        create_portfolio_manager()
       
        print("\n🎉 모든 프로세스 완료!")
        print(f"⏰ 완료 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        print(f"⏰ 중단 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"\n❌ 시스템 오류 발생: {e}")
        print(f"⏰ 오류 발생 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
