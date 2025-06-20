#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 투자 스크리너 - 메인 실행 파일

import os
import sys
import argparse
import traceback
import pandas as pd
import time
from datetime import datetime

from portfolio.manager import create_portfolio_manager

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'portfolio', 'long_short'))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'portfolio', 'manager'))

from orchestrator.tasks import (
    execute_strategies,
    check_strategy_file_status,
    ensure_directories,
    run_pattern_analysis,
    collect_data_main,
    run_all_screening_processes,
    run_volatility_skew_portfolio,
    run_setup_screener,
    run_gainers_screener,
    run_leader_stock_screener,
    run_momentum_signals_screener,
    run_ipo_investment_screener,
    run_market_regime_analysis,
    setup_scheduler,
    run_scheduler,
)

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
    parser.add_argument('--leader-stock', action='store_true', help='주도주 투자 전략 스크리너만 실행')
    parser.add_argument('--momentum-signals', action='store_true', help='상승 모멘텀 신호 스크리너만 실행')
    parser.add_argument('--ipo-investment', action='store_true', help='IPO 투자 전략 스크리너만 실행')
    parser.add_argument('--portfolio-only', action='store_true', help='포트폴리오 관리만 실행')
    parser.add_argument('--schedule', action='store_true', help='스케줄링 모드로 실행 (매일 오후 4시 30분)')
    parser.add_argument('--market-regime', action='store_true', help='시장 국면 분석만 실행')
    
    args = parser.parse_args()
    
    try:
        print(f"🚀 투자 스크리너 및 포트폴리오 관리 시스템 시작")
        print(f"⏰ 시작 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 필요한 디렉토리 생성
        print(f"\n📁 디렉토리 생성 중...")
        ensure_directories()
        print(f"✅ 디렉토리 생성 완료")
        
        # 시장 국면 분석은 전용 모드에서만 실행
        # (--market-regime 옵션 사용 시에만 실행됨)
        
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
            
        if args.leader_stock:
            print(f"\n🎯 주도주 투자 전략 스크리너 전용 모드")
            run_leader_stock_screener()
            return
            
        if args.momentum_signals:
            print(f"\n🎯 상승 모멘텀 신호 스크리너 전용 모드")
            run_momentum_signals_screener()
            return
            
        if args.ipo_investment:
            print(f"\n🎯 IPO 투자 전략 스크리너 전용 모드")
            run_ipo_investment_screener()
            return
            
        if args.market_regime:
            print(f"\n🎯 시장 국면 분석 전용 모드")
            run_market_regime_analysis()
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
