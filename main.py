#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 투자 스크리너 - 메인 실행 파일

import os
import sys
import argparse
import traceback
import pandas as pd


from portfolio.manager import create_portfolio_manager

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'portfolio', 'long_short'))
sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), 'portfolio', 'manager'))

from orchestrator.tasks import (
    execute_strategies,
    ensure_directories,
    collect_data_main,
    run_all_screening_processes,
    run_volatility_skew_portfolio,
    run_setup_screener,
    run_gainers_screener,
    run_leader_stock_screener,
    run_momentum_signals_screener,
    run_ipo_investment_screener,
    run_qullamaggie_strategy_task,
    run_market_regime_analysis,
    run_image_pattern_detection_task,
    run_ranking_system_task,
    setup_scheduler,
    run_scheduler,
)

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='투자 스크리너 및 포트폴리오 관리 시스템')
    parser.add_argument('--skip-data', action='store_true', help='데이터 수집 건너뛰기')
    parser.add_argument('--force-screening', action='store_true', help='강제 스크리닝 실행')
    parser.add_argument('--task', default='all',
                        choices=['all', 'screening', 'volatility-skew', 'setup', 'gainers', 'leader-stock',
                                 'momentum', 'ipo', 'qullamaggie', 'portfolio', 'market-regime', 'image-pattern', 'ranking'],
                        help='실행할 작업 선택')
    parser.add_argument('--schedule', action='store_true', help='스케줄러 모드 실행')
    
    args = parser.parse_args()

    try:
        print("🚀 투자 스크리너 및 포트폴리오 관리 시스템 시작")
        print(f"⏰ 시작 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print("\n📁 디렉토리 생성 중...")
        ensure_directories()
        print("✅ 디렉토리 생성 완료")

        if args.schedule:
            print("\n🕐 스케줄러 모드 시작")
            setup_scheduler()
            run_scheduler()
            return

        task = args.task

        if task == 'volatility-skew':
            print("\n🎯 변동성 스큐 전략 모드")
            run_volatility_skew_portfolio()
            return
        if task == 'setup':
            print("\n🎯 US Setup 스크리너 모드")
            run_setup_screener()
            return
        if task == 'gainers':
            print("\n🎯 US Gainers 스크리너 모드")
            run_gainers_screener()
            return
        if task == 'leader-stock':
            print("\n🎯 주도주 전략 모드")
            run_leader_stock_screener(skip_data=args.skip_data)
            return
        if task == 'screening':
            print("\n🎯 스크리닝 전용 모드")
            run_all_screening_processes(skip_data=args.skip_data)
            return
        if task == 'momentum':
            print("\n🎯 상승 모멘텀 신호 모드")
            run_momentum_signals_screener(skip_data=args.skip_data)
            return
        if task == 'ipo':
            print("\n🎯 IPO 투자 전략 모드")
            run_ipo_investment_screener(skip_data=args.skip_data)
            return
        if task == 'qullamaggie':
            run_qullamaggie_strategy_task(skip_data=args.skip_data)
            return
        if task == 'market-regime':
            run_market_regime_analysis(skip_data=args.skip_data)
            return
        if task == 'image-pattern':
            print("\n🎯 이미지 패턴 감지 모드")
            run_image_pattern_detection_task(skip_data=args.skip_data)
            return
        if task == 'ranking':
            print("\n🎯 MCDA 기반 종목 랭킹 모드")
            run_ranking_system_task(skip_data=args.skip_data)
            return
        if task == 'portfolio':
            create_portfolio_manager()
            return

        # task == 'all'
        print("\n🎯 전체 프로세스 실행 모드")

        if not args.skip_data:
            print("\n📊 1단계: 데이터 수집")
            collect_data_main()
        else:
            print("\n⏭️ 데이터 수집 건너뛰기")

        print("\n🔄 2단계: 스크리닝 실행 중...")
        run_all_screening_processes(skip_data=args.skip_data)
        execute_strategies()

        print("\n📊 3단계: MCDA 기반 종목 랭킹 실행")
        run_ranking_system_task(skip_data=args.skip_data)

        print("\n🏦 4단계: 포트폴리오 관리 실행")
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
