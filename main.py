#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 투자 스크리너 - 메인 실행 파일

import os
import sys
import argparse
import traceback
import pandas as pd


from utils.path_utils import add_project_root
from utils.file_cleanup import cleanup_old_timestamped_files

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

from orchestrator.tasks import (
    ensure_directories,
    collect_data_main,
    run_all_screening_processes,
    run_leader_stock_screener,
    run_momentum_signals_screener,
    run_qullamaggie_strategy_task,
    setup_scheduler,
    run_scheduler,
)

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='투자 스크리너 및 포트폴리오 관리 시스템')
    parser.add_argument('--skip-data', action='store_true', help='데이터 수집 건너뛰기')
    parser.add_argument('--force-screening', action='store_true', help='강제 스크리닝 실행')
    parser.add_argument('--no-symbol-update', action='store_true', help='종목 리스트 업데이트 건너뛰기')
    parser.add_argument('--task', default='all',
                        choices=['all', 'screening', 'leader-stock',
                                 'momentum', 'qullamaggie'],
                        help='실행할 작업 선택')
    parser.add_argument('--schedule', action='store_true', help='스케줄러 모드 실행')
    
    args = parser.parse_args()

    try:
        print("🚀 투자 스크리너 및 포트폴리오 관리 시스템 시작")
        print(f"⏰ 시작 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print("\n📁 디렉토리 생성 중...")
        ensure_directories()
        print("✅ 디렉토리 생성 완료")
        
        # 한 달 이상 된 타임스탬프 파일 자동 정리
        print("\n🧹 오래된 타임스탬프 파일 정리 중...")
        from config import RESULTS_DIR
        cleanup_result = cleanup_old_timestamped_files(
            directory=RESULTS_DIR,
            days_threshold=30,
            extensions=['.csv', '.json'],
            dry_run=False
        )
        if cleanup_result['deleted_count'] > 0:
            print(f"✅ {cleanup_result['deleted_count']}개 오래된 파일 정리 완료")
        else:
            print("📂 정리할 오래된 파일 없음")

        if args.schedule:
            print("\n🕐 스케줄러 모드 시작")
            setup_scheduler()
            run_scheduler()
            return

        task = args.task
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
        if task == 'qullamaggie':
            run_qullamaggie_strategy_task(skip_data=args.skip_data)
            return

        # task == 'all'
        print("\n🎯 전체 프로세스 실행 모드")

        print("\n📊 1단계: 데이터 수집")
        # 종목 리스트 업데이트 여부 결정
        update_symbols = not args.no_symbol_update
        if args.skip_data:
            print("⏭️ OHLCV 데이터 업데이트 건너뛰기 - 기타 데이터 수집은 진행")
            collect_data_main(update_symbols=False, skip_ohlcv=True)
        else:
            if args.no_symbol_update:
                print("📊 종목 리스트 업데이트 건너뛰기 - 기존 종목만 사용")
            else:
                print("🔄 종목 리스트 자동 업데이트 활성화")
            collect_data_main(update_symbols=update_symbols, skip_ohlcv=False)

        print("\n🔄 2단계: 스크리닝 실행 중...")
        # Orchestrator의 run_all_screening_processes가 모든 전략(미너비니, 주도주, 모멘텀, 쿨라매기)을 실행하고
        # 결과 저장 및 패턴 분석까지 통합 수행합니다.
        run_all_screening_processes(skip_data=args.skip_data)

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
