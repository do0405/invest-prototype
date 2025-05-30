#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
통합 포트폴리오 관리 시스템 실행 스크립트

모든 전략(strategy1~6)을 실행하고 포트폴리오를 통합 관리합니다.
포지션 추적, 손절매, 수익보호, 차익실현 등을 자동으로 처리합니다.
"""

import os
import sys
import time
from datetime import datetime, timedelta
import traceback

# schedule 모듈이 없는 경우를 대비한 처리
try:
    import schedule
    SCHEDULE_AVAILABLE = True
except ImportError:
    SCHEDULE_AVAILABLE = False
    print("⚠️ schedule 모듈이 설치되지 않았습니다. 스케줄링 기능을 사용하려면 'pip install schedule'을 실행하세요.")

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from portfolio_integration import StrategyPortfolioIntegrator
from utils import ensure_dir
from config import RESULTS_DIR  # 통합된 결과 디렉토리 사용


def run_once():
    """한 번만 실행"""
    print("\n" + "="*60)
    print("🚀 통합 포트폴리오 관리 시스템 - 단일 실행")
    print("="*60)
    
    try:
        # 통합 시스템 초기화
        integrator = StrategyPortfolioIntegrator(initial_capital=100000)
        
        # 일일 사이클 실행
        integrator.run_daily_cycle()
        
        print("\n✅ 실행 완료!")
        
    except Exception as e:
        print(f"❌ 실행 오류: {e}")
        print(traceback.format_exc())


def run_scheduled():
    """스케줄링된 실행"""
    print("\n" + "="*60)
    print("⏰ 통합 포트폴리오 관리 시스템 - 스케줄 실행")
    print(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    try:
        # 통합 시스템 초기화
        integrator = StrategyPortfolioIntegrator(initial_capital=100000)
        
        # 일일 사이클 실행
        integrator.run_daily_cycle()
        
        print(f"\n✅ 스케줄 실행 완료! 다음 실행: {schedule.next_run()}")
        
    except Exception as e:
        print(f"❌ 스케줄 실행 오류: {e}")
        print(traceback.format_exc())


def run_continuous():
    """연속 실행 (스케줄링)"""
    print("\n" + "="*60)
    print("🔄 통합 포트폴리오 관리 시스템 - 연속 실행 모드")
    print("="*60)
    
    if not SCHEDULE_AVAILABLE:
        print("❌ schedule 모듈이 설치되지 않아 스케줄링 기능을 사용할 수 없습니다.")
        print("💡 설치 방법: pip install schedule")
        return
    
    # 스케줄 설정
    # 평일 오전 9시 30분 (시장 개장 시간)
    schedule.every().monday.at("09:30").do(run_scheduled)
    schedule.every().tuesday.at("09:30").do(run_scheduled)
    schedule.every().wednesday.at("09:30").do(run_scheduled)
    schedule.every().thursday.at("09:30").do(run_scheduled)
    schedule.every().friday.at("09:30").do(run_scheduled)
    
    # 평일 오후 4시 (시장 마감 후)
    schedule.every().monday.at("16:00").do(run_scheduled)
    schedule.every().tuesday.at("16:00").do(run_scheduled)
    schedule.every().wednesday.at("16:00").do(run_scheduled)
    schedule.every().thursday.at("16:00").do(run_scheduled)
    schedule.every().friday.at("16:00").do(run_scheduled)
    
    print("📅 스케줄 설정 완료:")
    print("   - 평일 오전 9:30 (시장 개장)")
    print("   - 평일 오후 4:00 (시장 마감 후)")
    print(f"\n⏰ 다음 실행 예정: {schedule.next_run()}")
    print("\n🛑 중지하려면 Ctrl+C를 누르세요.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 1분마다 체크
    except KeyboardInterrupt:
        print("\n🛑 사용자에 의해 중지되었습니다.")
    except Exception as e:
        print(f"❌ 연속 실행 오류: {e}")
        print(traceback.format_exc())


def show_status():
    """현재 상태 확인"""
    print("\n" + "="*60)
    print("📊 통합 포트폴리오 관리 시스템 - 상태 확인")
    print("="*60)
    
    try:
        # 결과 파일들 확인
        strategies = ['strategy1', 'strategy2', 'strategy3', 'strategy4', 'strategy5', 'strategy6']
        
        print("\n📈 전략별 결과 파일 상태:")
        for strategy in strategies:
            result_file = os.path.join(RESULTS_DIR, f'{strategy}_results.csv')
            # portfolio_file 관련 로직은 중앙화된 포트폴리오 관리로 인해 제거
            
            result_exists = "✅" if os.path.exists(result_file) else "❌"
            
            print(f"   {strategy}: 결과파일 {result_exists}")
        
        # 일일 리포트 확인
        print("\n📋 최근 일일 리포트:")
        report_files = [f for f in os.listdir(RESULTS_DIR) if f.startswith('daily_report_') and f.endswith('.txt')] # RESULTS_DIR로 변경
        if report_files:
            report_files.sort(reverse=True)
            for report_file in report_files[:5]:  # 최근 5개만 표시
                report_date = report_file.replace('daily_report_', '').replace('.txt', '')
                print(f"   📄 {report_date}")
        else:
            print("   ⚠️ 일일 리포트가 없습니다.")
        
    except Exception as e:
        print(f"❌ 상태 확인 오류: {e}")
        print(traceback.format_exc())


def main():
    """메인 함수"""
    print("🎯 통합 포트폴리오 관리 시스템")
    print("\n실행 모드를 선택하세요:")
    print("1. 한 번만 실행")
    print("2. 연속 실행 (스케줄링)")
    print("3. 상태 확인")
    print("4. 종료")
    
    while True:
        try:
            choice = input("\n선택 (1-4): ").strip()
            
            if choice == '1':
                run_once()
                break
            elif choice == '2':
                run_continuous()
                break
            elif choice == '3':
                show_status()
                continue
            elif choice == '4':
                print("👋 프로그램을 종료합니다.")
                break
            else:
                print("❌ 잘못된 선택입니다. 1-4 중에서 선택해주세요.")
                
        except KeyboardInterrupt:
            print("\n👋 프로그램을 종료합니다.")
            break
        except Exception as e:
            print(f"❌ 입력 오류: {e}")


if __name__ == "__main__":
    # 필요한 디렉토리 생성
    ensure_dir(RESULTS_DIR) # 통합된 results 디렉토리만 생성
    # ensure_dir(os.path.join(RESULTS_VER2_DIR, 'buy')) # 개별 buy 디렉토리 생성 제거
    # ensure_dir(os.path.join(RESULTS_VER2_DIR, 'sell')) # 개별 sell 디렉토리 생성 제거
    
    main()