"""Utility tasks for running screeners and portfolio management."""

from __future__ import annotations

import os
import sys
import time
import traceback
import pandas as pd
import importlib.util
from datetime import datetime
from typing import List, Optional

from data_collector import collect_data
from utils import ensure_dir, create_required_dirs
from data_collectors.stock_metadata_collector import main as collect_stock_metadata_main
from screeners.markminervini.integrated_screener import IntegratedScreener
from screeners.markminervini.advanced_financial import run_advanced_financial_screening
from screeners.markminervini.screener import run_us_screening
from screeners.leader_stock.screener import run_leader_stock_screening
from screeners.momentum_signals.screener import run_momentum_signals_screening
from screeners.markminervini.ticker_tracker import track_new_tickers

from utils.first_buy_tracker import update_first_buy_signals
from config import (
    DATA_US_DIR,
    RESULTS_DIR,
    SCREENER_RESULTS_DIR,
    OPTION_RESULTS_DIR,
    ADVANCED_FINANCIAL_RESULTS_PATH,
    MARKMINERVINI_RESULTS_DIR,
    LEADER_STOCK_RESULTS_DIR,
    MOMENTUM_SIGNALS_RESULTS_DIR,
    RANKING_RESULTS_DIR,
)

__all__ = [
    "ensure_directories",
    "collect_data_main",
    "run_all_screening_processes",
    "run_leader_stock_screener",
    "run_momentum_signals_screener",
    "run_stock_metadata_collection",
    "run_qullamaggie_strategy_task",
    "run_keep_alive",
    "setup_scheduler",
    "run_scheduler",
]


def run_stock_metadata_collection() -> None:
    """Run stock metadata collection task."""
    print("\n📋 주식 메타데이터 수집 시작...")
    try:
        collect_stock_metadata_main()
        print("✅ 주식 메타데이터 수집 완료")
    except Exception as e:
        print(f"❌ 주식 메타데이터 수집 중 오류: {e}")
        # 메타데이터 수집 실패는 치명적이지 않으므로 로그만 남기고 계속 진행할 수도 있음
        # 하지만 여기서는 예외를 던지지 않고 넘어감


def ensure_directories() -> None:
    """Create required directories for the application."""
    # 기본 디렉터리 생성
    create_required_dirs()

    # 추가 디렉터리 목록
    additional = [
        SCREENER_RESULTS_DIR,
        OPTION_RESULTS_DIR,
        os.path.join(RESULTS_DIR, "leader_stock"),
        os.path.join(RESULTS_DIR, "momentum_signals"),
    ]

    for directory in additional:
        ensure_dir(directory)


def collect_data_main(update_symbols: bool = True, skip_ohlcv: bool = False) -> None:
    """Wrapper around the data collector.
    
    Args:
        update_symbols: 종목 리스트 업데이트 여부 (기본값: True)
        skip_ohlcv: OHLCV 데이터 수집 건너뛰기 여부 (기본값: False)
    """
    print("\n💾 데이터 수집 시작...")
    try:
        # 1. 주식 메타데이터 업데이트 (OHLCV 수집 전에 실행)
        print("\n📋 1단계: 주식 메타데이터 업데이트")
        run_stock_metadata_collection()
        
        # 2. 기본 주가 데이터 수집 (종목 리스트 업데이트 포함)
        if not skip_ohlcv:
            print("\n📈 2단계: 주가 데이터 수집")
            if update_symbols:
                print("🔄 종목 리스트 자동 업데이트 활성화")
            else:
                print("📊 기존 종목 리스트 사용")
            collect_data(update_symbols=update_symbols)
        else:
            print("\n⏭️ 2단계: OHLCV 데이터 수집 건너뛰기")
        
        # 3. 시장 폭 데이터 수집 (제거됨)
        # print("\n📊 3단계: 시장 폭 데이터 수집")
        # run_market_breadth_collection()
        
        print("✅ 모든 데이터 수집 완료")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 데이터 수집 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_all_screening_processes(skip_data: bool = False) -> None:
    """Execute all screening steps sequentially.

    Parameters
    ----------
    skip_data : bool, optional
        Currently unused flag that can be leveraged by scheduled keep alive
        tasks to indicate that heavy data collection should be skipped, by
        default False.
    """
    print("\n⚙️ 스크리닝 프로세스 시작...")
    try:
        if skip_data:
            print("\n⏭ 데이터 수집 단계 건너뛰기, 스크리닝만 실행")
        
        print("\n⏳ 1단계: 미국 주식 스크리닝 실행 중...")
        run_us_screening()
        print("✅ 1단계: 미국 주식 스크리닝 완료.")

        print("\n⏳ 2단계: 고급 재무 스크리닝 실행 중...")
        run_advanced_financial_screening(skip_data=skip_data)
        print("✅ 2단계: 고급 재무 스크리닝 완료.")

        print("\n⏳ 3단계: 통합 스크리닝 및 패턴 분석 실행 중...")
        screener = IntegratedScreener()
        screener.run_integrated_screening()
        print("✅ 3단계: 통합 스크리닝 및 패턴 분석 완료.")

        print("\n⏳ 4단계: 새로운 티커 추적 실행 중...")
        track_new_tickers(ADVANCED_FINANCIAL_RESULTS_PATH)
        print("✅ 4단계: 새로운 티커 추적 완료.")

        # 5. 패턴 분석 (IntegratedScreener에 통합됨)
        print("\n✅ 5단계: 완료 (패턴 분석은 마크 미너비니 통합 스크리너에서 수행됨)")

        print("\n⏳ 6단계: 주도주 투자 전략 스크리닝 실행 중...")
        run_leader_stock_screener(skip_data=skip_data)
        print("✅ 6단계: 주도주 투자 전략 스크리닝 완료.")

        print("\n⏳ 7단계: 상승 모멘텀 신호 스크리닝 실행 중...")
        run_momentum_signals_screener(skip_data=skip_data)
        print("✅ 7단계: 상승 모멘텀 신호 스크리닝 완료.")

        print("\n⏳ 8단계: 쿨라매기 전략 실행 중...")
        run_qullamaggie_strategy_task(skip_data=skip_data)
        print("✅ 8단계: 쿨라매기 전략 완료.")

        print("\n✅ 모든 스크리닝 프로세스 완료.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 스크리닝 프로세스 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_leader_stock_screener(skip_data=False):
    """Run the leader stock screener."""
    try:
        print("\n📊 주도주 투자 전략 스크리너 시작...")
        df = run_leader_stock_screening(skip_data=skip_data)
        if not df.empty:
            print(f"✅ 주도주 투자 전략 결과 저장 완료: {len(df)}개 종목")
            update_first_buy_signals(df, LEADER_STOCK_RESULTS_DIR)
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 주도주 투자 전략 스크리너 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_momentum_signals_screener(skip_data=False) -> None:
    """Run the Stan Weinstein Stage 2 breakout screener."""
    try:
        print("\n📊 Stan Weinstein Stage 2 Breakout 스크리너 시작...")
        df = run_momentum_signals_screening(skip_data=skip_data)
        if not df.empty:
            print(f"✅ Stage 2 Breakout 결과 저장 완료: {len(df)}개 종목")
            update_first_buy_signals(df, MOMENTUM_SIGNALS_RESULTS_DIR)
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ Stage 2 Breakout 스크리너 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


# run_market_breadth_collection 함수 제거됨
    """Collect and save stock metadata using StockMetadataUpdater."""
    try:
        print("\n📊 주식 메타데이터 수집 시작...")
        collect_stock_metadata_main()
        print("✅ 주식 메타데이터 업데이트 완료")
            
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 주식 메타데이터 수집 실패: {e}")
        print(traceback.format_exc())


def run_qullamaggie_strategy_task(setups: Optional[list[str]] | None = None, skip_data: bool = False) -> None:
    """Run the Qullamaggie trading strategy."""
    try:
        from screeners.qullamaggie import run_qullamaggie_strategy
    except Exception as e:  # pragma: no cover - optional dependency
        print(f"⚠️ 쿨라매기 모듈 로드 실패: {e}")
        return

    try:
        print("\n📊 쿨라매기 전략 시작...")
        run_qullamaggie_strategy(setups, skip_data=skip_data)
        print("✅ 쿨라매기 전략 완료")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 쿨라매기 전략 실행 중 오류 발생: {e}")
        print(traceback.format_exc())




def run_keep_alive() -> None:
    """Run a lightweight screening cycle used for keep-alive schedules."""
    run_all_screening_processes(skip_data=True)


def _convert_kst_to_local(time_str: str) -> str:
    """Convert HH:MM KST time string to local server time string."""
    try:
        import pytz
    except Exception:
        return time_str
    kst = pytz.timezone("Asia/Seoul")
    local_tz = datetime.now().astimezone().tzinfo
    dt = datetime.strptime(time_str, "%H:%M")
    kst_dt = kst.localize(dt)
    local_dt = kst_dt.astimezone(local_tz)
    return local_dt.strftime("%H:%M")


_SCHED_CONF = {"full_time": "14:30", "interval": 1}


def setup_scheduler(full_run_time: str = "14:30", keep_alive_interval: int = 1) -> None:
    """Configure parameters for the keep-alive scheduler."""
    _SCHED_CONF["full_time"] = full_run_time
    _SCHED_CONF["interval"] = keep_alive_interval
    print(
        f"📅 스케줄러 설정: 매일 {full_run_time} KST 이후 첫 실행 시 전체 모드,"
        f" {keep_alive_interval}분 간격 keep-alive"
    )


def run_scheduler() -> None:
    """Run the keep-alive loop with a daily full run after the set time."""
    try:
        import pytz
    except Exception:  # pragma: no cover - timezone optional
        pytz = None

    full_time = datetime.strptime(_SCHED_CONF["full_time"], "%H:%M").time()
    interval = _SCHED_CONF["interval"]
    kst_tz = pytz.timezone("Asia/Seoul") if pytz else None
    last_full_date = None

    print("🔄 스케줄러 시작... (Ctrl+C로 종료)")
    try:
        while True:
            run_keep_alive()
            end = datetime.now(kst_tz)
            if end.time() >= full_time and (last_full_date != end.date()):
                time.sleep(interval * 60)
                os.system(f"{sys.executable} main.py")
                last_full_date = (datetime.now(kst_tz)).date() if kst_tz else datetime.now().date()
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        print("\n⏹️ 스케줄러 종료")

        # run_image_pattern_detection_task 함수 제거됨 - 3단계 통합 스크리닝에서 패턴 감지 수행
