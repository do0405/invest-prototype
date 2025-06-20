"""Utility tasks for running screeners and portfolio management."""

from __future__ import annotations

import os
import time
import traceback
import pandas as pd
import importlib.util
from datetime import datetime
from typing import List, Optional

try:
    import schedule
except ImportError:  # pragma: no cover - optional dependency
    schedule = None

from portfolio.manager import create_portfolio_manager

from data_collector import collect_data
from utils import ensure_dir, create_required_dirs
from data_collectors.market_breadth_collector import MarketBreadthCollector
from utils.market_regime_indicator import analyze_market_regime
from screeners.markminervini.filter_stock import run_integrated_screening
from screeners.markminervini.advanced_financial import run_advanced_financial_screening
from screeners.markminervini.pattern_detection import analyze_tickers_from_results
from screeners.us_setup.screener import screen_us_setup
from screeners.us_gainer.screener import screen_us_gainers
from screeners.leader_stock.screener import run_leader_stock_screening
from screeners.momentum_signals.screener import run_momentum_signals_screening
from screeners.ipo_investment.screener import run_ipo_investment_screening
from screeners.markminervini.ticker_tracker import track_new_tickers
from config import (
    DATA_US_DIR,
    RESULTS_DIR,
    SCREENER_RESULTS_DIR,
    PORTFOLIO_BUY_DIR,
    PORTFOLIO_SELL_DIR,
    OPTION_VOLATILITY_DIR,
    ADVANCED_FINANCIAL_RESULTS_PATH,
    ALPHA_VANTAGE_API_KEY,
    MARKET_REGIME_DIR,
)

# Portfolio manager utilities
try:
    from portfolio.manager.core.portfolio_manager import PortfolioManager
    from portfolio.manager.core.strategy_config import StrategyConfig
except Exception:
    PortfolioManager = None
    StrategyConfig = None

__all__ = [
    "execute_strategies",
    "check_strategy_file_status",
    "ensure_directories",
    "run_pattern_analysis",
    "collect_data_main",
    "run_all_screening_processes",
    "run_volatility_skew_portfolio",
    "run_setup_screener",
    "run_gainers_screener",
    "run_leader_stock_screener",
    "run_momentum_signals_screener",
    "run_ipo_investment_screener",
    "run_market_breadth_collection",
    "run_qullamaggie_strategy_task",
    "run_market_regime_analysis",
    "load_strategy_module",
    "run_after_market_close",
    "setup_scheduler",
    "run_scheduler",
]


def execute_strategies(strategy_list: Optional[List[str]] = None,
                       monitoring_only: bool = False,
                       screening_mode: bool = False) -> bool:
    """Run portfolio strategies dynamically loaded from modules."""
    if strategy_list is None:
        if StrategyConfig is not None:
            strategy_list = StrategyConfig.get_all_strategies()
        else:
            strategy_list = [f"strategy{i}" for i in range(1, 7)]

    try:
        action_type = "모니터링" if monitoring_only else "스크리닝" if screening_mode else "실행"
        print(f"\n📊 전략 {action_type} 시작: {strategy_list}")
        print(f"🔍 총 {len(strategy_list)}개 전략을 처리합니다.")

        strategy_modules = {}
        print("\n📦 전략 모듈 로딩 시작...")
        for i, strategy_name in enumerate(strategy_list, 1):
            print(f"  [{i}/{len(strategy_list)}] {strategy_name} 모듈 로딩 중...")
            module = load_strategy_module(strategy_name)
            if module:
                strategy_modules[strategy_name] = module
                print(f"  ✅ {strategy_name} 모듈 로딩 성공")
            else:
                print(f"  ❌ {strategy_name} 모듈 로딩 실패")

        print(f"\n📊 로딩된 모듈: {len(strategy_modules)}/{len(strategy_list)}개")

        success_count = 0
        for i, (strategy_name, module) in enumerate(strategy_modules.items(), 1):
            try:
                print(f"\n🔄 [{i}/{len(strategy_modules)}] {strategy_name} {action_type} 시작...")
                print(f"⏰ 현재 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

                if monitoring_only:
                    if hasattr(module, "monitor_positions"):
                        print(f"  📊 {strategy_name}: monitor_positions() 실행 중...")
                        module.monitor_positions()
                    elif hasattr(module, "update_positions"):
                        print(f"  📊 {strategy_name}: update_positions() 실행 중...")
                        module.update_positions()
                    elif hasattr(module, "track_existing_positions"):
                        print(f"  📊 {strategy_name}: track_existing_positions() 실행 중...")
                        module.track_existing_positions()
                    else:
                        print(f"⚠️ {strategy_name}: 모니터링 함수를 찾을 수 없습니다. 스킵합니다.")
                        continue
                else:
                    if hasattr(module, "run_strategy"):
                        print(f"  🚀 {strategy_name}: run_strategy() 실행 중...")
                        module.run_strategy()
                    elif hasattr(module, f"run_{strategy_name}_screening"):
                        print(f"  🚀 {strategy_name}: run_{strategy_name}_screening() 실행 중...")
                        getattr(module, f"run_{strategy_name}_screening")()
                    elif hasattr(module, "main"):
                        print(f"  🚀 {strategy_name}: main() 실행 중...")
                        module.main()
                    else:
                        print(f"⚠️ {strategy_name}: 실행 함수를 찾을 수 없습니다.")
                        continue

                print(f"✅ {strategy_name} {action_type} 완료")
                success_count += 1
                print(f"📈 진행률: {success_count}/{len(strategy_modules)} ({success_count/len(strategy_modules)*100:.1f}%)")
            except Exception as e:  # pragma: no cover - runtime log
                print(f"❌ {strategy_name} {action_type} 중 오류: {e}")
                print(f"🔍 오류 발생 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
                if "name 'os' is not defined" not in str(e):
                    print(traceback.format_exc())

        print(f"\n✅ 전략 {action_type} 완료: {success_count}/{len(strategy_list)}개 성공")
        print(f"📊 성공률: {success_count/len(strategy_list)*100:.1f}%")
        return success_count > 0
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 전략 {action_type} 중 오류 발생: {e}")
        print(f"🔍 오류 발생 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(traceback.format_exc())
        return False


def check_strategy_file_status() -> List[str]:
    """Return strategies requiring screening based on existing result files."""
    strategy_files = {
        "strategy1": os.path.join(PORTFOLIO_BUY_DIR, "strategy1_results.csv"),
        "strategy2": os.path.join(PORTFOLIO_SELL_DIR, "strategy2_results.csv"),
        "strategy3": os.path.join(PORTFOLIO_BUY_DIR, "strategy3_results.csv"),
        "strategy4": os.path.join(PORTFOLIO_BUY_DIR, "strategy4_results.csv"),
        "strategy5": os.path.join(PORTFOLIO_BUY_DIR, "strategy5_results.csv"),
        "strategy6": os.path.join(PORTFOLIO_SELL_DIR, "strategy6_results.csv"),
        "volatility_skew": os.path.join(PORTFOLIO_BUY_DIR, "volatility_skew_results.csv"),
    }
    strategies_need_screening: List[str] = []

    print("\n🔍 전략 결과 파일 상태 확인 중...")
    for strategy_name, file_path in strategy_files.items():
        if not os.path.exists(file_path):
            strategies_need_screening.append(strategy_name)
            print(f"❌ {strategy_name}: 파일 없음")
        else:
            try:
                df = pd.read_csv(file_path)
                
                # 컬럼명을 소문자로 변환 (결과 파일이므로 선택적)
                if 'Close' in df.columns or 'Volume' in df.columns:
                    df.columns = [c.lower() for c in df.columns]
                
                if len(df) < 10:
                    strategies_need_screening.append(strategy_name)
                    print(f"⚠️ {strategy_name}: 종목 수 부족 ({len(df)}개)")
                else:
                    print(f"✅ {strategy_name}: 충분한 종목 수 ({len(df)}개)")
            except Exception:
                strategies_need_screening.append(strategy_name)
                print(f"❌ {strategy_name}: 파일 읽기 오류")
    return strategies_need_screening


def ensure_directories() -> None:
    """Create required directories for the application."""
    # 기본 디렉터리 생성
    create_required_dirs()

    # 추가 디렉터리 목록
    additional = [
        SCREENER_RESULTS_DIR,
        PORTFOLIO_BUY_DIR,
        PORTFOLIO_SELL_DIR,
        OPTION_VOLATILITY_DIR,
        MARKET_REGIME_DIR,
        os.path.join(RESULTS_DIR, "leader_stock"),
        os.path.join(RESULTS_DIR, "momentum_signals"),
        os.path.join(RESULTS_DIR, "ipo_investment"),
    ]

    for directory in additional:
        ensure_dir(directory)


def run_pattern_analysis() -> None:
    """Run pattern analysis on previously screened tickers."""
    try:
        print("\n📊 패턴 분석 시작...")
        output_dir = os.path.join(RESULTS_DIR, "results2")
        analyze_tickers_from_results(RESULTS_DIR, DATA_US_DIR, output_dir)
        print("✅ 패턴 분석 완료")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 패턴 분석 중 오류 발생: {e}")
        print(traceback.format_exc())


def collect_data_main() -> None:
    """Wrapper around the data collector."""
    print("\n💾 데이터 수집 시작...")
    try:
        collect_data()
        run_market_breadth_collection()
        print("✅ 데이터 수집 완료")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 데이터 수집 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_all_screening_processes() -> None:
    """Execute all screening steps sequentially."""
    print("\n⚙️ 스크리닝 프로세스 시작...")
    try:
        print("\n⏳ 1단계: 통합 스크리닝 실행 중...")
        run_integrated_screening()
        print("✅ 1단계: 통합 스크리닝 완료.")

        print("\n⏳ 2단계: 고급 재무 스크리닝 실행 중...")
        run_advanced_financial_screening()
        print("✅ 2단계: 고급 재무 스크리닝 완료.")

        print("\n⏳ 3단계: 새로운 티커 추적 실행 중...")
        track_new_tickers(ADVANCED_FINANCIAL_RESULTS_PATH)
        print("✅ 3단계: 새로운 티커 추적 완료.")

        print("\n⏳ 4단계: 변동성 스큐 스크리닝 실행 중...")
        run_volatility_skew_portfolio()
        print("✅ 4단계: 변동성 스큐 스크리닝 완료.")

        print("\n⏳ 5단계: US Setup 스크리닝 실행 중...")
        run_setup_screener()
        print("✅ 5단계: US Setup 스크리닝 완료.")

        print("\n⏳ 6단계: US Gainers 스크리닝 실행 중...")
        run_gainers_screener()
        print("✅ 6단계: US Gainers 스크리닝 완료.")

        print("\n⏳ 7단계: 주도주 투자 전략 스크리닝 실행 중...")
        run_leader_stock_screener()
        print("✅ 7단계: 주도주 투자 전략 스크리닝 완료.")

        print("\n⏳ 8단계: 상승 모멘텀 신호 스크리닝 실행 중...")
        run_momentum_signals_screener()
        print("✅ 8단계: 상승 모멘텀 신호 스크리닝 완료.")

        print("\n⏳ 9단계: IPO 투자 전략 스크리닝 실행 중...")
        run_ipo_investment_screener()
        print("✅ 9단계: IPO 투자 전략 스크리닝 완료.")

        print("\n✅ 모든 스크리닝 프로세스 완료.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 스크리닝 프로세스 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_volatility_skew_portfolio() -> None:
    """Run the volatility skew portfolio strategy."""
    try:
        from portfolio.manager.strategies import VolatilitySkewPortfolioStrategy
    except Exception as e:  # pragma: no cover - optional dependency
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
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 변동성 스큐 포트폴리오 생성 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_setup_screener() -> None:
    """Run the US Setup screener."""
    try:
        print("\n📊 US Setup Screener 시작...")
        df = screen_us_setup()
        if not df.empty:
            print(f"✅ US Setup 결과 저장 완료: {len(df)}개 종목")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ US Setup Screener 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_gainers_screener() -> None:
    """Run the US Gainers screener."""
    try:
        print("\n📊 US Gainers Screener 시작...")
        df = screen_us_gainers()
        if not df.empty:
            print(f"✅ US Gainers 결과 저장 완료: {len(df)}개 종목")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ US Gainers Screener 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_leader_stock_screener() -> None:
    """Run the leader stock screener."""
    try:
        print("\n📊 주도주 투자 전략 스크리너 시작...")
        df = run_leader_stock_screening()
        if not df.empty:
            print(f"✅ 주도주 투자 전략 결과 저장 완료: {len(df)}개 종목")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 주도주 투자 전략 스크리너 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_momentum_signals_screener() -> None:
    """Run the momentum signals screener."""
    try:
        print("\n📊 상승 모멘텀 신호 스크리너 시작...")
        df = run_momentum_signals_screening()
        if not df.empty:
            print(f"✅ 상승 모멘텀 신호 결과 저장 완료: {len(df)}개 종목")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 상승 모멘텀 신호 스크리너 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_market_breadth_collection(days: int = 252) -> None:
    """Collect market breadth indicators."""
    try:
        print("\n📊 시장 폭 데이터 수집 시작...")
        collector = MarketBreadthCollector()
        collector.collect_all_data(days)
        print("✅ 시장 폭 데이터 수집 완료")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 시장 폭 데이터 수집 실패: {e}")
        print(traceback.format_exc())


def run_ipo_investment_screener() -> None:
    """Run the IPO investment screener."""
    try:
        print("\n📊 IPO 투자 전략 스크리너 시작...")
        df = run_ipo_investment_screening()
        if not df.empty:
            print(f"✅ IPO 투자 전략 결과 저장 완료: {len(df)}개 종목")
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ IPO 투자 전략 스크리너 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_qullamaggie_strategy_task(setups: Optional[list[str]] | None = None) -> None:
    """Run the Qullamaggie trading strategy."""
    try:
        from qullamaggie import run_qullamaggie_strategy
    except Exception as e:  # pragma: no cover - optional dependency
        print(f"⚠️ 쿨라매기 모듈 로드 실패: {e}")
        return

    try:
        print("\n📊 쿨라매기 전략 시작...")
        run_qullamaggie_strategy(setups)
        print("✅ 쿨라매기 전략 완료")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 쿨라매기 전략 실행 중 오류 발생: {e}")
        print(traceback.format_exc())


def run_market_regime_analysis():
    """Perform market regime analysis and print summary."""
    import time
    unique_id = int(time.time() * 1000) % 10000
    try:
        print(f"\n📊 시장 국면 분석 시작... [ID: {unique_id}]")
        result = analyze_market_regime(save_result=True)

        print(f"\n📈 시장 국면 분석 결과:")
        print(f"  🔍 시장 점수: {result['score']}/100")
        print(f"  🔍 시장 국면: {result['regime_name']}")
        print(f"  🔍 설명: {result['description']}")
        print(f"  🔍 투자 전략: {result['strategy']}")

        print("\n📊 세부 점수:")
        if 'details' in result and 'scores' in result['details']:
            scores = result['details']['scores']
            base_score = scores.get('base_score', 0)
            tech_score = scores.get('tech_score', 0)
            print(f"  📌 지수 기본 점수: {base_score}/60")
            print(f"  📌 기술적 지표 점수: {tech_score}/40")
        else:
            print("  ⚠️ 세부 점수 정보를 찾을 수 없습니다.")
        if 'file_path' in result:
            print(f"\n💾 결과 저장 경로: {result['file_path']}")

        print("\n✅ 시장 국면 분석 완료")
        return result
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 시장 국면 분석 중 오류 발생: {e}")
        print(traceback.format_exc())
        return None


def load_strategy_module(strategy_name: str):
    """Dynamically load a portfolio strategy module."""
    try:
        strategy_path = os.path.join("portfolio", "long_short", f"{strategy_name}.py")
        if not os.path.exists(strategy_path):
            print(f"⚠️ {strategy_name}: 파일이 존재하지 않습니다 - {strategy_path}")
            return None
        spec = importlib.util.spec_from_file_location(strategy_name, strategy_path)
        if spec is None:
            print(f"⚠️ {strategy_name}: 모듈 스펙을 생성할 수 없습니다")
            return None
        module = importlib.util.module_from_spec(spec)
        module.os = os
        spec.loader.exec_module(module)  # type: ignore
        print(f"✅ {strategy_name} 모듈 로드 성공")
        return module
    except Exception as e:  # pragma: no cover - runtime log
        if "name 'os' is not defined" in str(e):
            print(f"⚠️ {strategy_name}: os 모듈 오류 - 스킵합니다")
        else:
            print(f"⚠️ {strategy_name} 모듈 로드 실패: {e}")
        return None


def run_after_market_close() -> None:
    """Update portfolio after market close."""
    try:
        print(f"\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 자동 포트폴리오 업데이트 시작")
        create_portfolio_manager()
        print(f"✅ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 자동 포트폴리오 업데이트 완료")
    except Exception as e:  # pragma: no cover - runtime log
        print(f"❌ 자동 포트폴리오 업데이트 실패: {e}")


def setup_scheduler() -> None:
    """Configure daily scheduler at 16:30."""
    if schedule is None:
        raise ImportError("schedule 패키지가 설치되어 있지 않습니다.")
    schedule.every().day.at("16:30").do(run_after_market_close)
    print("📅 스케줄러 설정 완료: 매일 오후 4시 30분에 포트폴리오 업데이트 실행")


def run_scheduler() -> None:
    """Run the configured scheduler."""
    if schedule is None:
        raise ImportError("schedule 패키지가 설치되어 있지 않습니다.")
    setup_scheduler()
    print("🔄 스케줄러 시작... (Ctrl+C로 종료)")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n⏹️ 스케줄러 종료")
