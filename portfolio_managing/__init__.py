# -*- coding: utf-8 -*-
"""
포트폴리오 관리 모듈

이 모듈은 다양한 투자 전략의 포트폴리오를 통합 관리하는 기능을 제공합니다.

주요 구성요소:
- PortfolioManager: 전체 포트폴리오 관리
- PositionTracker: 포지션 추적 및 관리
- RiskManager: 리스크 관리
- StrategyConfig: 전략 설정 관리
"""
import traceback
import pandas as pd
from .core.portfolio_manager import PortfolioManager
from .core.position_tracker import PositionTracker
from .core.risk_manager import RiskManager
from .core.strategy_config import StrategyConfig

# 버전 정보
__version__ = "1.0.0"
__author__ = "Investment Portfolio System"

# 주요 클래스들을 모듈 레벨에서 접근 가능하도록 export
__all__ = [
    'PortfolioManager',
    'PositionTracker', 
    'RiskManager',
    'StrategyConfig'
]

# 편의 함수들
def create_portfolio_manager(portfolio_name: str = "main_portfolio", initial_capital: float = 100000, **kwargs):
    """
    포트폴리오 매니저를 생성하는 편의 함수
    
    Args:
        portfolio_name: 포트폴리오 이름
        initial_capital: 초기 자본금
        **kwargs: 추가 설정 옵션
    
    Returns:
        PortfolioManager: 초기화된 포트폴리오 매니저
    """
    print(f"\n🏦 포트폴리오 매니저 생성 시작...")
    print(f"📊 포트폴리오 이름: {portfolio_name}")
    print(f"💰 초기 자본금: ${initial_capital:,.2f}")
    print(f"⏰ 생성 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # PortfolioManager는 내부에서 자체적으로 position_tracker와 risk_manager를 생성합니다
        print(f"🔧 PortfolioManager 인스턴스 생성 중...")
        manager = PortfolioManager(
            portfolio_name=portfolio_name,
            initial_capital=initial_capital
            # **kwargs는 제거 - PortfolioManager가 받지 않는 매개변수들이 포함될 수 있음
        )
        print(f"✅ 포트폴리오 매니저 생성 완료")
        
        # 포트폴리오 매니저 실행
        print(f"\n🚀 포트폴리오 관리 프로세스 시작...")
        
        # 통합 포트폴리오 관리 실행 - Static method로 호출
        print(f"📊 통합 포트폴리오 관리 실행 중...")
        PortfolioManager.run_integrated_portfolio_management()
        print(f"✅ 통합 포트폴리오 관리 완료")
        
        # 개별 전략 포트폴리오 관리 실행 - Static method로 호출
        print(f"📊 개별 전략 포트폴리오 관리 실행 중...")
        PortfolioManager.run_individual_strategy_portfolios()
        print(f"✅ 개별 전략 포트폴리오 관리 완료")
        
        # 트레이딩 신호 모니터링 및 처리 - Instance method로 호출
        print(f"📊 트레이딩 신호 모니터링 시작...")
        manager.monitor_and_process_trading_signals()
        print(f"✅ 트레이딩 신호 모니터링 완료")
        
        print(f"\n🎉 포트폴리오 관리 프로세스 모든 단계 완료!")
        return manager
        
    except Exception as e:
        print(f"❌ 포트폴리오 매니저 생성/실행 중 오류: {e}")
        print(f"🔍 오류 발생 시간: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(traceback.format_exc())
        return None

def create_strategy_config(name: str, strategy_type: str = "LONG", **kwargs):
    """
    전략 설정을 생성하는 편의 함수
    
    Args:
        name: 전략 이름
        strategy_type: 전략 타입 (LONG/SHORT)
        **kwargs: 추가 설정 옵션
    
    Returns:
        StrategyConfig: 전략 설정 객체
    """
    return StrategyConfig(
        name=name,
        strategy_type=strategy_type,
        **kwargs
    )