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
    # PortfolioManager는 내부에서 자체적으로 position_tracker와 risk_manager를 생성합니다
    return PortfolioManager(
        portfolio_name=portfolio_name,
        initial_capital=initial_capital
        # **kwargs는 제거 - PortfolioManager가 받지 않는 매개변수들이 포함될 수 있음
    )

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