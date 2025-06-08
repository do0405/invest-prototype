# -*- coding: utf-8 -*-
"""
전략별 설정 관리 모듈
각 전략의 매수/매도/손절/수익실현 조건을 정의
"""
import os
from typing import Dict, Any
from enum import Enum

class OrderType(Enum):
    MARKET = "market"  # 시장가
    LIMIT = "limit"    # 지정가

class PositionType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class ExitConditionType(Enum):
    PROFIT_TARGET = "profit_target"      # 수익 목표
    STOP_LOSS = "stop_loss"             # 손절매
    TRAILING_STOP = "trailing_stop"      # 추격 역지정가
    TIME_BASED = "time_based"           # 시간 기반
    CONDITION_BASED = "condition_based"  # 조건 기반

class StrategyConfig:
    """전략별 설정 클래스"""
    
    # 전략별 설정 정의
    STRATEGY_CONFIGS = {
        "strategy1": {
            "name": "트렌드 하이 모멘텀 롱",
            "position_type": PositionType.LONG,
            "result_file": "buy/strategy1_results.csv",
            "entry": {
                "order_type": OrderType.MARKET,
                "price_offset_pct": 0.0  # 시장가
            },
            "exit_conditions": {
                "stop_loss": {
                    "type": ExitConditionType.STOP_LOSS,
                    "atr_multiplier": 5.0,
                    "atr_period": 20
                },
                "trailing_stop": {
                    "type": ExitConditionType.TRAILING_STOP,
                    "trailing_pct": 0.25  # 25%
                }
            },
            "position_sizing": {
                "risk_pct": 0.02,  # 2% 리스크
                "max_allocation_pct": 0.10,  # 최대 10% 배분
                "max_positions": 10
            }
        },
        
        "strategy2": {
            "name": "평균회귀 단일 숏",
            "position_type": PositionType.SHORT,
            "result_file": "sell/strategy2_results.csv",
            "entry": {
                "order_type": OrderType.LIMIT,
                "price_offset_pct": 0.04  # 전일 종가 +4%
            },
            "exit_conditions": {
                "stop_loss": {
                    "type": ExitConditionType.STOP_LOSS,
                    "atr_multiplier": 3.0,
                    "atr_period": 10
                },
                "profit_target": {
                    "type": ExitConditionType.PROFIT_TARGET,
                    "target_pct": 0.04  # 4% 수익
                },
                "time_based": {
                    "type": ExitConditionType.TIME_BASED,
                    "max_holding_days": 2
                }
            },
            "position_sizing": {
                "risk_pct": 0.02,
                "max_allocation_pct": 0.10,
                "max_positions": 10
            }
        },
        
        "strategy3": {
            "name": "평균회귀 셀오프 롱",
            "position_type": PositionType.LONG,
            "entry": {
                "order_type": OrderType.LIMIT,
                "price_offset_pct": -0.07  # 직전 종가 -7%
            },
            "exit_conditions": {
                "stop_loss": {
                    "type": ExitConditionType.STOP_LOSS,
                    "atr_multiplier": 2.5,
                    "atr_period": 10
                },
                "profit_target": {
                    "type": ExitConditionType.PROFIT_TARGET,
                    "target_pct": 0.04  # 4% 수익
                },
                "time_based": {
                    "type": ExitConditionType.TIME_BASED,
                    "max_holding_days": 3
                }
            },
            "position_sizing": {
                "risk_pct": 0.02,
                "max_allocation_pct": 0.10,
                "max_positions": 10
            }
        },
        
        "strategy4": {
            "name": "트렌드 저변동성 롱",
            "position_type": PositionType.LONG,
            "entry": {
                "order_type": OrderType.MARKET,
                "price_offset_pct": 0.0  # 시장가
            },
            "exit_conditions": {
                "stop_loss": {
                    "type": ExitConditionType.STOP_LOSS,
                    "atr_multiplier": 1.5,
                    "atr_period": 40
                },
                "trailing_stop": {
                    "type": ExitConditionType.TRAILING_STOP,
                    "trailing_pct": 0.20  # 20%
                }
            },
            "position_sizing": {
                "risk_pct": 0.02,
                "max_allocation_pct": 0.10,
                "max_positions": 10
            }
        },
        
        "strategy5": {
            "name": "평균회귀 하이 ADX 리버설 롱",
            "position_type": PositionType.LONG,
            "entry": {
                "order_type": OrderType.LIMIT,
                "price_offset_pct": -0.03  # 직전 종가 -3%
            },
            "exit_conditions": {
                "stop_loss": {
                    "type": ExitConditionType.STOP_LOSS,
                    "atr_multiplier": 3.0,
                    "atr_period": 10
                },
                "condition_based": {
                    "type": ExitConditionType.CONDITION_BASED,
                    "condition": "close > 10day_atr_upper"
                },
                "time_based": {
                    "type": ExitConditionType.TIME_BASED,
                    "max_holding_days": 6
                }
            },
            "position_sizing": {
                "risk_pct": 0.02,
                "max_allocation_pct": 0.10,
                "max_positions": 10
            }
        },
        
        "strategy6": {
            "name": "평균회귀 6일 급등 숏",
            "position_type": PositionType.SHORT,
            "entry": {
                "order_type": OrderType.LIMIT,
                "price_offset_pct": 0.05  # 직전 종가 +5%
            },
            "exit_conditions": {
                "stop_loss": {
                    "type": ExitConditionType.STOP_LOSS,
                    "atr_multiplier": 3.0,
                    "atr_period": 10
                },
                "profit_target": {
                    "type": ExitConditionType.PROFIT_TARGET,
                    "target_pct": 0.05  # 5% 수익
                },
                "time_based": {
                    "type": ExitConditionType.TIME_BASED,
                    "max_holding_days": 3
                }
            },
            "position_sizing": {
                "risk_pct": 0.02,
                "max_allocation_pct": 0.10,
                "max_positions": 10
            }
        }
    }
    
    @classmethod
    def get_strategy_config(cls, strategy_name: str) -> Dict[str, Any]:
        """전략 설정 반환"""
        return cls.STRATEGY_CONFIGS.get(strategy_name, {})
    
    @classmethod
    def get_all_strategies(cls) -> list:
        """모든 전략 이름 반환"""
        return list(cls.STRATEGY_CONFIGS.keys())
    
    @classmethod
    def is_long_strategy(cls, strategy_name: str) -> bool:
        """롱 전략 여부 확인"""
        config = cls.get_strategy_config(strategy_name)
        return config.get('position_type') == PositionType.LONG
    
    @classmethod
    def is_short_strategy(cls, strategy_name: str) -> bool:
        """숏 전략 여부 확인"""
        config = cls.get_strategy_config(strategy_name)
        return config.get('position_type') == PositionType.SHORT

    @classmethod
    def get_result_file_path(cls, strategy_name: str, base_dir: str) -> str:
        """전략별 결과 파일 경로 반환"""
        config = cls.get_strategy_config(strategy_name)
        if config and 'result_file' in config:
            return os.path.join(base_dir, config['result_file'])
        return ""