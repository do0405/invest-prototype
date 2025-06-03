# -*- coding: utf-8 -*-
"""
리스크 관리 모듈 (RiskManager)
Trailing Stop 관리, 포지션 사이징, 리스크 지표 모니터링
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# 프로젝트 루트 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from config import RESULTS_VER2_DIR
from utils import ensure_dir

class RiskManager:
    """리스크 관리 클래스"""
    
    def __init__(self, portfolio_name: str = "default"):
        self.portfolio_name = portfolio_name
        self.risk_dir = os.path.join(RESULTS_VER2_DIR, 'risk_management')
        ensure_dir(self.risk_dir)
        
        # 기본 리스크 설정
        self.risk_limits = {
            'max_position_size': 0.05,  # 개별 포지션 최대 5%
            'max_sector_exposure': 0.20,  # 섹터별 최대 20%
            'max_portfolio_var': 0.02,  # 일일 VaR 2%
            'trailing_stop_pct': 0.08,  # 8% trailing stop
            'max_correlation': 0.7,  # 최대 상관관계 70%
        }
        
        self.stop_orders = self.load_stop_orders()
    
    def load_stop_orders(self) -> pd.DataFrame:
        """기존 스탑 오더 로드"""
        stop_file = os.path.join(self.risk_dir, f'{self.portfolio_name}_stop_orders.csv')
        
        if os.path.exists(stop_file):
            try:
                return pd.read_csv(stop_file)
            except Exception:
                pass
        
        return pd.DataFrame(columns=[
            'symbol', 'position_type', 'strategy', 'stop_price', 
            'trailing_stop_pct', 'highest_price', 'created_date', 'last_updated'
        ])
    
    def calculate_position_size(self, symbol: str, strategy: str, 
                              portfolio_value: float, 
                              strategy_config: Dict = None,
                              signal: pd.Series = None,
                              volatility: float = None) -> float:
        """통합 포지션 사이즈 계산"""
        try:
            # Strategy config based calculation (from PortfolioUtils)
            if strategy_config and signal is not None:
                risk_per_position = strategy_config.get('risk_per_position', 0.02)
                max_position_size = strategy_config.get('max_position_size', 0.10)
                
                risk_amount = portfolio_value * risk_per_position
                price = signal.get('price', 0)
                
                if price <= 0:
                    return 0
                
                max_amount = portfolio_value * max_position_size
                position_amount = min(risk_amount, max_amount)
                
                # Apply volatility adjustment if available
                if volatility is not None:
                    volatility_adjustment = min(1.0, 0.2 / max(volatility, 0.1))
                    position_amount *= volatility_adjustment
                
                return position_amount / price
            
            # Default risk-based calculation
            base_size = portfolio_value * self.risk_limits['max_position_size']
            
            if volatility is not None:
                volatility_adjustment = min(1.0, 0.2 / max(volatility, 0.1))
                base_size *= volatility_adjustment
            
            return base_size
            
        except Exception as e:
            print(f"⚠️ 포지션 사이즈 계산 실패 ({symbol}): {e}")
            return portfolio_value * 0.01  # 기본값 1%
    
    def set_trailing_stop(self, symbol: str, position_type: str, strategy: str,
                         current_price: float, trailing_pct: float = None) -> bool:
        """Trailing Stop 설정"""
        try:
            if trailing_pct is None:
                trailing_pct = self.risk_limits['trailing_stop_pct']
            
            # 스탑 가격 계산
            if position_type == 'LONG':
                stop_price = current_price * (1 - trailing_pct)
            else:  # SHORT
                stop_price = current_price * (1 + trailing_pct)
            
            # 기존 스탑 오더 확인
            mask = (self.stop_orders['symbol'] == symbol) & \
                   (self.stop_orders['position_type'] == position_type) & \
                   (self.stop_orders['strategy'] == strategy)
            
            stop_data = {
                'symbol': symbol,
                'position_type': position_type,
                'strategy': strategy,
                'stop_price': stop_price,
                'trailing_stop_pct': trailing_pct,
                'highest_price': current_price,
                'created_date': datetime.now().strftime('%Y-%m-%d'),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            if mask.any():
                # 기존 스탑 오더 업데이트
                idx = mask.idxmax()
                for key, value in stop_data.items():
                    self.stop_orders.loc[idx, key] = value
            else:
                # 새 스탑 오더 추가
                self.stop_orders = pd.concat([self.stop_orders, pd.DataFrame([stop_data])], 
                                           ignore_index=True)
            
            self.save_stop_orders()
            return True
            
        except Exception as e:
            print(f"❌ Trailing Stop 설정 실패 ({symbol}): {e}")
            return False
    
    def update_trailing_stops(self, positions_df: pd.DataFrame) -> List[Dict]:
        """Trailing Stop 업데이트 및 청산 신호 생성"""
        stop_signals = []
        
        try:
            for _, position in positions_df.iterrows():
                symbol = position['symbol']
                position_type = position['position_type']
                strategy = position['strategy']
                current_price = position['current_price']
                
                # 해당 포지션의 스탑 오더 찾기
                mask = (self.stop_orders['symbol'] == symbol) & \
                       (self.stop_orders['position_type'] == position_type) & \
                       (self.stop_orders['strategy'] == strategy)
                
                if not mask.any():
                    continue
                
                stop_order = self.stop_orders[mask].iloc[0]
                idx = mask.idxmax()
                
                # Trailing Stop 로직
                if position_type == 'LONG':
                    # 롱 포지션: 가격이 상승하면 스탑 가격도 상승
                    if current_price > stop_order['highest_price']:
                        new_stop_price = current_price * (1 - stop_order['trailing_stop_pct'])
                        self.stop_orders.loc[idx, 'highest_price'] = current_price
                        self.stop_orders.loc[idx, 'stop_price'] = new_stop_price
                        self.stop_orders.loc[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 스탑 가격 도달 시 청산 신호
                    if current_price <= stop_order['stop_price']:
                        stop_signals.append({
                            'symbol': symbol,
                            'position_type': position_type,
                            'strategy': strategy,
                            'action': 'STOP_LOSS',
                            'trigger_price': current_price,
                            'stop_price': stop_order['stop_price'],
                            'reason': 'Trailing Stop Triggered'
                        })
                
                else:  # SHORT
                    # 숏 포지션: 가격이 하락하면 스탑 가격도 하락
                    if current_price < stop_order['highest_price']:
                        new_stop_price = current_price * (1 + stop_order['trailing_stop_pct'])
                        self.stop_orders.loc[idx, 'highest_price'] = current_price
                        self.stop_orders.loc[idx, 'stop_price'] = new_stop_price
                        self.stop_orders.loc[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 스탑 가격 도달 시 청산 신호
                    if current_price >= stop_order['stop_price']:
                        stop_signals.append({
                            'symbol': symbol,
                            'position_type': position_type,
                            'strategy': strategy,
                            'action': 'STOP_LOSS',
                            'trigger_price': current_price,
                            'stop_price': stop_order['stop_price'],
                            'reason': 'Trailing Stop Triggered'
                        })
            
            if stop_signals:
                self.save_stop_orders()
            
            return stop_signals
            
        except Exception as e:
            print(f"❌ Trailing Stop 업데이트 실패: {e}")
            return []
    
    def calculate_portfolio_var(self, positions_df: pd.DataFrame, 
                               confidence_level: float = 0.95) -> float:
        """포트폴리오 VaR 계산"""
        try:
            if positions_df.empty:
                return 0.0
            
            # 간단한 VaR 계산 (정규분포 가정)
            total_value = positions_df['market_value'].sum()
            
            # 포지션별 변동성 추정 (미실현 손익 기반)
            if 'unrealized_pnl_pct' in positions_df.columns:
                portfolio_volatility = positions_df['unrealized_pnl_pct'].std() / 100
            else:
                portfolio_volatility = 0.02  # 기본값 2%
            
            # VaR 계산 (정규분포 가정)
            from scipy.stats import norm
            var_multiplier = norm.ppf(confidence_level)
            var_amount = total_value * portfolio_volatility * var_multiplier
            
            return var_amount
            
        except Exception as e:
            print(f"⚠️ VaR 계산 실패: {e}")
            return 0.0
    
    def check_risk_limits(self, positions_df: pd.DataFrame) -> List[Dict]:
        """리스크 한도 체크"""
        warnings = []
        
        try:
            if positions_df.empty:
                return warnings
            
            total_value = positions_df['market_value'].sum()
            
            # 1. 개별 포지션 사이즈 체크
            for _, position in positions_df.iterrows():
                position_pct = abs(position['market_value']) / total_value
                if position_pct > self.risk_limits['max_position_size']:
                    warnings.append({
                        'type': 'POSITION_SIZE_LIMIT',
                        'symbol': position['symbol'],
                        'current_pct': position_pct,
                        'limit_pct': self.risk_limits['max_position_size'],
                        'message': f"{position['symbol']} 포지션이 한도를 초과했습니다 ({position_pct:.1%} > {self.risk_limits['max_position_size']:.1%})"
                    })
            
            # 2. VaR 체크
            portfolio_var = self.calculate_portfolio_var(positions_df)
            var_pct = portfolio_var / total_value
            if var_pct > self.risk_limits['max_portfolio_var']:
                warnings.append({
                    'type': 'VAR_LIMIT',
                    'current_var': var_pct,
                    'limit_var': self.risk_limits['max_portfolio_var'],
                    'message': f"포트폴리오 VaR이 한도를 초과했습니다 ({var_pct:.1%} > {self.risk_limits['max_portfolio_var']:.1%})"
                })
            
            # 3. 전략별 집중도 체크
            strategy_exposure = positions_df.groupby('strategy')['market_value'].sum() / total_value
            for strategy, exposure in strategy_exposure.items():
                if exposure > self.risk_limits['max_sector_exposure']:
                    warnings.append({
                        'type': 'STRATEGY_CONCENTRATION',
                        'strategy': strategy,
                        'current_exposure': exposure,
                        'limit_exposure': self.risk_limits['max_sector_exposure'],
                        'message': f"{strategy} 전략 집중도가 한도를 초과했습니다 ({exposure:.1%} > {self.risk_limits['max_sector_exposure']:.1%})"
                    })
            
            return warnings
            
        except Exception as e:
            print(f"⚠️ 리스크 한도 체크 실패: {e}")
            return []
    
    def save_stop_orders(self):
        """스탑 오더 저장"""
        try:
            stop_file = os.path.join(self.risk_dir, f'{self.portfolio_name}_stop_orders.csv')
            self.stop_orders.to_csv(stop_file, index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"⚠️ 스탑 오더 저장 실패: {e}")
    
    def remove_stop_order(self, symbol: str, position_type: str, strategy: str):
        """스탑 오더 제거 (포지션 청산 시)"""
        try:
            mask = (self.stop_orders['symbol'] == symbol) & \
                   (self.stop_orders['position_type'] == position_type) & \
                   (self.stop_orders['strategy'] == strategy)
            
            self.stop_orders = self.stop_orders[~mask]
            self.save_stop_orders()
            
        except Exception as e:
            print(f"⚠️ 스탑 오더 제거 실패: {e}")
    
    def get_risk_summary(self, positions_df: pd.DataFrame) -> Dict:
        """리스크 요약 정보"""
        try:
            if positions_df.empty:
                return {'total_var': 0, 'risk_warnings': 0, 'active_stops': 0}
            
            total_value = positions_df['market_value'].sum()
            portfolio_var = self.calculate_portfolio_var(positions_df)
            risk_warnings = len(self.check_risk_limits(positions_df))
            active_stops = len(self.stop_orders)
            
            return {
                'total_portfolio_value': total_value,
                'portfolio_var': portfolio_var,
                'var_percentage': portfolio_var / total_value * 100 if total_value > 0 else 0,
                'risk_warnings': risk_warnings,
                'active_stop_orders': active_stops,
                'max_position_size_limit': self.risk_limits['max_position_size'] * 100,
                'trailing_stop_percentage': self.risk_limits['trailing_stop_pct'] * 100
            }
            
        except Exception as e:
            print(f"⚠️ 리스크 요약 생성 실패: {e}")
            return {}