# -*- coding: utf-8 -*-
"""
포지션 추적 모듈 (PositionTracker)
실시간 포지션 상태 추적 및 자동 데이터 업데이트
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json

from .price_calculator import PriceCalculator

from utils.path_utils import add_project_root

# 프로젝트 루트 추가
add_project_root()

from config import PORTFOLIO_RESULTS_DIR
from utils import ensure_dir
from .strategy_config import StrategyConfig, OrderType, PositionType, ExitConditionType

class PositionTracker:
    """포지션 추적 및 관리 클래스"""
    
    def __init__(self, portfolio_name: str = "default"):
        self.portfolio_name = portfolio_name
        self.positions_dir = os.path.join(PORTFOLIO_RESULTS_DIR, 'portfolio_positions')
        ensure_dir(self.positions_dir)
        
        self.positions_file = os.path.join(self.positions_dir, f'{portfolio_name}_positions.csv')
        self.history_file = os.path.join(self.positions_dir, f'{portfolio_name}_history.csv')
        
        # 포지션 데이터 로드
        self.positions = self.load_positions()
        
    def load_positions(self) -> pd.DataFrame:
        """기존 포지션 데이터 로드"""
        if os.path.exists(self.positions_file):
            try:
                from utils.screener_utils import read_csv_flexible
                df = read_csv_flexible(self.positions_file)
                if df is not None:
                    return df
                else:
                    print(f"⚠️ 포지션 파일 읽기 실패")
            except Exception as e:
                print(f"⚠️ 포지션 파일 로드 실패: {e}")
        
        # 빈 DataFrame 생성
        return pd.DataFrame(columns=[
            'symbol', 'position_type', 'quantity', 'entry_price', 'entry_date',
            'current_price', 'market_value', 'unrealized_pnl', 'unrealized_pnl_pct',
            'strategy', 'weight', 'last_updated', 'entry_order_type', 'target_entry_price',
            'holding_days', 'atr_value', 'stop_loss_price', 'profit_target_price',
            'trailing_stop_price', 'max_holding_days'
        ])

    def has_position(self, symbol: str, strategy: str) -> bool:
        """이미 동일 종목의 포지션이 존재하는지 확인합니다."""
        mask = (self.positions['symbol'] == symbol) & (self.positions['strategy'] == strategy)
        return mask.any()

    def get_portfolio_value(self) -> float:
        """현재 포트폴리오 가치 총합을 반환합니다."""
        if self.positions.empty:
            return 0.0
        return float(self.positions['market_value'].sum())

    def add_position(self, position_data: Dict) -> bool:
        """포지션 데이터(dict)를 추가합니다."""
        try:
            new_position = {
                'symbol': position_data['symbol'],
                'position_type': position_data.get('position_type', 'LONG'),
                'quantity': position_data['quantity'],
                'entry_price': position_data['entry_price'],
                'entry_date': position_data.get('entry_date', datetime.now().strftime('%Y-%m-%d')),
                'current_price': position_data.get('entry_price'),
                'market_value': position_data['quantity'] * position_data.get('entry_price', 0),
                'unrealized_pnl': 0.0,
                'unrealized_pnl_pct': 0.0,
                'strategy': position_data.get('strategy', ''),
                'weight': position_data.get('weight', 0.0),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'entry_order_type': position_data.get('entry_order_type', 'market'),
                'target_entry_price': position_data.get('entry_price'),
                'holding_days': 0,
                'atr_value': None,
                'stop_loss_price': position_data.get('stop_loss'),
                'profit_target_price': position_data.get('take_profit'),
                'trailing_stop_price': None,
                'max_holding_days': position_data.get('max_holding_days')
            }

            self.positions = pd.concat([self.positions, pd.DataFrame([new_position])], ignore_index=True)
            self.save_positions()
            print(f"✅ 포지션 추가: {new_position['symbol']} {new_position['position_type']} {new_position['quantity']:.2f}주")
            return True
        except Exception as e:
            print(f"❌ 포지션 추가 실패 ({position_data.get('symbol')}): {e}")
            return False
    
    
    def close_position(
        self,
        symbol: str,
        position_type: str,
        strategy: str,
        close_price: Optional[float] = None,
        exit_reason: str = "manual",
    ) -> Tuple[bool, Dict]:
        """포지션 청산 및 거래 기록 반환"""
        try:
            mask = (self.positions['symbol'] == symbol) & \
                   (self.positions['position_type'] == position_type) & \
                   (self.positions['strategy'] == strategy)
            
            if not mask.any():
                print(f"⚠️ 청산할 포지션을 찾을 수 없습니다: {symbol} {position_type} {strategy}")
                return False, {}
            
            if close_price is None:
                close_price = self.get_current_price(symbol)
                if close_price is None:
                    print(f"⚠️ {symbol} 현재가를 가져올 수 없습니다")
                    return False, {}
            
            position = self.positions[mask].iloc[0]
            
            # 실현 손익 계산
            if position_type == 'LONG':
                realized_pnl = (close_price - position['entry_price']) * position['quantity']
            else:
                realized_pnl = (position['entry_price'] - close_price) * position['quantity']
            
            realized_pnl_pct = realized_pnl / (position['entry_price'] * position['quantity']) * 100
            
            # 거래 기록 생성
            trade_record = {
                'symbol': symbol,
                'strategy': strategy,
                'entry_date': position['entry_date'],
                'exit_date': datetime.now().strftime('%Y-%m-%d'),
                'entry_price': position['entry_price'],
                'exit_price': close_price,
                'quantity': position['quantity'],
                'return_pct': realized_pnl_pct,
                'exit_reason': exit_reason,
                'holding_days': (datetime.now() - pd.to_datetime(position['entry_date'], utc=True)).days
            }
            
            # 포지션 제거
            self.positions = self.positions[~mask].reset_index(drop=True)
            self.save_positions()
            
            # 거래 기록 저장 (새로 추가)
            if hasattr(self, 'portfolio_manager') and self.portfolio_manager:
                self.portfolio_manager.portfolio_utils.record_trade(trade_record)
            
            print(f"✅ 포지션 청산 완료: {symbol} ({strategy}) - {exit_reason} (수익률: {realized_pnl_pct:.2f}%)")
            
            return True, trade_record
            
        except Exception as e:
            print(f"❌ 포지션 청산 실패: {e}")
            return False, {}
    
    def update_positions(self) -> bool:
        """모든 포지션의 현재가 및 손익 업데이트"""
        if self.positions.empty:
            return True
        
        try:
            updated_count = 0
            for idx, position in self.positions.iterrows():
                current_price = PriceCalculator.get_current_price(position['symbol'])
                
                if current_price is not None:
                    # 현재가 업데이트
                    self.positions.loc[idx, 'current_price'] = current_price
                    self.positions.loc[idx, 'market_value'] = position['quantity'] * current_price
                    
                    # 미실현 손익 계산
                    if position['position_type'] == 'LONG':
                        unrealized_pnl = (current_price - position['entry_price']) * position['quantity']
                    else:  # SHORT
                        unrealized_pnl = (position['entry_price'] - current_price) * position['quantity']
                    
                    unrealized_pnl_pct = unrealized_pnl / (position['entry_price'] * position['quantity']) * 100
                    
                    self.positions.loc[idx, 'unrealized_pnl'] = unrealized_pnl
                    self.positions.loc[idx, 'unrealized_pnl_pct'] = unrealized_pnl_pct
                    
                    # 보유일 업데이트
                    entry_date = pd.to_datetime(position['entry_date'], utc=True)
                    holding_days = (datetime.now() - entry_date).days
                    self.positions.loc[idx, 'holding_days'] = holding_days
                    
                    self.positions.loc[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    updated_count += 1
            
            self.save_positions()
            print(f"✅ 포지션 업데이트 완료: {updated_count}/{len(self.positions)}개")
            return True
            
        except Exception as e:
            print(f"❌ 포지션 업데이트 실패: {e}")
            return False
    
    # get_current_price 메서드 제거 - PriceCalculator.get_current_price를 직접 사용
    
    
    def save_positions(self):
        """포지션 데이터 저장"""
        try:
            self.positions.to_csv(self.positions_file, index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"⚠️ 포지션 저장 실패: {e}")
    
    def get_portfolio_summary(self) -> Dict:
        """포트폴리오 요약 정보 반환"""
        if self.positions.empty:
            return {
                'total_positions': 0,
                'total_market_value': 0.0,
                'total_unrealized_pnl': 0.0,
                'total_unrealized_pnl_pct': 0.0,
                'long_positions': 0,
                'short_positions': 0
            }
        
        summary = {
            'total_positions': len(self.positions),
            'total_market_value': self.positions['market_value'].sum(),
            'total_unrealized_pnl': self.positions['unrealized_pnl'].sum(),
            'total_unrealized_pnl_pct': (self.positions['unrealized_pnl'].sum() / 
                                       self.positions['market_value'].sum() * 100),
            'long_positions': len(self.positions[self.positions['position_type'] == 'LONG']),
            'short_positions': len(self.positions[self.positions['position_type'] == 'SHORT']),
            'strategies': self.positions['strategy'].unique().tolist(),
            'last_updated': self.positions['last_updated'].max() if not self.positions.empty else None
        }
        
        return summary
    
    def get_positions_by_strategy(self, strategy: str) -> pd.DataFrame:
        """특정 전략의 포지션만 반환"""
        return self.positions[self.positions['strategy'] == strategy].copy()
    
    def get_performance_metrics(self) -> Dict:
        """성과 지표 계산"""
        if not os.path.exists(self.history_file):
            return {}
        
        try:
            history_df = pd.read_csv(self.history_file)
            if history_df.empty:
                return {}
            
            metrics = {
                'total_trades': len(history_df),
                'winning_trades': len(history_df[history_df['realized_pnl'] > 0]),
                'losing_trades': len(history_df[history_df['realized_pnl'] < 0]),
                'win_rate': len(history_df[history_df['realized_pnl'] > 0]) / len(history_df) * 100,
                'total_realized_pnl': history_df['realized_pnl'].sum(),
                'avg_realized_pnl': history_df['realized_pnl'].mean(),
                'avg_holding_days': history_df['holding_days'].mean(),
                'best_trade': history_df['realized_pnl'].max(),
                'worst_trade': history_df['realized_pnl'].min()
            }
            
            return metrics
            
        except Exception as e:            print(f"⚠️ 성과 지표 계산 실패: {e}")
