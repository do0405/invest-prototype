# -*- coding: utf-8 -*-
"""
포지션 추적 모듈 (PositionTracker)
실시간 포지션 상태 추적 및 자동 데이터 업데이트
"""

import os
import sys
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json

# 프로젝트 루트 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from config import RESULTS_VER2_DIR
from utils import ensure_dir
from .strategy_config import StrategyConfig, OrderType, PositionType, ExitConditionType

class PositionTracker:
    """포지션 추적 및 관리 클래스"""
    
    def __init__(self, portfolio_name: str = "default"):
        self.portfolio_name = portfolio_name
        self.positions_dir = os.path.join(RESULTS_VER2_DIR, 'portfolio_positions')
        ensure_dir(self.positions_dir)
        
        self.positions_file = os.path.join(self.positions_dir, f'{portfolio_name}_positions.csv')
        self.history_file = os.path.join(self.positions_dir, f'{portfolio_name}_history.csv')
        
        # 포지션 데이터 로드
        self.positions = self.load_positions()
        
    def load_positions(self) -> pd.DataFrame:
        """기존 포지션 데이터 로드"""
        if os.path.exists(self.positions_file):
            try:
                return pd.read_csv(self.positions_file)
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
    
    def add_position_with_strategy(self, symbol: str, strategy_name: str, 
                                 current_price: float, weight: float = 0.0,
                                 atr_value: float = None) -> bool:
        """전략 설정에 따른 포지션 추가"""
        try:
            strategy_config = StrategyConfig.get_strategy_config(strategy_name)
            if not strategy_config:
                print(f"❌ 전략 설정을 찾을 수 없습니다: {strategy_name}")
                return False
            
            position_type = strategy_config['position_type'].value
            entry_config = strategy_config['entry']
            exit_conditions = strategy_config['exit_conditions']
            
            # 진입가 계산
            if entry_config['order_type'] == OrderType.MARKET:
                entry_price = current_price
                target_entry_price = current_price
            else:  # LIMIT
                offset_pct = entry_config['price_offset_pct']
                target_entry_price = current_price * (1 + offset_pct)
                entry_price = target_entry_price  # 지정가 주문의 경우
            
            # 수량 계산 (리스크 기반)
            risk_pct = strategy_config['position_sizing']['risk_pct']
            
            # 손절가 계산
            stop_loss_price = None
            if 'stop_loss' in exit_conditions:
                stop_config = exit_conditions['stop_loss']
                if atr_value:
                    atr_multiplier = stop_config['atr_multiplier']
                    if position_type == 'LONG':
                        stop_loss_price = entry_price - (atr_value * atr_multiplier)
                    else:  # SHORT
                        stop_loss_price = entry_price + (atr_value * atr_multiplier)
            
            # 수익 목표가 계산
            profit_target_price = None
            if 'profit_target' in exit_conditions:
                target_pct = exit_conditions['profit_target']['target_pct']
                if position_type == 'LONG':
                    profit_target_price = entry_price * (1 + target_pct)
                else:  # SHORT
                    profit_target_price = entry_price * (1 - target_pct)
            
            # 최대 보유일 설정
            max_holding_days = None
            if 'time_based' in exit_conditions:
                max_holding_days = exit_conditions['time_based']['max_holding_days']
            
            # 리스크 기반 수량 계산
            if stop_loss_price:
                risk_per_share = abs(entry_price - stop_loss_price)
                portfolio_value = self.get_portfolio_value() 
                risk_amount = portfolio_value * risk_pct
                quantity = risk_amount / risk_per_share if risk_per_share > 0 else 0
            else:
                # 기본 수량 계산
                portfolio_value = self.get_portfolio_value()
                position_value = portfolio_value * weight
                quantity = position_value / entry_price
            
            new_position = {
                'symbol': symbol,
                'position_type': position_type,
                'quantity': quantity,
                'entry_price': entry_price,
                'entry_date': datetime.now().strftime('%Y-%m-%d'),
                'current_price': current_price,
                'market_value': quantity * current_price,
                'unrealized_pnl': 0.0,
                'unrealized_pnl_pct': 0.0,
                'strategy': strategy_name,
                'weight': weight,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'entry_order_type': entry_config['order_type'].value,
                'target_entry_price': target_entry_price,
                'holding_days': 0,
                'atr_value': atr_value,
                'stop_loss_price': stop_loss_price,
                'profit_target_price': profit_target_price,
                'trailing_stop_price': None,
                'max_holding_days': max_holding_days
            }
            
            # 기존 포지션 확인
            existing_mask = (self.positions['symbol'] == symbol) & \
                          (self.positions['position_type'] == position_type) & \
                          (self.positions['strategy'] == strategy_name)
            
            if existing_mask.any():
                # 기존 포지션 업데이트 (평균 단가 계산)
                idx = existing_mask.idxmax()
                existing_qty = self.positions.loc[idx, 'quantity']
                existing_price = self.positions.loc[idx, 'entry_price']
                
                total_qty = existing_qty + quantity
                avg_price = (existing_qty * existing_price + quantity * entry_price) / total_qty
                
                self.positions.loc[idx, 'quantity'] = total_qty
                self.positions.loc[idx, 'entry_price'] = avg_price
                self.positions.loc[idx, 'market_value'] = total_qty * current_price
                self.positions.loc[idx, 'last_updated'] = new_position['last_updated']
            else:
                # 새 포지션 추가
                self.positions = pd.concat([self.positions, pd.DataFrame([new_position])], 
                                         ignore_index=True)
            
            self.save_positions()
            print(f"✅ {strategy_name} 포지션 추가: {symbol} {position_type} {quantity:.2f}주")
            return True
            
        except Exception as e:
            print(f"❌ 포지션 추가 실패 ({symbol}): {e}")
            return False
    
#    def check_exit_conditions(self) -> List[Dict]:
#        """모든 포지션의 청산 조건 확인"""
    """
        exit_signals = []
        
        if self.positions.empty:
            return exit_signals
        
        for idx, position in self.positions.iterrows():
            strategy_name = position['strategy']
            strategy_config = StrategyConfig.get_strategy_config(strategy_name)
            
            if not strategy_config:
                continue
            
            exit_conditions = strategy_config['exit_conditions']
            current_price = position['current_price']
            entry_price = position['entry_price']
            position_type = position['position_type']
            
            # 보유일 계산
            entry_date = pd.to_datetime(position['entry_date'])
            holding_days = (datetime.now() - entry_date).days
            
            # 손익률 계산
            if position_type == 'LONG':
                pnl_pct = (current_price - entry_price) / entry_price
            else:  # SHORT
                pnl_pct = (entry_price - current_price) / entry_price
            
            # 1. 손절매 조건 확인
            if position['stop_loss_price'] and current_price:
                if position_type == 'LONG' and current_price <= position['stop_loss_price']:
                    exit_signals.append({
                        'symbol': position['symbol'],
                        'strategy': strategy_name,
                        'position_type': position_type,
                        'exit_type': 'stop_loss',
                        'exit_price': current_price,
                        'reason': f"손절매 조건 달성 (${current_price:.2f} <= ${position['stop_loss_price']:.2f})"
                    })
                    continue
                elif position_type == 'SHORT' and current_price >= position['stop_loss_price']:
                    exit_signals.append({
                        'symbol': position['symbol'],
                        'strategy': strategy_name,
                        'position_type': position_type,
                        'exit_type': 'stop_loss',
                        'exit_price': current_price,
                        'reason': f"손절매 조건 달성 (${current_price:.2f} >= ${position['stop_loss_price']:.2f})"
                    })
                    continue
            
            # 2. 수익 목표 조건 확인
            if position['profit_target_price'] and current_price:
                if position_type == 'LONG' and current_price >= position['profit_target_price']:
                    exit_signals.append({
                        'symbol': position['symbol'],
                        'strategy': strategy_name,
                        'position_type': position_type,
                        'exit_type': 'profit_target',
                        'exit_price': current_price,
                        'reason': f"수익 목표 달성 ({pnl_pct:.1%})"
                    })
                    continue
                elif position_type == 'SHORT' and current_price <= position['profit_target_price']:
                    exit_signals.append({
                        'symbol': position['symbol'],
                        'strategy': strategy_name,
                        'position_type': position_type,
                        'exit_type': 'profit_target',
                        'exit_price': current_price,
                        'reason': f"수익 목표 달성 ({pnl_pct:.1%})"
                    })
                    continue
            
            # 3. 시간 기반 청산 조건 확인
            if position['max_holding_days'] and holding_days >= position['max_holding_days']:
                exit_signals.append({
                    'symbol': position['symbol'],
                    'strategy': strategy_name,
                    'position_type': position_type,
                    'exit_type': 'time_based',
                    'exit_price': current_price,
                    'reason': f"최대 보유일 도달 ({holding_days}일)"
                })
                continue
            
            # 4. 추격 역지정가 조건 확인 (별도 처리 필요)
            if 'trailing_stop' in exit_conditions:
                # 추격 역지정가 로직은 RiskManager에서 처리
                pass
        
        return exit_signals
    """
#    def get_total_portfolio_value(self) -> float:
#        """전체 포트폴리오 가치 계산"""
    """        if self.positions.empty:
            return 100000  # 기본값
        return self.positions['market_value'].sum()
    
    def close_position(self, symbol: str, position_type: str, strategy: str, 
                      close_price: Optional[float] = None, 
                      exit_reason: str = "manual") -> Tuple[bool, Dict]:"""
#        """포지션 청산 및 거래 기록 반환"""
    """
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
                'holding_days': (datetime.now() - pd.to_datetime(position['entry_date'])).days
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
            return False, {}"""
    
    def update_positions(self) -> bool:
        """모든 포지션의 현재가 및 손익 업데이트"""
        if self.positions.empty:
            return True
        
        try:
            updated_count = 0
            for idx, position in self.positions.iterrows():
                current_price = self.get_current_price(position['symbol'])
                
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
                    entry_date = pd.to_datetime(position['entry_date'])
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
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """현재가 가져오기"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")
            if not hist.empty:
                return hist['Close'].iloc[-1]
        except Exception:
            pass
        return None
    
    def record_closed_position(self, position: pd.Series, close_price: float, 
                             realized_pnl: float, realized_pnl_pct: float):
        """청산된 포지션을 히스토리에 기록"""
        try:
            history_record = {
                'symbol': position['symbol'],
                'position_type': position['position_type'],
                'quantity': position['quantity'],
                'entry_price': position['entry_price'],
                'entry_date': position['entry_date'],
                'close_price': close_price,
                'close_date': datetime.now().strftime('%Y-%m-%d'),
                'holding_days': (datetime.now() - pd.to_datetime(position['entry_date'])).days,
                'realized_pnl': realized_pnl,
                'realized_pnl_pct': realized_pnl_pct,
                'strategy': position['strategy'],
                'weight': position['weight'],
                'entry_order_type': position.get('entry_order_type', 'market'),
                'stop_loss_price': position.get('stop_loss_price'),
                'profit_target_price': position.get('profit_target_price')
            }
            
            # 히스토리 파일 로드 또는 생성
            if os.path.exists(self.history_file):
                history_df = pd.read_csv(self.history_file)
            else:
                history_df = pd.DataFrame()
            
            # 새 기록 추가
            history_df = pd.concat([history_df, pd.DataFrame([history_record])], ignore_index=True)
            history_df.to_csv(self.history_file, index=False, encoding='utf-8-sig')
            
        except Exception as e:
            print(f"⚠️ 히스토리 기록 실패: {e}")
    
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
            
        except Exception as e:
            print(f"⚠️ 성과 지표 계산 실패: {e}")