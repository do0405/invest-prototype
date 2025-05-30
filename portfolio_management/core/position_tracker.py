# 포지션 추적 모듈 (Position Tracker)
# 실시간 포지션 상태 추적 및 자동 데이터 업데이트

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import traceback

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from config import DATA_US_DIR, RESULTS_VER2_DIR
from utils import ensure_dir

class Position:
    """개별 포지션 클래스"""
    
    def __init__(self, symbol: str, strategy: str, entry_date: str, entry_price: float, 
                 quantity: int, position_type: str = 'long', stop_loss: float = None,
                 profit_protection: float = None):
        self.symbol = symbol
        self.strategy = strategy
        self.entry_date = pd.to_datetime(entry_date)
        self.entry_price = entry_price
        self.quantity = quantity
        self.position_type = position_type  # 'long' or 'short'
        self.stop_loss = stop_loss
        self.profit_protection = profit_protection
        self.current_price = entry_price
        self.current_value = entry_price * quantity
        self.unrealized_pnl = 0.0
        self.unrealized_pnl_pct = 0.0
        self.holding_days = 0
        self.last_update = datetime.now()
        
    def update_price(self, new_price: float) -> None:
        """현재가 업데이트"""
        self.current_price = new_price
        self.current_value = new_price * self.quantity
        
        if self.position_type == 'long':
            self.unrealized_pnl = (new_price - self.entry_price) * self.quantity
        else:  # short
            self.unrealized_pnl = (self.entry_price - new_price) * self.quantity
            
        self.unrealized_pnl_pct = (self.unrealized_pnl / (self.entry_price * self.quantity)) * 100
        self.holding_days = (datetime.now() - self.entry_date).days
        self.last_update = datetime.now()
        
    def update_stop_loss(self, new_stop_loss: float) -> None:
        """손절가 업데이트 (Trailing Stop)"""
        if self.position_type == 'long':
            # 롱 포지션: 손절가는 상승만 가능
            if new_stop_loss > self.stop_loss:
                self.stop_loss = new_stop_loss
        else:  # short
            # 숏 포지션: 손절가는 하락만 가능
            if new_stop_loss < self.stop_loss:
                self.stop_loss = new_stop_loss
                
    def should_stop_out(self) -> bool:
        """손절 조건 확인"""
        if self.stop_loss is None:
            return False
            
        if self.position_type == 'long':
            return self.current_price <= self.stop_loss
        else:  # short
            return self.current_price >= self.stop_loss
            
    def to_dict(self) -> Dict:
        """포지션 정보를 딕셔너리로 변환"""
        return {
            'symbol': self.symbol,
            'strategy': self.strategy,
            'entry_date': self.entry_date.strftime('%Y-%m-%d'),
            'entry_price': self.entry_price,
            'current_price': self.current_price,
            'quantity': self.quantity,
            'position_type': self.position_type,
            'current_value': self.current_value,
            'unrealized_pnl': self.unrealized_pnl,
            'unrealized_pnl_pct': self.unrealized_pnl_pct,
            'stop_loss': self.stop_loss,
            'profit_protection': self.profit_protection,
            'holding_days': self.holding_days,
            'last_update': self.last_update.strftime('%Y-%m-%d %H:%M:%S')
        }

class PositionTracker:
    """포지션 추적 관리자"""
    
    def __init__(self):
        self.positions: Dict[str, Position] = {}  # symbol -> Position
        self.closed_positions: List[Dict] = []  # 청산된 포지션 기록
        
    def add_position(self, symbol: str, strategy: str, entry_date: str, 
                    entry_price: float, quantity: int, position_type: str = 'long',
                    stop_loss: float = None, profit_protection: float = None) -> bool:
        """새 포지션 추가"""
        try:
            if symbol in self.positions:
                print(f"⚠️ {symbol} 포지션이 이미 존재합니다.")
                return False
                
            position = Position(
                symbol=symbol,
                strategy=strategy,
                entry_date=entry_date,
                entry_price=entry_price,
                quantity=quantity,
                position_type=position_type,
                stop_loss=stop_loss,
                profit_protection=profit_protection
            )
            
            self.positions[symbol] = position
            print(f"✅ {symbol} 포지션 추가됨: {position_type} {quantity}주 @ ${entry_price:.2f}")
            return True
            
        except Exception as e:
            print(f"❌ {symbol} 포지션 추가 오류: {e}")
            return False
            
    def close_position(self, symbol: str, exit_price: float, exit_date: str = None) -> bool:
        """포지션 청산"""
        try:
            if symbol not in self.positions:
                print(f"⚠️ {symbol} 포지션을 찾을 수 없습니다.")
                return False
                
            position = self.positions[symbol]
            
            if exit_date is None:
                exit_date = datetime.now().strftime('%Y-%m-%d')
                
            # 최종 손익 계산
            if position.position_type == 'long':
                realized_pnl = (exit_price - position.entry_price) * position.quantity
            else:  # short
                realized_pnl = (position.entry_price - exit_price) * position.quantity
                
            realized_pnl_pct = (realized_pnl / (position.entry_price * position.quantity)) * 100
            holding_days = (pd.to_datetime(exit_date) - position.entry_date).days
            
            # 청산 기록 저장
            closed_position = {
                'symbol': symbol,
                'strategy': position.strategy,
                'entry_date': position.entry_date.strftime('%Y-%m-%d'),
                'exit_date': exit_date,
                'entry_price': position.entry_price,
                'exit_price': exit_price,
                'quantity': position.quantity,
                'position_type': position.position_type,
                'realized_pnl': realized_pnl,
                'realized_pnl_pct': realized_pnl_pct,
                'holding_days': holding_days,
                'stop_loss': position.stop_loss,
                'closed_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.closed_positions.append(closed_position)
            
            # 활성 포지션에서 제거
            del self.positions[symbol]
            
            print(f"✅ {symbol} 포지션 청산: ${exit_price:.2f}, 손익: ${realized_pnl:.2f} ({realized_pnl_pct:.2f}%)")
            return True
            
        except Exception as e:
            print(f"❌ {symbol} 포지션 청산 오류: {e}")
            return False
            
    def update_all_prices(self) -> None:
        """모든 포지션의 현재가 업데이트"""
        print("📊 포지션 가격 업데이트 중...")
        
        updated_count = 0
        for symbol, position in self.positions.items():
            try:
                current_price = self._get_latest_price(symbol)
                if current_price is not None:
                    position.update_price(current_price)
                    updated_count += 1
                else:
                    print(f"⚠️ {symbol} 가격 데이터를 가져올 수 없습니다.")
                    
            except Exception as e:
                print(f"❌ {symbol} 가격 업데이트 오류: {e}")
                
        print(f"✅ {updated_count}/{len(self.positions)}개 포지션 가격 업데이트 완료")
        
    def _get_latest_price(self, symbol: str) -> Optional[float]:
        """특정 종목의 최신 가격 가져오기"""
        try:
            file_path = os.path.join(DATA_US_DIR, f'{symbol}.csv')
            
            if not os.path.exists(file_path):
                return None
                
            df = pd.read_csv(file_path)
            df.columns = [col.lower() for col in df.columns]
            
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                
            if df.empty or 'close' not in df.columns:
                return None
                
            return float(df.iloc[-1]['close'])
            
        except Exception as e:
            print(f"❌ {symbol} 가격 데이터 로드 오류: {e}")
            return None
            
    def get_portfolio_summary(self) -> Dict:
        """포트폴리오 요약 정보"""
        if not self.positions:
            return {
                'total_positions': 0,
                'total_value': 0.0,
                'total_unrealized_pnl': 0.0,
                'total_unrealized_pnl_pct': 0.0,
                'long_positions': 0,
                'short_positions': 0
            }
            
        total_value = sum(pos.current_value for pos in self.positions.values())
        total_unrealized_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())
        total_cost = sum(pos.entry_price * pos.quantity for pos in self.positions.values())
        
        long_positions = sum(1 for pos in self.positions.values() if pos.position_type == 'long')
        short_positions = sum(1 for pos in self.positions.values() if pos.position_type == 'short')
        
        return {
            'total_positions': len(self.positions),
            'total_value': total_value,
            'total_cost': total_cost,
            'total_unrealized_pnl': total_unrealized_pnl,
            'total_unrealized_pnl_pct': (total_unrealized_pnl / total_cost * 100) if total_cost > 0 else 0.0,
            'long_positions': long_positions,
            'short_positions': short_positions,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    def get_positions_dataframe(self) -> pd.DataFrame:
        """포지션을 DataFrame으로 반환"""
        if not self.positions:
            return pd.DataFrame()
            
        data = [pos.to_dict() for pos in self.positions.values()]
        return pd.DataFrame(data)
        
    def check_stop_losses(self) -> List[str]:
        """손절 조건 확인 및 해당 종목 리스트 반환"""
        stop_out_symbols = []
        
        for symbol, position in self.positions.items():
            if position.should_stop_out():
                stop_out_symbols.append(symbol)
                print(f"🚨 {symbol} 손절 조건 충족: 현재가 ${position.current_price:.2f}, 손절가 ${position.stop_loss:.2f}")
                
        return stop_out_symbols
        
    def get_strategy_summary(self) -> Dict[str, Dict]:
        """전략별 포지션 요약"""
        strategy_summary = {}
        
        for position in self.positions.values():
            strategy = position.strategy
            
            if strategy not in strategy_summary:
                strategy_summary[strategy] = {
                    'count': 0,
                    'total_value': 0.0,
                    'total_unrealized_pnl': 0.0,
                    'long_count': 0,
                    'short_count': 0
                }
                
            strategy_summary[strategy]['count'] += 1
            strategy_summary[strategy]['total_value'] += position.current_value
            strategy_summary[strategy]['total_unrealized_pnl'] += position.unrealized_pnl
            
            if position.position_type == 'long':
                strategy_summary[strategy]['long_count'] += 1
            else:
                strategy_summary[strategy]['short_count'] += 1
                
        return strategy_summary