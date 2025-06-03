import yfinance as yf
import pandas as pd
import os
from datetime import datetime
from typing import Dict, Optional, Tuple
from .strategy_config import StrategyConfig

class PortfolioUtils:
    """포트폴리오 유틸리티 클래스"""
    
    def __init__(self, portfolio_manager):
        self.pm = portfolio_manager
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """현재가 조회 (매수일 다음날 시가 반영)"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="2d")
            
            if len(hist) > 0:
                return float(hist['Open'].iloc[-1])
            return None
                
        except Exception as e:
            print(f"⚠️ {symbol} 현재가 조회 실패: {e}")
            return None
    
    def get_portfolio_value(self) -> float:
        """현재 포트폴리오 총 가치 계산"""
        try:
            positions = self.pm.position_tracker.positions
            if positions.empty:
                return self.pm.initial_capital
            return positions['market_value'].sum()
        except Exception:
            return self.pm.initial_capital
    
    def get_strategy_summary(self) -> Dict:
        """전략별 포트폴리오 요약"""
        try:
            positions = self.pm.position_tracker.positions
            if positions.empty:
                return {}
            
            strategy_summary = {}
            for strategy_name in positions['strategy'].unique():
                strategy_positions = positions[positions['strategy'] == strategy_name]
                # StrategyConfig 사용으로 변경
                strategy_config = StrategyConfig.get_strategy_config(strategy_name)
                
                total_value = strategy_positions['market_value'].sum()
                total_pnl = strategy_positions['unrealized_pnl'].sum()
                position_count = len(strategy_positions)
                
                strategy_summary[strategy_name] = {
                    'name': strategy_config.get('name', strategy_name),
                    'type': strategy_config.get('type', 'UNKNOWN'),
                    'position_count': position_count,
                    'total_value': total_value,
                    'total_pnl': total_pnl,
                    'weight': total_value / self.get_portfolio_value() if self.get_portfolio_value() > 0 else 0,
                    'avg_pnl_pct': (total_pnl / (total_value - total_pnl)) * 100 if (total_value - total_pnl) > 0 else 0
                }
            
            return strategy_summary
        except Exception as e:
            print(f"⚠️ 전략별 요약 생성 실패: {e}")
            return {}
    
    def get_portfolio_summary(self) -> Dict:
        """포트폴리오 종합 요약"""
        try:
            position_summary = self.pm.position_tracker.get_portfolio_summary()
            positions = self.pm.position_tracker.positions
            risk_summary = self.pm.risk_manager.get_risk_summary(positions)
            performance = self.pm.position_tracker.get_performance_metrics()
            strategy_summary = self.get_strategy_summary()
            
            summary = {
                'portfolio_name': self.pm.portfolio_name,
                'initial_capital': self.pm.initial_capital,
                'current_value': self.get_portfolio_value(),
                'total_return': self.get_portfolio_value() - self.pm.initial_capital,
                'total_return_pct': (self.get_portfolio_value() / self.pm.initial_capital - 1) * 100,
                'positions': position_summary,
                'risk': risk_summary,
                'performance': performance,
                'strategies': strategy_summary,
                'active_strategies': self.pm.config.get('strategies', []),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return summary
        except Exception as e:
            print(f"⚠️ 포트폴리오 요약 생성 실패: {e}")
            return {}
    
    def add_position_from_signal(self, strategy_name: str, signal: pd.Series, strategy_config: Dict) -> bool:
        """신호로부터 포지션 추가"""
        try:
            symbol = signal.get('symbol')
            if not symbol:
                return False
            
            # 중복 포지션 확인
            if self.pm.position_tracker.has_position(symbol, strategy_name):
                return False
            
            # 포지션 크기 계산
            position_size = self.calculate_position_size(signal, strategy_config)
            if position_size <= 0:
                return False
            
            # 포지션 추가
            position_data = {
                'symbol': symbol,
                'strategy': strategy_name,
                'position_type': strategy_config.get('type', 'LONG'),
                'entry_price': signal.get('price', 0),
                'quantity': position_size,
                'entry_date': datetime.now().strftime('%Y-%m-%d'),
                'stop_loss': self.calculate_stop_loss(signal, strategy_config),
                'take_profit': self.calculate_take_profit(signal, strategy_config)
            }
            
            return self.pm.position_tracker.add_position(position_data)
            
        except Exception as e:
            print(f"⚠️ 포지션 추가 실패: {e}")
            return False
    
    def calculate_position_size(self, signal: pd.Series, strategy_config: Dict) -> float:
        """포지션 크기 계산"""
        try:
            risk_per_position = strategy_config.get('risk_per_position', 0.02)
            max_position_size = strategy_config.get('max_position_size', 0.10)
            
            # 리스크 기반 포지션 크기
            risk_amount = self.get_portfolio_value() * risk_per_position
            price = signal.get('price', 0)
            
            if price <= 0:
                return 0
            
            # 최대 포지션 크기 제한
            max_amount = self.get_portfolio_value() * max_position_size
            position_amount = min(risk_amount, max_amount)
            
            return position_amount / price
            
        except Exception:
            return 0
    
    def calculate_stop_loss(self, signal: pd.Series, strategy_config: Dict) -> float:
        """손절가 계산"""
        try:
            price = signal.get('price', 0)
            stop_loss_pct = strategy_config.get('stop_loss', 0.05)
            
            if strategy_config.get('type') == 'LONG':
                return price * (1 - stop_loss_pct)
            else:
                return price * (1 + stop_loss_pct)
        except Exception:
            return 0
    
    def calculate_take_profit(self, signal: pd.Series, strategy_config: Dict) -> float:
        """익절가 계산"""
        try:
            price = signal.get('price', 0)
            take_profit_pct = strategy_config.get('take_profit', 0.10)
            
            if strategy_config.get('type') == 'LONG':
                return price * (1 + take_profit_pct)
            else:
                return price * (1 - take_profit_pct)
        except Exception:
            return 0
    
    def check_and_process_exit_conditions(self):
        """청산 조건 확인 및 처리"""
        try:
            positions = self.pm.position_tracker.positions
            if positions.empty:
                return
            
            print("🔍 청산 조건 확인 중...")
            positions_to_close = []
            
            for idx, position in positions.iterrows():
                symbol = position['symbol']
                current_price = self.get_current_price(symbol)
                
                if current_price is None:
                    continue
                
                # 현재가 업데이트
                positions.loc[idx, 'current_price'] = current_price
                
                # 청산 조건 확인
                should_close, reason = self.check_exit_condition(position, current_price)
                
                if should_close:
                    return_pct = self.calculate_return_pct(position, current_price)
                    positions_to_close.append((idx, symbol, position['strategy'], reason, return_pct))
            
            # 청산 처리
            for idx, symbol, strategy, reason, return_pct in positions_to_close:
                self.close_position(idx, symbol, strategy, reason, return_pct)
            
            # 포지션 파일 저장
            self.pm.position_tracker.save_positions()
            
        except Exception as e:
            print(f"❌ 청산 조건 처리 실패: {e}")
    
    def check_exit_condition(self, position: pd.Series, current_price: float) -> Tuple[bool, str]:
        """청산 조건 확인"""
        try:
            entry_price = position['entry_price']
            position_type = position['position_type']
            
            # 수익률 계산
            if position_type == 'LONG':
                return_pct = (current_price - entry_price) / entry_price * 100
            else:
                return_pct = (entry_price - current_price) / entry_price * 100
            
            # 4% 익절
            if return_pct >= 4.0:
                return True, "4% 익절"
            
            # 5% 익절
            if return_pct >= 5.0:
                return True, "5% 익절"
            
            # 손절 조건
            stop_loss = position.get('stop_loss', 0)
            if stop_loss > 0:
                if position_type == 'LONG' and current_price <= stop_loss:
                    return True, "손절"
                elif position_type == 'SHORT' and current_price >= stop_loss:
                    return True, "손절"
            
            # 최대 보유일 확인
            entry_date = pd.to_datetime(position['entry_date'])
            holding_days = (datetime.now() - entry_date).days
            
            if holding_days >= 30:  # 30일 최대 보유
                return True, "최대 보유일 도달"
            
            return False, ""
            
        except Exception:
            return False, ""
    
    def calculate_return_pct(self, position: pd.Series, current_price: float) -> float:
        """수익률 계산"""
        try:
            entry_price = position['entry_price']
            position_type = position['position_type']
            
            if position_type == 'LONG':
                return (current_price - entry_price) / entry_price * 100
            else:
                return (entry_price - current_price) / entry_price * 100
        except Exception:
            return 0.0
    
    def close_position(self, idx: int, symbol: str, strategy: str, reason: str, return_pct: float):
        """포지션 청산"""
        try:
            position = self.pm.position_tracker.positions.loc[idx]
            
            print(f"🔄 포지션 청산: {symbol} ({strategy}) - {reason} (수익률: {return_pct:.2f}%)")
            
            # 거래 기록 저장
            trade_record = {
                'symbol': symbol,
                'strategy': strategy,
                'entry_date': position['entry_date'],
                'exit_date': datetime.now().strftime('%Y-%m-%d'),
                'entry_price': position['entry_price'],
                'exit_price': position['current_price'],
                'quantity': position['quantity'],
                'return_pct': return_pct,
                'exit_reason': reason,
                'holding_days': position.get('holding_days', 0)
            }
            
            # 거래 기록을 히스토리에 추가
            self.add_trade_to_history(trade_record)
            
            # 포지션 제거
            self.pm.position_tracker.positions = self.pm.position_tracker.positions.drop(idx).reset_index(drop=True)
            
        except Exception as e:
            print(f"❌ 포지션 청산 처리 실패: {e}")
    
    def add_trade_to_history(self, trade_record: Dict):
        """거래 기록을 히스토리에 추가"""
        try:
            history_file = os.path.join(self.pm.portfolio_dir, f'{self.pm.portfolio_name}_trade_history.csv')
            
            # 기존 히스토리 로드
            if os.path.exists(history_file):
                history_df = pd.read_csv(history_file)
            else:
                history_df = pd.DataFrame()
            
            # 새 거래 기록 추가
            new_record_df = pd.DataFrame([trade_record])
            history_df = pd.concat([history_df, new_record_df], ignore_index=True)
            
            # 히스토리 저장
            history_df.to_csv(history_file, index=False, encoding='utf-8-sig')
            
        except Exception as e:
            print(f"⚠️ 거래 기록 저장 실패: {e}")