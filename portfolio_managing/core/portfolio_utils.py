import yfinance as yf
import pandas as pd
import os
from .price_calculator import PriceCalculator
from datetime import datetime
from typing import Dict, Tuple
class PortfolioUtils:
    """포트폴리오 유틸리티 클래스"""
    
    def __init__(self, portfolio_manager):
        self.pm = portfolio_manager
    
   
    def get_portfolio_summary(self) -> Dict:
        """포트폴리오 요약 정보 반환"""
        try:
            position_summary = self.pm.position_tracker.get_portfolio_summary()
            positions = self.pm.position_tracker.positions
            risk_summary = self.pm.risk_manager.get_risk_summary(positions)
            performance = self.pm.position_tracker.get_performance_metrics()
            
            # get_strategy_summary() 대신 기존 데이터로 전략 요약 생성
            strategy_summary = {}
            if not positions.empty:
                for strategy in positions['strategy'].unique():
                    strategy_positions = positions[positions['strategy'] == strategy]
                    strategy_summary[strategy] = {
                        'positions': len(strategy_positions),
                        'market_value': strategy_positions['market_value'].sum(),
                        'unrealized_pnl': strategy_positions['unrealized_pnl'].sum()
                    }
            
            # get_portfolio_value() 대신 기존 데이터 활용
            current_value = position_summary.get('total_market_value', self.pm.initial_capital)
            
            summary = {
                'portfolio_name': self.pm.portfolio_name,
                'initial_capital': self.pm.initial_capital,
                'current_value': current_value,
                'total_return': current_value - self.pm.initial_capital,
                'total_return_pct': (current_value / self.pm.initial_capital - 1) * 100,
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
            portfolio_value = self.pm.position_tracker.get_portfolio_value()
            position_size = self.pm.risk_manager.calculate_position_size(
                portfolio_value=portfolio_value,
                strategy_config=strategy_config,
                signal=signal
            )
            if position_size <= 0:
                return False
            
            # 포지션 추가
            entry_price = signal.get('price', 0)
            position_type = strategy_config.get('type', 'LONG')
            position_data = {
                'symbol': symbol,
                'strategy': strategy_name,
                'position_type': position_type,
                'entry_price': entry_price,
                'quantity': position_size,
                'entry_date': datetime.now().strftime('%Y-%m-%d'),
                'stop_loss': PriceCalculator.calculate_stop_loss_price(
                    entry_price, strategy_config, position_type
                ),
                'take_profit': PriceCalculator.calculate_profit_target_price(
                    entry_price, strategy_config, position_type
                )
            }
            
            # 포지션 추가
            position_added = self.pm.position_tracker.add_position(position_data)
            
            # 트레일링 스탑 초기화
            if position_added:
                # 전략 설정에서 트레일링 스탑 비율 가져오기
                exit_conditions = strategy_config.get('exit_conditions', {})
                trailing_stop_config = exit_conditions.get('trailing_stop', {})
                trailing_pct = trailing_stop_config.get('trailing_pct', 0.0)
                
                # 트레일링 스탑 비율이 설정되어 있으면 초기화
                if trailing_pct > 0:
                    self.pm.trailing_stop_manager.initialize_trailing_stop(
                        symbol=symbol,
                        position_type=position_type,
                        strategy=strategy_name,
                        entry_price=entry_price,
                        entry_date=position_data['entry_date'],
                        trailing_pct=trailing_pct
                    )
            
            return position_added
            
        except Exception as e:
            print(f"⚠️ 포지션 추가 실패: {e}")
            return False


  
    def check_and_process_exit_conditions(self):
        """청산 조건 확인 및 처리"""
        try:
            # self.pm.position_tracker.get_positions() 대신 positions 속성 직접 접근
            positions = self.pm.position_tracker.positions
            if positions.empty:
                return
            
            positions_to_close = []
            
            for idx, position in positions.iterrows():
                symbol = position['symbol']
                current_price = PriceCalculator.get_current_price(symbol)
                
                if current_price is None:
                    continue
                
                # 현재가 업데이트
                positions.loc[idx, 'current_price'] = current_price
                
                # 청산 조건 확인
                should_close, reason = self.check_exit_condition(position, current_price)
                
                if should_close:
                    return_pct = PriceCalculator.calculate_return_percentage(
                        position['entry_price'], current_price, position['position_type']
                    )
                    positions_to_close.append(
                        (idx, symbol, position['strategy'], reason, return_pct)
                    )
            
            # 청산 처리 - PositionTracker의 close_position 메서드 사용
            for idx, symbol, strategy, reason, return_pct in positions_to_close:
                position = positions.iloc[idx]
                position_type = position['position_type']
                current_price = PriceCalculator.get_current_price(symbol)
    
                success, trade_record = self.pm.position_tracker.close_position(
                    symbol=symbol,
                    position_type=position_type,
                    strategy=strategy,
                    close_price=current_price,
                    exit_reason=reason
                )
                
                # 트레일링 스탑 항목 제거
                self.pm.trailing_stop_manager.remove_trailing_stop(symbol, position_type, strategy)

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
    
    
    def record_trade(self, trade_record: Dict):
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

    def log_exit_transaction(self, symbol: str, position_type: str, purchase_price: float,
                              exit_price: float, return_pct: float, exit_reason: str):
        """청산 거래 기록"""
        try:
            log_file = os.path.join(self.pm.portfolio_dir, f"{self.pm.portfolio_name}_exit_log.csv")
            new_record = {
                '청산일시': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '종목명': symbol,
                '포지션': position_type,
                '매수가': purchase_price,
                '청산가': exit_price,
                '수익률': f"{return_pct:.2f}%",
                '청산사유': exit_reason
            }
            if os.path.exists(log_file):
                df = pd.read_csv(log_file)
                df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
            else:
                df = pd.DataFrame([new_record])
            df.to_csv(log_file, index=False)
            print(f"  📝 청산 기록 저장: {log_file}")
        except Exception as e:
            print(f"⚠️ 청산 기록 저장 실패: {e}")
