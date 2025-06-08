import yfinance as yf
from typing import Optional, Dict, Any

class PriceCalculator:
    """가격 계산 유틸리티 클래스"""
    
    @staticmethod
    def get_current_price(symbol: str) -> Optional[float]:
        """현재가 가져오기"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")
            if not hist.empty:
                return hist['Close'].iloc[-1]
        except Exception:
            pass
        return None
    
    @staticmethod
    def calculate_stop_loss_price(entry_price: float, strategy_config: Dict, position_type: str = 'LONG') -> Optional[float]:
        """손절가 계산"""
        try:
            exit_conditions = strategy_config.get('exit_conditions', {})
            
            # 손절 조건 찾기
            for condition in exit_conditions:
                if isinstance(condition, dict):
                    condition_type = condition.get('type')
                    
                    # 퍼센트 기반 손절
                    if condition_type == 'stop_loss_percent':
                        percent = condition.get('value', 0)
                        if position_type == 'LONG':
                            return entry_price * (1 - percent / 100)
                        else:  # SHORT
                            return entry_price * (1 + percent / 100)
                    
                    # 고정 가격 손절
                    elif condition_type == 'stop_loss_price':
                        return condition.get('value')
            
            # 기본 손절률 (2%)
            default_stop_loss_pct = strategy_config.get('stop_loss_pct', 2.0)
            if position_type == 'LONG':
                return entry_price * (1 - default_stop_loss_pct / 100)
            else:  # SHORT
                return entry_price * (1 + default_stop_loss_pct / 100)
                
        except Exception as e:
            print(f"⚠️ 손절가 계산 실패: {e}")
            return None
    
    @staticmethod
    def calculate_profit_target_price(entry_price: float, strategy_config: Dict, position_type: str = 'LONG') -> Optional[float]:
        """목표가 계산"""
        try:
            exit_conditions = strategy_config.get('exit_conditions', {})
            
            # 익절 조건 찾기
            for condition in exit_conditions:
                if isinstance(condition, dict):
                    condition_type = condition.get('type')
                    
                    # 퍼센트 기반 익절
                    if condition_type == 'take_profit_percent':
                        percent = condition.get('value', 0)
                        if position_type == 'LONG':
                            return entry_price * (1 + percent / 100)
                        else:  # SHORT
                            return entry_price * (1 - percent / 100)
                    
                    # 고정 가격 익절
                    elif condition_type == 'take_profit_price':
                        return condition.get('value')
            
            # 기본 익절률 (4%)
            default_profit_pct = strategy_config.get('profit_target_pct', 4.0)
            if position_type == 'LONG':
                return entry_price * (1 + default_profit_pct / 100)
            else:  # SHORT
                return entry_price * (1 - default_profit_pct / 100)
                
        except Exception as e:
            print(f"⚠️ 목표가 계산 실패: {e}")
            return None
    
    @staticmethod
    def calculate_return_percentage(entry_price: float, current_price: float, position_type: str = 'LONG') -> float:
        """수익률 계산"""
        try:
            if position_type == 'LONG':
                return (current_price - entry_price) / entry_price * 100
            else:  # SHORT
                return (entry_price - current_price) / entry_price * 100
        except Exception:
            return 0.0
    
    @staticmethod
    def get_price_data(symbol: str, period: str = "1d") -> Optional[Dict[str, Any]]:
        """가격 데이터 조회"""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period)
            if not hist.empty:
                latest = hist.iloc[-1]
                return {
                    'open': latest['Open'],
                    'high': latest['High'],
                    'low': latest['Low'],
                    'close': latest['Close'],
                    'volume': latest['Volume']
                }
        except Exception as e:
            print(f"⚠️ 가격 데이터 조회 실패 ({symbol}): {e}")
            return None