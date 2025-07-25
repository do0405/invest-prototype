import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import yfinance as yf


def use_local_only() -> bool:
    return os.getenv("USE_LOCAL_DATA_ONLY") == "1"

from config import DATA_US_DIR


class PriceCalculator:
    """가격 계산 및 조회 유틸리티"""

    @staticmethod
    def get_current_price(symbol: str) -> Optional[float]:
        """현재가 반환"""

        if use_local_only():
            path = os.path.join(DATA_US_DIR, f"{symbol.upper()}.csv")
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path)
                    if not df.empty:
                        return float(df.iloc[-1]['Close'])
                except Exception:
                    pass
            return None

        try:
            hist = yf.Ticker(symbol).history(period="1d")
            if not hist.empty:
                return float(hist['Close'].iloc[-1])
        except Exception:
            pass
        return None

    @staticmethod
    def parse_price(price_str) -> Optional[float]:
        """문자열 가격을 실수로 변환"""
        try:
            if pd.isna(price_str) or price_str in ['없음', '시장가']:
                return None
            price_clean = re.sub(r'[^0-9.-]', '', str(price_str))
            return float(price_clean) if price_clean else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def calculate_stop_loss_price(entry_price: float, strategy_config: Dict, position_type: str = 'LONG') -> Optional[float]:
        """손절가 계산"""
        try:
            exit_conditions = strategy_config.get('exit_conditions', {})
            for condition in exit_conditions:
                if isinstance(condition, dict):
                    ctype = condition.get('type')
                    if ctype == 'stop_loss_percent':
                        percent = condition.get('value', 0)
                        return entry_price * (1 - percent / 100) if position_type == 'LONG' else entry_price * (1 + percent / 100)
                    if ctype == 'stop_loss_price':
                        return condition.get('value')

            default_pct = strategy_config.get('stop_loss_pct', 2.0)
            return entry_price * (1 - default_pct / 100) if position_type == 'LONG' else entry_price * (1 + default_pct / 100)
        except Exception as e:
            print(f"⚠️ 손절가 계산 실패: {e}")
            return None

    @staticmethod
    def calculate_profit_target_price(entry_price: float, strategy_config: Dict, position_type: str = 'LONG') -> Optional[float]:
        """목표가 계산"""
        try:
            exit_conditions = strategy_config.get('exit_conditions', {})
            for condition in exit_conditions:
                if isinstance(condition, dict):
                    ctype = condition.get('type')
                    if ctype == 'take_profit_percent':
                        percent = condition.get('value', 0)
                        return entry_price * (1 + percent / 100) if position_type == 'LONG' else entry_price * (1 - percent / 100)
                    if ctype == 'take_profit_price':
                        return condition.get('value')

            default_pct = strategy_config.get('profit_target_pct', 4.0)
            return entry_price * (1 + default_pct / 100) if position_type == 'LONG' else entry_price * (1 - default_pct / 100)
        except Exception as e:
            print(f"⚠️ 목표가 계산 실패: {e}")
            return None

    @staticmethod
    def calculate_return_percentage(entry_price: float, current_price: float, position_type: str = 'LONG') -> float:
        """수익률 계산"""
        try:
            if position_type == 'LONG':
                return (current_price - entry_price) / entry_price * 100
            return (entry_price - current_price) / entry_price * 100
        except Exception:
            return 0.0


    @staticmethod
    def get_recent_price_data(symbol: str, days: int = 5) -> Optional[Dict[str, float]]:
        """최근 가격 데이터 조회"""
        if use_local_only():
            path = os.path.join(DATA_US_DIR, f"{symbol.upper()}.csv")
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    df_recent = df.tail(days)
                    if not df_recent.empty:
                        latest = df_recent.iloc[-1]
                        return {
                            'high': float(latest['High']),
                            'low': float(latest['Low']),
                            'close': float(latest['Close']),
                            'open': float(latest['Open']),
                            'volume': float(latest['Volume'])
                        }
                except Exception:
                    pass
            return None

        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            hist = yf.Ticker(symbol).history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
            if hist.empty:
                return None
            latest = hist.iloc[-1]
            return {
                'high': float(latest['High']),
                'low': float(latest['Low']),
                'close': float(latest['Close']),
                'open': float(latest['Open']),
                'volume': float(latest['Volume'])
            }
        except Exception as e:
            print(f"⚠️ {symbol} 가격 데이터 조회 실패: {e}")
            return None

    @staticmethod
    def get_next_day_open_price(symbol: str, purchase_date: str) -> Optional[float]:
        """매수일 다음날 시가 반환"""

        purchase_dt = datetime.strptime(purchase_date, '%Y-%m-%d')
        next_day = purchase_dt + timedelta(days=1)

        if use_local_only():
            path = os.path.join(DATA_US_DIR, f"{symbol.upper()}.csv")
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path)
                    df['date'] = pd.to_datetime(df['date']).dt.date
                    for i in range(5):
                        target = next_day.date() + timedelta(days=i)
                        row = df[df['date'] == target]
                        if not row.empty:
                            return float(row.iloc[0]['Open'])
                except Exception:
                    pass
            return None

        try:

            for i in range(5):
                check_date = next_day + timedelta(days=i)
                end_date = check_date + timedelta(days=1)
                hist = yf.Ticker(symbol).history(start=check_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
                if not hist.empty:
                    return float(hist['Open'].iloc[0])
            return None
        except Exception as e:
            print(f"⚠️ {symbol} 다음날 시가 조회 실패: {e}")
            return None

