import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd

from .strategy_config import StrategyConfig
from .portfolio_utils import PortfolioUtils
from .price_calculator import PriceCalculator


def calculate_profit_target_price(row) -> Optional[float]:
    """n% 수익 목표가 계산"""
    try:
        if row['매수가'] == '시장가':
            return None
        purchase_price = float(row['매수가'])
        match = re.search(r'(\d+)% 수익', str(row['차익실현']))
        if match:
            pct = float(match.group(1)) / 100
            return purchase_price * (1 + pct)
    except Exception as e:
        print(f"⚠️ 수익 목표가 계산 실패: {e}")
    return None


def calculate_remaining_days(purchase_date: str, exit_condition: str) -> int:
    """남은 보유일 계산"""
    try:
        purchase_dt = datetime.strptime(purchase_date, '%Y-%m-%d')
        current_dt = datetime.now()
        days_held = (current_dt - purchase_dt).days
        match = re.search(r'(\d+)일 후', str(exit_condition))
        if match:
            original_days = int(match.group(1))
            return original_days - days_held
        return 0
    except Exception as e:
        print(f"⚠️ 남은 일수 계산 실패: {e}")
        return 0


def update_days_condition(original_condition: str, remaining_days: int) -> str:
    """일수 조건 업데이트"""
    try:
        return re.sub(r'\d+일 후', f'{remaining_days}일 후', str(original_condition))
    except Exception as e:
        print(f"⚠️ 일수 조건 업데이트 실패: {e}")
        return original_condition


def should_check_exit_from_next_day(purchase_date: str) -> bool:
    """매수일 다음날부터 조건 확인 여부"""
    try:
        purchase_dt = datetime.strptime(purchase_date, '%Y-%m-%d')
        next_day = purchase_dt + timedelta(days=1)
        return datetime.now().date() >= next_day.date()
    except Exception as e:
        print(f"⚠️ 날짜 확인 실패: {e}")
        return True


def parse_complex_condition(condition_str: str, purchase_date: str) -> dict:
    """복합 청산 조건 파싱"""
    try:
        result = {
            'price': None,
            'days_remaining': None,
            'original_condition': str(condition_str),
            'has_or_condition': False
        }
        if pd.isna(condition_str) or condition_str == '없음':
            return result

        condition = str(condition_str)
        parts = condition.split('또는') if '또는' in condition else [condition]
        if '또는' in condition:
            result['has_or_condition'] = True

        for part in parts:
            part = part.strip()
            percent_match = re.search(r'(\d+(?:\.\d+)?)%', part)
            if percent_match:
                result['price_percent'] = float(percent_match.group(1))
            else:
                price_match = re.search(r'(\d+(?:\.\d+)?)', part)
                if price_match and '일' not in part:
                    result['price'] = float(price_match.group(1))

            days_match = re.search(r'(\d+)일\s*후', part)
            if days_match:
                remaining = calculate_remaining_days(purchase_date, part)
                result['days_remaining'] = remaining

        return result
    except Exception as e:
        print(f"⚠️ 복합 조건 파싱 실패: {e}")
        return {'price': None, 'days_remaining': None, 'original_condition': str(condition_str)}


def check_single_condition(condition: dict, purchase_price: float, current_price: float, condition_type: str, position_type: str = 'BUY') -> bool:
    """단일 조건 확인"""
    try:
        if condition.get('days_remaining') is not None and condition['days_remaining'] <= 0:
            return True
        if condition.get('has_or_condition') and condition.get('days_remaining') is not None and condition['days_remaining'] <= 0:
            return True

        if condition.get('price') and current_price:
            target = condition['price']
            if position_type == 'BUY':
                if condition_type in ['stop_loss', 'profit_protection']:
                    return current_price <= target
                if condition_type == 'profit_taking':
                    return current_price >= target
            else:
                if condition_type in ['stop_loss', 'profit_protection']:
                    return current_price >= target
                if condition_type == 'profit_taking':
                    return current_price <= target

        if condition.get('price_percent') and purchase_price and current_price:
            percent = condition['price_percent']
            if position_type == 'BUY':
                if condition_type == 'stop_loss':
                    return current_price <= purchase_price * (1 - percent / 100)
                if condition_type == 'profit_taking':
                    return current_price >= purchase_price * (1 + percent / 100)
            else:
                if condition_type == 'stop_loss':
                    return current_price >= purchase_price * (1 + percent / 100)
                if condition_type == 'profit_taking':
                    return current_price <= purchase_price * (1 - percent / 100)
        return False
    except Exception as e:
        print(f"⚠️ 단일 조건 확인 실패: {e}")
        return False


def check_legacy_complex_conditions(row, recent_data, position_type: str):
    """기존 복합 조건 로직"""
    try:
        symbol = row['종목명']
        current_price = PriceCalculator.get_current_price(symbol)
        purchase_price = PriceCalculator.parse_price(row['매수가'])
        if not current_price or not purchase_price:
            return False, ""

        basic_sl = {'price_percent': 5}
        basic_tp = {'price_percent': 10}
        if check_single_condition(basic_sl, purchase_price, current_price, 'stop_loss', position_type):
            return True, "기본 5% 손절매"
        if check_single_condition(basic_tp, purchase_price, current_price, 'profit_taking', position_type):
            return True, "기본 10% 익절"
        return False, ""
    except Exception as e:
        print(f"⚠️ 기존 조건 확인 실패: {e}")
        return False, ""


def check_strategy_based_exit_conditions(row, recent_data, position_type: str, exit_conditions):
    """전략별 청산 조건 확인"""
    try:
        symbol = row['종목명']
        temp_position = pd.Series({
            'entry_price': PriceCalculator.parse_price(row['매수가']),
            'position_type': 'LONG' if position_type == 'BUY' else 'SHORT',
            'entry_date': row.get('매수일', ''),
            'stop_loss': row.get('손절가', 0)
        })
        current_price = PriceCalculator.get_current_price(symbol)
        return PortfolioUtils(None).check_exit_condition(temp_position, current_price)
    except Exception as e:
        print(f"❌ 전략별 청산 조건 확인 실패: {e}")
        return False, ""


def check_complex_exit_condition(row, recent_data, position_type: str = 'BUY') -> Tuple[bool, str]:
    """복합 청산 조건 확인"""
    try:
        symbol = row['종목명']
        purchase_price = PriceCalculator.parse_price(row['매수가'])
        purchase_date = row.get('매수일', '')
        strategy_name = row.get('전략', '')
        if not purchase_price or not purchase_date:
            return False, ""

        strategy_config = StrategyConfig.get_strategy_config(strategy_name)
        if strategy_config:
            exit_conditions = strategy_config.get('exit_conditions', {})
            return check_strategy_based_exit_conditions(row, recent_data, position_type, exit_conditions)

        return check_legacy_complex_conditions(row, recent_data, position_type)
    except Exception as e:
        print(f"❌ 복합 청산 조건 확인 실패 ({symbol}): {e}")
        return False, ""

