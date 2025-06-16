import os
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd

from .price_calculator import PriceCalculator
from .strategy_config import StrategyConfig
from .portfolio_utils import PortfolioUtils
from .trailing_stop import TrailingStopManager


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
            remaining_days = original_days - days_held
            # 남은 일수가 0 이하면 -1 반환하여 포트폴리오에서 제거하도록 함
            return remaining_days
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




def check_single_condition(condition_str: str, purchase_price: float, current_price: float, purchase_date: str, recent_data: Dict = None, position_type: str = 'BUY') -> Tuple[bool, str]:
    """단일 조건 확인
    
    Args:
        condition_str: 조건 문자열
        purchase_price: 매수가
        current_price: 현재가
        purchase_date: 매수일
        recent_data: 최근 데이터
        position_type: 포지션 타입 (BUY 또는 SELL)
        
    Returns:
        (조건 충족 여부, 조건 충족 이유)
    """
    try:
        # 남은 일수 기반 조건 확인
        if '일 후' in condition_str:
            days_left = calculate_remaining_days(purchase_date, condition_str)
            if days_left <= 0:
                return True, f"보유 기간 만료 ({condition_str})"
            return False, ""
        
        # ATR 상단 또는 특정 가격 조건 확인
        price_match = re.search(r'(\d+\.\d+)\s*\(', condition_str)
        if price_match and recent_data:
            target_price = float(price_match.group(1))
            
            # 롱 포지션은 고가 확인, 숏 포지션은 저가 확인
            if position_type == 'BUY' and recent_data.get('high', 0) >= target_price:
                return True, f"목표가 {target_price:.2f} 도달 (고가: {recent_data.get('high', 0):.2f})"
            elif position_type == 'SELL' and recent_data.get('low', 0) <= target_price:
                return True, f"목표가 {target_price:.2f} 도달 (저가: {recent_data.get('low', 0):.2f})"
            return False, ""
        
        # 가격/퍼센트 기반 조건 확인
        target_price = None
        
        # 퍼센트 기반 조건 (예: 5% 수익)
        if '%' in condition_str:
            percent = float(condition_str.replace('%', '').strip())
            if position_type == 'BUY':
                target_price = purchase_price * (1 + percent / 100)
            else:  # SELL
                target_price = purchase_price * (1 - percent / 100)
        else:
            # 직접적인 가격 조건
            try:
                target_price = float(condition_str.strip())
            except ValueError:
                # 숫자로 변환할 수 없는 경우
                return False, ""
        
        if target_price is not None:
            # 고가/저가 확인 (recent_data가 있는 경우)
            if recent_data:
                if position_type == 'BUY' and recent_data.get('high', 0) >= target_price:
                    return True, f"목표가 {target_price:.2f} 도달 (고가: {recent_data.get('high', 0):.2f})"
                elif position_type == 'SELL' and recent_data.get('low', 0) <= target_price:
                    return True, f"목표가 {target_price:.2f} 도달 (저가: {recent_data.get('low', 0):.2f})"
            # 현재가 확인 (recent_data가 없는 경우)
            else:
                if position_type == 'BUY' and current_price >= target_price:
                    return True, f"목표가 {target_price:.2f} 도달 (현재가: {current_price:.2f})"
                elif position_type == 'SELL' and current_price <= target_price:
                    return True, f"목표가 {target_price:.2f} 도달 (현재가: {current_price:.2f})"
                
        return False, ""
    except Exception as e:
        print(f"❌ 단일 조건 확인 실패: {e}")
        return False, ""


def check_legacy_complex_conditions(row, recent_data, position_type: str = 'BUY') -> Tuple[bool, str]:
    """기본 복합 청산 조건 확인 (5% 손절, 10% 익절)"""
    try:
        symbol = row['종목명']
        purchase_price = PriceCalculator.parse_price(row['매수가'])
        current_price = recent_data.get('close', 0) if recent_data else 0
        high_price = recent_data.get('high', 0) if recent_data else 0
        low_price = recent_data.get('low', 0) if recent_data else 0
        
        if not purchase_price or not current_price:
            return False, ""
        
        # 수익률 계산
        if position_type == 'BUY':
            # 고가 기준 수익률 계산 (익절용)
            high_return_pct = (high_price - purchase_price) / purchase_price * 100
            # 종가 기준 수익률 계산 (손절용)
            close_return_pct = (current_price - purchase_price) / purchase_price * 100
            
            # 5% 손절 (종가 기준)
            if close_return_pct <= -5:
                return True, f"손절매 조건 충족 (수익률: {close_return_pct:.2f}%)"
            
            # 10% 익절 (고가 기준)
            if high_return_pct >= 10:
                return True, f"익절 조건 충족 (고가 기준 수익률: {high_return_pct:.2f}%)"
        else:  # SELL 포지션
            # 저가 기준 수익률 계산 (익절용)
            low_return_pct = (purchase_price - low_price) / purchase_price * 100
            # 종가 기준 수익률 계산 (손절용)
            close_return_pct = (purchase_price - current_price) / purchase_price * 100
            
            # 5% 손절 (종가 기준)
            if close_return_pct <= -5:
                return True, f"손절매 조건 충족 (수익률: {close_return_pct:.2f}%)"
            
            # 10% 익절 (저가 기준)
            if low_return_pct >= 10:
                return True, f"익절 조건 충족 (저가 기준 수익률: {low_return_pct:.2f}%)"
        
        return False, ""
    except Exception as e:
        print(f"❌ 기본 복합 청산 조건 확인 실패 ({symbol}): {e}")
        return False, ""


def check_strategy_based_exit_conditions(row, recent_data, position_type: str, exit_conditions: Dict, trailing_stop_manager: TrailingStopManager = None) -> Tuple[bool, str]:
    """전략 기반 청산 조건 확인"""
    try:
        symbol = row['종목명']
        purchase_price = PriceCalculator.parse_price(row['매수가'])
        current_price = recent_data.get('close', 0) if recent_data else 0
        high_price = recent_data.get('high', 0) if recent_data else 0
        low_price = recent_data.get('low', 0) if recent_data else 0
        purchase_date = row.get('매수일', '')
        strategy_name = row.get('전략', '')
        
        if not purchase_price or not current_price or not purchase_date:
            return False, ""
        
        # 트레일링 스탑 조건 확인
        trailing_stop = exit_conditions.get('trailing_stop', {})
        if trailing_stop and trailing_stop_manager:
            # 트레일링 스탑 타입 확인
            if trailing_stop.get('type') == 'trailing_stop':
                trailing_pct = trailing_stop.get('trailing_pct', 0.1)  # 기본값 10%
                
                # 트레일링 스탑 업데이트 (최고가/최저가 갱신 시 스탑 가격 조정)
                trailing_stop_manager.update_trailing_stop(symbol, position_type, strategy_name, current_price)
                
                # 트레일링 스탑 도달 여부 확인
                is_hit, stop_price = trailing_stop_manager.check_trailing_stop_hit(symbol, position_type, strategy_name, current_price)
                
                if is_hit:
                    return True, f"트레일링 스탑 도달 (스탑 가격: {stop_price:.2f}, 현재가: {current_price:.2f})"
        
        # 손절매 조건 확인
        stop_loss = exit_conditions.get('stop_loss', {})
        if stop_loss:
            # 가격 기반 손절매
            if 'price' in stop_loss:
                target_price = stop_loss['price']
                if position_type == 'BUY' and current_price <= target_price:
                    return True, f"손절매 가격 도달 (목표: {target_price:.2f}, 현재: {current_price:.2f})"
                elif position_type == 'SELL' and current_price >= target_price:
                    return True, f"손절매 가격 도달 (목표: {target_price:.2f}, 현재: {current_price:.2f})"
            
            # 퍼센트 기반 손절매
            if 'price_percent' in stop_loss:
                percent = stop_loss['price_percent']
                if position_type == 'BUY':
                    target_price = purchase_price * (1 - percent / 100)
                    if current_price <= target_price:
                        return True, f"손절매 퍼센트 도달 ({percent}%, 목표: {target_price:.2f}, 현재: {current_price:.2f})"
                else:  # SELL
                    target_price = purchase_price * (1 + percent / 100)
                    if current_price >= target_price:
                        return True, f"손절매 퍼센트 도달 ({percent}%, 목표: {target_price:.2f}, 현재: {current_price:.2f})"
        
        # 익절 조건 확인
        profit_taking = exit_conditions.get('profit_taking', {})
        if profit_taking:
            # 가격 기반 익절
            if 'price' in profit_taking:
                target_price = profit_taking['price']
                # 롱 포지션은 고가 확인, 숏 포지션은 저가 확인
                if position_type == 'BUY' and high_price >= target_price:
                    return True, f"익절 가격 도달 (목표: {target_price:.2f}, 고가: {high_price:.2f})"
                elif position_type == 'SELL' and low_price <= target_price:
                    return True, f"익절 가격 도달 (목표: {target_price:.2f}, 저가: {low_price:.2f})"
            
            # 퍼센트 기반 익절
            if 'price_percent' in profit_taking:
                percent = profit_taking['price_percent']
                if position_type == 'BUY':
                    target_price = purchase_price * (1 + percent / 100)
                    # 고가 기준 확인
                    if high_price >= target_price:
                        return True, f"익절 퍼센트 도달 ({percent}%, 목표: {target_price:.2f}, 고가: {high_price:.2f})"
                else:  # SELL
                    target_price = purchase_price * (1 - percent / 100)
                    # 저가 기준 확인
                    if low_price <= target_price:
                        return True, f"익절 퍼센트 도달 ({percent}%, 목표: {target_price:.2f}, 저가: {low_price:.2f})"
        
        # 시간 기반 청산 조건 확인
        time_based = exit_conditions.get('time_based', {})
        if time_based and 'max_days' in time_based:
            max_days = time_based['max_days']
            purchase_datetime = datetime.strptime(purchase_date, '%Y-%m-%d')
            current_datetime = datetime.now()
            days_held = (current_datetime - purchase_datetime).days
            
            if days_held >= max_days:
                return True, f"최대 보유 기간 도달 ({days_held}일/{max_days}일)"
        
        return False, ""
    except Exception as e:
        print(f"❌ 전략 기반 청산 조건 확인 실패 ({symbol}): {e}")
        return False, ""


def check_complex_exit_condition(row, recent_data, position_type: str = 'BUY', trailing_stop_manager: TrailingStopManager = None) -> Tuple[bool, str]:
    """복합 청산 조건 확인"""
    try:
        symbol = row['종목명']
        purchase_price = PriceCalculator.parse_price(row['매수가'])
        purchase_date = row.get('매수일', '')
        strategy_name = row.get('전략', '')
        if not purchase_price or not purchase_date:
            return False, ""
            
        # ATR 상단 또는 특정 가격 조건 확인
        profit_taking_str = str(row.get('차익실현', ''))
        price_match = re.search(r'(\d+\.\d+)\s*\(', profit_taking_str)
        
        if price_match and recent_data:
            target_price = float(price_match.group(1))
            
            # 롱 포지션은 고가 확인, 숏 포지션은 저가 확인
            if position_type == 'BUY' and recent_data.get('high', 0) >= target_price:
                return True, f"목표가 {target_price:.2f} 도달 (고가: {recent_data['high']:.2f})"
            elif position_type == 'SELL' and recent_data.get('low', 0) <= target_price:
                return True, f"목표가 {target_price:.2f} 도달 (저가: {recent_data['low']:.2f})"

        # 전략 기반 조건 확인
        strategy_config = StrategyConfig.get_strategy_config(strategy_name)
        if strategy_config:
            exit_conditions = strategy_config.get('exit_conditions', {})
            return check_strategy_based_exit_conditions(row, recent_data, position_type, exit_conditions, trailing_stop_manager)

        # 기본 조건 확인
        return check_legacy_complex_conditions(row, recent_data, position_type)
    except Exception as e:
        print(f"❌ 복합 청산 조건 확인 실패 ({symbol}): {e}")
        return False, ""

