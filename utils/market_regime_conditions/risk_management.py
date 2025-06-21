__all__ = ["check_risk_management_conditions"]
from typing import Dict, Tuple
import pandas as pd
from .common import (
    MARKET_REGIME_CRITERIA,
    calculate_high_low_index,
    calculate_advance_decline_trend,
    calculate_put_call_ratio,
    calculate_ma_distance,
    count_consecutive_below_ma
)

def check_risk_management_conditions(index_data: Dict[str, pd.DataFrame]) -> Tuple[bool, Dict]:
    """위험 관리장 필수조건 및 부가조건을 검사합니다.
    
    필수조건:
    - 주요 지수 3개 이상이 200일 이동평균선 하회
    - 주요 지수 고점 대비 -15% ~ -25% 조정
    - 개별 주식 20-30% 조정 빈발
    """
    essential_conditions = []
    additional_conditions = []
    details = {}
    
    main_indices = ['SPY', 'QQQ', 'IWM', 'MDY']
    
    # 필수조건 1: 주요 지수 3개 이상이 200일 이동평균선 하회
    below_ma200_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            latest = df.iloc[-1]
            if latest['close'] <= latest['ma200']:
                below_ma200_count += 1
    
    essential_1 = below_ma200_count >= 3
    essential_conditions.append(essential_1)
    details['indices_below_ma200'] = below_ma200_count
    
    # 필수조건 2: 고점 대비 -15% ~ -25% 조정
    deep_correction_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            if len(df) >= 60:
                current_price = df.iloc[-1]['close']
                recent_high = df.iloc[-60:]['close'].max()
                drawdown = (current_price - recent_high) / recent_high
                if -0.25 <= drawdown <= -0.15:
                    deep_correction_count += 1
    
    essential_2 = deep_correction_count >= 2
    essential_conditions.append(essential_2)
    details['deep_correction_count'] = deep_correction_count
    
    # 필수조건 3: 개별 주식 조정 빈발 (임시로 True)
    individual_stock_correction = True
    essential_conditions.append(individual_stock_correction)
    details['individual_stock_correction'] = individual_stock_correction
    
    # 부가조건들
    # VIX > 35
    vix_strength = 0.0
    if 'VIX' in index_data and index_data['VIX'] is not None:
        vix_value = index_data['VIX'].iloc[-1]['close']

        vix_condition = vix_value > 35
    additional_conditions.append(vix_condition)
    details['vix_high'] = vix_condition

    # Put/Call Ratio > 1.2
    pc_ratio = calculate_put_call_ratio()
    pc_condition = pc_ratio > 1.2
    additional_conditions.append(pc_condition)
    details['put_call_very_high'] = pc_condition
    details['put_call_ratio'] = pc_ratio

    # High-Low Index < 30
    hl_index = calculate_high_low_index(index_data)
    hl_condition = hl_index < 30
    additional_conditions.append(hl_condition)
    details['high_low_very_weak'] = hl_condition
    details['high_low_index'] = hl_index

    # Advance-Decline Line 급락
    ad_trend = calculate_advance_decline_trend(index_data)
    ad_condition = ad_trend <= -20
    additional_conditions.append(ad_condition)
    details['ad_line_plunge'] = ad_condition
    details['ad_trend'] = ad_trend

    # 이동평균선 역배열 (50일 < 200일)
    ma_inversion = False
    if 'SPY' in index_data and index_data['SPY'] is not None:
        df = index_data['SPY']
        latest = df.iloc[-1]
        ma_inversion = latest['ma50'] < latest['ma200']
    additional_conditions.append(ma_inversion)
    details['ma_inversion'] = ma_inversion
    
    # 필수조건 모두 충족 여부
    essential_met = all(essential_conditions)
    
    # 부가조건 50% 이상 충족 여부
    additional_met_ratio = sum(additional_conditions) / len(additional_conditions) if additional_conditions else 0
    additional_met = additional_met_ratio >= 0.5
    
    # 최종 판단
    regime_met = essential_met and additional_met
    
    details.update({
        'essential_conditions_met': essential_met,
        'additional_conditions_ratio': additional_met_ratio,
        'additional_conditions_met': additional_met,
        'regime_qualified': regime_met
    })
    
    return regime_met, details


