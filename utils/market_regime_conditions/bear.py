__all__ = ["check_bear_conditions"]
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

def check_bear_conditions(index_data: Dict[str, pd.DataFrame]) -> Tuple[bool, Dict]:
    """완전한 약세장 필수조건 및 부가조건을 검사합니다.
    
    필수조건:
    - 모든 주요 지수가 200일 이동평균선 하회
    - 주요 지수 고점 대비 -25% 이상 하락
    - 하락 추세가 2개월 이상 지속
    """
    essential_conditions = []
    additional_conditions = []
    details = {}
    
    main_indices = ['SPY', 'QQQ', 'IWM', 'MDY']
    
    # 필수조건 1: 모든 주요 지수가 200일 이동평균선 하회
    below_ma200_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            latest = df.iloc[-1]
            if latest['close'] <= latest['ma200']:
                below_ma200_count += 1
    
    essential_1 = below_ma200_count == len(main_indices)
    essential_conditions.append(essential_1)
    details['all_below_ma200'] = essential_1
    
    # 필수조건 2: 고점 대비 -25% 이상 하락
    severe_decline_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            if len(df) >= 60:
                current_price = df.iloc[-1]['close']
                recent_high = df.iloc[-60:]['close'].max()
                drawdown = (current_price - recent_high) / recent_high
                if drawdown <= -0.25:
                    severe_decline_count += 1
    
    essential_2 = severe_decline_count >= 3
    essential_conditions.append(essential_2)
    details['severe_decline_count'] = severe_decline_count
    

    # 필수조건 3: 하락 추세가 일정 기간 이상 지속
    downtrend_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            days_below = count_consecutive_below_ma(df, 'ma200')
            if days_below >= MARKET_REGIME_CRITERIA['bear_trend_min_days']:
                downtrend_count += 1

    prolonged_decline = downtrend_count >= 3
    essential_conditions.append(prolonged_decline)
    details['prolonged_decline'] = prolonged_decline
    details['downtrend_count'] = downtrend_count

    
    # 부가조건들
    # VIX > 40
    vix_strength = 0.0
    if 'VIX' in index_data and index_data['VIX'] is not None:
        vix_value = index_data['VIX'].iloc[-1]['close']
        vix_condition = vix_value > 40
    additional_conditions.append(vix_condition)
    details['vix_extreme'] = vix_condition

    # Put/Call Ratio > 1.5
    pc_ratio = calculate_put_call_ratio()
    pc_condition = pc_ratio > 1.5
    additional_conditions.append(pc_condition)
    details['put_call_extreme'] = pc_condition
    details['put_call_ratio'] = pc_ratio

    # High-Low Index < 20
    hl_index = calculate_high_low_index(index_data)
    hl_condition = hl_index < 20
    additional_conditions.append(hl_condition)
    details['high_low_collapse'] = hl_condition
    details['high_low_index'] = hl_index

    # Advance-Decline Line 지속적 하락
    ad_trend = calculate_advance_decline_trend(index_data)
    ad_condition = ad_trend <= -20
    additional_conditions.append(ad_condition)
    details['ad_line_decline'] = ad_condition
    details['ad_trend'] = ad_trend
    
    # 바이오텍 지수 급락
    biotech_crash = 0.0
    for ticker in ['IBB', 'XBI']:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            if len(df) >= 60:
                current_price = df.iloc[-1]['close']
                high_price = df.iloc[-60:]['close'].max()
                decline = (current_price - high_price) / high_price
                if decline <= -0.30:
                    biotech_crash = 1.0
                    break
    additional_conditions.append(biotech_crash)
    details['biotech_crash'] = biotech_crash
    
    # 필수조건 모두 충족 여부
    essential_met = all(essential_conditions)
    
    # 부가조건 확인 (약세장은 확인 지표로만 사용)
    additional_met_ratio = sum(additional_conditions) / len(additional_conditions) if additional_conditions else 0
    
    # 최종 판단
    regime_met = essential_met
    
    details.update({
        'essential_conditions_met': essential_met,
        'additional_conditions_ratio': additional_met_ratio,
        'regime_qualified': regime_met
    })
    
    return regime_met, details


