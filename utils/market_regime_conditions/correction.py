__all__ = ["check_correction_conditions"]
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

def check_correction_conditions(index_data: Dict[str, pd.DataFrame]) -> Tuple[bool, Dict]:
    """조정장 필수조건 및 부가조건을 검사합니다.
    
    필수조건:
    - 주요 지수 2개 이상이 50일 이동평균선 하회
    - 주요 지수 고점 대비 -5% ~ -15% 조정
    - 조정이 1주일 이상 지속
    """
    essential_conditions = []
    additional_conditions = []
    details = {}
    
    main_indices = ['SPY', 'QQQ', 'IWM', 'MDY']
    
    # 필수조건 1: 주요 지수 2개 이상이 50일 이동평균선 하회
    below_ma50_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            latest = df.iloc[-1]
            if latest['close'] <= latest['ma50']:
                below_ma50_count += 1
    
    essential_1 = below_ma50_count >= 2
    essential_conditions.append(essential_1)
    details['indices_below_ma50'] = below_ma50_count
    
    # 필수조건 2: 고점 대비 -5% ~ -15% 조정
    correction_range_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            if len(df) >= 60:  # 최근 3개월 데이터
                current_price = df.iloc[-1]['close']
                recent_high = df.iloc[-60:]['close'].max()
                drawdown = (current_price - recent_high) / recent_high
                if -0.15 <= drawdown <= -0.05:
                    correction_range_count += 1
    
    essential_2 = correction_range_count >= 2
    essential_conditions.append(essential_2)
    details['correction_range_count'] = correction_range_count
    
    # 필수조건 3: 조정이 일정 기간 이상 지속

    correction_duration_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            days_below = count_consecutive_below_ma(df, 'ma50')
            if days_below >= MARKET_REGIME_CRITERIA['correction_min_days']:

                correction_duration_count += 1

    essential_3 = correction_duration_count >= 2
    essential_conditions.append(essential_3)
    details['correction_duration_met'] = essential_3

    details['correction_duration_count'] = correction_duration_count
    
    # 부가조건들
    # VIX 25-35 구간
    vix_strength = 0.0
    if 'VIX' in index_data and index_data['VIX'] is not None:
        vix_value = index_data['VIX'].iloc[-1]['close']

        vix_condition = 25 <= vix_value <= 35
    additional_conditions.append(vix_condition)
    details['vix_elevated'] = vix_condition
    
    # Put/Call Ratio 0.9-1.2
    pc_ratio = calculate_put_call_ratio()
    pc_condition = 0.9 <= pc_ratio <= 1.2
    additional_conditions.append(pc_condition)
    details['put_call_elevated'] = pc_condition
    details['put_call_ratio'] = pc_ratio

    # High-Low Index 30-50
    hl_index = calculate_high_low_index(index_data)
    hl_condition = 30 <= hl_index <= 50
    additional_conditions.append(hl_condition)
    details['high_low_weak'] = hl_condition
    details['high_low_index'] = hl_index

    # Advance-Decline Line 하락 추세
    ad_trend = calculate_advance_decline_trend(index_data)
    ad_condition = ad_trend < 0
    additional_conditions.append(ad_condition)
    details['ad_line_declining'] = ad_condition
    details['ad_trend'] = ad_trend

    # 단기 반등 후 재하락 패턴
    retest_condition = False
    if 'SPY' in index_data and index_data['SPY'] is not None:
        df = index_data['SPY']
        if len(df) >= 10:
            retest_condition = df['close'].iloc[-5] > df['close'].iloc[-10] and df['close'].iloc[-1] < df['close'].iloc[-5]
    additional_conditions.append(retest_condition)
    details['retest_after_bounce'] = retest_condition
    
    # 필수조건 모두 충족 여부
    essential_met = all(essential_conditions)
    
    # 부가조건 60% 이상 충족 여부
    additional_met_ratio = sum(additional_conditions) / len(additional_conditions) if additional_conditions else 0
    additional_met = additional_met_ratio >= 0.6
    
    # 최종 판단
    regime_met = essential_met and additional_met
    
    details.update({
        'essential_conditions_met': essential_met,
        'additional_conditions_ratio': additional_met_ratio,
        'additional_conditions_met': additional_met,
        'regime_qualified': regime_met
    })
    
    return regime_met, details


