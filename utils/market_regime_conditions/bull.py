__all__ = ["check_bull_conditions"]
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

def check_bull_conditions(index_data: Dict[str, pd.DataFrame]) -> Tuple[bool, Dict]:
    """상승장 필수조건 및 부가조건을 검사합니다.
    
    필수조건:
    - S&P 500, QQQ는 50일 이동평균 위 유지
    - 중소형주 (IWM, MDY) 중 하나 이상이 50일 이동평균 하회 시작
    - 바이오텍 지수 상승 모멘텀 둔화 (월간 수익률 0~3%)
    """
    essential_conditions = []
    additional_conditions = []
    details = {}
    
    # 필수조건 1: SPY, QQQ 50일 이동평균 위
    large_cap_above_ma50 = 0
    for ticker in ['SPY', 'QQQ']:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            latest = df.iloc[-1]
            if latest['close'] > latest['ma50']:
                large_cap_above_ma50 += 1
    
    essential_1 = large_cap_above_ma50 == 2
    essential_conditions.append(essential_1)
    details['large_cap_above_ma50'] = essential_1
    
    # 필수조건 2: 중소형주 중 하나 이상이 50일 이동평균 하회
    mid_small_below_ma50 = 0
    for ticker in ['IWM', 'MDY']:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            latest = df.iloc[-1]
            if latest['close'] <= latest['ma50']:
                mid_small_below_ma50 += 1

    essential_2 = mid_small_below_ma50 >= 1
    essential_conditions.append(essential_2)
    details['mid_small_below_ma50_count'] = mid_small_below_ma50
    
    # 필수조건 3: 바이오텍 지수 모멘텀 둔화 (0~3%)
    biotech_momentum_slow = False
    for ticker in ['IBB', 'XBI']:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            if len(df) >= 22:
                current_price = df.iloc[-1]['close']
                month_ago_price = df.iloc[-22]['close']
                monthly_return = (current_price - month_ago_price) / month_ago_price
                if 0 <= monthly_return <= 0.03:
                    biotech_momentum_slow = True
                    break
    
    essential_conditions.append(biotech_momentum_slow)
    details['biotech_momentum_slow'] = biotech_momentum_slow
    
    # 부가조건들 (60% 이상 충족)
    # VIX 20-25 구간
    vix_condition = 0.0
    if 'VIX' in index_data and index_data['VIX'] is not None:
        vix_value = index_data['VIX'].iloc[-1]['close']
        vix_condition = 1.0 if 20 <= vix_value <= 25 else 0.0
    additional_conditions.append(vix_condition)
    details['vix_moderate'] = vix_condition

    
    # Put/Call Ratio 0.7-0.9
    pc_ratio = calculate_put_call_ratio()
    pc_condition = 0.7 <= pc_ratio <= 0.9
    additional_conditions.append(pc_condition)
    details['put_call_moderate'] = pc_condition
    details['put_call_ratio'] = pc_ratio

    # High-Low Index 50-70
    hl_index = calculate_high_low_index(index_data)
    hl_condition = 50 <= hl_index <= 70
    additional_conditions.append(hl_condition)
    details['high_low_moderate'] = hl_condition
    details['high_low_index'] = hl_index

    # 대형주 > 중형주 > 소형주 상대적 강세
    leadership_condition = False
    if all(ticker in index_data and index_data[ticker] is not None for ticker in ['SPY', 'MDY', 'IWM']):
        spy_df = index_data['SPY']
        mdy_df = index_data['MDY']
        iwm_df = index_data['IWM']
        if len(spy_df) >= 22 and len(mdy_df) >= 22 and len(iwm_df) >= 22:
            spy_ret = (spy_df.iloc[-1]['close'] - spy_df.iloc[-22]['close']) / spy_df.iloc[-22]['close']
            mdy_ret = (mdy_df.iloc[-1]['close'] - mdy_df.iloc[-22]['close']) / mdy_df.iloc[-22]['close']
            iwm_ret = (iwm_df.iloc[-1]['close'] - iwm_df.iloc[-22]['close']) / iwm_df.iloc[-22]['close']
            leadership_condition = spy_ret > mdy_ret > iwm_ret
    additional_conditions.append(leadership_condition)
    details['large_cap_leadership'] = leadership_condition

    # Advance-Decline Line 횡보 또는 완만한 상승
    ad_trend = calculate_advance_decline_trend(index_data)
    ad_condition = ad_trend >= -5
    additional_conditions.append(ad_condition)
    details['ad_line_flat_up'] = ad_condition
    details['ad_trend'] = ad_trend

    
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


