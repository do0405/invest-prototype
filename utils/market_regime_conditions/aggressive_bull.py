__all__ = ["check_aggressive_bull_conditions"]
from typing import Dict, Tuple
import pandas as pd
from .common import (
    MARKET_REGIME_CRITERIA,
    calculate_high_low_index,
    calculate_advance_decline_trend,
    calculate_put_call_ratio,
    calculate_ma_distance
)

def check_aggressive_bull_conditions(index_data: Dict[str, pd.DataFrame]) -> Tuple[bool, Dict]:
    """공격적 상승장 필수조건 및 부가조건을 검사합니다.
    
    필수조건:
    - S&P 500, QQQ, IWM, MDY 모두 50일 이동평균 위에서 거래
    - 4개 주요 지수 모두 200일 이동평균 대비 +5% 이상
    - 바이오텍 지수 (IBB, XBI) 상승세 지속 (월간 +3% 이상)
    
    부가조건 (70% 이상 충족 시 확인):
    - VIX < 20
    - Put/Call Ratio < 0.7
    - High-Low Index > 70
    - Advance-Decline Line 상승 추세
    - 소형주(IWM)가 대형주(SPY) 대비 아웃퍼폼
    """
    essential_conditions = []
    additional_conditions = []
    details = {}
    
    # 필수조건 1: 주요 4개 지수 모두 50일 이동평균 위
    main_indices = ['SPY', 'QQQ', 'IWM', 'MDY']
    above_ma50_count = 0
    
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            latest = df.iloc[-1]
            above_ma50 = latest['close'] > latest['ma50']
            if above_ma50:
                above_ma50_count += 1
    
    essential_1 = above_ma50_count == len(main_indices)
    essential_conditions.append(essential_1)
    details['all_above_ma50'] = essential_1
    

    # 필수조건 2: 4개 주요 지수 모두 200일 이동평균 대비 일정 비율 이상
    above_ma200_distance = 0


    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            distance = calculate_ma_distance(df, 'ma200')
            if distance >= MARKET_REGIME_CRITERIA['ma200_distance_pct']:
                above_ma200_distance += 1

    essential_2 = above_ma200_distance == len(main_indices)
    essential_conditions.append(essential_2)
    details['ma200_distance_count'] = above_ma200_distance
    details['ma200_distance_requirement_met'] = essential_2

    
    # 필수조건 3: 바이오텍 지수 상승세 (월간 3% 이상)
    biotech_positive = False
    biotech_tickers = ['IBB', 'XBI']
    
    for ticker in biotech_tickers:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            if len(df) >= 22:  # 약 1개월 데이터
                current_price = df.iloc[-1]['close']
                month_ago_price = df.iloc[-22]['close']
                monthly_return = (current_price - month_ago_price) / month_ago_price
                if monthly_return >= 0.03:  # 3% 이상
                    biotech_positive = True
                    break
    
    essential_conditions.append(biotech_positive)
    details['biotech_positive'] = biotech_positive
    
    # 부가조건들
    # VIX < 20
    vix_strength = 0.0
    vix_condition = False
    if 'VIX' in index_data and index_data['VIX'] is not None:
        vix_value = index_data['VIX'].iloc[-1]['close']
        vix_condition = vix_value < 20
    additional_conditions.append(vix_condition)
    details['vix_below_20'] = vix_condition
    
    # Put/Call Ratio < 0.7
    pc_ratio = calculate_put_call_ratio()
    put_call_condition = pc_ratio < 0.7
    additional_conditions.append(put_call_condition)
    details['put_call_low'] = put_call_condition
    details['put_call_ratio'] = pc_ratio
    
    # High-Low Index > 70
    hl_index = calculate_high_low_index(index_data)
    high_low_condition = hl_index > 70
    additional_conditions.append(high_low_condition)
    details['high_low_strong'] = high_low_condition
    details['high_low_index'] = hl_index
    
    # Advance-Decline Line 상승 추세
    ad_trend = calculate_advance_decline_trend(index_data)
    ad_line_condition = ad_trend > 0
    additional_conditions.append(ad_line_condition)
    details['ad_line_rising'] = ad_line_condition
    details['ad_trend'] = ad_trend
    
    # 소형주(IWM) vs 대형주(SPY) 아웃퍼폼
    iwm_outperform = False
    if 'IWM' in index_data and 'SPY' in index_data:
        if index_data['IWM'] is not None and index_data['SPY'] is not None:
            iwm_df = index_data['IWM']
            spy_df = index_data['SPY']
            if len(iwm_df) >= 22 and len(spy_df) >= 22:
                iwm_return = (iwm_df.iloc[-1]['close'] - iwm_df.iloc[-22]['close']) / iwm_df.iloc[-22]['close']
                spy_return = (spy_df.iloc[-1]['close'] - spy_df.iloc[-22]['close']) / spy_df.iloc[-22]['close']
                iwm_outperform = iwm_return > spy_return
    additional_conditions.append(iwm_outperform)
    details['iwm_outperform'] = iwm_outperform
    
    # 필수조건 모두 충족 여부
    essential_met = all(essential_conditions)
    
    # 부가조건 70% 이상 충족 여부
    additional_met_ratio = sum(additional_conditions) / len(additional_conditions) if additional_conditions else 0
    additional_met = additional_met_ratio >= 0.7
    
    # 최종 판단
    regime_met = essential_met and additional_met
    
    details.update({
        'essential_conditions_met': essential_met,
        'additional_conditions_ratio': additional_met_ratio,
        'additional_conditions_met': additional_met,
        'regime_qualified': regime_met
    })
    
    return regime_met, details


