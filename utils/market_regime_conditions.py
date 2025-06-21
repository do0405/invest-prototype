"""시장 국면 판단을 위한 필수조건 및 부가조건 검사 모듈"""

from typing import Dict, Tuple
import pandas as pd

from .market_regime_helpers import (
    calculate_put_call_ratio,
    calculate_high_low_index,
    calculate_advance_decline_trend,
)

from config import MARKET_REGIME_CRITERIA
from .market_regime_helpers import (
    calculate_high_low_index,
    calculate_advance_decline_trend,
    calculate_put_call_ratio,
)


def _strength_above(value: float, threshold: float) -> float:
    """Return normalized strength for values above ``threshold``."""
    if threshold == 0:
        return 0.0
    return max(0.0, min(1.0, (value - threshold) / abs(threshold)))


def _strength_below(value: float, threshold: float) -> float:
    """Return normalized strength for values below ``threshold``."""
    if threshold == 0:
        return 0.0
    return max(0.0, min(1.0, (threshold - value) / abs(threshold)))


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
    
    # 필수조건 2: 모든 지수가 200일 이동평균보다 5% 이상 위
    above_ma200_count = 0

    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            latest = df.iloc[-1]
            if latest['ma200'] > 0 and latest['close'] >= latest['ma200'] * 1.05:
                above_ma200_count += 1

    essential_2 = above_ma200_count == len(main_indices)
    essential_conditions.append(essential_2)
    details['above_ma200_plus5_count'] = above_ma200_count
    
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
    
    # 필수조건 3: 조정이 1주일 이상 지속
    correction_duration_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            if len(df) >= 7 and all(df.iloc[-i]['close'] <= df.iloc[-i]['ma50'] for i in range(1, 6)):
                correction_duration_count += 1

    essential_3 = correction_duration_count >= 2
    essential_conditions.append(essential_3)
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
    
    # 필수조건 3: 하락 추세 2개월 이상 지속
    prolonged_count = 0
    for ticker in main_indices:
        if ticker in index_data and index_data[ticker] is not None:
            df = index_data[ticker]
            if len(df) >= 44 and all(df.iloc[-i]['close'] <= df.iloc[-i]['ma50'] for i in range(1, 44)):
                prolonged_count += 1

    essential_3 = prolonged_count >= 2
    essential_conditions.append(essential_3)
    details['prolonged_decline_count'] = prolonged_count
    
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


def determine_regime_by_conditions(index_data: Dict[str, pd.DataFrame]) -> Tuple[Optional[str], Dict]:
    """필수조건과 부가조건을 기반으로 시장 국면을 판단합니다.
    
    Returns:
        (regime_code, details) - regime_code가 None이면 조건 기반 판단 실패
    """
    all_details = {}
    
    # 각 시장 국면별 조건 검사 (우선순위 순서)
    regime_checks = [
        ('aggressive_bull', check_aggressive_bull_conditions),
        ('bull', check_bull_conditions),
        ('correction', check_correction_conditions),
        ('risk_management', check_risk_management_conditions),
        ('bear', check_bear_conditions)
    ]
    
    qualified_regimes = []
    
    for regime_code, check_function in regime_checks:
        is_qualified, details = check_function(index_data)
        all_details[regime_code] = details
        
        if is_qualified:
            qualified_regimes.append(regime_code)
    
    # 결과 판단
    if len(qualified_regimes) == 1:
        # 정확히 하나의 국면에만 해당
        return qualified_regimes[0], all_details
    elif len(qualified_regimes) == 0:
        # 어느 국면에도 해당하지 않음
        return None, all_details
    else:
        # 여러 국면에 해당 - 우선순위가 높은 것 선택
        for regime_code, _ in regime_checks:
            if regime_code in qualified_regimes:
                return regime_code, all_details
    
    return None, all_details
