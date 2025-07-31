"""Calculation functions for market regime indicator."""

from typing import Dict, Tuple
import os
import pandas as pd

from .market_regime_helpers import (
    INDEX_TICKERS,
    MARKET_REGIMES,
    load_index_data,
    calculate_high_low_index,
    calculate_advance_decline_trend,
)
from .market_regime_conditions import determine_regime_by_conditions
from utils.calc_utils import get_us_market_today
from config import MARKET_REGIME_DIR, MARKET_REGIME_CRITERIA



def calculate_market_score(index_data: Dict[str, pd.DataFrame]) -> Tuple[int, Dict]:
    """시장 국면 점수를 계산합니다.
    
    Args:
        index_data: 지수 데이터 딕셔너리
        
    Returns:
        총점과 세부 점수 딕셔너리
    """
    scores = {}
    details = {}
    
    # 1. 기본 점수 계산 (60점 만점)
    # 각 주요 지수별 12점씩 배점 (50일 MA 위: +6점, 200일 MA 위: +6점)
    base_score = 0
    index_score_details = {}
    
    for ticker in ['SPY', 'QQQ', 'IWM', 'MDY', 'IBB']:
        if ticker not in index_data or index_data[ticker] is None:
            index_score_details[ticker] = {
                'above_ma50': False,
                'above_ma200': False,
                'score': 0
            }
            continue
            
        df = index_data[ticker]
        latest = df.iloc[-1]
        
        above_ma50 = latest['close'] > latest['ma50']
        above_ma200 = latest['close'] > latest['ma200']
        
        ticker_score = (above_ma50 * 6) + (above_ma200 * 6)
        base_score += ticker_score
        
        index_score_details[ticker] = {
            'above_ma50': above_ma50,
            'above_ma200': above_ma200,
            'score': ticker_score
        }
    
    scores['base_score'] = base_score
    details['index_scores'] = index_score_details
    
    # 2. 기술적 지표 점수 계산 (40점 만점)
    tech_score = 0
    tech_score_details = {}
    
    # VIX 점수 (8점 만점)
    vix_value = None
    if 'VIX' in index_data and index_data['VIX'] is not None:
        vix_value = index_data['VIX'].iloc[-1]['close']
    if vix_value is None:
        vix_value = 0
    vix_thresholds = MARKET_REGIME_CRITERIA['vix_thresholds']
    if vix_value < vix_thresholds[0]:
        vix_score = 8  # 매우 낮은 변동성 (강한 상승장)
    elif vix_value < vix_thresholds[1]:
        vix_score = 6  # 낮은 변동성 (상승장)
    elif vix_value < vix_thresholds[2]:
        vix_score = 4  # 보통 변동성 (상승장/조정장 경계)
    elif vix_value < vix_thresholds[3]:
        vix_score = 2  # 높은 변동성 (조정장/위험 관리장)
    else:
        vix_score = 0  # 매우 높은 변동성 (약세장)
    
    tech_score += vix_score
    tech_score_details['vix'] = {
        'value': vix_value,
        'score': vix_score
    }
    
    # Put/Call Ratio 점수 계산 제거됨
    
    # High-Low Index 점수 (8점 만점, 비례식)
    hl_index = calculate_high_low_index(index_data)
    hl_thresholds = MARKET_REGIME_CRITERIA['high_low_index_thresholds']
    if hl_index <= hl_thresholds[0]:
        hl_score = 0
    elif hl_index >= hl_thresholds[3]:
        hl_score = 8
    else:
        range_total = hl_thresholds[3] - hl_thresholds[0]
        hl_score = (hl_index - hl_thresholds[0]) / range_total * 8
    
    tech_score += hl_score
    tech_score_details['high_low_index'] = {
        'value': hl_index,
        'score': hl_score
    }
    
    # Advance-Decline Line 추세 점수 (8점 만점, 비례식)
    ad_trend = calculate_advance_decline_trend(index_data)
    ad_thresholds = MARKET_REGIME_CRITERIA['advance_decline_thresholds']
    if ad_trend <= ad_thresholds[0]:
        ad_score = 0
    elif ad_trend >= ad_thresholds[3]:
        ad_score = 8
    else:
        range_total = ad_thresholds[3] - ad_thresholds[0]
        ad_score = (ad_trend - ad_thresholds[0]) / range_total * 8
    
    tech_score += ad_score
    tech_score_details['advance_decline_trend'] = {
        'value': ad_trend,
        'score': ad_score
    }
    
    # 바이오텍 지수 상태 점수 (8점 만점)
    bio_score = 0
    bio_value = 0
    
    for ticker in ['IBB', 'XBI']:
        if ticker not in index_data or index_data[ticker] is None:
            continue
            
        df = index_data[ticker]
        if len(df) < 30:
            continue
            
        # 월간 수익률 계산
        monthly_return = (df['close'].iloc[-1] / df['close'].iloc[-30] - 1) * 100
        bio_value = max(bio_value, monthly_return)  # 더 높은 수익률 사용
    
    bio_thresholds = MARKET_REGIME_CRITERIA['biotech_return_thresholds']
    if bio_value > bio_thresholds[3]:
        bio_score = 8  # 매우 강한 상승 (강한 상승장)
    elif bio_value > bio_thresholds[2]:
        bio_score = 6  # 상승 (상승장)
    elif bio_value > bio_thresholds[1]:
        bio_score = 4  # 약한 상승 (조정장)
    elif bio_value > bio_thresholds[0]:
        bio_score = 2  # 하락 (위험 관리장)
    else:
        bio_score = 0  # 급락 (약세장)
    
    tech_score += bio_score
    tech_score_details['biotech_index'] = {
        'value': bio_value,
        'score': bio_score
    }
    
    scores['tech_score'] = tech_score
    details['tech_scores'] = tech_score_details

    # 총점 계산 (임시 점수)
    raw_total = base_score + tech_score
    scores['raw_total_score'] = raw_total

    # --- MD 파일 기준 점수 기반 시장 국면 판단 ---
    # 총점 = 기본 점수(60점) + 기술적 지표 점수(40점)
    total_score = raw_total
    
    # 점수별 시장 국면 결정 (MD 파일 기준)
    if total_score >= 80:
        selected = 'aggressive_bull'
        regime_name = '공격적 상승장 (Aggressive Bull Market)'
        description = '모든 지수가 강세를 보이며 소형주까지 상승하는 전면적 상승장입니다.'
        strategy = '소형주, 성장주 비중 확대'
    elif total_score >= 60:
        selected = 'bull'
        regime_name = '상승장 (Bull Market)'
        description = '대형주 중심의 상승세가 유지되나 일부 섹터에서 약세가 나타나기 시작합니다.'
        strategy = '대형주 중심, 리더주 선별 투자'
    elif total_score >= 40:
        selected = 'correction'
        regime_name = '조정장 (Correction Market)'
        description = '시장이 조정 국면에 있으며 변동성이 증가하고 있습니다.'
        strategy = '현금 비중 증대, 방어적 포지션'
    elif total_score >= 20:
        selected = 'risk_management'
        regime_name = '위험 관리장 (Risk Management Market)'
        description = '시장 위험이 높아 적극적인 위험 관리가 필요한 상황입니다.'
        strategy = '신규 투자 중단, 손절매 기준 엄격 적용'
    else:
        selected = 'bear'
        regime_name = '완전한 약세장 (Full Bear Market)'
        description = '전면적인 하락장으로 방어적 투자가 필요합니다.'
        strategy = '현금 보유, 적립식 투자 외 투자 자제'
    
    # 상세 정보 저장 (기존 조건 결과는 참고용으로 유지)
    details['determined_regime'] = selected
    details['regime_name'] = regime_name
    details['description'] = description
    details['strategy'] = strategy
    
    scores['total_score'] = total_score

    return total_score, {'scores': scores, 'details': details}


def get_market_regime(score: int) -> str:
    """점수에 따른 시장 국면을 반환합니다.
    
    Args:
        score: 시장 국면 점수 (0-100)
        
    Returns:
        시장 국면 코드
    """
    for regime, info in MARKET_REGIMES.items():
        min_score, max_score = info['score_range']
        if min_score <= score <= max_score:
            return regime
    
    # 기본값
    return "correction"


def get_regime_description(regime: str) -> str:
    """시장 국면에 대한 설명을 반환합니다.
    
    Args:
        regime: 시장 국면 코드
        
    Returns:
        시장 국면 설명
    """
    if regime in MARKET_REGIMES:
        return MARKET_REGIMES[regime]['description']
    return "시장 국면을 판단할 수 없습니다."


def get_investment_strategy(regime: str) -> str:
    """시장 국면에 따른 투자 전략을 반환합니다.
    
    Args:
        regime: 시장 국면 코드
        
    Returns:
        투자 전략 설명
    """
    if regime in MARKET_REGIMES:
        return MARKET_REGIMES[regime]['strategy']
    return "현금 보유 및 투자 자제"


def analyze_market_regime(save_result: bool = True, skip_data: bool = False) -> Dict:
    """현재 시장 국면을 분석합니다.
    
    우선순위:
    1. MD 파일의 필수조건 기반 시장 국면 판단
    2. 부가조건으로 점수 범위 세분화
    3. 조건 기반 판단 실패 시 기존 종합점수 산출 방식 사용
    
    Args:
        save_result: 결과를 파일로 저장할지 여부
        
    Returns:
        분석 결과 딕셔너리
    """
    # 1. 데이터 로드
    index_data = {}
    for ticker in INDEX_TICKERS.keys():
        index_data[ticker] = load_index_data(ticker)

    # 2. 우선적으로 조건 기반 시장 국면 판단 시도
    condition_regime, condition_details = determine_regime_by_conditions(index_data)
    
    # 3. 점수 계산 (조건 기반 판단 실패 시 또는 점수 범위 세분화용)
    score, score_details = calculate_market_score(index_data)
    
    # 4. 최종 시장 국면 결정
    if condition_regime is not None:
        # 조건 기반 판단 성공 - 부가조건으로 점수 범위 세분화
        regime = condition_regime
        regime_range = MARKET_REGIMES[regime]['score_range']
        
        # 해당 국면의 점수 범위 내에서 정규화
        min_score, max_score = regime_range
        if max_score > min_score:
            normalized_score = min_score + (score / 100) * (max_score - min_score)
            final_score = max(min_score, min(max_score, int(normalized_score)))
        else:
            final_score = min_score
            
        determination_method = "condition_based"
    else:
        # 조건 기반 판단 실패 - 기존 종합점수 산출 방식 사용
        regime = get_market_regime(score)
        final_score = score
        determination_method = "score_based"
    
    regime_name = MARKET_REGIMES[regime]['name']
    description = get_regime_description(regime)
    strategy = get_investment_strategy(regime)
    
    # 5. 결과 구성
    today = get_us_market_today()
    
    # 세부 정보 통합
    combined_details = {
        'scores': score_details['scores'],
        'details': score_details['details'],
        'condition_analysis': condition_details,
        'determination_method': determination_method,
        'raw_score': score,
        'final_score': final_score
    }
    
    result = {
        'date': today.strftime('%Y-%m-%d'),
        'score': final_score,
        'regime': regime,
        'regime_name': regime_name,
        'description': description,
        'strategy': strategy,
        'details': combined_details
    }
    
    # 5. 결과 저장 (옵션)
    if save_result:
        try:
            if not os.path.exists(MARKET_REGIME_DIR):
                os.makedirs(MARKET_REGIME_DIR)
                
            # JSON 및 CSV 형식으로 저장
            result_path = os.path.join(MARKET_REGIME_DIR, f"market_regime_{today.strftime('%Y%m%d')}")
            pd.Series(result).to_json(result_path + ".json")
            pd.DataFrame([result]).to_csv(result_path + ".csv", index=False)
            # 출력은 tasks.py에서 담당

            # 최신 결과 별도 저장
            latest_base = os.path.join(MARKET_REGIME_DIR, "latest_market_regime")
            pd.Series(result).to_json(latest_base + ".json")
            pd.DataFrame([result]).to_csv(latest_base + ".csv", index=False)
        except Exception as e:
            print(f"❌ 결과 저장 오류: {e}")
    
    return result


