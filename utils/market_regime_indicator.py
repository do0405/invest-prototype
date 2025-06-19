# -*- coding: utf-8 -*-
"""Market Regime Classification Indicator.

이 모듈은 시장 국면을 판단하기 위한 정량적 규칙 기반 지표를 제공합니다.
다양한 기술적 지표와 시장 지수를 분석하여 현재 시장 상태를 5가지 국면으로 분류합니다.

1. 공격적 상승장 (Aggressive Bull Market): 80-100점
2. 상승장 (Bull Market): 60-79점
3. 조정장 (Correction Market): 40-59점
4. 위험 관리장 (Risk Management Market): 20-39점
5. 완전한 약세장 (Full Bear Market): 0-19점
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union

from config import DATA_US_DIR, RESULTS_DIR, MARKET_REGIME_DIR, MARKET_REGIME_CRITERIA
from utils.calc_utils import get_us_market_today

__all__ = [
    "analyze_market_regime",
    "calculate_market_score",
    "get_market_regime",
    "get_regime_description",
    "get_investment_strategy",
]

# 주요 지수 티커 정의
INDEX_TICKERS = {
    "SPY": "S&P 500 (대형주)",
    "QQQ": "나스닥 100 (기술주)",
    "IWM": "Russell 2000 (소형주)",
    "MDY": "S&P 400 MidCap (중형주)",
    "IBB": "바이오텍 ETF",
    "XBI": "바이오텍 ETF",
    "VIX": "변동성 지수",
}

# 시장 국면 정의
MARKET_REGIMES = {
    "aggressive_bull": {
        "name": "공격적 상승장 (Aggressive Bull Market)",
        "score_range": MARKET_REGIME_CRITERIA['aggressive_bull_range'],
        "description": "모든 주요 지수가 강세를 보이며 시장 심리가 매우 낙관적인 상태입니다.",
        "strategy": "소형주, 성장주 비중 확대",
    },
    "bull": {
        "name": "상승장 (Bull Market)",
        "score_range": MARKET_REGIME_CRITERIA['bull_range'],
        "description": "대형주 중심의 상승세가 유지되나 일부 섹터에서 약세가 나타나기 시작합니다.",
        "strategy": "대형주 중심, 리더주 선별 투자",
    },
    "correction": {
        "name": "조정장 (Correction Market)",
        "score_range": MARKET_REGIME_CRITERIA['correction_range'],
        "description": "주요 지수가 단기 이동평균선 아래로 하락하며 조정이 진행 중입니다.",
        "strategy": "현금 비중 증대, 방어적 포지션",
    },
    "risk_management": {
        "name": "위험 관리장 (Risk Management Market)",
        "score_range": MARKET_REGIME_CRITERIA['risk_management_range'],
        "description": "주요 지수가 장기 이동평균선 아래로 하락하며 위험이 증가하고 있습니다.",
        "strategy": "신규 투자 중단, 손절매 기준 엄격 적용",
    },
    "bear": {
        "name": "완전한 약세장 (Full Bear Market)",
        "score_range": MARKET_REGIME_CRITERIA['bear_range'],
        "description": "모든 주요 지수가 장기 이동평균선 아래에서 지속적인 하락세를 보입니다.",
        "strategy": "현금 보유, 적립식 투자 외 투자 자제",
    },
}


def load_index_data(ticker: str, days: int = 200) -> Optional[pd.DataFrame]:
    """지수 데이터를 로드합니다.
    
    Args:
        ticker: 지수 티커 심볼
        days: 로드할 데이터의 일수
        
    Returns:
        DataFrame 또는 로드 실패 시 None
    """
    try:
        file_path = os.path.join(DATA_US_DIR, f"{ticker}.csv")
        if not os.path.exists(file_path):
            print(f"⚠️ {ticker} 데이터 파일을 찾을 수 없습니다.")
            return None
            
        df = pd.read_csv(file_path)
        df.columns = [col.lower() for col in df.columns]
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')
        else:
            print(f"⚠️ {ticker} 데이터에 날짜 컬럼이 없습니다.")
            return None
            
        if len(df) < days:
            print(f"⚠️ {ticker} 데이터가 충분하지 않습니다. (필요: {days}, 실제: {len(df)})")
            return None
            
        # 최근 데이터만 사용
        df = df.iloc[-days:].copy()
        
        # 이동평균선 계산
        df['ma50'] = df['close'].rolling(window=50).mean()
        df['ma200'] = df['close'].rolling(window=200).mean()
        
        return df
    except Exception as e:
        print(f"❌ {ticker} 데이터 로드 오류: {e}")
        return None


def calculate_high_low_index(index_data: Dict[str, pd.DataFrame]) -> float:
    """High-Low Index를 계산합니다.
    
    신고가/신저가 비율을 기반으로 0-100 사이의 값을 반환합니다.
    
    Args:
        index_data: 지수 데이터 딕셔너리
        
    Returns:
        High-Low Index 값 (0-100)
    """
    # 실제 구현에서는 신고가/신저가 데이터가 필요하지만,
    # 여기서는 간단한 예시로 구현합니다.
    # 실제로는 NYSE/NASDAQ 신고가/신저가 데이터를 사용해야 합니다.
    
    # 임시 구현: SPY의 52주 최고가 대비 현재가 비율을 사용
    try:
        if 'SPY' not in index_data or index_data['SPY'] is None:
            return 50  # 기본값
            
        spy_data = index_data['SPY']
        current_close = spy_data['close'].iloc[-1]
        high_52w = spy_data['high'].rolling(window=252).max().iloc[-1]
        low_52w = spy_data['low'].rolling(window=252).min().iloc[-1]
        
        # 현재가의 52주 범위 내 위치 (0-100%)
        position_in_range = (current_close - low_52w) / (high_52w - low_52w) * 100
        
        return min(max(position_in_range, 0), 100)
    except Exception as e:
        print(f"❌ High-Low Index 계산 오류: {e}")
        return 50  # 기본값


def calculate_advance_decline_trend(index_data: Dict[str, pd.DataFrame]) -> float:
    """Advance-Decline Line의 추세를 계산합니다.
    
    상승/하락 추세를 -100에서 100 사이의 값으로 반환합니다.
    
    Args:
        index_data: 지수 데이터 딕셔너리
        
    Returns:
        Advance-Decline 추세 값 (-100 ~ 100)
    """
    # 실제 구현에서는 NYSE/NASDAQ Advance-Decline 데이터가 필요하지만,
    # 여기서는 간단한 예시로 구현합니다.
    
    # 임시 구현: 주요 지수들의 최근 20일 방향성 평균을 사용
    try:
        trend_values = []
        
        for ticker, df in index_data.items():
            if df is None or len(df) < 20:
                continue
                
            # 최근 20일 종가 변화 방향 계산
            recent_changes = df['close'].diff().iloc[-20:]
            up_days = (recent_changes > 0).sum()
            down_days = (recent_changes < 0).sum()
            
            # -100 ~ 100 범위로 정규화
            if up_days + down_days == 0:
                trend = 0
            else:
                trend = ((up_days - down_days) / (up_days + down_days)) * 100
                
            trend_values.append(trend)
        
        if not trend_values:
            return 0  # 기본값
            
        return sum(trend_values) / len(trend_values)
    except Exception as e:
        print(f"❌ Advance-Decline 추세 계산 오류: {e}")
        return 0  # 기본값


def calculate_put_call_ratio() -> float:
    """Put/Call Ratio를 계산합니다.
    
    Args:
        None
        
    Returns:
        Put/Call Ratio 값
    """
    # 실제 구현에서는 옵션 데이터가 필요하지만,
    # 여기서는 기본값을 반환합니다.
    # 실제로는 CBOE Put/Call Ratio 데이터를 사용해야 합니다.
    return 0.9  # 기본값


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
    vix_value = index_data.get('VIX', {}).iloc[-1]['close'] if 'VIX' in index_data and index_data['VIX'] is not None else 20
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
    
    # Put/Call Ratio 점수 (8점 만점)
    pc_ratio = calculate_put_call_ratio()
    pc_thresholds = MARKET_REGIME_CRITERIA['put_call_ratio_thresholds']
    if pc_ratio < pc_thresholds[0]:
        pc_score = 8  # 매우 낙관적 (강한 상승장)
    elif pc_ratio < pc_thresholds[1]:
        pc_score = 6  # 낙관적 (상승장)
    elif pc_ratio < pc_thresholds[2]:
        pc_score = 4  # 중립 (조정장)
    elif pc_ratio < pc_thresholds[3]:
        pc_score = 2  # 비관적 (위험 관리장)
    else:
        pc_score = 0  # 매우 비관적 (약세장)
    
    tech_score += pc_score
    tech_score_details['put_call_ratio'] = {
        'value': pc_ratio,
        'score': pc_score
    }
    
    # High-Low Index 점수 (8점 만점)
    hl_index = calculate_high_low_index(index_data)
    hl_thresholds = MARKET_REGIME_CRITERIA['high_low_index_thresholds']
    if hl_index > hl_thresholds[3]:
        hl_score = 8  # 매우 강세 (강한 상승장)
    elif hl_index > hl_thresholds[2]:
        hl_score = 6  # 강세 (상승장)
    elif hl_index > hl_thresholds[1]:
        hl_score = 4  # 중립 (조정장)
    elif hl_index > hl_thresholds[0]:
        hl_score = 2  # 약세 (위험 관리장)
    else:
        hl_score = 0  # 매우 약세 (약세장)
    
    tech_score += hl_score
    tech_score_details['high_low_index'] = {
        'value': hl_index,
        'score': hl_score
    }
    
    # Advance-Decline Line 추세 점수 (8점 만점)
    ad_trend = calculate_advance_decline_trend(index_data)
    ad_thresholds = MARKET_REGIME_CRITERIA['advance_decline_thresholds']
    if ad_trend > ad_thresholds[3]:
        ad_score = 8  # 매우 강한 상승 추세
    elif ad_trend > ad_thresholds[2]:
        ad_score = 6  # 상승 추세
    elif ad_trend > ad_thresholds[1]:
        ad_score = 4  # 중립 추세
    elif ad_trend > ad_thresholds[0]:
        ad_score = 2  # 하락 추세
    else:
        ad_score = 0  # 매우 강한 하락 추세
    
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
    
    # 총점 계산
    total_score = base_score + tech_score
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


def analyze_market_regime(save_result: bool = True) -> Dict:
    """현재 시장 국면을 분석합니다.
    
    Args:
        save_result: 결과를 파일로 저장할지 여부
        
    Returns:
        분석 결과 딕셔너리
    """
    # 1. 데이터 로드
    index_data = {}
    for ticker in INDEX_TICKERS.keys():
        index_data[ticker] = load_index_data(ticker)
    
    # 2. 점수 계산
    score, details = calculate_market_score(index_data)
    
    # 3. 시장 국면 판단
    regime = get_market_regime(score)
    regime_name = MARKET_REGIMES[regime]['name']
    description = get_regime_description(regime)
    strategy = get_investment_strategy(regime)
    
    # 4. 결과 구성
    today = get_us_market_today()
    result = {
        'date': today.strftime('%Y-%m-%d'),
        'score': score,
        'regime': regime,
        'regime_name': regime_name,
        'description': description,
        'strategy': strategy,
        'details': details
    }
    
    # 5. 결과 저장 (옵션)
    if save_result:
        try:
            if not os.path.exists(MARKET_REGIME_DIR):
                os.makedirs(MARKET_REGIME_DIR)
                
            # JSON 형식으로 저장
            result_path = os.path.join(MARKET_REGIME_DIR, f"market_regime_{today.strftime('%Y%m%d')}.json")
            pd.Series(result).to_json(result_path)
            print(f"✅ 시장 국면 분석 결과 저장됨: {result_path}")
            
            # 최신 결과 별도 저장
            latest_path = os.path.join(MARKET_REGIME_DIR, "latest_market_regime.json")
            pd.Series(result).to_json(latest_path)
        except Exception as e:
            print(f"❌ 결과 저장 오류: {e}")
    
    return result


if __name__ == "__main__":
    # 모듈 테스트
    result = analyze_market_regime()
    print(f"\n📊 시장 국면 분석 결과 (점수: {result['score']})")
    print(f"🔍 현재 국면: {result['regime_name']}")
    print(f"📝 설명: {result['description']}")
    print(f"💡 투자 전략: {result['strategy']}")