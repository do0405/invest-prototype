"""Calculation functions for market regime indicator."""

from typing import Dict, Tuple
import pandas as pd

from .market_regime_helpers import (
    INDEX_TICKERS,
    MARKET_REGIMES,
    load_index_data,
    calculate_high_low_index,
    calculate_advance_decline_trend,
    calculate_put_call_ratio,
)
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

    # 총점 계산 (임시 점수)
    raw_total = base_score + tech_score
    scores['raw_total_score'] = raw_total

    # --- 필수/부가 조건 기반 시장 국면 판단 ---
    def pct_above_ma200(df):
        return (df['close'].iloc[-1] / df['ma200'].iloc[-1] - 1) * 100

    def drawdown_pct(df):
        high_52w = df['close'].rolling(window=252).max().iloc[-1]
        return (df['close'].iloc[-1] - high_52w) / high_52w * 100

    def monthly_return(df):
        if len(df) < 30:
            return 0.0
        return (df['close'].iloc[-1] / df['close'].iloc[-30] - 1) * 100

    def pct_change(df, days=20):
        if len(df) <= days:
            return 0.0
        return (df['close'].iloc[-1] / df['close'].iloc[-days] - 1) * 100

    metrics = {
        'vix': vix_value,
        'put_call_ratio': pc_ratio,
        'high_low_index': hl_index,
        'ad_trend': ad_trend,
        'bio_return': bio_value,
    }

    for t in ['SPY', 'QQQ', 'IWM', 'MDY']:
        df = index_data.get(t)
        if df is None:
            continue
        metrics[f'{t}_above_ma50'] = df['close'].iloc[-1] > df['ma50'].iloc[-1]
        metrics[f'{t}_pct_above_ma200'] = pct_above_ma200(df)
        metrics[f'{t}_drawdown'] = drawdown_pct(df)
        metrics[f'{t}_return'] = pct_change(df)
        metrics[f'{t}_below_ma50_5d'] = (
            len(df) >= 5 and (df['close'] < df['ma50']).iloc[-5:].all()
        )

    spy_ret = metrics.get('SPY_return', 0)
    iwm_ret = metrics.get('IWM_return', 0)
    metrics['iwm_outperform'] = iwm_ret > spy_ret

    conditions = {
        'aggressive_bull': {
            'threshold': 0.7,
            'mandatory': [
                all(
                    metrics.get(f'{t}_above_ma50', False)
                    for t in ['SPY', 'QQQ', 'IWM', 'MDY']
                ),
                all(
                    metrics.get(f'{t}_pct_above_ma200', 0) >= 5
                    for t in ['SPY', 'QQQ', 'IWM', 'MDY']
                ),
                metrics['bio_return'] >= 3,
            ],
            'optional': [
                metrics['vix'] < 20,
                metrics['put_call_ratio'] < 0.7,
                metrics['high_low_index'] > 70,
                metrics['ad_trend'] > 0,
                metrics['iwm_outperform'],
            ],
        },
        'bull': {
            'threshold': 0.6,
            'mandatory': [
                metrics.get('SPY_above_ma50', False)
                and metrics.get('QQQ_above_ma50', False),
                not (
                    metrics.get('IWM_above_ma50', True)
                    and metrics.get('MDY_above_ma50', True)
                ),
                0 <= metrics['bio_return'] < 3,
            ],
            'optional': [
                20 <= metrics['vix'] <= 25,
                0.7 <= metrics['put_call_ratio'] <= 0.9,
                50 <= metrics['high_low_index'] <= 70,
                metrics.get('SPY_return', 0)
                > metrics.get('MDY_return', 0)
                > metrics.get('IWM_return', 0),
                metrics['ad_trend'] >= 0,
            ],
        },
        'correction': {
            'threshold': 0.6,
            'mandatory': [
                sum(
                    not metrics.get(f'{t}_above_ma50', True)
                    for t in ['SPY', 'QQQ', 'IWM', 'MDY']
                )
                >= 2,
                sum(
                    -15 <= metrics.get(f'{t}_drawdown', 0) <= -5
                    for t in ['SPY', 'QQQ']
                )
                >= 1,
                metrics.get('SPY_below_ma50_5d', False),
            ],
            'optional': [
                25 <= metrics['vix'] <= 35,
                0.9 <= metrics['put_call_ratio'] <= 1.2,
                30 <= metrics['high_low_index'] <= 50,
                metrics['ad_trend'] < 0,
                iwm_ret < 0,
            ],
        },
        'risk_management': {
            'threshold': 0.5,
            'mandatory': [
                sum(
                    not metrics.get(f'{t}_pct_above_ma200', 1) > 0
                    for t in ['SPY', 'QQQ', 'IWM', 'MDY']
                )
                >= 3,
                sum(
                    -25 <= metrics.get(f'{t}_drawdown', 0) <= -15
                    for t in ['SPY', 'QQQ']
                )
                >= 1,
            ],
            'optional': [
                metrics['vix'] > 35,
                metrics['put_call_ratio'] > 1.2,
                metrics['high_low_index'] < 30,
                metrics['ad_trend'] <= -20,
                metrics.get('SPY_above_ma50', True)
                and metrics.get('SPY_pct_above_ma200', 0) < 0,
            ],
        },
        'bear': {
            'threshold': 0.0,
            'mandatory': [
                all(
                    metrics.get(f'{t}_pct_above_ma200', 0) < 0
                    for t in ['SPY', 'QQQ', 'IWM', 'MDY']
                ),
                sum(
                    metrics.get(f'{t}_drawdown', 0) <= -25
                    for t in ['SPY', 'QQQ']
                )
                >= 1,
            ],
            'optional': [
                metrics['vix'] > 40,
                metrics['put_call_ratio'] > 1.5,
                metrics['high_low_index'] < 20,
                metrics['ad_trend'] <= -30,
                metrics['bio_return'] <= -30,
            ],
        },
    }

    selected = None
    pass_ratio = 0.0
    condition_results = {}
    for regime in ['aggressive_bull', 'bull', 'correction', 'risk_management', 'bear']:
        conf = conditions[regime]
        mand_results = conf['mandatory']
        opt_results = conf['optional']
        condition_results[regime] = {
            'mandatory': mand_results,
            'optional': opt_results,
        }
        if all(mand_results):
            ratio = sum(bool(x) for x in opt_results) / len(opt_results) if opt_results else 1.0
            condition_results[regime]['optional_pass_ratio'] = ratio
            if ratio >= conf['threshold']:
                selected = regime
                pass_ratio = ratio
                break

    if selected is None:
        selected = 'bear'
        pass_ratio = 0.0

    details['condition_results'] = condition_results
    details['determined_regime'] = selected

    min_score, max_score = MARKET_REGIME_CRITERIA[f'{selected}_range']
    total_score = int(min_score + (max_score - min_score) * pass_ratio)
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


