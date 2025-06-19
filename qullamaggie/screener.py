# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 - 스크리너 모듈

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 및 유틸리티 임포트
from config import RESULTS_DIR, RESULTS_VER2_DIR, DATA_US_DIR
from utils import ensure_dir, load_csvs_parallel

# 결과 저장 경로 설정
QULLAMAGGIE_RESULTS_DIR = os.path.join(RESULTS_VER2_DIR, 'qullamaggie')
BREAKOUT_RESULTS_PATH = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'breakout_results.csv')
EPISODE_PIVOT_RESULTS_PATH = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'episode_pivot_results.csv')
PARABOLIC_SHORT_RESULTS_PATH = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'parabolic_short_results.csv')

# 기본 스크리닝 조건 함수
def apply_basic_filters(df):
    """
    모든 셋업에 공통적으로 적용되는 기본 필터링 조건 적용
    
    Args:
        df: 주가 데이터 DataFrame
        
    Returns:
        filtered_df: 필터링된 DataFrame
    """
    if df is None or df.empty:
        return pd.DataFrame()
    
    # 최신 데이터 추출
    latest = df.iloc[-1]
    
    # 1.1 기본 필터링 조건
    # 주가 범위: 현재가 ≥ 5,000원 (달러 기준 $5)
    price_condition = latest['close'] >= 5.0
    
    # 거래량 조건: 일평균 거래량(20일) ≥ 500,000주
    volume_condition = df['volume'].rolling(window=20).mean().iloc[-1] >= 500000
    
    # ADR 조건: ADR ≥ 3.5% (계산: (High-Low)/Close의 20일 평균)
    df['adr'] = (df['high'] - df['low']) / df['close'] * 100
    adr_condition = df['adr'].rolling(window=20).mean().iloc[-1] >= 3.5
    
    # 1.2 기본 상승 조건 (선택적 적용)
    # 현재가 위치: Close > 10일MA AND Close > 20일MA
    df['ma10'] = df['close'].rolling(window=10).mean()
    df['ma20'] = df['close'].rolling(window=20).mean()
    ma_condition = (latest['close'] > df['ma10'].iloc[-1]) and (latest['close'] > df['ma20'].iloc[-1])
    
    # 모든 조건 결합
    basic_condition = price_condition and volume_condition and adr_condition and ma_condition
    
    # 결과 반환
    return basic_condition, df

# 브레이크아웃 셋업 스크리닝 함수
def screen_breakout_setup(ticker, df):
    """
    브레이크아웃 셋업 스크리닝 함수
    
    Args:
        ticker: 종목 티커
        df: 주가 데이터 DataFrame
        
    Returns:
        result_dict: 스크리닝 결과 딕셔너리
    """
    # 기본 결과 딕셔너리 초기화
    result_dict = {
        'symbol': ticker,
        'setup_type': 'Breakout',
        'passed': False,
        'current_price': None,
        'volume_ratio': None,
        'adr': None,
        'vcp_pattern': False,
        'breakout_level': None,
        'stop_loss': None,
        'risk_reward_ratio': None,
        'score': 0
    }
    
    # 데이터 유효성 검사
    if df is None or df.empty or len(df) < 60:  # 최소 60일 데이터 필요
        return result_dict
    
    # 기본 필터 적용
    basic_passed, df = apply_basic_filters(df)
    if not basic_passed:
        return result_dict
    
    # 최신 데이터 추출
    latest = df.iloc[-1]
    result_dict['current_price'] = latest['close']
    
    # 거래량 비율 계산 (현재 거래량 / 20일 평균 거래량)
    avg_volume = df['volume'].rolling(window=20).mean().iloc[-1]
    result_dict['volume_ratio'] = latest['volume'] / avg_volume if avg_volume > 0 else 0
    
    # ADR 계산
    df['adr'] = (df['high'] - df['low']) / df['close'] * 100
    result_dict['adr'] = df['adr'].rolling(window=20).mean().iloc[-1]
    
    # 2.1 사전 조건 확인
    # 초기 상승: 지난 1-3개월간 30-100% 이상 상승
    df['return_60d'] = df['close'].pct_change(periods=60) * 100
    initial_rise_condition = df['return_60d'].iloc[-1] >= 30
    
    # 현재 위치: 52주 신고가 대비 70% 이상 수준 유지
    df['high_52w'] = df['high'].rolling(window=252).max()
    high_level_condition = latest['close'] >= df['high_52w'].iloc[-1] * 0.7
    
    # 2.2 VCP(Volatility Contraction Pattern) 패턴 정량화
    vcp_pattern = check_vcp_pattern(df)
    result_dict['vcp_pattern'] = vcp_pattern
    
    # 2.3 매수 시그널 조건
    # 통합구간 고점 계산 (최근 20일 중 최고가)
    consolidation_high = df['high'].iloc[-20:].max()
    result_dict['breakout_level'] = consolidation_high * 1.02  # 2% 돌파 기준
    
    # 돌파 확인: Close > 통합구간 고점 * 1.02
    breakout_condition = latest['close'] > consolidation_high * 1.02
    
    # 거래량 증가: Volume > 20일 평균거래량 * 1.5
    volume_surge_condition = latest['volume'] > avg_volume * 1.5
    
    # ADR 대비 위험: (진입가 - 당일 저점) / 진입가 ≤ ADR * 0.67
    risk_condition = (latest['close'] - latest['low']) / latest['close'] <= result_dict['adr'] * 0.67 / 100
    
    # 이동평균 조건: Close > 10일MA AND 10일MA > 20일MA
    ma_trend_condition = (latest['close'] > df['ma10'].iloc[-1]) and (df['ma10'].iloc[-1] > df['ma20'].iloc[-1])
    
    # 2.4 손절 및 익절 조건
    # 손절: 매수일 저점 (캔들 종가 기준)
    result_dict['stop_loss'] = latest['low']
    
    # 손익비 계산 (목표가는 현재가의 10% 상승으로 가정)
    target_price = latest['close'] * 1.1
    risk = latest['close'] - result_dict['stop_loss']
    reward = target_price - latest['close']
    result_dict['risk_reward_ratio'] = reward / risk if risk > 0 else 0
    
    # 모든 조건 결합
    all_conditions = [
        initial_rise_condition,
        high_level_condition,
        vcp_pattern,
        breakout_condition,
        volume_surge_condition,
        risk_condition,
        ma_trend_condition
    ]
    
    # 점수 계산 (충족된 조건 수)
    result_dict['score'] = sum(all_conditions)
    
    # 최종 판단 (모든 조건 충족 또는 점수가 5점 이상)
    result_dict['passed'] = all(all_conditions) or result_dict['score'] >= 5
    
    return result_dict

# VCP 패턴 확인 함수
def check_vcp_pattern(df):
    """
    VCP(Volatility Contraction Pattern) 패턴 확인 함수
    
    Args:
        df: 주가 데이터 DataFrame
        
    Returns:
        bool: VCP 패턴 존재 여부
    """
    # 데이터 유효성 검사
    if df is None or df.empty or len(df) < 60:  # 최소 60일 데이터 필요
        return False
    
    # 변동성 계산 (ADR: Average Daily Range)
    df['adr'] = (df['high'] - df['low']) / df['close'] * 100
    
    # 최근 60일 데이터 추출
    recent_df = df.iloc[-60:].copy()
    
    # 조정 구간 식별을 위한 20일 이동평균 계산
    recent_df['ma20'] = recent_df['close'].rolling(window=20).mean()
    
    # 조정 구간 식별 (종가가 20일 이동평균 아래로 내려가는 구간)
    recent_df['correction'] = recent_df['close'] < recent_df['ma20']
    
    # 연속된 조정 구간 식별
    correction_periods = []
    current_period = []
    in_correction = False
    
    for i, row in recent_df.iterrows():
        if row['correction'] and not in_correction:  # 조정 시작
            in_correction = True
            current_period = [i]
        elif row['correction'] and in_correction:  # 조정 계속
            current_period.append(i)
        elif not row['correction'] and in_correction:  # 조정 종료
            in_correction = False
            if len(current_period) >= 5:  # 최소 5일 이상의 조정 구간만 고려
                correction_periods.append(current_period)
            current_period = []
    
    # 마지막 조정 구간 처리
    if in_correction and len(current_period) >= 5:
        correction_periods.append(current_period)
    
    # 조정 구간이 3개 미만이면 VCP 패턴 아님
    if len(correction_periods) < 3:
        return False
    
    # 최근 3개의 조정 구간만 사용
    correction_periods = correction_periods[-3:]
    
    # 각 조정 구간의 변동성, 저점, 거래량 계산
    adr_values = []
    low_values = []
    volume_values = []
    
    for period in correction_periods:
        period_df = recent_df.loc[period]
        adr_values.append(period_df['adr'].mean())
        low_values.append(period_df['low'].min())
        volume_values.append(period_df['volume'].mean())
    
    # VCP 패턴 조건 확인
    # 1. 변동성 수축: 각 조정의 ADR이 이전 조정보다 20% 이상 감소
    adr_contraction = (adr_values[1] < adr_values[0] * 0.8) and (adr_values[2] < adr_values[1] * 0.8)
    
    # 2. 저점 상승: 각 조정의 저점이 이전 조정보다 높음
    low_rising = (low_values[1] > low_values[0]) and (low_values[2] > low_values[1])
    
    # 3. 거래량 수축: 각 조정의 거래량이 이전 조정보다 30% 이상 감소
    volume_contraction = (volume_values[1] < volume_values[0] * 0.7) and (volume_values[2] < volume_values[1] * 0.7)
    
    # 모든 조건 충족 시 VCP 패턴으로 판단
    return adr_contraction and low_rising and volume_contraction

# 에피소드 피벗 셋업 스크리닝 함수
def screen_episode_pivot_setup(ticker, df):
    """
    에피소드 피벗 셋업 스크리닝 함수
    
    Args:
        ticker: 종목 티커
        df: 주가 데이터 DataFrame
        
    Returns:
        result_dict: 스크리닝 결과 딕셔너리
    """
    # 기본 결과 딕셔너리 초기화
    result_dict = {
        'symbol': ticker,
        'setup_type': 'Episode Pivot',
        'passed': False,
        'current_price': None,
        'gap_percent': None,
        'volume_ratio': None,
        'ma50_relation': None,
        'stop_loss': None,
        'risk_reward_ratio': None,
        'score': 0
    }
    
    # 데이터 유효성 검사
    if df is None or df.empty or len(df) < 60:  # 최소 60일 데이터 필요
        return result_dict
    
    # 기본 필터 적용
    basic_passed, df = apply_basic_filters(df)
    if not basic_passed:
        return result_dict
    
    # 최신 데이터 추출
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    result_dict['current_price'] = latest['close']
    
    # 3.1 사전 조건
    # 기간 제한: 최근 3-6개월간 과도한 상승(100% 이상) 없음
    df['return_90d'] = df['close'].pct_change(periods=90) * 100
    no_excessive_rise = df['return_90d'].iloc[-1] < 100
    
    # 기준선 위치: Close > 50일MA (중장기 추세 양호)
    df['ma50'] = df['close'].rolling(window=50).mean()
    above_ma50 = latest['close'] > df['ma50'].iloc[-1]
    result_dict['ma50_relation'] = 'Above' if above_ma50 else 'Below'
    
    # 3.2 EP 시그널 조건 정량화
    # 갭 상승: 시초가 ≥ 전일 종가 * 1.1 (10% 이상 갭업)
    gap_percent = (latest['open'] / prev['close'] - 1) * 100
    result_dict['gap_percent'] = gap_percent
    gap_up_condition = gap_percent >= 10
    
    # 거래량 폭증: 당일 거래량 ≥ 평균 거래량(20일) * 3.0
    avg_volume = df['volume'].rolling(window=20).mean().iloc[-1]
    volume_ratio = latest['volume'] / avg_volume if avg_volume > 0 else 0
    result_dict['volume_ratio'] = volume_ratio
    volume_surge_condition = volume_ratio >= 3.0
    
    # 3.3 매수 및 관리 전략
    # 손절: 갭업 전 고점 하회 시
    result_dict['stop_loss'] = prev['high']
    
    # 손익비 계산 (목표가는 현재가의 10% 상승으로 가정)
    target_price = latest['close'] * 1.1
    risk = latest['close'] - result_dict['stop_loss']
    reward = target_price - latest['close']
    result_dict['risk_reward_ratio'] = reward / risk if risk > 0 else 0
    
    # 모든 조건 결합
    all_conditions = [
        no_excessive_rise,
        above_ma50,
        gap_up_condition,
        volume_surge_condition
    ]
    
    # 점수 계산 (충족된 조건 수)
    result_dict['score'] = sum(all_conditions)
    
    # 최종 판단 (모든 조건 충족 또는 점수가 3점 이상)
    result_dict['passed'] = all(all_conditions) or result_dict['score'] >= 3
    
    return result_dict

# 파라볼릭 숏 셋업 스크리닝 함수
def screen_parabolic_short_setup(ticker, df):
    """
    파라볼릭 숏 셋업 스크리닝 함수
    
    Args:
        ticker: 종목 티커
        df: 주가 데이터 DataFrame
        
    Returns:
        result_dict: 스크리닝 결과 딕셔너리
    """
    # 기본 결과 딕셔너리 초기화
    result_dict = {
        'symbol': ticker,
        'setup_type': 'Parabolic Short',
        'passed': False,
        'current_price': None,
        'short_term_rise': None,
        'consecutive_up_days': 0,
        'volume_ratio': None,
        'rsi14': None,
        'ma20_deviation': None,
        'first_down_candle': False,
        'stop_loss': None,
        'score': 0
    }
    
    # 데이터 유효성 검사
    if df is None or df.empty or len(df) < 60:  # 최소 60일 데이터 필요
        return result_dict
    
    # 기본 필터 적용 (파라볼릭 숏은 기본 필터 일부만 적용)
    # 최신 데이터 추출
    latest = df.iloc[-1]
    result_dict['current_price'] = latest['close']
    
    # 거래량 비율 계산 (현재 거래량 / 20일 평균 거래량)
    avg_volume = df['volume'].rolling(window=20).mean().iloc[-1]
    volume_ratio = latest['volume'] / avg_volume if avg_volume > 0 else 0
    result_dict['volume_ratio'] = volume_ratio
    
    # 4.1 과열 조건 확인
    # 단기 상승폭 계산 (10일간 상승률)
    short_term_rise = (latest['close'] / df['close'].iloc[-11] - 1) * 100
    result_dict['short_term_rise'] = short_term_rise
    
    # 시가총액에 따른 상승폭 조건 (대략적인 기준)
    market_cap_threshold = 10_000_000_000  # 100억 달러 (대형주 기준)
    if 'market_cap' in df.columns and df['market_cap'].iloc[-1] >= market_cap_threshold:
        # 대형주: 5-10일간 50-100% 상승
        rise_condition = short_term_rise >= 50
    else:
        # 중소형주: 5-10일간 200-500% 상승
        rise_condition = short_term_rise >= 200
    
    # 연속 상승: 3일 이상 연속 양봉
    consecutive_up = 0
    for i in range(1, min(6, len(df))):
        idx = -i
        if df['close'].iloc[idx] > df['open'].iloc[idx]:
            consecutive_up += 1
        else:
            break
    result_dict['consecutive_up_days'] = consecutive_up
    consecutive_up_condition = consecutive_up >= 3
    
    # 거래량 급증: 평균 거래량 대비 5배 이상 증가
    volume_surge_condition = volume_ratio >= 5
    
    # 4.2 숏 시그널 조건
    # RSI 계산
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    result_dict['rsi14'] = rsi.iloc[-1]
    rsi_condition = rsi.iloc[-1] >= 80
    
    # 현재가 > 20일MA * 1.5 (50% 이상 괴리)
    df['ma20'] = df['close'].rolling(window=20).mean()
    ma20_deviation = latest['close'] / df['ma20'].iloc[-1] - 1
    result_dict['ma20_deviation'] = ma20_deviation * 100  # 퍼센트로 변환
    ma_deviation_condition = ma20_deviation >= 0.5
    
    # 3일 연속 상승 후 첫 번째 음봉 발생
    first_down_candle = consecutive_up >= 3 and latest['close'] < latest['open']
    result_dict['first_down_candle'] = first_down_candle
    
    # 손절: 최근 고점 +5%
    recent_high = df['high'].iloc[-10:].max()
    result_dict['stop_loss'] = recent_high * 1.05
    
    # 모든 조건 결합
    all_conditions = [
        rise_condition,
        consecutive_up_condition,
        volume_surge_condition,
        rsi_condition,
        ma_deviation_condition,
        first_down_candle
    ]
    
    # 점수 계산 (충족된 조건 수)
    result_dict['score'] = sum(all_conditions)
    
    # 최종 판단 (모든 조건 충족 또는 점수가 4점 이상)
    result_dict['passed'] = all(all_conditions) or result_dict['score'] >= 4
    
    return result_dict

# 메인 스크리닝 함수
def run_qullamaggie_screening(setup_type=None):
    """
    쿨라매기 매매법 스크리닝 실행 함수
    
    Args:
        setup_type: 스크리닝할 셋업 타입 ('breakout', 'episode_pivot', 'parabolic_short', None=모두)
        
    Returns:
        dict: 각 셋업별 스크리닝 결과
    """
    print("\n🔍 쿨라매기 매매법 스크리닝 시작...")
    
    # 결과 디렉토리 생성
    ensure_dir(QULLAMAGGIE_RESULTS_DIR)
    
    # 데이터 디렉토리에서 모든 CSV 파일 경로 가져오기
    csv_files = [os.path.join(DATA_US_DIR, f) for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
    
    # 데이터 로드
    print(f"📊 총 {len(csv_files)}개 종목 데이터 로드 중...")
    stock_data = load_csvs_parallel(csv_files)
    print(f"✅ {len(stock_data)}개 종목 데이터 로드 완료")
    
    # 결과 저장용 딕셔너리
    results = {
        'breakout': [],
        'episode_pivot': [],
        'parabolic_short': []
    }
    
    # 각 종목에 대해 스크리닝 실행
    print("\n🔍 스크리닝 실행 중...")
    for i, (file_name, df) in enumerate(stock_data.items(), 1):
        ticker = os.path.splitext(file_name)[0]
        
        # 진행 상황 출력 (100개 단위)
        if i % 100 == 0 or i == len(stock_data):
            print(f"  진행률: {i}/{len(stock_data)} ({i/len(stock_data)*100:.1f}%)")
        
        # 셋업별 스크리닝 실행
        if setup_type is None or setup_type == 'breakout':
            breakout_result = screen_breakout_setup(ticker, df)
            if breakout_result['passed']:
                results['breakout'].append(breakout_result)
        
        if setup_type is None or setup_type == 'episode_pivot':
            episode_pivot_result = screen_episode_pivot_setup(ticker, df)
            if episode_pivot_result['passed']:
                results['episode_pivot'].append(episode_pivot_result)
        
        if setup_type is None or setup_type == 'parabolic_short':
            parabolic_short_result = screen_parabolic_short_setup(ticker, df)
            if parabolic_short_result['passed']:
                results['parabolic_short'].append(parabolic_short_result)
    
    # 결과 저장
    print("\n💾 스크리닝 결과 저장 중...")
    
    # 브레이크아웃 셋업 결과 저장
    if setup_type is None or setup_type == 'breakout':
        breakout_df = pd.DataFrame(results['breakout'])
        if not breakout_df.empty:
            # 점수 기준 내림차순 정렬
            breakout_df = breakout_df.sort_values('score', ascending=False)
            breakout_df.to_csv(BREAKOUT_RESULTS_PATH, index=False)
            # JSON 파일 생성
            breakout_df.to_json(BREAKOUT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"✅ 브레이크아웃 셋업 결과 저장 완료: {len(breakout_df)}개 종목")
        else:
            print("⚠️ 브레이크아웃 셋업 결과 없음")
    
    # 에피소드 피벗 셋업 결과 저장
    if setup_type is None or setup_type == 'episode_pivot':
        episode_pivot_df = pd.DataFrame(results['episode_pivot'])
        if not episode_pivot_df.empty:
            # 점수 기준 내림차순 정렬
            episode_pivot_df = episode_pivot_df.sort_values('score', ascending=False)
            episode_pivot_df.to_csv(EPISODE_PIVOT_RESULTS_PATH, index=False)
            # JSON 파일 생성
            episode_pivot_df.to_json(EPISODE_PIVOT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"✅ 에피소드 피벗 셋업 결과 저장 완료: {len(episode_pivot_df)}개 종목")
        else:
            print("⚠️ 에피소드 피벗 셋업 결과 없음")
    
    # 파라볼릭 숏 셋업 결과 저장
    if setup_type is None or setup_type == 'parabolic_short':
        parabolic_short_df = pd.DataFrame(results['parabolic_short'])
        if not parabolic_short_df.empty:
            # 점수 기준 내림차순 정렬
            parabolic_short_df = parabolic_short_df.sort_values('score', ascending=False)
            parabolic_short_df.to_csv(PARABOLIC_SHORT_RESULTS_PATH, index=False)
            # JSON 파일 생성
            parabolic_short_df.to_json(PARABOLIC_SHORT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"✅ 파라볼릭 숏 셋업 결과 저장 완료: {len(parabolic_short_df)}개 종목")
        else:
            print("⚠️ 파라볼릭 숏 셋업 결과 없음")
    
    # 결과 요약
    print("\n📊 스크리닝 결과 요약:")
    print(f"  브레이크아웃 셋업: {len(results['breakout'])}개 종목")
    print(f"  에피소드 피벗 셋업: {len(results['episode_pivot'])}개 종목")
    print(f"  파라볼릭 숏 셋업: {len(results['parabolic_short'])}개 종목")
    
    return results

# 메인 함수
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='쿨라매기 매매법 스크리너')
    parser.add_argument('--setup', choices=['breakout', 'episode_pivot', 'parabolic_short'], 
                        help='스크리닝할 셋업 타입')
    
    args = parser.parse_args()
    
    # 스크리닝 실행
    run_qullamaggie_screening(args.setup)

if __name__ == '__main__':
    main()