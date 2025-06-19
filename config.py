# -*- coding: utf-8 -*-
# 투자 스크리너 - 설정 파일

import os

# 기본 디렉토리 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')  # 데이터 디렉토리
RESULTS_DIR = os.path.join(BASE_DIR, 'results')  # 결과 디렉토리
SCREENER_RESULTS_DIR = os.path.join(RESULTS_DIR, 'screeners')
PORTFOLIO_RESULTS_DIR = os.path.join(RESULTS_DIR, 'portfolio')
PORTFOLIO_BUY_DIR = os.path.join(PORTFOLIO_RESULTS_DIR, 'buy')
PORTFOLIO_SELL_DIR = os.path.join(PORTFOLIO_RESULTS_DIR, 'sell')

# 기존 코드 호환성을 위한 별칭
RESULTS_VER2_DIR = PORTFOLIO_RESULTS_DIR
MARKMINERVINI_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'markminervini')
QULLAMAGGIE_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'qullamaggie')
US_GAINER_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'us_gainer')
US_SETUP_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'us_setup')
OPTION_VOLATILITY_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'option_volatility')
LEADER_STOCK_RESULTS_DIR = os.path.join(RESULTS_DIR, 'leader_stock')
MOMENTUM_SIGNALS_RESULTS_DIR = os.path.join(RESULTS_DIR, 'momentum_signals')
IPO_INVESTMENT_RESULTS_DIR = os.path.join(RESULTS_DIR, 'ipo_investment')
DATA_US_DIR = os.path.join(DATA_DIR, 'us')  # 미국 주식 데이터 디렉토리
BACKUP_DIR = os.path.join(BASE_DIR, 'backup')  # 백업 디렉토리
SCREENERS_DIR = os.path.join(BASE_DIR, 'screeners')
MARKMINERVINI_DIR = os.path.join(SCREENERS_DIR, 'markminervini')
QULLAMAGGIE_DIR = os.path.join(SCREENERS_DIR, 'qullamaggie')
US_GAINER_DIR = os.path.join(SCREENERS_DIR, 'us_gainer')
US_SETUP_DIR = os.path.join(SCREENERS_DIR, 'us_setup')
OPTION_VOLATILITY_DIR = os.path.join(SCREENERS_DIR, 'option_volatility')
LEADER_STOCK_DIR = os.path.join(SCREENERS_DIR, 'leader_stock')
MOMENTUM_SIGNALS_DIR = os.path.join(SCREENERS_DIR, 'momentum_signals')
IPO_INVESTMENT_DIR = os.path.join(SCREENERS_DIR, 'ipo_investment')

# 파일 경로 설정
US_WITH_RS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'us_with_rs.csv')
ADVANCED_FINANCIAL_RESULTS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'advanced_financial_results.csv')
INTEGRATED_RESULTS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'integrated_results.csv')
US_SETUP_RESULTS_PATH = os.path.join(US_SETUP_RESULTS_DIR, 'us_setup_results.csv')
US_GAINERS_RESULTS_PATH = os.path.join(US_GAINER_RESULTS_DIR, 'us_gainers_results.csv')

# 스크리닝 조건 설정
TECHNICAL_CONDITION_COUNT = 8  # 기술적 조건 수
FINANCIAL_CONDITION_COUNT = 11  # 재무제표 조건 수

# 재무제표 스크리닝 기준
ADVANCED_FINANCIAL_CRITERIA = {
    'min_quarterly_eps_growth': 20,  # 최근 분기 EPS 성장률 ≥ +20%
    'min_annual_eps_growth': 20,    # 최근 연간 EPS 성장률 ≥ +20%
    'min_quarterly_revenue_growth': 20,  # 최근 분기 매출 성장률 ≥ +20%
    'min_annual_revenue_growth': 20,     # 최근 연간 매출 성장률 ≥ +20%
    'min_quarterly_net_income_growth': 20,  # 최근 분기 순이익 증가율 ≥ +20%
    'min_annual_net_income_growth': 20,     # 최근 연간 순이익 증가율 ≥ +20%
    'min_roe': 15,                  # 최근 연간 ROE ≥ 15%
    'max_debt_to_equity': 150       # 부채비율 ≤ 150%
}

# 변동성 스큐 역전 전략 기준 (Xing et al. 2010 논문 기반)
VOLATILITY_SKEW_CRITERIA = {
    # 기본 조건
    'min_market_cap': 1_000_000_000,  # 최소 시가총액 $1B (논문 평균 $10.22B)
    'min_monthly_turnover': 0.1,      # 월 거래회전율 10% 이상
    'min_avg_daily_volume': 500_000,  # 일평균 거래량 50만주 이상
    
    # 옵션 데이터 조건
    'min_days_to_expiration': 10,     # 최소 만기일 10일
    'max_days_to_expiration': 60,     # 최대 만기일 60일
    'min_option_volume': 1,           # 최소 옵션 거래량
    'min_open_interest': 1,           # 최소 미결제약정
    
    # 머니니스 범위 (논문 기준)
    'atm_call_moneyness_min': 0.95,   # ATM 콜옵션 하한
    'atm_call_moneyness_max': 1.05,   # ATM 콜옵션 상한
    'otm_put_moneyness_min': 0.80,    # OTM 풋옵션 하한
    'otm_put_moneyness_max': 0.95,    # OTM 풋옵션 상한
    
    # 스큐 지수 기준 (논문 Table 1 기준)
    'low_skew_threshold': 2.4,        # 25 percentile (낮은 스큐)
    'median_skew_threshold': 4.76,    # 50 percentile (중간 스큐)
    'high_skew_threshold': 8.43,      # 75 percentile (높은 스큐)
    
    # 상승 후보 선별 기준
    'bullish_skew_percentile': 0.2,   # 하위 20% (lowest quintile)
    'min_past_6m_return': 0.0,        # 과거 6개월 수익률 양수
}

# 예상 수익률 매핑 (논문 Table 3 기반)
SKEW_EXPECTED_RETURNS = {
    'low_skew': 0.13,      # 13% (스큐 < 2.4%)
    'medium_skew': 0.08,   # 8% (2.4% ≤ 스큐 < 4.76%)
    'high_skew': 0.05,     # 5% (4.76% ≤ 스큐 < 8.43%)
    'very_high_skew': 0.02 # 2% (스큐 ≥ 8.43%)
}

# API 설정
YAHOO_FINANCE_MAX_RETRIES = 3  # Yahoo Finance API 최대 재시도 횟수
YAHOO_FINANCE_DELAY = 1        # Yahoo Finance API 재시도 간 지연 시간(초)

# Alpha Vantage API 설정 (옵션 데이터용)
ALPHA_VANTAGE_API_KEY = "YOUR_ALPHA_VANTAGE_KEY"  # Alpha Vantage API 키
ALPHA_VANTAGE_MAX_REQUESTS_PER_DAY = 500          # 무료 플랜 일일 요청 제한
ALPHA_VANTAGE_REQUEST_DELAY = 12                  # 요청 간 지연 시간(초) - 무료 플랜 5 requests/min

# 옵션 데이터 소스 우선순위
OPTION_DATA_SOURCES = [
    "alpha_vantage",  # 1순위: Alpha Vantage 옵션 API
    "yfinance",       # 2순위: yfinance 옵션 체인
    "exclusion"       # 3순위: 해당 종목 제외
]


# 변동성 스큐 역전 전략 관련 설정 (새 디렉토리 구조)
VOLATILITY_SKEW_RESULTS_PATH = os.path.join(OPTION_VOLATILITY_DIR, 'volatility_skew_results.csv')  # 변동성 스큐 스크리닝 결과
VOLATILITY_SKEW_DETAILED_PATH = os.path.join(OPTION_VOLATILITY_DIR, 'volatility_skew_detailed.csv')  # 상세 분석 결과
VOLATILITY_SKEW_PERFORMANCE_PATH = os.path.join(OPTION_VOLATILITY_DIR, 'volatility_skew_performance.csv')  # 성과 분석 결과
VOLATILITY_SKEW_LOG_PATH = os.path.join(OPTION_VOLATILITY_DIR, 'screening_log.txt')  # 스크리닝 로그

# 시장 국면 판단 지표 관련 설정
MARKET_REGIME_DIR = os.path.join(RESULTS_DIR, 'market_regime')  # 시장 국면 분석 결과 디렉토리
MARKET_REGIME_LATEST_PATH = os.path.join(MARKET_REGIME_DIR, 'latest_market_regime.json')  # 최신 시장 국면 분석 결과

# 시장 국면 판단 지표 기준
MARKET_REGIME_CRITERIA = {
    # 점수 범위
    'aggressive_bull_range': (80, 100),  # 공격적 상승장
    'bull_range': (60, 79),              # 상승장
    'correction_range': (40, 59),        # 조정장
    'risk_management_range': (20, 39),   # 위험 관리장
    'bear_range': (0, 19),               # 완전한 약세장
    
    # 기술적 지표 기준
    'vix_thresholds': [15, 20, 25, 35],  # VIX 임계값
    'put_call_ratio_thresholds': [0.7, 0.9, 1.2, 1.5],  # Put/Call Ratio 임계값
    'high_low_index_thresholds': [20, 30, 50, 70],  # High-Low Index 임계값
    'advance_decline_thresholds': [-50, -20, 20, 50],  # Advance-Decline 추세 임계값
    'biotech_return_thresholds': [-15, 0, 3, 10],  # 바이오텍 지수 월간 수익률 임계값
}

# 주도주 투자 전략 기준
LEADER_STOCK_CRITERIA = {
    # 시장 단계별 기준
    'stage1': {  # 공포 완화 시점
        'rsi_threshold': 30,  # RSI 과매도 탈출 기준
        'volume_surge': 2.0,  # 거래량 급증 기준 (20일 평균 대비)
    },
    'stage2': {  # 본격 상승장
        'sma_period': 30,  # 주 이동평균 기간 (30주 = 약 150일)
        'bollinger_breakout': True,  # 볼린저 밴드 상단 돌파 확인
        'volume_threshold': 1.5,  # 거래량 기준 (20일 평균 대비)
    },
    'stage3': {  # 과열 구간
        'rsi_overbought': 70,  # RSI 과매수 기준
        'volume_explosion': 5.0,  # 거래량 폭발 기준 (20일 평균 대비)
        'momentum_threshold': 10,  # 5일간 상승률 기준 (%)
    },
    'stage4': {  # 어깨 구간 (하락장)
        'price_decline_min': 5,  # 52주 신고가 대비 최소 하락률 (%)
        'price_decline_max': 10,  # 52주 신고가 대비 최대 하락률 (%)
    },
}

# 상승 모멘텀 신호 전략 기준
MOMENTUM_SIGNALS_CRITERIA = {
    # 모멘텀 점수 기준 (Weinstein Stage 2A 조건 반영)
    'min_momentum_score': 10,  # 최소 모멘텀 점수
    'min_core_signals': 4,    # 최소 핵심 신호 수
    
    # 기술적 지표 기준
    'rsi_uptrend': 50,        # RSI 상승 추세 기준
    'adx_strong_trend': 25,   # ADX 강한 추세 기준
    'volume_surge': 1.5,      # 거래량 급증 기준 (20일 평균 대비)
    'price_momentum': 5,      # 5일간 상승률 기준 (%)
    'ma_distance': 8,         # 20일 이동평균선 대비 이격도 기준 (%)
}

# IPO 투자 전략 기준
IPO_INVESTMENT_CRITERIA = {
    # IPO 기간 기준
    'min_days': 30,           # 최소 상장 일수
    'max_days': 365,          # 최대 상장 일수 (1년)
    
    # 베이스 패턴 기준
    'base_min_score': 3,      # 최소 베이스 패턴 점수
    'base_price_range': 15,   # 베이스 형성 중 가격 변동 범위 (%)
    'price_to_high_ratio': 0.7, # IPO 이후 고점 대비 현재가 비율
    
    # 브레이크아웃 기준
    'breakout_min_score': 3,  # 최소 브레이크아웃 점수
    'volume_surge': 2.0,      # 거래량 급증 기준 (20일 평균 대비)
    'daily_gain': 2,          # 당일 상승률 기준 (%)
    'rsi_threshold': 50,      # RSI 기준
}
