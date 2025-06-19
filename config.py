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
MARKMINERVINI_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'markminervini')
QULLAMAGGIE_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'qullamaggie')
US_GAINER_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'us_gainer')
US_SETUP_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'us_setup')
OPTION_VOLATILITY_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'option_volatility')
DATA_US_DIR = os.path.join(DATA_DIR, 'us')  # 미국 주식 데이터 디렉토리
BACKUP_DIR = os.path.join(BASE_DIR, 'backup')  # 백업 디렉토리
SCREENERS_DIR = os.path.join(BASE_DIR, 'screeners')
MARKMINERVINI_DIR = os.path.join(SCREENERS_DIR, 'markminervini')
QULLAMAGGIE_DIR = os.path.join(SCREENERS_DIR, 'qullamaggie')
US_GAINER_DIR = os.path.join(SCREENERS_DIR, 'us_gainer')
US_SETUP_DIR = os.path.join(SCREENERS_DIR, 'us_setup')
OPTION_VOLATILITY_DIR = os.path.join(SCREENERS_DIR, 'option_volatility')

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
