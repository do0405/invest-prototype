# -*- coding: utf-8 -*-
# 투자 스크리너 - 설정 파일

import os

# 기본 디렉토리 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')  # 데이터 디렉토리
RESULTS_DIR = os.path.join(BASE_DIR, 'results')  # 결과 디렉토리
SCREENER_RESULTS_DIR = os.path.join(RESULTS_DIR, 'screeners')
OPTION_RESULTS_DIR = os.path.join(RESULTS_DIR, 'option')


MARKMINERVINI_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'markminervini')
QULLAMAGGIE_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, 'qullamaggie')
LEADER_STOCK_RESULTS_DIR = os.path.join(RESULTS_DIR, 'leader_stock')
MOMENTUM_SIGNALS_RESULTS_DIR = os.path.join(RESULTS_DIR, 'momentum_signals')
RANKING_RESULTS_DIR = os.path.join(RESULTS_DIR, 'ranking')
DATA_US_DIR = os.path.join(DATA_DIR, 'us')  # 미국 주식 데이터 디렉토리
# 시장 폭(Breadth) 지표 및 옵션 데이터 디렉토리
BREADTH_DATA_DIR = os.path.join(DATA_DIR, 'breadth')
OPTION_DATA_DIR = os.path.join(DATA_DIR, 'options')
BACKUP_DIR = os.path.join(BASE_DIR, 'backup')  # 백업 디렉토리
SCREENERS_DIR = os.path.join(BASE_DIR, 'screeners')
MARKMINERVINI_DIR = os.path.join(SCREENERS_DIR, 'markminervini')
QULLAMAGGIE_DIR = os.path.join(SCREENERS_DIR, 'qullamaggie')
LEADER_STOCK_DIR = os.path.join(SCREENERS_DIR, 'leader_stock')
MOMENTUM_SIGNALS_DIR = os.path.join(SCREENERS_DIR, 'momentum_signals')

# 파일 경로 설정
US_WITH_RS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'us_with_rs.csv')
ADVANCED_FINANCIAL_RESULTS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'advanced_financial_results.csv')
INTEGRATED_RESULTS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'integrated_results.csv')

# 주식 메타데이터 (섹터, P/E, 매출 성장률 등)
STOCK_METADATA_PATH = os.path.join(DATA_DIR, 'stock_metadata.csv')

# 스크리닝 조건 설정
TECHNICAL_CONDITION_COUNT = 8  # 기술적 조건 수
FINANCIAL_CONDITION_COUNT = 9  # 재무제표 조건 수
ADVANCED_FINANCIAL_MIN_MET = 4  # 최소 충족해야 할 재무 조건 수

# 재무제표 스크리닝 기준
ADVANCED_FINANCIAL_CRITERIA = {
    # bullet points in ``prompts/to make it better.md`` 기준
    'min_annual_eps_growth': 20,      # 연간 EPS 성장률 ≥ 20%
    'min_annual_revenue_growth': 15,  # 연간 매출 성장률 ≥ 15%
    'max_debt_to_equity': 150         # 부채비율 ≤ 150%
}

# API 설정
YAHOO_FINANCE_MAX_RETRIES = 3  # Yahoo Finance API 최대 재시도 횟수
YAHOO_FINANCE_DELAY = 1        # Yahoo Finance API 재시도 간 지연 시간(초)

# 옵션 데이터 소스 우선순위
OPTION_DATA_SOURCES = [
    "yfinance",
    "exclusion",
]

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
