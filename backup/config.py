# -*- coding: utf-8 -*-
# 투자 스크리너 - 설정 파일

import os

# 기본 디렉토리 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')  # 데이터 디렉토리
RESULTS_DIR = os.path.join(BASE_DIR, 'results')  # 결과 디렉토리
#RESULTS2_DIR = os.path.join(BASE_DIR, 'results2')  # 재무 분석 결과 디렉토리
RESULTS_VER2_DIR = os.path.join(BASE_DIR, 'results_ver2')  # 새로운 스크리닝 결과 디렉토리
DATA_US_DIR = os.path.join(DATA_DIR, 'us')  # 미국 주식 데이터 디렉토리
BACKUP_DIR = os.path.join(BASE_DIR, 'backup')  # 백업 디렉토리
MARKMINERVINI_DIR = os.path.join(BASE_DIR, 'Markminervini')  # Markminervini 디렉토리

# 파일 경로 설정
US_WITH_RS_PATH = os.path.join(RESULTS_DIR, 'us_with_rs.csv')  # 미국 주식 RS 점수 결과
ADVANCED_FINANCIAL_RESULTS_PATH = os.path.join(RESULTS_DIR, 'advanced_financial_results.csv')  # 고급 재무 분석 결과
INTEGRATED_RESULTS_PATH = os.path.join(RESULTS_DIR, 'integrated_results.csv')  # 통합 결과

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

# API 설정
YAHOO_FINANCE_MAX_RETRIES = 3  # Yahoo Finance API 최대 재시도 횟수
YAHOO_FINANCE_DELAY = 1        # Yahoo Finance API 재시도 간 지연 시간(초)