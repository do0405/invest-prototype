# -*- coding: utf-8 -*-
# 투자 스크리너 - 고급 재무제표 스크리닝 모듈

import os
import sys
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import time
import traceback
from config import (
    BASE_DIR, DATA_DIR, RESULTS_DIR,
    US_WITH_RS_PATH, ADVANCED_FINANCIAL_RESULTS_PATH,
    ADVANCED_FINANCIAL_CRITERIA,
    YAHOO_FINANCE_MAX_RETRIES, YAHOO_FINANCE_DELAY
)

# 유틸리티 함수 임포트
try:
    from utils import ensure_dir
except Exception as e:
    # 간단한 디렉토리 생성 함수 정의
    def ensure_dir(path):
        os.makedirs(path, exist_ok=True)

# 필요한 디렉토리 생성
ensure_dir(RESULTS_DIR)

# 재무제표 데이터 수집 함수
def collect_financial_data(symbols, max_retries=YAHOO_FINANCE_MAX_RETRIES, delay=YAHOO_FINANCE_DELAY):
    """
    yfinance API를 사용하여 필요한 재무제표 데이터만 효율적으로 수집하는 함수
    
    Args:
        symbols: 수집할 종목 리스트
        max_retries: API 호출 최대 재시도 횟수
        delay: 재시도 간 지연 시간(초)
        
    Returns:
        DataFrame: 수집된 재무제표 데이터
    """
    print("\n💰 재무제표 데이터 수집 시작...")
    
    financial_data = []
    total = len(symbols)
    
    for i, symbol in enumerate(symbols):
        print(f"진행 중: {i+1}/{total} - {symbol}")
        
        # API 호출 재시도 로직
        for attempt in range(max_retries):
            try:
                # yfinance 객체 생성
                ticker = yf.Ticker(symbol)
                
                # 필요한 데이터만 가져오기
                income_quarterly = ticker.quarterly_financials
                income_annual = ticker.financials
                balance_annual = ticker.balance_sheet
                
                # 기본 데이터 구조 생성
                data = {
                    'symbol': symbol,
                    'has_error': False,
                    'error_details': [],
                    'quarterly_eps_growth': 0,
                    'annual_eps_growth': 0,
                    'eps_growth_acceleration': False,
                    'quarterly_revenue_growth': 0,
                    'annual_revenue_growth': 0,
                    'quarterly_op_margin_improved': False,
                    'annual_op_margin_improved': False,
                    'quarterly_net_income_growth': 0,
                    'annual_net_income_growth': 0,
                    'roe': 0,
                    'debt_to_equity': 0,
                    'last_updated': datetime.now().strftime('%Y-%m-%d')
                }
                
                # 데이터 유효성 검사
                if (income_quarterly is None or income_annual is None or balance_annual is None or
                    income_quarterly.empty or income_annual.empty or balance_annual.empty):
                    data['has_error'] = True
                    data['error_details'].append('기본 재무 데이터 없음')
                    financial_data.append(data)
                    break
                
                # 1. EPS 성장률 계산
                try:
                    if ('Basic EPS' in income_quarterly.index and 
                        len(income_quarterly) >= 2 and 
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[0]) and 
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[1])):
                        recent_quarterly_eps = income_quarterly.loc['Basic EPS'].iloc[0]
                        prev_quarterly_eps = income_quarterly.loc['Basic EPS'].iloc[1]
                        data['quarterly_eps_growth'] = ((recent_quarterly_eps - prev_quarterly_eps) / abs(prev_quarterly_eps)) * 100 if prev_quarterly_eps != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'EPS 계산 오류: {str(e)[:100]}')
                
                try:
                    if ('Basic EPS' in income_annual.index and 
                        len(income_annual) >= 2 and 
                        not pd.isna(income_annual.loc['Basic EPS'].iloc[0]) and 
                        not pd.isna(income_annual.loc['Basic EPS'].iloc[1])):
                        recent_annual_eps = income_annual.loc['Basic EPS'].iloc[0]
                        prev_annual_eps = income_annual.loc['Basic EPS'].iloc[1]
                        data['annual_eps_growth'] = ((recent_annual_eps - prev_annual_eps) / abs(prev_annual_eps)) * 100 if prev_annual_eps != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'연간 EPS 계산 오류: {str(e)[:100]}')
                
                try:
                    if ('Basic EPS' in income_quarterly.index and 
                        len(income_quarterly) >= 3 and 
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[0]) and 
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[1]) and 
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[2])):
                        growth_1 = ((income_quarterly.loc['Basic EPS'].iloc[0] - income_quarterly.loc['Basic EPS'].iloc[1]) / 
                                   abs(income_quarterly.loc['Basic EPS'].iloc[1])) * 100 if income_quarterly.loc['Basic EPS'].iloc[1] != 0 else 0
                        growth_2 = ((income_quarterly.loc['Basic EPS'].iloc[1] - income_quarterly.loc['Basic EPS'].iloc[2]) / 
                                   abs(income_quarterly.loc['Basic EPS'].iloc[2])) * 100 if income_quarterly.loc['Basic EPS'].iloc[2] != 0 else 0
                        data['eps_growth_acceleration'] = growth_1 > growth_2
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'EPS 가속화 계산 오류: {str(e)[:100]}')
                
                # 2. 매출 성장률 계산
                try:
                    if ('Total Revenue' in income_quarterly.index and 
                        len(income_quarterly) >= 2 and 
                        not pd.isna(income_quarterly.loc['Total Revenue'].iloc[0]) and 
                        not pd.isna(income_quarterly.loc['Total Revenue'].iloc[1])):
                        recent_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[0]
                        prev_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[1]
                        data['quarterly_revenue_growth'] = ((recent_quarterly_revenue - prev_quarterly_revenue) / abs(prev_quarterly_revenue)) * 100 if prev_quarterly_revenue != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'매출 성장률 계산 오류: {str(e)[:100]}')
                
                try:
                    if ('Total Revenue' in income_annual.index and 
                        len(income_annual) >= 2 and 
                        not pd.isna(income_annual.loc['Total Revenue'].iloc[0]) and 
                        not pd.isna(income_annual.loc['Total Revenue'].iloc[1])):
                        recent_annual_revenue = income_annual.loc['Total Revenue'].iloc[0]
                        prev_annual_revenue = income_annual.loc['Total Revenue'].iloc[1]
                        data['annual_revenue_growth'] = ((recent_annual_revenue - prev_annual_revenue) / abs(prev_annual_revenue)) * 100 if prev_annual_revenue != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'연간 매출 성장률 계산 오류: {str(e)[:100]}')
                
                # 3. 영업이익률 계산
                try:
                    if ('Operating Income' in income_quarterly.index and 'Total Revenue' in income_quarterly.index and
                        len(income_quarterly) >= 2 and
                        not pd.isna(income_quarterly.loc['Operating Income'].iloc[0]) and
                        not pd.isna(income_quarterly.loc['Total Revenue'].iloc[0]) and
                        not pd.isna(income_quarterly.loc['Operating Income'].iloc[1]) and
                        not pd.isna(income_quarterly.loc['Total Revenue'].iloc[1])):
                        recent_quarterly_op_income = income_quarterly.loc['Operating Income'].iloc[0]
                        recent_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[0]
                        recent_quarterly_op_margin = (recent_quarterly_op_income / recent_quarterly_revenue) * 100 if recent_quarterly_revenue != 0 else 0
                        
                        prev_quarterly_op_income = income_quarterly.loc['Operating Income'].iloc[1]
                        prev_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[1]
                        prev_quarterly_op_margin = (prev_quarterly_op_income / prev_quarterly_revenue) * 100 if prev_quarterly_revenue != 0 else 0
                        
                        data['quarterly_op_margin_improved'] = recent_quarterly_op_margin > prev_quarterly_op_margin
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'영업이익률 계산 오류: {str(e)[:100]}')
                
                try:
                    if ('Operating Income' in income_annual.index and 'Total Revenue' in income_annual.index and
                        len(income_annual) >= 2 and
                        not pd.isna(income_annual.loc['Operating Income'].iloc[0]) and
                        not pd.isna(income_annual.loc['Total Revenue'].iloc[0]) and
                        not pd.isna(income_annual.loc['Operating Income'].iloc[1]) and
                        not pd.isna(income_annual.loc['Total Revenue'].iloc[1])):
                        recent_annual_op_income = income_annual.loc['Operating Income'].iloc[0]
                        recent_annual_revenue = income_annual.loc['Total Revenue'].iloc[0]
                        recent_annual_op_margin = (recent_annual_op_income / recent_annual_revenue) * 100 if recent_annual_revenue != 0 else 0
                        
                        prev_annual_op_income = income_annual.loc['Operating Income'].iloc[1]
                        prev_annual_revenue = income_annual.loc['Total Revenue'].iloc[1]
                        prev_annual_op_margin = (prev_annual_op_income / prev_annual_revenue) * 100 if prev_annual_revenue != 0 else 0
                        
                        data['annual_op_margin_improved'] = recent_annual_op_margin > prev_annual_op_margin
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'연간 영업이익률 계산 오류: {str(e)[:100]}')
                
                # 4. 순이익 성장률 계산
                try:
                    if ('Net Income' in income_quarterly.index and 
                        len(income_quarterly) >= 2 and 
                        not pd.isna(income_quarterly.loc['Net Income'].iloc[0]) and 
                        not pd.isna(income_quarterly.loc['Net Income'].iloc[1])):
                        recent_quarterly_net_income = income_quarterly.loc['Net Income'].iloc[0]
                        prev_quarterly_net_income = income_quarterly.loc['Net Income'].iloc[1]
                        data['quarterly_net_income_growth'] = ((recent_quarterly_net_income - prev_quarterly_net_income) / abs(prev_quarterly_net_income)) * 100 if prev_quarterly_net_income != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'순이익 성장률 계산 오류: {str(e)[:100]}')
                
                try:
                    if ('Net Income' in income_annual.index and 
                        len(income_annual) >= 2 and 
                        not pd.isna(income_annual.loc['Net Income'].iloc[0]) and 
                        not pd.isna(income_annual.loc['Net Income'].iloc[1])):
                        recent_annual_net_income = income_annual.loc['Net Income'].iloc[0]
                        prev_annual_net_income = income_annual.loc['Net Income'].iloc[1]
                        data['annual_net_income_growth'] = ((recent_annual_net_income - prev_annual_net_income) / abs(prev_annual_net_income)) * 100 if prev_annual_net_income != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'연간 순이익 성장률 계산 오류: {str(e)[:100]}')
                
                # 5. ROE와 부채비율 계산
                try:
                    if ('Net Income' in income_annual.index and 'Total Stockholder Equity' in balance_annual.index and
                        not pd.isna(income_annual.loc['Net Income'].iloc[0]) and
                        not pd.isna(balance_annual.loc['Total Stockholder Equity'].iloc[0])):
                        net_income = income_annual.loc['Net Income'].iloc[0]
                        equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
                        data['roe'] = (net_income / equity) * 100 if equity != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'ROE 계산 오류: {str(e)[:100]}')
                
                try:
                    if ('Total Liabilities' in balance_annual.index and 'Total Stockholder Equity' in balance_annual.index and
                        not pd.isna(balance_annual.loc['Total Liabilities'].iloc[0]) and
                        not pd.isna(balance_annual.loc['Total Stockholder Equity'].iloc[0])):
                        total_liabilities = balance_annual.loc['Total Liabilities'].iloc[0]
                        total_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
                        data['debt_to_equity'] = (total_liabilities / total_equity) * 100 if total_equity != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'부채비율 계산 오류: {str(e)[:100]}')
                
                financial_data.append(data)
                break
                
            except Exception as e:
                print(f"⚠️ {symbol} 데이터 수집 오류 (시도 {attempt+1}/{max_retries}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (2 ** attempt))
                else:
                    data = {
                        'symbol': symbol,
                        'has_error': True,
                        'error_details': [f'전체 데이터 수집 실패: {str(e)[:100]}'],
                        'quarterly_eps_growth': 0,
                        'annual_eps_growth': 0,
                        'eps_growth_acceleration': False,
                        'quarterly_revenue_growth': 0,
                        'annual_revenue_growth': 0,
                        'quarterly_op_margin_improved': False,
                        'annual_op_margin_improved': False,
                        'quarterly_net_income_growth': 0,
                        'annual_net_income_growth': 0,
                        'roe': 0,
                        'debt_to_equity': 0,
                        'last_updated': datetime.now().strftime('%Y-%m-%d')
                    }
                    financial_data.append(data)
    
    # 데이터프레임 생성
    if financial_data:
        df = pd.DataFrame(financial_data)
        print(f"✅ 재무제표 데이터 수집 완료: {len(df)}개 종목")
        return df
    else:
        print("❌ 수집된 재무제표 데이터가 없습니다.")
        return pd.DataFrame()

# 실제 API를 사용한 데이터 수집 함수 (현재는 사용하지 않음)
def collect_real_financial_data(symbols, max_retries=3, delay=1):
    """
    yfinance API를 사용하여 실제 재무제표 데이터를 수집하는 함수
    
    Args:
        symbols: 수집할 종목 리스트
        max_retries: API 호출 최대 재시도 횟수
        delay: 재시도 간 지연 시간(초)
    """
    print("\n💰 실제 재무제표 데이터 수집 시작...")
    
    financial_data = []
    total = len(symbols)
    
    for i, symbol in enumerate(symbols):
        print(f"진행 중: {i+1}/{total} - {symbol}")
        
        # API 호출 재시도 로직
        for attempt in range(max_retries):
            try:
                # yfinance 객체 생성
                ticker = yf.Ticker(symbol)
                
                # 재무제표 데이터 가져오기
                income_quarterly = ticker.quarterly_income_stmt
                income_annual = ticker.income_stmt
                balance_annual = ticker.balance_sheet
                
                # 데이터가 충분한지 확인
                if (income_quarterly.empty or income_annual.empty or balance_annual.empty or 
                    len(income_quarterly.columns) < 2 or len(income_annual.columns) < 2):
                    print(f"⚠️ {symbol}: 충분한 재무 데이터가 없습니다.")
                    break
                
                # 분기별 EPS 성장률 계산
                try:
                    recent_quarterly_eps = ticker.quarterly_earnings.iloc[-1]['Earnings']
                    prev_quarterly_eps = ticker.quarterly_earnings.iloc[-2]['Earnings']
                    quarterly_eps_growth = ((recent_quarterly_eps - prev_quarterly_eps) / abs(prev_quarterly_eps)) * 100 if prev_quarterly_eps != 0 else 0
                except (IndexError, KeyError, AttributeError):
                    quarterly_eps_growth = 0
                
                # 연간 EPS 성장률 계산
                try:
                    recent_annual_eps = ticker.earnings.iloc[-1]['Earnings']
                    prev_annual_eps = ticker.earnings.iloc[-2]['Earnings']
                    annual_eps_growth = ((recent_annual_eps - prev_annual_eps) / abs(prev_annual_eps)) * 100 if prev_annual_eps != 0 else 0
                except (IndexError, KeyError, AttributeError):
                    annual_eps_growth = 0
                
                # 분기별 EPS 성장 가속화 확인
                try:
                    quarterly_earnings = ticker.quarterly_earnings
                    if len(quarterly_earnings) >= 3:
                        growth_1 = ((quarterly_earnings.iloc[-1]['Earnings'] - quarterly_earnings.iloc[-2]['Earnings']) / 
                                   abs(quarterly_earnings.iloc[-2]['Earnings'])) * 100 if quarterly_earnings.iloc[-2]['Earnings'] != 0 else 0
                        growth_2 = ((quarterly_earnings.iloc[-2]['Earnings'] - quarterly_earnings.iloc[-3]['Earnings']) / 
                                   abs(quarterly_earnings.iloc[-3]['Earnings'])) * 100 if quarterly_earnings.iloc[-3]['Earnings'] != 0 else 0
                        eps_growth_acceleration = growth_1 > growth_2
                    else:
                        eps_growth_acceleration = False
                except (IndexError, KeyError, AttributeError):
                    eps_growth_acceleration = False
                
                # 분기별 매출 성장률 계산
                try:
                    recent_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[0]
                    prev_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[1]
                    quarterly_revenue_growth = ((recent_quarterly_revenue - prev_quarterly_revenue) / abs(prev_quarterly_revenue)) * 100 if prev_quarterly_revenue != 0 else 0
                except (IndexError, KeyError):
                    quarterly_revenue_growth = 0
                
                # 연간 매출 성장률 계산
                try:
                    recent_annual_revenue = income_annual.loc['Total Revenue'].iloc[0]
                    prev_annual_revenue = income_annual.loc['Total Revenue'].iloc[1]
                    annual_revenue_growth = ((recent_annual_revenue - prev_annual_revenue) / abs(prev_annual_revenue)) * 100 if prev_annual_revenue != 0 else 0
                except (IndexError, KeyError):
                    annual_revenue_growth = 0
                
                # 분기별 영업이익률 계산 및 개선 확인
                try:
                    recent_quarterly_operating_income = income_quarterly.loc['Operating Income'].iloc[0]
                    prev_quarterly_operating_income = income_quarterly.loc['Operating Income'].iloc[1]
                    recent_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[0]
                    prev_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[1]
                    
                    recent_quarterly_op_margin = (recent_quarterly_operating_income / recent_quarterly_revenue) * 100 if recent_quarterly_revenue != 0 else 0
                    prev_quarterly_op_margin = (prev_quarterly_operating_income / prev_quarterly_revenue) * 100 if prev_quarterly_revenue != 0 else 0
                    
                    quarterly_op_margin_improved = recent_quarterly_op_margin > prev_quarterly_op_margin
                except (IndexError, KeyError):
                    recent_quarterly_op_margin = 0
                    prev_quarterly_op_margin = 0
                    quarterly_op_margin_improved = False
                
                # 연간 영업이익률 계산 및 개선 확인
                try:
                    recent_annual_operating_income = income_annual.loc['Operating Income'].iloc[0]
                    prev_annual_operating_income = income_annual.loc['Operating Income'].iloc[1]
                    recent_annual_revenue = income_annual.loc['Total Revenue'].iloc[0]
                    prev_annual_revenue = income_annual.loc['Total Revenue'].iloc[1]
                    
                    recent_annual_op_margin = (recent_annual_operating_income / recent_annual_revenue) * 100 if recent_annual_revenue != 0 else 0
                    prev_annual_op_margin = (prev_annual_operating_income / prev_annual_revenue) * 100 if prev_annual_revenue != 0 else 0
                    
                    annual_op_margin_improved = recent_annual_op_margin > prev_annual_op_margin
                except (IndexError, KeyError):
                    recent_annual_op_margin = 0
                    prev_annual_op_margin = 0
                    annual_op_margin_improved = False
                
                # 분기별 순이익 성장률 계산
                try:
                    recent_quarterly_net_income = income_quarterly.loc['Net Income'].iloc[0]
                    prev_quarterly_net_income = income_quarterly.loc['Net Income'].iloc[1]
                    quarterly_net_income_growth = ((recent_quarterly_net_income - prev_quarterly_net_income) / abs(prev_quarterly_net_income)) * 100 if prev_quarterly_net_income != 0 else 0
                except (IndexError, KeyError):
                    quarterly_net_income_growth = 0
                
                # 연간 순이익 성장률 계산
                try:
                    recent_annual_net_income = income_annual.loc['Net Income'].iloc[0]
                    prev_annual_net_income = income_annual.loc['Net Income'].iloc[1]
                    annual_net_income_growth = ((recent_annual_net_income - prev_annual_net_income) / abs(prev_annual_net_income)) * 100 if prev_annual_net_income != 0 else 0
                except (IndexError, KeyError):
                    annual_net_income_growth = 0
                
                # ROE 계산
                try:
                    recent_annual_net_income = income_annual.loc['Net Income'].iloc[0]
                    recent_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
                    roe = (recent_annual_net_income / recent_equity) * 100 if recent_equity != 0 else 0
                except (IndexError, KeyError):
                    roe = 0
                
                # 부채비율 계산
                try:
                    total_liabilities = balance_annual.loc['Total Liabilities'].iloc[0]
                    total_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
                    debt_to_equity = (total_liabilities / total_equity) * 100 if total_equity != 0 else 0
                except (IndexError, KeyError):
                    debt_to_equity = 0
                
                # 데이터 저장
                financial_data.append({
                    'symbol': symbol,
                    'quarterly_eps_growth': quarterly_eps_growth,
                    'annual_eps_growth': annual_eps_growth,
                    'eps_growth_acceleration': eps_growth_acceleration,
                    'quarterly_revenue_growth': quarterly_revenue_growth,
                    'annual_revenue_growth': annual_revenue_growth,
                    'quarterly_op_margin_improved': quarterly_op_margin_improved,
                    'annual_op_margin_improved': annual_op_margin_improved,
                    'quarterly_net_income_growth': quarterly_net_income_growth,
                    'annual_net_income_growth': annual_net_income_growth,
                    'roe': roe,
                    'debt_to_equity': debt_to_equity,
                    'last_updated': datetime.now().strftime('%Y-%m-%d')
                })
                
                # 성공적으로 데이터를 가져왔으므로 재시도 루프 종료
                break
                
            except Exception as e:
                print(f"⚠️ {symbol} 데이터 수집 오류 (시도 {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay)  # API 호출 제한 방지를 위한 지연
                else:
                    print(f"❌ {symbol} 데이터 수집 실패")
    
    # 데이터프레임 생성
    if financial_data:
        df = pd.DataFrame(financial_data)
        print(f"✅ 재무제표 데이터 수집 완료: {len(df)}개 종목")
        return df
    else:
        print("❌ 수집된 재무제표 데이터가 없습니다.")
        return pd.DataFrame()

# 고급 재무제표 스크리닝 함수
def screen_advanced_financials(financial_data):
    """
    고급 재무제표 기반 스크리닝 실행
    
    Args:
        financial_data: 재무제표 데이터프레임
        
    Returns:
        pd.DataFrame: 스크리닝 결과 데이터프레임
    """
    print("\n📊 고급 재무제표 스크리닝 시작...")
    
    if financial_data.empty:
        print("❌ 재무제표 데이터가 비어 있습니다.")
        return pd.DataFrame()
    
    # 조건 적용
    result_df = financial_data.copy()
    
    # 각 조건별로 독립적으로 계산
    # 조건 1: 최근 분기 EPS 성장률 ≥ +20%
    result_df['fin_cond1'] = result_df['quarterly_eps_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_eps_growth']
    
    # 조건 2: 최근 연간 EPS 성장률 ≥ +20%
    result_df['fin_cond2'] = result_df['annual_eps_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_eps_growth']
    
    # 조건 3: 최근 2개 이상 분기 EPS 성장률 증가 (가속화)
    result_df['fin_cond3'] = result_df['eps_growth_acceleration']
    
    # 조건 4: 최근 분기 매출 성장률 ≥ +20%
    result_df['fin_cond4'] = result_df['quarterly_revenue_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_revenue_growth']
    
    # 조건 5: 최근 연간 매출 성장률 ≥ +20%
    result_df['fin_cond5'] = result_df['annual_revenue_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_revenue_growth']
    
    # 조건 6: 최근 분기 영업이익률 > 직전 분기 영업이익률
    result_df['fin_cond6'] = result_df['quarterly_op_margin_improved']
    
    # 조건 7: 최근 연간 영업이익률 > 전년도 영업이익률
    result_df['fin_cond7'] = result_df['annual_op_margin_improved']
    
    # 조건 8: 최근 분기 순이익 증가율 ≥ +20%
    result_df['fin_cond8'] = result_df['quarterly_net_income_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_net_income_growth']
    
    # 조건 9: 최근 연간 순이익 증가율 ≥ +20%
    result_df['fin_cond9'] = result_df['annual_net_income_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_net_income_growth']
    
    # 조건 10: 최근 연간 ROE ≥ 15%
    result_df['fin_cond10'] = result_df['roe'] >= ADVANCED_FINANCIAL_CRITERIA['min_roe']
    
    # 조건 11: 부채비율 ≤ 150%
    result_df['fin_cond11'] = result_df['debt_to_equity'] <= ADVANCED_FINANCIAL_CRITERIA['max_debt_to_equity']
    
    # 충족 조건 수 계산
    condition_cols = [f'fin_cond{i}' for i in range(1, 12)]
    result_df['fin_met_count'] = result_df[condition_cols].sum(axis=1)
    
    # 결과 저장
    ensure_dir(RESULTS_DIR)
    
    # 최종 결과 컬럼 선택 (간소화된 버전)
    final_columns = ['symbol', 'fin_met_count', 'has_error']
    result_df = result_df[final_columns]
    
    return result_df

def calculate_percentile_rank(series):
    """
    시리즈의 각 값에 대한 백분위 순위를 계산하는 함수
    낮은 값일수록 높은 백분위(하위 %)를 가짐
    """
    return series.rank(pct=True) * 100

def run_advanced_financial_screening(force_update=False):
    """고급 재무 분석 실행"""
    print("\n=== 고급 재무 분석 시작 ===")
    
    
    
    try:
        print("\n📊 고급 재무제표 스크리닝 시작...")
        
        # US 주식 데이터 로드
        if not os.path.exists(US_WITH_RS_PATH):
            print(f"❌ US 주식 데이터 파일이 없습니다: {US_WITH_RS_PATH}")
            return

        us_df = pd.read_csv(US_WITH_RS_PATH)
        print(f"✅ US 주식 데이터 로드 완료: {len(us_df)}개 종목")
        
        # 심볼 목록 추출
        if 'symbol' not in us_df.columns:
            print(f"⚠️ 'symbol' 컬럼이 없습니다. 사용 가능한 컬럼: {', '.join(us_df.columns.tolist())}")
            return
        
        # us_with_rs.csv에 있는 종목만 처리
        symbols = us_df['symbol'].tolist()
        
        if not symbols:
            print("❌ 분석할 심볼이 없습니다.")
            return
        
        print(f"📈 분석할 종목 수: {len(symbols)}")
        
        # 재무제표 데이터 수집
        financial_data = collect_financial_data(symbols)
        
        # 재무제표 스크리닝
        if not financial_data.empty:
            # 재무제표 스크리닝 실행
            result_df = screen_advanced_financials(financial_data)
            
            if not result_df.empty:
                # RS 점수 데이터 병합
                if 'rs_score' in us_df.columns:
                    rs_data = us_df[['symbol', 'rs_score']]
                    final_df = pd.merge(result_df, rs_data, on='symbol', how='right')  # right join으로 변경
                    
                    # 각 지표의 하위 백분위 계산 (us_with_rs.csv의 종목들끼리 비교)
                    # RS 점수 백분위 계산
                    rs_percentiles = calculate_percentile_rank(us_df['rs_score'])
                    rs_percentile_dict = dict(zip(us_df['symbol'], rs_percentiles))
                    final_df['rs_percentile'] = final_df['symbol'].map(rs_percentile_dict)
                    
                    # 재무 지표 백분위 계산 (us_with_rs.csv의 종목들끼리 비교)
                    fin_percentiles = calculate_percentile_rank(result_df['fin_met_count'])
                    fin_percentile_dict = dict(zip(result_df['symbol'], fin_percentiles))
                    final_df['fin_percentile'] = final_df['symbol'].map(fin_percentile_dict)
                    
                    # 누락된 값 처리
                    final_df['fin_met_count'] = final_df['fin_met_count'].fillna(0)
                    final_df['has_error'] = final_df['has_error'].fillna(True)
                    final_df['fin_percentile'] = final_df['fin_percentile'].fillna(0)
                    
                    # 백분위 합계 계산
                    final_df['total_percentile'] = final_df['rs_percentile'] + final_df['fin_percentile']
                    
                    # 정렬 기준:
                    # 1. fin_met_count가 11인 종목 우선
                    # 2. total_percentile (내림차순)
                    # 3. rs_score (내림차순)
                    final_df['is_perfect'] = final_df['fin_met_count'] == 11
                    final_df = final_df.sort_values(
                        ['is_perfect', 'total_percentile', 'rs_score'],
                        ascending=[False, False, False]
                    )
                    final_df = final_df.drop('is_perfect', axis=1)  # 임시 컬럼 제거
                    
                    # 결과 저장 (간소화된 컬럼만)
                    final_df.to_csv(ADVANCED_FINANCIAL_RESULTS_PATH, index=False, mode='w')  # mode='w'로 변경하여 덮어쓰기
                    print(f"✅ RS 점수가 포함된 최종 결과 저장 완료: {len(final_df)}개 종목")
                    
                    # 에러가 있는 종목 출력
                    error_df = final_df[final_df['has_error'] == True]
                    if not error_df.empty:
                        print("\n⚠️ 데이터 수집 또는 계산 중 오류가 발생한 종목:")
                        for _, row in error_df.iterrows():
                            print(f"- {row['symbol']}")
                    
                    # 상위 10개 종목 출력
                    top_10 = final_df.head(10)
                    print("\n🏆 상위 10개 종목:")
                    print(top_10[['symbol', 'fin_met_count', 'rs_score', 'total_percentile', 'has_error']])
                else:
                    # RS 점수가 없는 경우
                    result_df.to_csv(ADVANCED_FINANCIAL_RESULTS_PATH, index=False, mode='w')  # mode='w'로 변경하여 덮어쓰기
                    print(f"✅ 재무제표 스크리닝 결과 저장 완료: {len(result_df)}개 종목")
                    
                    # 에러가 있는 종목 출력
                    error_df = result_df[result_df['has_error'] == True]
                    if not error_df.empty:
                        print("\n⚠️ 데이터 수집 또는 계산 중 오류가 발생한 종목:")
                        for _, row in error_df.iterrows():
                            print(f"- {row['symbol']}")
                    
                    # 상위 10개 종목 출력
                    top_10 = result_df.sort_values('fin_met_count', ascending=False).head(10)
                    print("\n🏆 고급 재무제표 스크리닝 상위 10개 종목:")
                    print(top_10[['symbol', 'fin_met_count', 'has_error']])
            else:
                print("❌ 스크리닝 결과가 비어 있습니다.")
        else:
            print("❌ 재무제표 데이터가 없어 스크리닝을 진행할 수 없습니다.")
    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

# 직접 실행 시
if __name__ == "__main__":
    run_advanced_financial_screening()