import os
import pandas as pd
import numpy as np
import yfinance as yf
from yahooquery import Ticker
import requests
import time
from datetime import datetime, timedelta
from config import (
    YAHOO_FINANCE_MAX_RETRIES, YAHOO_FINANCE_DELAY,
    ADVANCED_FINANCIAL_CRITERIA
)
from .financial_calculators import (
    calculate_eps_metrics, calculate_revenue_metrics, calculate_margin_metrics,
    calculate_financial_ratios, merge_financial_metrics
)

__all__ = [
    "collect_financial_data",
    "collect_financial_data_yahooquery",
    "collect_financial_data_hybrid",
]




def collect_financial_data(symbols, max_retries=YAHOO_FINANCE_MAX_RETRIES, delay=YAHOO_FINANCE_DELAY):
    """yfinance를 사용하여 재무 데이터 수집"""
    print("\n💰 yfinance를 사용한 재무 데이터 수집 시작...")
    financial_data = []
    total = len(symbols)
    
    for i, symbol in enumerate(symbols):
        print(f"진행 중: {i+1}/{total} - {symbol}")
        
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(symbol)
                time.sleep(delay)
                
                income_quarterly = ticker.quarterly_financials
                income_annual = ticker.financials
                balance_annual = ticker.balance_sheet
                
                # 기본 데이터 구조 생성
                data = {'symbol': symbol}
                
                # 기본 재무 데이터 유효성 검사
                if (income_quarterly is None or income_annual is None or balance_annual is None or
                    income_quarterly.empty or income_annual.empty or balance_annual.empty):
                    data['has_error'] = True
                    data['error_details'].append('기본 재무 데이터 없음')
                    financial_data.append(data)
                    break
                
                # 각종 재무 지표 계산
                eps_metrics = calculate_eps_metrics(income_quarterly, income_annual)
                revenue_metrics = calculate_revenue_metrics(income_quarterly, income_annual)
                margin_metrics = calculate_margin_metrics(income_quarterly, income_annual)
                ratio_metrics = calculate_financial_ratios(income_annual, balance_annual)
                
                # 모든 지표를 데이터에 병합
                data.update(merge_financial_metrics(eps_metrics, revenue_metrics, margin_metrics, ratio_metrics))
                
                financial_data.append(data)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    print(f"❌ {symbol}: API 호출 실패 - {str(e)[:100]}")
                    continue
    
    return pd.DataFrame(financial_data)

def collect_financial_data_yahooquery(symbols, max_retries=2, delay=1.0):
    """yahooquery를 사용하여 재무 데이터 수집"""
    print("\n💰 yahooquery를 사용한 재무 데이터 수집 시작...")
    financial_data = []
    total = len(symbols)
    
    for i, symbol in enumerate(symbols):
        print(f"진행 중: {i+1}/{total} - {symbol}")
        
        for attempt in range(max_retries):
            try:
                ticker = Ticker(symbol)
                time.sleep(delay)
                
                # yahooquery로 재무 데이터 수집
                income_stmt_q = ticker.income_statement(frequency='quarterly')
                income_stmt_a = ticker.income_statement(frequency='annual')
                balance_sheet = ticker.balance_sheet(frequency='annual')
                
                # 기본 데이터 구조 생성
                data = {'symbol': symbol}
                
                # yahooquery 데이터를 yfinance 형식으로 변환하여 계산 함수 재사용
                # 간단한 매핑만 수행 (yahooquery는 제한적인 데이터만 제공)
                if isinstance(income_stmt_q, pd.DataFrame) and not income_stmt_q.empty:
                    if 'TotalRevenue' in income_stmt_q.columns:
                        revenue_data = income_stmt_q['TotalRevenue'].dropna()
                        if len(revenue_data) >= 2:
                            recent_revenue = revenue_data.iloc[0]
                            prev_revenue = revenue_data.iloc[1]
                            if prev_revenue != 0:
                                if prev_revenue > 0:
                                    data['quarterly_revenue_growth'] = ((recent_revenue - prev_revenue) / prev_revenue) * 100
                                else:
                                    # 매출은 일반적으로 음수가 되지 않으므로 기존 로직 유지
                                    data['quarterly_revenue_growth'] = ((recent_revenue - prev_revenue) / abs(prev_revenue)) * 100
                    
                    if 'NetIncome' in income_stmt_q.columns:
                        net_income_data = income_stmt_q['NetIncome'].dropna()
                        if len(net_income_data) >= 2:
                            recent_ni = net_income_data.iloc[0]
                            prev_ni = net_income_data.iloc[1]
                            if prev_ni != 0:
                                if prev_ni > 0:
                                    data['quarterly_net_income_growth'] = ((recent_ni - prev_ni) / prev_ni) * 100
                                else:
                                    if recent_ni >= 0:
                                        data['quarterly_net_income_growth'] = 200  # 흑자 전환
                                    else:
                                        data['quarterly_net_income_growth'] = ((recent_ni - prev_ni) / abs(prev_ni)) * 100
                
                # ROE 및 부채비율 계산
                if isinstance(balance_sheet, pd.DataFrame) and not balance_sheet.empty:
                    if ('StockholdersEquity' in balance_sheet.columns and 
                        isinstance(income_stmt_a, pd.DataFrame) and 'NetIncome' in income_stmt_a.columns):
                        equity = balance_sheet['StockholdersEquity'].dropna()
                        net_income = income_stmt_a['NetIncome'].dropna()
                        if len(equity) > 0 and len(net_income) > 0 and equity.iloc[0] != 0:
                            data['roe'] = (net_income.iloc[0] / equity.iloc[0]) * 100
                    
                    if ('TotalLiabilitiesNetMinorityInterest' in balance_sheet.columns and 
                        'StockholdersEquity' in balance_sheet.columns):
                        debt = balance_sheet['TotalLiabilitiesNetMinorityInterest'].dropna()
                        equity = balance_sheet['StockholdersEquity'].dropna()
                        if len(debt) > 0 and len(equity) > 0 and equity.iloc[0] != 0:
                            data['debt_to_equity'] = debt.iloc[0] / equity.iloc[0]
                
                financial_data.append(data)
                break
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    print(f"❌ {symbol}: yahooquery API 호출 실패 - {str(e)[:100]}")
                    continue
    
    return pd.DataFrame(financial_data)


def collect_financial_data_hybrid(symbols, max_retries=2, delay=1.0):
    """yfinance와 yahooquery를 함께 사용하는 하이브리드 방식"""
    print("\n🔄 하이브리드 방식으로 재무 데이터 수집 시작...")
    financial_data = []
    total = len(symbols)
    
    for i, symbol in enumerate(symbols):
        print(f"진행 중: {i+1}/{total} - {symbol}")
        
        for attempt in range(max_retries):
            try:
                # yfinance로 먼저 시도
                ticker_yf = yf.Ticker(symbol)
                time.sleep(delay)
                
                # 기본 데이터 구조 생성
                data = {
                    'symbol': symbol,
                    'error_details': [],
                    'has_error': False
                }
                
                # yfinance로 EPS 관련 지표 계산
                try:
                    income_quarterly = ticker_yf.quarterly_financials
                    income_annual = ticker_yf.financials
                    balance_annual = ticker_yf.balance_sheet
                    
                    if (income_quarterly is not None and not income_quarterly.empty and
                        income_annual is not None and not income_annual.empty and
                        balance_annual is not None and not balance_annual.empty):
                        
                        # 각종 재무 지표 계산
                        eps_metrics = calculate_eps_metrics(income_quarterly, income_annual)
                        revenue_metrics = calculate_revenue_metrics(income_quarterly, income_annual)
                        margin_metrics = calculate_margin_metrics(income_quarterly, income_annual)
                        ratio_metrics = calculate_financial_ratios(income_annual, balance_annual)
                        
                        # 모든 지표를 데이터에 병합
                        data.update(merge_financial_metrics(eps_metrics, revenue_metrics, margin_metrics, ratio_metrics))
                        
                except Exception as e:
                    data['error_details'].append(f'yfinance 계산 실패: {str(e)[:50]}')
                
                # yahooquery로 부족한 데이터 보완
                try:
                    ticker_yq = Ticker(symbol)
                    income_stmt_q = ticker_yq.income_statement(frequency='quarterly')
                    income_stmt_a = ticker_yq.income_statement(frequency='annual')
                    balance_sheet = ticker_yq.balance_sheet(frequency='annual')
                    
                    # 매출 관련 지표 계산
                    if isinstance(income_stmt_q, pd.DataFrame) and not income_stmt_q.empty:
                        # yahooquery 데이터를 yfinance 형식으로 변환
                        yq_quarterly_financials = income_stmt_q.T if 'TotalRevenue' in income_stmt_q.columns else None
                        if yq_quarterly_financials is not None:
                            revenue_metrics = calculate_revenue_metrics(yq_quarterly_financials, None)
                            data.update(revenue_metrics)
                    
                    # 재무 비율 계산
                    if (isinstance(balance_sheet, pd.DataFrame) and not balance_sheet.empty and
                        isinstance(income_stmt_a, pd.DataFrame) and not income_stmt_a.empty):
                        # yahooquery 데이터를 yfinance 형식으로 변환
                        yq_balance_sheet = balance_sheet.T
                        yq_financials = income_stmt_a.T
                        financial_ratios = calculate_financial_ratios(yq_financials, yq_balance_sheet)
                        data.update(financial_ratios)
                        
                except Exception as e:
                    data['error_details'].append(f'yahooquery 보완 데이터 실패: {str(e)[:50]}')
                
                # 에러가 있으면 has_error 플래그 설정
                if data.get('error_details') and len(data['error_details']) > 0:
                    data['has_error'] = True
                else:
                    data['has_error'] = False
                
                financial_data.append(data)
                break
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    print(f"❌ {symbol}: 하이브리드 API 호출 실패 - {str(e)[:100]}")
                    continue
    
    return pd.DataFrame(financial_data)

