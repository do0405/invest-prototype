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

__all__ = [
    "collect_financial_data",
    "collect_financial_data_yahooquery",
    "collect_financial_data_hybrid",
]




def collect_financial_data(symbols, max_retries=YAHOO_FINANCE_MAX_RETRIES, delay=YAHOO_FINANCE_DELAY):
    """yfinance API를 사용하여 필요한 재무제표 데이터만 수집"""
    print("\n💰 재무제표 데이터 수집 시작...")
    financial_data = []
    total = len(symbols)
    for i, symbol in enumerate(symbols):
        print(f"진행 중: {i+1}/{total} - {symbol}")
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(symbol)
                income_quarterly = ticker.quarterly_financials
                income_annual = ticker.financials
                balance_annual = ticker.balance_sheet
                data = {
                    'symbol': symbol,
                    'has_error': False,
                    'error_details': [],
                    'quarterly_eps_growth': 0,
                    'annual_eps_growth': 0,
                    'eps_growth_acceleration': False,
                    'revenue_growth_acceleration': False,
                    'net_margin_improved': False,
                    'eps_3q_accel': False,
                    'sales_3q_accel': False,
                    'margin_3q_accel': False,
                    'quarterly_revenue_growth': 0,
                    'annual_revenue_growth': 0,
                    'quarterly_op_margin_improved': False,
                    'annual_op_margin_improved': False,
                    'quarterly_net_income_growth': 0,
                    'annual_net_income_growth': 0,
                    'roe': 0,
                    'debt_to_equity': 0,
                    'last_updated': datetime.now().strftime('%Y-%m-%d'),
                }
                if (
                    income_quarterly is None or income_annual is None or balance_annual is None or
                    income_quarterly.empty or income_annual.empty or balance_annual.empty
                ):
                    data['has_error'] = True
                    data['error_details'].append('기본 재무 데이터 없음')
                    financial_data.append(data)
                    break
                try:
                    if (
                        'Basic EPS' in income_quarterly.index and len(income_quarterly) >= 2 and
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[0]) and
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[1])
                    ):
                        recent_quarterly_eps = income_quarterly.loc['Basic EPS'].iloc[0]
                        prev_quarterly_eps = income_quarterly.loc['Basic EPS'].iloc[1]
                        data['quarterly_eps_growth'] = ((recent_quarterly_eps - prev_quarterly_eps) / abs(prev_quarterly_eps)) * 100 if prev_quarterly_eps != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'EPS 계산 오류: {str(e)[:100]}')
                try:
                    if (
                        'Basic EPS' in income_annual.index and len(income_annual) >= 2 and
                        not pd.isna(income_annual.loc['Basic EPS'].iloc[0]) and
                        not pd.isna(income_annual.loc['Basic EPS'].iloc[1])
                    ):
                        recent_annual_eps = income_annual.loc['Basic EPS'].iloc[0]
                        prev_annual_eps = income_annual.loc['Basic EPS'].iloc[1]
                        data['annual_eps_growth'] = ((recent_annual_eps - prev_annual_eps) / abs(prev_annual_eps)) * 100 if prev_annual_eps != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'연간 EPS 계산 오류: {str(e)[:100]}')
                try:
                    if (
                        'Basic EPS' in income_quarterly.index and len(income_quarterly) >= 3 and
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[0]) and
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[1]) and
                        not pd.isna(income_quarterly.loc['Basic EPS'].iloc[2])
                    ):
                        growth_1 = ((income_quarterly.loc['Basic EPS'].iloc[0] - income_quarterly.loc['Basic EPS'].iloc[1]) / abs(income_quarterly.loc['Basic EPS'].iloc[1])) * 100 if income_quarterly.loc['Basic EPS'].iloc[1] != 0 else 0
                        growth_2 = ((income_quarterly.loc['Basic EPS'].iloc[1] - income_quarterly.loc['Basic EPS'].iloc[2]) / abs(income_quarterly.loc['Basic EPS'].iloc[2])) * 100 if income_quarterly.loc['Basic EPS'].iloc[2] != 0 else 0
                        data['eps_growth_acceleration'] = growth_1 > growth_2
                        if len(income_quarterly) >= 4 and not pd.isna(income_quarterly.loc['Basic EPS'].iloc[3]):
                            growth_3 = ((income_quarterly.loc['Basic EPS'].iloc[2] - income_quarterly.loc['Basic EPS'].iloc[3]) / abs(income_quarterly.loc['Basic EPS'].iloc[3])) * 100 if income_quarterly.loc['Basic EPS'].iloc[3] != 0 else 0
                            data['eps_3q_accel'] = growth_1 > growth_2 > growth_3
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'EPS 가속화 계산 오류: {str(e)[:100]}')
                try:
                    if (
                        'Total Revenue' in income_quarterly.index and len(income_quarterly) >= 2 and
                        not pd.isna(income_quarterly.loc['Total Revenue'].iloc[0]) and
                        not pd.isna(income_quarterly.loc['Total Revenue'].iloc[1])
                    ):
                        recent_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[0]
                        prev_quarterly_revenue = income_quarterly.loc['Total Revenue'].iloc[1]
                        data['quarterly_revenue_growth'] = ((recent_quarterly_revenue - prev_quarterly_revenue) / abs(prev_quarterly_revenue)) * 100 if prev_quarterly_revenue != 0 else 0
                        if len(income_quarterly) >= 3 and not pd.isna(income_quarterly.loc['Total Revenue'].iloc[2]):
                            growth_prev = (income_quarterly.loc['Total Revenue'].iloc[1] - income_quarterly.loc['Total Revenue'].iloc[2]) / abs(income_quarterly.loc['Total Revenue'].iloc[2]) if income_quarterly.loc['Total Revenue'].iloc[2] != 0 else 0
                            data['revenue_growth_acceleration'] = data['quarterly_revenue_growth'] > growth_prev
                            if len(income_quarterly) >= 4 and not pd.isna(income_quarterly.loc['Total Revenue'].iloc[3]):
                                growth_earlier = (income_quarterly.loc['Total Revenue'].iloc[2] - income_quarterly.loc['Total Revenue'].iloc[3]) / abs(income_quarterly.loc['Total Revenue'].iloc[3]) if income_quarterly.loc['Total Revenue'].iloc[3] != 0 else 0
                                data['sales_3q_accel'] = data['quarterly_revenue_growth'] > growth_prev > growth_earlier
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'매출 성장률 계산 오류: {str(e)[:100]}')
                try:
                    if (
                        'Total Revenue' in income_annual.index and len(income_annual) >= 2 and
                        not pd.isna(income_annual.loc['Total Revenue'].iloc[0]) and
                        not pd.isna(income_annual.loc['Total Revenue'].iloc[1])
                    ):
                        recent_annual_revenue = income_annual.loc['Total Revenue'].iloc[0]
                        prev_annual_revenue = income_annual.loc['Total Revenue'].iloc[1]
                        data['annual_revenue_growth'] = ((recent_annual_revenue - prev_annual_revenue) / abs(prev_annual_revenue)) * 100 if prev_annual_revenue != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'연간 매출 성장률 계산 오류: {str(e)[:100]}')
                try:
                    if (
                        'Operating Income' in income_quarterly.index and len(income_quarterly) >= 2 and
                        not pd.isna(income_quarterly.loc['Operating Income'].iloc[0]) and
                        not pd.isna(income_quarterly.loc['Operating Income'].iloc[1])
                    ):
                        recent_op_margin = income_quarterly.loc['Operating Income'].iloc[0] / income_quarterly.loc['Total Revenue'].iloc[0]
                        prev_op_margin = income_quarterly.loc['Operating Income'].iloc[1] / income_quarterly.loc['Total Revenue'].iloc[1]
                        data['quarterly_op_margin_improved'] = recent_op_margin > prev_op_margin
                        if len(income_quarterly) >= 3 and not pd.isna(income_quarterly.loc['Operating Income'].iloc[2]):
                            prev_prev_margin = income_quarterly.loc['Operating Income'].iloc[2] / income_quarterly.loc['Total Revenue'].iloc[2]
                            accel1 = recent_op_margin - prev_op_margin
                            accel2 = prev_op_margin - prev_prev_margin
                            data['net_margin_improved'] = accel1 > 0
                            if len(income_quarterly) >= 4 and not pd.isna(income_quarterly.loc['Operating Income'].iloc[3]):
                                prev3_margin = income_quarterly.loc['Operating Income'].iloc[3] / income_quarterly.loc['Total Revenue'].iloc[3]
                                accel3 = prev_prev_margin - prev3_margin
                                data['margin_3q_accel'] = accel1 > accel2 > accel3
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'영업이익률 개선 계산 오류: {str(e)[:100]}')
                try:
                    if (
                        'Operating Income' in income_annual.index and len(income_annual) >= 2 and
                        not pd.isna(income_annual.loc['Operating Income'].iloc[0]) and
                        not pd.isna(income_annual.loc['Operating Income'].iloc[1])
                    ):
                        recent_op_margin = income_annual.loc['Operating Income'].iloc[0] / income_annual.loc['Total Revenue'].iloc[0]
                        prev_op_margin = income_annual.loc['Operating Income'].iloc[1] / income_annual.loc['Total Revenue'].iloc[1]
                        data['annual_op_margin_improved'] = recent_op_margin > prev_op_margin
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'연간 영업이익률 개선 계산 오류: {str(e)[:100]}')
                try:
                    if (
                        'Net Income' in income_quarterly.index and len(income_quarterly) >= 2 and
                        not pd.isna(income_quarterly.loc['Net Income'].iloc[0]) and
                        not pd.isna(income_quarterly.loc['Net Income'].iloc[1])
                    ):
                        recent_net_income = income_quarterly.loc['Net Income'].iloc[0]
                        prev_net_income = income_quarterly.loc['Net Income'].iloc[1]
                        data['quarterly_net_income_growth'] = ((recent_net_income - prev_net_income) / abs(prev_net_income)) * 100 if prev_net_income != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'순이익 성장률 계산 오류: {str(e)[:100]}')
                try:
                    if (
                        'Net Income' in income_annual.index and len(income_annual) >= 2 and
                        not pd.isna(income_annual.loc['Net Income'].iloc[0]) and
                        not pd.isna(income_annual.loc['Net Income'].iloc[1])
                    ):
                        recent_net_income = income_annual.loc['Net Income'].iloc[0]
                        prev_net_income = income_annual.loc['Net Income'].iloc[1]
                        data['annual_net_income_growth'] = ((recent_net_income - prev_net_income) / abs(prev_net_income)) * 100 if prev_net_income != 0 else 0
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'연간 순이익 성장률 계산 오류: {str(e)[:100]}')
                try:
                    if 'Total Stockholder Equity' in balance_annual.index and 'Total Liab' in balance_annual.index:
                        total_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
                        total_debt = balance_annual.loc['Total Liab'].iloc[0]
                        data['debt_to_equity'] = total_debt / total_equity if total_equity != 0 else np.nan
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'D/E 계산 오류: {str(e)[:100]}')
                try:
                    if 'Net Income' in income_annual.index and 'Total Stockholder Equity' in balance_annual.index:
                        net_income = income_annual.loc['Net Income'].iloc[0]
                        total_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
                        data['roe'] = (net_income / total_equity) * 100 if total_equity != 0 else np.nan
                except Exception as e:
                    data['has_error'] = True
                    data['error_details'].append(f'ROE 계산 오류: {str(e)[:100]}')
                financial_data.append(data)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    data = {
                        'symbol': symbol,
                        'has_error': True,
                        'error_details': [f'API 호출 실패: {str(e)[:100]}'],
                    }
                    financial_data.append(data)
    df = pd.DataFrame(financial_data)
    return df

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
                income_stmt = ticker.income_statement(frequency='quarterly')
                balance_sheet = ticker.balance_sheet(frequency='annual')
                cash_flow = ticker.cash_flow(frequency='quarterly')
                
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
                    'last_updated': datetime.now().strftime('%Y-%m-%d'),
                }
                
                # 분기 매출 성장률 계산
                if isinstance(income_stmt, pd.DataFrame) and not income_stmt.empty and 'TotalRevenue' in income_stmt.columns:
                    revenue_data = income_stmt['TotalRevenue'].dropna()
                    if len(revenue_data) >= 2:
                        recent_revenue = revenue_data.iloc[0]
                        prev_revenue = revenue_data.iloc[1]
                        if prev_revenue != 0:
                            data['quarterly_revenue_growth'] = ((recent_revenue - prev_revenue) / abs(prev_revenue)) * 100
                
                # 분기 순이익 성장률 계산
                if isinstance(income_stmt, pd.DataFrame) and not income_stmt.empty and 'NetIncome' in income_stmt.columns:
                    net_income_data = income_stmt['NetIncome'].dropna()
                    if len(net_income_data) >= 2:
                        recent_ni = net_income_data.iloc[0]
                        prev_ni = net_income_data.iloc[1]
                        if prev_ni != 0:
                            data['quarterly_net_income_growth'] = ((recent_ni - prev_ni) / abs(prev_ni)) * 100
                
                # ROE 계산
                if isinstance(balance_sheet, pd.DataFrame) and not balance_sheet.empty:
                    if 'StockholdersEquity' in balance_sheet.columns and isinstance(income_stmt, pd.DataFrame) and 'NetIncome' in income_stmt.columns:
                        equity = balance_sheet['StockholdersEquity'].dropna()
                        net_income = income_stmt['NetIncome'].dropna()
                        if len(equity) > 0 and len(net_income) > 0 and equity.iloc[0] != 0:
                            data['roe'] = (net_income.iloc[0] / equity.iloc[0]) * 100
                
                # 부채비율 계산
                if isinstance(balance_sheet, pd.DataFrame) and not balance_sheet.empty:
                    if 'TotalLiabilitiesNetMinorityInterest' in balance_sheet.columns and 'StockholdersEquity' in balance_sheet.columns:
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
                    data = {
                        'symbol': symbol,
                        'has_error': True,
                        'error_details': [f'yahooquery API 호출 실패: {str(e)[:100]}'],
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
                        'last_updated': datetime.now().strftime('%Y-%m-%d'),
                    }
                    financial_data.append(data)
    
    return pd.DataFrame(financial_data)


def collect_financial_data_hybrid(symbols, max_retries=2, delay=1.0):
    """yfinance와 yahooquery를 함께 사용하여 재무 데이터 수집 - 9가지 조건 완전 구현"""
    print("\n💰 하이브리드 방식 재무 데이터 수집 시작 (yfinance + yahooquery)...")
    financial_data = []
    total = len(symbols)
    
    for i, symbol in enumerate(symbols):
        print(f"진행 중: {i+1}/{total} - {symbol}")
        
        data = {
            'symbol': symbol,
            'has_error': False,
            'error_details': [],
            'quarterly_eps_growth': 0,
            'annual_eps_growth': 0,
            'eps_growth_acceleration': False,
            'revenue_growth_acceleration': False,
            'net_margin_improved': False,
            'eps_3q_accel': False,
            'sales_3q_accel': False,
            'margin_3q_accel': False,
            'quarterly_revenue_growth': 0,
            'annual_revenue_growth': 0,
            'quarterly_op_margin_improved': False,
            'annual_op_margin_improved': False,
            'quarterly_net_income_growth': 0,
            'annual_net_income_growth': 0,
            'roe': 0,
            'debt_to_equity': 0,
            'last_updated': datetime.now().strftime('%Y-%m-%d'),
        }
        
        # 먼저 yfinance로 시도 - 모든 9가지 조건 계산
        try:
            ticker_yf = yf.Ticker(symbol)
            income_quarterly = ticker_yf.quarterly_financials
            income_annual = ticker_yf.financials
            balance_annual = ticker_yf.balance_sheet
            
            if (income_quarterly is not None and not income_quarterly.empty and
                income_annual is not None and not income_annual.empty and
                balance_annual is not None and not balance_annual.empty):
                
                # 1) 연간 EPS 성장률 계산
                try:
                    if 'Basic EPS' in income_annual.index and len(income_annual) >= 2:
                        recent_annual_eps = income_annual.loc['Basic EPS'].iloc[0]
                        prev_annual_eps = income_annual.loc['Basic EPS'].iloc[1]
                        if not pd.isna(recent_annual_eps) and not pd.isna(prev_annual_eps) and prev_annual_eps != 0:
                            data['annual_eps_growth'] = ((recent_annual_eps - prev_annual_eps) / abs(prev_annual_eps)) * 100
                except Exception:
                    pass
                
                # 2) 분기별 EPS 가속화 및 3분기 연속 가속화
                try:
                    if 'Basic EPS' in income_quarterly.index and len(income_quarterly) >= 4:
                        eps_data = [income_quarterly.loc['Basic EPS'].iloc[i] for i in range(4)]
                        if all(not pd.isna(eps) for eps in eps_data) and all(eps != 0 for eps in eps_data[1:]):
                            growth_1 = ((eps_data[0] - eps_data[1]) / abs(eps_data[1])) * 100
                            growth_2 = ((eps_data[1] - eps_data[2]) / abs(eps_data[2])) * 100
                            growth_3 = ((eps_data[2] - eps_data[3]) / abs(eps_data[3])) * 100
                            
                            data['eps_growth_acceleration'] = growth_1 > growth_2
                            data['eps_3q_accel'] = growth_1 > growth_2 > growth_3
                except Exception:
                    pass
                
                # 3) 연간 매출 성장률 계산
                try:
                    if 'Total Revenue' in income_annual.index and len(income_annual) >= 2:
                        recent_annual_revenue = income_annual.loc['Total Revenue'].iloc[0]
                        prev_annual_revenue = income_annual.loc['Total Revenue'].iloc[1]
                        if not pd.isna(recent_annual_revenue) and not pd.isna(prev_annual_revenue) and prev_annual_revenue != 0:
                            data['annual_revenue_growth'] = ((recent_annual_revenue - prev_annual_revenue) / abs(prev_annual_revenue)) * 100
                except Exception:
                    pass
                
                # 4) 분기별 매출 가속화 및 3분기 연속 가속화
                try:
                    if 'Total Revenue' in income_quarterly.index and len(income_quarterly) >= 4:
                        revenue_data = [income_quarterly.loc['Total Revenue'].iloc[i] for i in range(4)]
                        if all(not pd.isna(rev) for rev in revenue_data) and all(rev != 0 for rev in revenue_data[1:]):
                            growth_1 = ((revenue_data[0] - revenue_data[1]) / abs(revenue_data[1])) * 100
                            growth_2 = ((revenue_data[1] - revenue_data[2]) / abs(revenue_data[2])) * 100
                            growth_3 = ((revenue_data[2] - revenue_data[3]) / abs(revenue_data[3])) * 100
                            
                            data['revenue_growth_acceleration'] = growth_1 > growth_2
                            data['sales_3q_accel'] = growth_1 > growth_2 > growth_3
                            data['quarterly_revenue_growth'] = growth_1
                except Exception:
                    pass
                
                # 5) 분기별 순이익률 개선 및 3분기 연속 가속화
                try:
                    if ('Net Income' in income_quarterly.index and 'Total Revenue' in income_quarterly.index and 
                        len(income_quarterly) >= 4):
                        margins = []
                        for i in range(4):
                            net_income = income_quarterly.loc['Net Income'].iloc[i]
                            revenue = income_quarterly.loc['Total Revenue'].iloc[i]
                            if not pd.isna(net_income) and not pd.isna(revenue) and revenue != 0:
                                margins.append(net_income / revenue)
                            else:
                                margins.append(None)
                        
                        if len([m for m in margins if m is not None]) >= 3:
                            valid_margins = [m for m in margins if m is not None]
                            if len(valid_margins) >= 3:
                                data['net_margin_improved'] = valid_margins[0] > valid_margins[1]
                                if len(valid_margins) >= 4:
                                    accel1 = valid_margins[0] - valid_margins[1]
                                    accel2 = valid_margins[1] - valid_margins[2]
                                    accel3 = valid_margins[2] - valid_margins[3]
                                    data['margin_3q_accel'] = accel1 > accel2 > accel3
                except Exception:
                    pass
                
                # 6) 부채비율 계산
                try:
                    if 'Total Stockholder Equity' in balance_annual.index and 'Total Liab' in balance_annual.index:
                        total_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
                        total_debt = balance_annual.loc['Total Liab'].iloc[0]
                        if not pd.isna(total_equity) and not pd.isna(total_debt) and total_equity != 0:
                            data['debt_to_equity'] = (total_debt / total_equity) * 100  # 백분율로 변환
                except Exception:
                    pass
                
                # 7) ROE 계산
                try:
                    if 'Net Income' in income_annual.index and 'Total Stockholder Equity' in balance_annual.index:
                        net_income = income_annual.loc['Net Income'].iloc[0]
                        total_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
                        if not pd.isna(net_income) and not pd.isna(total_equity) and total_equity != 0:
                            data['roe'] = (net_income / total_equity) * 100
                except Exception:
                    pass
                
        except Exception as e:
            data['error_details'].append(f'yfinance 실패: {str(e)[:50]}')
            data['has_error'] = True
        
        # yahooquery로 보완 (yfinance에서 실패한 경우)
        if data['has_error'] or (data['annual_eps_growth'] == 0 and data['annual_revenue_growth'] == 0):
            try:
                time.sleep(delay)
                ticker_yq = Ticker(symbol)
                income_stmt_q = ticker_yq.income_statement(frequency='quarterly')
                income_stmt_a = ticker_yq.income_statement(frequency='annual')
                balance_sheet = ticker_yq.balance_sheet(frequency='annual')
                
                # yahooquery로 부족한 데이터 보완
                if isinstance(income_stmt_q, pd.DataFrame) and not income_stmt_q.empty:
                    # 매출 성장률 보완
                    if data['quarterly_revenue_growth'] == 0 and 'TotalRevenue' in income_stmt_q.columns:
                        revenue_data = income_stmt_q['TotalRevenue'].dropna()
                        if len(revenue_data) >= 2:
                            recent_revenue = revenue_data.iloc[0]
                            prev_revenue = revenue_data.iloc[1]
                            if prev_revenue != 0:
                                data['quarterly_revenue_growth'] = ((recent_revenue - prev_revenue) / abs(prev_revenue)) * 100
                
                # ROE 및 부채비율 보완
                if isinstance(balance_sheet, pd.DataFrame) and not balance_sheet.empty:
                    if data['roe'] == 0 and 'StockholdersEquity' in balance_sheet.columns:
                        if isinstance(income_stmt_a, pd.DataFrame) and 'NetIncome' in income_stmt_a.columns:
                            equity = balance_sheet['StockholdersEquity'].dropna()
                            net_income = income_stmt_a['NetIncome'].dropna()
                            if len(equity) > 0 and len(net_income) > 0 and equity.iloc[0] != 0:
                                data['roe'] = (net_income.iloc[0] / equity.iloc[0]) * 100
                    
                    if data['debt_to_equity'] == 0 and 'TotalLiabilitiesNetMinorityInterest' in balance_sheet.columns:
                        debt = balance_sheet['TotalLiabilitiesNetMinorityInterest'].dropna()
                        equity = balance_sheet['StockholdersEquity'].dropna()
                        if len(debt) > 0 and len(equity) > 0 and equity.iloc[0] != 0:
                            data['debt_to_equity'] = (debt.iloc[0] / equity.iloc[0]) * 100
                
                # 오류 상태 해제 (일부 데이터라도 수집된 경우)
                if data['annual_eps_growth'] != 0 or data['annual_revenue_growth'] != 0 or data['roe'] != 0:
                    data['has_error'] = False
                
            except Exception as e:
                data['error_details'].append(f'yahooquery 보완 실패: {str(e)[:50]}')
                data['has_error'] = True
        
        financial_data.append(data)
    
    return pd.DataFrame(financial_data)

