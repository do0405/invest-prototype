import os
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from config import (
    YAHOO_FINANCE_MAX_RETRIES, YAHOO_FINANCE_DELAY,
    ADVANCED_FINANCIAL_CRITERIA,
)
from utils import ensure_dir

__all__ = [
    "collect_financial_data",
    "collect_real_financial_data",
    "screen_advanced_financials",
    "calculate_percentile_rank",
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

def collect_real_financial_data(symbols, max_retries=3, delay=1):
    """alpha_vantage를 사용한 재무 데이터 수집 예시 (placeholder)"""
    return pd.DataFrame()

def screen_advanced_financials(financial_data: pd.DataFrame) -> pd.DataFrame:
    """수집된 재무 데이터를 조건에 맞춰 스크리닝"""
    results = []
    for _, row in financial_data.iterrows():
        met_count = 0
        
        # 각 재무 조건을 체크
        try:
            # 분기 EPS 성장률 조건
            if pd.notna(row.get('quarterly_eps_growth')) and row['quarterly_eps_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_eps_growth']:
                met_count += 1
            
            # 연간 EPS 성장률 조건
            if pd.notna(row.get('annual_eps_growth')) and row['annual_eps_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_eps_growth']:
                met_count += 1
            
            # 분기 매출 성장률 조건
            if pd.notna(row.get('quarterly_revenue_growth')) and row['quarterly_revenue_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_revenue_growth']:
                met_count += 1
            
            # 연간 매출 성장률 조건
            if pd.notna(row.get('annual_revenue_growth')) and row['annual_revenue_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_revenue_growth']:
                met_count += 1
            
            # 분기 순이익 성장률 조건
            if pd.notna(row.get('quarterly_net_income_growth')) and row['quarterly_net_income_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_net_income_growth']:
                met_count += 1
            
            # 연간 순이익 성장률 조건
            if pd.notna(row.get('annual_net_income_growth')) and row['annual_net_income_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_net_income_growth']:
                met_count += 1
            
            # ROE 조건
            if pd.notna(row.get('roe')) and row['roe'] >= ADVANCED_FINANCIAL_CRITERIA['min_roe']:
                met_count += 1
            
            # 부채비율 조건
            if pd.notna(row.get('debt_to_equity')) and row['debt_to_equity'] <= ADVANCED_FINANCIAL_CRITERIA['max_debt_to_equity']:
                met_count += 1
                
        except Exception as e:
            print(f"⚠️ {row.get('symbol', 'Unknown')} 재무 조건 체크 중 오류: {e}")
            
        results.append({
            'symbol': row['symbol'], 
            'fin_met_count': met_count, 
            'has_error': row.get('has_error', False)
        })
    
    return pd.DataFrame(results)

def calculate_percentile_rank(series: pd.Series) -> pd.Series:
    return series.rank(pct=True) * 100
