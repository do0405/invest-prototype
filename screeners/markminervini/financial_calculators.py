"""Mark Minervini 스크리너 재무 계산 모듈

이 모듈은 재무 데이터에서 다양한 지표를 계산하는 공통 함수들을 제공합니다.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional

def calculate_eps_metrics(income_quarterly: pd.DataFrame, income_annual: pd.DataFrame) -> Dict[str, Any]:
    """EPS 관련 지표 계산
    
    Args:
        income_quarterly: 분기별 손익계산서 데이터
        income_annual: 연간 손익계산서 데이터
        
    Returns:
        Dict: EPS 관련 지표들
    """
    metrics = {
        'quarterly_eps_growth': 0,
        'annual_eps_growth': 0,
        'eps_growth_acceleration': False,
        'eps_3q_accel': False
    }
    
    try:
        # 분기별 EPS 성장률
        if ('Basic EPS' in income_quarterly.index and len(income_quarterly) >= 2 and
            not pd.isna(income_quarterly.loc['Basic EPS'].iloc[0]) and
            not pd.isna(income_quarterly.loc['Basic EPS'].iloc[1])):
            recent_eps = income_quarterly.loc['Basic EPS'].iloc[0]
            prev_eps = income_quarterly.loc['Basic EPS'].iloc[1]
            if prev_eps != 0:
                if prev_eps > 0:
                    metrics['quarterly_eps_growth'] = ((recent_eps - prev_eps) / prev_eps) * 100
                else:
                    if recent_eps >= 0:
                        metrics['quarterly_eps_growth'] = 200  # 흑자 전환
                    else:
                        metrics['quarterly_eps_growth'] = ((recent_eps - prev_eps) / abs(prev_eps)) * 100
        
        # 연간 EPS 성장률
        if ('Basic EPS' in income_annual.index and len(income_annual) >= 2 and
            not pd.isna(income_annual.loc['Basic EPS'].iloc[0]) and
            not pd.isna(income_annual.loc['Basic EPS'].iloc[1])):
            recent_annual_eps = income_annual.loc['Basic EPS'].iloc[0]
            prev_annual_eps = income_annual.loc['Basic EPS'].iloc[1]
            if prev_annual_eps != 0:
                if prev_annual_eps > 0:
                    metrics['annual_eps_growth'] = ((recent_annual_eps - prev_annual_eps) / prev_annual_eps) * 100
                else:
                    if recent_annual_eps >= 0:
                        metrics['annual_eps_growth'] = 200  # 흑자 전환
                    else:
                        metrics['annual_eps_growth'] = ((recent_annual_eps - prev_annual_eps) / abs(prev_annual_eps)) * 100
        
        # EPS 가속화 및 3분기 연속 가속화
        if ('Basic EPS' in income_quarterly.index and len(income_quarterly) >= 4):
            eps_data = [income_quarterly.loc['Basic EPS'].iloc[i] for i in range(4)]
            if all(not pd.isna(eps) for eps in eps_data) and all(eps != 0 for eps in eps_data[1:]):
                # EPS 성장률 계산 (음수 고려)
                def calc_eps_growth(current, previous):
                    if previous > 0:
                        return ((current - previous) / previous) * 100
                    else:
                        if current >= 0:
                            return 200  # 흑자 전환
                        else:
                            return ((current - previous) / abs(previous)) * 100
                
                growth_1 = calc_eps_growth(eps_data[0], eps_data[1])
                growth_2 = calc_eps_growth(eps_data[1], eps_data[2])
                growth_3 = calc_eps_growth(eps_data[2], eps_data[3])
                
                metrics['eps_growth_acceleration'] = growth_1 > growth_2
                metrics['eps_3q_accel'] = growth_1 > growth_2 > growth_3
    except Exception:
        pass
    
    return metrics

def calculate_revenue_metrics(income_quarterly: pd.DataFrame, income_annual: pd.DataFrame) -> Dict[str, Any]:
    """매출 관련 지표 계산
    
    Args:
        income_quarterly: 분기별 손익계산서 데이터
        income_annual: 연간 손익계산서 데이터
        
    Returns:
        Dict: 매출 관련 지표들
    """
    metrics = {
        'quarterly_revenue_growth': 0,
        'annual_revenue_growth': 0,
        'revenue_growth_acceleration': False,
        'sales_3q_accel': False
    }
    
    try:
        # 분기별 매출 성장률
        if ('Total Revenue' in income_quarterly.index and len(income_quarterly) >= 2 and
            not pd.isna(income_quarterly.loc['Total Revenue'].iloc[0]) and
            not pd.isna(income_quarterly.loc['Total Revenue'].iloc[1])):
            recent_revenue = income_quarterly.loc['Total Revenue'].iloc[0]
            prev_revenue = income_quarterly.loc['Total Revenue'].iloc[1]
            if prev_revenue != 0:
                metrics['quarterly_revenue_growth'] = ((recent_revenue - prev_revenue) / abs(prev_revenue)) * 100
        
        # 연간 매출 성장률
        if ('Total Revenue' in income_annual.index and len(income_annual) >= 2 and
            not pd.isna(income_annual.loc['Total Revenue'].iloc[0]) and
            not pd.isna(income_annual.loc['Total Revenue'].iloc[1])):
            recent_annual_revenue = income_annual.loc['Total Revenue'].iloc[0]
            prev_annual_revenue = income_annual.loc['Total Revenue'].iloc[1]
            if prev_annual_revenue != 0:
                metrics['annual_revenue_growth'] = ((recent_annual_revenue - prev_annual_revenue) / abs(prev_annual_revenue)) * 100
        
        # 매출 가속화 및 3분기 연속 가속화
        if ('Total Revenue' in income_quarterly.index and len(income_quarterly) >= 4):
            revenue_data = [income_quarterly.loc['Total Revenue'].iloc[i] for i in range(4)]
            if all(not pd.isna(rev) for rev in revenue_data) and all(rev != 0 for rev in revenue_data[1:]):
                growth_1 = ((revenue_data[0] - revenue_data[1]) / abs(revenue_data[1])) * 100
                growth_2 = ((revenue_data[1] - revenue_data[2]) / abs(revenue_data[2])) * 100
                growth_3 = ((revenue_data[2] - revenue_data[3]) / abs(revenue_data[3])) * 100
                
                metrics['revenue_growth_acceleration'] = growth_1 > growth_2
                metrics['sales_3q_accel'] = growth_1 > growth_2 > growth_3
    except Exception:
        pass
    
    return metrics

def calculate_margin_metrics(income_quarterly: pd.DataFrame, income_annual: pd.DataFrame) -> Dict[str, Any]:
    """마진 관련 지표 계산
    
    Args:
        income_quarterly: 분기별 손익계산서 데이터
        income_annual: 연간 손익계산서 데이터
        
    Returns:
        Dict: 마진 관련 지표들
    """
    metrics = {
        'quarterly_op_margin_improved': False,
        'annual_op_margin_improved': False,
        'net_margin_improved': False,
        'margin_3q_accel': False,
        'quarterly_net_income_growth': 0,
        'annual_net_income_growth': 0
    }
    
    try:
        # 분기별 영업이익률 개선
        if ('Operating Income' in income_quarterly.index and 'Total Revenue' in income_quarterly.index and
            len(income_quarterly) >= 2):
            recent_op_margin = income_quarterly.loc['Operating Income'].iloc[0] / income_quarterly.loc['Total Revenue'].iloc[0]
            prev_op_margin = income_quarterly.loc['Operating Income'].iloc[1] / income_quarterly.loc['Total Revenue'].iloc[1]
            metrics['quarterly_op_margin_improved'] = recent_op_margin > prev_op_margin
            
            # 3분기 연속 마진 가속화
            if len(income_quarterly) >= 4:
                margins = []
                for i in range(4):
                    op_income = income_quarterly.loc['Operating Income'].iloc[i]
                    revenue = income_quarterly.loc['Total Revenue'].iloc[i]
                    if not pd.isna(op_income) and not pd.isna(revenue) and revenue != 0:
                        margins.append(op_income / revenue)
                
                if len(margins) >= 4:
                    accel1 = margins[0] - margins[1]
                    accel2 = margins[1] - margins[2]
                    accel3 = margins[2] - margins[3]
                    metrics['margin_3q_accel'] = accel1 > accel2 > accel3
        
        # 연간 영업이익률 개선
        if ('Operating Income' in income_annual.index and 'Total Revenue' in income_annual.index and
            len(income_annual) >= 2):
            recent_op_margin = income_annual.loc['Operating Income'].iloc[0] / income_annual.loc['Total Revenue'].iloc[0]
            prev_op_margin = income_annual.loc['Operating Income'].iloc[1] / income_annual.loc['Total Revenue'].iloc[1]
            metrics['annual_op_margin_improved'] = recent_op_margin > prev_op_margin
        
        # 순이익 성장률
        if ('Net Income' in income_quarterly.index and len(income_quarterly) >= 2):
            recent_net_income = income_quarterly.loc['Net Income'].iloc[0]
            prev_net_income = income_quarterly.loc['Net Income'].iloc[1]
            if not pd.isna(recent_net_income) and not pd.isna(prev_net_income) and prev_net_income != 0:
                if prev_net_income > 0:
                    metrics['quarterly_net_income_growth'] = ((recent_net_income - prev_net_income) / prev_net_income) * 100
                else:
                    if recent_net_income >= 0:
                        metrics['quarterly_net_income_growth'] = 200  # 흑자 전환
                    else:
                        metrics['quarterly_net_income_growth'] = ((recent_net_income - prev_net_income) / abs(prev_net_income)) * 100
        
        if ('Net Income' in income_annual.index and len(income_annual) >= 2):
            recent_net_income = income_annual.loc['Net Income'].iloc[0]
            prev_net_income = income_annual.loc['Net Income'].iloc[1]
            if not pd.isna(recent_net_income) and not pd.isna(prev_net_income) and prev_net_income != 0:
                if prev_net_income > 0:
                    metrics['annual_net_income_growth'] = ((recent_net_income - prev_net_income) / prev_net_income) * 100
                else:
                    if recent_net_income >= 0:
                        metrics['annual_net_income_growth'] = 200  # 흑자 전환
                    else:
                        metrics['annual_net_income_growth'] = ((recent_net_income - prev_net_income) / abs(prev_net_income)) * 100
        
        # 순이익률 개선
        if ('Net Income' in income_quarterly.index and 'Total Revenue' in income_quarterly.index and
            len(income_quarterly) >= 3):
            margins = []
            for i in range(3):
                net_income = income_quarterly.loc['Net Income'].iloc[i]
                revenue = income_quarterly.loc['Total Revenue'].iloc[i]
                if not pd.isna(net_income) and not pd.isna(revenue) and revenue != 0:
                    margins.append(net_income / revenue)
            
            if len(margins) >= 2:
                metrics['net_margin_improved'] = margins[0] > margins[1]
    except Exception:
        pass
    
    return metrics

def calculate_financial_ratios(income_annual: pd.DataFrame, balance_annual: pd.DataFrame) -> Dict[str, Any]:
    """재무비율 계산
    
    Args:
        income_annual: 연간 손익계산서 데이터
        balance_annual: 연간 대차대조표 데이터
        
    Returns:
        Dict: 재무비율들
    """
    metrics = {
        'roe': 0,
        'debt_to_equity': 0
    }
    
    try:
        # ROE 계산
        if ('Net Income' in income_annual.index and 'Total Stockholder Equity' in balance_annual.index):
            net_income = income_annual.loc['Net Income'].iloc[0]
            total_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
            if not pd.isna(net_income) and not pd.isna(total_equity) and total_equity != 0:
                metrics['roe'] = (net_income / total_equity) * 100
        
        # 부채비율 계산
        if ('Total Stockholder Equity' in balance_annual.index and 'Total Liab' in balance_annual.index):
            total_equity = balance_annual.loc['Total Stockholder Equity'].iloc[0]
            total_debt = balance_annual.loc['Total Liab'].iloc[0]
            if not pd.isna(total_equity) and not pd.isna(total_debt) and total_equity != 0:
                metrics['debt_to_equity'] = total_debt / total_equity
    except Exception:
        pass
    
    return metrics

# 기본 재무 데이터 생성 기능 제거됨

def merge_financial_metrics(*metric_dicts: Dict[str, Any]) -> Dict[str, Any]:
    """여러 재무 지표 딕셔너리를 병합
    
    Args:
        *metric_dicts: 병합할 지표 딕셔너리들
        
    Returns:
        Dict: 병합된 지표 딕셔너리
    """
    result = {}
    for metrics in metric_dicts:
        result.update(metrics)
    return result