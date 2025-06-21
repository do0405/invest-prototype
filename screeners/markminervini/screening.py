import pandas as pd
from config import ADVANCED_FINANCIAL_CRITERIA

__all__ = ["screen_advanced_financials"]


def screen_advanced_financials(financial_data: pd.DataFrame) -> pd.DataFrame:
    """Filter financial data by advanced criteria."""
    results = []
    for _, row in financial_data.iterrows():
        met_count = 0
        try:
            if pd.notna(row.get('quarterly_eps_growth')) and row['quarterly_eps_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_eps_growth']:
                met_count += 1
            if pd.notna(row.get('annual_eps_growth')) and row['annual_eps_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_eps_growth']:
                met_count += 1
            if row.get('eps_growth_acceleration'):
                met_count += 1
            if pd.notna(row.get('quarterly_revenue_growth')) and row['quarterly_revenue_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_revenue_growth']:
                met_count += 1
            if pd.notna(row.get('annual_revenue_growth')) and row['annual_revenue_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_revenue_growth']:
                met_count += 1
            if row.get('revenue_growth_acceleration'):
                met_count += 1
            if row.get('net_margin_improved'):
                met_count += 1
            if row.get('eps_3q_accel'):
                met_count += 1
            if row.get('sales_3q_accel'):
                met_count += 1
            if row.get('margin_3q_accel'):
                met_count += 1
            if pd.notna(row.get('quarterly_net_income_growth')) and row['quarterly_net_income_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_quarterly_net_income_growth']:
                met_count += 1
            if pd.notna(row.get('annual_net_income_growth')) and row['annual_net_income_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_net_income_growth']:
                met_count += 1
            if pd.notna(row.get('roe')) and row['roe'] >= ADVANCED_FINANCIAL_CRITERIA['min_roe']:
                met_count += 1
            if pd.notna(row.get('debt_to_equity')) and row['debt_to_equity'] <= ADVANCED_FINANCIAL_CRITERIA['max_debt_to_equity']:
                met_count += 1
        except Exception as e:
            print(f"⚠️ {row.get('symbol', 'Unknown')} 재무 조건 체크 중 오류: {e}")
        if met_count >= 5:
            results.append({'symbol': row['symbol'], 'fin_met_count': met_count, 'has_error': row.get('has_error', False)})
    return pd.DataFrame(results)
