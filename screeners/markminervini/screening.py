import pandas as pd
from config import ADVANCED_FINANCIAL_CRITERIA

__all__ = ["screen_advanced_financials"]


def screen_advanced_financials(financial_data: pd.DataFrame) -> pd.DataFrame:
    """Filter financial data by advanced criteria.

    The score (`fin_met_count`) is calculated using the nine conditions
    described in ``to make it better.md``. Only stocks meeting five or more
    of these conditions are returned.
    """

    results = []
    for _, row in financial_data.iterrows():
        met_count = 0
        try:
            # 1) 연간 EPS 성장률
            if pd.notna(row.get('annual_eps_growth')) and row['annual_eps_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_eps_growth']:
                met_count += 1

            # 2) 분기별 EPS 가속화
            if row.get('eps_growth_acceleration'):
                met_count += 1

            # 3) 연간 매출 성장률 (15% 이상)
            if pd.notna(row.get('annual_revenue_growth')) and row['annual_revenue_growth'] >= 15:
                met_count += 1

            # 4) 분기별 매출 가속화
            if row.get('revenue_growth_acceleration'):
                met_count += 1

            # 5) 분기별 순이익률 증가
            if row.get('net_margin_improved'):
                met_count += 1

            # 6) EPS 3분기 연속 가속화
            if row.get('eps_3q_accel'):
                met_count += 1

            # 7) 매출 3분기 연속 가속화
            if row.get('sales_3q_accel'):
                met_count += 1

            # 8) 순이익률 3분기 연속 가속화
            if row.get('margin_3q_accel'):
                met_count += 1

            # 9) 부채비율 ≤ 150%
            if pd.notna(row.get('debt_to_equity')) and row['debt_to_equity'] <= ADVANCED_FINANCIAL_CRITERIA['max_debt_to_equity']:
                met_count += 1
        except Exception as e:
            print(f"⚠️ {row.get('symbol', 'Unknown')} 재무 조건 체크 중 오류: {e}")

        if met_count >= 5:
            results.append({'symbol': row['symbol'], 'fin_met_count': met_count, 'has_error': row.get('has_error', False)})

    return pd.DataFrame(results)
