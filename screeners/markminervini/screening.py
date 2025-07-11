import pandas as pd
from config import (
    ADVANCED_FINANCIAL_CRITERIA,
    ADVANCED_FINANCIAL_MIN_MET,
)

__all__ = ["screen_advanced_financials"]


def screen_advanced_financials(financial_data: pd.DataFrame) -> pd.DataFrame:
    """Filter financial data by advanced criteria.

    The score (``fin_met_count``) is calculated using nine financial
    conditions described in ``to make it better.md``. 4개 이상 충족한
    종목만 반환한다.
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

            # 3) 연간 매출 성장률
            if pd.notna(row.get('annual_revenue_growth')) and row['annual_revenue_growth'] >= ADVANCED_FINANCIAL_CRITERIA['min_annual_revenue_growth']:
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

        # 모든 종목을 포함하되 fin_met_count 값을 그대로 유지
        results.append({
            'symbol': row['symbol'],
            'fin_met_count': met_count,
            'has_error': row.get('has_error', False)
        })

    df = pd.DataFrame(results)
    df = df[df['fin_met_count'] >= ADVANCED_FINANCIAL_MIN_MET]
    return df.reset_index(drop=True)
