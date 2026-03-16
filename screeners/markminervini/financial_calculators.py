from __future__ import annotations

from typing import Any

import pandas as pd


def _empty_numeric_series() -> pd.Series:
    return pd.Series(dtype=float)


def _numeric_row(frame: pd.DataFrame, label: str) -> pd.Series:
    if frame is None or frame.empty or label not in frame.index:
        return _empty_numeric_series()

    row = frame.loc[label]
    if isinstance(row, pd.DataFrame):
        if row.empty:
            return _empty_numeric_series()
        row = row.iloc[0]
    if not isinstance(row, pd.Series):
        return _empty_numeric_series()
    return pd.to_numeric(row, errors="coerce").dropna()


def _growth_pct(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    if previous > 0:
        return ((current - previous) / previous) * 100.0
    if current >= 0:
        return 200.0
    return ((current - previous) / abs(previous)) * 100.0


def _two_point_growth(series: pd.Series) -> float:
    if len(series) < 2:
        return 0.0
    current = float(series.iloc[0])
    previous = float(series.iloc[1])
    growth = _growth_pct(current, previous)
    return float(growth) if growth is not None else 0.0


def _successive_growths(series: pd.Series, *, periods: int) -> list[float]:
    values = [float(item) for item in series.iloc[:periods].tolist()]
    growths: list[float] = []
    for idx in range(len(values) - 1):
        growth = _growth_pct(values[idx], values[idx + 1])
        if growth is None:
            return []
        growths.append(float(growth))
    return growths


def _ratio_series(numerator: pd.Series, denominator: pd.Series) -> list[float]:
    length = min(len(numerator), len(denominator))
    ratios: list[float] = []
    for idx in range(length):
        denominator_value = float(denominator.iloc[idx])
        if denominator_value == 0:
            continue
        ratios.append(float(numerator.iloc[idx]) / denominator_value)
    return ratios


def calculate_eps_metrics(income_quarterly: pd.DataFrame, income_annual: pd.DataFrame) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "quarterly_eps_growth": 0.0,
        "annual_eps_growth": 0.0,
        "eps_growth_acceleration": False,
        "eps_3q_accel": False,
    }

    quarterly_eps = _numeric_row(income_quarterly, "Basic EPS")
    annual_eps = _numeric_row(income_annual, "Basic EPS")

    metrics["quarterly_eps_growth"] = _two_point_growth(quarterly_eps)
    metrics["annual_eps_growth"] = _two_point_growth(annual_eps)

    quarterly_growths = _successive_growths(quarterly_eps, periods=4)
    if len(quarterly_growths) >= 2:
        metrics["eps_growth_acceleration"] = quarterly_growths[0] > quarterly_growths[1]
    if len(quarterly_growths) >= 3:
        metrics["eps_3q_accel"] = quarterly_growths[0] > quarterly_growths[1] > quarterly_growths[2]

    return metrics


def calculate_revenue_metrics(income_quarterly: pd.DataFrame, income_annual: pd.DataFrame) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "quarterly_revenue_growth": 0.0,
        "annual_revenue_growth": 0.0,
        "revenue_growth_acceleration": False,
        "sales_3q_accel": False,
    }

    quarterly_revenue = _numeric_row(income_quarterly, "Total Revenue")
    annual_revenue = _numeric_row(income_annual, "Total Revenue")

    metrics["quarterly_revenue_growth"] = _two_point_growth(quarterly_revenue)
    metrics["annual_revenue_growth"] = _two_point_growth(annual_revenue)

    quarterly_growths = _successive_growths(quarterly_revenue, periods=4)
    if len(quarterly_growths) >= 2:
        metrics["revenue_growth_acceleration"] = quarterly_growths[0] > quarterly_growths[1]
    if len(quarterly_growths) >= 3:
        metrics["sales_3q_accel"] = quarterly_growths[0] > quarterly_growths[1] > quarterly_growths[2]

    return metrics


def calculate_margin_metrics(income_quarterly: pd.DataFrame, income_annual: pd.DataFrame) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "quarterly_op_margin_improved": False,
        "annual_op_margin_improved": False,
        "net_margin_improved": False,
        "margin_3q_accel": False,
        "quarterly_net_income_growth": 0.0,
        "annual_net_income_growth": 0.0,
    }

    quarterly_revenue = _numeric_row(income_quarterly, "Total Revenue")
    annual_revenue = _numeric_row(income_annual, "Total Revenue")
    quarterly_op_income = _numeric_row(income_quarterly, "Operating Income")
    annual_op_income = _numeric_row(income_annual, "Operating Income")
    quarterly_net_income = _numeric_row(income_quarterly, "Net Income")
    annual_net_income = _numeric_row(income_annual, "Net Income")

    quarterly_op_margins = _ratio_series(quarterly_op_income, quarterly_revenue)
    annual_op_margins = _ratio_series(annual_op_income, annual_revenue)
    quarterly_net_margins = _ratio_series(quarterly_net_income, quarterly_revenue)

    if len(quarterly_op_margins) >= 2:
        metrics["quarterly_op_margin_improved"] = quarterly_op_margins[0] > quarterly_op_margins[1]
    if len(annual_op_margins) >= 2:
        metrics["annual_op_margin_improved"] = annual_op_margins[0] > annual_op_margins[1]
    if len(quarterly_net_margins) >= 2:
        metrics["net_margin_improved"] = quarterly_net_margins[0] > quarterly_net_margins[1]
    if len(quarterly_op_margins) >= 4:
        accel1 = quarterly_op_margins[0] - quarterly_op_margins[1]
        accel2 = quarterly_op_margins[1] - quarterly_op_margins[2]
        accel3 = quarterly_op_margins[2] - quarterly_op_margins[3]
        metrics["margin_3q_accel"] = accel1 > accel2 > accel3

    metrics["quarterly_net_income_growth"] = _two_point_growth(quarterly_net_income)
    metrics["annual_net_income_growth"] = _two_point_growth(annual_net_income)

    return metrics


def calculate_financial_ratios(income_annual: pd.DataFrame, balance_annual: pd.DataFrame) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "roe": 0.0,
        "debt_to_equity": 0.0,
    }

    annual_net_income = _numeric_row(income_annual, "Net Income")
    equity = _numeric_row(balance_annual, "Total Stockholder Equity")
    debt = _numeric_row(balance_annual, "Total Liab")

    if len(annual_net_income) > 0 and len(equity) > 0 and float(equity.iloc[0]) != 0:
        metrics["roe"] = (float(annual_net_income.iloc[0]) / float(equity.iloc[0])) * 100.0
    if len(debt) > 0 and len(equity) > 0 and float(equity.iloc[0]) != 0:
        metrics["debt_to_equity"] = float(debt.iloc[0]) / float(equity.iloc[0])

    return metrics


def merge_financial_metrics(*metric_dicts: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for metrics in metric_dicts:
        result.update(metrics)
    return result
