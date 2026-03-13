from __future__ import annotations

import os

import pandas as pd

from config import ADVANCED_FINANCIAL_CRITERIA, ADVANCED_FINANCIAL_MIN_MET
from utils.market_runtime import (
    ensure_market_dirs,
    get_markminervini_advanced_financial_results_path,
    get_markminervini_integrated_results_path,
    get_markminervini_with_rs_path,
    market_key,
)
from .data_fetching import collect_financial_data_hybrid
from .financial_metrics import calculate_percentile_rank


def screen_advanced_financials(financial_data: pd.DataFrame) -> pd.DataFrame:
    results: list[dict[str, object]] = []
    for _, row in financial_data.iterrows():
        met_count = 0
        if pd.notna(row.get("annual_eps_growth")) and row["annual_eps_growth"] >= ADVANCED_FINANCIAL_CRITERIA["min_annual_eps_growth"]:
            met_count += 1
        if row.get("eps_growth_acceleration"):
            met_count += 1
        if pd.notna(row.get("annual_revenue_growth")) and row["annual_revenue_growth"] >= ADVANCED_FINANCIAL_CRITERIA["min_annual_revenue_growth"]:
            met_count += 1
        if row.get("revenue_growth_acceleration"):
            met_count += 1
        if row.get("net_margin_improved"):
            met_count += 1
        if row.get("eps_3q_accel"):
            met_count += 1
        if row.get("sales_3q_accel"):
            met_count += 1
        if row.get("margin_3q_accel"):
            met_count += 1
        if pd.notna(row.get("debt_to_equity")) and row["debt_to_equity"] <= ADVANCED_FINANCIAL_CRITERIA["max_debt_to_equity"]:
            met_count += 1

        results.append(
            {
                "symbol": row["symbol"],
                "provider_symbol": row.get("provider_symbol"),
                "fin_met_count": met_count,
                "has_error": bool(row.get("has_error", False)),
            }
        )

    result_df = pd.DataFrame(results)
    if result_df.empty:
        return result_df
    return result_df[result_df["fin_met_count"] >= ADVANCED_FINANCIAL_MIN_MET].reset_index(drop=True)


def run_advanced_financial_screening(
    force_update: bool = False,
    skip_data: bool = False,
    *,
    market: str = "us",
) -> pd.DataFrame:
    _ = (force_update, skip_data)
    normalized_market = market_key(market)
    ensure_market_dirs(normalized_market)

    with_rs_path = get_markminervini_with_rs_path(normalized_market)
    advanced_path = get_markminervini_advanced_financial_results_path(normalized_market)
    integrated_path = get_markminervini_integrated_results_path(normalized_market)

    if not os.path.exists(with_rs_path):
        empty = pd.DataFrame(columns=["symbol", "fin_met_count", "rs_score", "has_error"])
        empty.to_csv(advanced_path, index=False)
        empty.to_json(advanced_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
        return empty

    technical_df = pd.read_csv(with_rs_path)
    if technical_df.empty or "symbol" not in technical_df.columns:
        empty = pd.DataFrame(columns=["symbol", "fin_met_count", "rs_score", "has_error"])
        empty.to_csv(advanced_path, index=False)
        empty.to_json(advanced_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
        return empty

    symbols = technical_df["symbol"].dropna().astype(str).str.upper().tolist()
    print(f"[AdvancedFinancial] Financial fetch started ({normalized_market}) - symbols={len(symbols)}")
    financial_data = collect_financial_data_hybrid(symbols, max_retries=2, delay=1.0, market=normalized_market)
    if financial_data.empty:
        empty = technical_df.loc[:, [column for column in technical_df.columns if column in {"symbol", "rs_score"}]].copy()
        empty["fin_met_count"] = 0
        empty["has_error"] = True
        empty.to_csv(advanced_path, index=False)
        empty.to_json(advanced_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
        return empty

    filtered_financial_df = screen_advanced_financials(financial_data)
    final_df = pd.merge(
        technical_df.loc[:, [column for column in technical_df.columns if column in {"symbol", "rs_score", "met_count"}]],
        filtered_financial_df,
        on="symbol",
        how="left",
    )

    if not final_df.empty:
        rs_percentiles = calculate_percentile_rank(final_df["rs_score"].fillna(0))
        fin_percentiles = calculate_percentile_rank(final_df["fin_met_count"].fillna(0))
        final_df["rs_percentile"] = rs_percentiles
        final_df["fin_percentile"] = fin_percentiles
        final_df["total_percentile"] = final_df["rs_percentile"] + final_df["fin_percentile"]
        final_df["fin_met_count"] = final_df["fin_met_count"].fillna(0).astype(int)
        final_df["has_error"] = final_df["has_error"].fillna(True)
        final_df = final_df.sort_values(
            ["fin_met_count", "total_percentile", "rs_score"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    final_df.to_csv(advanced_path, index=False)
    final_df.to_json(advanced_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)

    # Preserve the historical integrated seed output expected by the integrated screener.
    final_df.to_csv(integrated_path, index=False)
    final_df.to_json(integrated_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
    print(
        f"[AdvancedFinancial] Financial screening saved ({normalized_market}) - "
        f"rows={len(final_df)}, passed={int((final_df['fin_met_count'] >= ADVANCED_FINANCIAL_MIN_MET).sum()) if not final_df.empty else 0}"
    )
    return final_df
