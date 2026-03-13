from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd

from utils.market_runtime import (
    ensure_market_dirs,
    get_markminervini_integrated_results_path,
    get_markminervini_new_tickers_path,
    get_markminervini_previous_with_rs_path,
    get_markminervini_with_rs_path,
    market_key,
)


def track_new_tickers(advanced_financial_results_path: str, *, market: str = "us") -> pd.DataFrame:
    normalized_market = market_key(market)
    ensure_market_dirs(normalized_market)

    current_path = get_markminervini_with_rs_path(normalized_market)
    previous_path = get_markminervini_previous_with_rs_path(normalized_market)
    new_tickers_path = get_markminervini_new_tickers_path(normalized_market)
    integrated_path = get_markminervini_integrated_results_path(normalized_market)

    if not os.path.exists(current_path):
        return pd.DataFrame()

    current_df = pd.read_csv(current_path)
    if current_df.empty or "symbol" not in current_df.columns:
        return pd.DataFrame()

    previous_symbols: set[str] = set()
    if os.path.exists(previous_path):
        previous_df = pd.read_csv(previous_path)
        if "symbol" in previous_df.columns:
            previous_symbols = set(previous_df["symbol"].dropna().astype(str).str.upper())

    new_symbols = set(current_df["symbol"].dropna().astype(str).str.upper()) - previous_symbols

    if os.path.exists(advanced_financial_results_path):
        financial_df = pd.read_csv(advanced_financial_results_path)
    else:
        financial_df = pd.DataFrame(columns=["symbol", "fin_met_count"])

    if os.path.exists(integrated_path):
        integrated_df = pd.read_csv(integrated_path)
    else:
        integrated_df = pd.DataFrame(columns=["symbol", "met_count", "total_met_count"])

    if os.path.exists(new_tickers_path):
        tracked_df = pd.read_csv(new_tickers_path)
    else:
        tracked_df = pd.DataFrame(columns=["symbol", "fin_met_count", "rs_score", "met_count", "total_met_count", "added_date"])

    today = datetime.now().date()
    records: list[dict[str, object]] = []
    for symbol in sorted(new_symbols):
        current_row = current_df[current_df["symbol"].astype(str).str.upper() == symbol]
        financial_row = financial_df[financial_df["symbol"].astype(str).str.upper() == symbol]
        integrated_row = integrated_df[integrated_df["symbol"].astype(str).str.upper() == symbol]
        if current_row.empty:
            continue
        records.append(
            {
                "symbol": symbol,
                "fin_met_count": int(financial_row.iloc[0]["fin_met_count"]) if not financial_row.empty and "fin_met_count" in financial_row.columns else 0,
                "rs_score": float(current_row.iloc[0].get("rs_score", 0)),
                "met_count": int(integrated_row.iloc[0]["met_count"]) if not integrated_row.empty and "met_count" in integrated_row.columns else int(current_row.iloc[0].get("met_count", 0)),
                "total_met_count": int(integrated_row.iloc[0]["total_met_count"]) if not integrated_row.empty and "total_met_count" in integrated_row.columns else 0,
                "added_date": today.strftime("%Y-%m-%d"),
            }
        )

    if records:
        tracked_df = pd.concat([tracked_df, pd.DataFrame(records)], ignore_index=True)

    tracked_df["added_date"] = pd.to_datetime(tracked_df["added_date"], errors="coerce").dt.date
    cutoff = today - timedelta(days=14)
    tracked_df = tracked_df[tracked_df["added_date"] > cutoff].sort_values("added_date", ascending=False)

    current_df.to_csv(previous_path, index=False)
    current_df.to_json(previous_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
    tracked_df.to_csv(new_tickers_path, index=False)
    tracked_df.to_json(new_tickers_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
    return tracked_df
