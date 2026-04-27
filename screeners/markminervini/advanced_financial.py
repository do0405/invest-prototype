from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import ADVANCED_FINANCIAL_CRITERIA, ADVANCED_FINANCIAL_MIN_MET
from utils.market_runtime import (
    ensure_market_dirs,
    get_markminervini_advanced_financial_results_path,
    get_markminervini_integrated_results_path,
    get_markminervini_with_rs_path,
    market_key,
)
from utils.io_utils import write_dataframe_csv_with_fallback, write_dataframe_json_with_fallback
from utils.runtime_context import RuntimeContext
from utils.typing_utils import to_float_or_none
from .data_fetching import collect_financial_data_hybrid


def calculate_percentile_rank(series: pd.Series) -> pd.Series:
    """Return percentile rank in percentage."""
    return series.rank(pct=True) * 100


def _normalize_symbol_column(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "symbol" not in frame.columns:
        return frame
    normalized = frame.copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.strip().str.upper()
    return normalized


def _write_frame_with_snapshot(frame: pd.DataFrame, csv_path: str) -> None:
    json_path = csv_path.replace(".csv", ".json")
    write_dataframe_csv_with_fallback(frame, csv_path, index=False)
    write_dataframe_json_with_fallback(frame, json_path, orient="records", indent=2, force_ascii=False)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    csv_target = Path(csv_path)
    json_target = Path(json_path)
    frame.to_csv(csv_target.with_name(f"{csv_target.stem}_{timestamp}{csv_target.suffix}"), index=False)
    frame.to_json(json_target.with_name(f"{json_target.stem}_{timestamp}{json_target.suffix}"), orient="records", indent=2, force_ascii=False)


def _technical_seed_frame(context_technical_df: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in context_technical_df.columns if column in {"symbol", "rs_score", "met_count"}]
    seed = context_technical_df.loc[:, columns].copy()
    if "symbol" in seed.columns:
        seed = _normalize_symbol_column(seed)
    return seed


def _finalize_advanced_frame(final_df: pd.DataFrame) -> pd.DataFrame:
    if final_df.empty:
        return final_df.reset_index(drop=True)

    if "rs_score" not in final_df.columns:
        final_df["rs_score"] = 0.0
    if "fin_met_count" not in final_df.columns:
        final_df["fin_met_count"] = 0
    if "fetch_status" not in final_df.columns:
        final_df["fetch_status"] = "failed"
    if "unavailable_reason" not in final_df.columns:
        final_df["unavailable_reason"] = None
    if "has_error" not in final_df.columns:
        final_df["has_error"] = True

    final_df["fin_met_count"] = pd.to_numeric(final_df["fin_met_count"], errors="coerce").fillna(0).astype(int)
    final_df["fetch_status"] = final_df["fetch_status"].astype(object).where(final_df["fetch_status"].notna(), "failed").astype(str)
    final_df["has_error"] = final_df["has_error"].astype(object).where(final_df["has_error"].notna(), True).astype(bool)
    final_df["rs_percentile"] = calculate_percentile_rank(final_df["rs_score"].fillna(0))
    final_df["fin_percentile"] = calculate_percentile_rank(final_df["fin_met_count"].fillna(0))
    final_df["total_percentile"] = final_df["rs_percentile"] + final_df["fin_percentile"]
    return final_df.sort_values(
        ["fin_met_count", "total_percentile", "rs_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def _load_cached_advanced_financial_rows(advanced_path: str) -> pd.DataFrame:
    if not os.path.exists(advanced_path):
        return pd.DataFrame(columns=["symbol", "provider_symbol", "fin_met_count", "fetch_status", "unavailable_reason", "has_error"])
    try:
        cached = pd.read_csv(advanced_path, dtype={"symbol": str})
    except Exception:
        return pd.DataFrame(columns=["symbol", "provider_symbol", "fin_met_count", "fetch_status", "unavailable_reason", "has_error"])
    cached = _normalize_symbol_column(cached)
    if cached.empty or "symbol" not in cached.columns:
        return pd.DataFrame(columns=["symbol", "provider_symbol", "fin_met_count", "fetch_status", "unavailable_reason", "has_error"])
    keep_columns = [
        column
        for column in ["symbol", "provider_symbol", "fin_met_count", "fetch_status", "unavailable_reason", "has_error"]
        if column in cached.columns
    ]
    cached = cached.loc[:, keep_columns].dropna(subset=["symbol"])
    return cached.drop_duplicates(subset=["symbol"], keep="first")


def _build_local_only_financial_frame(context_technical_df: pd.DataFrame, advanced_path: str) -> pd.DataFrame:
    seed = _technical_seed_frame(context_technical_df)
    if seed.empty or "symbol" not in seed.columns:
        return pd.DataFrame(columns=["symbol", "fin_met_count", "rs_score", "fetch_status", "unavailable_reason", "has_error"])

    cached = _load_cached_advanced_financial_rows(advanced_path)
    final_df = pd.merge(seed, cached, on="symbol", how="left")
    for column in ("fetch_status", "unavailable_reason", "has_error"):
        if column in final_df.columns:
            final_df[column] = final_df[column].astype(object)
    missing_financials = final_df["fin_met_count"].isna() if "fin_met_count" in final_df.columns else pd.Series(True, index=final_df.index)
    final_df.loc[missing_financials, "fin_met_count"] = 0
    final_df.loc[missing_financials, "fetch_status"] = "skipped_local_only"
    final_df.loc[missing_financials, "unavailable_reason"] = "local_only_no_cached_financials"
    final_df.loc[missing_financials, "has_error"] = False
    if "provider_symbol" not in final_df.columns:
        final_df["provider_symbol"] = None
    return _finalize_advanced_frame(final_df)


def screen_advanced_financials(financial_data: pd.DataFrame) -> pd.DataFrame:
    results: list[dict[str, object]] = []
    for _, row in financial_data.iterrows():
        met_count = 0
        annual_eps_growth = to_float_or_none(row.get("annual_eps_growth"))
        annual_revenue_growth = to_float_or_none(row.get("annual_revenue_growth"))
        debt_to_equity = to_float_or_none(row.get("debt_to_equity"))
        if annual_eps_growth is not None and annual_eps_growth >= ADVANCED_FINANCIAL_CRITERIA["min_annual_eps_growth"]:
            met_count += 1
        if row.get("eps_growth_acceleration"):
            met_count += 1
        if annual_revenue_growth is not None and annual_revenue_growth >= ADVANCED_FINANCIAL_CRITERIA["min_annual_revenue_growth"]:
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
        if debt_to_equity is not None and debt_to_equity <= ADVANCED_FINANCIAL_CRITERIA["max_debt_to_equity"]:
            met_count += 1

        results.append(
            {
                "symbol": row["symbol"],
                "provider_symbol": row.get("provider_symbol"),
                "fin_met_count": met_count,
                "fetch_status": str(row.get("fetch_status") or ("complete" if not bool(row.get("has_error", False)) else "failed")),
                "unavailable_reason": row.get("unavailable_reason"),
                "has_error": bool(row.get("has_error", False)),
            }
        )

    result_df = pd.DataFrame(results)
    if result_df.empty:
        return result_df
    return result_df.reset_index(drop=True)


def run_advanced_financial_screening(
    force_update: bool = False,
    skip_data: bool = False,
    *,
    market: str = "us",
    technical_df: pd.DataFrame | None = None,
    runtime_context: RuntimeContext | None = None,
) -> pd.DataFrame:
    _ = force_update
    normalized_market = market_key(market)
    ensure_market_dirs(normalized_market)

    with_rs_path = get_markminervini_with_rs_path(normalized_market)
    advanced_path = get_markminervini_advanced_financial_results_path(normalized_market)
    integrated_path = get_markminervini_integrated_results_path(normalized_market)

    context_technical_df = None
    if technical_df is not None:
        context_technical_df = technical_df.copy()
    elif runtime_context is not None:
        cached = runtime_context.screening_frames.get("markminervini_with_rs")
        if isinstance(cached, pd.DataFrame):
            context_technical_df = cached.copy()

    if context_technical_df is None and not os.path.exists(with_rs_path):
        empty = pd.DataFrame(columns=["symbol", "fin_met_count", "rs_score", "fetch_status", "unavailable_reason", "has_error"])
        _write_frame_with_snapshot(empty, advanced_path)
        return empty

    if context_technical_df is None:
        context_technical_df = pd.read_csv(with_rs_path, dtype={"symbol": str})
    context_technical_df = _normalize_symbol_column(context_technical_df)
    if context_technical_df.empty or "symbol" not in context_technical_df.columns:
        empty = pd.DataFrame(columns=["symbol", "fin_met_count", "rs_score", "fetch_status", "unavailable_reason", "has_error"])
        _write_frame_with_snapshot(empty, advanced_path)
        return empty

    symbols = context_technical_df["symbol"].dropna().astype(str).str.upper().tolist()
    if skip_data:
        print(f"[AdvancedFinancial] Local-only financial screening ({normalized_market}) - symbols={len(symbols)}")
        final_df = _build_local_only_financial_frame(context_technical_df, advanced_path)
        _write_frame_with_snapshot(final_df, advanced_path)
        _write_frame_with_snapshot(final_df, integrated_path)
        if runtime_context is not None:
            runtime_context.screening_frames["advanced_financial_df"] = final_df.copy()
        print(
            f"[AdvancedFinancial] Local-only financial screening saved ({normalized_market}) - "
            f"rows={len(final_df)}, cached={int((final_df['fetch_status'] != 'skipped_local_only').sum()) if not final_df.empty else 0}, "
            f"placeholders={int((final_df['fetch_status'] == 'skipped_local_only').sum()) if not final_df.empty else 0}"
        )
        return final_df

    print(f"[AdvancedFinancial] Financial fetch started ({normalized_market}) - symbols={len(symbols)}")
    financial_data = collect_financial_data_hybrid(symbols, max_retries=2, delay=1.0, market=normalized_market)
    if financial_data.empty:
        empty = context_technical_df.loc[:, [column for column in context_technical_df.columns if column in {"symbol", "rs_score"}]].copy()
        empty.attrs.update(getattr(financial_data, "attrs", {}) or {})
        empty["fin_met_count"] = 0
        empty["fetch_status"] = "failed"
        empty["unavailable_reason"] = "financial data unavailable"
        empty["has_error"] = True
        _write_frame_with_snapshot(empty, advanced_path)
        return empty

    filtered_financial_df = _normalize_symbol_column(screen_advanced_financials(financial_data))
    final_df = pd.merge(
        _technical_seed_frame(context_technical_df),
        filtered_financial_df,
        on="symbol",
        how="left",
    )
    final_df = _finalize_advanced_frame(final_df)
    final_df.attrs.update(getattr(financial_data, "attrs", {}) or {})

    _write_frame_with_snapshot(final_df, advanced_path)

    # Preserve the historical integrated seed output expected by the integrated screener.
    _write_frame_with_snapshot(final_df, integrated_path)
    if runtime_context is not None:
        runtime_context.screening_frames["advanced_financial_df"] = final_df.copy()
    print(
        f"[AdvancedFinancial] Financial screening saved ({normalized_market}) - "
        f"rows={len(final_df)}, passed={int((final_df['fin_met_count'] >= ADVANCED_FINANCIAL_MIN_MET).sum()) if not final_df.empty else 0}"
    )
    return final_df
