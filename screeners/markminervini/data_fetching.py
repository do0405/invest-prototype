import json
import logging
import os
import time
from typing import Any, Dict

import pandas as pd
import yfinance as yf
from yahooquery import Ticker

from config import DATA_DIR, YAHOO_FINANCE_DELAY, YAHOO_FINANCE_MAX_RETRIES
from utils.external_data_cache import load_csv_if_fresh, write_csv_atomic
from utils.io_utils import safe_filename
from .financial_calculators import (
    calculate_eps_metrics,
    calculate_financial_ratios,
    calculate_margin_metrics,
    calculate_revenue_metrics,
    merge_financial_metrics,
)

__all__ = [
    "collect_financial_data",
    "collect_financial_data_yahooquery",
    "collect_financial_data_hybrid",
]


logger = logging.getLogger(__name__)
_FINANCIAL_CACHE_DIR = os.path.join(DATA_DIR, "external", "financials", "us")
_FINANCIAL_CACHE_MAX_AGE_SECONDS = 24 * 60 * 60


def _cache_path(symbol: str) -> str:
    safe_symbol = safe_filename(str(symbol or "").strip().upper())
    return os.path.join(_FINANCIAL_CACHE_DIR, f"{safe_symbol}.csv")


def _base_financial_payload(symbol: str) -> Dict[str, Any]:
    return {
        "symbol": str(symbol or "").strip().upper(),
        "error_details": [],
        "has_error": False,
    }


def _append_error(payload: Dict[str, Any], message: str) -> None:
    details = payload.get("error_details")
    if not isinstance(details, list):
        details = []
    details.append(str(message))
    payload["error_details"] = details


def _deserialize_error_details(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    except Exception:
        pass
    return [text]


def _load_cached_hybrid_payload(symbol: str, max_age_seconds: int) -> Dict[str, Any] | None:
    cache_file = _cache_path(symbol)
    cached = load_csv_if_fresh(cache_file, max_age_seconds=max_age_seconds)
    if cached is None or cached.empty:
        return None

    row = cached.iloc[-1].to_dict()
    row["symbol"] = str(symbol or "").strip().upper()
    row["error_details"] = _deserialize_error_details(row.get("error_details"))
    row["has_error"] = bool(row.get("has_error", False))
    return row


def _save_cached_hybrid_payload(symbol: str, payload: Dict[str, Any]) -> None:
    serializable = dict(payload)
    serializable["symbol"] = str(symbol or "").strip().upper()
    serializable["error_details"] = json.dumps(_deserialize_error_details(serializable.get("error_details")))
    serializable["cached_at"] = pd.Timestamp.now("UTC").isoformat()
    write_csv_atomic(pd.DataFrame([serializable]), _cache_path(symbol), index=False)


def _has_financial_metrics(payload: Dict[str, Any]) -> bool:
    reserved = {"symbol", "error_details", "has_error", "cached_at"}
    for key, value in payload.items():
        if key in reserved:
            continue
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


def _collect_yfinance_metrics(symbol: str, payload: Dict[str, Any], delay: float) -> None:
    ticker = yf.Ticker(symbol)
    if delay > 0:
        time.sleep(delay)

    income_quarterly = ticker.quarterly_financials
    income_annual = ticker.financials
    balance_annual = ticker.balance_sheet

    if (
        income_quarterly is None
        or income_annual is None
        or balance_annual is None
        or income_quarterly.empty
        or income_annual.empty
        or balance_annual.empty
    ):
        _append_error(payload, "yfinance_missing_financial_statements")
        return

    eps_metrics = calculate_eps_metrics(income_quarterly, income_annual)
    revenue_metrics = calculate_revenue_metrics(income_quarterly, income_annual)
    margin_metrics = calculate_margin_metrics(income_quarterly, income_annual)
    ratio_metrics = calculate_financial_ratios(income_annual, balance_annual)
    payload.update(merge_financial_metrics(eps_metrics, revenue_metrics, margin_metrics, ratio_metrics))


def _collect_yahooquery_metrics(symbol: str, payload: Dict[str, Any]) -> None:
    ticker = Ticker(symbol)
    income_stmt_q = ticker.income_statement(frequency="quarterly")
    income_stmt_a = ticker.income_statement(frequency="annual")
    balance_sheet = ticker.balance_sheet(frequency="annual")

    if isinstance(income_stmt_q, pd.DataFrame) and not income_stmt_q.empty:
        if "TotalRevenue" in income_stmt_q.columns:
            revenue_data = income_stmt_q["TotalRevenue"].dropna()
            if len(revenue_data) >= 2:
                recent_revenue = revenue_data.iloc[0]
                prev_revenue = revenue_data.iloc[1]
                if prev_revenue != 0:
                    payload["quarterly_revenue_growth"] = ((recent_revenue - prev_revenue) / abs(prev_revenue)) * 100

        if "NetIncome" in income_stmt_q.columns:
            net_income_data = income_stmt_q["NetIncome"].dropna()
            if len(net_income_data) >= 2:
                recent_ni = net_income_data.iloc[0]
                prev_ni = net_income_data.iloc[1]
                if prev_ni != 0:
                    if prev_ni > 0:
                        payload["quarterly_net_income_growth"] = ((recent_ni - prev_ni) / prev_ni) * 100
                    elif recent_ni >= 0:
                        payload["quarterly_net_income_growth"] = 200
                    else:
                        payload["quarterly_net_income_growth"] = ((recent_ni - prev_ni) / abs(prev_ni)) * 100

    if isinstance(balance_sheet, pd.DataFrame) and not balance_sheet.empty:
        if (
            "StockholdersEquity" in balance_sheet.columns
            and isinstance(income_stmt_a, pd.DataFrame)
            and "NetIncome" in income_stmt_a.columns
        ):
            equity = balance_sheet["StockholdersEquity"].dropna()
            net_income = income_stmt_a["NetIncome"].dropna()
            if len(equity) > 0 and len(net_income) > 0 and equity.iloc[0] != 0:
                payload["roe"] = (net_income.iloc[0] / equity.iloc[0]) * 100

        if (
            "TotalLiabilitiesNetMinorityInterest" in balance_sheet.columns
            and "StockholdersEquity" in balance_sheet.columns
        ):
            debt = balance_sheet["TotalLiabilitiesNetMinorityInterest"].dropna()
            equity = balance_sheet["StockholdersEquity"].dropna()
            if len(debt) > 0 and len(equity) > 0 and equity.iloc[0] != 0:
                payload["debt_to_equity"] = debt.iloc[0] / equity.iloc[0]


def _collect_symbol_metrics(
    symbol: str,
    *,
    mode: str,
    max_retries: int,
    delay: float,
) -> Dict[str, Any]:
    symbol_key = str(symbol or "").strip().upper()
    retries = max(1, int(max_retries))

    last_payload = _base_financial_payload(symbol_key)
    for attempt in range(retries):
        payload = _base_financial_payload(symbol_key)

        if mode in {"yfinance", "hybrid"}:
            try:
                _collect_yfinance_metrics(symbol_key, payload, delay=delay)
            except Exception as exc:
                _append_error(payload, f"yfinance_fetch_failed:{str(exc)[:80]}")

        if mode in {"yahooquery", "hybrid"}:
            try:
                _collect_yahooquery_metrics(symbol_key, payload)
            except Exception as exc:
                _append_error(payload, f"yahooquery_fetch_failed:{str(exc)[:80]}")

        payload["has_error"] = bool(payload.get("error_details"))
        if _has_financial_metrics(payload):
            return payload

        last_payload = payload
        if attempt < (retries - 1) and delay > 0:
            time.sleep(delay)

    return last_payload


def _collect_financial_data(
    symbols,
    *,
    mode: str,
    max_retries: int,
    delay: float,
    use_cache: bool,
    cache_max_age_hours: int,
) -> pd.DataFrame:
    total = len(symbols)
    rows: list[Dict[str, Any]] = []
    cache_max_age_seconds = max(0, int(cache_max_age_hours) * 3600)

    for index, symbol in enumerate(symbols):
        symbol_key = str(symbol or "").strip().upper()
        if not symbol_key:
            continue
        print(f"processing {index + 1}/{total} - {symbol_key}")

        cached_payload = None
        if use_cache and mode == "hybrid":
            cached_payload = _load_cached_hybrid_payload(symbol_key, max_age_seconds=cache_max_age_seconds)

        if cached_payload is not None:
            rows.append(cached_payload)
            continue

        payload = _collect_symbol_metrics(
            symbol_key,
            mode=mode,
            max_retries=max_retries,
            delay=delay,
        )

        if mode == "hybrid":
            try:
                _save_cached_hybrid_payload(symbol_key, payload)
            except Exception as exc:
                logger.debug("failed_to_write_financial_cache symbol=%s error=%s", symbol_key, exc)

        rows.append(payload)

    return pd.DataFrame(rows)


def collect_financial_data(symbols, max_retries=YAHOO_FINANCE_MAX_RETRIES, delay=YAHOO_FINANCE_DELAY):
    """Collect financial metrics via yfinance only."""
    print("\ncollecting financial metrics via yfinance")
    return _collect_financial_data(
        symbols,
        mode="yfinance",
        max_retries=max_retries,
        delay=delay,
        use_cache=False,
        cache_max_age_hours=0,
    )


def collect_financial_data_yahooquery(symbols, max_retries=2, delay=1.0):
    """Collect financial metrics via yahooquery only."""
    print("\ncollecting financial metrics via yahooquery")
    return _collect_financial_data(
        symbols,
        mode="yahooquery",
        max_retries=max_retries,
        delay=delay,
        use_cache=False,
        cache_max_age_hours=0,
    )


def collect_financial_data_hybrid(
    symbols,
    max_retries=2,
    delay=1.0,
    *,
    use_cache: bool = True,
    cache_max_age_hours: int = 24,
):
    """Collect financial metrics with local CSV cache + hybrid fallback."""
    print("\ncollecting financial metrics via hybrid mode (cache + yfinance + yahooquery)")
    return _collect_financial_data(
        symbols,
        mode="hybrid",
        max_retries=max_retries,
        delay=delay,
        use_cache=use_cache,
        cache_max_age_hours=cache_max_age_hours,
    )
