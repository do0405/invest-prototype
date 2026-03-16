from __future__ import annotations

import json
import inspect
import logging
import os
import time
from typing import Any

import pandas as pd
import yfinance as yf
from yahooquery import Ticker

from config import YAHOO_FINANCE_DELAY, YAHOO_FINANCE_MAX_RETRIES
from utils.external_data_cache import load_csv_if_fresh, write_csv_atomic
from utils.io_utils import safe_filename
from utils.market_runtime import get_financial_cache_dir, iter_provider_symbols, market_key
from utils.typing_utils import row_to_record
from utils.yahoo_throttle import extend_yahoo_cooldown, wait_for_yahoo_request_slot
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
_FINANCIAL_CACHE_DIR = get_financial_cache_dir("us")
FINANCIAL_MIN_REQUEST_DELAY_SECONDS = 1.0
FINANCIAL_RATE_LIMIT_COOLDOWN_SECONDS = 45.0


def _cache_dir_for_market(market: str) -> str:
    normalized_market = market_key(market)
    if normalized_market == "us":
        return _FINANCIAL_CACHE_DIR
    return get_financial_cache_dir(normalized_market)


def _cache_path(symbol: str, market: str) -> str:
    cache_dir = _cache_dir_for_market(market)
    safe_symbol = safe_filename(str(symbol or "").strip().upper())
    return f"{cache_dir}/{safe_symbol}.csv"


def _base_financial_payload(symbol: str) -> dict[str, Any]:
    return {
        "symbol": str(symbol or "").strip().upper(),
        "provider_symbol": None,
        "fetch_status": "pending",
        "unavailable_reason": None,
        "source": "",
        "cache_status": "",
        "error_details": [],
        "has_error": False,
    }


def _combine_source_labels(existing: Any, new: Any) -> str:
    labels: list[str] = []
    for raw in (existing, new):
        text = str(raw or "").strip()
        if not text:
            continue
        for part in text.split("+"):
            item = str(part or "").strip()
            if item and item not in labels:
                labels.append(item)
    return "+".join(labels)


def _set_financial_status(
    payload: dict[str, Any],
    *,
    status: str,
    source: str | None = None,
    unavailable_reason: str | None = None,
) -> None:
    payload["fetch_status"] = str(status or "").strip().lower() or "pending"
    if source:
        payload["source"] = _combine_source_labels(payload.get("source"), source)
    if payload["fetch_status"] == "complete":
        payload["unavailable_reason"] = None
    elif unavailable_reason is not None:
        payload["unavailable_reason"] = str(unavailable_reason).strip() or None


def _append_error(payload: dict[str, Any], message: str) -> None:
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


def _load_cached_hybrid_payload(symbol: str, market: str, max_age_seconds: int) -> dict[str, Any] | None:
    cache_file = _cache_path(symbol, market)
    cached = load_csv_if_fresh(cache_file, max_age_seconds=max_age_seconds)
    if cached is None or cached.empty:
        return None

    row = row_to_record(cached.iloc[-1])
    row["symbol"] = str(symbol or "").strip().upper()
    row["error_details"] = _deserialize_error_details(row.get("error_details"))
    row["has_error"] = bool(row.get("has_error", False))
    row["fetch_status"] = str(row.get("fetch_status") or ("complete" if _has_financial_metrics(row) else "failed")).strip().lower()
    row["unavailable_reason"] = str(row.get("unavailable_reason") or "").strip() or None
    row["source"] = str(row.get("source") or "").strip()
    row["cache_status"] = str(row.get("cache_status") or "").strip()
    return row


def _save_cached_hybrid_payload(symbol: str, market: str, payload: dict[str, Any]) -> None:
    serializable = dict(payload)
    serializable["symbol"] = str(symbol or "").strip().upper()
    serializable["error_details"] = json.dumps(_deserialize_error_details(serializable.get("error_details")))
    serializable["cached_at"] = pd.Timestamp.now("UTC").isoformat()
    write_csv_atomic(pd.DataFrame([serializable]), _cache_path(symbol, market), index=False)


def _load_cached_hybrid_payload_any_age(symbol: str, market: str) -> dict[str, Any] | None:
    cache_file = _cache_path(symbol, market)
    if not os.path.exists(cache_file):
        return None
    try:
        cached = pd.read_csv(cache_file)
    except Exception:
        return None
    if cached is None or cached.empty:
        return None

    row = row_to_record(cached.iloc[-1])
    row["symbol"] = str(symbol or "").strip().upper()
    row["error_details"] = _deserialize_error_details(row.get("error_details"))
    row["has_error"] = bool(row.get("has_error", False))
    row["fetch_status"] = str(row.get("fetch_status") or ("complete" if _has_financial_metrics(row) else "failed")).strip().lower()
    row["unavailable_reason"] = str(row.get("unavailable_reason") or "").strip() or None
    row["source"] = str(row.get("source") or "").strip()
    row["cache_status"] = str(row.get("cache_status") or "").strip()
    return row


def _is_financial_rate_limit_error(message: Any) -> bool:
    normalized = str(message or "").lower()
    return (
        "rate limit" in normalized
        or "429" in normalized
        or "too many requests" in normalized
        or "try after a while" in normalized
    )


def _is_financial_unavailable_error(message: Any) -> bool:
    normalized = str(message or "").lower()
    keywords = (
        "delisted",
        "no timezone found",
        "possibly delisted",
        "quote not found",
        "invalid ticker",
        "symbol may be delisted",
        "no price data found",
        "no financial data",
        "financial data unavailable",
        "404",
    )
    return any(keyword in normalized for keyword in keywords)


def _normalize_financial_unavailable_reason(error_msg: Any) -> str:
    raw = str(error_msg or "").strip()
    normalized = raw.lower()

    if "no timezone found" in normalized:
        return "possibly delisted; no timezone found"
    if "no price data found" in normalized:
        return "possibly delisted; no price data found"
    if "symbol may be delisted" in normalized:
        return "possibly delisted; symbol may be delisted"
    if "quote not found" in normalized:
        return "quote not found"
    if "invalid ticker" in normalized:
        return "invalid ticker"
    if "404" in normalized or "not found" in normalized:
        return "quote not found"
    if "no financial data" in normalized or "financial data unavailable" in normalized:
        return "financial data unavailable"

    compact = " ".join(raw.split())
    return compact[:120] if compact else "financial data unavailable"


def _classify_financial_unavailable_reason(reason: str) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized.startswith("possibly delisted;"):
        return "soft"
    if normalized in {"quote not found", "invalid ticker"}:
        return "hard"
    if "financial data unavailable" in normalized or "no financial data" in normalized:
        return "soft"
    if "delisted" in normalized:
        return "hard"
    return "soft"


def _finalize_financial_payload(payload: dict[str, Any]) -> dict[str, Any]:
    finalized = dict(payload)
    errors = _deserialize_error_details(finalized.get("error_details"))
    status = str(finalized.get("fetch_status") or "pending").strip().lower()

    if _has_financial_metrics(finalized):
        finalized["fetch_status"] = "complete"
        finalized["unavailable_reason"] = None
        finalized["has_error"] = bool(errors)
        return finalized

    if status not in {"rate_limited", "soft_unavailable", "delisted", "failed"}:
        unavailable_reason = None
        unavailable_status = ""
        for error_text in errors:
            if _is_financial_rate_limit_error(error_text) or str(error_text).lower().startswith("rate_limited:"):
                unavailable_status = "rate_limited"
                unavailable_reason = "rate limited"
                break
            if _is_financial_unavailable_error(error_text):
                unavailable_reason = _normalize_financial_unavailable_reason(error_text)
                unavailable_status = (
                    "soft_unavailable"
                    if _classify_financial_unavailable_reason(unavailable_reason) == "soft"
                    else "delisted"
                )
                break

        if unavailable_status:
            finalized["fetch_status"] = unavailable_status
            finalized["unavailable_reason"] = unavailable_reason
        elif not errors:
            finalized["fetch_status"] = "soft_unavailable"
            finalized["unavailable_reason"] = "financial data unavailable"
        else:
            finalized["fetch_status"] = "failed"
            finalized["unavailable_reason"] = str(finalized.get("unavailable_reason") or "").strip() or None
    else:
        reason_text = str(finalized.get("unavailable_reason") or "").strip()
        if not reason_text:
            if status == "rate_limited":
                reason_text = "rate limited"
            elif status == "soft_unavailable":
                reason_text = "financial data unavailable"
        finalized["unavailable_reason"] = reason_text or None

    finalized["has_error"] = finalized["fetch_status"] != "complete" or bool(errors)
    return finalized


def _has_financial_metrics(payload: dict[str, Any]) -> bool:
    reserved = {
        "symbol",
        "provider_symbol",
        "fetch_status",
        "unavailable_reason",
        "source",
        "cache_status",
        "error_details",
        "has_error",
        "cached_at",
    }
    for key, value in payload.items():
        if key in reserved:
            continue
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


def _collect_yfinance_metrics(provider_symbol: str, payload: dict[str, Any], delay: float) -> bool:
    wait_for_yahoo_request_slot("MarkMinervini financials", min_interval=max(float(delay), FINANCIAL_MIN_REQUEST_DELAY_SECONDS))
    ticker = yf.Ticker(provider_symbol)

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
        return False

    payload["provider_symbol"] = provider_symbol
    _set_financial_status(payload, status="complete", source="yfinance")
    eps_metrics = calculate_eps_metrics(income_quarterly, income_annual)
    revenue_metrics = calculate_revenue_metrics(income_quarterly, income_annual)
    margin_metrics = calculate_margin_metrics(income_quarterly, income_annual)
    ratio_metrics = calculate_financial_ratios(income_annual, balance_annual)
    payload.update(merge_financial_metrics(eps_metrics, revenue_metrics, margin_metrics, ratio_metrics))
    return True


def _collect_yahooquery_metrics(provider_symbol: str, payload: dict[str, Any], delay: float) -> bool:
    wait_for_yahoo_request_slot("MarkMinervini financials", min_interval=max(float(delay), FINANCIAL_MIN_REQUEST_DELAY_SECONDS))
    ticker = Ticker(provider_symbol)
    income_stmt_q = ticker.income_statement(frequency="quarterly")
    income_stmt_a = ticker.income_statement(frequency="annual")
    balance_sheet = ticker.balance_sheet(frequency="annual")
    updated = False

    if isinstance(income_stmt_q, pd.DataFrame) and not income_stmt_q.empty:
        payload["provider_symbol"] = provider_symbol
        payload["source"] = _combine_source_labels(payload.get("source"), "yahooquery")
        updated = True
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
        payload["provider_symbol"] = provider_symbol
        payload["source"] = _combine_source_labels(payload.get("source"), "yahooquery")
        updated = True
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

    if updated:
        _set_financial_status(payload, status="complete", source="yahooquery")
    return updated


def _collect_symbol_metrics(
    symbol: str,
    mode: str,
    max_retries: int,
    delay: float,
    market: str = "us",
) -> dict[str, Any]:
    symbol_key = str(symbol or "").strip().upper()
    retries = max(1, int(max_retries))
    provider_symbols = iter_provider_symbols(symbol_key, market)
    last_payload = _base_financial_payload(symbol_key)

    for attempt in range(retries):
        payload = _base_financial_payload(symbol_key)
        rate_limited = False

        for provider_symbol in provider_symbols:
            provider_payload = dict(payload)

            if mode in {"yfinance", "hybrid"}:
                try:
                    if _collect_yfinance_metrics(provider_symbol, provider_payload, delay=delay):
                        return _finalize_financial_payload(provider_payload)
                except Exception as exc:
                    error_text = str(exc)
                    if _is_financial_rate_limit_error(error_text):
                        cooldown = FINANCIAL_RATE_LIMIT_COOLDOWN_SECONDS
                        extend_yahoo_cooldown("MarkMinervini financials", cooldown)
                        _append_error(provider_payload, f"rate_limited:{provider_symbol}:{error_text[:80]}")
                        _set_financial_status(
                            provider_payload,
                            status="rate_limited",
                            source="yfinance",
                            unavailable_reason="rate limited",
                        )
                        rate_limited = True
                    elif _is_financial_unavailable_error(error_text):
                        unavailable_reason = _normalize_financial_unavailable_reason(error_text)
                        _append_error(provider_payload, f"unavailable:{provider_symbol}:{unavailable_reason}")
                        _set_financial_status(
                            provider_payload,
                            status=(
                                "soft_unavailable"
                                if _classify_financial_unavailable_reason(unavailable_reason) == "soft"
                                else "delisted"
                            ),
                            source="yfinance",
                            unavailable_reason=unavailable_reason,
                        )
                    else:
                        _append_error(provider_payload, f"yfinance_fetch_failed:{provider_symbol}:{error_text[:80]}")
                        _set_financial_status(provider_payload, status="failed", source="yfinance")

                if rate_limited:
                    last_payload = _finalize_financial_payload(provider_payload)
                    break

            if mode in {"yahooquery", "hybrid"}:
                try:
                    if _collect_yahooquery_metrics(provider_symbol, provider_payload, delay=delay):
                        return _finalize_financial_payload(provider_payload)
                except Exception as exc:
                    error_text = str(exc)
                    if _is_financial_rate_limit_error(error_text):
                        cooldown = FINANCIAL_RATE_LIMIT_COOLDOWN_SECONDS
                        extend_yahoo_cooldown("MarkMinervini financials", cooldown)
                        _append_error(provider_payload, f"rate_limited:{provider_symbol}:{error_text[:80]}")
                        _set_financial_status(
                            provider_payload,
                            status="rate_limited",
                            source="yahooquery",
                            unavailable_reason="rate limited",
                        )
                        rate_limited = True
                    elif _is_financial_unavailable_error(error_text):
                        unavailable_reason = _normalize_financial_unavailable_reason(error_text)
                        _append_error(provider_payload, f"unavailable:{provider_symbol}:{unavailable_reason}")
                        _set_financial_status(
                            provider_payload,
                            status=(
                                "soft_unavailable"
                                if _classify_financial_unavailable_reason(unavailable_reason) == "soft"
                                else "delisted"
                            ),
                            source="yahooquery",
                            unavailable_reason=unavailable_reason,
                        )
                    else:
                        _append_error(provider_payload, f"yahooquery_fetch_failed:{provider_symbol}:{error_text[:80]}")
                        _set_financial_status(provider_payload, status="failed", source="yahooquery")

                if rate_limited:
                    last_payload = _finalize_financial_payload(provider_payload)
                    break

            last_payload = _finalize_financial_payload(provider_payload)

        if _has_financial_metrics(last_payload):
            return _finalize_financial_payload(last_payload)

        if rate_limited:
            continue

        if attempt < (retries - 1) and delay > 0:
            time.sleep(min(delay, 1.0))

    return _finalize_financial_payload(last_payload)


def _collect_financial_data(
    symbols,
    *,
    market: str,
    mode: str,
    max_retries: int,
    delay: float,
    use_cache: bool,
    cache_max_age_hours: int,
) -> pd.DataFrame:
    total = len(symbols)
    rows: list[dict[str, Any]] = []
    cache_max_age_seconds = max(0, int(cache_max_age_hours) * 3600)
    normalized_market = market_key(market)

    for index, symbol in enumerate(symbols):
        symbol_key = str(symbol or "").strip().upper()
        if not symbol_key:
            continue
        print(f"processing {index + 1}/{total} - {normalized_market}:{symbol_key}")

        stale_cached_payload = None
        if use_cache and mode == "hybrid":
            stale_cached_payload = _load_cached_hybrid_payload_any_age(symbol_key, normalized_market)

        collect_params = inspect.signature(_collect_symbol_metrics).parameters
        if "market" in collect_params:
            payload = _collect_symbol_metrics(
                symbol_key,
                mode=mode,
                max_retries=max_retries,
                delay=delay,
                market=normalized_market,
            )
        else:
            payload = _collect_symbol_metrics(
                symbol_key,
                mode=mode,
                max_retries=max_retries,
                delay=delay,
            )

        if (
            stale_cached_payload is not None
            and not _has_financial_metrics(payload)
            and any(_is_financial_rate_limit_error(item) for item in _deserialize_error_details(payload.get("error_details")))
        ):
            stale_payload = dict(stale_cached_payload)
            _append_error(stale_payload, "stale_cache_reused_after_rate_limit")
            stale_payload["cache_status"] = "stale_reused_after_rate_limit"
            stale_payload["source"] = _combine_source_labels(stale_payload.get("source"), "cache")
            stale_payload["has_error"] = True
            rows.append(stale_payload)
            continue

        if mode == "hybrid":
            try:
                _save_cached_hybrid_payload(symbol_key, normalized_market, payload)
            except Exception as exc:
                logger.debug("failed_to_write_financial_cache symbol=%s market=%s error=%s", symbol_key, normalized_market, exc)

        rows.append(payload)

    return pd.DataFrame(rows)


def collect_financial_data(
    symbols,
    max_retries=YAHOO_FINANCE_MAX_RETRIES,
    delay=YAHOO_FINANCE_DELAY,
    *,
    market: str = "us",
):
    print("\ncollecting financial metrics via yfinance")
    return _collect_financial_data(
        symbols,
        market=market,
        mode="yfinance",
        max_retries=max_retries,
        delay=delay,
        use_cache=False,
        cache_max_age_hours=0,
    )


def collect_financial_data_yahooquery(symbols, max_retries=2, delay=1.0, *, market: str = "us"):
    print("\ncollecting financial metrics via yahooquery")
    return _collect_financial_data(
        symbols,
        market=market,
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
    market: str = "us",
    use_cache: bool = True,
    cache_max_age_hours: int = 24,
):
    print("\ncollecting financial metrics via hybrid mode (cache + yfinance + yahooquery)")
    return _collect_financial_data(
        symbols,
        market=market,
        mode="hybrid",
        max_retries=max_retries,
        delay=delay,
        use_cache=use_cache,
        cache_max_age_hours=cache_max_age_hours,
    )
