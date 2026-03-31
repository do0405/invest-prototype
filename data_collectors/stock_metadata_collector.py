#!/usr/bin/env python3
"""Collect market metadata for US/KR symbols from Yahoo providers."""

from __future__ import annotations

import argparse
import contextlib
import io
import logging
import os
import time
from datetime import datetime, timezone
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Dict, List, Optional

import pandas as pd
import yfinance as yf
from yahooquery import Ticker

from config import DATA_DIR
from config import YAHOO_FINANCE_DELAY, YAHOO_FINANCE_MAX_RETRIES
from data_collectors.symbol_universe import load_kr_symbol_universe, load_us_symbol_universe
from utils.console_runtime import bootstrap_windows_utf8
from utils.io_utils import ensure_dir
from utils.market_runtime import (
    get_market_data_dir,
    get_stock_metadata_path,
    iter_provider_symbols,
    market_key,
)
from utils.typing_utils import frame_keyed_records, is_na_like, row_to_record
from utils.yfinance_runtime import bootstrap_yfinance_cache
from utils.yahoo_throttle import extend_yahoo_cooldown, wait_for_yahoo_request_slot


logger = logging.getLogger(__name__)

METADATA_BATCH_SIZE = 50
METADATA_BATCH_PAUSE_SECONDS = 15.0
METADATA_MAX_WORKERS = 1
METADATA_PROGRESS_HEARTBEAT_SECONDS = 15.0
METADATA_RATE_LIMIT_COOLDOWN_SECONDS = 45.0
METADATA_MIN_REQUEST_DELAY_SECONDS = 1.0
METADATA_FRESHNESS_WINDOW_DAYS = 7

METADATA_COLUMNS: tuple[str, ...] = (
    "symbol",
    "market",
    "provider_symbol",
    "exchange",
    "sector",
    "industry",
    "pe_ratio",
    "revenue_growth",
    "earnings_growth",
    "return_on_equity",
    "market_cap",
    "shares_outstanding",
    "fetch_status",
    "source",
    "last_attempted_at",
)

_MEANINGFUL_KEYS: tuple[str, ...] = (
    "exchange",
    "sector",
    "industry",
    "revenue_growth",
    "earnings_growth",
    "return_on_equity",
    "market_cap",
    "shares_outstanding",
)


def _emit_progress(message: str) -> None:
    print(message, flush=True)
    logger.info(message)


def _iter_batches(items: List[str], batch_size: int) -> List[List[str]]:
    size = max(1, int(batch_size))
    return [items[index : index + size] for index in range(0, len(items), size)]


def _format_eta(completed: int, total: int, elapsed: float) -> str:
    if completed <= 0 or elapsed <= 0:
        return "unknown"
    remaining = max(0, total - completed)
    rate = completed / elapsed
    if rate <= 0:
        return "unknown"
    eta_seconds = remaining / rate
    return f"{eta_seconds:.1f}s"


def _blank_record(symbol: str, market: str, provider_symbol: str | None = None) -> dict[str, object]:
    normalized_market = market_key(market)
    return {
        "symbol": str(symbol or "").strip().upper(),
        "market": normalized_market,
        "provider_symbol": provider_symbol,
        "exchange": "",
        "sector": "",
        "industry": "",
        "pe_ratio": None,
        "revenue_growth": None,
        "earnings_growth": None,
        "return_on_equity": None,
        "market_cap": None,
        "shares_outstanding": None,
        "fetch_status": "pending",
        "source": "",
        "last_attempted_at": "",
    }


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "null"}:
        return ""
    return text


def _clean_number(value: Any) -> float | int | None:
    if value is None or is_na_like(value):
        return None

    try:
        numeric = float(value)
    except Exception:
        return None

    if is_na_like(numeric):
        return None
    if numeric.is_integer():
        return int(numeric)
    return numeric


def _clean_ratio(value: Any) -> float | None:
    numeric = _clean_number(value)
    if numeric is None:
        return None
    return float(numeric)


def _clean_growth_percent(value: Any) -> float | None:
    numeric = _clean_ratio(value)
    if numeric is None:
        return None
    return float(numeric) * 100.0


def _combine_source_labels(existing: Any, new: Any) -> str:
    labels: list[str] = []
    for raw in (existing, new):
        text = _clean_text(raw)
        if not text:
            continue
        for part in text.split("+"):
            item = _clean_text(part)
            if item and item not in labels:
                labels.append(item)
    return "+".join(labels)


def _has_identity_metadata(record: dict[str, object]) -> bool:
    return any(_clean_text(record.get(key)) for key in ("exchange", "sector", "industry"))


def _has_descriptive_identity_metadata(record: dict[str, object]) -> bool:
    return any(_clean_text(record.get(key)) for key in ("sector", "industry"))


def _has_core_metric_metadata(record: dict[str, object]) -> bool:
    for key in ("market_cap", "shares_outstanding", "revenue_growth", "earnings_growth", "return_on_equity"):
        value = record.get(key)
        if is_na_like(value):
            continue
        if value is not None:
            return True
    return False


def _is_metadata_complete(record: dict[str, object]) -> bool:
    return _has_identity_metadata(record) and _has_core_metric_metadata(record)


def _is_yfinance_candidate_sufficient(record: dict[str, object]) -> bool:
    return _has_descriptive_identity_metadata(record) and _has_core_metric_metadata(record)


def _mark_record(
    record: dict[str, object],
    *,
    status: str | None = None,
    source: str | None = None,
    attempted_at: str | None = None,
) -> dict[str, object]:
    updated = dict(record)
    if status:
        updated["fetch_status"] = str(status).strip().lower()
    if source:
        updated["source"] = _combine_source_labels(updated.get("source"), source)
    if attempted_at:
        updated["last_attempted_at"] = str(attempted_at)
    return updated


def _is_retryable_metadata_record(record: dict[str, object]) -> bool:
    status = _clean_text(record.get("fetch_status")).lower()
    if status == "complete":
        return False
    if status == "not_found":
        return False
    if status in {"partial_fast_info", "rate_limited", "failed", "pending", ""}:
        return True
    return not _is_metadata_complete(record)


def _has_meaningful_metadata(record: dict[str, object]) -> bool:
    for key in _MEANINGFUL_KEYS:
        value = record.get(key)
        if isinstance(value, str):
            if value.strip():
                return True
            continue
        if is_na_like(value):
            continue
        if value is not None:
            return True
    return False


def _should_preserve_cached_metadata(base: dict[str, object], update: dict[str, object]) -> bool:
    base_status = _clean_text(base.get("fetch_status")).lower()
    update_status = _clean_text(update.get("fetch_status")).lower()

    if _has_meaningful_metadata(base) and not _has_meaningful_metadata(update):
        return True

    if _is_metadata_complete(base) and update_status in {"partial_fast_info", "rate_limited", "failed", "pending", ""}:
        return True

    if base_status == "not_found" and update_status in {"rate_limited", "failed", "pending", ""}:
        return not _has_meaningful_metadata(update)

    return False


def _merge_records(base: dict[str, object], update: dict[str, object]) -> dict[str, object]:
    merged = dict(base)
    for key in METADATA_COLUMNS:
        value = update.get(key)
        if key in {"symbol", "market"}:
            if value:
                merged[key] = value
            continue
        if key in {"exchange", "sector", "industry", "provider_symbol", "fetch_status", "last_attempted_at"}:
            if _clean_text(value):
                merged[key] = _clean_text(value)
            continue
        if key == "source":
            merged[key] = _combine_source_labels(merged.get(key), value)
            continue
        if value is not None:
            merged[key] = value
    return merged


def _normalize_metadata_frame(frame: pd.DataFrame | None, market: str) -> pd.DataFrame:
    normalized_market = market_key(market)
    if frame is None or frame.empty:
        return pd.DataFrame(columns=METADATA_COLUMNS)

    normalized = frame.copy()
    for column in METADATA_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = None if column not in {"symbol", "market", "provider_symbol", "exchange", "sector", "industry", "fetch_status", "source", "last_attempted_at"} else ""

    normalized["symbol"] = normalized["symbol"].map(_clean_text).str.upper()
    normalized["market"] = normalized["market"].map(_clean_text).str.lower()
    normalized.loc[normalized["market"] == "", "market"] = normalized_market

    for text_column in ("provider_symbol", "exchange", "sector", "industry", "fetch_status", "source", "last_attempted_at"):
        normalized[text_column] = normalized[text_column].map(_clean_text)

    for numeric_column in (
        "pe_ratio",
        "revenue_growth",
        "earnings_growth",
        "return_on_equity",
        "market_cap",
        "shares_outstanding",
    ):
        normalized[numeric_column] = pd.to_numeric(normalized[numeric_column], errors="coerce")

    normalized = normalized[list(METADATA_COLUMNS)]
    normalized = normalized[normalized["symbol"] != ""]
    normalized = normalized.drop_duplicates(subset=["symbol"], keep="last").sort_values("symbol").reset_index(drop=True)
    return normalized


def _pick_provider_payload(payload: Any, provider_symbol: str) -> dict[str, Any]:
    if isinstance(payload, dict):
        if provider_symbol in payload and isinstance(payload[provider_symbol], dict):
            return payload[provider_symbol]
        if len(payload) == 1:
            only_value = next(iter(payload.values()))
            if isinstance(only_value, dict):
                return only_value
        return payload
    return {}


def _is_definitive_not_found(message: str | None) -> bool:
    normalized = str(message or "").strip().lower()
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "not found",
            "quote not found",
            "404",
            "no data found",
            "symbol may be delisted",
        )
    )


def _is_rate_limited_message(message: str | None) -> bool:
    normalized = str(message or "").strip().lower()
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "too many requests",
            "rate limited",
            "rate limit",
            "429",
            "try after a while",
        )
    )


def _fetch_yfinance_info_quietly(provider_symbol: str) -> tuple[dict[str, Any], dict[str, Any], bool, bool]:
    sink = io.StringIO()
    info: dict[str, Any] = {}
    fast_info: dict[str, Any] = {}
    definitive_missing = False
    rate_limited = False
    try:
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            ticker = yf.Ticker(provider_symbol)
            try:
                info_raw = ticker.info or {}
                info = info_raw if isinstance(info_raw, dict) else {}
            except Exception as exc:
                definitive_missing = definitive_missing or _is_definitive_not_found(str(exc))
                rate_limited = rate_limited or _is_rate_limited_message(str(exc))
            try:
                fast_info_raw = ticker.fast_info
                fast_info = dict(fast_info_raw) if fast_info_raw is not None else {}
            except Exception as exc:
                definitive_missing = definitive_missing or _is_definitive_not_found(str(exc))
                rate_limited = rate_limited or _is_rate_limited_message(str(exc))
    except Exception as exc:
        definitive_missing = definitive_missing or _is_definitive_not_found(str(exc))
        rate_limited = rate_limited or _is_rate_limited_message(str(exc))

    noisy_output = sink.getvalue()
    definitive_missing = definitive_missing or _is_definitive_not_found(noisy_output)
    rate_limited = rate_limited or _is_rate_limited_message(noisy_output)
    return info, fast_info, definitive_missing, rate_limited
def _record_from_yfinance(
    symbol: str,
    market: str,
    provider_symbol: str,
    info: dict[str, Any],
    fast_info: dict[str, Any] | None = None,
) -> dict[str, object]:
    fast_info = fast_info if isinstance(fast_info, dict) else {}
    record = _blank_record(symbol, market, provider_symbol=provider_symbol)
    record.update(
        {
            "exchange": _clean_text(info.get("exchange") or info.get("fullExchangeName") or fast_info.get("exchange")),
            "sector": _clean_text(info.get("sector")),
            "industry": _clean_text(info.get("industry")),
            "pe_ratio": _clean_number(info.get("trailingPE")),
            "revenue_growth": _clean_growth_percent(info.get("revenueGrowth")),
            "earnings_growth": _clean_growth_percent(
                info.get("earningsQuarterlyGrowth") or info.get("earningsGrowth")
            ),
            "return_on_equity": _clean_ratio(info.get("returnOnEquity")),
            "market_cap": _clean_number(info.get("marketCap") or fast_info.get("marketCap")),
            "shares_outstanding": _clean_number(info.get("sharesOutstanding") or fast_info.get("shares")),
        }
    )
    return record


def fetch_metadata_yahooquery(
    symbol: str,
    provider_symbol: str,
    *,
    market: str,
    delay: float = 1.0,
) -> tuple[dict[str, object], bool]:
    record = _blank_record(symbol, market, provider_symbol=provider_symbol)
    try:
        ticker = Ticker(provider_symbol)
        wait_for_yahoo_request_slot(f"{market_key(market).upper()} metadata", min_interval=max(float(delay), METADATA_MIN_REQUEST_DELAY_SECONDS))

        summary = _pick_provider_payload(ticker.summary_detail, provider_symbol)
        key_stats = _pick_provider_payload(ticker.key_stats, provider_symbol)
        profile = _pick_provider_payload(ticker.summary_profile, provider_symbol)
        financial_data = _pick_provider_payload(getattr(ticker, "financial_data", {}), provider_symbol)

        record.update(
            {
                "exchange": _clean_text(profile.get("exchange") or summary.get("exchange") or summary.get("fullExchangeName")),
                "sector": _clean_text(profile.get("sector")),
                "industry": _clean_text(profile.get("industry")),
                "pe_ratio": _clean_number(summary.get("trailingPE")),
                "revenue_growth": _clean_growth_percent(
                    key_stats.get("revenueQuarterlyGrowth") or financial_data.get("revenueGrowth")
                ),
                "earnings_growth": _clean_growth_percent(
                    financial_data.get("earningsGrowth") or key_stats.get("earningsQuarterlyGrowth")
                ),
                "return_on_equity": _clean_ratio(financial_data.get("returnOnEquity")),
                "market_cap": _clean_number(summary.get("marketCap") or key_stats.get("marketCap")),
                "shares_outstanding": _clean_number(
                    key_stats.get("sharesOutstanding") or summary.get("sharesOutstanding")
                ),
            }
        )
    except Exception as exc:
        logger.debug("metadata_yahooquery_failed symbol=%s provider=%s error=%s", symbol, provider_symbol, str(exc)[:120])
        return record, _is_definitive_not_found(str(exc))
    return record, False


def _load_fdr_module() -> Any | None:
    try:
        import FinanceDataReader as fdr  # type: ignore
    except Exception:
        return None
    return fdr


def _pick_listing_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    lookup = {str(column).strip().lower(): column for column in frame.columns}
    for alias in aliases:
        column = lookup.get(str(alias).strip().lower())
        if column is not None:
            return column
    return None


def _normalize_kr_exchange(value: Any, fallback: str = "") -> str:
    text = _clean_text(value).upper()
    if text in {"KOSPI", "STK"}:
        return "KOSPI"
    if text in {"KOSDAQ", "KSQ"}:
        return "KOSDAQ"
    if "ETF" in text:
        return "ETF"
    if "ETN" in text:
        return "ETN"
    fallback_text = _clean_text(fallback).upper()
    if fallback_text:
        if "ETF" in fallback_text:
            return "ETF"
        if "ETN" in fallback_text:
            return "ETN"
        if "KOSDAQ" in fallback_text:
            return "KOSDAQ"
        if "KOSPI" in fallback_text or fallback_text == "KRX":
            return "KOSPI"
    return text or fallback_text


def _records_from_fdr_listing(frame: pd.DataFrame, *, listing_name: str) -> dict[str, dict[str, object]]:
    if frame is None or frame.empty:
        return {}

    symbol_column = _pick_listing_column(frame, ("Symbol", "Code", "symbol", "code"))
    if symbol_column is None:
        return {}

    exchange_column = _pick_listing_column(frame, ("Market", "market", "Exchange", "exchange"))
    sector_column = _pick_listing_column(frame, ("Sector", "sector", "업종"))
    industry_column = _pick_listing_column(frame, ("Industry", "industry", "Dept", "dept"))
    market_cap_column = _pick_listing_column(frame, ("Marcap", "MarketCap", "market_cap", "marcap"))
    shares_column = _pick_listing_column(frame, ("Stocks", "stocks", "Shares", "shares_outstanding", "ListedShares"))

    records: dict[str, dict[str, object]] = {}
    for _, row in frame.iterrows():
        symbol = _clean_text(row.get(symbol_column)).upper()
        if not symbol.isdigit() or len(symbol) != 6:
            continue
        record = _blank_record(symbol, "kr", provider_symbol=symbol)
        record.update(
            {
                "exchange": _normalize_kr_exchange(row.get(exchange_column) if exchange_column else "", fallback=listing_name),
                "sector": _clean_text(row.get(sector_column)) if sector_column else "",
                "industry": _clean_text(row.get(industry_column)) if industry_column else "",
                "market_cap": _clean_number(row.get(market_cap_column)) if market_cap_column else None,
                "shares_outstanding": _clean_number(row.get(shares_column)) if shares_column else None,
            }
        )
        records[symbol] = record
    return records


def _prefetch_kr_listing_metadata(symbols: List[str]) -> dict[str, dict[str, object]]:
    requested = {str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()}
    if not requested:
        return {}

    fdr_module = _load_fdr_module()
    if fdr_module is None:
        return {}

    listing_names = ("KRX", "KOSPI", "KOSDAQ", "ETF/KR")
    listing_records: dict[str, dict[str, object]] = {}
    for listing_name in listing_names:
        try:
            frame = fdr_module.StockListing(listing_name)
        except Exception as exc:
            logger.debug("metadata_fdr_listing_failed listing=%s error=%s", listing_name, str(exc)[:160])
            continue
        parsed = _records_from_fdr_listing(frame, listing_name=listing_name)
        for symbol, record in parsed.items():
            if symbol not in requested:
                continue
            listing_records[symbol] = _merge_records(listing_records.get(symbol, _blank_record(symbol, "kr")), record)
    return listing_records


def fetch_metadata(
    symbol: str,
    *,
    market: str = "us",
    max_retries: int = YAHOO_FINANCE_MAX_RETRIES,
    delay: float = YAHOO_FINANCE_DELAY,
) -> dict[str, object]:
    symbol_key = str(symbol or "").strip().upper()
    normalized_market = market_key(market)
    retries = max(1, int(max_retries))
    request_delay = max(float(delay), METADATA_MIN_REQUEST_DELAY_SECONDS)
    phase_label = f"{normalized_market.upper()} metadata"
    best_record = _mark_record(_blank_record(symbol_key, normalized_market), status="failed")

    for provider_symbol in iter_provider_symbols(symbol_key, normalized_market):
        provider_best = _mark_record(
            _blank_record(symbol_key, normalized_market, provider_symbol=provider_symbol),
            status="failed",
        )

        for attempt in range(retries):
            attempted_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            candidate = _mark_record(
                _blank_record(symbol_key, normalized_market, provider_symbol=provider_symbol),
                status="failed",
                attempted_at=attempted_at,
            )
            definitive_missing = False

            try:
                wait_for_yahoo_request_slot(phase_label, min_interval=request_delay)
                info, fast_info, yfinance_missing, yfinance_rate_limited = _fetch_yfinance_info_quietly(provider_symbol)
                candidate = _merge_records(
                    candidate,
                    _record_from_yfinance(symbol_key, normalized_market, provider_symbol, info, fast_info),
                )
                candidate = _mark_record(candidate, source="yfinance", attempted_at=attempted_at)
                definitive_missing = definitive_missing or yfinance_missing

                if yfinance_rate_limited:
                    provider_best = _merge_records(provider_best, candidate)
                    status = "partial_fast_info" if _has_meaningful_metadata(provider_best) else "rate_limited"
                    provider_best = _mark_record(provider_best, status=status, source="yfinance", attempted_at=attempted_at)
                    cooldown = METADATA_RATE_LIMIT_COOLDOWN_SECONDS
                    extend_yahoo_cooldown(phase_label, cooldown)
                    if attempt < retries - 1:
                        continue
                    break

                if _is_yfinance_candidate_sufficient(candidate):
                    return _mark_record(candidate, status="complete", source="yfinance", attempted_at=attempted_at)
            except Exception as exc:
                logger.debug(
                    "metadata_yfinance_failed symbol=%s provider=%s error=%s",
                    symbol_key,
                    provider_symbol,
                    str(exc)[:120],
                )
                definitive_missing = definitive_missing or _is_definitive_not_found(str(exc))

            yahooquery_record, yahooquery_missing = fetch_metadata_yahooquery(
                symbol_key,
                provider_symbol,
                market=normalized_market,
                delay=request_delay,
            )
            candidate = _merge_records(candidate, yahooquery_record)
            candidate = _mark_record(candidate, source="yahooquery", attempted_at=attempted_at)
            definitive_missing = definitive_missing or yahooquery_missing

            if _is_metadata_complete(candidate):
                return _mark_record(candidate, status="complete", attempted_at=attempted_at)

            provider_best = _merge_records(provider_best, candidate)
            if definitive_missing:
                provider_best = _mark_record(provider_best, status="not_found", attempted_at=attempted_at)
                break
            if _has_meaningful_metadata(provider_best):
                provider_best = _mark_record(provider_best, status="partial_fast_info", attempted_at=attempted_at)
            else:
                provider_best = _mark_record(provider_best, status="failed", attempted_at=attempted_at)

        best_record = _merge_records(best_record, provider_best)
        best_status = _clean_text(best_record.get("fetch_status")).lower()
        if _is_metadata_complete(best_record) and best_status not in {"partial_fast_info", "rate_limited"}:
            return _mark_record(best_record, status="complete")

    final_status = _clean_text(best_record.get("fetch_status")).lower()
    if _is_metadata_complete(best_record) and final_status not in {"partial_fast_info", "rate_limited"}:
        return _mark_record(best_record, status="complete")
    if _has_meaningful_metadata(best_record):
        return _mark_record(best_record, status=final_status or "partial_fast_info")
    return _mark_record(best_record, status=_clean_text(best_record.get("fetch_status")) or "failed")


def collect_stock_metadata(
    symbols: List[str],
    *,
    market: str = "us",
    max_workers: int = METADATA_MAX_WORKERS,
    max_retries: int = YAHOO_FINANCE_MAX_RETRIES,
    delay: float = YAHOO_FINANCE_DELAY,
) -> pd.DataFrame:
    normalized_market = market_key(market)
    records: list[dict[str, object]] = []
    requested_symbols = [str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()]
    if not requested_symbols:
        return pd.DataFrame(columns=METADATA_COLUMNS)

    prefilled_records: dict[str, dict[str, object]] = {}
    symbols_to_fetch = requested_symbols
    if normalized_market == "kr":
        prefilled_records = _prefetch_kr_listing_metadata(requested_symbols)
        if prefilled_records:
            normalized_prefill: dict[str, dict[str, object]] = {}
            for symbol in requested_symbols:
                prefilled = prefilled_records.get(symbol)
                if not prefilled:
                    continue
                status = "complete" if _is_metadata_complete(prefilled) else "partial_fast_info"
                normalized_prefill[symbol] = _mark_record(
                    prefilled,
                    status=status,
                    source="fdr_listing_prefill",
                    attempted_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                )
            prefilled_records = normalized_prefill
            records.extend(prefilled_records.values())
            symbols_to_fetch = [
                symbol for symbol in requested_symbols
                if not _has_meaningful_metadata(prefilled_records.get(symbol, {}))
            ]
            _emit_progress(
                f"[Metadata] KR listing prefill ({normalized_market}) - "
                f"prefilled={sum(1 for symbol in requested_symbols if _has_meaningful_metadata(prefilled_records.get(symbol, {})))}, "
                f"remaining={len(symbols_to_fetch)}"
            )

    total = len(symbols_to_fetch)
    if total == 0:
        return _normalize_metadata_frame(pd.DataFrame(records), normalized_market)

    completed = 0
    successful = sum(1 for record in records if _has_meaningful_metadata(record))
    started_at = time.time()
    progress_interval = 25 if total <= 1000 else 50 if total <= 5000 else 100

    _emit_progress(
        f"[Metadata] Fetch started ({normalized_market}) - total={total}, "
        f"workers={max(1, min(int(max_workers), max(1, total)))}"
    )
    with ThreadPoolExecutor(max_workers=max(1, min(int(max_workers), max(1, total)))) as executor:
        future_to_symbol = {
            executor.submit(
                fetch_metadata,
                symbol,
                market=normalized_market,
                max_retries=max_retries,
                delay=delay,
            ): symbol
            for symbol in symbols_to_fetch
        }
        pending = set(future_to_symbol.keys())
        while pending:
            done, pending = wait(
                pending,
                timeout=METADATA_PROGRESS_HEARTBEAT_SECONDS,
                return_when=FIRST_COMPLETED,
            )

            if not done:
                elapsed = time.time() - started_at
                _emit_progress(
                    f"[Metadata] Heartbeat ({normalized_market}) - "
                    f"completed={completed}/{total}, success={successful}, pending={len(pending)}, "
                    f"elapsed={elapsed:.1f}s, eta={_format_eta(completed, total, elapsed)}"
                )
                continue

            for future in done:
                symbol = future_to_symbol[future]
                completed += 1
                try:
                    record = future.result()
                except Exception as exc:
                    logger.error("metadata collection failed: market=%s symbol=%s error=%s", normalized_market, symbol, exc)
                    record = _blank_record(symbol, normalized_market)
                else:
                    symbol = str(record.get("symbol") or "").strip().upper() or symbol

                records.append(record)
                if _has_meaningful_metadata(record):
                    successful += 1
                if completed in {1, 5, 10} or completed % progress_interval == 0 or completed == total:
                    elapsed = time.time() - started_at
                    _emit_progress(
                        f"[Metadata] Progress ({normalized_market}) - "
                        f"completed={completed}/{total}, success={successful}, pending={len(pending)}, "
                        f"elapsed={elapsed:.1f}s, eta={_format_eta(completed, total, elapsed)}"
                    )

    return _normalize_metadata_frame(pd.DataFrame(records), normalized_market)


def _list_us_symbols() -> list[str]:
    return load_us_symbol_universe(
        data_dir=DATA_DIR,
        us_data_dir=get_market_data_dir("us"),
        stock_metadata_path=get_stock_metadata_path("us"),
        progress=_emit_progress,
    )


def _list_non_index_symbols(market: str, *, stock_module=None) -> list[str]:
    normalized_market = market_key(market)
    return load_kr_symbol_universe(
        data_dir=get_market_data_dir(normalized_market),
        stock_metadata_path=get_stock_metadata_path(normalized_market),
        include_kosdaq=True,
        include_etf=True,
        include_etn=True,
        stock_module=stock_module,
    )


def get_symbols(market: str = "us", *, stock_module=None) -> List[str]:
    normalized_market = market_key(market)
    if normalized_market == "us":
        return _list_us_symbols()
    return _list_non_index_symbols(normalized_market, stock_module=stock_module)


def load_cached_metadata(
    market: str = "us",
    max_age_days: int = 7,
    *,
    allow_stale: bool = True,
) -> Optional[pd.DataFrame]:
    metadata_path = get_stock_metadata_path(market)
    if not os.path.exists(metadata_path):
        return None

    try:
        raw_frame = pd.read_csv(metadata_path)
        required_columns = {"earnings_growth", "return_on_equity", "fetch_status", "source", "last_attempted_at"}
        if not required_columns.issubset(set(raw_frame.columns)):
            logger.info(
                "metadata cache schema outdated: market=%s path=%s missing=%s",
                market_key(market),
                metadata_path,
                ",".join(sorted(required_columns.difference(set(raw_frame.columns)))),
            )
            return None

        file_age = time.time() - os.path.getmtime(metadata_path)
        if file_age > max_age_days * 24 * 3600:
            if not allow_stale:
                logger.info("metadata cache expired: market=%s path=%s", market_key(market), metadata_path)
                return None
            logger.info("metadata cache stale but reused: market=%s path=%s", market_key(market), metadata_path)

        return _normalize_metadata_frame(raw_frame, market)
    except Exception as exc:
        logger.warning("metadata cache load failed: market=%s path=%s error=%s", market_key(market), metadata_path, exc)
        return None


def get_missing_symbols(cached_df: Optional[pd.DataFrame], all_symbols: List[str]) -> List[str]:
    _ = cached_df
    return list(all_symbols)


def merge_metadata(cached_df: Optional[pd.DataFrame], new_df: pd.DataFrame, *, market: str) -> pd.DataFrame:
    normalized_market = market_key(market)
    if (cached_df is None or cached_df.empty) and (new_df is None or new_df.empty):
        return pd.DataFrame(columns=METADATA_COLUMNS)
    if cached_df is None or cached_df.empty:
        return _normalize_metadata_frame(new_df, normalized_market)
    if new_df is None or new_df.empty:
        return _normalize_metadata_frame(cached_df, normalized_market)

    merged_by_symbol: dict[str, dict[str, object]] = {
        symbol: record
        for symbol, record in frame_keyed_records(
            cached_df,
            key_column="symbol",
            uppercase_keys=True,
        ).items()
    }

    for _, row in new_df.iterrows():
        symbol_key = str(row.get("symbol") or "").strip().upper()
        if not symbol_key:
            continue
        update_record = row_to_record(row)
        base_record = merged_by_symbol.get(symbol_key)
        if not base_record:
            merged_by_symbol[symbol_key] = update_record
            continue

        merged_record = _merge_records(base_record, update_record)
        if _should_preserve_cached_metadata(base_record, update_record):
            preserved_status = _clean_text(base_record.get("fetch_status")).lower()
            if preserved_status:
                merged_record["fetch_status"] = preserved_status
        merged_by_symbol[symbol_key] = merged_record

    return _normalize_metadata_frame(pd.DataFrame(list(merged_by_symbol.values())), normalized_market)


def main(*, market: str = "us") -> pd.DataFrame:
    bootstrap_windows_utf8()
    bootstrap_yfinance_cache()

    normalized_market = market_key(market)
    metadata_path = get_stock_metadata_path(normalized_market)
    ensure_dir(os.path.dirname(metadata_path))

    _emit_progress(f"[Metadata] Resolving symbol universe ({normalized_market})")
    symbols = get_symbols(normalized_market)
    if not symbols:
        raise RuntimeError(f"Metadata symbol universe is empty ({normalized_market})")

    cached_df = load_cached_metadata(normalized_market, allow_stale=True)
    cached_count = 0 if cached_df is None or cached_df.empty else len(cached_df)
    missing_symbols = get_missing_symbols(cached_df, symbols)

    if os.path.exists(metadata_path):
        file_age_days = (time.time() - os.path.getmtime(metadata_path)) / (24 * 3600)
        freshness_label = "stale-reused" if file_age_days > 7 else "fresh"
        _emit_progress(
            f"[Metadata] Cache status ({normalized_market}) - rows={cached_count}, age_days={file_age_days:.1f}, {freshness_label}"
        )
    else:
        _emit_progress(f"[Metadata] Cache status ({normalized_market}) - no local cache")

    _emit_progress(
        f"[Metadata] Target summary ({normalized_market}) - "
        f"total={len(symbols)}, cached={cached_count}, missing={len(missing_symbols)}"
    )

    if not missing_symbols:
        _emit_progress(f"[Metadata] Already up to date ({normalized_market})")
        return cached_df if cached_df is not None else pd.DataFrame(columns=METADATA_COLUMNS)

    final_df = cached_df if cached_df is not None else pd.DataFrame(columns=METADATA_COLUMNS)
    batches = _iter_batches(missing_symbols, METADATA_BATCH_SIZE)
    processed = 0
    for batch_index, batch in enumerate(batches, start=1):
        _emit_progress(
            f"[Metadata] Batch {batch_index}/{len(batches)} ({normalized_market}) - "
            f"size={len(batch)}, processed={processed}/{len(missing_symbols)}"
        )
        new_df = collect_stock_metadata(batch, market=normalized_market)
        final_df = merge_metadata(final_df, new_df, market=normalized_market)
        final_df.to_csv(metadata_path, index=False)
        processed += len(batch)
        _emit_progress(
            f"[Metadata] Checkpoint saved ({normalized_market}) - "
            f"processed={processed}/{len(missing_symbols)}, total_rows={len(final_df)}, path={metadata_path}"
        )
        if batch_index < len(batches):
            _emit_progress(
                f"[Throttle] Metadata batch pause ({normalized_market}) - wait={METADATA_BATCH_PAUSE_SECONDS:.1f}s"
            )
            time.sleep(METADATA_BATCH_PAUSE_SECONDS)

    _emit_progress(
        f"[Metadata] Saved ({normalized_market}) - total={len(final_df)}, path={metadata_path}"
    )
    return final_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market metadata collector")
    parser.add_argument("--market", default="us", help="Target market (us|kr)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main(market=args.market)
