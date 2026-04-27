#!/usr/bin/env python3
"""Collect market metadata for US/KR symbols with KR reference prefill and Yahoo fallback."""

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
from config import YAHOO_FINANCE_MAX_RETRIES
from data_collectors.kr_reference_sources import (
    fetch_kr_reference_metadata,
    listing_records_from_fdr,
    load_fdr_module as load_kr_fdr_module,
    normalize_kr_exchange as normalize_reference_kr_exchange,
    pick_listing_column as pick_reference_listing_column,
)
from data_collectors.symbol_universe import load_kr_symbol_universe, load_us_symbol_universe
from utils.console_runtime import bootstrap_windows_utf8
from utils.collector_diagnostics import CollectorDiagnostics, attach_collector_diagnostics
from utils.io_utils import ensure_dir
from utils.market_runtime import (
    get_market_data_dir,
    get_stock_metadata_path,
    iter_provider_symbols,
    limit_runtime_symbols,
    market_key,
)
from utils.security_profile import (
    CANONICAL_METADATA_COLUMNS,
    derive_kr_share_class_fields,
    enrich_metadata_record,
)
from utils.symbol_normalization import normalize_symbol_value
from utils.typing_utils import frame_keyed_records, is_na_like, row_to_record
from utils.yfinance_runtime import bootstrap_yfinance_cache
from utils.yahoo_throttle import (
    extend_yahoo_cooldown,
    get_yahoo_throttle_state,
    record_yahoo_request_success,
    reset_yahoo_throttle_state,
    wait_for_yahoo_request_slot,
)


logger = logging.getLogger(__name__)


def _env_int_default(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return int(default)


def _env_float_default(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return float(default)


def _env_bool_default(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


METADATA_BATCH_SIZE = max(1, _env_int_default("INVEST_PROTO_METADATA_BATCH_SIZE", 200))
METADATA_MAX_WORKERS = max(1, _env_int_default("INVEST_PROTO_METADATA_MAX_WORKERS", 3))
METADATA_PROGRESS_HEARTBEAT_SECONDS = 15.0
METADATA_RATE_LIMIT_COOLDOWN_SECONDS = 45.0
METADATA_RATE_LIMIT_DELAY_BACKOFF_SECONDS = max(
    0.0,
    _env_float_default("INVEST_PROTO_METADATA_RATE_LIMIT_DELAY_BACKOFF_SECONDS", 0.12),
)
METADATA_MIN_REQUEST_DELAY_SECONDS = max(
    0.1,
    _env_float_default("INVEST_PROTO_METADATA_REQUEST_DELAY_SECONDS", 1.0),
)
METADATA_BATCH_PAUSE_SECONDS = max(
    0.0,
    _env_float_default("INVEST_PROTO_METADATA_BATCH_PAUSE_SECONDS", 0.0),
)
METADATA_RATE_LIMIT_PROBE_CANDIDATES: tuple[tuple[int, float], ...] = tuple(
    dict.fromkeys(
        (
            (METADATA_MAX_WORKERS, METADATA_MIN_REQUEST_DELAY_SECONDS),
            (3, METADATA_MIN_REQUEST_DELAY_SECONDS),
            (2, METADATA_MIN_REQUEST_DELAY_SECONDS),
            (1, METADATA_MIN_REQUEST_DELAY_SECONDS),
        )
    )
)
METADATA_RATE_LIMIT_PROBE_FALLBACK_PROFILE = {
    "workers": 1,
    "interval": METADATA_MIN_REQUEST_DELAY_SECONDS,
}
METADATA_KR_REFERENCE_IDENTITY_COMPLETE = _env_bool_default(
    "INVEST_PROTO_METADATA_KR_REFERENCE_IDENTITY_COMPLETE",
    True,
)
METADATA_COMPLETE_MAX_AGE_DAYS = max(1, _env_int_default("INVEST_PROTO_METADATA_COMPLETE_MAX_AGE_DAYS", 30))
METADATA_NOT_FOUND_MAX_AGE_DAYS = max(1, _env_int_default("INVEST_PROTO_METADATA_NOT_FOUND_MAX_AGE_DAYS", 30))
METADATA_RETRYABLE_MAX_AGE_DAYS = max(1, _env_int_default("INVEST_PROTO_METADATA_RETRYABLE_MAX_AGE_DAYS", 7))
METADATA_FRESHNESS_WINDOW_DAYS = METADATA_RETRYABLE_MAX_AGE_DAYS

METADATA_COLUMNS: tuple[str, ...] = CANONICAL_METADATA_COLUMNS
METADATA_REFRESH_COUNT_KEYS: tuple[str, ...] = (
    "cached_fresh",
    "stale_complete",
    "retryable",
    "not_found_cached",
    "missing",
    "to_fetch",
)
_RETRYABLE_METADATA_STATUSES = {"partial_fast_info", "failed", "pending", "rate_limited", ""}

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


def _metadata_max_workers() -> int:
    return max(1, _env_int_default("INVEST_PROTO_METADATA_MAX_WORKERS", METADATA_MAX_WORKERS))


def _metadata_worker_count(max_workers: int | None, total: int) -> int:
    configured = _metadata_max_workers() if max_workers is None else int(max_workers)
    return max(1, min(max(1, configured), max(1, int(total))))


def _metadata_request_delay(delay: float | None = None) -> float:
    configured = max(
        0.1,
        _env_float_default("INVEST_PROTO_METADATA_REQUEST_DELAY_SECONDS", METADATA_MIN_REQUEST_DELAY_SECONDS),
    )
    if delay is None:
        return configured
    return max(float(delay), configured)


def _metadata_rate_limit_backoff_delay(current_delay: float) -> float:
    return max(
        _metadata_request_delay(None),
        float(current_delay) + METADATA_RATE_LIMIT_DELAY_BACKOFF_SECONDS,
    )


def _metadata_rate_limit_worker_count(initial_workers: int, fallback_events: int) -> int:
    configured_workers = max(1, int(initial_workers))
    events = max(0, int(fallback_events))
    if configured_workers >= 3:
        if events <= 3:
            return 3
        if events == 4:
            return 2
        return 1
    if configured_workers == 2:
        return 1
    return 1


def _blank_record(symbol: str, market: str, provider_symbol: str | None = None) -> dict[str, object]:
    normalized_market = market_key(market)
    return {
        "symbol": str(symbol or "").strip().upper(),
        "market": normalized_market,
        "provider_symbol": provider_symbol,
        "name": "",
        "quote_type": "",
        "security_type": "",
        "fund_family": "",
        "exchange": "",
        "sector": "",
        "industry": "",
        "pe_ratio": None,
        "revenue_growth": None,
        "earnings_growth": None,
        "return_on_equity": None,
        "market_cap": None,
        "shares_outstanding": None,
        "issuer_symbol": "",
        "share_class_type": "",
        "earnings_anchor_symbol": "",
        "earnings_expected": None,
        "earnings_skip_reason": "",
        "fundamentals_expected": None,
        "provider_trace": "",
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


def _clean_bool_flag(value: Any) -> bool | None:
    if value is None or is_na_like(value):
        return None
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


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
    if not _has_identity_metadata(record):
        return False
    fundamentals_expected = _clean_bool_flag(record.get("fundamentals_expected"))
    if fundamentals_expected is False:
        return True
    return _has_core_metric_metadata(record)


def _is_kr_reference_complete(record: dict[str, object]) -> bool:
    if _is_metadata_complete(record):
        return True
    if not METADATA_KR_REFERENCE_IDENTITY_COMPLETE:
        return False
    source = _clean_text(record.get("source")).lower()
    if "fdr_listing" not in source and "financedatabase" not in source:
        return False
    if not _has_identity_metadata(record):
        return False
    return bool(
        _clean_text(record.get("provider_symbol"))
        or _clean_text(record.get("name"))
        or _clean_text(record.get("exchange"))
    )


def _is_yfinance_candidate_sufficient(record: dict[str, object]) -> bool:
    fundamentals_expected = _clean_bool_flag(record.get("fundamentals_expected"))
    if fundamentals_expected is False:
        return _has_identity_metadata(record)
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
    if _is_metadata_complete(record):
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
        if key in {
            "provider_symbol",
            "name",
            "quote_type",
            "security_type",
            "fund_family",
            "exchange",
            "sector",
            "industry",
            "earnings_skip_reason",
            "provider_trace",
            "fetch_status",
            "last_attempted_at",
        }:
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
            normalized[column] = None if column not in {
                "symbol",
                "market",
                "provider_symbol",
                "name",
                "quote_type",
                "security_type",
                "fund_family",
                "exchange",
                "sector",
                "industry",
                "issuer_symbol",
                "share_class_type",
                "earnings_anchor_symbol",
                "earnings_skip_reason",
                "provider_trace",
                "fetch_status",
                "source",
                "last_attempted_at",
            } else ""

    normalized["symbol"] = normalized["symbol"].map(_clean_text).str.upper()
    normalized["market"] = normalized["market"].map(_clean_text).str.lower()
    normalized.loc[normalized["market"] == "", "market"] = normalized_market

    for text_column in (
        "provider_symbol",
        "name",
        "quote_type",
        "security_type",
        "fund_family",
        "exchange",
        "sector",
        "industry",
        "issuer_symbol",
        "share_class_type",
        "earnings_anchor_symbol",
        "earnings_skip_reason",
        "provider_trace",
        "fetch_status",
        "source",
        "last_attempted_at",
    ):
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

    raw_records_by_symbol = {
        normalize_symbol_value(row.get("symbol"), normalized_market): row_to_record(row)
        for _, row in normalized.iterrows()
        if _clean_text(row.get("symbol"))
    }
    records = [
        enrich_metadata_record(
            row_to_record(row),
            market=normalized_market,
            records_by_symbol=raw_records_by_symbol,
        )
        for _, row in normalized.iterrows()
        if _clean_text(row.get("symbol"))
    ]
    if normalized_market == "kr":
        enriched_by_symbol = {record["symbol"]: dict(record) for record in records if _clean_text(record.get("symbol"))}
        for symbol, record in list(enriched_by_symbol.items()):
            record.update(derive_kr_share_class_fields(record, enriched_by_symbol))
            enriched_by_symbol[symbol] = record
        records = list(enriched_by_symbol.values())
    normalized = pd.DataFrame(records, columns=METADATA_COLUMNS)
    normalized = normalized[normalized["symbol"] != ""]
    normalized = normalized.drop_duplicates(subset=["symbol"], keep="last").sort_values("symbol").reset_index(drop=True)
    return normalized


def _read_metadata_csv(metadata_path: str) -> pd.DataFrame:
    return pd.read_csv(
        metadata_path,
        dtype={
            "symbol": "string",
            "provider_symbol": "string",
            "issuer_symbol": "string",
            "earnings_anchor_symbol": "string",
        },
        encoding="utf-8-sig",
    )


def _write_metadata_csv(frame: pd.DataFrame, metadata_path: str) -> None:
    frame.to_csv(metadata_path, index=False, encoding="utf-8-sig")


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
            "name": _clean_text(info.get("longName") or info.get("shortName")),
            "quote_type": _clean_text(info.get("quoteType")).upper(),
            "security_type": _clean_text(info.get("quoteType")).upper(),
            "fund_family": _clean_text(info.get("fundFamily")),
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
    return enrich_metadata_record(record, market=market)


def fetch_metadata_yahooquery(
    symbol: str,
    provider_symbol: str,
    *,
    market: str,
    delay: float | None = None,
) -> tuple[dict[str, object], bool, bool]:
    record = _blank_record(symbol, market, provider_symbol=provider_symbol)
    try:
        ticker = Ticker(provider_symbol)
        wait_for_yahoo_request_slot(
            f"{market_key(market).upper()} metadata",
            min_interval=_metadata_request_delay(delay),
        )

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
        message = str(exc)
        return record, _is_definitive_not_found(message), _is_rate_limited_message(message)
    if _has_meaningful_metadata(record):
        record_yahoo_request_success(f"{market_key(market).upper()} metadata")
    return record, False, False


def _load_fdr_module() -> Any | None:
    return load_kr_fdr_module(required=True)


def _pick_listing_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    return pick_reference_listing_column(frame, aliases)


def _normalize_kr_exchange(value: Any, fallback: str = "") -> str:
    return normalize_reference_kr_exchange(value, fallback)


def _records_from_fdr_listing(frame: pd.DataFrame, *, listing_name: str) -> dict[str, dict[str, object]]:
    records = listing_records_from_fdr(frame, listing_name=listing_name)
    return {
        symbol: enrich_metadata_record(_merge_records(_blank_record(symbol, "kr"), record), market="kr")
        for symbol, record in records.items()
    }

    if frame is None or frame.empty:
        return {}

    symbol_column = _pick_listing_column(frame, ("Symbol", "Code", "symbol", "code"))
    if symbol_column is None:
        return {}

    exchange_column = _pick_listing_column(frame, ("Market", "market", "Exchange", "exchange"))
    name_column = _pick_listing_column(frame, ("Name", "name", "Company", "company"))
    sector_column = _pick_listing_column(frame, ("Sector", "sector", "업종"))
    industry_column = _pick_listing_column(frame, ("Industry", "industry", "Dept", "dept"))
    market_cap_column = _pick_listing_column(frame, ("Marcap", "MarketCap", "market_cap", "marcap"))
    shares_column = _pick_listing_column(frame, ("Stocks", "stocks", "Shares", "shares_outstanding", "ListedShares"))

    records: dict[str, dict[str, object]] = {}
    for _, row in frame.iterrows():
        symbol = _clean_text(row.get(symbol_column)).upper()
        if not symbol.isdigit() or len(symbol) != 6:
            continue
        exchange = _normalize_kr_exchange(
            row.get(exchange_column) if exchange_column else "",
            fallback=listing_name,
        )
        provider_suffix = ".KQ" if exchange == "KOSDAQ" else ".KS"
        security_type = "ETF" if exchange == "ETF" else "ETN" if exchange == "ETN" else "COMMON_STOCK"
        record = _blank_record(symbol, "kr", provider_symbol=f"{symbol}{provider_suffix}")
        record.update(
            {
                "name": _clean_text(row.get(name_column)) if name_column else "",
                "exchange": exchange,
                "security_type": security_type,
                "sector": _clean_text(row.get(sector_column)) if sector_column else "",
                "industry": _clean_text(row.get(industry_column)) if industry_column else "",
                "market_cap": _clean_number(row.get(market_cap_column)) if market_cap_column else None,
                "shares_outstanding": _clean_number(row.get(shares_column)) if shares_column else None,
            }
        )
        records[symbol] = enrich_metadata_record(record, market="kr")
    return records


def _prefetch_kr_reference_metadata(symbols: List[str]) -> dict[str, dict[str, object]]:
    requested = [str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()]
    if not requested:
        return {}
    try:
        records = fetch_kr_reference_metadata(requested)
    except RuntimeError:
        raise
    except Exception as exc:
        logger.debug("metadata_kr_reference_prefetch_failed error=%s", str(exc)[:160])
        return {}
    return {
        symbol: enrich_metadata_record(_merge_records(_blank_record(symbol, "kr"), record), market="kr")
        for symbol, record in records.items()
    }

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


def _normalize_kr_prefill_records(
    requested_symbols: List[str],
    prefilled_records: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    normalized_prefill: dict[str, dict[str, object]] = {}
    attempted_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for symbol in requested_symbols:
        symbol_key = str(symbol or "").strip().upper()
        prefilled = prefilled_records.get(symbol_key)
        if not prefilled:
            continue
        status = "complete" if _is_kr_reference_complete(prefilled) else "partial_fast_info"
        normalized_prefill[symbol_key] = _mark_record(
            prefilled,
            status=status,
            source=_combine_source_labels(prefilled.get("source"), None),
            attempted_at=attempted_at,
        )
    return normalized_prefill


def fetch_metadata(
    symbol: str,
    *,
    market: str = "us",
    max_retries: int = YAHOO_FINANCE_MAX_RETRIES,
    delay: float | None = None,
) -> dict[str, object]:
    symbol_key = str(symbol or "").strip().upper()
    normalized_market = market_key(market)
    retries = max(1, int(max_retries))
    request_delay = _metadata_request_delay(delay)
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
                    record_yahoo_request_success(phase_label)
                    return _mark_record(candidate, status="complete", source="yfinance", attempted_at=attempted_at)
            except Exception as exc:
                logger.debug(
                    "metadata_yfinance_failed symbol=%s provider=%s error=%s",
                    symbol_key,
                    provider_symbol,
                    str(exc)[:120],
                )
                message = str(exc)
                if _is_rate_limited_message(message):
                    provider_best = _merge_records(provider_best, candidate)
                    status = "partial_fast_info" if _has_meaningful_metadata(provider_best) else "rate_limited"
                    provider_best = _mark_record(provider_best, status=status, source="yfinance", attempted_at=attempted_at)
                    extend_yahoo_cooldown(phase_label, METADATA_RATE_LIMIT_COOLDOWN_SECONDS)
                    if attempt < retries - 1:
                        continue
                    break
                definitive_missing = definitive_missing or _is_definitive_not_found(message)

            yahooquery_result = fetch_metadata_yahooquery(
                symbol_key,
                provider_symbol,
                market=normalized_market,
                delay=request_delay,
            )
            if len(yahooquery_result) == 2:
                yahooquery_record, yahooquery_missing = yahooquery_result
                yahooquery_rate_limited = False
            else:
                yahooquery_record, yahooquery_missing, yahooquery_rate_limited = yahooquery_result
            candidate = _merge_records(candidate, yahooquery_record)
            candidate = _mark_record(candidate, source="yahooquery", attempted_at=attempted_at)
            definitive_missing = definitive_missing or yahooquery_missing

            if yahooquery_rate_limited:
                provider_best = _merge_records(provider_best, candidate)
                status = "partial_fast_info" if _has_meaningful_metadata(provider_best) else "rate_limited"
                provider_best = _mark_record(provider_best, status=status, source="yahooquery", attempted_at=attempted_at)
                extend_yahoo_cooldown(phase_label, METADATA_RATE_LIMIT_COOLDOWN_SECONDS)
                if attempt < retries - 1:
                    continue
                break

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
    max_workers: int | None = None,
    max_retries: int = YAHOO_FINANCE_MAX_RETRIES,
    delay: float | None = None,
    prefilled_records: Optional[dict[str, dict[str, object]]] = None,
) -> pd.DataFrame:
    diagnostics = CollectorDiagnostics()
    diagnostics.increment("provider_fetch_symbols", 0)
    normalized_market = market_key(market)
    records: list[dict[str, object]] = []
    requested_symbols = [str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()]
    if not requested_symbols:
        return attach_collector_diagnostics(pd.DataFrame(columns=METADATA_COLUMNS), diagnostics)

    prefill_provided = prefilled_records is not None
    resolved_prefilled_records: dict[str, dict[str, object]] = dict(prefilled_records or {})
    symbols_to_fetch = requested_symbols
    if normalized_market == "kr":
        if not prefill_provided:
            with diagnostics.time_block("symbol_prepare_seconds"):
                resolved_prefilled_records = _prefetch_kr_reference_metadata(requested_symbols)
        if resolved_prefilled_records:
            resolved_prefilled_records = _normalize_kr_prefill_records(requested_symbols, resolved_prefilled_records)
            records.extend(resolved_prefilled_records.values())
            symbols_to_fetch = [
                symbol for symbol in requested_symbols
                if not _is_kr_reference_complete(resolved_prefilled_records.get(symbol, {}))
            ]
            diagnostics.increment(
                "kr_reference_prefill_symbols",
                sum(1 for symbol in requested_symbols if _has_meaningful_metadata(resolved_prefilled_records.get(symbol, {}))),
            )
            diagnostics.increment(
                "kr_reference_complete",
                sum(1 for symbol in requested_symbols if _is_kr_reference_complete(resolved_prefilled_records.get(symbol, {}))),
            )
            _emit_progress(
                f"[Metadata] KR reference prefill ({normalized_market}) - "
                f"prefilled={sum(1 for symbol in requested_symbols if _has_meaningful_metadata(resolved_prefilled_records.get(symbol, {})))}, "
                f"remaining={len(symbols_to_fetch)}"
            )

    total = len(symbols_to_fetch)
    if total == 0:
        return attach_collector_diagnostics(
            _normalize_metadata_frame(pd.DataFrame(records), normalized_market),
            diagnostics,
        )
    diagnostics.increment("provider_fetch_symbols", total)

    records_by_symbol: dict[str, dict[str, object]] = {
        symbol: dict(record)
        for symbol, record in resolved_prefilled_records.items()
    }
    completed = 0
    successful = sum(1 for record in records if _has_meaningful_metadata(record))
    started_at = time.time()
    progress_interval = 25 if total <= 1000 else 50 if total <= 5000 else 100
    worker_count = _metadata_worker_count(max_workers, total)

    _emit_progress(
        f"[Metadata] Fetch started ({normalized_market}) - total={total}, "
        f"workers={worker_count}"
    )
    with diagnostics.time_block("provider_fetch_seconds"):
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
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
                    diagnostics.increment("heartbeats")
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

                    base_record = records_by_symbol.get(symbol)
                    merged_record = _merge_records(base_record, record) if base_record else record
                    records_by_symbol[symbol] = merged_record
                    records.append(merged_record)
                    if _has_meaningful_metadata(record):
                        successful += 1
                    if completed in {1, 5, 10} or completed % progress_interval == 0 or completed == total:
                        elapsed = time.time() - started_at
                        _emit_progress(
                            f"[Metadata] Progress ({normalized_market}) - "
                            f"completed={completed}/{total}, success={successful}, pending={len(pending)}, "
                            f"elapsed={elapsed:.1f}s, eta={_format_eta(completed, total, elapsed)}"
                        )

    if records_by_symbol:
        return attach_collector_diagnostics(
            _normalize_metadata_frame(pd.DataFrame(records_by_symbol.values()), normalized_market),
            diagnostics,
        )
    return attach_collector_diagnostics(
        _normalize_metadata_frame(pd.DataFrame(records), normalized_market),
        diagnostics,
    )


def _list_us_symbols() -> list[str]:
    return load_us_symbol_universe(
        data_dir=DATA_DIR,
        us_data_dir=get_market_data_dir("us"),
        stock_metadata_path=get_stock_metadata_path("us"),
        progress=_emit_progress,
    )


def _list_non_index_symbols(market: str, *, fdr_module=None) -> list[str]:
    normalized_market = market_key(market)
    return load_kr_symbol_universe(
        data_dir=get_market_data_dir(normalized_market),
        stock_metadata_path=get_stock_metadata_path(normalized_market),
        include_kosdaq=True,
        include_etf=True,
        include_etn=True,
        fdr_module=fdr_module,
    )


def get_symbols(market: str = "us", *, fdr_module=None) -> List[str]:
    normalized_market = market_key(market)
    if normalized_market == "us":
        return _list_us_symbols()
    return _list_non_index_symbols(normalized_market, fdr_module=fdr_module)


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
        raw_frame = _read_metadata_csv(metadata_path)
        required_columns = {
            "symbol",
            "market",
            "earnings_growth",
            "return_on_equity",
            "fetch_status",
            "source",
            "last_attempted_at",
        }
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


def _attempted_at_is_stale(
    value: object,
    *,
    max_age_days: int,
    now_ts: float | None = None,
) -> bool:
    if max_age_days <= 0:
        return False
    text = _clean_text(value)
    if not text:
        return True
    try:
        attempted_ts = pd.Timestamp(text, tz="UTC")
    except Exception:
        return True
    if pd.isna(attempted_ts):
        return True
    if attempted_ts.tzinfo is None:
        attempted_ts = attempted_ts.tz_localize("UTC")
    current_ts = (
        pd.Timestamp.fromtimestamp(float(now_ts), tz="UTC")
        if now_ts is not None
        else pd.Timestamp.now(tz="UTC")
    )
    if current_ts.tzinfo is None:
        current_ts = current_ts.tz_localize("UTC")
    age_seconds = max((current_ts - attempted_ts).total_seconds(), 0.0)
    return age_seconds > float(max_age_days * 24 * 3600)


def _new_refresh_counts() -> dict[str, int]:
    return {key: 0 for key in METADATA_REFRESH_COUNT_KEYS}


def _format_refresh_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{key}={int(counts.get(key, 0) or 0)}" for key in METADATA_REFRESH_COUNT_KEYS)


def _metadata_rate_limit_count(frame: pd.DataFrame | None) -> int:
    if frame is None or frame.empty or "fetch_status" not in frame.columns:
        return 0
    statuses = frame["fetch_status"].map(_clean_text).str.lower()
    return int(statuses.eq("rate_limited").sum())


def _metadata_rate_limited_symbols(frame: pd.DataFrame | None) -> list[str]:
    if frame is None or frame.empty or "fetch_status" not in frame.columns or "symbol" not in frame.columns:
        return []
    symbols: list[str] = []
    for _, row in frame.iterrows():
        if _clean_text(row.get("fetch_status")).lower() != "rate_limited":
            continue
        symbol = _clean_text(row.get("symbol")).upper()
        if symbol:
            symbols.append(symbol)
    return symbols


def _metadata_status_counts(frame: pd.DataFrame | None) -> dict[str, int]:
    counts = {"complete": 0, "partial": 0, "failed": 0, "rate_limited": 0, "not_found": 0}
    if frame is None or frame.empty or "fetch_status" not in frame.columns:
        return counts
    statuses = frame["fetch_status"].map(_clean_text).str.lower()
    counts["complete"] = int(statuses.eq("complete").sum())
    counts["partial"] = int(statuses.str.startswith("partial").sum())
    counts["failed"] = int(statuses.isin({"failed", "pending", ""}).sum())
    counts["rate_limited"] = int(statuses.eq("rate_limited").sum())
    counts["not_found"] = int(statuses.eq("not_found").sum())
    return counts


def _metadata_throttle_rate_limit_count(snapshot: dict[str, object]) -> int:
    raw_counts = snapshot.get("rate_limit_count", {})
    if not isinstance(raw_counts, dict):
        return 0
    total = 0
    for value in raw_counts.values():
        try:
            total += int(value)
        except (TypeError, ValueError):
            continue
    return total


def _metadata_market_throttle_rate_limit_count(snapshot: dict[str, object], market: str) -> int:
    raw_counts = snapshot.get("rate_limit_count", {})
    if not isinstance(raw_counts, dict):
        return 0
    source = f"{market_key(market).upper()} metadata"
    if source in raw_counts:
        try:
            return int(raw_counts.get(source) or 0)
        except (TypeError, ValueError):
            return 0
    return _metadata_throttle_rate_limit_count(snapshot)


def _profile_dict(workers: int, interval: float) -> dict[str, float | int]:
    return {"workers": int(workers), "interval": float(interval)}


def _metadata_probe_symbols(market: str, probe_count: int) -> list[str]:
    normalized_market = market_key(market)
    requested_count = max(1, int(probe_count))
    clean_symbols: list[str] = []

    cached_df = load_cached_metadata(normalized_market, allow_stale=True)
    if cached_df is not None and not cached_df.empty:
        for _, row in cached_df.iterrows():
            record = row_to_record(row)
            symbol = normalize_symbol_value(record.get("symbol"), normalized_market)
            if not symbol or symbol in clean_symbols:
                continue
            status = _clean_text(record.get("fetch_status")).lower()
            if status != "complete":
                continue
            if not _is_metadata_complete(record):
                continue
            clean_symbols.append(symbol)
            if len(clean_symbols) >= requested_count:
                return clean_symbols

    if clean_symbols:
        return clean_symbols[:requested_count]

    excluded: set[str] = set()
    if cached_df is not None and not cached_df.empty:
        for _, row in cached_df.iterrows():
            record = row_to_record(row)
            status = _clean_text(record.get("fetch_status")).lower()
            if status in {"not_found", "failed", "rate_limited"}:
                symbol = normalize_symbol_value(record.get("symbol"), normalized_market)
                if symbol:
                    excluded.add(symbol)

    for symbol in limit_runtime_symbols(get_symbols(normalized_market)):
        symbol_key = normalize_symbol_value(symbol, normalized_market)
        if not symbol_key or symbol_key in excluded or symbol_key in clean_symbols:
            continue
        clean_symbols.append(symbol_key)
        if len(clean_symbols) >= requested_count:
            break
    return clean_symbols


def _rate_limit_probe_failed(
    *,
    status_counts: dict[str, int],
    throttle_snapshot: dict[str, object],
    captured_output: str,
    error_message: str,
) -> bool:
    if error_message:
        return True
    if int(status_counts.get("rate_limited", 0) or 0) > 0:
        return True
    if _metadata_throttle_rate_limit_count(throttle_snapshot) > 0:
        return True
    try:
        if float(throttle_snapshot.get("cooldown_in", 0.0) or 0.0) > 0.0:
            return True
    except (TypeError, ValueError):
        return True
    return _is_rate_limited_message(captured_output) or _is_rate_limited_message(error_message)


def run_metadata_rate_limit_probe(
    *,
    market: str = "us",
    probe_count: int = 100,
    candidate_profiles: tuple[tuple[int, float], ...] | None = None,
    run_canary: bool = True,
    probe_max_retries: int = 1,
) -> dict[str, object]:
    bootstrap_windows_utf8()
    bootstrap_yfinance_cache()

    normalized_market = market_key(market)
    requested_count = max(1, int(probe_count))
    symbols = _metadata_probe_symbols(normalized_market, requested_count)
    if not symbols:
        raise RuntimeError(f"Metadata probe symbol universe is empty ({normalized_market})")

    profiles = tuple(candidate_profiles or METADATA_RATE_LIMIT_PROBE_CANDIDATES)
    recommended_profile: dict[str, float | int] | None = None
    recommended_elapsed: float | None = None
    results: list[dict[str, object]] = []
    canary_result: dict[str, object] | None = None
    _emit_progress(
        f"[Metadata Probe] Starting ({normalized_market}) - "
        f"symbols={len(symbols)}, candidates={len(profiles)}"
    )

    if run_canary:
        reset_yahoo_throttle_state()
        started_at = time.time()
        captured = io.StringIO()
        frame: pd.DataFrame | None = None
        error_message = ""
        canary_delay = max(_metadata_request_delay(None) * 2.0, 1.5)
        try:
            with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
                frame = collect_stock_metadata(
                    [symbols[0]],
                    market=normalized_market,
                    max_workers=1,
                    max_retries=max(1, int(probe_max_retries)),
                    delay=canary_delay,
                )
        except Exception as exc:
            error_message = str(exc)
        throttle_snapshot = dict(get_yahoo_throttle_state())
        status_counts = _metadata_status_counts(frame)
        captured_output = captured.getvalue()
        blocked = _rate_limit_probe_failed(
            status_counts=status_counts,
            throttle_snapshot=throttle_snapshot,
            captured_output=captured_output,
            error_message=error_message,
        )
        canary_result = {
            "workers": 1,
            "interval": canary_delay,
            "elapsed": time.time() - started_at,
            "status_counts": status_counts,
            "throttle": throttle_snapshot,
            "ok": not blocked,
            "error": error_message,
        }
        if blocked:
            recommended_profile = dict(METADATA_RATE_LIMIT_PROBE_FALLBACK_PROFILE)
            _emit_progress(
                f"[Metadata Probe] Canary blocked ({normalized_market}) - "
                f"symbol={symbols[0]}, interval={canary_delay:.2f}, "
                f"rate_limited={status_counts['rate_limited']}, "
                f"throttle_rate_limited={_metadata_throttle_rate_limit_count(throttle_snapshot)}, "
                f"cooldown_in={float(throttle_snapshot.get('cooldown_in', 0.0) or 0.0):.1f}s"
            )
            _emit_progress(
                f"[Metadata Probe] recommended_profile=workers={int(recommended_profile['workers'])}, "
                f"interval={float(recommended_profile['interval']):.2f}"
            )
            return {
                "market": normalized_market,
                "probe_count": len(symbols),
                "symbols": symbols,
                "recommended_profile": recommended_profile,
                "results": results,
                "canary": canary_result,
                "probe_blocked": True,
            }

    for index, (workers, interval) in enumerate(profiles, start=1):
        reset_yahoo_throttle_state()
        started_at = time.time()
        captured = io.StringIO()
        frame: pd.DataFrame | None = None
        error_message = ""
        try:
            with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
                frame = collect_stock_metadata(
                    symbols,
                    market=normalized_market,
                    max_workers=int(workers),
                    max_retries=max(1, int(probe_max_retries)),
                    delay=float(interval),
                )
        except Exception as exc:
            error_message = str(exc)

        elapsed = time.time() - started_at
        throttle_snapshot = dict(get_yahoo_throttle_state())
        status_counts = _metadata_status_counts(frame)
        captured_output = captured.getvalue()
        ok = not _rate_limit_probe_failed(
            status_counts=status_counts,
            throttle_snapshot=throttle_snapshot,
            captured_output=captured_output,
            error_message=error_message,
        )
        result = {
            "workers": int(workers),
            "interval": float(interval),
            "elapsed": elapsed,
            "status_counts": status_counts,
            "throttle": throttle_snapshot,
            "ok": ok,
            "error": error_message,
        }
        results.append(result)

        throttle_rate_limited = _metadata_throttle_rate_limit_count(throttle_snapshot)
        cooldown_in = float(throttle_snapshot.get("cooldown_in", 0.0) or 0.0)
        _emit_progress(
            f"[Metadata Probe] Candidate {index}/{len(profiles)} ({normalized_market}) - "
            f"workers={int(workers)}, interval={float(interval):.2f}, elapsed={elapsed:.1f}s, "
            f"complete={status_counts['complete']}, partial={status_counts['partial']}, "
            f"failed={status_counts['failed']}, rate_limited={status_counts['rate_limited']}, "
            f"throttle_rate_limited={throttle_rate_limited}, cooldown_in={cooldown_in:.1f}s, ok={ok}, "
            f"throttle={throttle_snapshot}"
        )

        if not ok:
            break
        if recommended_elapsed is None or elapsed < recommended_elapsed:
            recommended_profile = _profile_dict(int(workers), float(interval))
            recommended_elapsed = elapsed

    if recommended_profile is None:
        recommended_profile = dict(METADATA_RATE_LIMIT_PROBE_FALLBACK_PROFILE)

    _emit_progress(
        f"[Metadata Probe] recommended_profile=workers={int(recommended_profile['workers'])}, "
        f"interval={float(recommended_profile['interval']):.2f}"
    )
    return {
        "market": normalized_market,
        "probe_count": len(symbols),
        "symbols": symbols,
        "recommended_profile": recommended_profile,
        "results": results,
        "canary": canary_result,
        "probe_blocked": False,
    }


def _classify_metadata_refresh_targets(
    cached_df: Optional[pd.DataFrame],
    all_symbols: List[str],
    *,
    now_ts: float | None = None,
    complete_max_age_days: int = METADATA_COMPLETE_MAX_AGE_DAYS,
    not_found_max_age_days: int = METADATA_NOT_FOUND_MAX_AGE_DAYS,
    retryable_max_age_days: int = METADATA_RETRYABLE_MAX_AGE_DAYS,
) -> tuple[List[str], dict[str, int]]:
    normalized_symbols = [str(symbol).strip().upper() for symbol in all_symbols if str(symbol).strip()]
    counts = _new_refresh_counts()
    if cached_df is None or cached_df.empty:
        counts["missing"] = len(normalized_symbols)
        counts["to_fetch"] = len(normalized_symbols)
        return normalized_symbols, counts

    cached_records = frame_keyed_records(
        cached_df,
        key_column="symbol",
        uppercase_keys=True,
        drop_na=True,
    )
    targets: list[str] = []
    for symbol in normalized_symbols:
        record = cached_records.get(symbol)
        if not record:
            counts["missing"] += 1
            targets.append(symbol)
            continue

        status = _clean_text(record.get("fetch_status")).lower()
        if status == "not_found":
            if _attempted_at_is_stale(
                record.get("last_attempted_at"),
                max_age_days=not_found_max_age_days,
                now_ts=now_ts,
            ):
                counts["retryable"] += 1
                targets.append(symbol)
            else:
                counts["not_found_cached"] += 1
            continue

        if status == "complete":
            if _attempted_at_is_stale(
                record.get("last_attempted_at"),
                max_age_days=complete_max_age_days,
                now_ts=now_ts,
            ):
                counts["stale_complete"] += 1
                targets.append(symbol)
            else:
                counts["cached_fresh"] += 1
            continue

        if status in _RETRYABLE_METADATA_STATUSES or _is_retryable_metadata_record(record):
            if _attempted_at_is_stale(
                record.get("last_attempted_at"),
                max_age_days=retryable_max_age_days,
                now_ts=now_ts,
            ):
                counts["retryable"] += 1
                targets.append(symbol)
            else:
                counts["cached_fresh"] += 1
            continue

        if _is_metadata_complete(record):
            if _attempted_at_is_stale(
                record.get("last_attempted_at"),
                max_age_days=complete_max_age_days,
                now_ts=now_ts,
            ):
                counts["stale_complete"] += 1
                targets.append(symbol)
            else:
                counts["cached_fresh"] += 1
            continue

        if _attempted_at_is_stale(
            record.get("last_attempted_at"),
            max_age_days=retryable_max_age_days,
            now_ts=now_ts,
        ):
            counts["retryable"] += 1
            targets.append(symbol)
        else:
            counts["cached_fresh"] += 1

    counts["to_fetch"] = len(targets)
    return targets, counts


def get_missing_symbols(
    cached_df: Optional[pd.DataFrame],
    all_symbols: List[str],
    *,
    max_age_days: int | None = None,
    now_ts: float | None = None,
) -> List[str]:
    legacy_max_age_days = None if max_age_days is None else max(1, int(max_age_days))
    targets, _counts = _classify_metadata_refresh_targets(
        cached_df,
        all_symbols,
        now_ts=now_ts,
        complete_max_age_days=METADATA_COMPLETE_MAX_AGE_DAYS if legacy_max_age_days is None else legacy_max_age_days,
        not_found_max_age_days=METADATA_NOT_FOUND_MAX_AGE_DAYS if legacy_max_age_days is None else legacy_max_age_days,
        retryable_max_age_days=METADATA_RETRYABLE_MAX_AGE_DAYS if legacy_max_age_days is None else legacy_max_age_days,
    )
    return targets


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
    diagnostics = CollectorDiagnostics()
    bootstrap_windows_utf8()
    bootstrap_yfinance_cache()

    normalized_market = market_key(market)
    metadata_path = get_stock_metadata_path(normalized_market)
    ensure_dir(os.path.dirname(metadata_path))

    _emit_progress(f"[Metadata] Resolving symbol universe ({normalized_market})")
    symbols = get_symbols(normalized_market)
    symbols = limit_runtime_symbols(symbols)
    if not symbols:
        raise RuntimeError(f"Metadata symbol universe is empty ({normalized_market})")

    cached_df = load_cached_metadata(normalized_market, allow_stale=True)
    cached_count = 0 if cached_df is None or cached_df.empty else len(cached_df)
    missing_symbols, refresh_counts = _classify_metadata_refresh_targets(cached_df, symbols)

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
        f"total={len(symbols)}, cached={cached_count}, missing={len(missing_symbols)}, "
        f"refresh_counts={_format_refresh_counts(refresh_counts)}"
    )

    if not missing_symbols:
        _emit_progress(f"[Metadata] Already up to date ({normalized_market})")
        return attach_collector_diagnostics(
            cached_df if cached_df is not None else pd.DataFrame(columns=METADATA_COLUMNS),
            diagnostics,
        )

    final_df = cached_df if cached_df is not None else pd.DataFrame(columns=METADATA_COLUMNS)
    main_prefilled_records: dict[str, dict[str, object]] = {}
    kr_reference_prefetch_attempted = False
    if normalized_market == "kr" and missing_symbols:
        kr_reference_prefetch_attempted = True
        with diagnostics.time_block("symbol_prepare_seconds"):
            main_prefilled_records = _normalize_kr_prefill_records(
                missing_symbols,
                _prefetch_kr_reference_metadata(missing_symbols),
            )
        if main_prefilled_records:
            complete_records = {
                symbol: record
                for symbol, record in main_prefilled_records.items()
                if _is_kr_reference_complete(record)
            }
            diagnostics.increment(
                "kr_reference_prefill_symbols",
                sum(1 for symbol in missing_symbols if _has_meaningful_metadata(main_prefilled_records.get(symbol, {}))),
            )
            diagnostics.increment("kr_reference_complete", len(complete_records))
            if complete_records:
                reference_df = _normalize_metadata_frame(pd.DataFrame(complete_records.values()), normalized_market)
                final_df = merge_metadata(final_df, reference_df, market=normalized_market)
                _write_metadata_csv(final_df, metadata_path)
                _emit_progress(
                    f"[Metadata] KR reference prefill checkpoint ({normalized_market}) - "
                    f"complete={len(complete_records)}, total_rows={len(final_df)}, path={metadata_path}"
                )
            missing_symbols = [
                symbol
                for symbol in missing_symbols
                if not _is_kr_reference_complete(main_prefilled_records.get(symbol, {}))
            ]

    if not missing_symbols:
        _emit_progress(f"[Metadata] Saved ({normalized_market}) - total={len(final_df)}, path={metadata_path}")
        return attach_collector_diagnostics(final_df, diagnostics)

    batches = _iter_batches(missing_symbols, METADATA_BATCH_SIZE)
    processed = 0
    initial_batch_max_workers = _metadata_max_workers()
    batch_max_workers = initial_batch_max_workers
    batch_request_delay = _metadata_request_delay(None)
    metadata_rate_limit_fallback_events = 0
    for batch_index, batch in enumerate(batches, start=1):
        _emit_progress(
            f"[Metadata] Batch {batch_index}/{len(batches)} ({normalized_market}) - "
            f"size={len(batch)}, processed={processed}/{len(missing_symbols)}"
        )
        batch_prefill = {
            symbol: main_prefilled_records[symbol]
            for symbol in batch
            if symbol in main_prefilled_records
        }
        throttle_before = _metadata_market_throttle_rate_limit_count(
            dict(get_yahoo_throttle_state()),
            normalized_market,
        )
        new_df = collect_stock_metadata(
            batch,
            market=normalized_market,
            max_workers=batch_max_workers,
            delay=batch_request_delay,
            prefilled_records=(
                batch_prefill
                if batch_prefill or (normalized_market == "kr" and kr_reference_prefetch_attempted)
                else None
            ),
        )
        throttle_after = _metadata_market_throttle_rate_limit_count(
            dict(get_yahoo_throttle_state()),
            normalized_market,
        )
        batch_rate_limit_events = max(0, throttle_after - throttle_before)
        batch_rate_limited = _metadata_rate_limit_count(new_df)
        if batch_rate_limited > 0:
            retry_symbols = _metadata_rate_limited_symbols(new_df)
            metadata_rate_limit_fallback_events += 1
            batch_max_workers = _metadata_rate_limit_worker_count(
                initial_batch_max_workers,
                metadata_rate_limit_fallback_events,
            )
            batch_request_delay = _metadata_rate_limit_backoff_delay(batch_request_delay)
            _emit_progress(
                f"[Throttle] Metadata throttle fallback ({normalized_market}) - "
                f"rate_limited={batch_rate_limited}, workers={batch_max_workers}, "
                f"delay={batch_request_delay:.2f}s"
            )
            if retry_symbols:
                _emit_progress(
                    f"[Throttle] Metadata rate-limit retry ({normalized_market}) - "
                    f"symbols={len(retry_symbols)}, wait={METADATA_RATE_LIMIT_COOLDOWN_SECONDS:.1f}s"
                )
                time.sleep(METADATA_RATE_LIMIT_COOLDOWN_SECONDS)
                retry_prefill = {
                    symbol: main_prefilled_records[symbol]
                    for symbol in retry_symbols
                    if symbol in main_prefilled_records
                }
                retry_df = collect_stock_metadata(
                    retry_symbols,
                    market=normalized_market,
                    max_workers=batch_max_workers,
                    delay=batch_request_delay,
                    prefilled_records=(
                        retry_prefill
                        if retry_prefill or (normalized_market == "kr" and kr_reference_prefetch_attempted)
                        else None
                    ),
                )
                diagnostics.merge_from(
                    timings=retry_df.attrs.get("timings") if isinstance(retry_df.attrs, dict) else None,
                    diagnostics=retry_df.attrs.get("collector_diagnostics") if isinstance(retry_df.attrs, dict) else None,
                )
                new_df = merge_metadata(new_df, retry_df, market=normalized_market)
        elif batch_rate_limit_events > 0:
            metadata_rate_limit_fallback_events += 1
            batch_max_workers = _metadata_rate_limit_worker_count(
                initial_batch_max_workers,
                metadata_rate_limit_fallback_events,
            )
            batch_request_delay = _metadata_rate_limit_backoff_delay(batch_request_delay)
            _emit_progress(
                f"[Throttle] Metadata throttle fallback ({normalized_market}) - "
                f"rate_limit_events={batch_rate_limit_events}, workers={batch_max_workers}, "
                f"delay={batch_request_delay:.2f}s"
            )
        batch_provider_fetch_symbols = len(batch)
        if isinstance(new_df.attrs, dict):
            raw_diagnostics = new_df.attrs.get("collector_diagnostics")
            if isinstance(raw_diagnostics, dict):
                raw_counts = raw_diagnostics.get("counts")
                if isinstance(raw_counts, dict) and "provider_fetch_symbols" in raw_counts:
                    batch_provider_fetch_symbols = int(raw_counts.get("provider_fetch_symbols") or 0)
        diagnostics.merge_from(
            timings=new_df.attrs.get("timings") if isinstance(new_df.attrs, dict) else None,
            diagnostics=new_df.attrs.get("collector_diagnostics") if isinstance(new_df.attrs, dict) else None,
        )
        final_df = merge_metadata(final_df, new_df, market=normalized_market)
        _write_metadata_csv(final_df, metadata_path)
        processed += len(batch)
        _emit_progress(
            f"[Metadata] Checkpoint saved ({normalized_market}) - "
            f"processed={processed}/{len(missing_symbols)}, total_rows={len(final_df)}, path={metadata_path}"
        )
        if batch_index < len(batches) and batch_provider_fetch_symbols > 0 and METADATA_BATCH_PAUSE_SECONDS > 0:
            _emit_progress(
                f"[Throttle] Metadata batch pause ({normalized_market}) - wait={METADATA_BATCH_PAUSE_SECONDS:.1f}s"
            )
            time.sleep(METADATA_BATCH_PAUSE_SECONDS)
        elif batch_index < len(batches) and batch_provider_fetch_symbols > 0:
            _emit_progress(
                f"[Throttle] Metadata batch pause skipped ({normalized_market}) - throttle handles request pacing"
            )
        elif batch_index < len(batches):
            _emit_progress(
                f"[Throttle] Metadata batch pause skipped ({normalized_market}) - no provider fetch symbols"
            )

    _emit_progress(
        f"[Metadata] Saved ({normalized_market}) - total={len(final_df)}, path={metadata_path}"
    )
    return attach_collector_diagnostics(final_df, diagnostics)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market metadata collector")
    parser.add_argument("--market", default="us", help="Target market (us|kr)")
    parser.add_argument(
        "--probe-rate-limit",
        action="store_true",
        help="Run non-persistent Yahoo metadata throughput probe instead of writing metadata CSV",
    )
    parser.add_argument(
        "--probe-count",
        type=int,
        default=100,
        help="Number of symbols to use for --probe-rate-limit",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    if args.probe_rate_limit:
        run_metadata_rate_limit_probe(market=args.market, probe_count=args.probe_count)
    else:
        main(market=args.market)
