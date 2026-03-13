#!/usr/bin/env python3
"""Collect market metadata for US/KR symbols from Yahoo providers."""

from __future__ import annotations

import argparse
import contextlib
import io
import logging
import os
import time
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
from utils.yfinance_runtime import bootstrap_yfinance_cache


logger = logging.getLogger(__name__)

METADATA_BATCH_SIZE = 250
METADATA_PROGRESS_HEARTBEAT_SECONDS = 15.0

METADATA_COLUMNS: tuple[str, ...] = (
    "symbol",
    "market",
    "provider_symbol",
    "exchange",
    "sector",
    "industry",
    "pe_ratio",
    "revenue_growth",
    "market_cap",
    "shares_outstanding",
)

_MEANINGFUL_KEYS: tuple[str, ...] = (
    "exchange",
    "sector",
    "industry",
    "pe_ratio",
    "revenue_growth",
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
        "market_cap": None,
        "shares_outstanding": None,
    }


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "null"}:
        return ""
    return text


def _clean_number(value: Any) -> float | int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    try:
        numeric = float(value)
    except Exception:
        return None

    if pd.isna(numeric):
        return None
    if numeric.is_integer():
        return int(numeric)
    return numeric


def _has_meaningful_metadata(record: dict[str, object]) -> bool:
    for key in _MEANINGFUL_KEYS:
        value = record.get(key)
        if isinstance(value, str):
            if value.strip():
                return True
            continue
        if value is not None:
            return True
    return False


def _merge_records(base: dict[str, object], update: dict[str, object]) -> dict[str, object]:
    merged = dict(base)
    for key in METADATA_COLUMNS:
        value = update.get(key)
        if key in {"symbol", "market"}:
            if value:
                merged[key] = value
            continue
        if key in {"exchange", "sector", "industry"}:
            if _clean_text(value):
                merged[key] = _clean_text(value)
            continue
        if key == "provider_symbol":
            if _clean_text(value):
                merged[key] = _clean_text(value)
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
            normalized[column] = None if column not in {"symbol", "market", "provider_symbol", "exchange", "sector", "industry"} else ""

    normalized["symbol"] = normalized["symbol"].map(_clean_text).str.upper()
    normalized["market"] = normalized["market"].map(_clean_text).str.lower()
    normalized.loc[normalized["market"] == "", "market"] = normalized_market

    for text_column in ("provider_symbol", "exchange", "sector", "industry"):
        normalized[text_column] = normalized[text_column].map(_clean_text)

    for numeric_column in ("pe_ratio", "revenue_growth", "market_cap", "shares_outstanding"):
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


def _fetch_yfinance_info_quietly(provider_symbol: str) -> tuple[dict[str, Any], bool]:
    sink = io.StringIO()
    try:
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            ticker = yf.Ticker(provider_symbol)
            info = ticker.info or {}
        noisy_output = sink.getvalue()
        return info if isinstance(info, dict) else {}, _is_definitive_not_found(noisy_output)
    except Exception as exc:
        noisy_output = sink.getvalue()
        return {}, _is_definitive_not_found(f"{exc}\n{noisy_output}")


def _record_from_yfinance(symbol: str, market: str, provider_symbol: str, info: dict[str, Any]) -> dict[str, object]:
    record = _blank_record(symbol, market, provider_symbol=provider_symbol)
    record.update(
        {
            "exchange": _clean_text(info.get("exchange") or info.get("fullExchangeName")),
            "sector": _clean_text(info.get("sector")),
            "industry": _clean_text(info.get("industry")),
            "pe_ratio": _clean_number(info.get("trailingPE")),
            "revenue_growth": _clean_number(info.get("revenueGrowth")),
            "market_cap": _clean_number(info.get("marketCap")),
            "shares_outstanding": _clean_number(info.get("sharesOutstanding")),
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
        if delay > 0:
            time.sleep(delay)

        summary = _pick_provider_payload(ticker.summary_detail, provider_symbol)
        key_stats = _pick_provider_payload(ticker.key_stats, provider_symbol)
        profile = _pick_provider_payload(ticker.summary_profile, provider_symbol)

        record.update(
            {
                "exchange": _clean_text(profile.get("exchange") or summary.get("exchange") or summary.get("fullExchangeName")),
                "sector": _clean_text(profile.get("sector")),
                "industry": _clean_text(profile.get("industry")),
                "pe_ratio": _clean_number(summary.get("trailingPE")),
                "revenue_growth": _clean_number(key_stats.get("revenueQuarterlyGrowth")),
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
    best_record = _blank_record(symbol_key, normalized_market)

    for provider_symbol in iter_provider_symbols(symbol_key, normalized_market):
        provider_best = _blank_record(symbol_key, normalized_market, provider_symbol=provider_symbol)

        for attempt in range(retries):
            candidate = _blank_record(symbol_key, normalized_market, provider_symbol=provider_symbol)
            definitive_missing = False

            try:
                if delay > 0:
                    time.sleep(delay)
                info, yfinance_missing = _fetch_yfinance_info_quietly(provider_symbol)
                candidate = _merge_records(candidate, _record_from_yfinance(symbol_key, normalized_market, provider_symbol, info))
                definitive_missing = definitive_missing or yfinance_missing
            except Exception as exc:
                logger.debug("metadata_yfinance_failed symbol=%s provider=%s error=%s", symbol_key, provider_symbol, str(exc)[:120])
                definitive_missing = definitive_missing or _is_definitive_not_found(str(exc))

            yahooquery_record, yahooquery_missing = fetch_metadata_yahooquery(
                symbol_key,
                provider_symbol,
                market=normalized_market,
                delay=max(1.0, float(delay)),
            )
            candidate = _merge_records(candidate, yahooquery_record)
            definitive_missing = definitive_missing or yahooquery_missing

            if _has_meaningful_metadata(candidate):
                return candidate

            provider_best = _merge_records(provider_best, candidate)
            if definitive_missing:
                break
            if attempt < (retries - 1) and delay > 0:
                time.sleep(delay)

        best_record = _merge_records(best_record, provider_best)
        if _has_meaningful_metadata(best_record):
            return best_record

    return best_record


def collect_stock_metadata(
    symbols: List[str],
    *,
    market: str = "us",
    max_workers: int = 8,
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
        file_age = time.time() - os.path.getmtime(metadata_path)
        if file_age > max_age_days * 24 * 3600:
            if not allow_stale:
                logger.info("metadata cache expired: market=%s path=%s", market_key(market), metadata_path)
                return None
            logger.info("metadata cache stale but reused: market=%s path=%s", market_key(market), metadata_path)

        return _normalize_metadata_frame(pd.read_csv(metadata_path), market)
    except Exception as exc:
        logger.warning("metadata cache load failed: market=%s path=%s error=%s", market_key(market), metadata_path, exc)
        return None


def get_missing_symbols(cached_df: Optional[pd.DataFrame], all_symbols: List[str]) -> List[str]:
    if cached_df is None or cached_df.empty:
        return all_symbols
    symbol_lookup = {
        str(row.get("symbol") or "").strip().upper(): row.to_dict()
        for _, row in cached_df.iterrows()
    }
    missing: list[str] = []
    for symbol in all_symbols:
        record = symbol_lookup.get(str(symbol or "").strip().upper())
        if not record or not _has_meaningful_metadata(record):
            missing.append(symbol)
    return missing


def merge_metadata(cached_df: Optional[pd.DataFrame], new_df: pd.DataFrame, *, market: str) -> pd.DataFrame:
    normalized_market = market_key(market)
    if cached_df is None or cached_df.empty:
        return _normalize_metadata_frame(new_df, normalized_market)

    new_symbols = set(new_df["symbol"].astype(str).str.upper().tolist()) if not new_df.empty else set()
    filtered_cached = cached_df[~cached_df["symbol"].astype(str).str.upper().isin(new_symbols)]
    frames = [frame for frame in (filtered_cached, new_df) if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=METADATA_COLUMNS)
    return _normalize_metadata_frame(pd.concat(frames, ignore_index=True), normalized_market)


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
