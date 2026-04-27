from __future__ import annotations

import csv
import os
import re
from functools import lru_cache
from typing import Any, Mapping

import pandas as pd

from utils.market_runtime import get_stock_metadata_path, market_key
from utils.symbol_normalization import normalize_provider_symbol_value, normalize_symbol_value


CANONICAL_METADATA_COLUMNS: tuple[str, ...] = (
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
    "pe_ratio",
    "revenue_growth",
    "earnings_growth",
    "return_on_equity",
    "market_cap",
    "shares_outstanding",
    "issuer_symbol",
    "share_class_type",
    "earnings_anchor_symbol",
    "earnings_expected",
    "earnings_skip_reason",
    "fundamentals_expected",
    "provider_trace",
    "fetch_status",
    "source",
    "last_attempted_at",
)


_TEXT_COLUMNS = {
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
}
_NUMERIC_COLUMNS = {
    "pe_ratio",
    "revenue_growth",
    "earnings_growth",
    "return_on_equity",
    "market_cap",
    "shares_outstanding",
}
_KR_PREFERRED_SUFFIX_PATTERN = re.compile(r"(?:\d+)?우(?:[A-Z]+)?$")


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "null"}:
        return ""
    return text


def _clean_number(value: Any) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if pd.isna(numeric):
        return None
    if numeric.is_integer():
        return int(numeric)
    return numeric


def _clean_bool(value: Any) -> bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _kr_provider_symbol(symbol: str, exchange: str, provider_symbol: str) -> str:
    normalized = normalize_provider_symbol_value(provider_symbol)
    if normalized:
        return normalized
    if not symbol.isdigit() or len(symbol) != 6:
        return symbol
    exchange_key = _clean_text(exchange).upper()
    suffix = ".KQ" if exchange_key == "KOSDAQ" else ".KS"
    return f"{symbol}{suffix}"


def _infer_security_type(
    *,
    symbol: str,
    market: str,
    existing_security_type: str,
    quote_type: str,
    exchange: str,
    name: str,
    fund_family: str,
    official_hint: Mapping[str, Any] | None,
) -> str:
    if existing_security_type:
        return existing_security_type

    quote_type_key = quote_type.upper()
    exchange_key = exchange.upper()
    name_key = name.upper()

    if bool(official_hint and official_hint.get("is_etf")):
        return "ETF"
    if "ETF" in {quote_type_key, exchange_key} or " ETF" in name_key or name_key.endswith("ETF"):
        return "ETF"
    if "ETN" in {quote_type_key, exchange_key} or " ETN" in name_key or name_key.endswith("ETN"):
        return "ETN"
    if quote_type_key in {"INDEX", "INDX"}:
        return "INDEX"
    if quote_type_key in {"MUTUALFUND", "MUTUAL_FUND"} or fund_family:
        return "FUND"
    if "TRUST" in name_key and quote_type_key not in {"EQUITY", "COMMON_STOCK"}:
        return "TRUST"
    if market == "kr" and symbol.isdigit() and len(symbol) == 6 and exchange_key not in {"ETF", "ETN"}:
        return "COMMON_STOCK"
    if quote_type_key in {"EQUITY", "COMMON_STOCK"}:
        if "ADR" in name_key:
            return "ADR"
        return "COMMON_STOCK"
    if "ADR" in name_key:
        return "ADR"
    return ""


def _skip_reason(fetch_status: str, security_type: str) -> str:
    status_key = fetch_status.lower()
    security_key = security_type.upper()

    if status_key == "not_found":
        return "metadata_not_found"
    if security_key == "ETF":
        return "etf"
    if security_key == "ETN":
        return "etn"
    if security_key == "FUND":
        return "fund"
    if security_key == "TRUST":
        return "trust"
    if security_key == "INDEX":
        return "index"
    return ""


def _derive_kr_share_class_type(
    *,
    name: str,
    security_type: str,
    existing_share_class_type: str,
) -> str:
    existing = _clean_text(existing_share_class_type).upper()
    if existing:
        return existing
    security_key = _clean_text(security_type).upper()
    if security_key not in {"COMMON_STOCK", "ADR"}:
        return ""
    cleaned_name = _clean_text(name)
    if not cleaned_name:
        return "COMMON"
    return "PREFERRED" if _KR_PREFERRED_SUFFIX_PATTERN.search(cleaned_name) else "COMMON"


def _derive_kr_base_name(name: str) -> str:
    cleaned_name = _clean_text(name)
    if not cleaned_name:
        return ""
    return _KR_PREFERRED_SUFFIX_PATTERN.sub("", cleaned_name).strip()


def _is_kr_common_candidate(record: Mapping[str, Any] | None) -> bool:
    source = dict(record or {})
    security_type = _clean_text(source.get("security_type")).upper()
    exchange = _clean_text(source.get("exchange")).upper()
    return security_type == "COMMON_STOCK" and exchange not in {"ETF", "ETN"}


def derive_kr_share_class_fields(
    record: Mapping[str, Any] | None,
    records_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, str]:
    source = dict(record or {})
    symbol = normalize_symbol_value(source.get("symbol"), "kr")
    security_type = _clean_text(source.get("security_type")).upper()
    share_class_type = _derive_kr_share_class_type(
        name=_clean_text(source.get("name")),
        security_type=security_type,
        existing_share_class_type=_clean_text(source.get("share_class_type")),
    )
    issuer_symbol = normalize_symbol_value(source.get("issuer_symbol"), "kr")
    anchor_symbol = normalize_symbol_value(source.get("earnings_anchor_symbol"), "kr")

    if not symbol or security_type != "COMMON_STOCK":
        return {
            "share_class_type": share_class_type,
            "issuer_symbol": issuer_symbol,
            "earnings_anchor_symbol": anchor_symbol,
        }

    if share_class_type == "PREFERRED" and not issuer_symbol:
        target_base_name = _derive_kr_base_name(source.get("name"))
        for candidate_symbol, candidate in (records_by_symbol or {}).items():
            normalized_candidate = normalize_symbol_value(candidate_symbol, "kr")
            if not normalized_candidate or normalized_candidate == symbol:
                continue
            if not _is_kr_common_candidate(candidate):
                continue
            candidate_share_class = _derive_kr_share_class_type(
                name=_clean_text(candidate.get("name")),
                security_type=_clean_text(candidate.get("security_type")),
                existing_share_class_type=_clean_text(candidate.get("share_class_type")),
            )
            if candidate_share_class != "COMMON":
                continue
            if _derive_kr_base_name(candidate.get("name")) == target_base_name:
                issuer_symbol = normalized_candidate
                break

    if share_class_type == "PREFERRED":
        anchor_symbol = anchor_symbol or issuer_symbol
    elif not anchor_symbol:
        anchor_symbol = symbol

    return {
        "share_class_type": share_class_type,
        "issuer_symbol": issuer_symbol,
        "earnings_anchor_symbol": anchor_symbol,
    }


def enrich_metadata_record(
    record: Mapping[str, Any] | None,
    *,
    market: str,
    official_hint: Mapping[str, Any] | None = None,
    records_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_market = market_key(market)
    source = dict(record or {})
    symbol = normalize_symbol_value(source.get("symbol"), normalized_market)
    if not symbol:
        symbol = _clean_text(source.get("symbol")).upper()

    enriched: dict[str, Any] = {}
    for column in CANONICAL_METADATA_COLUMNS:
        if column in _TEXT_COLUMNS:
            enriched[column] = _clean_text(source.get(column))
        elif column in _NUMERIC_COLUMNS:
            enriched[column] = _clean_number(source.get(column))
        elif column in {"earnings_expected", "fundamentals_expected"}:
            enriched[column] = _clean_bool(source.get(column))
        else:
            enriched[column] = source.get(column)

    enriched["symbol"] = symbol
    enriched["market"] = normalized_market
    enriched["quote_type"] = enriched["quote_type"].upper()
    enriched["security_type"] = enriched["security_type"].upper()
    enriched["name"] = enriched["name"] or _clean_text(
        (official_hint or {}).get("security_name")
    )
    enriched["fund_family"] = enriched["fund_family"] or _clean_text(
        (official_hint or {}).get("fund_family")
    )

    enriched["security_type"] = _infer_security_type(
        symbol=symbol,
        market=normalized_market,
        existing_security_type=enriched["security_type"],
        quote_type=enriched["quote_type"],
        exchange=enriched["exchange"],
        name=enriched["name"],
        fund_family=enriched["fund_family"],
        official_hint=official_hint,
    )

    provider_symbol = normalize_provider_symbol_value(enriched["provider_symbol"])
    if normalized_market == "kr":
        provider_symbol = _kr_provider_symbol(symbol, enriched["exchange"], provider_symbol)
    elif not provider_symbol:
        provider_symbol = symbol
    enriched["provider_symbol"] = provider_symbol

    if normalized_market == "kr":
        kr_fields = derive_kr_share_class_fields(enriched, records_by_symbol)
        enriched["share_class_type"] = kr_fields["share_class_type"]
        enriched["issuer_symbol"] = kr_fields["issuer_symbol"]
        enriched["earnings_anchor_symbol"] = kr_fields["earnings_anchor_symbol"]
    else:
        enriched["share_class_type"] = _clean_text(enriched.get("share_class_type")).upper()
        enriched["issuer_symbol"] = normalize_symbol_value(enriched.get("issuer_symbol"), normalized_market)
        enriched["earnings_anchor_symbol"] = normalize_symbol_value(
            enriched.get("earnings_anchor_symbol"),
            normalized_market,
        )

    skip_reason = _skip_reason(_clean_text(enriched["fetch_status"]), enriched["security_type"])
    if enriched["earnings_expected"] is None:
        enriched["earnings_expected"] = not bool(skip_reason)
    if enriched["fundamentals_expected"] is None:
        enriched["fundamentals_expected"] = not bool(skip_reason)
    if skip_reason:
        enriched["earnings_expected"] = False
        enriched["fundamentals_expected"] = False
        enriched["earnings_skip_reason"] = skip_reason
    else:
        enriched["earnings_skip_reason"] = ""

    if not enriched["provider_trace"]:
        trace_parts = [provider_symbol, _clean_text(enriched["source"])]
        enriched["provider_trace"] = "|".join(part for part in trace_parts if part)

    return enriched


def get_official_us_security_hint(symbol: str) -> dict[str, Any]:
    return dict(_load_us_security_hints().get(normalize_symbol_value(symbol, "us"), {}))


def get_security_profile(symbol: str, market: str) -> dict[str, Any]:
    normalized_market = market_key(market)
    symbol_key = normalize_symbol_value(symbol, normalized_market)
    metadata_records = _load_metadata_records(normalized_market)
    metadata_record = metadata_records.get(symbol_key)
    if metadata_record is None:
        metadata_record = {
            "symbol": symbol_key,
            "market": normalized_market,
            "provider_symbol": symbol_key,
            "fetch_status": "",
        }
    official_hint = (
        get_official_us_security_hint(symbol_key)
        if normalized_market == "us" and symbol_key in _load_metadata_records(normalized_market)
        else None
    )
    profile = enrich_metadata_record(
        metadata_record,
        market=normalized_market,
        official_hint=official_hint,
        records_by_symbol=metadata_records,
    )
    profile["preferred_provider_symbol"] = profile.get("provider_symbol") or symbol_key
    return profile


@lru_cache(maxsize=1)
def _load_us_security_hints() -> dict[str, dict[str, Any]]:
    base_dir = os.path.join(
        os.path.dirname(get_stock_metadata_path("us")),
        "external",
        "nasdaqtrader",
        "symboldirectory",
    )
    mapping: dict[str, dict[str, Any]] = {}
    for filename, symbol_field in (
        ("nasdaqlisted.txt", "Symbol"),
        ("otherlisted.txt", "ACT Symbol"),
    ):
        path = os.path.join(base_dir, filename)
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="|")
                for row in reader:
                    symbol = normalize_symbol_value(row.get(symbol_field), "us")
                    if not symbol or symbol.endswith("FILE CREATION TIME"):
                        continue
                    mapping[symbol] = {
                        "security_name": _clean_text(row.get("Security Name")),
                        "is_etf": _clean_text(row.get("ETF")).upper() == "Y",
                    }
        except Exception:
            continue
    return mapping


def _metadata_cache_key(market: str) -> tuple[str, float] | None:
    path = get_stock_metadata_path(market)
    if not os.path.exists(path):
        return None
    try:
        return path, os.path.getmtime(path)
    except OSError:
        return None


def _load_metadata_records(market: str) -> dict[str, dict[str, Any]]:
    cache_key = _metadata_cache_key(market)
    if cache_key is None:
        return {}
    return _load_metadata_records_cached(cache_key[0], cache_key[1], market_key(market))


@lru_cache(maxsize=4)
def _load_metadata_records_cached(
    metadata_path: str,
    _mtime: float,
    market: str,
) -> dict[str, dict[str, Any]]:
    try:
        frame = pd.read_csv(
            metadata_path,
            dtype={
                "symbol": "string",
                "provider_symbol": "string",
                "issuer_symbol": "string",
                "earnings_anchor_symbol": "string",
            },
            encoding="utf-8-sig",
        )
    except Exception:
        return {}

    if frame.empty or "symbol" not in frame.columns:
        return {}

    records: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict("records"):
        symbol = normalize_symbol_value(row.get("symbol"), market)
        if not symbol:
            continue
        records[symbol] = dict(row)
    return records
