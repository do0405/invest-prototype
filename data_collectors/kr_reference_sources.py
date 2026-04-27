from __future__ import annotations

from functools import lru_cache
from typing import Any

import pandas as pd


KR_FDR_LISTING_NAMES: dict[str, tuple[str, ...]] = {
    "KOSPI": ("KOSPI",),
    "KOSDAQ": ("KOSDAQ",),
    "ETF": ("ETF/KR", "ETF"),
    "ETN": ("ETN/KR", "ETN"),
}

KR_INDEX_READER_SYMBOLS: dict[str, str] = {
    "KOSPI": "KS11",
    "KOSDAQ": "KQ11",
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
        numeric = float(value)
    except Exception:
        return None
    if pd.isna(numeric):
        return None
    if float(numeric).is_integer():
        return int(numeric)
    return numeric


def _combine_source_labels(*values: Any) -> str:
    labels: list[str] = []
    for raw in values:
        text = _clean_text(raw)
        if not text:
            continue
        for part in text.split("+"):
            label = _clean_text(part)
            if label and label not in labels:
                labels.append(label)
    return "+".join(labels)


def load_fdr_module(*, required: bool = True) -> Any | None:
    try:
        import FinanceDataReader as fdr  # type: ignore
    except Exception as exc:
        if not required:
            return None
        raise RuntimeError(
            "FinanceDataReader is required for KR intake. Install it with "
            "'python -m pip install -r requirements.txt'."
        ) from exc
    return fdr


def load_finance_database_module(*, required: bool = True) -> Any | None:
    try:
        import financedatabase as fd  # type: ignore
    except Exception as exc:
        if not required:
            return None
        raise RuntimeError(
            "FinanceDatabase is required for KR metadata enrichment. Install it with "
            "'python -m pip install -r requirements.txt'."
        ) from exc
    return fd


def pick_listing_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    lookup = {str(column).strip().lower(): column for column in frame.columns}
    for alias in aliases:
        column = lookup.get(str(alias).strip().lower())
        if column is not None:
            return column
    return None


def normalize_kr_symbol(value: Any) -> str:
    text = _clean_text(value).upper()
    if not text:
        return ""
    if "." in text:
        text = text.split(".", 1)[0]
    digits = "".join(char for char in text if char.isdigit())
    if len(digits) == 6:
        return digits
    if text.isdigit() and len(text) == 6:
        return text
    return ""


def infer_kr_exchange_from_provider_symbol(value: Any) -> str:
    text = _clean_text(value).upper()
    if text.endswith(".KQ"):
        return "KOSDAQ"
    if text.endswith(".KS"):
        return "KOSPI"
    return ""


def normalize_kr_exchange(value: Any, fallback: str = "") -> str:
    text = _clean_text(value).upper()
    if text in {"KOSPI", "STK", "KS"}:
        return "KOSPI"
    if text in {"KOSDAQ", "KSQ", "KQ"}:
        return "KOSDAQ"
    if "ETF" in text:
        return "ETF"
    if "ETN" in text:
        return "ETN"

    inferred = infer_kr_exchange_from_provider_symbol(text)
    if inferred:
        return inferred

    fallback_text = _clean_text(fallback).upper()
    if fallback_text:
        if "ETF" in fallback_text:
            return "ETF"
        if "ETN" in fallback_text:
            return "ETN"
        if "KOSDAQ" in fallback_text or fallback_text == "KQ":
            return "KOSDAQ"
        if "KOSPI" in fallback_text or fallback_text in {"KRX", "KS"}:
            return "KOSPI"
        inferred = infer_kr_exchange_from_provider_symbol(fallback_text)
        if inferred:
            return inferred
    return text or fallback_text


def _provider_symbol_for_exchange(symbol: str, exchange: str) -> str:
    suffix = ".KQ" if exchange == "KOSDAQ" else ".KS"
    return f"{symbol}{suffix}"


def _security_type_for_exchange(exchange: str, fallback: str = "") -> str:
    fallback_text = _clean_text(fallback).upper()
    if exchange == "ETF" or "ETF" in fallback_text:
        return "ETF"
    if exchange == "ETN" or "ETN" in fallback_text:
        return "ETN"
    return "COMMON_STOCK"


def merge_partial_records(base: dict[str, object] | None, update: dict[str, object] | None) -> dict[str, object]:
    merged = dict(base or {})
    for key, value in (update or {}).items():
        if key == "source":
            merged[key] = _combine_source_labels(merged.get(key), value)
            continue
        text_value = value if isinstance(value, str) else None
        if text_value is not None:
            cleaned = _clean_text(text_value)
            if cleaned:
                merged[key] = cleaned
            continue
        if value is not None:
            merged[key] = value
    return merged


def listing_records_from_fdr(frame: pd.DataFrame, *, listing_name: str) -> dict[str, dict[str, object]]:
    if frame is None or frame.empty:
        return {}

    symbol_column = pick_listing_column(frame, ("Symbol", "Code", "symbol", "code"))
    if symbol_column is None:
        return {}

    exchange_column = pick_listing_column(frame, ("Market", "market", "Exchange", "exchange"))
    name_column = pick_listing_column(frame, ("Name", "name", "Company", "company"))
    sector_column = pick_listing_column(frame, ("Sector", "sector", "?낆쥌"))
    industry_column = pick_listing_column(frame, ("Industry", "industry", "Dept", "dept", "IndustryGroup", "industry_group"))
    market_cap_column = pick_listing_column(frame, ("Marcap", "MarketCap", "market_cap", "marcap"))
    shares_column = pick_listing_column(frame, ("Stocks", "stocks", "Shares", "shares_outstanding", "ListedShares"))

    records: dict[str, dict[str, object]] = {}
    for _, row in frame.iterrows():
        symbol = normalize_kr_symbol(row.get(symbol_column))
        if not symbol:
            continue
        exchange = normalize_kr_exchange(
            row.get(exchange_column) if exchange_column else "",
            fallback=listing_name,
        )
        if not exchange:
            exchange = normalize_kr_exchange("", fallback=listing_name)
        security_type = _security_type_for_exchange(exchange, fallback=listing_name)
        record: dict[str, object] = {
            "symbol": symbol,
            "market": "kr",
            "provider_symbol": _provider_symbol_for_exchange(symbol, exchange),
            "name": _clean_text(row.get(name_column)) if name_column else "",
            "exchange": exchange,
            "security_type": security_type,
            "sector": _clean_text(row.get(sector_column)) if sector_column else "",
            "industry": _clean_text(row.get(industry_column)) if industry_column else "",
            "market_cap": _clean_number(row.get(market_cap_column)) if market_cap_column else None,
            "shares_outstanding": _clean_number(row.get(shares_column)) if shares_column else None,
            "source": "fdr_listing",
        }
        records[symbol] = merge_partial_records(records.get(symbol), record)
    return records


def _iter_requested_listing_names(
    *,
    include_kosdaq: bool,
    include_etf: bool,
    include_etn: bool,
) -> list[str]:
    names = list(KR_FDR_LISTING_NAMES["KOSPI"])
    if include_kosdaq:
        names.extend(KR_FDR_LISTING_NAMES["KOSDAQ"])
    if include_etf:
        names.extend(KR_FDR_LISTING_NAMES["ETF"])
    if include_etn:
        names.extend(KR_FDR_LISTING_NAMES["ETN"])
    return names


def fetch_kr_listing_records(
    *,
    include_kosdaq: bool,
    include_etf: bool,
    include_etn: bool,
    fdr_module: Any | None = None,
) -> dict[str, dict[str, object]]:
    fdr = fdr_module if fdr_module is not None else load_fdr_module(required=True)
    records: dict[str, dict[str, object]] = {}
    for listing_name in _iter_requested_listing_names(
        include_kosdaq=include_kosdaq,
        include_etf=include_etf,
        include_etn=include_etn,
    ):
        try:
            frame = fdr.StockListing(listing_name)
        except Exception:
            continue
        for symbol, record in listing_records_from_fdr(frame, listing_name=listing_name).items():
            records[symbol] = merge_partial_records(records.get(symbol), record)
    return records


def fetch_kr_listing_symbols(
    *,
    include_kosdaq: bool,
    include_etf: bool,
    include_etn: bool,
    fdr_module: Any | None = None,
) -> set[str]:
    return set(
        fetch_kr_listing_records(
            include_kosdaq=include_kosdaq,
            include_etf=include_etf,
            include_etn=include_etn,
            fdr_module=fdr_module,
        ).keys()
    )


def _finance_database_selection(
    database_class: Any,
    *,
    country: str = "South Korea",
) -> pd.DataFrame:
    try:
        selection = database_class.select(country=country, only_primary_listing=True)
    except TypeError:
        selection = database_class.select(country=country)
    if selection is None:
        return pd.DataFrame()
    if isinstance(selection, pd.Series):
        selection = selection.to_frame().T
    if not isinstance(selection, pd.DataFrame):
        return pd.DataFrame()
    return selection.copy()


def _normalize_finance_database_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    normalized = frame.copy()
    if "symbol" not in normalized.columns:
        index_name = str(normalized.index.name or "").strip().lower()
        if index_name in {"symbol", "ticker", ""}:
            normalized = normalized.reset_index()
            if "index" in normalized.columns and "symbol" not in normalized.columns:
                normalized = normalized.rename(columns={"index": "symbol"})
    return normalized


def _records_from_finance_database_frame(
    frame: pd.DataFrame,
    *,
    requested: set[str],
    default_security_type: str,
) -> dict[str, dict[str, object]]:
    normalized_frame = _normalize_finance_database_frame(frame)
    if normalized_frame.empty:
        return {}

    symbol_column = pick_listing_column(normalized_frame, ("symbol", "ticker", "Symbol", "Ticker"))
    if symbol_column is None:
        return {}

    name_column = pick_listing_column(normalized_frame, ("name", "Name"))
    exchange_column = pick_listing_column(normalized_frame, ("exchange", "Exchange"))
    market_column = pick_listing_column(normalized_frame, ("market", "Market"))
    sector_column = pick_listing_column(normalized_frame, ("sector", "Sector"))
    industry_column = pick_listing_column(normalized_frame, ("industry", "Industry", "industry_group", "Industry Group", "category", "Category"))
    market_cap_column = pick_listing_column(normalized_frame, ("market_cap", "Market Cap"))
    shares_column = pick_listing_column(normalized_frame, ("shares_outstanding", "Shares Outstanding"))

    records: dict[str, dict[str, object]] = {}
    for _, row in normalized_frame.iterrows():
        symbol = normalize_kr_symbol(row.get(symbol_column))
        if not symbol or symbol not in requested:
            continue
        exchange = normalize_kr_exchange(
            row.get(exchange_column) if exchange_column else "",
            fallback=row.get(market_column) if market_column else "",
        )
        if not exchange:
            exchange = infer_kr_exchange_from_provider_symbol(row.get(symbol_column)) or "KOSPI"

        security_type = _security_type_for_exchange(exchange, fallback=default_security_type)
        record: dict[str, object] = {
            "symbol": symbol,
            "market": "kr",
            "provider_symbol": _provider_symbol_for_exchange(symbol, exchange),
            "name": _clean_text(row.get(name_column)) if name_column else "",
            "exchange": exchange,
            "security_type": security_type,
            "sector": _clean_text(row.get(sector_column)) if sector_column else "",
            "industry": _clean_text(row.get(industry_column)) if industry_column else "",
            "market_cap": _clean_number(row.get(market_cap_column)) if market_cap_column else None,
            "shares_outstanding": _clean_number(row.get(shares_column)) if shares_column else None,
            "source": "financedatabase",
        }
        records[symbol] = merge_partial_records(records.get(symbol), record)
    return records


@lru_cache(maxsize=1)
def _load_finance_database_country_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    fd = load_finance_database_module(required=True)

    equities_frame = pd.DataFrame()
    equities_cls = getattr(fd, "Equities", None)
    if equities_cls is not None:
        equities_frame = _finance_database_selection(equities_cls())

    etf_frame = pd.DataFrame()
    etf_cls = getattr(fd, "ETFs", None)
    if etf_cls is not None:
        etf_frame = _finance_database_selection(etf_cls())

    return equities_frame, etf_frame


def fetch_finance_database_kr_records(
    symbols: set[str],
    *,
    finance_database_module: Any | None = None,
) -> dict[str, dict[str, object]]:
    requested = {normalize_kr_symbol(symbol) for symbol in symbols}
    requested.discard("")
    if not requested:
        return {}

    if finance_database_module is None:
        equities_frame, etf_frame = _load_finance_database_country_frames()
    else:
        equities_cls = getattr(finance_database_module, "Equities", None)
        etf_cls = getattr(finance_database_module, "ETFs", None)
        equities_frame = _finance_database_selection(equities_cls()) if equities_cls is not None else pd.DataFrame()
        etf_frame = _finance_database_selection(etf_cls()) if etf_cls is not None else pd.DataFrame()

    records: dict[str, dict[str, object]] = {}
    for symbol, record in _records_from_finance_database_frame(
        equities_frame,
        requested=requested,
        default_security_type="COMMON_STOCK",
    ).items():
        records[symbol] = merge_partial_records(records.get(symbol), record)
    for symbol, record in _records_from_finance_database_frame(
        etf_frame,
        requested=requested,
        default_security_type="ETF",
    ).items():
        records[symbol] = merge_partial_records(records.get(symbol), record)
    return records


def fetch_kr_reference_metadata(
    symbols: list[str],
    *,
    fdr_module: Any | None = None,
    finance_database_module: Any | None = None,
) -> dict[str, dict[str, object]]:
    requested = {normalize_kr_symbol(symbol) for symbol in symbols}
    requested.discard("")
    if not requested:
        return {}

    listing_records = {
        symbol: record
        for symbol, record in fetch_kr_listing_records(
            include_kosdaq=True,
            include_etf=True,
            include_etn=True,
            fdr_module=fdr_module,
        ).items()
        if symbol in requested
    }
    finance_database_records = fetch_finance_database_kr_records(
        requested,
        finance_database_module=finance_database_module,
    )

    merged: dict[str, dict[str, object]] = {}
    for symbol in sorted(requested):
        merged[symbol] = merge_partial_records(
            listing_records.get(symbol, {"symbol": symbol, "market": "kr"}),
            finance_database_records.get(symbol),
        )
    return merged


def get_kr_index_reader_symbol(symbol: str) -> str:
    symbol_key = _clean_text(symbol).upper()
    return KR_INDEX_READER_SYMBOLS.get(symbol_key, symbol_key)


def fetch_fdr_ohlcv_frame(
    symbol: str,
    *,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    fdr_module: Any | None = None,
) -> pd.DataFrame:
    fdr = fdr_module if fdr_module is not None else load_fdr_module(required=True)
    return fdr.DataReader(symbol, start=start_yyyymmdd, end=end_yyyymmdd)
