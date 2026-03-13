from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Any

from utils.calc_utils import clean_tickers
from utils.io_utils import safe_filename


US_ALWAYS_INCLUDE_SYMBOLS: set[str] = {
    "SPY",
    "QQQ",
    "DIA",
    "IWM",
    "^GSPC",
    "^IXIC",
    "^DJI",
    "^RUT",
    "^VIX",
    "^VVIX",
    "^SKEW",
}

US_SEED_FILENAMES: tuple[str, ...] = (
    "nasdaq_symbols.csv",
    "nyse_symbols.csv",
    "amex_symbols.csv",
    "us_symbols.csv",
    "us_all_symbols.csv",
    "us_symbol_universe.csv",
    "us_etf_symbols.csv",
    "us_etn_symbols.csv",
    "us_inverse_etf_symbols.csv",
    "us_commodity_etf_symbols.csv",
)


def _read_symbols_from_csv(path: str) -> set[str]:
    symbols: set[str] = set()
    if not path or not os.path.exists(path):
        return symbols

    try:
        import pandas as pd

        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        return symbols
    if frame is None or frame.empty:
        return symbols

    candidate_columns = ("symbol", "ticker", "Symbol", "Ticker")
    selected = None
    for column in candidate_columns:
        if column in frame.columns:
            selected = frame[column]
            break
    if selected is None and len(frame.columns) > 0:
        selected = frame.iloc[:, 0]
    if selected is None:
        return symbols

    for raw in selected.tolist():
        if raw is None:
            continue
        symbol = str(raw).strip().upper()
        if symbol:
            symbols.add(symbol)
    return symbols


def _existing_file_symbols(data_dir: str) -> set[str]:
    if not data_dir or not os.path.isdir(data_dir):
        return set()
    symbols: set[str] = set()
    for name in os.listdir(data_dir):
        if not name.endswith(".csv"):
            continue
        path = os.path.join(data_dir, name)
        symbol_from_content = _read_symbol_from_existing_csv(path)
        if symbol_from_content:
            symbols.add(symbol_from_content)
            continue

        base_name = os.path.splitext(name)[0].strip().upper()
        if _is_reasonable_symbol_basename(base_name):
            symbols.add(base_name)
    return symbols


def _read_symbol_from_existing_csv(path: str) -> str:
    if not path or not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                return ""
            field_lookup = {str(field or "").strip().lower(): field for field in reader.fieldnames}
            symbol_field = field_lookup.get("symbol") or field_lookup.get("ticker")
            if not symbol_field:
                return ""
            for row in reader:
                raw = row.get(symbol_field)
                symbol = str(raw or "").strip().upper()
                if symbol:
                    return symbol
    except Exception:
        return ""
    return ""


def _is_reasonable_symbol_basename(base_name: str) -> bool:
    symbol = str(base_name or "").strip().upper()
    if not symbol:
        return False
    if symbol.endswith("_FILE"):
        original = symbol[:-5]
        return bool(original) and safe_filename(original) == symbol
    return "_" not in symbol


def _is_us_collectable_symbol(symbol: str) -> bool:
    symbol_key = str(symbol or "").strip().upper()
    if not symbol_key:
        return False
    if symbol_key.startswith("^"):
        return True
    if "$" in symbol_key:
        return False
    if any(symbol_key.endswith(suffix) for suffix in (".W", ".U", ".RT", ".PR", ".WS")):
        return False
    if len(symbol_key) == 5 and symbol_key.isalpha() and symbol_key[-1] in {"W", "U", "R"}:
        return False
    return True


def load_us_symbol_universe(
    *,
    data_dir: str,
    us_data_dir: str,
    stock_metadata_path: str | None = None,
) -> list[str]:
    discovered: set[str] = set()
    discovered.update(_existing_file_symbols(us_data_dir))
    for filename in US_SEED_FILENAMES:
        discovered.update(_read_symbols_from_csv(os.path.join(data_dir, filename)))
    if stock_metadata_path:
        discovered.update(_read_symbols_from_csv(stock_metadata_path))
    discovered.update(US_ALWAYS_INCLUDE_SYMBOLS)

    cleaned = clean_tickers(list(discovered))
    filtered = [symbol for symbol in cleaned if _is_us_collectable_symbol(symbol)]
    return sorted(set(filtered))


def _existing_numeric_kr_symbols(data_dir: str) -> set[str]:
    symbols = _existing_file_symbols(data_dir)
    return {
        symbol
        for symbol in symbols
        if symbol.isdigit() and len(symbol) == 6
    }


def load_kr_symbol_universe(
    *,
    data_dir: str,
    stock_metadata_path: str | None = None,
    include_kosdaq: bool = True,
    include_etf: bool = True,
    include_etn: bool = True,
    stock_module: Any | None = None,
    fdr_module: Any | None = None,
    as_of: datetime | None = None,
) -> list[str]:
    discovered: set[str] = set()
    discovered.update(_existing_numeric_kr_symbols(data_dir))
    if stock_metadata_path:
        discovered.update(
            {
                symbol
                for symbol in _read_symbols_from_csv(stock_metadata_path)
                if symbol.isdigit() and len(symbol) == 6
            }
        )

    return sorted(
        symbol
        for symbol in discovered
        if symbol
        and symbol.isdigit()
        and len(symbol) == 6
    )
