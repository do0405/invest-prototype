from __future__ import annotations

import csv
import json
import os
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Callable

import requests

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

US_OFFICIAL_SEED_FILENAMES: tuple[tuple[str, str], ...] = (
    ("nasdaq", "nasdaq_symbols.csv"),
    ("nyse", "nyse_symbols.csv"),
    ("amex", "amex_symbols.csv"),
    ("nyse_arca", "nyse_arca_symbols.csv"),
    ("bats", "bats_symbols.csv"),
    ("iex", "iex_symbols.csv"),
)

US_SUPPLEMENTAL_SEED_FILENAMES: tuple[str, ...] = (
    "us_symbols.csv",
    "us_all_symbols.csv",
    "us_symbol_universe.csv",
    "us_etf_symbols.csv",
    "us_etn_symbols.csv",
    "us_inverse_etf_symbols.csv",
    "us_commodity_etf_symbols.csv",
)

US_BROAD_SEED_FILENAMES: tuple[str, ...] = ("broad_us_seed.csv",)
US_LEGACY_BROAD_SEED_FILENAMES: tuple[str, ...] = ("nasdaq_symbols.csv",)
US_OFFICIAL_SYMBOL_DIRECTORY_URLS: dict[str, str] = {
    "nasdaqlisted": "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    "otherlisted": "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
}
US_OFFICIAL_OTHERLISTED_EXCHANGE_BUCKETS: dict[str, str] = {
    "N": "nyse",
    "A": "amex",
    "P": "nyse_arca",
    "Z": "bats",
    "V": "iex",
}
US_SYMBOL_DIRECTORY_RAW_RELATIVE_DIR = os.path.join("external", "nasdaqtrader", "symboldirectory")
US_SYMBOL_DIRECTORY_MANIFEST_RELATIVE_PATH = os.path.join(
    "external",
    "nasdaqtrader",
    "symboldirectory_manifest.json",
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


def _emit_progress(progress: Callable[[str], None] | None, message: str) -> None:
    if progress is not None:
        progress(message)



def _existing_file_symbols(
    data_dir: str,
    *,
    progress: Callable[[str], None] | None = None,
    progress_every: int = 1000,
    heartbeat_seconds: float = 20.0,
) -> set[str]:
    if not data_dir or not os.path.isdir(data_dir):
        return set()

    names = sorted(name for name in os.listdir(data_dir) if name.endswith(".csv"))
    total = len(names)
    symbols: set[str] = set()
    started = perf_counter()
    last_progress = started

    _emit_progress(progress, f"[Universe] Local OHLCV scan started - total_files={total}, dir={data_dir}")

    for index, name in enumerate(names, start=1):
        base_name = os.path.splitext(name)[0].strip().upper()
        if _is_reasonable_symbol_basename(base_name):
            symbols.add(base_name)
        else:
            path = os.path.join(data_dir, name)
            symbol_from_content = _read_symbol_from_existing_csv(path)
            if symbol_from_content:
                symbols.add(symbol_from_content)

        now = perf_counter()
        should_emit = (
            index == total
            or index % max(1, int(progress_every)) == 0
            or (now - last_progress) >= float(heartbeat_seconds)
        )
        if should_emit:
            elapsed = now - started
            _emit_progress(
                progress,
                f"[Universe] Local OHLCV scan progress - processed={index}/{total}, discovered={len(symbols)}, elapsed={elapsed:.1f}s",
            )
            last_progress = now

    return symbols


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


def _normalize_seed_symbols(symbols: list[str] | set[str]) -> list[str]:
    cleaned = clean_tickers(list(symbols))
    return sorted({symbol for symbol in cleaned if _is_us_collectable_symbol(symbol)})


def _seed_file_path(data_dir: str, filename: str) -> str:
    return os.path.join(data_dir, filename)


def _raw_symbol_directory_paths(data_dir: str) -> dict[str, str]:
    raw_dir = os.path.join(data_dir, US_SYMBOL_DIRECTORY_RAW_RELATIVE_DIR)
    return {
        "raw_dir": raw_dir,
        "nasdaqlisted": os.path.join(raw_dir, "nasdaqlisted.txt"),
        "otherlisted": os.path.join(raw_dir, "otherlisted.txt"),
        "manifest": os.path.join(data_dir, US_SYMBOL_DIRECTORY_MANIFEST_RELATIVE_PATH),
    }


def _write_symbols_to_csv(path: str, symbols: list[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["symbol"])
        for symbol in symbols:
            writer.writerow([symbol])


def _iter_symbol_directory_rows(raw_text: str) -> list[dict[str, str]]:
    if not raw_text:
        return []
    lines = [line.strip() for line in str(raw_text).splitlines() if str(line).strip()]
    filtered = [line.lstrip("\ufeff") for line in lines if not line.startswith("File Creation Time")]
    if not filtered:
        return []
    reader = csv.DictReader(filtered, delimiter="|")
    rows: list[dict[str, str]] = []
    for row in reader:
        rows.append({str(key or "").strip(): str(value or "").strip() for key, value in row.items()})
    return rows


def _parse_official_nasdaq_seed(raw_text: str) -> list[str]:
    symbols: set[str] = set()
    for row in _iter_symbol_directory_rows(raw_text):
        if str(row.get("Test Issue") or "").strip().upper() == "Y":
            continue
        symbol = str(row.get("Symbol") or "").strip().upper()
        if symbol:
            symbols.add(symbol)
    return _normalize_seed_symbols(symbols)


def _parse_official_otherlisted_seeds(raw_text: str) -> dict[str, list[str]]:
    buckets: dict[str, set[str]] = {
        bucket: set()
        for bucket in US_OFFICIAL_OTHERLISTED_EXCHANGE_BUCKETS.values()
    }
    for row in _iter_symbol_directory_rows(raw_text):
        if str(row.get("Test Issue") or "").strip().upper() == "Y":
            continue
        exchange_code = str(row.get("Exchange") or "").strip().upper()
        bucket = US_OFFICIAL_OTHERLISTED_EXCHANGE_BUCKETS.get(exchange_code)
        if not bucket:
            continue
        symbol = (
            str(row.get("ACT Symbol") or "").strip().upper()
            or str(row.get("NASDAQ Symbol") or "").strip().upper()
            or str(row.get("CQS Symbol") or "").strip().upper()
        )
        if symbol:
            buckets[bucket].add(symbol)
    return {bucket: _normalize_seed_symbols(symbols) for bucket, symbols in buckets.items()}


def _download_symbol_directory_text(
    url: str,
    *,
    timeout_seconds: float = 20.0,
    requests_module=None,
) -> str:
    client = requests if requests_module is None else requests_module
    response = client.get(
        url,
        timeout=max(1.0, float(timeout_seconds)),
        headers={"User-Agent": "invest-prototype-symbol-sync/1.0"},
    )
    response.raise_for_status()
    return str(response.text or "")


def sync_official_us_symbol_directory(
    *,
    data_dir: str,
    progress: Callable[[str], None] | None = None,
    requests_module=None,
    timeout_seconds: float = 20.0,
) -> dict[str, int]:
    paths = _raw_symbol_directory_paths(data_dir)
    os.makedirs(paths["raw_dir"], exist_ok=True)

    raw_payloads: dict[str, str] = {}
    for name, url in US_OFFICIAL_SYMBOL_DIRECTORY_URLS.items():
        _emit_progress(progress, f"[Universe] Official seed sync started - source={name}")
        raw_payload = _download_symbol_directory_text(
            url,
            timeout_seconds=timeout_seconds,
            requests_module=requests_module,
        )
        raw_payloads[name] = raw_payload
        with open(paths[name], "w", encoding="utf-8", newline="\n") as handle:
            handle.write(raw_payload)

    output_symbols: dict[str, list[str]] = {
        "nasdaq_symbols.csv": _parse_official_nasdaq_seed(raw_payloads["nasdaqlisted"]),
    }
    otherlisted_buckets = _parse_official_otherlisted_seeds(raw_payloads["otherlisted"])
    for bucket, filename in US_OFFICIAL_SEED_FILENAMES:
        if bucket == "nasdaq":
            continue
        output_symbols[filename] = otherlisted_buckets.get(bucket, [])

    broad_symbols: set[str] = set(output_symbols["nasdaq_symbols.csv"])
    for bucket, filename in US_OFFICIAL_SEED_FILENAMES:
        if bucket == "nasdaq":
            continue
        broad_symbols.update(output_symbols.get(filename, []))
    output_symbols["broad_us_seed.csv"] = sorted(broad_symbols)

    summary: dict[str, int] = {}
    for filename, symbols in output_symbols.items():
        _write_symbols_to_csv(_seed_file_path(data_dir, filename), symbols)
        summary[filename] = len(symbols)
        label = "Broad seed synced" if filename == "broad_us_seed.csv" else "Official seed synced"
        _emit_progress(progress, f"[Universe] {label} - file={filename}, count={len(symbols)}")

    manifest = {
        "synced_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": US_OFFICIAL_SYMBOL_DIRECTORY_URLS,
        "outputs": summary,
    }
    os.makedirs(os.path.dirname(paths["manifest"]) or ".", exist_ok=True)
    with open(paths["manifest"], "w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
    _emit_progress(progress, f"[Universe] Official seed manifest saved - path={paths['manifest']}")
    return summary


def _has_official_seed_manifest(data_dir: str) -> bool:
    return os.path.exists(_raw_symbol_directory_paths(data_dir)["manifest"])


def _load_existing_seed_file(
    data_dir: str,
    filename: str,
    *,
    progress: Callable[[str], None] | None = None,
    label: str,
) -> set[str]:
    path = _seed_file_path(data_dir, filename)
    if not os.path.exists(path):
        return set()
    symbols = _read_symbols_from_csv(path)
    _emit_progress(progress, f"[Universe] {label} - file={filename}, count={len(symbols)}")
    return symbols


def load_us_symbol_universe(
    *,
    data_dir: str,
    us_data_dir: str,
    stock_metadata_path: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> list[str]:
    discovered: set[str] = set()

    existing_symbols = _existing_file_symbols(us_data_dir, progress=progress)
    discovered.update(existing_symbols)
    _emit_progress(progress, f"[Universe] Local OHLCV symbols loaded - count={len(existing_symbols)}")

    official_mode = _has_official_seed_manifest(data_dir) or any(
        os.path.exists(_seed_file_path(data_dir, filename))
        for bucket, filename in US_OFFICIAL_SEED_FILENAMES
        if bucket != "nasdaq"
    )

    if official_mode:
        for _, filename in US_OFFICIAL_SEED_FILENAMES:
            discovered.update(
                _load_existing_seed_file(
                    data_dir,
                    filename,
                    progress=progress,
                    label="Official split seed loaded",
                )
            )
    else:
        loaded_broad = False
        for filename in US_BROAD_SEED_FILENAMES:
            symbols = _load_existing_seed_file(
                data_dir,
                filename,
                progress=progress,
                label="Broad seed loaded",
            )
            if symbols:
                discovered.update(symbols)
                loaded_broad = True
        if not loaded_broad:
            for filename in US_LEGACY_BROAD_SEED_FILENAMES:
                symbols = _load_existing_seed_file(
                    data_dir,
                    filename,
                    progress=progress,
                    label="Legacy broad seed loaded",
                )
                if symbols:
                    discovered.update(symbols)
                    _emit_progress(
                        progress,
                        f"[Universe] Legacy broad seed alias active - file={filename}, logical_name=broad_us_seed.csv",
                    )

    for filename in US_SUPPLEMENTAL_SEED_FILENAMES:
        discovered.update(
            _load_existing_seed_file(
                data_dir,
                filename,
                progress=progress,
                label="Supplemental seed loaded",
            )
        )

    if stock_metadata_path:
        metadata_symbols = _read_symbols_from_csv(stock_metadata_path)
        discovered.update(metadata_symbols)
        _emit_progress(progress, f"[Universe] Metadata seed loaded - count={len(metadata_symbols)}")

    discovered.update(US_ALWAYS_INCLUDE_SYMBOLS)
    cleaned = clean_tickers(list(discovered))
    filtered = [symbol for symbol in cleaned if _is_us_collectable_symbol(symbol)]
    final_symbols = sorted(set(filtered))

    _emit_progress(
        progress,
        f"[Universe] US universe ready - raw={len(discovered)}, cleaned={len(cleaned)}, collectable={len(final_symbols)}",
    )
    return final_symbols


def _existing_numeric_kr_symbols(data_dir: str) -> set[str]:
    symbols = _existing_file_symbols(data_dir)
    return {
        symbol
        for symbol in symbols
        if symbol.isdigit() and len(symbol) == 6
    }


def _resolve_kr_business_day_yyyymmdd(stock_module: Any, as_of: datetime | None) -> str:
    day = as_of or datetime.now(UTC)
    day_str = day.strftime("%Y%m%d")
    resolver = getattr(stock_module, "get_nearest_business_day_in_a_week", None)
    if resolver is None:
        return day_str
    try:
        resolved = str(resolver(day_str) or "").strip()
    except Exception:
        return day_str
    return resolved or day_str


def _load_kr_seed_symbols(
    *,
    stock_module: Any | None,
    include_kosdaq: bool,
    include_etf: bool,
    include_etn: bool,
    as_of: datetime | None,
) -> set[str]:
    if stock_module is None:
        return set()
    client = stock_module

    getter = getattr(client, "get_market_ticker_list", None)
    if getter is None:
        return set()

    as_of_yyyymmdd = _resolve_kr_business_day_yyyymmdd(client, as_of)
    market_buckets: list[str] = ["KOSPI"]
    if include_kosdaq:
        market_buckets.append("KOSDAQ")
    if include_etf:
        market_buckets.append("ETF")
    if include_etn:
        market_buckets.append("ETN")

    discovered: set[str] = set()
    for market_name in market_buckets:
        try:
            tickers = getter(as_of_yyyymmdd, market=market_name)
        except TypeError:
            try:
                tickers = getter(as_of_yyyymmdd, market_name)
            except Exception:
                continue
        except Exception:
            continue

        for raw in tickers or []:
            symbol = str(raw or "").strip().upper()
            if symbol.isdigit() and len(symbol) == 6:
                discovered.add(symbol)

    return discovered


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
    del fdr_module
    discovered: set[str] = set()
    discovered.update(_existing_numeric_kr_symbols(data_dir))
    discovered.update(
        _load_kr_seed_symbols(
            stock_module=stock_module,
            include_kosdaq=include_kosdaq,
            include_etf=include_etf,
            include_etn=include_etn,
            as_of=as_of,
        )
    )
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
