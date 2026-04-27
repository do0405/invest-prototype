"""KR OHLCV collector using FinanceDataReader primary fetch with yfinance fallback."""

from __future__ import annotations

import contextlib
import io
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from config import DATA_KR_DIR
from data_collectors.kr_reference_sources import (
    fetch_fdr_ohlcv_frame,
    get_kr_index_reader_symbol,
    load_fdr_module as load_kr_fdr_module,
)
from data_collectors.symbol_universe import load_kr_symbol_universe
from utils.console_runtime import bootstrap_windows_utf8
from utils.collector_diagnostics import CollectorDiagnostics
from utils.collector_run_state import (
    build_collector_summary,
    collector_tickers_for_run,
    load_collector_run_state,
    record_collector_symbol_status,
    write_collector_run_state,
)
from utils.io_utils import ensure_dir
from utils.market_data_contract import (
    CANONICAL_OHLCV_COLUMNS,
    LEGACY_MOJIBAKE_ALIASES,
    OHLCV_COLUMN_ALIASES,
    PricePolicy,
    normalize_ohlcv_columns,
    normalize_ohlcv_frame,
)
from utils.market_runtime import get_collector_run_state_path, get_stock_metadata_path, iter_provider_symbols, limit_runtime_symbols
from utils.ohlcv_progress import format_us_style_chunk_start, format_us_style_chunk_summary
from utils.yfinance_runtime import bootstrap_yfinance_cache
from utils.yahoo_throttle import extend_yahoo_cooldown, record_yahoo_request_success, wait_for_yahoo_request_slot


logger = logging.getLogger(__name__)

CANONICAL_KR_OHLCV_COLUMNS = CANONICAL_OHLCV_COLUMNS
KR_OHLCV_STORAGE_COLUMNS = (
    *CANONICAL_KR_OHLCV_COLUMNS,
    "adj_close",
    "dividends",
    "stock_splits",
    "split_factor",
)
KR_OHLCV_COLUMN_ALIASES = {
    alias: canonical
    for canonical, aliases in OHLCV_COLUMN_ALIASES.items()
    for alias in aliases
}
KR_OHLCV_COLUMN_ALIASES.update(
    {
        "날짜": "date",
        "일자": "date",
        "시가": "open",
        "고가": "high",
        "저가": "low",
        "종가": "close",
        "거래량": "volume",
    }
)
for canonical, aliases in LEGACY_MOJIBAKE_ALIASES.items():
    for alias in aliases:
        KR_OHLCV_COLUMN_ALIASES[alias] = canonical

KR_OHLCV_CHUNK_SIZE = 10
KR_OHLCV_REQUEST_DELAY_SECONDS = 0.50
KR_OHLCV_CHUNK_PAUSE_SECONDS = KR_OHLCV_REQUEST_DELAY_SECONDS
KR_OHLCV_MAX_RETRIES = 2
KR_OHLCV_EMPTY_RETRY_DELAY_SECONDS = 1.0
KR_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS = 45.0
KR_OHLCV_REFRESH_OVERLAP_BARS = 2
KR_OHLCV_TARGET_BARS = 330
KR_OHLCV_DEFAULT_LOOKBACK_DAYS = 520
_KR_COLLECTOR_STATE_WRITE_WARNED: set[str] = set()
_kr_collector_diagnostics_local = threading.local()
_KR_COLLECTOR_COMPLETED_STATUSES = {"saved", "latest", "kept_existing", "delisted"}
_KR_COLLECTOR_RETRYABLE_STATUSES = {
    "failed",
    "partial",
    "rate_limited",
    "soft_unavailable",
}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _kr_fdr_worker_count(total_items: int) -> int:
    if total_items <= 1:
        return 1
    configured = _env_int("INVEST_PROTO_KR_FDR_WORKERS", 4)
    return max(1, min(configured, total_items))


def _normalize_kr_ohlcv_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=list(KR_OHLCV_STORAGE_COLUMNS))

    frame = df.copy()
    frame = frame.rename(columns=KR_OHLCV_COLUMN_ALIASES)
    frame = normalize_ohlcv_columns(frame)
    frame = frame.loc[:, ~frame.columns.duplicated()]
    normalized = normalize_ohlcv_frame(frame, symbol=ticker, price_policy=PricePolicy.RAW)
    if normalized.empty:
        return pd.DataFrame(columns=list(KR_OHLCV_STORAGE_COLUMNS))

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized["adj_close"] = pd.to_numeric(normalized["adj_close"], errors="coerce").fillna(normalized["close"])
    normalized["dividends"] = pd.to_numeric(normalized["dividends"], errors="coerce").fillna(0.0)
    normalized["stock_splits"] = pd.to_numeric(normalized["stock_splits"], errors="coerce").fillna(0.0)
    normalized["split_factor"] = pd.to_numeric(normalized["split_factor"], errors="coerce").fillna(1.0)
    normalized = normalized.dropna(subset=["date", "close"]).copy()
    normalized = normalized.sort_values("date").reset_index(drop=True)
    return normalized[list(KR_OHLCV_STORAGE_COLUMNS)]


def _active_kr_collector_diagnostics() -> CollectorDiagnostics | None:
    diagnostics = getattr(_kr_collector_diagnostics_local, "diagnostics", None)
    return diagnostics if isinstance(diagnostics, CollectorDiagnostics) else None


def _resolve_market_day(day: datetime) -> datetime:
    if day.tzinfo is not None:
        day = day.astimezone(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
    current = day.replace(hour=0, minute=0, second=0, microsecond=0)
    has_intraday_time = any((day.hour, day.minute, day.second, day.microsecond))
    if has_intraday_time and (day.hour, day.minute) < (15, 30):
        current -= timedelta(days=1)
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current


def _compute_kr_overlap_fetch_start(
    existing_dates: pd.Series,
    fallback_start_dt: datetime,
    *,
    overlap_bars: int = KR_OHLCV_REFRESH_OVERLAP_BARS,
) -> tuple[datetime, Optional[pd.Timestamp], int]:
    dates = pd.to_datetime(existing_dates, errors="coerce").dropna()
    if dates.empty:
        return fallback_start_dt, None, 0

    unique_dates = sorted({value.normalize() for value in dates.tolist()})
    if not unique_dates:
        return fallback_start_dt, None, 0

    last_date = unique_dates[-1]
    overlap_count = max(1, min(int(overlap_bars), len(unique_dates)))
    start_dt = unique_dates[-overlap_count].to_pydatetime()
    return start_dt, last_date, overlap_count


def _resolve_kr_universe(
    *,
    target_dir: str,
    include_kosdaq: bool,
    include_etf: bool,
    include_etn: bool,
    fdr_module: Any | None,
) -> Tuple[List[str], List[Dict[str, str]]]:
    universe = load_kr_symbol_universe(
        data_dir=target_dir,
        stock_metadata_path=get_stock_metadata_path("kr"),
        include_kosdaq=include_kosdaq,
        include_etf=include_etf,
        include_etn=include_etn,
        fdr_module=fdr_module,
    )
    diagnostics: List[Dict[str, str]] = []
    if universe:
        diagnostics.append(
            {
                "source": "fdr_listing",
                "scope": "universe",
                "error": "using FinanceDataReader listings plus local KR csv files and stock_metadata_kr.csv",
            }
        )
    return universe, diagnostics


@lru_cache(maxsize=1)
def _load_kr_provider_symbol_map() -> dict[str, str]:
    metadata_path = get_stock_metadata_path("kr")
    if not os.path.exists(metadata_path):
        return {}
    try:
        frame = pd.read_csv(metadata_path)
    except Exception:
        return {}
    if frame is None or frame.empty or "symbol" not in frame.columns:
        return {}

    mapping: dict[str, str] = {}
    for _, row in frame.iterrows():
        symbol = str(row.get("symbol") or "").strip().upper()
        provider_symbol = str(row.get("provider_symbol") or "").strip().upper()
        if symbol and provider_symbol:
            mapping[symbol] = provider_symbol
    return mapping


def _iter_kr_provider_symbols(ticker: str) -> list[str]:
    provider_symbols: list[str] = []
    preferred = _load_kr_provider_symbol_map().get(str(ticker or "").strip().upper(), "")
    if preferred:
        provider_symbols.append(preferred)
    for candidate in iter_provider_symbols(ticker, "kr"):
        if candidate not in provider_symbols:
            provider_symbols.append(candidate)
    return provider_symbols


def _fetch_kr_ohlcv_via_yfinance(
    *,
    ticker: str,
    start_dt: datetime,
    end_dt: datetime,
) -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    last_error: Optional[str] = None
    for provider_symbol in _iter_kr_provider_symbols(ticker):
        sink = io.StringIO()
        try:
            with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                history = yf.Ticker(provider_symbol).history(
                    start=start_dt.strftime("%Y-%m-%d"),
                    end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
                    interval="1d",
                    auto_adjust=False,
                    actions=True,
                )
        except Exception as exc:
            last_error = str(exc)
            noisy_output = sink.getvalue().strip()
            if noisy_output:
                last_error = f"{last_error} | {noisy_output}" if last_error else noisy_output
            continue

        normalized = _normalize_kr_ohlcv_frame(history, ticker=ticker)
        if not normalized.empty:
            record_yahoo_request_success("KR OHLCV")
            return normalized, provider_symbol, None

        noisy_output = sink.getvalue().strip()
        if noisy_output:
            last_error = noisy_output

    return pd.DataFrame(), None, last_error or "yfinance returned empty frame"


def _fetch_kr_ohlcv_via_fdr(
    *,
    ticker: str,
    start_dt: datetime,
    end_dt: datetime,
    fdr_module: Any,
) -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    try:
        frame = fetch_fdr_ohlcv_frame(
            ticker,
            start_yyyymmdd=start_dt.strftime("%Y-%m-%d"),
            end_yyyymmdd=end_dt.strftime("%Y-%m-%d"),
            fdr_module=fdr_module,
        )
    except Exception as exc:
        return pd.DataFrame(), None, str(exc)

    normalized = _normalize_kr_ohlcv_frame(frame, ticker=ticker)
    if not normalized.empty:
        return normalized, ticker, None
    return pd.DataFrame(), None, "FinanceDataReader returned empty frame"


def _fetch_kr_ohlcv_with_fallback(
    *,
    ticker: str,
    start_dt: datetime,
    end_dt: datetime,
    end_yyyymmdd: str,
    stock_client: Any,
    fdr_module: Any | None,
    provider_mode: str = "yfinance_only",
    fdr_primary_enabled: bool = True,
    allow_yahoo_fallback: bool = True,
) -> Tuple[pd.DataFrame, str, Optional[str]]:
    del end_yyyymmdd, stock_client, provider_mode
    fetch_error: Optional[str] = None
    diagnostics = _active_kr_collector_diagnostics()
    fdr_client = fdr_module if fdr_module is not None else load_kr_fdr_module(required=False)
    if fdr_primary_enabled and fdr_client is not None:
        if diagnostics is not None:
            diagnostics.increment("fdr_attempts")
        fdr_started = time.perf_counter()
        try:
            normalized, fdr_symbol, fetch_error = _fetch_kr_ohlcv_via_fdr(
                ticker=ticker,
                start_dt=start_dt,
                end_dt=end_dt,
                fdr_module=fdr_client,
            )
        finally:
            if diagnostics is not None:
                diagnostics.add_timing("provider_fetch_seconds", time.perf_counter() - fdr_started)
        if not normalized.empty:
            if diagnostics is not None:
                diagnostics.increment("fdr_successes")
            return normalized, f"fdr_ohlcv:{fdr_symbol}", None
    if not allow_yahoo_fallback:
        return pd.DataFrame(), "needs_yahoo_fallback", fetch_error or "FinanceDataReader returned empty frame"
    if diagnostics is not None:
        diagnostics.increment("yahoo_fetch_symbols")
        if (not fdr_primary_enabled) or fdr_client is not None:
            diagnostics.increment("yahoo_fallback_symbols")
    wait_seconds = wait_for_yahoo_request_slot("KR OHLCV", min_interval=KR_OHLCV_REQUEST_DELAY_SECONDS)
    if diagnostics is not None:
        diagnostics.add_timing("provider_wait_seconds", float(wait_seconds or 0.0))
    yahoo_started = time.perf_counter()
    try:
        normalized, provider_symbol, fetch_error = _fetch_kr_ohlcv_via_yfinance(
            ticker=ticker,
            start_dt=start_dt,
            end_dt=end_dt,
        )
    finally:
        if diagnostics is not None:
            diagnostics.add_timing("provider_fetch_seconds", time.perf_counter() - yahoo_started)
    if not normalized.empty:
        return normalized, f"yfinance:{provider_symbol}", None
    return pd.DataFrame(), "unavailable", fetch_error


def _fetch_yfinance_index_ohlcv(
    *,
    symbol: str,
    provider_symbol: str,
    start_dt: datetime,
    end_dt: datetime,
) -> pd.DataFrame:
    sink = io.StringIO()
    try:
        wait_seconds = wait_for_yahoo_request_slot("KR OHLCV", min_interval=KR_OHLCV_REQUEST_DELAY_SECONDS)
        diagnostics = _active_kr_collector_diagnostics()
        if diagnostics is not None:
            diagnostics.add_timing("provider_wait_seconds", float(wait_seconds or 0.0))
        provider_started = time.perf_counter()
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            history = yf.Ticker(provider_symbol).history(
                start=start_dt.strftime("%Y-%m-%d"),
                end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
                interval="1d",
                auto_adjust=False,
                actions=True,
            )
        if diagnostics is not None:
            diagnostics.add_timing("provider_fetch_seconds", time.perf_counter() - provider_started)
    except Exception:
        return pd.DataFrame()
    normalized = _normalize_kr_ohlcv_frame(history, ticker=symbol)
    if not normalized.empty:
        record_yahoo_request_success("KR OHLCV")
    return normalized


def _fetch_fdr_index_ohlcv(
    *,
    symbol: str,
    start_dt: datetime,
    end_dt: datetime,
    fdr_module: Any | None,
) -> pd.DataFrame:
    fdr_client = fdr_module if fdr_module is not None else load_kr_fdr_module(required=True)
    try:
        frame = fetch_fdr_ohlcv_frame(
            get_kr_index_reader_symbol(symbol),
            start_yyyymmdd=start_dt.strftime("%Y-%m-%d"),
            end_yyyymmdd=end_dt.strftime("%Y-%m-%d"),
            fdr_module=fdr_client,
        )
    except Exception:
        return pd.DataFrame()
    return _normalize_kr_ohlcv_frame(frame, ticker=symbol)


def _collect_index_benchmarks(
    *,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    target_dir: str,
    fdr_module: Any | None = None,
) -> list[str]:
    start_dt = datetime.strptime(start_yyyymmdd, "%Y%m%d")
    end_dt = datetime.strptime(end_yyyymmdd, "%Y%m%d")
    saved: list[str] = []
    index_specs = [
        ("KOSPI", "^KS11"),
        ("KOSDAQ", "^KQ11"),
    ]
    for symbol, provider_symbol in index_specs:
        normalized = _fetch_fdr_index_ohlcv(
            symbol=symbol,
            start_dt=start_dt,
            end_dt=end_dt,
            fdr_module=fdr_module,
        )
        if normalized.empty:
            normalized = _fetch_yfinance_index_ohlcv(
                symbol=symbol,
                provider_symbol=provider_symbol,
                start_dt=start_dt,
                end_dt=end_dt,
            )
        if normalized.empty:
            continue
        out_path = os.path.join(target_dir, f"{symbol}.csv")
        normalized.to_csv(out_path, index=False)
        saved.append(symbol)
    return saved


def _read_existing_kr_ohlcv(path: str, ticker: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    existing = pd.read_csv(path)
    return _normalize_kr_ohlcv_frame(existing, ticker=ticker)


def _trim_kr_ohlcv_window(frame: pd.DataFrame, start_dt: datetime) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(KR_OHLCV_STORAGE_COLUMNS))

    trimmed = frame.copy()
    trimmed["date"] = pd.to_datetime(trimmed["date"], errors="coerce")
    trimmed = trimmed.dropna(subset=["date"])
    trimmed = trimmed[trimmed["date"] >= pd.Timestamp(start_dt.date())].copy()
    if len(trimmed) > KR_OHLCV_TARGET_BARS:
        trimmed = trimmed.sort_values("date", ascending=False).head(KR_OHLCV_TARGET_BARS).copy()
    trimmed["date"] = trimmed["date"].dt.strftime("%Y-%m-%d")
    trimmed = trimmed.sort_values("date").reset_index(drop=True)
    for column in KR_OHLCV_STORAGE_COLUMNS:
        if column not in trimmed.columns:
            if column == "adj_close":
                trimmed[column] = trimmed["close"]
            elif column in {"dividends", "stock_splits"}:
                trimmed[column] = 0.0
            elif column == "split_factor":
                trimmed[column] = 1.0
            else:
                trimmed[column] = pd.NA
    return trimmed[list(KR_OHLCV_STORAGE_COLUMNS)]


def _merge_kr_ohlcv_frames(existing: Optional[pd.DataFrame], new_frame: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        return new_frame.copy()

    merged_existing = existing.copy()
    merged_new = new_frame.copy()
    merged_existing["date"] = pd.to_datetime(merged_existing["date"], errors="coerce")
    merged_new["date"] = pd.to_datetime(merged_new["date"], errors="coerce")
    merged_existing = merged_existing.dropna(subset=["date"])
    merged_new = merged_new.dropna(subset=["date"])

    merged_existing["date_key"] = merged_existing["date"].dt.strftime("%Y-%m-%d")
    merged_new["date_key"] = merged_new["date"].dt.strftime("%Y-%m-%d")
    merged_existing = merged_existing[~merged_existing["date_key"].isin(merged_new["date_key"])].copy()

    merged = pd.concat([merged_existing, merged_new], ignore_index=True)
    merged = merged.sort_values("date").reset_index(drop=True)
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    merged = merged.drop(columns=["date_key"], errors="ignore")
    for column in KR_OHLCV_STORAGE_COLUMNS:
        if column not in merged.columns:
            if column == "adj_close":
                merged[column] = merged["close"]
            elif column in {"dividends", "stock_splits"}:
                merged[column] = 0.0
            elif column == "split_factor":
                merged[column] = 1.0
            else:
                merged[column] = pd.NA
    return merged[list(KR_OHLCV_STORAGE_COLUMNS)]


def _is_kr_rate_limit_error(message: str) -> bool:
    lowered = (message or "").lower()
    return any(
        token in lowered
        for token in (
            "429",
            "too many requests",
            "rate limit",
            "rate exceeded",
            "service unavailable",
            "temporarily unavailable",
            "try again later",
            "failed to establish a new connection",
            "max retries exceeded",
            "connection aborted",
            "connection pool",
            "read timed out",
        )
    )


def _is_kr_unavailable_error(message: str) -> bool:
    normalized = str(message or "").lower()
    return any(
        token in normalized
        for token in (
            "delisted",
            "no timezone found",
            "possibly delisted",
            "not found",
            "invalid ticker",
            "symbol may be delisted",
            "404",
            "no price data found",
        )
    )


def _normalize_kr_unavailable_reason(message: str) -> str:
    raw = str(message or "").strip()
    normalized = raw.lower()

    if "no timezone found" in normalized:
        return "possibly delisted; no timezone found"
    if "quote not found for symbol" in normalized:
        return "quote not found"
    if "no price data found" in normalized:
        return "possibly delisted; no price data found"
    if "symbol may be delisted" in normalized:
        return "possibly delisted; symbol may be delisted"
    if "invalid ticker" in normalized:
        return "invalid ticker"
    if "not found" in normalized or "404" in normalized:
        return "not found"
    compact = " ".join(raw.split())
    return compact[:120] if compact else "possibly delisted"


def _classify_kr_unavailable_reason(reason: str) -> str:
    normalized = str(reason or "").lower().strip()
    if normalized.startswith("possibly delisted;"):
        return "soft"
    return "hard"


def _format_kr_chunk_summary(chunk_num: int, total_chunks: int, statuses: List[str]) -> str:
    return format_us_style_chunk_summary(chunk_num, total_chunks, statuses)


def _collector_data_root_for_output_dir(output_dir: str) -> str:
    target = os.path.abspath(output_dir)
    if os.path.basename(target).lower() == "kr":
        return os.path.dirname(target)
    return target


def _load_kr_collector_run_state(path: str, *, as_of_date: str) -> dict:
    return load_collector_run_state(
        path,
        market="kr",
        as_of_date=as_of_date,
        cooldown_snapshot={},
    )


def _write_kr_collector_run_state(path: str, state: dict) -> None:
    write_collector_run_state(path, state, warned_paths=_KR_COLLECTOR_STATE_WRITE_WARNED)


def _record_kr_collector_symbol_status(state: dict, *, symbol: str, status: str) -> dict:
    return record_collector_symbol_status(
        state,
        symbol=symbol,
        status=status,
        completed_statuses=_KR_COLLECTOR_COMPLETED_STATUSES,
        retryable_statuses=_KR_COLLECTOR_RETRYABLE_STATUSES,
        cooldown_snapshot={},
    )


def collect_kr_ohlcv_csv(
    days: int = KR_OHLCV_DEFAULT_LOOKBACK_DAYS,
    include_kosdaq: bool = True,
    include_etf: bool = True,
    include_etn: bool = True,
    *,
    fdr_module=None,
    output_dir: Optional[str] = None,
    tickers: Optional[List[str]] = None,
    as_of: Optional[datetime] = None,
    max_failed_samples: int = 20,
    provider_mode: str = "yfinance_only",
) -> Dict[str, object]:
    """Collect KR OHLCV for all tickers and save canonical CSV files."""
    started_perf = time.perf_counter()
    diagnostics = CollectorDiagnostics()
    bootstrap_windows_utf8()
    bootstrap_yfinance_cache()
    requested_mode = str(provider_mode or "yfinance_only").strip().lower()
    normalized_provider_mode = "yfinance_only"
    if requested_mode not in {"", "yfinance_only", "yfinance", "yahoo"}:
        print(f"[KR] provider_mode={requested_mode} is ignored; using yfinance_only")
    fdr_client = fdr_module if fdr_module is not None else load_kr_fdr_module(required=False)

    target_dir = output_dir or DATA_KR_DIR
    ensure_dir(target_dir)

    end_dt = _resolve_market_day(as_of or datetime.now())
    end_yyyymmdd = end_dt.strftime("%Y%m%d")
    start_dt = end_dt - timedelta(days=days)
    start_yyyymmdd = start_dt.strftime("%Y%m%d")

    universe_errors: List[Dict[str, str]] = []
    with diagnostics.time_block("universe_resolve_seconds"):
        if tickers is not None:
            universe = sorted({str(ticker or "").strip().upper() for ticker in tickers if str(ticker or "").strip()})
        else:
            universe, universe_errors = _resolve_kr_universe(
                target_dir=target_dir,
                include_kosdaq=include_kosdaq,
                include_etf=include_etf,
                include_etn=include_etn,
                fdr_module=fdr_client,
            )
            for item in universe_errors:
                print(f"[KR] universe source: {item['source']}/{item['scope']} - {item['error']}")

    if not universe:
        if tickers is not None:
            raise RuntimeError("KR ticker universe is empty: explicit ticker override is empty")
        raise RuntimeError(
            "KR ticker universe is empty (no numeric symbols found in data/kr or data/stock_metadata_kr.csv)"
        )

    requested_universe = limit_runtime_symbols(universe)
    state_path = get_collector_run_state_path(
        "kr",
        data_dir=_collector_data_root_for_output_dir(target_dir),
    )
    with diagnostics.time_block("state_load_seconds"):
        run_state = _load_kr_collector_run_state(
            state_path,
            as_of_date=end_dt.date().isoformat(),
        )

    def write_state_snapshot() -> None:
        run_state["diagnostics_snapshot"] = diagnostics.snapshot()
        with diagnostics.time_block("state_write_seconds"):
            _write_kr_collector_run_state(state_path, run_state)

    run_state["last_progress_at"] = run_state.get("last_progress_at") or datetime.now().isoformat()
    write_state_snapshot()

    with diagnostics.time_block("universe_resolve_seconds"):
        universe = collector_tickers_for_run(
            requested_universe,
            run_state,
            skip_completed=False,
            terminal_statuses={"delisted"},
        )
    print(f"[KR] Runtime data collection scope - symbols={len(universe)}")

    status_counts = {
        "saved": 0,
        "latest": 0,
        "kept_existing": 0,
        "soft_unavailable": 0,
        "delisted": 0,
        "rate_limited": 0,
        "failed": 0,
    }
    failed_samples: List[Dict[str, str]] = []
    used_sources: set[str] = set()

    def process_ticker(
        ticker: str,
        *,
        fdr_primary_enabled: bool = True,
        allow_yahoo_fallback: bool = True,
    ) -> Tuple[str, Optional[str]]:
        _kr_collector_diagnostics_local.diagnostics = diagnostics
        symbol_prepare_started = time.perf_counter()
        out_path = os.path.join(target_dir, f"{ticker}.csv")
        existing: Optional[pd.DataFrame] = None
        last_date: Optional[pd.Timestamp] = None
        fetch_start_dt = start_dt
        overlap_count = 0

        if os.path.exists(out_path):
            try:
                existing = _read_existing_kr_ohlcv(out_path, ticker=ticker)
                if existing is None or existing.empty:
                    print(f"[KR] Empty file detected, collecting fresh data: {ticker}")
                    existing = None
                else:
                    existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
                    existing = existing.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
                    if not existing.empty:
                        cached_bar_count = int(existing["date"].dt.normalize().nunique())
                        if cached_bar_count < KR_OHLCV_TARGET_BARS:
                            fetch_start_dt = start_dt
                            last_date = existing["date"].max()
                            print(
                                f"[KR] History backfill active: {ticker} - cached bars={cached_bar_count}, "
                                f"refetching window from {fetch_start_dt.strftime('%Y-%m-%d')}"
                            )
                        else:
                            fetch_start_dt, last_date, overlap_count = _compute_kr_overlap_fetch_start(
                                existing["date"],
                                start_dt,
                            )
            except Exception as exc:
                print(f"[KR] Existing file read failed: {ticker} - {exc}")
                existing = None

        diagnostics.add_timing("symbol_prepare_seconds", time.perf_counter() - symbol_prepare_started)

        if fetch_start_dt.date() > end_dt.date():
            last_date_str = last_date.strftime("%Y-%m-%d") if last_date is not None else end_dt.strftime("%Y-%m-%d")
            print(f"[KR] Latest status: {ticker} (last data: {last_date_str})")
            return "latest", None

        if overlap_count > 0 and last_date is not None:
            print(
                f"[KR] Refresh overlap active: {ticker} - refetching last {overlap_count} stored bars "
                f"from {fetch_start_dt.strftime('%Y-%m-%d')}"
            )

        print(
            f"[DEBUG] {ticker}: collection start {fetch_start_dt.strftime('%Y-%m-%d')}, "
            f"end {end_dt.strftime('%Y-%m-%d')}"
        )

        saw_rate_limit = False
        last_error: Optional[str] = None

        for attempt in range(KR_OHLCV_MAX_RETRIES):
            print(
                f"[KR] 📊 데이터 요청 중: {ticker} "
                f"({fetch_start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')})"
            )
            normalized, provider_source, fetch_error = _fetch_kr_ohlcv_with_fallback(
                ticker=ticker,
                start_dt=fetch_start_dt,
                end_dt=end_dt,
                end_yyyymmdd=end_yyyymmdd,
                stock_client=None,
                fdr_module=fdr_client,
                provider_mode=normalized_provider_mode,
                fdr_primary_enabled=fdr_primary_enabled,
                allow_yahoo_fallback=allow_yahoo_fallback,
            )

            if provider_source == "needs_yahoo_fallback":
                return "needs_yahoo_fallback", fetch_error

            if provider_source != "unavailable":
                used_sources.add(provider_source.split(":", 1)[0])

            if not normalized.empty:
                before_len = len(existing) if existing is not None else 0
                merged = _merge_kr_ohlcv_frames(existing, normalized)
                merged = _trim_kr_ohlcv_window(merged, start_dt)
                with diagnostics.time_block("merge_write_seconds"):
                    merged.to_csv(out_path, index=False)

                if before_len > 0:
                    delta = len(merged) - before_len
                    if delta > 0:
                        print(f"[KR] ✅ 저장됨: {ticker} ({len(merged)} rows, +{delta})")
                    else:
                        print(f"[KR] 🔄 데이터 업데이트됨: {ticker} ({len(merged)} rows)")
                else:
                    print(f"[KR] ✅ 신규 저장: {ticker} ({len(merged)} rows)")
                return "saved", None

            last_error = fetch_error
            if fetch_error and _is_kr_rate_limit_error(fetch_error):
                saw_rate_limit = True
                wait = KR_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS
                extend_yahoo_cooldown("KR OHLCV", wait)
                print(
                    f"[KR] Rate limit detected: {ticker} - cooldown applied before retry"
                )
                if attempt < KR_OHLCV_MAX_RETRIES - 1:
                    print(
                        f"[KR] Retry scheduled after rate limit: {ticker} "
                        f"({attempt + 1}/{KR_OHLCV_MAX_RETRIES}, wait={wait:.0f}s)"
                    )
                    diagnostics.add_timing("retry_sleep_seconds", wait)
                    time.sleep(wait)
                    continue
                break

            if attempt < KR_OHLCV_MAX_RETRIES - 1:
                wait = KR_OHLCV_EMPTY_RETRY_DELAY_SECONDS * (attempt + 1)
                print(
                    f"[KR] Empty response, retry scheduled: {ticker} "
                    f"({attempt + 1}/{KR_OHLCV_MAX_RETRIES}, wait={wait:.0f}s)"
                )
                diagnostics.add_timing("retry_sleep_seconds", wait)
                time.sleep(wait)
                continue

        if saw_rate_limit:
            if existing is not None and len(existing) > 0:
                print(f"[KR] 🚫 API 제한으로 수집 보류, 기존 데이터 유지: {ticker}")
                return "rate_limited", last_error
            print(f"[KR] 🚫 API 제한으로 수집 보류, 다음 실행에서 재시도: {ticker}")
            return "rate_limited", last_error

        if last_error:
            if _is_kr_unavailable_error(last_error):
                reason = _normalize_kr_unavailable_reason(last_error)
                if _classify_kr_unavailable_reason(reason) == "soft":
                    print(f"[KR] ⚠️ Yahoo soft signal: {ticker} - {reason}")
                    if existing is not None and len(existing) > 0:
                        print(f"[KR] ⏩ Yahoo soft signal, 기존 데이터 유지: {ticker}")
                        return "kept_existing", last_error
                    print(f"[KR] ⚠️ 미확정 심볼 응답, 다음 실행에서 재시도: {ticker}")
                    return "soft_unavailable", last_error

                if existing is not None and len(existing) > 0:
                    print(f"[KR] 🚫 공급자 미지원/비활성 심볼: {ticker} - {reason}")
                    print(f"[KR] 🚫 비활성 심볼로 마킹: {ticker}")
                else:
                    print(f"[KR] 🚫 공급자 미지원/비활성 심볼: {ticker} - {reason}")

                empty_df = pd.DataFrame(columns=list(KR_OHLCV_STORAGE_COLUMNS))
                with diagnostics.time_block("merge_write_seconds"):
                    empty_df.to_csv(out_path, index=False)
                return "delisted", last_error

            if existing is not None and len(existing) > 0:
                print(f"[KR] ⏩ 새 데이터 없음, 기존 데이터 유지: {ticker}")
                return "kept_existing", last_error
            print(f"[KR] ❌ 오류 발생 ({ticker}): {last_error[:100]}")
            return "failed", last_error

        if existing is not None and len(existing) > 0:
            print(f"[KR] ❌ 빈 데이터: {ticker}")
            print(f"[KR] ⏩ 새 데이터 없음, 기존 데이터 유지: {ticker}")
            return "kept_existing", last_error

        print(f"[KR] ❌ 빈 데이터: {ticker}")
        print(f"[KR] ⚠️ 신규 데이터 수집 실패, 다음 실행에서 재시도: {ticker}")
        return "failed", last_error

    def record_result(ticker: str, status: str, error: Optional[str]) -> None:
        status_counts[status] = status_counts.get(status, 0) + 1
        _record_kr_collector_symbol_status(run_state, symbol=ticker, status=status)
        if status in {"failed", "rate_limited", "soft_unavailable", "delisted"} and error and len(failed_samples) < max_failed_samples:
            failed_samples.append({"ticker": ticker, "error": error})

    total_chunks = (len(universe) + KR_OHLCV_CHUNK_SIZE - 1) // KR_OHLCV_CHUNK_SIZE
    for chunk_index, offset in enumerate(range(0, len(universe), KR_OHLCV_CHUNK_SIZE), start=1):
        chunk = universe[offset : offset + KR_OHLCV_CHUNK_SIZE]
        print(format_us_style_chunk_start(chunk_index, total_chunks, chunk))

        chunk_statuses: List[str] = []
        fdr_workers = _kr_fdr_worker_count(len(chunk)) if fdr_client is not None else 1
        if fdr_workers > 1:
            with ThreadPoolExecutor(max_workers=fdr_workers) as executor:
                first_pass_results = list(
                    executor.map(
                        lambda ticker: process_ticker(
                            ticker,
                            fdr_primary_enabled=True,
                            allow_yahoo_fallback=False,
                        ),
                        chunk,
                    )
                )
            fallback_tickers: list[str] = []
            for ticker, (status, error) in zip(chunk, first_pass_results):
                if status == "needs_yahoo_fallback":
                    fallback_tickers.append(ticker)
                    diagnostics.increment("fdr_fallback_symbols")
                    continue
                chunk_statuses.append(status)
                record_result(ticker, status, error)
            for ticker in fallback_tickers:
                status, error = process_ticker(
                    ticker,
                    fdr_primary_enabled=False,
                    allow_yahoo_fallback=True,
                )
                chunk_statuses.append(status)
                record_result(ticker, status, error)
        else:
            for ticker in chunk:
                status, error = process_ticker(ticker)
                chunk_statuses.append(status)
                record_result(ticker, status, error)

        write_state_snapshot()
        print(_format_kr_chunk_summary(chunk_index, total_chunks, chunk_statuses))
        if chunk_index < total_chunks:
            if chunk_statuses and all(status == "latest" for status in chunk_statuses):
                print("[KR] Chunk pause skipped - all symbols already latest")
                continue
            pause = KR_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS if "rate_limited" in chunk_statuses else KR_OHLCV_CHUNK_PAUSE_SECONDS
            print(f"⏳ {pause:.1f}초 대기 중...")
            diagnostics.add_timing("chunk_pause_seconds", pause)
            time.sleep(pause)

    with diagnostics.time_block("index_benchmark_seconds"):
        benchmark_files = _collect_index_benchmarks(
            start_yyyymmdd=start_yyyymmdd,
            end_yyyymmdd=end_yyyymmdd,
            target_dir=target_dir,
            fdr_module=fdr_client,
        )

    source_label = "+".join(sorted(used_sources)) if used_sources else "yfinance"
    elapsed_seconds = time.perf_counter() - started_perf
    run_state["diagnostics_snapshot"] = diagnostics.snapshot()
    write_state_snapshot()
    summary = build_collector_summary(
        market="kr",
        as_of=end_yyyymmdd,
        requested_total=len(requested_universe),
        run_state=run_state,
        elapsed_seconds=elapsed_seconds,
        failed_samples=failed_samples,
        retryable_statuses=_KR_COLLECTOR_RETRYABLE_STATUSES,
        hard_failure_statuses={"failed"},
        extra={
            "source": source_label,
            "include_kosdaq": bool(include_kosdaq),
            "include_etf": bool(include_etf),
            "include_etn": bool(include_etn),
            "from": start_yyyymmdd,
            "to": end_yyyymmdd,
            "skipped_empty": int((run_state.get("status_counts") or {}).get("soft_unavailable", 0) or 0),
            "benchmark_files": benchmark_files,
            "data_dir": target_dir,
            "sources_used": sorted(used_sources),
            "provider_mode": normalized_provider_mode,
            "collector_state_path": state_path,
            "timings": diagnostics.timings(process_total_seconds=elapsed_seconds),
            "collector_diagnostics": diagnostics.snapshot(),
        },
    )
    return summary


if __name__ == "__main__":
    bootstrap_windows_utf8()
    bootstrap_yfinance_cache()
    summary = collect_kr_ohlcv_csv()
    print(summary)
