#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Mark Minervini Screener - 데이터 수집 모듈

import os
import csv
import json
import time
import threading
import logging
import re
import requests
import pandas as pd
import yfinance as yf
import yfinance.shared as yf_shared
from yfinance.exceptions import YFPricesMissingError, YFRateLimitError, YFTickerMissingError, YFTzMissingError
from datetime import date, datetime, timedelta, timezone as dt_timezone
from pytz import timezone
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set

from data_collectors.symbol_universe import (
    _is_us_collectable_symbol,
    _read_symbol_from_existing_csv,
    load_us_symbol_universe,
    sync_official_us_symbol_directory,
)
from utils.calc_utils import get_us_market_today
from utils.collector_diagnostics import CollectorDiagnostics
from utils.collector_run_state import (
    build_collector_summary,
    collector_status_counts,
    collector_tickers_for_run,
    collector_timestamp,
    default_collector_run_state,
    load_collector_run_state,
    record_collector_symbol_status,
    write_collector_run_state,
)
from utils.console_runtime import bootstrap_windows_utf8
from utils.io_utils import ensure_dir, safe_filename
from utils.market_runtime import get_collector_run_state_path, limit_runtime_symbols
from utils.ohlcv_progress import format_us_style_chunk_start, format_us_style_chunk_summary
from utils.yfinance_runtime import bootstrap_yfinance_cache
from utils.yahoo_throttle import (
    extend_yahoo_cooldown,
    get_yahoo_throttle_state,
    record_yahoo_request_failure,
    record_yahoo_request_success,
    wait_for_yahoo_request_slot,
)

from config import (
    DATA_DIR, DATA_US_DIR, RESULTS_DIR, STOCK_METADATA_PATH
)

_COLLECTOR_STATE_WRITE_WARNED: Set[str] = set()

# 주식 심볼 수집 (NASDAQ API 제거됨 - 타임아웃 문제로 인해)
# 대신 로컬 심볼 소스(csv) + 기존 데이터 디렉터리를 합쳐 전체 유니버스를 구성한다.

US_ALWAYS_INCLUDE_SYMBOLS = {
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


def _env_int_default(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


US_OHLCV_BATCH_SIZE = _env_int_default("INVEST_PROTO_US_OHLCV_BATCH_SIZE", 8)
US_OHLCV_CHUNK_SIZE = _env_int_default("INVEST_PROTO_US_OHLCV_CHUNK_SIZE", max(1, US_OHLCV_BATCH_SIZE))
US_OHLCV_MAX_WORKERS = 1
US_OHLCV_REQUEST_DELAY_SECONDS = 1.0
US_OHLCV_CHUNK_PAUSE_SECONDS = US_OHLCV_REQUEST_DELAY_SECONDS
US_OHLCV_MAX_RETRIES = 2
US_OHLCV_EMPTY_RETRY_DELAY_SECONDS = 3.0
US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS = 45.0
US_OHLCV_REFRESH_OVERLAP_BARS = 2
US_OHLCV_TARGET_BARS = 330
US_OHLCV_DEFAULT_LOOKBACK_DAYS = 520
US_OHLCV_STORAGE_COLUMNS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "dividends",
    "stock_splits",
    "split_factor",
]

_us_rate_limit_lock = threading.Lock()
_us_collector_diagnostics_local = threading.local()
_us_rate_limit_cooldown_until = 0.0
_yfinance_logger_configured = False
_US_COLLECTOR_COMPLETED_STATUSES = {"saved", "latest", "kept_existing", "delisted"}
_US_COLLECTOR_RETRYABLE_STATUSES = {
    "failed",
    "partial",
    "rate_limited",
    "soft_unavailable",
}


class RateLimitError(RuntimeError):
    """Raised when the upstream provider throttles the request."""


class DelistedSymbolError(RuntimeError):
    """Raised when the upstream provider indicates the symbol is unavailable."""

    def __init__(self, error_msg: str):
        self.raw_error_msg = str(error_msg)
        self.reason = _normalize_us_delisted_reason(self.raw_error_msg)
        self.classification = _classify_us_unavailable_reason(self.reason)
        super().__init__(self.reason)

    @property
    def is_soft(self) -> bool:
        return self.classification == "soft"


def _configure_yfinance_logger() -> None:
    global _yfinance_logger_configured

    if _yfinance_logger_configured:
        return

    yf.config.debug.hide_exceptions = False
    logger = logging.getLogger("yfinance")
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    logger.propagate = False
    logger.setLevel(logging.CRITICAL)
    _yfinance_logger_configured = True


def _list_symbols_from_existing_us_csv() -> Set[str]:
    symbols: Set[str] = set()
    if not os.path.isdir(DATA_US_DIR):
        return symbols

    for name in os.listdir(DATA_US_DIR):
        if not name.endswith(".csv"):
            continue
        path = os.path.join(DATA_US_DIR, name)
        base_name = os.path.splitext(name)[0].strip().upper()
        symbol = base_name
        if not _is_us_collectable_symbol(base_name) or base_name.endswith("_FILE"):
            symbol = _read_symbol_from_existing_csv(path).strip().upper() or base_name
        if symbol and _is_us_collectable_symbol(symbol):
            symbols.add(symbol)
    return symbols


def _read_symbols_from_csv(path: str) -> Set[str]:
    symbols: Set[str] = set()
    if not os.path.exists(path):
        return symbols

    try:
        frame = pd.read_csv(path)
        if frame is None or frame.empty:
            return symbols
    except Exception as e:
        print(f"⚠️ CSV 종목 로드 실패 ({path}): {e}")
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
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        symbol = str(raw).strip()
        if symbol:
            symbols.add(symbol)

    return symbols


def _load_us_symbol_universe(progress=None) -> Set[str]:
    return set(
        load_us_symbol_universe(
            data_dir=DATA_DIR,
            us_data_dir=DATA_US_DIR,
            stock_metadata_path=STOCK_METADATA_PATH,
            progress=progress,
        )
    )


def _is_us_rate_limit_error(error_msg: str) -> bool:
    normalized = error_msg.lower()
    return (
        "rate limit" in normalized
        or "429" in normalized
        or "too many requests" in normalized
        or "try after a while" in normalized
    )


def _is_us_delisted_error(error_msg: str) -> bool:
    normalized = error_msg.lower()
    delisted_keywords = [
        "delisted",
        "no timezone found",
        "possibly delisted",
        "not found",
        "invalid ticker",
        "symbol may be delisted",
        "404",
    ]
    return any(keyword in normalized for keyword in delisted_keywords)


def _normalize_us_delisted_reason(error_msg: str) -> str:
    raw = str(error_msg).strip()
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

    compact = re.sub(r"\s+", " ", raw)
    compact = re.sub(r"^\$[A-Z0-9.\-^]+:\s*", "", compact, flags=re.IGNORECASE)
    return compact[:120] if compact else "possibly delisted"


def _classify_us_unavailable_reason(reason: str) -> str:
    normalized = str(reason).lower().strip()

    if normalized.startswith("possibly delisted;"):
        return "soft"

    return "hard"


def _format_us_chunk_summary(chunk_num: int, total_chunks: int, statuses: list[str]) -> str:
    return format_us_style_chunk_summary(chunk_num, total_chunks, statuses)


def _compute_us_overlap_start_date(
    existing_dates: pd.Series,
    fallback_start: date,
    *,
    overlap_bars: int = US_OHLCV_REFRESH_OVERLAP_BARS,
) -> tuple[date, date | None, int]:
    dates = pd.to_datetime(existing_dates, utc=True, errors="coerce").dropna()
    if dates.empty:
        return fallback_start, None, 0

    unique_dates = sorted({value.date() for value in dates.tolist()})
    if not unique_dates:
        return fallback_start, None, 0

    last_date = unique_dates[-1]
    overlap_count = max(1, min(int(overlap_bars), len(unique_dates)))
    start_date = unique_dates[-overlap_count]
    return start_date, last_date, overlap_count


def _default_us_fetch_start(today: date) -> date:
    return today - timedelta(days=US_OHLCV_DEFAULT_LOOKBACK_DAYS)


def _extend_us_rate_limit_cooldown(seconds: float) -> None:
    global _us_rate_limit_cooldown_until

    if seconds <= 0:
        return

    with _us_rate_limit_lock:
        target = time.monotonic() + seconds
        if target > _us_rate_limit_cooldown_until:
            _us_rate_limit_cooldown_until = target

    extend_yahoo_cooldown("US OHLCV", seconds)


def _active_us_collector_diagnostics() -> CollectorDiagnostics | None:
    diagnostics = getattr(_us_collector_diagnostics_local, "diagnostics", None)
    return diagnostics if isinstance(diagnostics, CollectorDiagnostics) else None


def _record_us_provider_wait(seconds: float) -> None:
    diagnostics = _active_us_collector_diagnostics()
    if diagnostics is not None:
        diagnostics.add_timing("provider_wait_seconds", seconds)


def _wait_for_us_rate_limit_cooldown() -> float:
    waited = 0.0
    with _us_rate_limit_lock:
        remaining = _us_rate_limit_cooldown_until - time.monotonic()

    if remaining > 0:
        print(f"[US] cooldown applied: {remaining:.1f}s")
        time.sleep(remaining)
        waited += max(0.0, float(remaining))

    slot_wait = wait_for_yahoo_request_slot("US OHLCV", min_interval=US_OHLCV_REQUEST_DELAY_SECONDS)
    waited += max(0.0, float(slot_wait or 0.0))
    _record_us_provider_wait(waited)
    return waited


def _pop_yfinance_error(ticker: str) -> str | None:
    return yf_shared._ERRORS.pop(str(ticker).upper(), None)


def _collector_timestamp() -> str:
    return collector_timestamp()


def _default_collector_run_state(*, market: str, as_of_date: str) -> dict:
    return default_collector_run_state(
        market=market,
        as_of_date=as_of_date,
        cooldown_snapshot=get_yahoo_throttle_state(),
    )


def _write_collector_run_state(path: str, state: dict) -> None:
    write_collector_run_state(path, state, warned_paths=_COLLECTOR_STATE_WRITE_WARNED)


def _load_collector_run_state(path: str, *, market: str, as_of_date: str) -> dict:
    return load_collector_run_state(
        path,
        market=market,
        as_of_date=as_of_date,
        cooldown_snapshot=get_yahoo_throttle_state(),
    )


def _collector_status_counts(symbol_statuses: dict[str, str]) -> dict[str, int]:
    return collector_status_counts(symbol_statuses)


def _collector_tickers_for_run(
    tickers: list[str],
    state: dict,
    *,
    skip_completed: bool = True,
    terminal_statuses: set[str] | None = None,
) -> list[str]:
    return collector_tickers_for_run(
        tickers,
        state,
        skip_completed=skip_completed,
        terminal_statuses=terminal_statuses,
    )


def _record_collector_symbol_status(state: dict, *, symbol: str, status: str) -> dict:
    return record_collector_symbol_status(
        state,
        symbol=symbol,
        status=status,
        completed_statuses=_US_COLLECTOR_COMPLETED_STATUSES,
        retryable_statuses=_US_COLLECTOR_RETRYABLE_STATUSES,
        cooldown_snapshot=get_yahoo_throttle_state(),
    )


def update_symbol_list() -> Set[str]:
    """Refresh the US symbol list from local files and seed sources."""
    print("\n[US] Symbol list refresh started")

    existing_symbols = _list_symbols_from_existing_us_csv()
    print(f"[US] Existing symbol count: {len(existing_symbols)}")
    print("[Universe] US symbol universe build started")

    try:
        sync_official_us_symbol_directory(
            data_dir=DATA_DIR,
            progress=lambda message: print(message, flush=True),
        )
    except Exception as exc:
        print(f"[Universe] Official seed sync skipped - reason={exc}")

    new_symbols = _load_us_symbol_universe(progress=lambda message: print(message, flush=True))

    truly_new_symbols = new_symbols - existing_symbols

    if truly_new_symbols:
        print(f"[US] Newly discovered symbols: {len(truly_new_symbols)}")
        for symbol in sorted(truly_new_symbols):
            try:
                safe_symbol = safe_filename(symbol)
                csv_path = os.path.join(DATA_US_DIR, f"{safe_symbol}.csv")
                if not os.path.exists(csv_path):
                    empty_df = pd.DataFrame(columns=US_OHLCV_STORAGE_COLUMNS)
                    empty_df.to_csv(csv_path, index=False)
                    print(f"[US] Created placeholder CSV: {symbol}")
            except Exception as e:
                print(f"[US] Placeholder CSV creation failed: {symbol} - {e}")
    else:
        print("[US] No newly discovered symbols")

    all_symbols = existing_symbols.union(new_symbols)
    print(f"[Universe] US symbol universe ready - existing={len(existing_symbols)}, seed_total={len(new_symbols)}, final={len(all_symbols)}")
    print(f"[US] Total symbol count: {len(all_symbols)}")
    return all_symbols


def fetch_us_single(ticker, start, end):
    """yfinance API를 사용하여 단일 종목의 주가 데이터를 가져오는 함수
    
    Args:
        ticker: 주식 티커 심볼
        start: 시작 날짜
        end: 종료 날짜
        
    Returns:
        DataFrame: 주가 데이터 또는 None(오류 발생 시)
    """
    try:
        bootstrap_windows_utf8()
        bootstrap_yfinance_cache()
        _configure_yfinance_logger()

        # 공용 Yahoo throttle이 요청 간격을 보장한다.
        _wait_for_us_rate_limit_cooldown()
        
        # 타임아웃 설정 추가
        print(f"[US] 📊 데이터 요청 중: {ticker} ({start} ~ {end})")
        ticker_obj = yf.Ticker(ticker)
        _pop_yfinance_error(ticker)
        
        # 주가 데이터 가져오기
        df = ticker_obj.history(start=start, end=end, interval="1d",
                               auto_adjust=False, actions=True, timeout=10)

        yf_error = _pop_yfinance_error(ticker)
        if yf_error:
            normalized = yf_error.lower()
            if _is_us_rate_limit_error(normalized):
                raise RateLimitError(yf_error)
            if _is_us_delisted_error(normalized):
                raise DelistedSymbolError(yf_error)
            print(f"[US] ⚠️ yfinance 오류 응답: {ticker} - {yf_error}")
            return None
        
        if df.empty:
            print(f"[US] ❌ 빈 데이터 반환됨: {ticker}")
            return pd.DataFrame(columns=US_OHLCV_STORAGE_COLUMNS)
            
        print(f"[US] ✅ 데이터 수신 성공: {ticker} ({len(df)} 행)")
        record_yahoo_request_success("US OHLCV")
        df = df.rename_axis("date").reset_index()
        df["symbol"] = ticker
        return df
    except YFRateLimitError as e:
        raise RateLimitError(str(e)) from e
    except (YFTzMissingError, YFPricesMissingError, YFTickerMissingError) as e:
        raise DelistedSymbolError(str(e)) from e
    except requests.exceptions.ReadTimeout:
        record_yahoo_request_failure("US OHLCV", reason="timeout")
        print(f"[US] ⏱️ 타임아웃 발생: {ticker} - API 응답 지연 (재시도 필요)")
        return None
    except requests.exceptions.ConnectionError:
        record_yahoo_request_failure("US OHLCV", reason="connection_error")
        print(f"[US] 🌐 연결 오류: {ticker} - 네트워크 문제 발생 (재시도 필요)")
        return None
    except Exception as e:
        error_msg = str(e)
        normalized = error_msg.lower()
        if _is_us_delisted_error(normalized):
            raise DelistedSymbolError(error_msg) from e
        if _is_us_rate_limit_error(normalized):
            raise RateLimitError(error_msg) from e

        record_yahoo_request_failure("US OHLCV", reason="error")
        print(f"[US] ❌ 오류 발생 ({ticker}): {normalized[:100]}")
        return None

# 미국 주식 데이터 수집 및 저장 (병렬 처리 버전)
# Batch prefetch keeps same-window live collection on one Yahoo request when possible.
def _fetch_us_batch_ohlcv(symbols, start, end) -> dict[str, pd.DataFrame]:
    """Fetch a same-window US OHLCV batch through yfinance with single-symbol fallback upstream."""
    normalized_symbols = [
        str(symbol or "").strip().upper()
        for symbol in list(symbols or [])
        if str(symbol or "").strip()
    ]
    normalized_symbols = list(dict.fromkeys(normalized_symbols))
    if not normalized_symbols:
        return {}

    bootstrap_windows_utf8()
    bootstrap_yfinance_cache()
    _configure_yfinance_logger()
    _wait_for_us_rate_limit_cooldown()

    print(
        f"[US] Batch OHLCV request: symbols={len(normalized_symbols)} "
        f"({start} ~ {end})"
    )
    provider_started = time.perf_counter()
    try:
        history = yf.download(
            normalized_symbols,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=False,
            actions=True,
            group_by="ticker",
            threads=True,
            progress=False,
            timeout=10,
        )
        diagnostics = _active_us_collector_diagnostics()
        if diagnostics is not None:
            diagnostics.add_timing("provider_fetch_seconds", time.perf_counter() - provider_started)
    except YFRateLimitError as exc:
        diagnostics = _active_us_collector_diagnostics()
        if diagnostics is not None:
            diagnostics.add_timing("provider_fetch_seconds", time.perf_counter() - provider_started)
        raise RateLimitError(str(exc)) from exc
    except Exception as exc:
        diagnostics = _active_us_collector_diagnostics()
        if diagnostics is not None:
            diagnostics.add_timing("provider_fetch_seconds", time.perf_counter() - provider_started)
        normalized = str(exc).lower()
        if _is_us_rate_limit_error(normalized):
            raise RateLimitError(str(exc)) from exc
        print(f"[US] Batch OHLCV failed, falling back to single requests: {normalized[:120]}")
        return {}

    if history is None or history.empty:
        return {}

    fetched: dict[str, pd.DataFrame] = {}
    for symbol in normalized_symbols:
        yf_error = _pop_yfinance_error(symbol)
        if yf_error and _is_us_rate_limit_error(yf_error.lower()):
            raise RateLimitError(yf_error)

        frame = pd.DataFrame()
        try:
            if isinstance(history.columns, pd.MultiIndex):
                level0_values = {str(value).upper() for value in history.columns.get_level_values(0)}
                level1_values = {str(value).upper() for value in history.columns.get_level_values(1)}
                if symbol in level0_values:
                    frame = history[symbol]
                elif symbol in level1_values:
                    frame = history.xs(symbol, axis=1, level=1)
            elif len(normalized_symbols) == 1:
                frame = history
        except Exception:
            frame = pd.DataFrame()

        if frame is None or frame.empty:
            continue
        frame = frame.dropna(how="all").copy()
        if frame.empty:
            continue
        frame = frame.rename_axis("date").reset_index()
        frame["symbol"] = symbol
        fetched[symbol] = frame
    if fetched:
        record_yahoo_request_success("US OHLCV")
    return fetched


def fetch_and_save_us_ohlcv_chunked(
    tickers,
    save_dir=DATA_US_DIR,
    chunk_size=US_OHLCV_CHUNK_SIZE,
    pause=US_OHLCV_CHUNK_PAUSE_SECONDS,
    start_chunk=0,
    max_chunks=None,
    max_workers=US_OHLCV_MAX_WORKERS,
):
    started_perf = time.perf_counter()
    diagnostics = CollectorDiagnostics()
    ensure_dir(save_dir)
    today = get_us_market_today()
    fetch_end = today + timedelta(days=1)
    as_of_date = today.isoformat()
    state_path = get_collector_run_state_path("us", data_dir=DATA_DIR)
    with diagnostics.time_block("state_load_seconds"):
        run_state = _load_collector_run_state(
            state_path,
            market="us",
            as_of_date=as_of_date,
        )
    with diagnostics.time_block("universe_resolve_seconds"):
        requested_tickers = [
            str(ticker).strip().upper()
            for ticker in list(tickers or [])
            if str(ticker).strip()
        ]
        tickers = _collector_tickers_for_run(
            requested_tickers,
            run_state,
            skip_completed=False,
            terminal_statuses={"delisted"},
        )

    def write_state_snapshot() -> None:
        run_state["diagnostics_snapshot"] = diagnostics.snapshot()
        with diagnostics.time_block("state_write_seconds"):
            _write_collector_run_state(state_path, run_state)

    run_state["cooldown_snapshot"] = dict(get_yahoo_throttle_state())
    run_state["last_progress_at"] = _collector_timestamp()
    write_state_snapshot()
    batch_prefetch: dict[str, pd.DataFrame] = {}

    # 총 청크 수 계산
    total_chunks = (len(tickers) + chunk_size - 1) // chunk_size
    if max_chunks is not None:
        total_chunks = min(total_chunks, start_chunk + max_chunks)

    # 시작 청크 설정
    if start_chunk > 0:
        print(f"🔄 청크 {start_chunk}부터 다시 시작합니다 (총 {total_chunks} 청크)")
    
    # 티커 처리 함수 정의 (병렬 처리용)
    ticker_prepare_cache: dict[str, dict[str, object]] = {}

    def _prepare_us_ticker_cached(ticker) -> dict[str, object]:
        symbol_key = str(ticker or "").strip().upper()
        cached = ticker_prepare_cache.get(symbol_key)
        if cached is not None:
            return cached

        symbol_prepare_started = time.perf_counter()
        safe_ticker = safe_filename(ticker)
        path = os.path.join(save_dir, f"{safe_ticker}.csv")
        existing = None
        last_date = None
        overlap_count = 0
        if os.path.exists(path):
            try:
                existing = pd.read_csv(path)
                if "date" not in existing.columns:
                    raise ValueError("missing 'date' column")
                existing["date"] = pd.to_datetime(existing["date"], utc=True, errors="coerce")
                invalid_date_rows = int(existing["date"].isna().sum())
                if invalid_date_rows > 0:
                    print(f"[US] Dropping invalid cached date rows: {ticker} ({invalid_date_rows} rows)")
                    existing = existing.dropna(subset=["date"]).reset_index(drop=True)
                if len(existing) == 0:
                    print(f"[US] Empty or invalid-dated file detected, collecting fresh data: {ticker}")
                    existing = None
                    start_date = _default_us_fetch_start(today)
                else:
                    existing = existing.sort_values("date", ascending=True).reset_index(drop=True)
                    if len(existing) > US_OHLCV_TARGET_BARS:
                        print(f"[US] Trim existing window: {ticker} ({len(existing)} -> {US_OHLCV_TARGET_BARS} rows)")
                        existing = existing.sort_values("date", ascending=False).head(US_OHLCV_TARGET_BARS).reset_index(drop=True)
                        existing = existing.sort_values("date", ascending=True).reset_index(drop=True)
                    cached_bar_count = int(existing["date"].dt.normalize().nunique())
                    if cached_bar_count < US_OHLCV_TARGET_BARS:
                        start_date = _default_us_fetch_start(today)
                        last_date = existing["date"].max().date()
                        print(
                            f"[US] History backfill active: {ticker} - cached bars={cached_bar_count}, "
                            f"refetching window from {start_date.isoformat()}"
                        )
                    else:
                        start_date, last_date, overlap_count = _compute_us_overlap_start_date(
                            existing["date"],
                            _default_us_fetch_start(today),
                        )
            except Exception as e:
                print(f"[US] Existing file read failed: {ticker} - {e}")
                existing = None
                start_date = _default_us_fetch_start(today)
        else:
            start_date = _default_us_fetch_start(today)

        diagnostics.add_timing("symbol_prepare_seconds", time.perf_counter() - symbol_prepare_started)
        prepared = {
            "path": path,
            "existing": existing,
            "start_date": start_date,
            "last_date": last_date,
            "overlap_count": overlap_count,
        }
        ticker_prepare_cache[symbol_key] = prepared
        return prepared

    def process_ticker(ticker):
        prepared = _prepare_us_ticker_cached(ticker)
        path = str(prepared["path"])
        existing = prepared.get("existing")
        start_date = prepared["start_date"]
        last_date = prepared.get("last_date")
        overlap_count = int(prepared.get("overlap_count") or 0)
        if start_date > today:
            last_date_str = last_date.isoformat() if last_date is not None else today.isoformat()
            print(f"[US] Latest status: {ticker} (last data: {last_date_str})")
            return "latest"

        if overlap_count > 0 and last_date is not None:
            print(
                f"[US] Refresh overlap active: {ticker} - refetching last {overlap_count} stored bars "
                f"from {start_date.isoformat()}"
            )

        print(f"[DEBUG] {ticker}: collection start {start_date}, end {fetch_end}")
        df_new = None
        saw_rate_limit = False
        batch_frame = batch_prefetch.get(str(ticker).strip().upper())
        if batch_frame is not None and not batch_frame.empty:
            df_new = batch_frame.copy()
        for j in range(US_OHLCV_MAX_RETRIES):  # 재시도 횟수 감소
            if df_new is not None and not df_new.empty:
                break
            try:
                diagnostics.increment("single_fetches")
                previous_diagnostics = getattr(_us_collector_diagnostics_local, "diagnostics", None)
                _us_collector_diagnostics_local.diagnostics = diagnostics
                wait_before = diagnostics.get_timing("provider_wait_seconds")
                provider_started = time.perf_counter()
                try:
                    df_new = fetch_us_single(ticker, start=start_date, end=fetch_end)
                finally:
                    elapsed = time.perf_counter() - provider_started
                    wait_delta = diagnostics.get_timing("provider_wait_seconds") - wait_before
                    diagnostics.add_timing("provider_fetch_seconds", max(0.0, elapsed - wait_delta))
                    if previous_diagnostics is None:
                        try:
                            delattr(_us_collector_diagnostics_local, "diagnostics")
                        except AttributeError:
                            pass
                    else:
                        _us_collector_diagnostics_local.diagnostics = previous_diagnostics

                if df_new is not None and df_new.empty:
                    if existing is not None and len(existing) > 0:
                        print(f"[US] ⏩ 새 데이터 없음, 기존 데이터 유지: {ticker}")
                        return "kept_existing"
                    if j < US_OHLCV_MAX_RETRIES - 1:
                        wait = US_OHLCV_EMPTY_RETRY_DELAY_SECONDS * (j + 1)
                        print(
                            f"[US] ⚠️ {ticker} 빈 데이터 반환, 재시도 {j+1}/{US_OHLCV_MAX_RETRIES} "
                            f"({wait:.0f}s 대기)"
                        )
                        diagnostics.add_timing("retry_sleep_seconds", wait)
                        time.sleep(wait)
                        continue

                    df_new = None
                    break

                if df_new is not None and not df_new.empty:
                    break

                if j < US_OHLCV_MAX_RETRIES - 1:
                    wait = US_OHLCV_EMPTY_RETRY_DELAY_SECONDS * (j + 1)
                    print(
                        f"[US] ⚠️ {ticker} 빈 데이터 반환, 재시도 {j+1}/{US_OHLCV_MAX_RETRIES} "
                        f"({wait:.0f}s 대기)"
                    )
                    diagnostics.add_timing("retry_sleep_seconds", wait)
                    time.sleep(wait)
            except RateLimitError:
                saw_rate_limit = True
                wait = US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS
                diagnostics.add_timing("retry_sleep_seconds", wait)
                _extend_us_rate_limit_cooldown(wait)
                print(
                    f"[US] 🚫 API 제한 응답: {ticker} | 재시도 {j+1}/{US_OHLCV_MAX_RETRIES} "
                    f"(cooldown {wait:.0f}s)"
                )
                continue
            except DelistedSymbolError as e:
                if e.is_soft:
                    print(f"[US] ⚠️ Yahoo soft signal: {ticker} - {e.reason}")
                    if existing is not None and len(existing) > 0:
                        print(f"[US] ⏩ Yahoo soft signal, 기존 데이터 유지: {ticker}")
                        return "kept_existing"

                    print(f"[US] ⚠️ 미확정 심볼 응답, 다음 실행에서 재시도: {ticker}")
                    return "soft_unavailable"

                if existing is not None and len(existing) > 0:
                    print(f"[US] 🚫 공급자 미지원/비활성 심볼: {ticker} - {e.reason}")
                    print(f"[US] 🚫 비활성 심볼로 마킹: {ticker}")
                else:
                    print(f"[US] 🚫 공급자 미지원/비활성 심볼: {ticker} - {e.reason}")

                empty_df = pd.DataFrame(columns=US_OHLCV_STORAGE_COLUMNS)
                with diagnostics.time_block("merge_write_seconds"):
                    empty_df.to_csv(path, index=False)
                return "delisted"
            except Exception as e:
                error_msg = str(e).lower()
                wait = 2 ** j + 2
                print(
                    f"[US] ⚠️ {ticker} 재시도 {j+1}/{US_OHLCV_MAX_RETRIES} "
                    f"→ {wait}s 대기: {error_msg[:100]}"
                )
                if j < US_OHLCV_MAX_RETRIES - 1:
                    diagnostics.add_timing("retry_sleep_seconds", wait)
                    time.sleep(wait)

        if df_new is None or df_new.empty:
            if saw_rate_limit:
                if existing is not None and len(existing) > 0:
                    print(f"[US] 🚫 API 제한으로 수집 보류, 기존 데이터 유지: {ticker}")
                else:
                    print(f"[US] 🚫 API 제한으로 수집 보류, 다음 실행에서 재시도: {ticker}")
                return "rate_limited"

            print(f"[US] ❌ 빈 데이터: {ticker}")
            if existing is not None and len(existing) > 0:
                print(f"[US] ⏩ 새 데이터 없음, 기존 데이터 유지: {ticker}")
                return "kept_existing"

            print(f"[US] ⚠️ 신규 데이터 수집 실패, 다음 실행에서 재시도: {ticker}")
            return "failed"

        if "date" in df_new.columns:
            df_new = df_new.copy()
            df_new["date"] = pd.to_datetime(df_new["date"], utc=True, errors="coerce")
            df_new = df_new.dropna(subset=["date"])
            df_new = df_new[df_new["date"].dt.date <= today].reset_index(drop=True)
            if df_new.empty:
                if existing is not None and len(existing) > 0:
                    print(f"[US] No completed bars through {as_of_date}, keeping existing data: {ticker}")
                    return "kept_existing"
                print(f"[US] No completed bars through {as_of_date}: {ticker}")
                return "failed"

        if existing is not None:
            before_len = len(existing)
            
            # 날짜 형식 통일 (모든 날짜를 UTC 시간대로 변환)
            if not isinstance(existing["date"].dtype, pd.DatetimeTZDtype):
                existing["date"] = pd.to_datetime(existing["date"], utc=True)
            if not isinstance(df_new["date"].dtype, pd.DatetimeTZDtype):
                df_new["date"] = pd.to_datetime(df_new["date"], utc=True)
                
            # 날짜 문자열 형식으로 변환하여 중복 제거 (시간대 문제 해결)
            existing["date_str"] = existing["date"].dt.strftime("%Y-%m-%d")
            df_new["date_str"] = df_new["date"].dt.strftime("%Y-%m-%d")
            
            # 기존 데이터에서 새 데이터와 중복되는 날짜 제거
            existing_filtered = existing[~existing["date_str"].isin(df_new["date_str"])]
            
            # 데이터 병합
            df_combined = pd.concat([existing_filtered, df_new], ignore_index=True)
            
            # 임시 컬럼 제거
            df_combined.drop("date_str", axis=1, inplace=True)
            
            # 330 영업일 제한 적용 (데이터 병합 후 다시 확인)
            if len(df_combined) > US_OHLCV_TARGET_BARS:
                print(f"[US] ✂️ {ticker}: 병합 후 {US_OHLCV_TARGET_BARS} 영업일 초과 데이터 정리 중 ({len(df_combined)} → {US_OHLCV_TARGET_BARS})")
                df_combined = df_combined.sort_values("date", ascending=False).head(US_OHLCV_TARGET_BARS).reset_index(drop=True)
            
            # 최종 저장 전 오래된 데이터가 위에 오도록 정렬
            df_combined = df_combined.sort_values("date", ascending=True).reset_index(drop=True)
            
            after_len = len(df_combined)

            # 항상 저장하여 데이터 업데이트 보장
            with diagnostics.time_block("merge_write_seconds"):
                df_combined.to_csv(path, index=False)
            if after_len > before_len:
                print(f"[US] ✅ 저장됨: {ticker} ({after_len} rows, +{after_len - before_len})")
            else:
                print(f"[US] 🔄 데이터 업데이트됨: {ticker} ({after_len} rows)")
            return "saved"
        else:
            # 신규 데이터도 330 영업일 제한 적용
            if len(df_new) > US_OHLCV_TARGET_BARS:
                print(f"[US] ✂️ {ticker}: 신규 데이터 {US_OHLCV_TARGET_BARS} 영업일 초과 정리 중 ({len(df_new)} → {US_OHLCV_TARGET_BARS})")
                df_new = df_new.sort_values("date", ascending=False).head(US_OHLCV_TARGET_BARS).reset_index(drop=True)
            
            # 최종 저장 전 오래된 데이터가 위에 오도록 정렬
            df_new = df_new.sort_values("date", ascending=True).reset_index(drop=True)
                
            with diagnostics.time_block("merge_write_seconds"):
                df_new.to_csv(path, index=False)
            print(f"[US] ✅ 신규 저장: {ticker} ({len(df_new)} rows)")
            return "saved"
    
    # 청크별 처리 시작
    failed_samples: list[dict[str, str]] = []

    def _batch_fetch_start_for_ticker(ticker: str) -> date | None:
        return _prepare_us_ticker_cached(ticker).get("start_date")  # type: ignore[return-value]

    for i in range(start_chunk * chunk_size, len(tickers), chunk_size):
        chunk_num = i // chunk_size
        
        # 최대 청크 수 제한 확인
        if max_chunks is not None and chunk_num >= start_chunk + max_chunks:
            print(f"🛑 최대 청크 수 ({max_chunks}) 도달. 작업 중단.")
            break
            
        chunk = tickers[i:i+chunk_size]
        
        # NaN 값 필터링
        chunk = [t for t in chunk if isinstance(t, str) or (isinstance(t, (int, float)) and not pd.isna(t))]
        
        print(f"\n⏱️ Chunk {chunk_num + 1}/{total_chunks} 시작 ({len(chunk)}개): {chunk}")

        # 청크 내 티커 병렬 처리
        batch_prefetch = {}
        chunk_counts_before = diagnostics.snapshot().get("counts", {})
        try:
            batch_size = max(0, int(US_OHLCV_BATCH_SIZE))
        except (TypeError, ValueError):
            batch_size = 0
        if batch_size > 1:
            batch_groups: dict[tuple[date, date], list[str]] = {}
            for ticker in chunk:
                symbol = str(ticker).strip().upper()
                if not symbol:
                    continue
                start_date = _batch_fetch_start_for_ticker(symbol)
                if start_date is None or start_date > today:
                    continue
                batch_groups.setdefault((start_date, fetch_end), []).append(symbol)
            stop_batching = False
            for (group_start, group_end), group_symbols in batch_groups.items():
                if len(group_symbols) < 2:
                    continue
                for offset in range(0, len(group_symbols), batch_size):
                    batch_symbols = group_symbols[offset:offset + batch_size]
                    if not batch_symbols:
                        continue
                    try:
                        diagnostics.increment("batch_prefetch_requests")
                        diagnostics.increment("batch_prefetch_symbols", len(batch_symbols))
                        previous_diagnostics = getattr(_us_collector_diagnostics_local, "diagnostics", None)
                        _us_collector_diagnostics_local.diagnostics = diagnostics
                        try:
                            before_keys = set(batch_prefetch.keys())
                            with diagnostics.time_block("batch_prefetch_seconds"):
                                fetched_batch = _fetch_us_batch_ohlcv(
                                    batch_symbols,
                                    start=group_start,
                                    end=group_end,
                                )
                        finally:
                            if previous_diagnostics is None:
                                try:
                                    delattr(_us_collector_diagnostics_local, "diagnostics")
                                except AttributeError:
                                    pass
                            else:
                                _us_collector_diagnostics_local.diagnostics = previous_diagnostics
                        batch_prefetch.update(fetched_batch)
                        after_keys = set(batch_prefetch.keys())
                        hits = len(after_keys.difference(before_keys))
                        diagnostics.increment("batch_prefetch_hits", hits)
                        diagnostics.increment("batch_prefetch_misses", max(0, len(batch_symbols) - hits))
                    except RateLimitError:
                        wait = US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS
                        diagnostics.add_timing("retry_sleep_seconds", wait)
                        _extend_us_rate_limit_cooldown(wait)
                        print(
                            f"[US] Batch OHLCV rate-limited; falling back to single requests "
                            f"after cooldown ({wait:.0f}s)"
                        )
                        batch_prefetch = {}
                        stop_batching = True
                        break
                if stop_batching:
                    break

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 병렬로 티커 처리 실행
            results = list(executor.map(process_ticker, chunk))
        for ticker, status in zip(chunk, results):
            _record_collector_symbol_status(run_state, symbol=str(ticker), status=str(status))
            if (
                str(status) in _US_COLLECTOR_RETRYABLE_STATUSES
                and len(failed_samples) < 20
            ):
                failed_samples.append({"ticker": str(ticker), "status": str(status)})
        write_state_snapshot()
        
        # 청크 완료 후 상태 출력 및 대기
        print(_format_us_chunk_summary(chunk_num + 1, total_chunks, results))
        
        # API 제한 방지를 위한 대기
        if chunk_num + 1 < total_chunks:  # 마지막 청크가 아니면 대기
            chunk_pause = pause
            chunk_counts_after = diagnostics.snapshot().get("counts", {})
            if "rate_limited" in results:
                chunk_pause = max(chunk_pause, US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS)
            provider_requests = (
                int(chunk_counts_after.get("batch_prefetch_requests", 0))
                - int(chunk_counts_before.get("batch_prefetch_requests", 0))
                + int(chunk_counts_after.get("single_fetches", 0))
                - int(chunk_counts_before.get("single_fetches", 0))
            )
            if provider_requests <= 0 and "rate_limited" not in results:
                print("[US] Chunk pause skipped - no provider request in this chunk")
                continue
            print(f"⏳ {chunk_pause}초 대기 중...")
            diagnostics.add_timing("chunk_pause_seconds", chunk_pause)
            time.sleep(chunk_pause)
            run_state["cooldown_snapshot"] = dict(get_yahoo_throttle_state())
            run_state["last_progress_at"] = _collector_timestamp()
            write_state_snapshot()

# 크라켄 관련 함수 제거됨

# 메인 데이터 수집 함수
    elapsed_seconds = time.perf_counter() - started_perf
    run_state["diagnostics_snapshot"] = diagnostics.snapshot()
    return build_collector_summary(
        market="us",
        as_of=as_of_date,
        requested_total=len(requested_tickers),
        run_state=run_state,
        elapsed_seconds=elapsed_seconds,
        failed_samples=failed_samples,
        retryable_statuses=_US_COLLECTOR_RETRYABLE_STATUSES,
        hard_failure_statuses={"failed"},
        extra={
            "data_dir": save_dir,
            "collector_state_path": state_path,
            "timings": diagnostics.timings(process_total_seconds=elapsed_seconds),
            "collector_diagnostics": diagnostics.snapshot(),
        },
    )

def collect_data(max_us_chunks=None, start_chunk=0, update_symbols=True):
    bootstrap_windows_utf8()

    # 필요한 디렉토리 생성
    for directory in [DATA_DIR, DATA_US_DIR, RESULTS_DIR]:
        ensure_dir(directory)
        
    print("\n🇺🇸 미국 주식 데이터 수집 시작...")
    
    # 종목 리스트 업데이트 (선택적)
    if update_symbols:
        try:
            print("\n🔄 1단계: 종목 리스트 업데이트")
            all_symbols = update_symbol_list()
            us_tickers = sorted(all_symbols)
        except Exception as e:
            print(f"⚠️ 종목 리스트 업데이트 실패: {e}")
            print("📊 기존 CSV 파일 기준으로 계속 진행합니다.")
            us_tickers = sorted(_list_symbols_from_existing_us_csv())
    else:
        print("\n📊 기존 CSV 파일 기준으로 종목 목록 생성")
        us_tickers = sorted(_list_symbols_from_existing_us_csv())
    
    if not us_tickers:
        print("[US] No tickers found; stopping data collection")
        return {
            "schema_version": "1.0",
            "market": "us",
            "ok": False,
            "status": "failed",
            "retryable": False,
            "total": 0,
            "failed": 1,
            "error": "US ticker universe is empty",
        }
    
    print(f"\n📊 2단계: 총 {len(us_tickers)}개 종목의 OHLCV 데이터를 업데이트합니다.")
    
    # OHLCV 데이터 수집 실행
    us_tickers = limit_runtime_symbols(us_tickers)
    print(f"[US] Runtime data collection scope - symbols={len(us_tickers)}")

    try:
        return fetch_and_save_us_ohlcv_chunked(
            tickers=us_tickers,
            save_dir=DATA_US_DIR,
            chunk_size=US_OHLCV_CHUNK_SIZE,
            pause=US_OHLCV_CHUNK_PAUSE_SECONDS,
            start_chunk=start_chunk,
            max_chunks=max_us_chunks,
            max_workers=US_OHLCV_MAX_WORKERS
        )
        
    except Exception as e:
        print(f"❌ OHLCV 데이터 수집 중 오류 발생: {e}")
        print("⚠️ 데이터 수집을 중단합니다.")

# 명령행 인터페이스
if __name__ == "__main__":
    import argparse

    bootstrap_windows_utf8()
    bootstrap_yfinance_cache()
    
    parser = argparse.ArgumentParser(description="Mark Minervini 스크리너 - 데이터 수집")
    parser.add_argument("--max-us-chunks", type=int, help="최대 미국 주식 청크 수 제한")
    parser.add_argument("--start-chunk", type=int, default=0, help="시작할 청크 번호")
    parser.add_argument("--no-symbol-update", action="store_true", help="종목 리스트 업데이트 건너뛰기")
    
    args = parser.parse_args()
    
    collect_data(
        max_us_chunks=args.max_us_chunks,
        start_chunk=args.start_chunk,
        update_symbols=not args.no_symbol_update
    )
