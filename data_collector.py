#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Mark Minervini Screener - 데이터 수집 모듈

import os
import csv
import time
import threading
import logging
import re
import requests
import pandas as pd
import yfinance as yf
import yfinance.shared as yf_shared
from yfinance.exceptions import YFPricesMissingError, YFRateLimitError, YFTickerMissingError, YFTzMissingError
from datetime import date, datetime, timedelta
from pytz import timezone
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set

from data_collectors.symbol_universe import (
    _is_us_collectable_symbol,
    _read_symbol_from_existing_csv,
    load_us_symbol_universe,
    sync_official_us_symbol_directory,
)
from utils.console_runtime import bootstrap_windows_utf8
from utils.ohlcv_progress import format_us_style_chunk_start, format_us_style_chunk_summary
from utils.yfinance_runtime import bootstrap_yfinance_cache
from utils.yahoo_throttle import extend_yahoo_cooldown, wait_for_yahoo_request_slot
from utils import (
    ensure_dir, get_us_market_today, safe_filename
)

from config import (
    DATA_DIR, DATA_US_DIR, RESULTS_DIR, STOCK_METADATA_PATH
)

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

US_OHLCV_CHUNK_SIZE = 4
US_OHLCV_CHUNK_PAUSE_SECONDS = 8.0
US_OHLCV_MAX_WORKERS = 1
US_OHLCV_REQUEST_DELAY_SECONDS = 1.25
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
_us_rate_limit_cooldown_until = 0.0
_yfinance_logger_configured = False


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


def _legacy_format_us_chunk_summary(chunk_num: int, total_chunks: int, statuses: list[str]) -> str:
    counter = Counter(statuses)
    return (
        f"✅ 청크 {chunk_num}/{total_chunks} 완료: "
        f"처리 {len(statuses)}개 | "
        f"저장 {counter['saved']} | "
        f"최신 {counter['latest']} | "
        f"유지 {counter['kept_existing']} | "
        f"soft {counter['soft_unavailable']} | "
        f"상폐 {counter['delisted']} | "
        f"제한 {counter['rate_limited']} | "
        f"실패 {counter['failed']}"
    )


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


def _wait_for_us_rate_limit_cooldown() -> None:
    with _us_rate_limit_lock:
        remaining = _us_rate_limit_cooldown_until - time.monotonic()

    if remaining > 0:
        print(f"[US] cooldown applied: {remaining:.1f}s")
        time.sleep(remaining)

    wait_for_yahoo_request_slot("US OHLCV", min_interval=US_OHLCV_REQUEST_DELAY_SECONDS)


def _pop_yfinance_error(ticker: str) -> str | None:
    return yf_shared._ERRORS.pop(str(ticker).upper(), None)


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
        df = df.rename_axis("date").reset_index()
        df["symbol"] = ticker
        return df
    except YFRateLimitError as e:
        raise RateLimitError(str(e)) from e
    except (YFTzMissingError, YFPricesMissingError, YFTickerMissingError) as e:
        raise DelistedSymbolError(str(e)) from e
    except requests.exceptions.ReadTimeout:
        print(f"[US] ⏱️ 타임아웃 발생: {ticker} - API 응답 지연 (재시도 필요)")
        return None
    except requests.exceptions.ConnectionError:
        print(f"[US] 🌐 연결 오류: {ticker} - 네트워크 문제 발생 (재시도 필요)")
        return None
    except Exception as e:
        error_msg = str(e)
        normalized = error_msg.lower()
        if _is_us_delisted_error(normalized):
            raise DelistedSymbolError(error_msg) from e
        if _is_us_rate_limit_error(normalized):
            raise RateLimitError(error_msg) from e

        print(f"[US] ❌ 오류 발생 ({ticker}): {normalized[:100]}")
        return None

# 미국 주식 데이터 수집 및 저장 (병렬 처리 버전)
def fetch_and_save_us_ohlcv_chunked(
    tickers,
    save_dir=DATA_US_DIR,
    chunk_size=US_OHLCV_CHUNK_SIZE,
    pause=US_OHLCV_CHUNK_PAUSE_SECONDS,
    start_chunk=0,
    max_chunks=None,
    max_workers=US_OHLCV_MAX_WORKERS,
):
    ensure_dir(save_dir)
    today = get_us_market_today()

    # 총 청크 수 계산
    total_chunks = (len(tickers) + chunk_size - 1) // chunk_size
    if max_chunks is not None:
        total_chunks = min(total_chunks, start_chunk + max_chunks)

    # 시작 청크 설정
    if start_chunk > 0:
        print(f"🔄 청크 {start_chunk}부터 다시 시작합니다 (총 {total_chunks} 청크)")
    
    # 티커 처리 함수 정의 (병렬 처리용)
    def process_ticker(ticker):
        # Windows 예약 파일명 처리
        safe_ticker = safe_filename(ticker)
        path = os.path.join(save_dir, f"{safe_ticker}.csv")
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
            existing = None
            start_date = _default_us_fetch_start(today)

        if start_date > today:
            last_date_str = last_date.isoformat() if last_date is not None else today.isoformat()
            print(f"[US] Latest status: {ticker} (last data: {last_date_str})")
            return "latest"

        if overlap_count > 0 and last_date is not None:
            print(
                f"[US] Refresh overlap active: {ticker} - refetching last {overlap_count} stored bars "
                f"from {start_date.isoformat()}"
            )

        print(f"[DEBUG] {ticker}: collection start {start_date}, end {today}")
        df_new = None
        saw_rate_limit = False
        for j in range(US_OHLCV_MAX_RETRIES):  # 재시도 횟수 감소
            try:
                df_new = fetch_us_single(ticker, start=start_date, end=today)

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
                    time.sleep(wait)
            except RateLimitError:
                saw_rate_limit = True
                wait = US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS
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
                
            df_new.to_csv(path, index=False)
            print(f"[US] ✅ 신규 저장: {ticker} ({len(df_new)} rows)")
            return "saved"
    
    # 청크별 처리 시작
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
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 병렬로 티커 처리 실행
            results = list(executor.map(process_ticker, chunk))
        
        # 청크 완료 후 상태 출력 및 대기
        print(_format_us_chunk_summary(chunk_num + 1, total_chunks, results))
        
        # API 제한 방지를 위한 대기
        if chunk_num + 1 < total_chunks:  # 마지막 청크가 아니면 대기
            chunk_pause = pause
            if "rate_limited" in results:
                chunk_pause = max(chunk_pause, US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS)
            print(f"⏳ {chunk_pause}초 대기 중...")
            time.sleep(chunk_pause)

# 크라켄 관련 함수 제거됨

# 메인 데이터 수집 함수
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
            us_tickers = list(all_symbols)
        except Exception as e:
            print(f"⚠️ 종목 리스트 업데이트 실패: {e}")
            print("📊 기존 CSV 파일 기준으로 계속 진행합니다.")
            us_tickers = sorted(_list_symbols_from_existing_us_csv())
    else:
        print("\n📊 기존 CSV 파일 기준으로 종목 목록 생성")
        us_tickers = sorted(_list_symbols_from_existing_us_csv())
    
    if not us_tickers:
        print("⚠️ 종목을 찾을 수 없습니다. 데이터 수집을 중단합니다.")
        return
    
    print(f"\n📊 2단계: 총 {len(us_tickers)}개 종목의 OHLCV 데이터를 업데이트합니다.")
    
    # OHLCV 데이터 수집 실행
    try:
        fetch_and_save_us_ohlcv_chunked(
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
