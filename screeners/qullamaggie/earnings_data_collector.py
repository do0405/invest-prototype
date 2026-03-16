# -*- coding: utf-8 -*-
"""Qullamaggie earnings surprise collector with CSV-backed cache."""

from __future__ import annotations

import logging
import os
import re
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
import yfinance as yf

from utils.external_data_cache import is_file_fresh, load_csv, write_csv_atomic
from utils.io_utils import safe_filename
from utils.market_runtime import get_earnings_cache_dir, iter_provider_symbols, market_key
from utils.typing_utils import to_float_or_none
from utils.yahoo_throttle import extend_yahoo_cooldown, wait_for_yahoo_request_slot

try:
    import yahoo_fin.stock_info as si

    YAHOO_FIN_AVAILABLE = True
except Exception:
    YAHOO_FIN_AVAILABLE = False


logger = logging.getLogger(__name__)
EARNINGS_MIN_REQUEST_DELAY_SECONDS = 1.0
EARNINGS_RATE_LIMIT_COOLDOWN_SECONDS = 45.0


def _is_rate_limited_error(message: Any) -> bool:
    normalized = str(message or "").lower()
    return (
        "rate limit" in normalized
        or "429" in normalized
        or "too many requests" in normalized
        or "try after a while" in normalized
    )


def _is_earnings_unavailable_error(message: Any) -> bool:
    normalized = str(message or "").lower()
    keywords = (
        "delisted",
        "no timezone found",
        "possibly delisted",
        "quote not found",
        "invalid ticker",
        "symbol may be delisted",
        "no price data found",
        "no earnings data",
        "no earnings history",
        "404",
    )
    return any(keyword in normalized for keyword in keywords)


def _normalize_earnings_unavailable_reason(error_msg: Any) -> str:
    raw = str(error_msg or "").strip()
    normalized = raw.lower()

    if "no timezone found" in normalized:
        return "possibly delisted; no timezone found"
    if "no price data found" in normalized:
        return "possibly delisted; no price data found"
    if "symbol may be delisted" in normalized:
        return "possibly delisted; symbol may be delisted"
    if "quote not found" in normalized:
        return "quote not found"
    if "invalid ticker" in normalized:
        return "invalid ticker"
    if "404" in normalized or "not found" in normalized:
        return "quote not found"
    if "no earnings data" in normalized or "no earnings history" in normalized:
        return "earnings data unavailable"

    compact = " ".join(raw.split())
    return compact[:120] if compact else "earnings data unavailable"


def _classify_earnings_unavailable_reason(reason: str) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized.startswith("possibly delisted;"):
        return "soft"
    if normalized in {"quote not found", "invalid ticker"}:
        return "hard"
    if "earnings data unavailable" in normalized or "no earnings data" in normalized:
        return "soft"
    if "delisted" in normalized:
        return "hard"
    return "soft"


def _status_priority(status: str | None) -> int:
    priority = {
        "": 0,
        None: 0,
        "failed": 1,
        "soft_unavailable": 2,
        "delisted": 3,
        "rate_limited": 4,
    }
    return priority.get(str(status or "").strip().lower(), 0)


class EarningsDataCollector:
    """Collect and score earnings surprise data with in-memory + CSV cache."""

    def __init__(self, cache_dir: Optional[str] = None, cache_duration: int = 3600, *, market: str = "us"):
        self.cache: dict[str, Dict[str, Any]] = {}
        self.cache_expiry: dict[str, datetime] = {}
        self.cache_duration = int(cache_duration)
        self.market = market_key(market)
        self.cache_dir = cache_dir or get_earnings_cache_dir(self.market)
        self._cache_lock = threading.Lock()

    def _symbol_key(self, symbol: str) -> str:
        return str(symbol or "").strip().upper()

    def _disk_cache_path(self, symbol: str) -> str:
        symbol_key = self._symbol_key(symbol)
        return os.path.join(self.cache_dir, f"{safe_filename(symbol_key)}.csv")

    def _load_disk_cache(self, symbol: str, *, fresh_only: bool) -> Optional[pd.DataFrame]:
        cache_path = self._disk_cache_path(symbol)
        if not os.path.exists(cache_path):
            return None

        if fresh_only and not is_file_fresh(cache_path, max_age_seconds=self.cache_duration):
            return None

        frame = load_csv(cache_path)
        if frame is None or frame.empty:
            return None

        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        for col in ("eps_actual", "eps_estimate", "revenue_actual", "revenue_estimate"):
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame

    def _save_disk_cache(self, symbol: str, earnings_data: pd.DataFrame) -> None:
        if earnings_data is None or earnings_data.empty:
            return
        frame = earnings_data.copy()
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
            frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
        write_csv_atomic(frame, self._disk_cache_path(symbol), index=False)

    @staticmethod
    def _find_column(frame: pd.DataFrame, candidates: list[str]) -> Optional[str]:
        normalized_map: dict[str, str] = {}
        for column in frame.columns:
            key = re.sub(r"[^a-z0-9]", "", str(column).lower())
            normalized_map[key] = str(column)

        for candidate in candidates:
            key = re.sub(r"[^a-z0-9]", "", str(candidate).lower())
            if key in normalized_map:
                return normalized_map[key]
        return None

    def _merge_fetch_issue(
        self,
        current_status: str | None,
        current_reason: str | None,
        candidate_status: str | None,
        candidate_reason: str | None,
    ) -> tuple[str | None, str | None]:
        if _status_priority(candidate_status) > _status_priority(current_status):
            return candidate_status, candidate_reason
        if _status_priority(candidate_status) == _status_priority(current_status) and not current_reason and candidate_reason:
            return current_status, candidate_reason
        return current_status, current_reason

    @staticmethod
    def _coerce_fetch_result(result: Any) -> tuple[Optional[pd.DataFrame], str | None, str | None]:
        if isinstance(result, tuple) and len(result) == 3:
            frame, status, reason = result
            return frame, (str(status).strip().lower() if status else None), (str(reason).strip() or None if reason else None)
        if isinstance(result, pd.DataFrame):
            return result, "complete", None
        return None, None, None

    def _decorate_surprise_payload(
        self,
        payload: Dict[str, Any],
        *,
        fetch_status: str,
        unavailable_reason: str | None = None,
        data_quality: str | None = None,
        cache_fallback_status: str | None = None,
    ) -> Dict[str, Any]:
        updated = dict(payload)
        updated["fetch_status"] = str(fetch_status).strip().lower()
        updated["unavailable_reason"] = str(unavailable_reason).strip() if unavailable_reason else None
        if data_quality:
            updated["data_quality"] = data_quality
        if cache_fallback_status:
            updated["cache_fallback_status"] = str(cache_fallback_status).strip().lower()
        return updated

    def _unavailable_payload(self, symbol: str, status: str, reason: str | None) -> Dict[str, Any]:
        normalized_status = str(status or "failed").strip().lower() or "failed"
        normalized_reason = str(reason or "").strip() or None
        return {
            "symbol": self._symbol_key(symbol),
            "fetch_status": normalized_status,
            "unavailable_reason": normalized_reason,
            "meets_criteria": False,
            "data_source": "unavailable",
            "data_quality": "unavailable",
            "earnings_date": None,
            "eps_actual": None,
            "eps_estimate": None,
            "eps_surprise_pct": None,
            "revenue_actual": None,
            "revenue_estimate": None,
            "revenue_surprise_pct": None,
            "yoy_eps_growth": None,
            "yoy_revenue_growth": None,
            "eps_growth_condition": False,
            "revenue_growth_condition": False,
            "eps_surprise_condition": False,
            "revenue_surprise_condition": False,
        }

    def get_earnings_surprise(self, symbol: str) -> Optional[Dict[str, Any]]:
        symbol_key = self._symbol_key(symbol)

        if self._is_cached(symbol_key):
            with self._cache_lock:
                return self.cache.get(symbol_key)

        stale_disk = self._load_disk_cache(symbol_key, fresh_only=False)
        live_status: str | None = None
        live_reason: str | None = None

        try:
            yahoo_fin_data, yahoo_fin_status, yahoo_fin_reason = self._coerce_fetch_result(self._fetch_yahoo_fin_earnings(symbol_key))
            live_status, live_reason = self._merge_fetch_issue(live_status, live_reason, yahoo_fin_status, yahoo_fin_reason)
            earnings_data = yahoo_fin_data
            if earnings_data is None:
                yf_data, yf_status, yf_reason = self._coerce_fetch_result(self._fetch_yf_earnings(symbol_key))
                live_status, live_reason = self._merge_fetch_issue(live_status, live_reason, yf_status, yf_reason)
                earnings_data = yf_data

            if earnings_data is not None:
                surprise_data = self._calculate_surprise(earnings_data)
                if surprise_data is not None:
                    surprise_data = self._decorate_surprise_payload(surprise_data, fetch_status="complete")
                    self._save_disk_cache(symbol_key, earnings_data)
                    self._cache_data(symbol_key, surprise_data)
                    return surprise_data

            if stale_disk is not None:
                surprise_data = self._calculate_surprise(stale_disk)
                if surprise_data is not None:
                    surprise_data = self._decorate_surprise_payload(
                        surprise_data,
                        fetch_status="complete",
                        unavailable_reason=live_reason,
                        data_quality="stale_cache",
                        cache_fallback_status=live_status,
                    )
                    self._cache_data(symbol_key, surprise_data)
                    return surprise_data

            unavailable_status = live_status or "soft_unavailable"
            unavailable_reason = live_reason or "earnings data unavailable"
            logger.warning("%s: earnings data unavailable (%s) - %s", symbol_key, unavailable_status, unavailable_reason)
            return self._unavailable_payload(symbol_key, unavailable_status, unavailable_reason)
        except Exception as exc:
            error_text = str(exc)
            if _is_rate_limited_error(error_text):
                unavailable_status = "rate_limited"
                unavailable_reason = "rate limited"
            elif _is_earnings_unavailable_error(error_text):
                unavailable_reason = _normalize_earnings_unavailable_reason(error_text)
                unavailable_status = (
                    "soft_unavailable"
                    if _classify_earnings_unavailable_reason(unavailable_reason) == "soft"
                    else "delisted"
                )
            else:
                unavailable_status = "failed"
                unavailable_reason = error_text[:120]
            logger.error("%s: failed to collect earnings data - %s", symbol_key, error_text)
            if stale_disk is not None:
                surprise_data = self._calculate_surprise(stale_disk)
                if surprise_data is not None:
                    surprise_data = self._decorate_surprise_payload(
                        surprise_data,
                        fetch_status="complete",
                        unavailable_reason=unavailable_reason,
                        data_quality="stale_cache",
                        cache_fallback_status=unavailable_status,
                    )
                    self._cache_data(symbol_key, surprise_data)
                    return surprise_data
            return self._unavailable_payload(symbol_key, unavailable_status, unavailable_reason)

    def _fetch_yf_earnings(self, symbol: str) -> tuple[Optional[pd.DataFrame], str | None, str | None]:
        issue_status: str | None = None
        issue_reason: str | None = None
        for provider_symbol in iter_provider_symbols(symbol, self.market):
            try:
                wait_for_yahoo_request_slot("Qullamaggie earnings", min_interval=EARNINGS_MIN_REQUEST_DELAY_SECONDS)
                ticker = yf.Ticker(provider_symbol)
                earnings_history = getattr(ticker, "earnings_history", None)
                if earnings_history is None or earnings_history.empty:
                    continue

                history = earnings_history.copy()
                if isinstance(history.columns, pd.MultiIndex):
                    history.columns = [col[0] if isinstance(col, tuple) else col for col in history.columns]
                if isinstance(history.index, pd.MultiIndex) or not isinstance(history.index, pd.RangeIndex):
                    history = history.reset_index()

                date_col = self._find_column(history, ["earnings date", "earnings_date", "date", "asofdate"])
                eps_actual_col = self._find_column(history, ["reported eps", "eps actual", "epsactual", "actualeps"])
                eps_estimate_col = self._find_column(history, ["eps estimate", "epsestimate", "estimateeps", "consensuseps"])
                revenue_actual_col = self._find_column(history, ["revenue actual", "revenueactual", "actualrevenue"])
                revenue_estimate_col = self._find_column(history, ["revenue estimate", "revenueestimate", "estimatedrevenue"])

                if not date_col or not eps_actual_col or not eps_estimate_col:
                    continue

                earnings_data = pd.DataFrame(
                    {
                        "date": pd.to_datetime(history[date_col], errors="coerce"),
                        "eps_actual": pd.to_numeric(history[eps_actual_col], errors="coerce"),
                        "eps_estimate": pd.to_numeric(history[eps_estimate_col], errors="coerce"),
                    }
                )
                if revenue_actual_col:
                    earnings_data["revenue_actual"] = pd.to_numeric(history[revenue_actual_col], errors="coerce")
                else:
                    earnings_data["revenue_actual"] = pd.NA

                if revenue_estimate_col:
                    earnings_data["revenue_estimate"] = pd.to_numeric(history[revenue_estimate_col], errors="coerce")
                else:
                    earnings_data["revenue_estimate"] = pd.NA

                earnings_data = earnings_data.dropna(subset=["date"]).sort_values("date", ascending=False).head(4).reset_index(drop=True)
                if earnings_data.empty:
                    continue
                if earnings_data["eps_actual"].notna().sum() == 0 or earnings_data["eps_estimate"].notna().sum() == 0:
                    continue

                earnings_data["data_source"] = f"yfinance_actual:{provider_symbol}"
                return earnings_data, "complete", None
            except Exception as exc:
                error_text = str(exc)
                if _is_rate_limited_error(error_text):
                    extend_yahoo_cooldown("Qullamaggie earnings", EARNINGS_RATE_LIMIT_COOLDOWN_SECONDS)
                    logger.debug("%s: yfinance earnings rate limited for %s - %s", symbol, provider_symbol, error_text)
                    return None, "rate_limited", "rate limited"
                if _is_earnings_unavailable_error(error_text):
                    normalized_reason = _normalize_earnings_unavailable_reason(error_text)
                    issue_status, issue_reason = self._merge_fetch_issue(
                        issue_status,
                        issue_reason,
                        "soft_unavailable" if _classify_earnings_unavailable_reason(normalized_reason) == "soft" else "delisted",
                        normalized_reason,
                    )
                    logger.debug("%s: yfinance earnings unavailable for %s - %s", symbol, provider_symbol, normalized_reason)
                    continue
                issue_status, issue_reason = self._merge_fetch_issue(issue_status, issue_reason, "failed", error_text[:120])
                logger.debug("%s: yfinance earnings fetch failed for %s - %s", symbol, provider_symbol, error_text)
        return None, issue_status or "soft_unavailable", issue_reason or "earnings data unavailable"

    def _fetch_yahoo_fin_earnings(self, symbol: str) -> tuple[Optional[pd.DataFrame], str | None, str | None]:
        if not YAHOO_FIN_AVAILABLE:
            return None, None, None

        issue_status: str | None = None
        issue_reason: str | None = None
        for provider_symbol in iter_provider_symbols(symbol, self.market):
            try:
                wait_for_yahoo_request_slot("Qullamaggie earnings", min_interval=EARNINGS_MIN_REQUEST_DELAY_SECONDS)
                earnings_history = si.get_earnings_history(provider_symbol)
                if not earnings_history:
                    continue

                earnings_df = pd.DataFrame(earnings_history)
                recent_earnings = earnings_df.head(8)
                earnings_data = pd.DataFrame(
                    {
                        "date": pd.to_datetime(recent_earnings["startdatetime"]),
                        "eps_actual": pd.to_numeric(recent_earnings["epsactual"], errors="coerce"),
                        "eps_estimate": pd.to_numeric(recent_earnings["epsestimate"], errors="coerce"),
                        "revenue_actual": [pd.NA] * len(recent_earnings),
                        "revenue_estimate": [pd.NA] * len(recent_earnings),
                    }
                )
                earnings_data = earnings_data.dropna(subset=["date"]).sort_values("date", ascending=False).head(4).reset_index(drop=True)
                if earnings_data.empty:
                    continue
                if earnings_data["eps_actual"].notna().sum() == 0 or earnings_data["eps_estimate"].notna().sum() == 0:
                    continue
                earnings_data["data_source"] = f"yahoo_fin_actual:{provider_symbol}"
                return earnings_data, "complete", None
            except Exception as exc:
                error_text = str(exc)
                if _is_rate_limited_error(error_text):
                    extend_yahoo_cooldown("Qullamaggie earnings", EARNINGS_RATE_LIMIT_COOLDOWN_SECONDS)
                    logger.debug("%s: yahoo_fin earnings rate limited for %s - %s", symbol, provider_symbol, error_text)
                    return None, "rate_limited", "rate limited"
                if _is_earnings_unavailable_error(error_text):
                    normalized_reason = _normalize_earnings_unavailable_reason(error_text)
                    issue_status, issue_reason = self._merge_fetch_issue(
                        issue_status,
                        issue_reason,
                        "soft_unavailable" if _classify_earnings_unavailable_reason(normalized_reason) == "soft" else "delisted",
                        normalized_reason,
                    )
                    logger.debug("%s: yahoo_fin earnings unavailable for %s - %s", symbol, provider_symbol, normalized_reason)
                    continue
                issue_status, issue_reason = self._merge_fetch_issue(issue_status, issue_reason, "failed", error_text[:120])
                logger.debug("%s: yahoo_fin earnings fetch failed for %s - %s", symbol, provider_symbol, error_text)
        return None, issue_status or "soft_unavailable", issue_reason or "earnings data unavailable"

    def _calculate_surprise(self, earnings_data: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if earnings_data is None or earnings_data.empty:
            return None

        latest = earnings_data.iloc[0]
        data_source = latest.get("data_source", "unknown")

        eps_actual = to_float_or_none(latest.get("eps_actual"))
        eps_estimate = to_float_or_none(latest.get("eps_estimate"))

        if eps_actual is not None and eps_estimate is not None and eps_estimate != 0:
            eps_surprise_pct: Optional[float] = ((eps_actual - eps_estimate) / abs(eps_estimate)) * 100.0
        else:
            eps_surprise_pct = None

        revenue_actual = to_float_or_none(latest.get("revenue_actual"))
        revenue_estimate = to_float_or_none(latest.get("revenue_estimate"))

        if revenue_actual is not None and revenue_estimate is not None and revenue_estimate != 0:
            revenue_surprise_pct: Optional[float] = ((revenue_actual - revenue_estimate) / revenue_estimate) * 100.0
        else:
            revenue_surprise_pct = None

        if len(earnings_data) >= 4:
            prev_eps = to_float_or_none(earnings_data.iloc[3].get("eps_actual"))
            prev_revenue = to_float_or_none(earnings_data.iloc[3].get("revenue_actual"))

            if prev_eps is not None and eps_actual is not None and prev_eps != 0:
                if prev_eps > 0:
                    yoy_eps_growth: Optional[float] = ((eps_actual - prev_eps) / prev_eps) * 100.0
                elif eps_actual >= 0:
                    yoy_eps_growth = 200.0
                else:
                    yoy_eps_growth = ((eps_actual - prev_eps) / abs(prev_eps)) * 100.0
            else:
                yoy_eps_growth = None

            if prev_revenue is not None and revenue_actual is not None and prev_revenue != 0:
                yoy_revenue_growth: Optional[float] = ((revenue_actual - prev_revenue) / prev_revenue) * 100.0
            else:
                yoy_revenue_growth = None
        else:
            yoy_eps_growth = None
            yoy_revenue_growth = None

        eps_growth_condition = yoy_eps_growth is not None and yoy_eps_growth >= 100
        revenue_growth_condition = yoy_revenue_growth is not None and yoy_revenue_growth >= 20
        eps_surprise_condition = eps_surprise_pct is not None and eps_surprise_pct >= 20
        revenue_surprise_condition = revenue_surprise_pct is not None and revenue_surprise_pct >= 20

        has_revenue_observation = (yoy_revenue_growth is not None) or (revenue_surprise_pct is not None)
        revenue_requirement = revenue_growth_condition if has_revenue_observation else True

        data_source_text = str(data_source or "")
        if data_source_text.startswith("yahoo_fin_actual") or data_source_text.startswith("yfinance_actual"):
            data_quality = "actual"
        else:
            data_quality = "unknown"

        meets_criteria = (
            eps_growth_condition
            and revenue_requirement
            and (eps_surprise_condition or revenue_surprise_condition)
        )

        earnings_date = latest.get("date", datetime.now())
        if hasattr(earnings_date, "strftime"):
            earnings_date = earnings_date.strftime("%Y-%m-%d")
        else:
            earnings_date = str(earnings_date)

        return {
            "eps_actual": float(eps_actual) if eps_actual is not None else None,
            "eps_estimate": float(eps_estimate) if eps_estimate is not None else None,
            "eps_surprise_pct": float(eps_surprise_pct) if eps_surprise_pct is not None else None,
            "revenue_actual": float(revenue_actual) if revenue_actual is not None else None,
            "revenue_estimate": float(revenue_estimate) if revenue_estimate is not None else None,
            "revenue_surprise_pct": float(revenue_surprise_pct) if revenue_surprise_pct is not None else None,
            "yoy_eps_growth": float(yoy_eps_growth) if yoy_eps_growth is not None else None,
            "yoy_revenue_growth": float(yoy_revenue_growth) if yoy_revenue_growth is not None else None,
            "eps_growth_condition": bool(eps_growth_condition),
            "revenue_growth_condition": bool(revenue_growth_condition),
            "eps_surprise_condition": bool(eps_surprise_condition),
            "revenue_surprise_condition": bool(revenue_surprise_condition),
            "meets_criteria": bool(meets_criteria),
            "data_source": data_source,
            "data_quality": data_quality,
            "earnings_date": earnings_date,
            "fetch_status": "complete",
            "unavailable_reason": None,
        }

    def _is_cached(self, symbol: str) -> bool:
        with self._cache_lock:
            if symbol not in self.cache:
                return False
            expiry = self.cache_expiry.get(symbol)
            if expiry is None:
                return False
            return datetime.now() < expiry

    def _cache_data(self, symbol: str, data: Dict[str, Any]) -> None:
        with self._cache_lock:
            self.cache[symbol] = data
            self.cache_expiry[symbol] = datetime.now() + timedelta(seconds=self.cache_duration)

    def clear_cache(self) -> None:
        with self._cache_lock:
            self.cache.clear()
            self.cache_expiry.clear()

    def is_earnings_season(self, days_threshold: int = 7) -> bool:
        try:
            lookaround = max(0, int(days_threshold))
        except Exception:
            lookaround = 7

        today = datetime.utcnow().date()
        if YAHOO_FIN_AVAILABLE:
            try:
                start = (today - timedelta(days=lookaround)).isoformat()
                end = (today + timedelta(days=lookaround)).isoformat()
                calendar = si.get_earnings_in_date_range(start, end)
                if isinstance(calendar, pd.DataFrame):
                    return not calendar.empty
                if isinstance(calendar, list):
                    return len(calendar) > 0
            except Exception:
                pass

        # Fallback heuristic when external calendar feed is unavailable.
        return today.month in {1, 2, 4, 5, 7, 8, 10, 11}
