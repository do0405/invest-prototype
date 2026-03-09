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

from config import EARNINGS_CACHE_DIR
from utils.external_data_cache import is_file_fresh, load_csv, write_csv_atomic
from utils.io_utils import safe_filename

try:
    import yahoo_fin.stock_info as si

    YAHOO_FIN_AVAILABLE = True
except Exception:
    YAHOO_FIN_AVAILABLE = False


logger = logging.getLogger(__name__)


class EarningsDataCollector:
    """Collect and score earnings surprise data with in-memory + CSV cache."""

    def __init__(self, cache_dir: Optional[str] = None, cache_duration: int = 3600):
        self.cache: dict[str, Dict[str, Any]] = {}
        self.cache_expiry: dict[str, datetime] = {}
        self.cache_duration = int(cache_duration)
        self.cache_dir = cache_dir or EARNINGS_CACHE_DIR
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

    def get_earnings_surprise(self, symbol: str) -> Optional[Dict[str, Any]]:
        symbol_key = self._symbol_key(symbol)

        if self._is_cached(symbol_key):
            with self._cache_lock:
                return self.cache.get(symbol_key)

        fresh_disk = self._load_disk_cache(symbol_key, fresh_only=True)
        if fresh_disk is not None:
            surprise_data = self._calculate_surprise(fresh_disk)
            if surprise_data is not None:
                self._cache_data(symbol_key, surprise_data)
                return surprise_data

        stale_disk = self._load_disk_cache(symbol_key, fresh_only=False)

        try:
            earnings_data = self._fetch_yahoo_fin_earnings(symbol_key)
            if earnings_data is None:
                earnings_data = self._fetch_yf_earnings(symbol_key)

            if earnings_data is not None:
                surprise_data = self._calculate_surprise(earnings_data)
                if surprise_data is not None:
                    self._save_disk_cache(symbol_key, earnings_data)
                    self._cache_data(symbol_key, surprise_data)
                    return surprise_data

            if stale_disk is not None:
                surprise_data = self._calculate_surprise(stale_disk)
                if surprise_data is not None:
                    self._cache_data(symbol_key, surprise_data)
                    return surprise_data

            logger.warning("%s: earnings data unavailable", symbol_key)
            return None
        except Exception as exc:
            logger.error("%s: failed to collect earnings data - %s", symbol_key, str(exc))
            if stale_disk is not None:
                surprise_data = self._calculate_surprise(stale_disk)
                if surprise_data is not None:
                    self._cache_data(symbol_key, surprise_data)
                    return surprise_data
            return None

    def _fetch_yf_earnings(self, symbol: str) -> Optional[pd.DataFrame]:
        try:
            ticker = yf.Ticker(symbol)
            earnings_history = getattr(ticker, "earnings_history", None)
            if earnings_history is None or earnings_history.empty:
                return None

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
                return None

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
                return None
            if earnings_data["eps_actual"].notna().sum() == 0 or earnings_data["eps_estimate"].notna().sum() == 0:
                return None

            earnings_data["data_source"] = "yfinance_actual"
            return earnings_data
        except Exception as exc:
            logger.error("%s: yfinance earnings fetch failed - %s", symbol, str(exc))
            return None

    def _fetch_yahoo_fin_earnings(self, symbol: str) -> Optional[pd.DataFrame]:
        if not YAHOO_FIN_AVAILABLE:
            return None

        try:
            earnings_history = si.get_earnings_history(symbol)
            if not earnings_history:
                return None

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
                return None
            if earnings_data["eps_actual"].notna().sum() == 0 or earnings_data["eps_estimate"].notna().sum() == 0:
                return None
            earnings_data["data_source"] = "yahoo_fin_actual"
            return earnings_data
        except Exception as exc:
            logger.error("%s: yahoo_fin earnings fetch failed - %s", symbol, str(exc))
            return None

    def _calculate_surprise(self, earnings_data: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if earnings_data is None or earnings_data.empty:
            return None

        latest = earnings_data.iloc[0]
        data_source = latest.get("data_source", "unknown")

        eps_actual_raw = latest.get("eps_actual")
        eps_estimate_raw = latest.get("eps_estimate")
        eps_actual = float(eps_actual_raw) if pd.notna(eps_actual_raw) else None
        eps_estimate = float(eps_estimate_raw) if pd.notna(eps_estimate_raw) else None

        if eps_actual is not None and eps_estimate is not None and eps_estimate != 0:
            eps_surprise_pct: Optional[float] = ((eps_actual - eps_estimate) / abs(eps_estimate)) * 100.0
        else:
            eps_surprise_pct = None

        revenue_actual_raw = latest.get("revenue_actual")
        revenue_estimate_raw = latest.get("revenue_estimate")
        revenue_actual = float(revenue_actual_raw) if pd.notna(revenue_actual_raw) else None
        revenue_estimate = float(revenue_estimate_raw) if pd.notna(revenue_estimate_raw) else None

        if revenue_actual is not None and revenue_estimate is not None and revenue_estimate != 0:
            revenue_surprise_pct: Optional[float] = ((revenue_actual - revenue_estimate) / revenue_estimate) * 100.0
        else:
            revenue_surprise_pct = None

        if len(earnings_data) >= 4:
            prev_eps_raw = earnings_data.iloc[3].get("eps_actual")
            prev_revenue_raw = earnings_data.iloc[3].get("revenue_actual")
            prev_eps = float(prev_eps_raw) if pd.notna(prev_eps_raw) else None
            prev_revenue = float(prev_revenue_raw) if pd.notna(prev_revenue_raw) else None

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

        if str(data_source) in {"yahoo_fin_actual", "yfinance_actual"}:
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
