# -*- coding: utf-8 -*-
"""Qullamaggie earnings surprise collector with CSV-backed cache."""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from collections import Counter
import contextlib
from datetime import UTC, datetime, timedelta
import io
from typing import Any, Dict, Optional

import pandas as pd
import requests
import yfinance as yf

from utils.external_data_cache import is_file_fresh, load_csv, write_csv_atomic
from utils.io_utils import safe_filename
from utils.market_runtime import (
    get_earnings_cache_dir,
    get_preferred_provider_symbol,
    iter_provider_symbols,
    market_key,
)
from utils.security_profile import get_security_profile
from utils.typing_utils import to_float_or_none
from utils.yahoo_throttle import extend_yahoo_cooldown, record_yahoo_request_success, wait_for_yahoo_request_slot

try:
    _IMPORT_SINK = io.StringIO()
    with contextlib.redirect_stdout(_IMPORT_SINK), contextlib.redirect_stderr(_IMPORT_SINK):
        import yahoo_fin.stock_info as si

    YAHOO_FIN_AVAILABLE = True
except Exception:
    YAHOO_FIN_AVAILABLE = False


logger = logging.getLogger(__name__)
EARNINGS_MIN_REQUEST_DELAY_SECONDS = 1.0
EARNINGS_RATE_LIMIT_COOLDOWN_SECONDS = 45.0
_FNGUIDE_TIMEOUT = (3.0, 4.0)
_FNGUIDE_URL_TEMPLATE = "https://comp.fnguide.com/SVO2/asp/SVD_Main.asp?gicode=A{symbol}"
_FNGUIDE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0 Safari/537.36"
    )
}
_FNGUIDE_DATE_LABELS = (
    "Date of Scheduled Disclosure",
    "Scheduled Disclosure Date",
    "Date of Disclosure",
    "공시예정일",
    "실적발표예정일",
)
_FNGUIDE_UNDECIDED_TOKENS = ("undecided", "미정")


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


def _is_provider_parser_unavailable_error(error: Any) -> bool:
    if isinstance(error, IndexError):
        return True
    normalized = str(error or "").strip().lower()
    return normalized in {
        "list index out of range",
        "single positional indexer is out-of-bounds",
    }


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
        "not_expected": 0,
        "no_event_announced": 0,
        "scheduled_undecided": 0,
        "failed": 1,
        "soft_unavailable": 2,
        "delisted": 3,
        "rate_limited": 4,
    }
    return priority.get(str(status or "").strip().lower(), 0)


def _strip_html_fragment(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""), flags=re.IGNORECASE)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&#160;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
    )
    return " ".join(text.split()).strip()


def _normalize_event_date_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _strip_html_fragment(str(value))
    if not text:
        return None
    match = re.search(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})", text)
    if not match:
        return None
    year, month, day = match.groups()
    try:
        parsed = datetime(int(year), int(month), int(day), tzinfo=UTC)
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


class EarningsDataCollector:
    """Collect and score earnings surprise data with in-memory + CSV cache."""

    def __init__(self, cache_dir: Optional[str] = None, cache_duration: int = 3600, *, market: str = "us"):
        self.cache: dict[str, Dict[str, Any]] = {}
        self.cache_expiry: dict[str, datetime] = {}
        self.cache_duration = int(cache_duration)
        self.market = market_key(market)
        self.cache_dir = cache_dir or get_earnings_cache_dir(self.market)
        self._cache_lock = threading.Lock()
        self._inflight_fetches: dict[str, dict[str, Any]] = {}
        self._provider_diagnostics: list[dict[str, Any]] = []
        self._diagnostic_counters: Counter[str] = Counter()
        self._provider_timings: Counter[str] = Counter()

    def _symbol_key(self, symbol: str) -> str:
        return str(symbol or "").strip().upper()

    def _disk_cache_path(self, symbol: str) -> str:
        symbol_key = self._symbol_key(symbol)
        return os.path.join(self.cache_dir, f"{safe_filename(symbol_key)}.csv")

    def _provider_symbols(self, symbol: str) -> list[str]:
        preferred = get_preferred_provider_symbol(symbol, self.market)
        if preferred:
            return [preferred]
        return iter_provider_symbols(symbol, self.market)

    @staticmethod
    def _suppressed_call(fn: Any) -> Any:
        sink = io.StringIO()
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            return fn()

    def _record_diagnostic(
        self,
        *,
        symbol: str,
        provider_symbol: str | None,
        anchor_symbol: str | None = None,
        eligibility: str,
        skip_reason: str,
        attempt_chain: list[str],
        terminal_status: str,
        terminal_reason: str | None,
        resolved_source: str,
        provider_lineage: str | None = None,
        resolution_class: str | None = None,
        event_confidence: str | None = None,
    ) -> None:
        row = {
            "symbol": self._symbol_key(symbol),
            "market": self.market,
            "provider_symbol": str(provider_symbol or "").strip(),
            "anchor_symbol": self._symbol_key(anchor_symbol) if anchor_symbol else "",
            "eligibility": eligibility,
            "skip_reason": str(skip_reason or "").strip(),
            "attempt_chain": " > ".join(part for part in attempt_chain if part),
            "terminal_status": str(terminal_status or "").strip().lower(),
            "terminal_reason": str(terminal_reason or "").strip(),
            "resolved_source": str(resolved_source or "").strip(),
            "provider_lineage": str(provider_lineage or "").strip(),
            "resolution_class": str(resolution_class or "").strip().lower(),
            "event_confidence": str(event_confidence or "").strip().lower(),
            "as_of": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        self._provider_diagnostics.append(row)
        counter_key = row["skip_reason"] or row["resolution_class"] or row["resolved_source"] or row["terminal_status"]
        if counter_key:
            self._diagnostic_counters[counter_key] += 1
        if len(self._provider_diagnostics) % 100 == 0:
            self._emit_progress_summary()

    def _emit_progress_summary(self) -> None:
        summary_keys = (
            "not_expected",
            "inherited_event",
            "exact_event",
            "scheduled_undecided",
            "no_event_announced",
            "soft_unavailable",
            "rate_limited",
        )
        summary = ", ".join(
            f"{key}={self._diagnostic_counters.get(key, 0)}"
            for key in summary_keys
        )
        logger.info(
            "earnings diagnostics progress market=%s processed=%s %s",
            self.market,
            len(self._provider_diagnostics),
            summary,
        )

    def provider_diagnostics_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._provider_diagnostics]

    def provider_diagnostics_summary(self) -> dict[str, Any]:
        return {
            "counts": {key: int(value) for key, value in self._diagnostic_counters.items()},
            "timings": {
                key: round(float(value), 6)
                for key, value in self._provider_timings.items()
            },
        }

    def log_provider_summary(self) -> None:
        if not self._provider_diagnostics:
            return
        top_reasons = ", ".join(
            f"{reason}={count}"
            for reason, count in self._diagnostic_counters.most_common(6)
        )
        logger.info(
            "earnings diagnostics summary market=%s processed=%s %s",
            self.market,
            len(self._provider_diagnostics),
            top_reasons,
        )

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
        resolution_class: str | None = None,
        anchor_symbol: str | None = None,
        provider_lineage: str | None = None,
        event_confidence: str | None = None,
    ) -> Dict[str, Any]:
        updated = dict(payload)
        updated["fetch_status"] = str(fetch_status).strip().lower()
        updated["unavailable_reason"] = str(unavailable_reason).strip() if unavailable_reason else None
        if data_quality:
            updated["data_quality"] = data_quality
        if cache_fallback_status:
            updated["cache_fallback_status"] = str(cache_fallback_status).strip().lower()
        if resolution_class:
            updated["resolution_class"] = str(resolution_class).strip().lower()
        if anchor_symbol:
            updated["anchor_symbol"] = self._symbol_key(anchor_symbol)
        if provider_lineage:
            updated["provider_lineage"] = str(provider_lineage).strip()
        if event_confidence:
            updated["event_confidence"] = str(event_confidence).strip().lower()
        return updated

    def _build_provider_lineage(self, attempt_chain: list[str], resolved_source: str) -> str:
        lineage = [part for part in attempt_chain if part]
        if resolved_source:
            lineage.append(str(resolved_source).strip())
        return " > ".join(lineage)

    def _terminal_state_payload(
        self,
        symbol: str,
        *,
        status: str,
        reason: str | None,
        data_source: str,
        resolution_class: str,
        anchor_symbol: str | None = None,
        provider_lineage: str | None = None,
        event_confidence: str | None = None,
    ) -> Dict[str, Any]:
        payload = self._unavailable_payload(symbol, status, reason)
        payload["data_source"] = data_source
        payload["resolution_class"] = resolution_class
        payload["anchor_symbol"] = self._symbol_key(anchor_symbol) if anchor_symbol else ""
        payload["provider_lineage"] = str(provider_lineage or "").strip()
        payload["event_confidence"] = str(event_confidence or "").strip().lower()
        return payload

    def _event_proxy_payload(
        self,
        symbol: str,
        *,
        earnings_date: str,
        data_source: str,
        resolution_class: str,
        anchor_symbol: str | None = None,
        provider_lineage: str | None = None,
        event_confidence: str = "proxy",
    ) -> Dict[str, Any]:
        payload = self._unavailable_payload(symbol, "complete", None)
        payload.update(
            {
                "data_source": data_source,
                "data_quality": "event_proxy",
                "earnings_date": earnings_date,
                "resolution_class": resolution_class,
                "anchor_symbol": self._symbol_key(anchor_symbol) if anchor_symbol else "",
                "provider_lineage": str(provider_lineage or "").strip(),
                "event_confidence": str(event_confidence or "proxy").strip().lower(),
            }
        )
        return payload

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

    def _attempt_live_earnings_fetch(
        self,
        symbol: str,
        *,
        attempt_chain: list[str],
    ) -> tuple[Optional[pd.DataFrame], str | None, str | None]:
        live_status: str | None = None
        live_reason: str | None = None

        yahoo_fin_data, yahoo_fin_status, yahoo_fin_reason = self._coerce_fetch_result(
            self._fetch_yahoo_fin_earnings(symbol)
        )
        attempt_chain.append(f"yahoo_fin:{symbol}")
        live_status, live_reason = self._merge_fetch_issue(live_status, live_reason, yahoo_fin_status, yahoo_fin_reason)
        if yahoo_fin_data is not None:
            return yahoo_fin_data, live_status, live_reason

        yf_data, yf_status, yf_reason = self._coerce_fetch_result(self._fetch_yf_earnings(symbol))
        attempt_chain.append(f"yfinance:{symbol}")
        live_status, live_reason = self._merge_fetch_issue(live_status, live_reason, yf_status, yf_reason)
        return yf_data, live_status, live_reason

    def _extract_fnguide_disclosure_cell(self, html: str) -> str:
        for label in _FNGUIDE_DATE_LABELS:
            pattern = re.compile(
                rf"<(?:th|td)[^>]*>\s*{re.escape(label)}\s*</(?:th|td)>\s*<(?:td|th)[^>]*>(.*?)</(?:td|th)>",
                flags=re.IGNORECASE | re.DOTALL,
            )
            match = pattern.search(html)
            if match:
                return _strip_html_fragment(match.group(1))
        fallback_match = re.search(
            r"(Date of Scheduled Disclosure|공시예정일|실적발표예정일).{0,120}?(20\d{2}[./-]\d{1,2}[./-]\d{1,2}|Undecided|미정|-)",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if fallback_match:
            return _strip_html_fragment(fallback_match.group(2))
        return ""

    def _fetch_kr_fnguide_event(self, symbol: str) -> tuple[str, str | None, str | None]:
        if not symbol:
            return "soft_unavailable", "missing symbol", None
        try:
            response = requests.get(
                _FNGUIDE_URL_TEMPLATE.format(symbol=self._symbol_key(symbol)),
                headers=_FNGUIDE_HEADERS,
                timeout=_FNGUIDE_TIMEOUT,
            )
            response.raise_for_status()
        except Exception as exc:
            logger.debug("%s: fnguide request failed - %s", symbol, str(exc)[:160])
            return "soft_unavailable", str(exc)[:160], None

        disclosure_value = self._extract_fnguide_disclosure_cell(response.text)
        normalized_value = disclosure_value.strip()
        normalized_key = normalized_value.lower()
        if any(token in normalized_key for token in _FNGUIDE_UNDECIDED_TOKENS):
            return "scheduled_undecided", "scheduled disclosure undecided", None

        event_date = _normalize_event_date_text(normalized_value)
        if event_date:
            return "complete", None, event_date

        if normalized_value in {"", "-", "--", "N/A", "nan"}:
            return "no_event_announced", "no event announced", None
        return "no_event_announced", normalized_value[:120], None

    def _resolve_kr_event_proxy(
        self,
        *,
        symbol: str,
        anchor_symbol: str,
        attempt_chain: list[str],
    ) -> Dict[str, Any] | None:
        target_symbol = self._symbol_key(anchor_symbol or symbol)
        fnguide_status, fnguide_reason, event_date = self._fetch_kr_fnguide_event(target_symbol)
        attempt_chain.append(f"fnguide:{target_symbol}")
        provider_lineage = self._build_provider_lineage(attempt_chain, "fnguide_earning_issue")
        resolution_class = "inherited_event" if target_symbol != self._symbol_key(symbol) else "exact_event"
        event_confidence = "inherited" if target_symbol != self._symbol_key(symbol) else "proxy"

        if fnguide_status == "complete" and event_date:
            return self._event_proxy_payload(
                symbol,
                earnings_date=event_date,
                data_source="fnguide_earning_issue",
                resolution_class=resolution_class,
                anchor_symbol=target_symbol,
                provider_lineage=provider_lineage,
                event_confidence=event_confidence,
            )
        if fnguide_status in {"scheduled_undecided", "no_event_announced"}:
            return self._terminal_state_payload(
                symbol,
                status=fnguide_status,
                reason=fnguide_reason,
                data_source="fnguide_earning_issue",
                resolution_class=fnguide_status,
                anchor_symbol=target_symbol,
                provider_lineage=provider_lineage,
                event_confidence="scheduled" if fnguide_status == "scheduled_undecided" else "none",
            )
        return None

    def get_earnings_surprise(self, symbol: str) -> Optional[Dict[str, Any]]:
        symbol_key = self._symbol_key(symbol)
        profile = get_security_profile(symbol_key, self.market)
        provider_symbol = str(profile.get("preferred_provider_symbol") or profile.get("provider_symbol") or symbol_key).strip()
        anchor_symbol = self._symbol_key(
            profile.get("earnings_anchor_symbol") or profile.get("issuer_symbol") or symbol_key
        )

        if not bool(profile.get("earnings_expected", True)):
            skip_reason = str(profile.get("earnings_skip_reason") or "ineligible").strip() or "ineligible"
            payload = self._terminal_state_payload(
                symbol_key,
                status="not_expected",
                reason=f"ineligible:{skip_reason}",
                data_source="metadata_skip",
                resolution_class="not_expected",
                anchor_symbol=anchor_symbol,
                provider_lineage="metadata_skip",
                event_confidence="not_expected",
            )
            self._cache_data(symbol_key, payload)
            self._record_diagnostic(
                symbol=symbol_key,
                provider_symbol=provider_symbol,
                anchor_symbol=anchor_symbol,
                eligibility="ineligible",
                skip_reason=f"ineligible:{skip_reason}",
                attempt_chain=["metadata_skip"],
                terminal_status="not_expected",
                terminal_reason=f"ineligible:{skip_reason}",
                resolved_source="metadata_skip",
                provider_lineage="metadata_skip",
                resolution_class="not_expected",
                event_confidence="not_expected",
            )
            return payload

        waited_for_inflight = False
        fetch_slot: dict[str, Any] | None = None
        while True:
            cached = self._get_cached_payload(symbol_key)
            if cached is not None:
                self._diagnostic_counters["memory_cache_hits"] += 1
                self._record_diagnostic(
                    symbol=symbol_key,
                    provider_symbol=provider_symbol,
                    anchor_symbol=str(cached.get("anchor_symbol") or anchor_symbol),
                    eligibility="eligible",
                    skip_reason="",
                    attempt_chain=["memory_cache_wait" if waited_for_inflight else "memory_cache"],
                    terminal_status=str(cached.get("fetch_status") or "complete"),
                    terminal_reason=str(cached.get("unavailable_reason") or "").strip() or None,
                    resolved_source=str(cached.get("data_source") or "memory_cache"),
                    provider_lineage=str(cached.get("provider_lineage") or "memory_cache"),
                    resolution_class=str(cached.get("resolution_class") or ""),
                    event_confidence=str(cached.get("event_confidence") or ""),
                )
                return cached

            fetch_owner, fetch_slot = self._claim_symbol_fetch_slot(symbol_key)
            if fetch_owner:
                break
            waited_for_inflight = True
            wait_event = fetch_slot.get("event") if isinstance(fetch_slot, dict) else None
            if isinstance(wait_event, threading.Event):
                wait_event.wait()
            shared_result = self._consume_completed_symbol_fetch(symbol_key, fetch_slot)
            if shared_result is not None:
                self._diagnostic_counters["memory_cache_hits"] += 1
                self._record_diagnostic(
                    symbol=symbol_key,
                    provider_symbol=provider_symbol,
                    anchor_symbol=str(shared_result.get("anchor_symbol") or anchor_symbol),
                    eligibility="eligible",
                    skip_reason="",
                    attempt_chain=["memory_cache_wait"],
                    terminal_status=str(shared_result.get("fetch_status") or "complete"),
                    terminal_reason=str(shared_result.get("unavailable_reason") or "").strip() or None,
                    resolved_source=str(shared_result.get("data_source") or "memory_cache"),
                    provider_lineage=str(shared_result.get("provider_lineage") or "memory_cache"),
                    resolution_class=str(shared_result.get("resolution_class") or ""),
                    event_confidence=str(shared_result.get("event_confidence") or ""),
                )
                return shared_result

        stale_disk = self._load_disk_cache(symbol_key, fresh_only=False)
        attempt_chain: list[str] = []
        shared_result: Dict[str, Any] | None = None

        try:
            self._diagnostic_counters["live_provider_attempts"] += 1
            live_started = time.perf_counter()
            try:
                earnings_data, live_status, live_reason = self._attempt_live_earnings_fetch(
                    symbol_key,
                    attempt_chain=attempt_chain,
                )
            finally:
                self._provider_timings["provider_fetch_seconds"] += time.perf_counter() - live_started
            resolution_class = "exact_event"
            event_confidence = "exact"
            resolved_anchor_symbol = symbol_key

            if earnings_data is None and self.market == "kr" and anchor_symbol and anchor_symbol != symbol_key:
                self._diagnostic_counters["live_provider_attempts"] += 1
                anchor_started = time.perf_counter()
                try:
                    anchor_data, anchor_status, anchor_reason = self._attempt_live_earnings_fetch(
                        anchor_symbol,
                        attempt_chain=attempt_chain,
                    )
                finally:
                    self._provider_timings["provider_fetch_seconds"] += time.perf_counter() - anchor_started
                live_status, live_reason = self._merge_fetch_issue(live_status, live_reason, anchor_status, anchor_reason)
                if anchor_data is not None:
                    earnings_data = anchor_data
                    resolution_class = "inherited_event"
                    event_confidence = "inherited"
                    resolved_anchor_symbol = anchor_symbol

            if earnings_data is not None:
                surprise_data = self._calculate_surprise(earnings_data)
                if surprise_data is not None:
                    resolved_source = str(surprise_data.get("data_source") or "complete")
                    surprise_data = self._decorate_surprise_payload(
                        surprise_data,
                        fetch_status="complete",
                        resolution_class=resolution_class,
                        anchor_symbol=resolved_anchor_symbol,
                        provider_lineage=self._build_provider_lineage(attempt_chain, resolved_source),
                        event_confidence=event_confidence,
                    )
                    self._save_disk_cache(symbol_key, earnings_data)
                    self._cache_data(symbol_key, surprise_data)
                    self._record_diagnostic(
                        symbol=symbol_key,
                        provider_symbol=str(earnings_data.iloc[0].get("provider_symbol") or provider_symbol),
                        anchor_symbol=resolved_anchor_symbol,
                        eligibility="eligible",
                        skip_reason="",
                        attempt_chain=attempt_chain,
                        terminal_status="complete",
                        terminal_reason=None,
                        resolved_source=resolved_source,
                        provider_lineage=str(surprise_data.get("provider_lineage") or ""),
                        resolution_class=str(surprise_data.get("resolution_class") or resolution_class),
                        event_confidence=str(surprise_data.get("event_confidence") or event_confidence),
                    )
                    shared_result = surprise_data
                    return surprise_data

            if stale_disk is not None:
                self._diagnostic_counters["stale_disk_cache_available"] += 1
                surprise_data = self._calculate_surprise(stale_disk)
                if surprise_data is not None:
                    stale_resolution_class = "inherited_event" if self.market == "kr" and anchor_symbol != symbol_key else "exact_event"
                    surprise_data = self._decorate_surprise_payload(
                        surprise_data,
                        fetch_status="complete",
                        unavailable_reason=live_reason,
                        data_quality="stale_cache",
                        cache_fallback_status=live_status,
                        resolution_class=stale_resolution_class,
                        anchor_symbol=anchor_symbol if stale_resolution_class == "inherited_event" else symbol_key,
                        provider_lineage=self._build_provider_lineage(attempt_chain + ["stale_disk_cache"], "stale_disk_cache"),
                        event_confidence="stale",
                    )
                    self._cache_data(symbol_key, surprise_data)
                    self._record_diagnostic(
                        symbol=symbol_key,
                        provider_symbol=provider_symbol,
                        anchor_symbol=anchor_symbol if stale_resolution_class == "inherited_event" else symbol_key,
                        eligibility="eligible",
                        skip_reason="",
                        attempt_chain=attempt_chain + ["stale_disk_cache"],
                        terminal_status="complete",
                        terminal_reason=live_reason,
                        resolved_source="stale_disk_cache",
                        provider_lineage=str(surprise_data.get("provider_lineage") or ""),
                        resolution_class=stale_resolution_class,
                        event_confidence="stale",
                    )
                    shared_result = surprise_data
                    return surprise_data

            if self.market == "kr":
                proxy_payload = self._resolve_kr_event_proxy(
                    symbol=symbol_key,
                    anchor_symbol=anchor_symbol,
                    attempt_chain=attempt_chain,
                )
                if proxy_payload is not None:
                    self._cache_data(symbol_key, proxy_payload)
                    self._record_diagnostic(
                        symbol=symbol_key,
                        provider_symbol=provider_symbol,
                        anchor_symbol=str(proxy_payload.get("anchor_symbol") or anchor_symbol),
                        eligibility="eligible",
                        skip_reason="",
                        attempt_chain=attempt_chain,
                        terminal_status=str(proxy_payload.get("fetch_status") or ""),
                        terminal_reason=str(proxy_payload.get("unavailable_reason") or "").strip() or None,
                        resolved_source=str(proxy_payload.get("data_source") or ""),
                        provider_lineage=str(proxy_payload.get("provider_lineage") or ""),
                        resolution_class=str(proxy_payload.get("resolution_class") or ""),
                        event_confidence=str(proxy_payload.get("event_confidence") or ""),
                    )
                    shared_result = proxy_payload
                    return proxy_payload

            unavailable_status = live_status or "soft_unavailable"
            unavailable_reason = live_reason or "earnings data unavailable"
            payload = self._unavailable_payload(symbol_key, unavailable_status, unavailable_reason)
            self._record_diagnostic(
                symbol=symbol_key,
                provider_symbol=provider_symbol,
                anchor_symbol=anchor_symbol,
                eligibility="eligible",
                skip_reason="",
                attempt_chain=attempt_chain,
                terminal_status=unavailable_status,
                terminal_reason=unavailable_reason,
                resolved_source="",
                provider_lineage=self._build_provider_lineage(attempt_chain, ""),
                resolution_class="",
                event_confidence="",
            )
            logger.debug("%s: earnings data unavailable (%s) - %s", symbol_key, unavailable_status, unavailable_reason)
            shared_result = payload
            return payload
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
            logger.debug("%s: failed to collect earnings data - %s", symbol_key, error_text)
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
                    self._record_diagnostic(
                        symbol=symbol_key,
                        provider_symbol=provider_symbol,
                        anchor_symbol=anchor_symbol,
                        eligibility="eligible",
                        skip_reason="",
                        attempt_chain=attempt_chain + ["stale_disk_cache"],
                        terminal_status="complete",
                        terminal_reason=unavailable_reason,
                        resolved_source="stale_disk_cache",
                        provider_lineage=self._build_provider_lineage(attempt_chain + ["stale_disk_cache"], "stale_disk_cache"),
                        resolution_class="exact_event",
                        event_confidence="stale",
                    )
                    shared_result = surprise_data
                    return surprise_data
            payload = self._unavailable_payload(symbol_key, unavailable_status, unavailable_reason)
            self._record_diagnostic(
                symbol=symbol_key,
                provider_symbol=provider_symbol,
                anchor_symbol=anchor_symbol,
                eligibility="eligible",
                skip_reason="",
                attempt_chain=attempt_chain or ["exception"],
                terminal_status=unavailable_status,
                terminal_reason=unavailable_reason,
                resolved_source="",
                provider_lineage=self._build_provider_lineage(attempt_chain or ["exception"], ""),
                resolution_class="",
                event_confidence="",
            )
            shared_result = payload
            return payload
        finally:
            if fetch_slot is not None:
                self._release_symbol_fetch_slot(symbol_key, fetch_slot, shared_result)

    def _frame_from_earnings_history(
        self,
        history: pd.DataFrame,
        provider_symbol: str,
    ) -> Optional[pd.DataFrame]:
        if history is None or history.empty:
            return None
        normalized = history.copy()
        if isinstance(normalized.columns, pd.MultiIndex):
            normalized.columns = [col[0] if isinstance(col, tuple) else col for col in normalized.columns]
        if isinstance(normalized.index, pd.MultiIndex) or not isinstance(normalized.index, pd.RangeIndex):
            normalized = normalized.reset_index()

        date_col = self._find_column(normalized, ["earnings date", "earnings_date", "date", "asofdate"])
        eps_actual_col = self._find_column(normalized, ["reported eps", "eps actual", "epsactual", "actualeps"])
        eps_estimate_col = self._find_column(normalized, ["eps estimate", "epsestimate", "estimateeps", "consensuseps"])
        revenue_actual_col = self._find_column(normalized, ["revenue actual", "revenueactual", "actualrevenue"])
        revenue_estimate_col = self._find_column(normalized, ["revenue estimate", "revenueestimate", "estimatedrevenue"])

        if not date_col or not eps_actual_col or not eps_estimate_col:
            return None

        earnings_data = pd.DataFrame(
            {
                "date": pd.to_datetime(normalized[date_col], errors="coerce"),
                "eps_actual": pd.to_numeric(normalized[eps_actual_col], errors="coerce"),
                "eps_estimate": pd.to_numeric(normalized[eps_estimate_col], errors="coerce"),
            }
        )
        earnings_data["revenue_actual"] = (
            pd.to_numeric(normalized[revenue_actual_col], errors="coerce")
            if revenue_actual_col
            else pd.NA
        )
        earnings_data["revenue_estimate"] = (
            pd.to_numeric(normalized[revenue_estimate_col], errors="coerce")
            if revenue_estimate_col
            else pd.NA
        )
        earnings_data = earnings_data.dropna(subset=["date"]).sort_values("date", ascending=False).head(4).reset_index(drop=True)
        if earnings_data.empty:
            return None
        if earnings_data["eps_actual"].notna().sum() == 0 or earnings_data["eps_estimate"].notna().sum() == 0:
            return None
        earnings_data["data_source"] = f"yfinance_actual:{provider_symbol}"
        earnings_data["provider_symbol"] = provider_symbol
        return earnings_data

    def _frame_from_earnings_dates(
        self,
        earnings_dates: pd.DataFrame,
        provider_symbol: str,
    ) -> Optional[pd.DataFrame]:
        if earnings_dates is None or earnings_dates.empty:
            return None
        normalized = earnings_dates.copy()
        if isinstance(normalized.columns, pd.MultiIndex):
            normalized.columns = [col[0] if isinstance(col, tuple) else col for col in normalized.columns]
        if isinstance(normalized.index, pd.MultiIndex) or not isinstance(normalized.index, pd.RangeIndex):
            normalized = normalized.reset_index()

        date_col = self._find_column(normalized, ["earnings date", "date", "index"])
        eps_actual_col = self._find_column(normalized, ["reported eps", "eps actual"])
        eps_estimate_col = self._find_column(normalized, ["eps estimate"])
        if not date_col or not eps_actual_col or not eps_estimate_col:
            return None

        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(normalized[date_col], errors="coerce"),
                "eps_actual": pd.to_numeric(normalized[eps_actual_col], errors="coerce"),
                "eps_estimate": pd.to_numeric(normalized[eps_estimate_col], errors="coerce"),
                "revenue_actual": [pd.NA] * len(normalized),
                "revenue_estimate": [pd.NA] * len(normalized),
            }
        )
        frame = frame.dropna(subset=["date"]).sort_values("date", ascending=False).head(4).reset_index(drop=True)
        if frame.empty:
            return None
        if frame["eps_actual"].notna().sum() == 0 or frame["eps_estimate"].notna().sum() == 0:
            return None
        frame["data_source"] = f"yfinance_earnings_dates:{provider_symbol}"
        frame["provider_symbol"] = provider_symbol
        return frame

    def _frame_from_calendar(self, calendar: Any, provider_symbol: str) -> Optional[pd.DataFrame]:
        if isinstance(calendar, pd.DataFrame):
            payload = calendar.to_dict()
        elif isinstance(calendar, dict):
            payload = calendar
        else:
            payload = {}
        candidates = (
            payload.get("Earnings Date"),
            payload.get("earningsDate"),
            payload.get("earnings_date"),
        )
        for value in candidates:
            parsed = pd.NaT
            if isinstance(value, (list, tuple)):
                if value:
                    parsed = pd.to_datetime(value[0], errors="coerce")
            elif value is not None:
                parsed = pd.to_datetime(value, errors="coerce")
                if isinstance(parsed, pd.DatetimeIndex):
                    parsed = parsed[0] if len(parsed) else pd.NaT
            if pd.isna(parsed):
                continue
            frame = pd.DataFrame(
                [
                    {
                        "date": parsed,
                        "eps_actual": pd.NA,
                        "eps_estimate": pd.NA,
                        "revenue_actual": pd.NA,
                        "revenue_estimate": pd.NA,
                        "data_source": f"yfinance_calendar:{provider_symbol}",
                        "provider_symbol": provider_symbol,
                    }
                ]
            )
            return frame
        return None

    def _fetch_yf_earnings(self, symbol: str) -> tuple[Optional[pd.DataFrame], str | None, str | None]:
        issue_status: str | None = None
        issue_reason: str | None = None
        for provider_symbol in self._provider_symbols(symbol):
            try:
                wait_for_yahoo_request_slot("Qullamaggie earnings", min_interval=EARNINGS_MIN_REQUEST_DELAY_SECONDS)
                ticker = self._suppressed_call(lambda: yf.Ticker(provider_symbol))
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
                continue

            ladder = (
                (
                    "earnings_history",
                    lambda: self._suppressed_call(lambda: getattr(ticker, "earnings_history", None)),
                    lambda raw: self._frame_from_earnings_history(raw, provider_symbol),
                ),
                (
                    "earnings_dates",
                    lambda: self._suppressed_call(lambda: ticker.get_earnings_dates(limit=8, offset=1)),
                    lambda raw: self._frame_from_earnings_dates(raw, provider_symbol),
                ),
                (
                    "calendar",
                    lambda: self._suppressed_call(lambda: getattr(ticker, "calendar", {})),
                    lambda raw: self._frame_from_calendar(raw, provider_symbol),
                ),
            )

            for step_name, fetch_fn, build_fn in ladder:
                try:
                    frame = build_fn(fetch_fn())
                    if frame is not None:
                        record_yahoo_request_success("Qullamaggie earnings")
                        return frame, "complete", None
                except Exception as exc:
                    error_text = str(exc)
                    if _is_rate_limited_error(error_text):
                        extend_yahoo_cooldown("Qullamaggie earnings", EARNINGS_RATE_LIMIT_COOLDOWN_SECONDS)
                        logger.debug(
                            "%s: yfinance %s rate limited for %s - %s",
                            symbol,
                            step_name,
                            provider_symbol,
                            error_text,
                        )
                        return None, "rate_limited", "rate limited"
                    if _is_earnings_unavailable_error(error_text) or _is_provider_parser_unavailable_error(exc):
                        normalized_reason = (
                            _normalize_earnings_unavailable_reason(error_text)
                            if _is_earnings_unavailable_error(error_text)
                            else "earnings data unavailable"
                        )
                        issue_status, issue_reason = self._merge_fetch_issue(
                            issue_status,
                            issue_reason,
                            "soft_unavailable"
                            if _classify_earnings_unavailable_reason(normalized_reason) == "soft"
                            else "delisted",
                            normalized_reason,
                        )
                        logger.debug(
                            "%s: yfinance %s unavailable for %s - %s",
                            symbol,
                            step_name,
                            provider_symbol,
                            normalized_reason,
                        )
                        continue
                    issue_status, issue_reason = self._merge_fetch_issue(issue_status, issue_reason, "failed", error_text[:120])
                    logger.debug(
                        "%s: yfinance %s failed for %s - %s",
                        symbol,
                        step_name,
                        provider_symbol,
                        error_text,
                    )
        return None, issue_status or "soft_unavailable", issue_reason or "earnings data unavailable"

    def _fetch_yahoo_fin_earnings(self, symbol: str) -> tuple[Optional[pd.DataFrame], str | None, str | None]:
        if not YAHOO_FIN_AVAILABLE:
            return None, None, None

        issue_status: str | None = None
        issue_reason: str | None = None
        for provider_symbol in self._provider_symbols(symbol):
            try:
                wait_for_yahoo_request_slot("Qullamaggie earnings", min_interval=EARNINGS_MIN_REQUEST_DELAY_SECONDS)
                earnings_history = self._suppressed_call(lambda: si.get_earnings_history(provider_symbol))
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
                earnings_data["provider_symbol"] = provider_symbol
                record_yahoo_request_success("Qullamaggie earnings")
                return earnings_data, "complete", None
            except Exception as exc:
                error_text = str(exc)
                if _is_rate_limited_error(error_text):
                    extend_yahoo_cooldown("Qullamaggie earnings", EARNINGS_RATE_LIMIT_COOLDOWN_SECONDS)
                    logger.debug("%s: yahoo_fin earnings rate limited for %s - %s", symbol, provider_symbol, error_text)
                    return None, "rate_limited", "rate limited"
                if _is_earnings_unavailable_error(error_text) or _is_provider_parser_unavailable_error(exc):
                    normalized_reason = (
                        _normalize_earnings_unavailable_reason(error_text)
                        if _is_earnings_unavailable_error(error_text)
                        else "earnings data unavailable"
                    )
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
        if data_source_text.startswith("yfinance_calendar"):
            data_quality = "date_only"
        elif data_source_text.startswith("yahoo_fin_actual") or data_source_text.startswith("yfinance_actual") or data_source_text.startswith("yfinance_earnings_dates"):
            data_quality = "actual"
        else:
            data_quality = "unknown"

        meets_criteria = False if data_quality == "date_only" else (
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
            return self._get_cached_payload_locked(symbol) is not None

    def _get_cached_payload_locked(self, symbol: str) -> Dict[str, Any] | None:
        if symbol not in self.cache:
            return None
        expiry = self.cache_expiry.get(symbol)
        if expiry is None or datetime.now() >= expiry:
            return None
        return self.cache.get(symbol)

    def _get_cached_payload(self, symbol: str) -> Dict[str, Any] | None:
        with self._cache_lock:
            return self._get_cached_payload_locked(symbol)

    def _claim_symbol_fetch_slot(self, symbol: str) -> tuple[bool, dict[str, Any]]:
        with self._cache_lock:
            slot = self._inflight_fetches.get(symbol)
            if slot is None:
                slot = {
                    "event": threading.Event(),
                    "waiters": 0,
                    "result": None,
                }
                self._inflight_fetches[symbol] = slot
                return True, slot
            slot["waiters"] = int(slot.get("waiters", 0) or 0) + 1
            return False, slot

    def _consume_completed_symbol_fetch(
        self,
        symbol: str,
        slot: dict[str, Any] | None,
    ) -> Dict[str, Any] | None:
        with self._cache_lock:
            current = self._inflight_fetches.get(symbol)
            if current is None or current is not slot:
                return None
            result = current.get("result")
            current["waiters"] = max(0, int(current.get("waiters", 0) or 0) - 1)
            if current["waiters"] == 0 and bool(current.get("completed")):
                self._inflight_fetches.pop(symbol, None)
            return result if isinstance(result, dict) else None

    def _release_symbol_fetch_slot(
        self,
        symbol: str,
        slot: dict[str, Any],
        result: Dict[str, Any] | None,
    ) -> None:
        wait_event = slot.get("event")
        with self._cache_lock:
            current = self._inflight_fetches.get(symbol)
            if current is slot:
                current["result"] = dict(result) if isinstance(result, dict) else None
                current["completed"] = True
                if int(current.get("waiters", 0) or 0) == 0:
                    self._inflight_fetches.pop(symbol, None)
        if isinstance(wait_event, threading.Event):
            wait_event.set()

    def _cache_data(self, symbol: str, data: Dict[str, Any]) -> None:
        with self._cache_lock:
            self.cache[symbol] = data
            self.cache_expiry[symbol] = datetime.now() + timedelta(seconds=self.cache_duration)

    def clear_cache(self) -> None:
        with self._cache_lock:
            self.cache.clear()
            self.cache_expiry.clear()
            self._inflight_fetches.clear()

    def is_earnings_season(self, days_threshold: int = 7) -> bool:
        try:
            lookaround = max(0, int(days_threshold))
        except Exception:
            lookaround = 7

        today = datetime.now(UTC).date()
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
