from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    GoodFriday,
    Holiday,
    USLaborDay,
    USMartinLutherKingJr,
    USMemorialDay,
    USPresidentsDay,
    USThanksgivingDay,
    nearest_workday,
)
from pandas.tseries.offsets import CustomBusinessDay

from utils.market_runtime import market_key


class _NYSEHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("New Year's Day", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday("Juneteenth", month=6, day=19, observance=nearest_workday, start_date="2022-06-19"),
        Holiday("Independence Day", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("Christmas Day", month=12, day=25, observance=nearest_workday),
    ]


_US_BUSINESS_DAY = CustomBusinessDay(calendar=_NYSEHolidayCalendar())
_US_TZ = ZoneInfo("America/New_York")
_KR_TZ = ZoneInfo("Asia/Seoul")
_US_MARKET_CLOSE = time(16, 0)
_KR_MARKET_CLOSE = time(15, 30)
_KRX_WEEKLY_FINAL_SESSIONS_PATH = os.path.join(
    os.path.dirname(__file__),
    "resources",
    "krx_weekly_final_sessions.json",
)


@dataclass(frozen=True)
class AsOfResolution:
    market: str
    as_of_date: str
    latest_completed_session: str
    benchmark_as_of_date: str = ""
    freshness_status: str = "ok"
    reason: str = ""
    explicit: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@lru_cache(maxsize=1)
def _load_krx_weekly_final_sessions() -> dict[str, pd.Timestamp]:
    if not os.path.exists(_KRX_WEEKLY_FINAL_SESSIONS_PATH):
        return {}
    try:
        with open(_KRX_WEEKLY_FINAL_SESSIONS_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return {}
    sessions: dict[str, pd.Timestamp] = {}
    if not isinstance(payload, dict):
        return sessions
    for week_key, date_text in payload.items():
        normalized_key = str(week_key or "").strip()
        if not normalized_key:
            continue
        parsed = pd.to_datetime(date_text, errors="coerce")
        if pd.isna(parsed):
            continue
        sessions[normalized_key] = pd.Timestamp(parsed).normalize()
    return sessions


def expected_week_final_session(week_key: pd.Period, *, market: str) -> pd.Timestamp:
    week_end = pd.Timestamp(week_key.end_time).normalize()
    week_start = week_end - pd.Timedelta(days=6)
    normalized_market = market_key(market)

    if normalized_market == "us":
        sessions = pd.date_range(week_start, week_end, freq=_US_BUSINESS_DAY)
    elif normalized_market == "kr":
        mapped = _load_krx_weekly_final_sessions().get(str(week_key))
        if mapped is not None:
            return mapped
        sessions = pd.date_range(week_start, week_end, freq="B")
    else:
        sessions = pd.date_range(week_start, week_end, freq="B")

    if len(sessions) == 0:
        return (week_end - pd.Timedelta(days=2)).normalize()
    return pd.Timestamp(sessions[-1]).normalize()


def _coerce_date_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:
        return ""
    if pd.isna(parsed):
        return ""
    return pd.Timestamp(parsed).date().isoformat()


def _market_timezone(market: str) -> ZoneInfo:
    return _KR_TZ if market_key(market) == "kr" else _US_TZ


def _market_close_time(market: str) -> time:
    return _KR_MARKET_CLOSE if market_key(market) == "kr" else _US_MARKET_CLOSE


def _localize_now(market: str, now: datetime | date | None = None) -> datetime:
    tz = _market_timezone(market)
    if now is None:
        return datetime.now(tz)
    if isinstance(now, datetime):
        if now.tzinfo is None:
            return now.replace(tzinfo=tz)
        return now.astimezone(tz)
    return datetime.combine(now, time(23, 59), tzinfo=tz)


def _sessions_on_or_before(day: date, market: str) -> list[pd.Timestamp]:
    end = pd.Timestamp(day).normalize()
    start = end - pd.Timedelta(days=14)
    normalized_market = market_key(market)
    if normalized_market == "us":
        sessions = pd.date_range(start, end, freq=_US_BUSINESS_DAY)
    else:
        sessions = pd.date_range(start, end, freq="B")
    return [pd.Timestamp(session).normalize() for session in sessions]


def latest_session_on_or_before(day: date | datetime | str, *, market: str) -> str:
    text = _coerce_date_text(day)
    if not text:
        raise ValueError("day must be parseable as a date")
    parsed_day = pd.Timestamp(text).date()
    sessions = _sessions_on_or_before(parsed_day, market)
    if not sessions:
        return (pd.Timestamp(parsed_day) - pd.Timedelta(days=1)).date().isoformat()
    return sessions[-1].date().isoformat()


def previous_session_before(day: date | datetime | str, *, market: str) -> str:
    text = _coerce_date_text(day)
    if not text:
        raise ValueError("day must be parseable as a date")
    previous_day = pd.Timestamp(text).date() - timedelta(days=1)
    return latest_session_on_or_before(previous_day, market=market)


def resolve_latest_completed_session(
    market: str,
    *,
    now: datetime | date | None = None,
) -> str:
    normalized_market = market_key(market)
    local_now = _localize_now(normalized_market, now)
    local_day = local_now.date()
    latest_session = latest_session_on_or_before(local_day, market=normalized_market)
    if latest_session != local_day.isoformat():
        return latest_session
    if local_now.time() < _market_close_time(normalized_market):
        return previous_session_before(local_day, market=normalized_market)
    return latest_session


def resolve_latest_completed_as_of(
    market: str,
    *,
    explicit_as_of: Any = None,
    benchmark_as_of: Any = None,
    now: datetime | date | None = None,
) -> AsOfResolution:
    normalized_market = market_key(market)
    explicit_text = _coerce_date_text(explicit_as_of)
    latest_completed = resolve_latest_completed_session(normalized_market, now=now)
    benchmark_text = _coerce_date_text(benchmark_as_of)
    if explicit_text:
        return AsOfResolution(
            market=normalized_market,
            as_of_date=explicit_text,
            latest_completed_session=latest_completed,
            benchmark_as_of_date=benchmark_text,
            freshness_status="explicit",
            reason="explicit_as_of",
            explicit=True,
        )

    if not benchmark_text:
        return AsOfResolution(
            market=normalized_market,
            as_of_date=latest_completed,
            latest_completed_session=latest_completed,
            benchmark_as_of_date="",
            freshness_status="benchmark_missing",
            reason="no_local_benchmark_as_of",
        )

    benchmark_day = pd.Timestamp(benchmark_text).date()
    completed_day = pd.Timestamp(latest_completed).date()
    if benchmark_day < completed_day:
        return AsOfResolution(
            market=normalized_market,
            as_of_date=benchmark_text,
            latest_completed_session=latest_completed,
            benchmark_as_of_date=benchmark_text,
            freshness_status="stale_benchmark",
            reason="benchmark_older_than_latest_completed_session",
        )
    if benchmark_day > completed_day:
        return AsOfResolution(
            market=normalized_market,
            as_of_date=latest_completed,
            latest_completed_session=latest_completed,
            benchmark_as_of_date=benchmark_text,
            freshness_status="future_benchmark_clipped",
            reason="benchmark_newer_than_latest_completed_session",
        )
    return AsOfResolution(
        market=normalized_market,
        as_of_date=latest_completed,
        latest_completed_session=latest_completed,
        benchmark_as_of_date=benchmark_text,
        freshness_status="ok",
        reason="benchmark_matches_latest_completed_session",
    )
