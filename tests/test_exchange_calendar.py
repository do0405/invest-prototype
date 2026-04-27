from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from utils.exchange_calendar import (
    resolve_latest_completed_as_of,
    resolve_latest_completed_session,
)


def test_us_latest_completed_session_uses_previous_session_before_close() -> None:
    now = datetime(2026, 4, 21, 15, 59, tzinfo=ZoneInfo("America/New_York"))

    assert resolve_latest_completed_session("us", now=now) == "2026-04-20"


def test_us_latest_completed_session_uses_same_session_after_close() -> None:
    now = datetime(2026, 4, 21, 16, 1, tzinfo=ZoneInfo("America/New_York"))

    assert resolve_latest_completed_session("us", now=now) == "2026-04-21"


def test_us_latest_completed_session_skips_exchange_holiday_after_close() -> None:
    now = datetime(2026, 4, 3, 17, 0, tzinfo=ZoneInfo("America/New_York"))

    assert resolve_latest_completed_session("us", now=now) == "2026-04-02"


def test_kr_latest_completed_session_uses_previous_session_before_close() -> None:
    now = datetime(2026, 4, 21, 15, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    assert resolve_latest_completed_session("kr", now=now) == "2026-04-20"


def test_kr_latest_completed_session_uses_same_session_after_close() -> None:
    now = datetime(2026, 4, 21, 15, 31, tzinfo=ZoneInfo("Asia/Seoul"))

    assert resolve_latest_completed_session("kr", now=now) == "2026-04-21"


def test_resolve_latest_completed_as_of_clamps_to_stale_benchmark() -> None:
    now = datetime(2026, 4, 21, 16, 1, tzinfo=ZoneInfo("America/New_York"))

    resolution = resolve_latest_completed_as_of(
        "us",
        benchmark_as_of="2026-04-17",
        now=now,
    )

    assert resolution.as_of_date == "2026-04-17"
    assert resolution.latest_completed_session == "2026-04-21"
    assert resolution.freshness_status == "stale_benchmark"
