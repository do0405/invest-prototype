from __future__ import annotations

import pytest

import utils.yahoo_throttle as throttle


def test_wait_for_yahoo_request_slot_does_not_delay_first_request(monkeypatch):
    current = {"value": 100.0}
    sleeps: list[float] = []

    def _monotonic() -> float:
        return current["value"]

    def _sleep(seconds: float) -> None:
        sleeps.append(seconds)
        current["value"] += seconds

    monkeypatch.setattr(throttle.time, "monotonic", _monotonic)
    monkeypatch.setattr(throttle.time, "sleep", _sleep)
    throttle.reset_yahoo_throttle_state()

    first_wait = throttle.wait_for_yahoo_request_slot("test", min_interval=1.2)
    second_wait = throttle.wait_for_yahoo_request_slot("test", min_interval=1.2)

    assert first_wait == pytest.approx(0.0)
    assert second_wait == pytest.approx(0.72)
    assert sleeps == [pytest.approx(0.72)]


def test_wait_for_yahoo_request_slot_respects_cooldown_without_extra_interval(monkeypatch):
    current = {"value": 50.0}
    sleeps: list[float] = []

    def _monotonic() -> float:
        return current["value"]

    def _sleep(seconds: float) -> None:
        sleeps.append(seconds)
        current["value"] += seconds

    monkeypatch.setattr(throttle.time, "monotonic", _monotonic)
    monkeypatch.setattr(throttle.time, "sleep", _sleep)
    throttle.reset_yahoo_throttle_state()

    throttle.extend_yahoo_cooldown("test", 5.0)
    waited = throttle.wait_for_yahoo_request_slot("test", min_interval=1.5)

    assert waited == pytest.approx(5.0)
    assert sleeps == [pytest.approx(5.0)]


def test_rate_limit_increases_future_interval_scale(monkeypatch):
    current = {"value": 10.0}
    sleeps: list[float] = []

    def _monotonic() -> float:
        return current["value"]

    def _sleep(seconds: float) -> None:
        sleeps.append(seconds)
        current["value"] += seconds

    monkeypatch.setattr(throttle.time, "monotonic", _monotonic)
    monkeypatch.setattr(throttle.time, "sleep", _sleep)
    throttle.reset_yahoo_throttle_state()

    throttle.wait_for_yahoo_request_slot("test", min_interval=1.0)
    throttle.extend_yahoo_cooldown("test", 2.0)
    waited = throttle.wait_for_yahoo_request_slot("test", min_interval=1.0)
    state = throttle.get_yahoo_throttle_state()

    assert waited == pytest.approx(2.0)
    assert sleeps == [pytest.approx(2.0)]
    assert state["adaptive_interval_scale"]["test"] == pytest.approx(0.72)


def test_interval_scale_does_not_decay_after_idle_period(monkeypatch):
    current = {"value": 10.0}

    def _monotonic() -> float:
        return current["value"]

    def _sleep(seconds: float) -> None:
        current["value"] += seconds

    monkeypatch.setattr(throttle.time, "monotonic", _monotonic)
    monkeypatch.setattr(throttle.time, "sleep", _sleep)
    throttle.reset_yahoo_throttle_state()

    throttle.extend_yahoo_cooldown("test", 2.0)
    current["value"] += 10_000.0
    throttle.wait_for_yahoo_request_slot("test", min_interval=1.0)
    state = throttle.get_yahoo_throttle_state()

    assert state["adaptive_interval_scale"]["test"] == pytest.approx(0.72)
