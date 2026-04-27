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
    assert state["success_streak"]["test"] == 0
    assert state["rate_limit_count"]["test"] == 1


def test_success_feedback_gradually_reduces_interval_scale(monkeypatch):
    current = {"value": 10.0}

    def _monotonic() -> float:
        return current["value"]

    monkeypatch.setattr(throttle.time, "monotonic", _monotonic)
    throttle.reset_yahoo_throttle_state()

    throttle.wait_for_yahoo_request_slot("test", min_interval=1.0)
    for _ in range(4):
        throttle.record_yahoo_request_success("test")

    state = throttle.get_yahoo_throttle_state()

    assert state["adaptive_interval_scale"]["test"] == pytest.approx(0.52)
    assert state["success_streak"]["test"] == 4
    assert state["success_count"]["test"] == 4
    assert state["rate_limit_count"].get("test", 0) == 0


def test_timeout_feedback_backs_off_and_resets_success_streak(monkeypatch):
    current = {"value": 10.0}

    def _monotonic() -> float:
        return current["value"]

    monkeypatch.setattr(throttle.time, "monotonic", _monotonic)
    throttle.reset_yahoo_throttle_state()

    throttle.wait_for_yahoo_request_slot("test", min_interval=1.0)
    throttle.record_yahoo_request_success("test")
    throttle.record_yahoo_request_failure("test", reason="timeout", cooldown_seconds=3.0)
    state = throttle.get_yahoo_throttle_state()

    assert state["adaptive_interval_scale"]["test"] == pytest.approx(0.70)
    assert state["success_streak"]["test"] == 0
    assert state["rate_limit_count"]["test"] == 1
    assert state["last_rate_limit_source"] == "test"
    assert state["cooldown_in"] == pytest.approx(3.0)


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
