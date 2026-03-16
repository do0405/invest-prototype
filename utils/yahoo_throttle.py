from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


YAHOO_PHASE_HANDOFF_SECONDS = 30.0
YAHOO_ADAPTIVE_INITIAL_INTERVAL_SCALE = 0.60
YAHOO_ADAPTIVE_MAX_INTERVAL_SCALE = 3.0
YAHOO_ADAPTIVE_RATE_LIMIT_STEP = 0.12


@dataclass
class _YahooAdaptiveSourceState:
    interval_scale: float = YAHOO_ADAPTIVE_INITIAL_INTERVAL_SCALE
    rate_limit_streak: int = 0


@dataclass
class _YahooThrottleState:
    next_request_at: float = 0.0
    cooldown_until: float = 0.0
    last_rate_limit_source: str = ""
    last_request_source: str = ""
    adaptive_sources: dict[str, _YahooAdaptiveSourceState] = field(default_factory=dict)


_STATE = _YahooThrottleState()
_LOCK = threading.Lock()


def _emit(message: str) -> None:
    print(message, flush=True)


def reset_yahoo_throttle_state() -> None:
    with _LOCK:
        _STATE.next_request_at = 0.0
        _STATE.cooldown_until = 0.0
        _STATE.last_rate_limit_source = ""
        _STATE.last_request_source = ""
        _STATE.adaptive_sources.clear()


def _source_state(source_name: str) -> _YahooAdaptiveSourceState:
    state = _STATE.adaptive_sources.get(source_name)
    if state is None:
        state = _YahooAdaptiveSourceState()
        _STATE.adaptive_sources[source_name] = state
    return state


def get_yahoo_throttle_state() -> dict[str, object]:
    with _LOCK:
        now = time.monotonic()
        return {
            "next_request_in": max(0.0, _STATE.next_request_at - now),
            "cooldown_in": max(0.0, _STATE.cooldown_until - now),
            "last_rate_limit_source": _STATE.last_rate_limit_source,
            "last_request_source": _STATE.last_request_source,
            "adaptive_interval_scale": {
                source: round(state.interval_scale, 4)
                for source, state in _STATE.adaptive_sources.items()
            },
        }


def wait_for_yahoo_request_slot(source: str, *, min_interval: float = 0.0) -> float:
    source_name = str(source or "Yahoo").strip() or "Yahoo"
    interval = max(0.0, float(min_interval))
    with _LOCK:
        now = time.monotonic()
        source_state = _source_state(source_name)
        effective_interval = interval * source_state.interval_scale
        request_at = max(now, _STATE.next_request_at, _STATE.cooldown_until)
        wait_seconds = max(0.0, request_at - now)
        _STATE.next_request_at = request_at + effective_interval
        _STATE.last_request_source = source_name

    if wait_seconds > 0:
        if interval > 0:
            _emit(
                f"[Throttle] Yahoo request pacing - source={source_name}, "
                f"wait={wait_seconds:.1f}s, base={interval:.2f}s, scale={source_state.interval_scale:.2f}x"
            )
        else:
            _emit(f"[Throttle] Yahoo request pacing - source={source_name}, wait={wait_seconds:.1f}s")
        time.sleep(wait_seconds)
    return wait_seconds


def extend_yahoo_cooldown(source: str, seconds: float) -> float:
    source_name = str(source or "Yahoo").strip() or "Yahoo"
    cooldown = max(0.0, float(seconds))
    if cooldown <= 0:
        return 0.0
    with _LOCK:
        now = time.monotonic()
        source_state = _source_state(source_name)
        source_state.rate_limit_streak += 1
        source_state.interval_scale = min(
            YAHOO_ADAPTIVE_MAX_INTERVAL_SCALE,
            source_state.interval_scale + YAHOO_ADAPTIVE_RATE_LIMIT_STEP,
        )
        target = max(_STATE.cooldown_until, now + cooldown)
        _STATE.cooldown_until = target
        _STATE.last_rate_limit_source = source_name
        remaining = max(0.0, target - now)

    _emit(
        f"[Throttle] Yahoo cooldown extended - source={source_name}, wait={remaining:.1f}s, "
        f"scale={source_state.interval_scale:.2f}x, streak={source_state.rate_limit_streak}"
    )
    return remaining


def wait_for_yahoo_phase_handoff(next_phase: str, *, minimum_pause: float = YAHOO_PHASE_HANDOFF_SECONDS) -> float:
    phase_name = str(next_phase or "next phase").strip() or "next phase"
    pause = max(0.0, float(minimum_pause))
    with _LOCK:
        now = time.monotonic()
        if _STATE.next_request_at <= 0 and _STATE.cooldown_until <= 0:
            return 0.0
        target = max(_STATE.cooldown_until, _STATE.next_request_at + pause)
        wait_seconds = max(0.0, target - now)
        if wait_seconds > 0:
            _STATE.next_request_at = target

    if wait_seconds > 0:
        _emit(f"[Throttle] Yahoo phase handoff - waiting {wait_seconds:.1f}s before {phase_name}")
        time.sleep(wait_seconds)
    return wait_seconds
