"""Internal collector timing and count diagnostics."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import threading
import time
from typing import Any, Iterator, Mapping

import pandas as pd


COLLECTOR_TIMING_KEYS: tuple[str, ...] = (
    "universe_resolve_seconds",
    "state_load_seconds",
    "state_write_seconds",
    "batch_prefetch_seconds",
    "symbol_prepare_seconds",
    "provider_wait_seconds",
    "provider_fetch_seconds",
    "retry_sleep_seconds",
    "merge_write_seconds",
    "chunk_pause_seconds",
    "index_benchmark_seconds",
    "process_total_seconds",
)


class CollectorDiagnostics:
    """Thread-safe accumulator for live collector diagnostics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._timings: dict[str, float] = {key: 0.0 for key in COLLECTOR_TIMING_KEYS}
        self._counts: Counter[str] = Counter()
        self._examples: list[dict[str, Any]] = []

    def add_timing(self, key: str, seconds: float) -> None:
        normalized = str(key or "").strip()
        if not normalized:
            return
        value = max(0.0, float(seconds or 0.0))
        with self._lock:
            self._timings[normalized] = float(self._timings.get(normalized, 0.0)) + value

    def get_timing(self, key: str) -> float:
        normalized = str(key or "").strip()
        with self._lock:
            return float(self._timings.get(normalized, 0.0))

    @contextmanager
    def time_block(self, key: str) -> Iterator[None]:
        started = time.perf_counter()
        try:
            yield
        finally:
            self.add_timing(key, time.perf_counter() - started)

    def increment(self, key: str, amount: int = 1) -> None:
        normalized = str(key or "").strip()
        if not normalized:
            return
        with self._lock:
            self._counts[normalized] += int(amount)

    def add_example(self, **payload: Any) -> None:
        clean = {str(key): value for key, value in payload.items() if str(key).strip()}
        if not clean:
            return
        with self._lock:
            if len(self._examples) < 20:
                self._examples.append(clean)

    def merge_from(self, *, timings: Mapping[str, Any] | None = None, diagnostics: Mapping[str, Any] | None = None) -> None:
        if isinstance(timings, Mapping):
            for key, value in timings.items():
                self.add_timing(str(key), float(value or 0.0))
        if isinstance(diagnostics, Mapping):
            raw_counts = diagnostics.get("counts")
            if isinstance(raw_counts, Mapping):
                for key, value in raw_counts.items():
                    self.increment(str(key), int(value or 0))
            raw_examples = diagnostics.get("examples")
            if isinstance(raw_examples, list):
                for example in raw_examples:
                    if isinstance(example, Mapping):
                        self.add_example(**dict(example))

    def timings(self, *, process_total_seconds: float | None = None) -> dict[str, float]:
        with self._lock:
            payload = {key: round(float(self._timings.get(key, 0.0)), 6) for key in COLLECTOR_TIMING_KEYS}
            for key, value in self._timings.items():
                if key not in payload:
                    payload[key] = round(float(value), 6)
        if process_total_seconds is not None:
            payload["process_total_seconds"] = round(max(0.0, float(process_total_seconds)), 6)
        return payload

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counts": {key: int(value) for key, value in sorted(self._counts.items())},
                "examples": [dict(item) for item in self._examples],
            }


def attach_collector_diagnostics(frame: pd.DataFrame, diagnostics: CollectorDiagnostics) -> pd.DataFrame:
    frame.attrs["timings"] = diagnostics.timings()
    frame.attrs["collector_diagnostics"] = diagnostics.snapshot()
    return frame


def merge_collector_diagnostics(items: list[Mapping[str, Any]]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        raw_counts = item.get("counts")
        if isinstance(raw_counts, Mapping):
            for key, value in raw_counts.items():
                if str(key).strip():
                    counts[str(key)] += int(value or 0)
        raw_examples = item.get("examples")
        if isinstance(raw_examples, list):
            for example in raw_examples:
                if isinstance(example, Mapping) and len(examples) < 20:
                    examples.append(dict(example))
    return {"counts": dict(counts), "examples": examples} if counts or examples else {}
