from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .market_runtime import market_key


_DATA_FRESHNESS_STATUSES: tuple[str, ...] = (
    "closed",
    "stale",
    "future_or_partial",
    "empty",
)
_DATA_FRESHNESS_EXAMPLE_LIMIT = 10


def _empty_data_freshness_counts() -> dict[str, int]:
    return {status: 0 for status in _DATA_FRESHNESS_STATUSES}


def _summary_to_payload(summary: Any) -> dict[str, Any]:
    if hasattr(summary, "to_dict"):
        raw = summary.to_dict()
    elif isinstance(summary, dict):
        raw = dict(summary)
    else:
        raw = {}
    raw_counts = raw.get("counts") if isinstance(raw.get("counts"), dict) else {}
    return {
        "counts": {
            status: int(raw_counts.get(status, 0) or 0)
            for status in _DATA_FRESHNESS_STATUSES
        },
        "target_date": str(raw.get("target_date") or "").strip(),
        "latest_completed_session": str(raw.get("latest_completed_session") or "").strip(),
        "mode": str(raw.get("mode") or "default_completed_session").strip()
        or "default_completed_session",
        "examples": [
            dict(item)
            for item in list(raw.get("examples") or [])[:_DATA_FRESHNESS_EXAMPLE_LIMIT]
            if isinstance(item, dict)
        ],
    }


def _recount_data_freshness(stages: dict[str, Any]) -> dict[str, int]:
    counts = _empty_data_freshness_counts()
    for payload in stages.values():
        if not isinstance(payload, dict):
            continue
        stage_counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
        for status in _DATA_FRESHNESS_STATUSES:
            counts[status] += int(stage_counts.get(status, 0) or 0)
    return counts


def runtime_context_has_explicit_as_of(runtime_context: Any) -> bool:
    if runtime_context is None:
        return False
    state = getattr(runtime_context, "runtime_state", {}) or {}
    status = str(state.get("freshness_status") or "").strip().lower() if isinstance(state, dict) else ""
    if status:
        return status == "explicit"
    return bool(str(getattr(runtime_context, "as_of_date", "") or "").strip())


@dataclass
class RuntimeContext:
    market: str
    as_of_date: str = ""
    run_id: str = ""
    metadata_map: dict[str, dict[str, Any]] = field(default_factory=dict)
    financial_map: dict[str, dict[str, Any]] = field(default_factory=dict)
    source_registry_snapshot: dict[str, Any] | None = None
    screening_frames: dict[str, Any] = field(default_factory=dict)
    runtime_state: dict[str, Any] = field(default_factory=dict)
    progress_callback: Callable[[dict[str, Any]], None] | None = None
    ohlcv_frame_cache: dict[tuple[Any, ...], Any] = field(default_factory=dict)
    cache_stats: dict[str, int] = field(
        default_factory=lambda: {"hits": 0, "misses": 0}
    )
    timings: dict[str, float] = field(default_factory=dict)
    runtime_metrics: dict[str, Any] = field(default_factory=dict)
    rows_read: int = 0
    rows_written: int = 0

    def __post_init__(self) -> None:
        self.market = market_key(self.market)
        self.as_of_date = str(self.as_of_date or "").strip()
        self.run_id = str(self.run_id or "").strip()

    def record_cache_hit(self, _label: str | None = None) -> None:
        self.cache_stats["hits"] = int(self.cache_stats.get("hits", 0)) + 1

    def record_cache_miss(self, _label: str | None = None) -> None:
        self.cache_stats["misses"] = int(self.cache_stats.get("misses", 0)) + 1

    def get_ohlcv_frame_cache(self, key: tuple[Any, ...]) -> Any | None:
        cached = self.ohlcv_frame_cache.get(key)
        if cached is None:
            return None
        self.record_cache_hit("ohlcv_frame")
        if hasattr(cached, "copy"):
            return cached.copy()
        return cached

    def set_ohlcv_frame_cache(self, key: tuple[Any, ...], frame: Any) -> None:
        self.record_cache_miss("ohlcv_frame")
        self.ohlcv_frame_cache[key] = frame.copy() if hasattr(frame, "copy") else frame

    def add_timing(self, label: str, seconds: float) -> None:
        if not label:
            return
        self.timings[label] = round(float(seconds), 6)

    def add_rows_read(self, count: int) -> None:
        self.rows_read += max(int(count), 0)

    def add_rows_written(self, count: int) -> None:
        self.rows_written += max(int(count), 0)

    def add_runtime_metric(self, section: str, key: str, value: int | float) -> None:
        section_key = str(section or "").strip()
        metric_key = str(key or "").strip()
        if not section_key or not metric_key:
            return
        section_payload = self.runtime_metrics.setdefault(section_key, {})
        if not isinstance(section_payload, dict):
            section_payload = {}
            self.runtime_metrics[section_key] = section_payload
        current = section_payload.get(metric_key, 0)
        if isinstance(value, float) or isinstance(current, float):
            section_payload[metric_key] = round(float(current or 0.0) + float(value or 0.0), 6)
        else:
            section_payload[metric_key] = int(current or 0) + int(value or 0)

    def set_runtime_metric(self, section: str, key: str, value: Any) -> None:
        section_key = str(section or "").strip()
        metric_key = str(key or "").strip()
        if not section_key or not metric_key:
            return
        section_payload = self.runtime_metrics.setdefault(section_key, {})
        if not isinstance(section_payload, dict):
            section_payload = {}
            self.runtime_metrics[section_key] = section_payload
        section_payload[metric_key] = value

    def record_worker_budget(
        self,
        scope: str,
        *,
        total_items: int,
        workers: int,
        env_var: str,
        configured: str = "",
        cap: int | None = None,
        stage_parallel: bool | None = None,
    ) -> None:
        scope_key = str(scope or "").strip()
        if not scope_key:
            return
        worker_payload = self.runtime_metrics.setdefault("worker_budget", {})
        if not isinstance(worker_payload, dict):
            worker_payload = {}
            self.runtime_metrics["worker_budget"] = worker_payload
        worker_payload[scope_key] = {
            "total_items": int(max(total_items, 0)),
            "workers": int(max(workers, 1)),
            "env_var": str(env_var or ""),
            "configured": str(configured or ""),
            "cap": int(cap) if cap is not None else None,
            "stage_parallel": bool(stage_parallel) if stage_parallel is not None else None,
        }

    def record_output_write(
        self,
        *,
        path: str,
        rows: int,
        bytes_written: int,
        seconds: float,
        kind: str,
        label: str = "",
    ) -> None:
        self.add_runtime_metric("output_persist", "files", 1)
        self.add_runtime_metric("output_persist", "rows", int(max(rows, 0)))
        self.add_runtime_metric("output_persist", "bytes", int(max(bytes_written, 0)))
        self.add_runtime_metric("output_persist", "seconds", float(max(seconds, 0.0)))
        examples = self.runtime_metrics.setdefault("output_persist", {}).setdefault("examples", [])
        if isinstance(examples, list) and len(examples) < 20:
            examples.append(
                {
                    "path": str(path or ""),
                    "rows": int(max(rows, 0)),
                    "bytes": int(max(bytes_written, 0)),
                    "seconds": round(float(max(seconds, 0.0)), 6),
                    "kind": str(kind or ""),
                    "label": str(label or ""),
                }
            )

    def set_as_of_date(self, value: Any) -> None:
        text = str(value or "").strip()
        if not text:
            return
        self.as_of_date = text
        if self.runtime_state:
            self.runtime_state["as_of_date"] = text

    def bind_progress_callback(
        self,
        callback: Callable[[dict[str, Any]], None] | None,
        *,
        run_id: str | None = None,
    ) -> None:
        self.progress_callback = callback
        if str(run_id or "").strip():
            self.run_id = str(run_id).strip()

    def update_runtime_state(self, **updates: Any) -> dict[str, Any]:
        payload = dict(self.runtime_state)
        payload["market"] = self.market
        if self.run_id:
            payload["run_id"] = self.run_id
        if self.as_of_date:
            payload["as_of_date"] = self.as_of_date
        payload.setdefault("started_at", datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
        payload["last_progress_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        for key, value in updates.items():
            if key == "as_of_date":
                self.set_as_of_date(value)
                if self.as_of_date:
                    payload["as_of_date"] = self.as_of_date
                continue
            if value is None:
                continue
            payload[key] = value
        if "status" not in payload:
            payload["status"] = "idle"
        self.runtime_state = payload
        if self.progress_callback is not None:
            self.progress_callback(dict(payload))
        return dict(payload)

    def update_data_freshness(self, stage_key: str, summary: Any) -> dict[str, Any]:
        key = str(stage_key or "").strip()
        if not key:
            return dict(self.runtime_state)
        existing = self.runtime_state.get("data_freshness")
        existing_payload = dict(existing) if isinstance(existing, dict) else {}
        raw_stages = existing_payload.get("stages")
        stages = dict(raw_stages) if isinstance(raw_stages, dict) else {}
        stages[key] = _summary_to_payload(summary)
        payload = {
            "counts": _recount_data_freshness(stages),
            "stages": stages,
        }
        return self.update_runtime_state(data_freshness=payload)
