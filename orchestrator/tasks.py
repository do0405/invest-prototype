"""Utility tasks for running collectors and screeners."""

from __future__ import annotations

import json
import os
import copy
import subprocess
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd

from screeners.leader_core_bridge import probe_market_intel_compat_availability
from utils.collector_diagnostics import merge_collector_diagnostics
from utils.market_runtime import (
    ensure_market_dirs,
    get_full_run_summary_path,
    get_market_data_dir,
    get_market_screeners_root,
    get_market_source_registry_snapshot_path,
    get_signal_engine_results_dir,
    preflight_market_output_dirs,
    get_runtime_profile_path,
    get_runtime_state_path,
    is_index_symbol,
    limit_runtime_symbols,
    market_key,
    require_market_key,
)
from utils.exchange_calendar import resolve_latest_completed_as_of
from utils.market_data_contract import (
    PricePolicy,
    SCREENING_OHLCV_READ_COLUMNS,
    load_benchmark_data,
    load_local_ohlcv_frame,
    load_local_ohlcv_frames_ordered,
)
from utils.runtime_context import RuntimeContext
from utils.yahoo_throttle import wait_for_yahoo_phase_handoff

__all__ = [
    "ensure_directories",
    "collect_data_main",
    "run_kr_ohlcv_collection",
    "run_all_screening_processes",
    "run_market_analysis_pipeline",
    "run_screening_augment_processes",
    "run_signal_engine_processes",
    "run_leader_lagging_screening",
    "run_stock_metadata_collection",
    "run_qullamaggie_strategy_task",
    "run_weinstein_stage2_screening",
    "run_scheduler",
    "setup_scheduler",
    "write_full_run_summaries",
]

_RUNTIME_STATE_WRITE_WARNED: set[str] = set()
_RUNTIME_STATE_CONDITION = threading.Condition()
_RUNTIME_STATE_PENDING: dict[str, dict[str, Any]] = {}
_RUNTIME_STATE_DIRTY: set[str] = set()
_RUNTIME_STATE_INFLIGHT: set[str] = set()
_RUNTIME_STATE_WRITER_THREAD: threading.Thread | None = None
_DATA_FRESHNESS_STATUSES = ("closed", "stale", "future_or_partial", "empty")
_PARALLEL_STAGE_PROGRESS_LOCK = threading.Lock()


@dataclass
class TaskStepOutcome:
    ok: bool
    label: str
    market: str
    elapsed_seconds: float
    summary: str = ""
    error: str = ""
    result: Any = None
    status: str = "ok"
    status_counts: dict[str, int] | None = None
    timings: dict[str, float] | None = None
    cache_stats: dict[str, int] | None = None
    runtime_metrics: dict[str, Any] | None = None
    rows_read: int = 0
    rows_written: int = 0
    retryable: bool = False
    error_code: str = ""
    error_detail: str = ""
    started_at: str = ""
    finished_at: str = ""
    current_symbol: str = ""
    current_chunk: str = ""
    as_of_date: str = ""
    freshness_status: str = ""
    freshness_reason: str = ""
    benchmark_as_of_date: str = ""
    latest_completed_session: str = ""
    data_freshness: dict[str, Any] | None = None
    collector_diagnostics: dict[str, Any] | None = None
    cooldown_snapshot: dict[str, Any] | None = None
    last_retryable_error: str = ""
    market_truth_mode: str = ""
    fallback_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "label": self.label,
            "market": self.market,
            "elapsed_seconds": self.elapsed_seconds,
            "summary": self.summary,
            "error": self.error,
            "status": self.status,
            "status_counts": dict(self.status_counts or {}),
            "timings": dict(self.timings or {}),
            "cache_stats": dict(self.cache_stats or {}),
            "runtime_metrics": dict(self.runtime_metrics or {}),
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "retryable": self.retryable,
            "error_code": self.error_code,
            "error_detail": self.error_detail,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "current_symbol": self.current_symbol,
            "current_chunk": self.current_chunk,
            "as_of_date": self.as_of_date,
            "freshness_status": self.freshness_status,
            "freshness_reason": self.freshness_reason,
            "benchmark_as_of_date": self.benchmark_as_of_date,
            "latest_completed_session": self.latest_completed_session,
            "data_freshness": dict(self.data_freshness or {}),
            "collector_diagnostics": dict(self.collector_diagnostics or {}),
            "cooldown_snapshot": dict(self.cooldown_snapshot or {}),
            "last_retryable_error": self.last_retryable_error,
            "market_truth_mode": self.market_truth_mode,
            "fallback_reason": self.fallback_reason,
        }


@dataclass(frozen=True)
class _ScreeningStageSpec:
    step_number: int
    total_steps: int
    label: str
    market: str
    action: Callable[[RuntimeContext | None], Any]
    parent_context: RuntimeContext | None = None


def _env_flag_enabled(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _env_int_default(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _screening_stage_parallel_enabled() -> bool:
    return _env_flag_enabled("INVEST_PROTO_SCREENING_STAGE_PARALLEL", default=True)


def _screening_stage_worker_count(total_items: int) -> int:
    if total_items <= 1:
        return 1
    default = min(4, os.cpu_count() or 1, total_items)
    configured = _env_int_default("INVEST_PROTO_SCREENING_STAGE_WORKERS", default)
    return max(1, min(configured, total_items))


def _screening_shared_ohlcv_cache_enabled() -> bool:
    return _env_flag_enabled("INVEST_PROTO_SCREENING_SHARED_OHLCV_CACHE", default=True)


def _screening_shared_ohlcv_symbols(market: str) -> list[str]:
    normalized_market = require_market_key(market)
    data_dir = get_market_data_dir(normalized_market)
    if not os.path.isdir(data_dir):
        return []
    symbols = sorted(
        {
            os.path.splitext(name)[0].strip().upper()
            for name in os.listdir(data_dir)
            if name.endswith(".csv")
            and not is_index_symbol(normalized_market, os.path.splitext(name)[0].upper())
            and os.path.splitext(name)[0].strip()
        }
    )
    return limit_runtime_symbols(symbols)


def _frame_map_memory_bytes(frames: dict[str, pd.DataFrame]) -> int:
    total = 0
    for frame in frames.values():
        if frame is None or frame.empty:
            continue
        try:
            total += int(frame.memory_usage(deep=True).sum())
        except Exception:
            continue
    return total


def _set_runtime_section_payload(
    runtime_context: RuntimeContext | None,
    section: str,
    payload: dict[str, Any],
) -> None:
    if runtime_context is None:
        return
    for key, value in payload.items():
        runtime_context.set_runtime_metric(section, key, value)


def _preload_shared_screening_ohlcv_cache(
    market: str,
    runtime_context: RuntimeContext | None,
) -> dict[str, Any]:
    if runtime_context is None:
        return {}
    started = time.perf_counter()
    normalized_market = require_market_key(market)
    if not _screening_shared_ohlcv_cache_enabled():
        payload = {
            "enabled": False,
            "status": "disabled",
            "symbols": 0,
            "loaded": 0,
            "seconds": 0.0,
            "cache_hits": 0,
            "cache_misses": 0,
            "bytes_estimate": 0,
        }
        _set_runtime_section_payload(runtime_context, "shared_ohlcv_cache", payload)
        return payload

    symbols = _screening_shared_ohlcv_symbols(normalized_market)
    cache_before = dict(runtime_context.cache_stats)
    try:
        frame_map = load_local_ohlcv_frames_ordered(
            normalized_market,
            symbols,
            as_of=runtime_context.as_of_date or None,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
            runtime_context=runtime_context,
            required_columns=SCREENING_OHLCV_READ_COLUMNS,
            worker_scope="shared_ohlcv_cache.preload",
            load_frame_fn=load_local_ohlcv_frame,
        )
        elapsed = time.perf_counter() - started
        loaded = sum(1 for frame in frame_map.values() if frame is not None and not frame.empty)
        payload = {
            "enabled": True,
            "status": "ok",
            "symbols": int(len(symbols)),
            "loaded": int(loaded),
            "seconds": round(float(elapsed), 6),
            "cache_hits": max(0, int(runtime_context.cache_stats.get("hits", 0)) - int(cache_before.get("hits", 0))),
            "cache_misses": max(0, int(runtime_context.cache_stats.get("misses", 0)) - int(cache_before.get("misses", 0))),
            "bytes_estimate": _frame_map_memory_bytes(frame_map),
        }
        runtime_context.add_timing("shared_ohlcv_cache.seconds", elapsed)
        _set_runtime_section_payload(runtime_context, "shared_ohlcv_cache", payload)
        runtime_context.update_runtime_state(
            current_stage="Shared OHLCV cache preload",
            current_symbol="",
            current_chunk=f"shared_ohlcv_cache:{loaded}/{len(symbols)}",
            status="running",
        )
        return payload
    except Exception as exc:
        elapsed = time.perf_counter() - started
        payload = {
            "enabled": True,
            "status": "fallback",
            "symbols": int(len(symbols)),
            "loaded": 0,
            "seconds": round(float(elapsed), 6),
            "cache_hits": max(0, int(runtime_context.cache_stats.get("hits", 0)) - int(cache_before.get("hits", 0))),
            "cache_misses": max(0, int(runtime_context.cache_stats.get("misses", 0)) - int(cache_before.get("misses", 0))),
            "bytes_estimate": 0,
            "error": str(exc),
        }
        runtime_context.add_timing("shared_ohlcv_cache.seconds", elapsed)
        _set_runtime_section_payload(runtime_context, "shared_ohlcv_cache", payload)
        print(
            f"[Task] Shared OHLCV cache preload fallback ({normalized_market}) - {exc}"
        )
        return payload


def _copy_runtime_value(value: Any) -> Any:
    if hasattr(value, "copy"):
        return value.copy()
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        return list(value)
    return value


def _elapsed_since_timestamp(started_at: str) -> float | None:
    text = str(started_at or "").strip()
    if not text:
        return None
    try:
        started = datetime.fromisoformat(text)
    except ValueError:
        return None
    return max(0.0, (datetime.now() - started).total_seconds())


def _parallel_stage_progress_payload(
    label: str,
    progress: dict[str, Any] | None,
    *,
    outcome: TaskStepOutcome | None = None,
) -> dict[str, Any]:
    source = dict(progress or {})
    now_text = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    started_at = str(
        source.get("started_at")
        or (outcome.started_at if outcome is not None else "")
        or now_text
    )
    last_progress_at = str(
        source.get("last_progress_at")
        or (outcome.finished_at if outcome is not None else "")
        or now_text
    )
    elapsed = (
        float(outcome.elapsed_seconds)
        if outcome is not None
        else _elapsed_since_timestamp(started_at)
    )
    payload = {
        "started_at": started_at,
        "last_progress_at": last_progress_at,
        "elapsed_seconds": round(float(elapsed or 0.0), 6),
        "status": str(
            (outcome.status if outcome is not None else "")
            or source.get("status")
            or "running"
        ),
        "current_stage": str(source.get("current_stage") or label),
        "current_symbol": str(
            (outcome.current_symbol if outcome is not None and outcome.current_symbol else "")
            or source.get("current_symbol")
            or ""
        ),
        "current_chunk": str(
            (outcome.current_chunk if outcome is not None and outcome.current_chunk else "")
            or source.get("current_chunk")
            or ""
        ),
        "timings": dict(outcome.timings or {}) if outcome is not None else {},
    }
    if outcome is not None:
        payload["ok"] = bool(outcome.ok)
        payload["error_code"] = str(outcome.error_code or "")
        payload["error_detail"] = str(outcome.error_detail or "")
    return payload


def _record_parallel_stage_progress(
    parent: RuntimeContext | None,
    label: str,
    progress: dict[str, Any] | None = None,
    *,
    outcome: TaskStepOutcome | None = None,
) -> None:
    if parent is None:
        return
    with _PARALLEL_STAGE_PROGRESS_LOCK:
        existing = (
            dict(parent.runtime_metrics.get("parallel_stages") or {})
            if isinstance(parent.runtime_metrics.get("parallel_stages"), dict)
            else {}
        )
        stage_payload = dict(existing.get(label) or {})
        stage_payload.update(
            _parallel_stage_progress_payload(label, progress, outcome=outcome)
        )
        existing[label] = stage_payload
        parent.runtime_metrics["parallel_stages"] = existing
        parent.update_runtime_state(
            parallel_stages=existing,
            current_stage=f"Parallel: {label}",
            current_symbol=stage_payload.get("current_symbol", ""),
            current_chunk=stage_payload.get("current_chunk", ""),
            status="running" if outcome is None else stage_payload.get("status", "ok"),
        )


def _make_child_runtime_context(
    parent: RuntimeContext | None,
    *,
    market: str,
    label: str,
) -> RuntimeContext | None:
    if parent is None:
        return None
    child = RuntimeContext(
        market=market,
        as_of_date=parent.as_of_date,
        run_id=f"{parent.run_id}:{label}" if parent.run_id else "",
    )
    child.metadata_map = {key: dict(value) for key, value in parent.metadata_map.items()}
    child.financial_map = {key: dict(value) for key, value in parent.financial_map.items()}
    child.source_registry_snapshot = (
        dict(parent.source_registry_snapshot)
        if isinstance(parent.source_registry_snapshot, dict)
        else parent.source_registry_snapshot
    )
    child.screening_frames = {
        key: _copy_runtime_value(value)
        for key, value in parent.screening_frames.items()
    }
    child.runtime_state = dict(parent.runtime_state)
    child.ohlcv_frame_cache = parent.ohlcv_frame_cache

    def _child_progress_callback(payload: dict[str, Any]) -> None:
        _record_parallel_stage_progress(parent, label, payload)

    child.bind_progress_callback(_child_progress_callback, run_id=child.run_id)
    return child


def _merge_data_freshness_payloads(
    base: dict[str, Any] | None,
    incoming: dict[str, Any] | None,
) -> dict[str, Any]:
    base_stages = (
        dict(base.get("stages") or {})
        if isinstance(base, dict) and isinstance(base.get("stages"), dict)
        else {}
    )
    incoming_stages = (
        dict(incoming.get("stages") or {})
        if isinstance(incoming, dict) and isinstance(incoming.get("stages"), dict)
        else {}
    )
    stages = {**base_stages, **incoming_stages}
    counts = {status: 0 for status in _DATA_FRESHNESS_STATUSES}
    for payload in stages.values():
        if not isinstance(payload, dict):
            continue
        raw_counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
        for status in _DATA_FRESHNESS_STATUSES:
            counts[status] += int(raw_counts.get(status, 0) or 0)
    return {"counts": counts, "stages": stages}


def _merge_child_runtime_context(
    parent: RuntimeContext | None,
    child: RuntimeContext | None,
) -> None:
    if parent is None or child is None:
        return
    for key, value in child.timings.items():
        parent.timings[key] = round(
            float(parent.timings.get(key, 0.0)) + float(value or 0.0),
            6,
        )
    for key, value in child.cache_stats.items():
        parent.cache_stats[key] = int(parent.cache_stats.get(key, 0)) + int(value or 0)
    parent.runtime_metrics = _merge_runtime_metrics(parent.runtime_metrics, child.runtime_metrics)
    parent.rows_read += max(int(child.rows_read), 0)
    parent.rows_written += max(int(child.rows_written), 0)
    if child.ohlcv_frame_cache is not parent.ohlcv_frame_cache:
        parent.ohlcv_frame_cache.update(child.ohlcv_frame_cache)
    for key, value in child.screening_frames.items():
        parent.screening_frames[key] = _copy_runtime_value(value)
    incoming_freshness = child.runtime_state.get("data_freshness")
    if isinstance(incoming_freshness, dict):
        parent.update_runtime_state(
            data_freshness=_merge_data_freshness_payloads(
                parent.runtime_state.get("data_freshness")
                if isinstance(parent.runtime_state.get("data_freshness"), dict)
                else None,
                incoming_freshness,
            )
        )


def _run_screening_stage_spec(
    spec: _ScreeningStageSpec,
) -> tuple[TaskStepOutcome, RuntimeContext | None]:
    runtime_context = _make_child_runtime_context(
        spec.parent_context,
        market=spec.market,
        label=spec.label,
    )
    outcome = _run_timed_step(
        spec.step_number,
        spec.total_steps,
        spec.label,
        spec.market,
        lambda: spec.action(runtime_context),
        runtime_context=runtime_context,
    )
    return outcome, runtime_context


def _run_screening_stage_specs(
    specs: list[_ScreeningStageSpec],
    *,
    parallel: bool,
) -> list[TaskStepOutcome]:
    if not specs:
        return []
    if not parallel or _screening_stage_worker_count(len(specs)) <= 1:
        outcomes: list[TaskStepOutcome] = []
        for spec in specs:
            outcome, child_context = _run_screening_stage_spec(spec)
            _merge_child_runtime_context(spec.parent_context, child_context)
            outcomes.append(outcome)
        return outcomes

    workers = _screening_stage_worker_count(len(specs))
    outcomes_by_index: dict[int, TaskStepOutcome] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_by_index = {
            executor.submit(_run_screening_stage_spec, spec): index
            for index, spec in enumerate(specs)
        }
        for future in as_completed(future_by_index):
            index = future_by_index[future]
            spec = specs[index]
            outcome, child_context = future.result()
            outcomes_by_index[index] = outcome
            _merge_child_runtime_context(spec.parent_context, child_context)
            _record_parallel_stage_progress(
                spec.parent_context,
                spec.label,
                dict(child_context.runtime_state) if child_context is not None else {},
                outcome=outcome,
            )
            stage_metrics = (
                dict(spec.parent_context.runtime_metrics.get("parallel_stages", {})).get(spec.label, {})
                if spec.parent_context is not None
                and isinstance(spec.parent_context.runtime_metrics.get("parallel_stages"), dict)
                else {}
            )
            if stage_metrics:
                outcome.runtime_metrics = _merge_runtime_metrics(
                    outcome.runtime_metrics,
                    {"parallel_stages": {spec.label: stage_metrics}},
                )
    shared_attached_for_parent: set[int] = set()
    for index, spec in enumerate(specs):
        parent = spec.parent_context
        if parent is None or id(parent) in shared_attached_for_parent:
            continue
        shared_metrics = parent.runtime_metrics.get("shared_ohlcv_cache")
        if isinstance(shared_metrics, dict) and shared_metrics:
            outcomes_by_index[index].runtime_metrics = _merge_runtime_metrics(
                {"shared_ohlcv_cache": dict(shared_metrics)},
                outcomes_by_index[index].runtime_metrics,
            )
            shared_attached_for_parent.add(id(parent))
    return [outcomes_by_index[index] for index in range(len(specs))]



def _normalize_markets(markets: Optional[list[str]] = None) -> list[str]:
    normalized_markets: list[str] = []
    for item in markets or ["us"]:
        normalized = require_market_key(item)
        if normalized not in normalized_markets:
            normalized_markets.append(normalized)
    return normalized_markets


def _get_runtime_context(
    runtime_contexts: dict[str, RuntimeContext] | None,
    market: str,
) -> RuntimeContext | None:
    if runtime_contexts is None:
        return None
    normalized_market = require_market_key(market)
    runtime_context = runtime_contexts.get(normalized_market)
    if runtime_context is None:
        runtime_context = RuntimeContext(market=normalized_market)
        runtime_contexts[normalized_market] = runtime_context
    _ensure_runtime_context_state(runtime_context, market=normalized_market)
    return runtime_context


def _latest_benchmark_as_of(market: str) -> str:
    try:
        from utils.market_runtime import get_benchmark_candidates

        _symbol, frame = load_benchmark_data(
            market,
            get_benchmark_candidates(market),
            allow_yfinance_fallback=False,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
        )
    except Exception:
        return ""
    if frame is None or frame.empty or "date" not in frame.columns:
        return ""
    latest = None
    try:
        latest = max(pd.to_datetime(frame["date"], errors="coerce").dropna())
    except Exception:
        return ""
    if latest is None:
        return ""
    return pd.Timestamp(latest).date().isoformat()


def _initialize_runtime_context_as_of(
    runtime_contexts: dict[str, RuntimeContext],
    markets: list[str],
    *,
    explicit_as_of: str | None = None,
) -> None:
    for market in markets:
        normalized_market = require_market_key(market)
        runtime_context = runtime_contexts.setdefault(
            normalized_market,
            RuntimeContext(market=normalized_market),
        )
        benchmark_as_of = _latest_benchmark_as_of(normalized_market)
        resolution = resolve_latest_completed_as_of(
            normalized_market,
            explicit_as_of=explicit_as_of,
            benchmark_as_of=benchmark_as_of,
        )
        runtime_context.set_as_of_date(resolution.as_of_date)
        runtime_context.runtime_state.update(
            {
                "market": normalized_market,
                "as_of_date": resolution.as_of_date,
                "latest_completed_session": resolution.latest_completed_session,
                "benchmark_as_of_date": resolution.benchmark_as_of_date,
                "freshness_status": resolution.freshness_status,
                "freshness_reason": resolution.reason,
            }
        )


def _resolve_market_truth_probe_as_of(
    market: str,
    *,
    runtime_context: RuntimeContext | None = None,
    explicit_as_of: str | None = None,
) -> str:
    explicit_text = str(explicit_as_of or "").strip()
    if explicit_text:
        return explicit_text
    context_as_of = (
        str(runtime_context.as_of_date or "").strip()
        if runtime_context is not None
        else ""
    )
    if context_as_of:
        return context_as_of
    benchmark_as_of = _latest_benchmark_as_of(market)
    resolution = resolve_latest_completed_as_of(
        market,
        explicit_as_of=None,
        benchmark_as_of=benchmark_as_of,
    )
    return str(resolution.as_of_date or "").strip()


def _resolve_effective_standalone(
    market: str,
    *,
    standalone: bool,
    runtime_context: RuntimeContext | None = None,
    explicit_as_of: str | None = None,
) -> bool:
    normalized_market = require_market_key(market)
    resolved_as_of = _resolve_market_truth_probe_as_of(
        normalized_market,
        runtime_context=runtime_context,
        explicit_as_of=explicit_as_of,
    )
    if runtime_context is not None and resolved_as_of:
        runtime_context.set_as_of_date(resolved_as_of)

    cached_as_of = ""
    cached_mode = ""
    cached_fallback_reason = ""
    cached_compat_availability = ""
    if runtime_context is not None:
        cached_as_of = str(runtime_context.runtime_state.get("market_truth_probe_as_of") or "").strip()
        cached_mode = str(runtime_context.runtime_state.get("market_truth_mode") or "").strip()
        cached_fallback_reason = str(runtime_context.runtime_state.get("fallback_reason") or "").strip()
        cached_compat_availability = str(runtime_context.runtime_state.get("compat_availability") or "").strip()

    if standalone:
        if cached_as_of == resolved_as_of and cached_mode in {"standalone_auto", "standalone_manual"}:
            runtime_context.update_runtime_state(
                market_truth_mode=cached_mode,
                fallback_reason=cached_fallback_reason,
                compat_availability=cached_compat_availability or (
                    "manual" if cached_mode == "standalone_manual" else cached_fallback_reason or "missing"
                ),
                market_truth_probe_as_of=resolved_as_of,
                effective_standalone=True,
            )
            return True
        if runtime_context is not None:
            runtime_context.update_runtime_state(
                market_truth_mode="standalone_manual",
                fallback_reason="",
                compat_availability="manual",
                market_truth_probe_as_of=resolved_as_of,
                effective_standalone=True,
            )
        return True

    if runtime_context is not None:
        if cached_as_of == resolved_as_of and cached_mode in {
            "compat",
            "standalone_auto",
            "standalone_manual",
        }:
            return cached_mode != "compat"

    probe = probe_market_intel_compat_availability(
        normalized_market,
        as_of_date=resolved_as_of,
    )
    market_truth_mode = "compat" if probe.status == "compat" else "standalone_auto"
    fallback_reason = "" if probe.status == "compat" else probe.status
    effective_standalone = market_truth_mode != "compat"
    if runtime_context is not None:
        runtime_context.update_runtime_state(
            market_truth_mode=market_truth_mode,
            fallback_reason=fallback_reason,
            compat_availability=probe.status,
            market_truth_probe_as_of=resolved_as_of,
            effective_standalone=effective_standalone,
        )
    return effective_standalone


def _write_runtime_state(path: str, payload: dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
    except OSError as exc:
        if path not in _RUNTIME_STATE_WRITE_WARNED:
            _RUNTIME_STATE_WRITE_WARNED.add(path)
            print(f"[Task] Runtime state write skipped - path={path}, error={exc}")


def _runtime_state_writer_loop() -> None:
    while True:
        with _RUNTIME_STATE_CONDITION:
            while not _RUNTIME_STATE_DIRTY:
                _RUNTIME_STATE_CONDITION.wait()
            path = next(iter(_RUNTIME_STATE_DIRTY))
            payload = dict(_RUNTIME_STATE_PENDING.get(path) or {})
            _RUNTIME_STATE_DIRTY.discard(path)
            _RUNTIME_STATE_INFLIGHT.add(path)
        try:
            _write_runtime_state(path, payload)
        finally:
            with _RUNTIME_STATE_CONDITION:
                _RUNTIME_STATE_INFLIGHT.discard(path)
                _RUNTIME_STATE_CONDITION.notify_all()


def _ensure_runtime_state_writer_started() -> None:
    global _RUNTIME_STATE_WRITER_THREAD
    with _RUNTIME_STATE_CONDITION:
        if _RUNTIME_STATE_WRITER_THREAD is not None and _RUNTIME_STATE_WRITER_THREAD.is_alive():
            return
        _RUNTIME_STATE_WRITER_THREAD = threading.Thread(
            target=_runtime_state_writer_loop,
            name="runtime-state-writer",
            daemon=True,
        )
        _RUNTIME_STATE_WRITER_THREAD.start()


def _schedule_runtime_state_write(path: str, payload: dict[str, Any]) -> None:
    _ensure_runtime_state_writer_started()
    with _RUNTIME_STATE_CONDITION:
        _RUNTIME_STATE_PENDING[path] = dict(payload)
        _RUNTIME_STATE_DIRTY.add(path)
        _RUNTIME_STATE_CONDITION.notify_all()


def _flush_runtime_state_writes(paths: list[str] | None = None) -> None:
    target_paths = {str(path).strip() for path in list(paths or []) if str(path).strip()}
    with _RUNTIME_STATE_CONDITION:
        while True:
            active_paths = set(_RUNTIME_STATE_DIRTY) | set(_RUNTIME_STATE_INFLIGHT)
            if target_paths:
                if not (active_paths & target_paths):
                    return
            elif not active_paths:
                return
            _RUNTIME_STATE_CONDITION.wait(timeout=0.05)


def _runtime_state_paths_for_markets(markets: list[str]) -> list[str]:
    return [get_runtime_state_path(market) for market in _normalize_markets(markets)]


def _ensure_runtime_context_state(
    runtime_context: RuntimeContext,
    *,
    market: str,
) -> RuntimeContext:
    normalized_market = require_market_key(market)
    started_at = str(
        runtime_context.runtime_state.get("started_at")
        or datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    )
    if not runtime_context.run_id:
        runtime_context.run_id = f"{normalized_market}:{started_at}"
    path = get_runtime_state_path(normalized_market)

    def _callback(payload: dict[str, Any]) -> None:
        _schedule_runtime_state_write(path, payload)

    runtime_context.bind_progress_callback(_callback, run_id=runtime_context.run_id)
    if not runtime_context.runtime_state:
        runtime_context.update_runtime_state(
            as_of_date=runtime_context.as_of_date,
            started_at=started_at,
            current_stage="",
            last_successful_stage="",
            current_symbol="",
            current_chunk="",
            last_error_code="",
            last_error_detail="",
            last_retryable_error="",
            cooldown_snapshot={},
            status="initialized",
        )
    return runtime_context


def _result_status(result: Any) -> str:
    if isinstance(result, dict):
        status = str(result.get("status") or "").strip().lower()
        if status:
            return status
        if result.get("ok") is False or str(result.get("error") or "").strip():
            return "failed"
        return "ok"
    return "ok"


def _status_counts(result: Any, *, status: str) -> dict[str, int]:
    if isinstance(result, dict) and isinstance(result.get("status_counts"), dict):
        return {
            str(key): int(value)
            for key, value in result["status_counts"].items()
            if str(key).strip()
        }
    return {status: 1}


def _result_cache_stats(result: Any) -> dict[str, int]:
    if isinstance(result, dict) and isinstance(result.get("cache_stats"), dict):
        return {
            str(key): int(value)
            for key, value in result["cache_stats"].items()
            if str(key).strip()
        }
    return {}


def _result_timings(result: Any) -> dict[str, float]:
    if isinstance(result, dict) and isinstance(result.get("timings"), dict):
        return {
            str(key): float(value)
            for key, value in result["timings"].items()
            if str(key).strip()
        }
    attrs = getattr(result, "attrs", None)
    if isinstance(attrs, dict) and isinstance(attrs.get("timings"), dict):
        return {
            str(key): float(value)
            for key, value in attrs["timings"].items()
            if str(key).strip()
        }
    return {}


def _result_runtime_metrics(result: Any) -> dict[str, Any]:
    if isinstance(result, dict) and isinstance(result.get("runtime_metrics"), dict):
        return dict(result["runtime_metrics"])
    attrs = getattr(result, "attrs", None)
    if isinstance(attrs, dict) and isinstance(attrs.get("runtime_metrics"), dict):
        return dict(attrs["runtime_metrics"])
    return {}


def _merge_runtime_metrics(*payloads: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}

    def _merge_value(left: Any, right: Any) -> Any:
        if isinstance(left, dict) and isinstance(right, dict):
            nested = dict(left)
            for nested_key, nested_value in right.items():
                nested[nested_key] = _merge_value(nested.get(nested_key), nested_value)
            return nested
        if isinstance(left, list) and isinstance(right, list):
            return (left + right)[:20]
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            total = float(left) + float(right)
            return round(total, 6) if isinstance(left, float) or isinstance(right, float) else int(total)
        if left is None:
            if isinstance(right, dict):
                return dict(right)
            if isinstance(right, list):
                return list(right)
            return right
        if right in (None, "", {}, []):
            return left
        return right

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key, value in payload.items():
            merged[key] = _merge_value(merged.get(key), value)
    return merged


def _runtime_metrics_delta(
    before: dict[str, Any],
    after: dict[str, Any],
) -> dict[str, Any]:
    def _delta_value(previous: Any, current: Any) -> Any:
        if isinstance(current, dict):
            previous_dict = previous if isinstance(previous, dict) else {}
            nested: dict[str, Any] = {}
            for key, value in current.items():
                delta = _delta_value(previous_dict.get(key), value)
                if delta not in (None, {}, [], 0, 0.0, ""):
                    nested[key] = delta
            return nested
        if isinstance(current, list):
            previous_list = previous if isinstance(previous, list) else []
            if len(current) <= len(previous_list):
                return []
            return current[len(previous_list) :][:20]
        if isinstance(current, (int, float)):
            previous_number = previous if isinstance(previous, (int, float)) else 0
            delta = float(current) - float(previous_number)
            if isinstance(current, float) or isinstance(previous_number, float):
                return round(delta, 6)
            return int(delta)
        if current != previous:
            return current
        return None

    payload: dict[str, Any] = {}
    for key, value in after.items():
        delta = _delta_value(before.get(key), value)
        if delta not in (None, {}, [], 0, 0.0, ""):
            payload[key] = delta
    return payload


def _result_collector_diagnostics(result: Any) -> dict[str, Any]:
    if isinstance(result, dict) and isinstance(result.get("collector_diagnostics"), dict):
        return dict(result["collector_diagnostics"])
    attrs = getattr(result, "attrs", None)
    if isinstance(attrs, dict) and isinstance(attrs.get("collector_diagnostics"), dict):
        return dict(attrs["collector_diagnostics"])
    return {}


def _result_rows_read(result: Any) -> int:
    if isinstance(result, dict):
        return int(result.get("rows_read") or 0)
    return 0


def _result_rows_written(result: Any) -> int:
    if isinstance(result, dict):
        return int(result.get("rows_written") or 0)
    if hasattr(result, "shape"):
        try:
            return int(result.shape[0])  # type: ignore[index]
        except Exception:
            return 0
    if isinstance(result, (list, tuple, set)):
        return len(result)
    return 0


def _result_retryable(result: Any, *, status: str) -> bool:
    if isinstance(result, dict) and "retryable" in result:
        return bool(result.get("retryable"))
    return status in {"partial", "soft_unavailable", "rate_limited"}


def _result_error_code(result: Any, *, status: str) -> str:
    if isinstance(result, dict):
        code = str(result.get("error_code") or "").strip()
        if code:
            return code
        if status == "failed":
            return "failed"
    return ""


def _result_error_detail(result: Any) -> str:
    if isinstance(result, dict):
        detail = str(result.get("error_detail") or result.get("error") or "").strip()
        if detail:
            return detail
    return ""


def _aggregate_status_counts(outcomes: list[TaskStepOutcome]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for outcome in outcomes:
        for key, value in (outcome.status_counts or {}).items():
            counts[key] = counts.get(key, 0) + int(value)
    return counts


def _aggregate_cache_stats(outcomes: list[TaskStepOutcome]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for outcome in outcomes:
        for key, value in (outcome.cache_stats or {}).items():
            counts[key] = counts.get(key, 0) + int(value)
    return counts


def _aggregate_timings(outcomes: list[TaskStepOutcome]) -> dict[str, float]:
    timings: dict[str, float] = {}
    for outcome in outcomes:
        for key, value in (outcome.timings or {}).items():
            timings[key] = timings.get(key, 0.0) + float(value)
    return {key: round(value, 6) for key, value in timings.items()}


def _aggregate_runtime_metrics(outcomes: list[TaskStepOutcome]) -> dict[str, Any]:
    return _merge_runtime_metrics(
        *[
            outcome.runtime_metrics
            for outcome in outcomes
            if isinstance(outcome.runtime_metrics, dict)
        ]
    )


def _derived_runtime_sections(
    timings: dict[str, float],
    runtime_metrics: dict[str, Any],
) -> dict[str, Any]:
    sections = {
        "frame_load": dict(runtime_metrics.get("frame_load") or {}),
        "feature_analysis": dict(runtime_metrics.get("feature_analysis") or {}),
        "output_persist": dict(runtime_metrics.get("output_persist") or {}),
        "augment": dict(runtime_metrics.get("augment") or {}),
        "worker_budget": dict(runtime_metrics.get("worker_budget") or {}),
        "parallel_stages": dict(runtime_metrics.get("parallel_stages") or {}),
        "shared_ohlcv_cache": dict(runtime_metrics.get("shared_ohlcv_cache") or {}),
    }
    for key, value in timings.items():
        if ".frame_load_seconds" in key:
            sections["frame_load"]["seconds"] = round(
                float(sections["frame_load"].get("seconds", 0.0) or 0.0) + float(value),
                6,
            )
        elif key.endswith(".feature_analysis_seconds") or key.endswith(".symbol_analysis_seconds") or key.endswith(".context_build_seconds"):
            sections["feature_analysis"]["seconds"] = round(
                float(sections["feature_analysis"].get("seconds", 0.0) or 0.0) + float(value),
                6,
            )
        elif key.endswith(".persist_outputs_seconds"):
            sections["output_persist"]["seconds"] = round(
                float(sections["output_persist"].get("seconds", 0.0) or 0.0) + float(value),
                6,
            )
        elif key in {
            "stumpy_seconds",
            "lag_diagnostics_seconds",
            "chronos2_seconds",
            "timesfm2p5_seconds",
        }:
            module = key.removesuffix("_seconds")
            module_payload = sections["augment"].setdefault(module, {})
            if isinstance(module_payload, dict):
                module_payload["seconds"] = round(float(value), 6)
    return sections


def _aggregate_collector_diagnostics(outcomes: list[TaskStepOutcome]) -> dict[str, Any]:
    return merge_collector_diagnostics(
        [
            outcome.collector_diagnostics
            for outcome in outcomes
            if isinstance(outcome.collector_diagnostics, dict)
        ]
    )


def _aggregate_data_freshness(outcomes: list[TaskStepOutcome]) -> dict[str, Any]:
    statuses = ("closed", "stale", "future_or_partial", "empty")
    stages: dict[str, Any] = {}
    for outcome in outcomes:
        payload = outcome.data_freshness
        if not isinstance(payload, dict):
            continue
        raw_stages = payload.get("stages")
        if not isinstance(raw_stages, dict):
            continue
        for key, value in raw_stages.items():
            if str(key).strip() and isinstance(value, dict):
                stages[str(key)] = dict(value)
    counts = {status: 0 for status in statuses}
    for stage_payload in stages.values():
        stage_counts = stage_payload.get("counts") if isinstance(stage_payload.get("counts"), dict) else {}
        for status in statuses:
            counts[status] += int(stage_counts.get(status, 0) or 0)
    return {"counts": counts, "stages": stages} if stages else {}


def _aggregate_market_truth_modes(outcomes: list[TaskStepOutcome]) -> dict[str, str]:
    modes: dict[str, str] = {}
    for outcome in outcomes:
        mode = str(outcome.market_truth_mode or "").strip()
        if mode:
            modes[outcome.market] = mode
    return modes


def _aggregate_fallback_reasons(outcomes: list[TaskStepOutcome]) -> dict[str, str]:
    reasons: dict[str, str] = {}
    for outcome in outcomes:
        reason = str(outcome.fallback_reason or "").strip()
        if reason:
            reasons[outcome.market] = reason
    return reasons


def _write_runtime_profiles(label: str, outcomes: list[TaskStepOutcome]) -> None:
    by_market: dict[str, list[TaskStepOutcome]] = {}
    for outcome in outcomes:
        by_market.setdefault(outcome.market, []).append(outcome)
    for market, market_outcomes in by_market.items():
        if market not in {"us", "kr"}:
            continue
        generated_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        started_at = next(
            (outcome.started_at for outcome in market_outcomes if outcome.started_at),
            "",
        ) or generated_at
        last_outcome = market_outcomes[-1] if market_outcomes else None
        last_progress_at = next(
            (
                outcome.finished_at
                for outcome in reversed(market_outcomes)
                if outcome.finished_at
            ),
            "",
        ) or generated_at
        last_successful = next(
            (outcome for outcome in reversed(market_outcomes) if outcome.ok),
            None,
        )
        last_error = next(
            (
                outcome
                for outcome in reversed(market_outcomes)
                if outcome.error_code or outcome.error_detail or not outcome.ok
            ),
            None,
        )
        payload = {
            "run_id": f"{market}:{label}:{started_at}",
            "label": label,
            "market": market,
            "as_of_date": next(
                (
                    outcome.as_of_date
                    for outcome in reversed(market_outcomes)
                    if outcome.as_of_date
                ),
                "",
            ),
            "freshness_status": next(
                (
                    outcome.freshness_status
                    for outcome in reversed(market_outcomes)
                    if outcome.freshness_status
                ),
                "",
            ),
            "freshness_reason": next(
                (
                    outcome.freshness_reason
                    for outcome in reversed(market_outcomes)
                    if outcome.freshness_reason
                ),
                "",
            ),
            "benchmark_as_of_date": next(
                (
                    outcome.benchmark_as_of_date
                    for outcome in reversed(market_outcomes)
                    if outcome.benchmark_as_of_date
                ),
                "",
            ),
            "latest_completed_session": next(
                (
                    outcome.latest_completed_session
                    for outcome in reversed(market_outcomes)
                    if outcome.latest_completed_session
                ),
                "",
            ),
            "market_truth_mode": next(
                (
                    outcome.market_truth_mode
                    for outcome in reversed(market_outcomes)
                    if outcome.market_truth_mode
                ),
                "",
            ),
            "fallback_reason": next(
                (
                    outcome.fallback_reason
                    for outcome in reversed(market_outcomes)
                    if outcome.fallback_reason
                ),
                "",
            ),
            "_runtime_metrics": _aggregate_runtime_metrics(market_outcomes),
            "generated_at": generated_at,
            "started_at": started_at,
            "last_progress_at": last_progress_at,
            "elapsed_seconds": round(
                sum(outcome.elapsed_seconds for outcome in market_outcomes),
                6,
            ),
            "last_stage": last_outcome.label if last_outcome is not None else "",
            "last_stage_status": last_outcome.status if last_outcome is not None else "",
            "last_successful_stage": (
                last_successful.label if last_successful is not None else ""
            ),
            "last_error_code": (
                last_error.error_code if last_error is not None else ""
            ),
            "last_error_detail": (
                last_error.error_detail if last_error is not None else ""
            ),
            "current_symbol": (
                last_outcome.current_symbol if last_outcome is not None else ""
            ),
            "current_chunk": (
                last_outcome.current_chunk if last_outcome is not None else ""
            ),
            "last_retryable_error": (
                last_error.last_retryable_error if last_error is not None else ""
            ),
            "cooldown_snapshot": (
                dict(last_outcome.cooldown_snapshot or {})
                if last_outcome is not None
                else {}
            ),
            "status_counts": _aggregate_status_counts(market_outcomes),
            "cache_stats": _aggregate_cache_stats(market_outcomes),
            "timings": _aggregate_timings(market_outcomes),
            "collector_diagnostics": _aggregate_collector_diagnostics(market_outcomes),
            "rows_read": sum(outcome.rows_read for outcome in market_outcomes),
            "rows_written": sum(outcome.rows_written for outcome in market_outcomes),
            "data_freshness": _aggregate_data_freshness(market_outcomes),
            "steps": [outcome.to_dict() for outcome in market_outcomes],
        }
        runtime_metrics = dict(payload.pop("_runtime_metrics") or {})
        payload["runtime_metrics"] = runtime_metrics
        payload.update(_derived_runtime_sections(payload["timings"], runtime_metrics))
        path = get_runtime_profile_path(market)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)


def write_full_run_summaries(
    label: str,
    summary: dict[str, Any],
    *,
    markets: list[str],
) -> None:
    generated_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    for market in _normalize_markets(markets):
        path = get_full_run_summary_path(market)
        payload = {
            "schema_version": "1.0",
            "generated_at": generated_at,
            "label": label,
            "market": market,
            "summary": summary,
        }
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
        except OSError as exc:
            print(f"[Task] Full-run summary write skipped ({market}) - {exc}")


def _build_and_store_source_registry_snapshot(
    market: str,
    runtime_context: RuntimeContext | None,
) -> dict[str, Any] | None:
    if runtime_context is None:
        return None
    from screeners.signals import source_registry as signal_source_registry
    from screeners.source_contracts import (
        CANONICAL_SOURCE_SPECS,
        primary_source_style,
        source_engine_bonus,
        source_priority_score,
        source_style_tags,
        source_tag_priority,
        sorted_source_tags,
        stage_priority,
    )
    as_of_date = ""
    if runtime_context is not None:
        as_of_date = str(runtime_context.as_of_date or "").strip()

    snapshot = signal_source_registry.build_source_registry_snapshot(
        screeners_root=get_market_screeners_root(market),
        market=market,
        source_specs=CANONICAL_SOURCE_SPECS,
        stage_priority=stage_priority,
        source_tag_priority=source_tag_priority,
        sorted_source_tags=sorted_source_tags,
        source_style_tags=source_style_tags,
        primary_source_style=primary_source_style,
        source_priority_score=source_priority_score,
        source_engine_bonus=source_engine_bonus,
        safe_text=lambda value: str(value or "").strip(),
        as_of_date=as_of_date or None,
    )
    signal_source_registry.write_source_registry_snapshot(
        get_market_source_registry_snapshot_path(market),
        snapshot,
    )
    runtime_context.source_registry_snapshot = snapshot
    runtime_context.set_as_of_date(snapshot.get("as_of_date"))
    runtime_context.record_cache_miss("source_registry_snapshot")
    runtime_context.update_runtime_state(
        current_stage="Source registry snapshot",
        current_symbol="",
        current_chunk="snapshot_write",
        last_successful_stage="Source registry snapshot",
        status="ok",
    )
    return snapshot



def _extract_error(result: Any) -> str:
    if isinstance(result, TaskStepOutcome):
        return result.error
    if isinstance(result, dict):
        error = str(result.get("error") or "").strip()
        if error:
            return error
        if result.get("ok") is False and "failed_steps" in result:
            return f"failed_steps={result.get('failed_steps')}"
    return ""



def _summarize_step_result(result: Any) -> str:
    if result is None:
        return ""
    if isinstance(result, TaskStepOutcome):
        return result.summary
    if hasattr(result, "shape"):
        try:
            rows = int(result.shape[0])  # type: ignore[index]
            return f"rows={rows}"
        except Exception:
            return ""
    if isinstance(result, dict):
        signal_summary = result.get("signal_summary")
        if isinstance(signal_summary, dict):
            counts = signal_summary.get("counts") or {}
            if isinstance(counts, dict):
                return (
                    f"as_of={signal_summary.get('as_of_date', '')}, "
                    f"buy_all={counts.get('buy_signals_all_symbols_v1', 0)}, "
                    f"sell_all={counts.get('sell_signals_all_symbols_v1', 0)}, "
                    f"buy_screened={counts.get('buy_signals_screened_symbols_v1', 0)}, "
                    f"sell_screened={counts.get('sell_signals_screened_symbols_v1', 0)}"
                )
        if result.get("ok") is False and "failed_steps" in result:
            return f"failed_steps={result.get('failed_steps')}"
        error = str(result.get("error") or "").strip()
        if error:
            return f"error={error}"
        for key in ("saved", "latest", "failed", "count", "total"):
            if key in result:
                return ", ".join(
                    f"{name}={result[name]}"
                    for name in ("total", "saved", "latest", "failed")
                    if name in result
                )
        return f"keys={len(result)}"
    if isinstance(result, (list, tuple, set)):
        return f"items={len(result)}"
    return ""



def _run_timed_step(
    step_number: int,
    total_steps: int,
    label: str,
    market: str,
    action: Callable[[], Any],
    runtime_context: RuntimeContext | None = None,
) -> TaskStepOutcome:
    print(f"[Task] Step {step_number}/{total_steps} - {label} ({market})")
    context_timings_before = dict(runtime_context.timings) if runtime_context is not None else {}
    context_cache_before = dict(runtime_context.cache_stats) if runtime_context is not None else {}
    context_runtime_metrics_before = (
        copy.deepcopy(runtime_context.runtime_metrics)
        if runtime_context is not None
        else {}
    )
    context_rows_read_before = int(runtime_context.rows_read) if runtime_context is not None else 0
    context_rows_written_before = int(runtime_context.rows_written) if runtime_context is not None else 0
    if runtime_context is not None:
        runtime_context.update_runtime_state(
            current_stage=label,
            current_symbol="",
            current_chunk="",
            status="running",
        )
    started_at_text = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    started_perf = time.perf_counter()
    try:
        result = action()
        status = _result_status(result)
        ok = status != "failed"
        if isinstance(result, dict) and result.get("ok") is False:
            ok = False
        error = _extract_error(result)
    except Exception as exc:
        result = None
        ok = False
        error = str(exc)
        status = "failed"
        print(f"[Task] Step {step_number}/{total_steps} raised ({market}) - {label}: {exc}")
        print(traceback.format_exc())
    elapsed = time.perf_counter() - started_perf
    summary = _summarize_step_result(result)
    summary_suffix = f" - {summary}" if summary else ""
    if ok:
        print(f"[Task] Step {step_number}/{total_steps} completed ({market}) - {label} in {elapsed:.1f}s{summary_suffix}")
    else:
        error_suffix = f" - {error}" if error and not summary else ""
        print(f"[Task] Step {step_number}/{total_steps} failed ({market}) - {label} in {elapsed:.1f}s{summary_suffix}{error_suffix}")
    finished_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    progress_state: dict[str, Any] = {}
    if runtime_context is not None:
        progress_state = dict(runtime_context.runtime_state)
    context_timings: dict[str, float] = {}
    context_cache_stats: dict[str, int] = {}
    context_runtime_metrics: dict[str, Any] = {}
    context_rows_read = 0
    context_rows_written = 0
    if runtime_context is not None:
        context_timings = {
            key: value
            for key, value in dict(runtime_context.timings).items()
            if context_timings_before.get(key) != value
        }
        context_cache_stats = {
            key: int(runtime_context.cache_stats.get(key, 0))
            - int(context_cache_before.get(key, 0))
            for key in set(runtime_context.cache_stats) | set(context_cache_before)
        }
        context_runtime_metrics = _runtime_metrics_delta(
            context_runtime_metrics_before,
            dict(runtime_context.runtime_metrics),
        )
        context_rows_read = max(0, int(runtime_context.rows_read) - context_rows_read_before)
        context_rows_written = max(0, int(runtime_context.rows_written) - context_rows_written_before)
    if runtime_context is not None:
        runtime_context.update_runtime_state(
            current_stage=label,
            current_symbol="",
            current_chunk="",
            last_successful_stage=label if ok else runtime_context.runtime_state.get("last_successful_stage", ""),
            last_error_code=_result_error_code(result, status=status),
            last_error_detail=_result_error_detail(result),
            last_retryable_error=(
                _result_error_detail(result)
                if _result_retryable(result, status=status)
                else ""
            ),
            status=status,
        )
    return TaskStepOutcome(
        ok=ok,
        label=label,
        market=market,
        elapsed_seconds=elapsed,
        summary=summary,
        error=error,
        result=result,
        status=status,
        status_counts=_status_counts(result, status=status),
        timings={**context_timings, **_result_timings(result)},
        cache_stats={**context_cache_stats, **_result_cache_stats(result)},
        runtime_metrics=_merge_runtime_metrics(
            context_runtime_metrics,
            _result_runtime_metrics(result),
        ),
        collector_diagnostics=_result_collector_diagnostics(result),
        rows_read=context_rows_read + _result_rows_read(result),
        rows_written=context_rows_written + _result_rows_written(result),
        retryable=_result_retryable(result, status=status),
        error_code=_result_error_code(result, status=status),
        error_detail=_result_error_detail(result),
        started_at=started_at_text,
        finished_at=finished_at,
        current_symbol=str(progress_state.get("current_symbol") or "").strip(),
        current_chunk=str(progress_state.get("current_chunk") or "").strip(),
        as_of_date=str(
            progress_state.get("as_of_date")
            or (runtime_context.as_of_date if runtime_context is not None else "")
        ).strip(),
        freshness_status=str(progress_state.get("freshness_status") or "").strip(),
        freshness_reason=str(progress_state.get("freshness_reason") or "").strip(),
        benchmark_as_of_date=str(progress_state.get("benchmark_as_of_date") or "").strip(),
        latest_completed_session=str(progress_state.get("latest_completed_session") or "").strip(),
        data_freshness=dict(progress_state.get("data_freshness") or {}),
        cooldown_snapshot=(
            dict(
                progress_state.get("cooldown_snapshot")
                or (
                    runtime_context.runtime_state.get("cooldown_snapshot")
                    if runtime_context is not None
                    else {}
                )
                or {}
            )
        ),
        last_retryable_error=(
            str(progress_state.get("last_retryable_error") or _result_error_detail(result)).strip()
            if _result_retryable(result, status=status)
            else ""
        ),
        market_truth_mode=str(
            progress_state.get("market_truth_mode")
            or (
                runtime_context.runtime_state.get("market_truth_mode")
                if runtime_context is not None
                else ""
            )
        ).strip(),
        fallback_reason=str(
            progress_state.get("fallback_reason")
            or (
                runtime_context.runtime_state.get("fallback_reason")
                if runtime_context is not None
                else ""
            )
        ).strip(),
    )


def _record_outcome(
    outcomes: list[TaskStepOutcome],
    *,
    label: str,
    outcome: TaskStepOutcome,
) -> None:
    outcomes.append(outcome)
    _write_runtime_profiles(label, outcomes)


def _set_process_wall_elapsed(
    summary: dict[str, Any],
    *,
    wall_elapsed_seconds: float,
) -> dict[str, Any]:
    wall_elapsed = round(float(wall_elapsed_seconds), 6)
    summary["elapsed_seconds"] = wall_elapsed
    timings = dict(summary.get("timings") or {})
    timings["process_total_seconds"] = wall_elapsed
    summary["timings"] = timings
    return summary


def _build_process_summary(label: str, outcomes: list[TaskStepOutcome]) -> dict[str, Any]:
    failed = [outcome for outcome in outcomes if not outcome.ok]
    elapsed = sum(outcome.elapsed_seconds for outcome in outcomes)
    summary = {
        "label": label,
        "ok": not failed,
        "status": "failed" if failed else "ok",
        "failed_steps": len(failed),
        "total_steps": len(outcomes),
        "elapsed_seconds": elapsed,
        "markets": [outcome.market for outcome in outcomes],
        "steps": [outcome.to_dict() for outcome in outcomes],
        "status_counts": _aggregate_status_counts(outcomes),
        "cache_stats": _aggregate_cache_stats(outcomes),
        "runtime_metrics": _aggregate_runtime_metrics(outcomes),
        "timings": {
            **_aggregate_timings(outcomes),
            "process_total_seconds": round(float(elapsed), 6),
        },
        "collector_diagnostics": _aggregate_collector_diagnostics(outcomes),
        "rows_read": sum(outcome.rows_read for outcome in outcomes),
        "rows_written": sum(outcome.rows_written for outcome in outcomes),
        "data_freshness": _aggregate_data_freshness(outcomes),
        "market_truth_modes": _aggregate_market_truth_modes(outcomes),
        "fallback_reasons": _aggregate_fallback_reasons(outcomes),
    }
    if failed:
        summary["error"] = "; ".join(
            f"{outcome.market}:{outcome.label}:{outcome.error or outcome.summary or 'failed'}"
            for outcome in failed
        )
        print(
            f"[Task] {label} completed with degraded status - "
            f"failed_steps={len(failed)}, total_steps={len(outcomes)}, elapsed={elapsed:.1f}s"
        )
    else:
        print(f"[Task] {label} completed - total_steps={len(outcomes)}, elapsed={elapsed:.1f}s")
    _write_runtime_profiles(label, outcomes)
    return summary



def _unexpected_process_summary(label: str, exc: Exception) -> dict[str, Any]:
    print(f"[Task] {label} failed: {exc}")
    print(traceback.format_exc())
    return {
        "label": label,
        "ok": False,
        "failed_steps": 1,
        "total_steps": 0,
        "elapsed_seconds": 0.0,
        "markets": [],
        "steps": [],
        "status": "failed",
        "status_counts": {"failed": 1},
        "cache_stats": {},
        "runtime_metrics": {},
        "timings": {"process_total_seconds": 0.0},
        "rows_read": 0,
        "rows_written": 0,
        "error": str(exc),
    }



def _combine_process_summaries(label: str, summaries: list[dict[str, Any]]) -> dict[str, Any]:
    valid_summaries = [summary for summary in summaries if summary]
    failed_steps = sum(int(summary.get("failed_steps", 0)) for summary in valid_summaries)
    total_steps = sum(int(summary.get("total_steps", 0)) for summary in valid_summaries)
    elapsed = sum(float(summary.get("elapsed_seconds", 0.0)) for summary in valid_summaries)
    combined = {
        "label": label,
        "ok": failed_steps == 0,
        "status": "failed" if failed_steps else "ok",
        "failed_steps": failed_steps,
        "total_steps": total_steps,
        "elapsed_seconds": elapsed,
        "summaries": valid_summaries,
        "status_counts": {
            key: sum(
                int(summary.get("status_counts", {}).get(key, 0))
                for summary in valid_summaries
            )
            for key in {
                status_key
                for summary in valid_summaries
                for status_key in summary.get("status_counts", {}).keys()
            }
        },
        "cache_stats": {
            key: sum(
                int(summary.get("cache_stats", {}).get(key, 0))
                for summary in valid_summaries
            )
            for key in {
                cache_key
                for summary in valid_summaries
                for cache_key in summary.get("cache_stats", {}).keys()
            }
        },
        "runtime_metrics": _merge_runtime_metrics(
            *[
                summary.get("runtime_metrics")
                for summary in valid_summaries
                if isinstance(summary.get("runtime_metrics"), dict)
            ]
        ),
        "timings": {"process_total_seconds": round(float(elapsed), 6)},
        "collector_diagnostics": merge_collector_diagnostics(
            [
                summary.get("collector_diagnostics")
                for summary in valid_summaries
                if isinstance(summary.get("collector_diagnostics"), dict)
            ]
        ),
        "rows_read": sum(int(summary.get("rows_read", 0)) for summary in valid_summaries),
        "rows_written": sum(
            int(summary.get("rows_written", 0)) for summary in valid_summaries
        ),
        "market_truth_modes": {
            str(market): str(mode)
            for summary in valid_summaries
            for market, mode in dict(summary.get("market_truth_modes") or {}).items()
            if str(market).strip() and str(mode).strip()
        },
        "fallback_reasons": {
            str(market): str(reason)
            for summary in valid_summaries
            for market, reason in dict(summary.get("fallback_reasons") or {}).items()
            if str(market).strip() and str(reason).strip()
        },
    }
    if failed_steps:
        combined["error"] = "; ".join(
            str(summary.get("error") or summary.get("label") or "failed")
            for summary in valid_summaries
            if summary.get("ok") is False
        )
        print(
            f"[Task] {label} completed with degraded status - "
            f"failed_steps={failed_steps}, total_steps={total_steps}, elapsed={elapsed:.1f}s"
        )
    else:
        print(f"[Task] {label} completed - total_steps={total_steps}, elapsed={elapsed:.1f}s")
    return combined



def run_stock_metadata_collection(*, market: str = "us") -> Any:
    normalized_market = require_market_key(market)
    print(f"\n[Task] Stock metadata collection started ({normalized_market})")
    try:
        from data_collectors.stock_metadata_collector import main as collect_stock_metadata_main

        result = collect_stock_metadata_main(market=normalized_market)
        row_count = len(result) if hasattr(result, "__len__") else 0
        print(f"[Task] Stock metadata collection completed ({normalized_market}) - rows={row_count}")
        return result
    except Exception as exc:
        print(f"[Task] Stock metadata collection failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def ensure_directories(
    *,
    markets: Optional[list[str]] = None,
    include_signal_dirs: bool = False,
    include_augment_dirs: bool = False,
) -> None:
    from utils.io_utils import create_required_dirs

    create_required_dirs()
    for market in _normalize_markets(markets):
        ensure_market_dirs(market, include_signal_dirs=include_signal_dirs)
        preflight_market_output_dirs(
            market,
            include_signal_dirs=include_signal_dirs,
            include_augment_dirs=include_augment_dirs,
        )



def collect_data_main(
    update_symbols: bool = True,
    skip_ohlcv: bool = False,
    include_kr: bool = False,
    include_us: bool = True,
    skip_us_ohlcv: bool = False,
) -> dict[str, Any]:
    print("\n[Task] Data collection started")
    try:
        from data_collector import collect_data

        steps: list[tuple[str, Callable[[], Any], bool]] = []
        if not skip_ohlcv:
            if include_us and not skip_us_ohlcv:
                steps.append(("US OHLCV", lambda: collect_data(update_symbols=update_symbols), True))
            if include_kr:
                steps.append(("KR OHLCV", lambda: run_kr_ohlcv_collection(include_etn=True), False))
        if include_us:
            steps.append(("US stock metadata", lambda: run_stock_metadata_collection(market="us"), True))
        if include_kr:
            steps.append(("KR stock metadata", lambda: run_stock_metadata_collection(market="kr"), False))

        outcomes: list[TaskStepOutcome] = []
        total_steps = len(steps)
        for index, (label, action, yahoo_backed) in enumerate(steps, start=1):
            if yahoo_backed:
                wait_for_yahoo_phase_handoff(label)
            _record_outcome(
                outcomes,
                label="Data collection",
                outcome=_run_timed_step(
                    index,
                    total_steps,
                    label,
                    "pipeline",
                    action,
                ),
            )

        return _build_process_summary("Data collection", outcomes)
    except Exception as exc:
        return _unexpected_process_summary("Data collection", exc)



def run_kr_ohlcv_collection(
    days: Optional[int] = None,
    include_kosdaq: bool = True,
    include_etf: bool = True,
    include_etn: bool = True,
    provider_mode: str = "yfinance_only",
) -> dict:
    try:
        import data_collectors.kr_ohlcv_collector as kr_ohlcv_collector

        effective_days = int(
            kr_ohlcv_collector.KR_OHLCV_DEFAULT_LOOKBACK_DAYS if days is None else days
        )

        summary = kr_ohlcv_collector.collect_kr_ohlcv_csv(
            days=effective_days,
            include_kosdaq=include_kosdaq,
            include_etf=include_etf,
            include_etn=include_etn,
            provider_mode=provider_mode,
        )
        soft_unavailable = summary.get("soft_unavailable", summary.get("skipped_empty", 0))
        delisted = summary.get("delisted", 0)
        print(
            "[Task] KR OHLCV completed - "
            f"source={summary.get('source')}, total={summary.get('total')}, "
            f"saved={summary.get('saved')}, latest={summary.get('latest')}, "
            f"kept_existing={summary.get('kept_existing')}, soft_unavailable={soft_unavailable}, "
            f"delisted={delisted}, rate_limited={summary.get('rate_limited')}, failed={summary.get('failed')}"
        )
        return summary
    except Exception as exc:
        print(f"[Task] KR OHLCV failed: {exc}")
        print(traceback.format_exc())
        return {"total": 0, "saved": 0, "failed": 0, "error": str(exc)}



def run_markminervini_screening(
    *,
    market: str,
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
) -> Any:
    normalized_market = require_market_key(market)
    _ = standalone
    from screeners.markminervini.screener import run_market_screening

    print(f"[Task] Mark Minervini technical screening ({normalized_market})")
    result = run_market_screening(
        market=normalized_market,
        **(
            {"runtime_context": runtime_context}
            if runtime_context is not None
            else {}
        ),
    )
    if runtime_context is not None and hasattr(result, "copy"):
        runtime_context.screening_frames["markminervini_with_rs"] = result.copy()
    return result



def run_advanced_financial_screening(
    *,
    market: str,
    skip_data: bool,
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
) -> Any:
    normalized_market = require_market_key(market)
    _ = standalone
    from screeners.markminervini.advanced_financial import run_advanced_financial_screening

    print(f"[Task] Advanced financial screening ({normalized_market})")
    kwargs = {
        "skip_data": skip_data,
        "market": normalized_market,
    }
    if runtime_context is not None:
        kwargs["runtime_context"] = runtime_context
    return run_advanced_financial_screening(**kwargs)



def run_integrated_screening(
    *,
    market: str,
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
) -> Any:
    normalized_market = require_market_key(market)
    _ = standalone
    from screeners.markminervini.integrated_screener import IntegratedScreener

    print(f"[Task] Integrated screening ({normalized_market})")
    kwargs: dict[str, Any] = {}
    if runtime_context is not None:
        kwargs["runtime_context"] = runtime_context
    return IntegratedScreener(market=normalized_market).run_integrated_screening(
        **kwargs
    )



def run_new_ticker_tracking(*, market: str, standalone: bool = False) -> Any:
    normalized_market = require_market_key(market)
    _ = standalone
    from screeners.markminervini.ticker_tracker import track_new_tickers
    from utils.market_runtime import get_markminervini_advanced_financial_results_path

    print(f"[Task] New ticker tracking ({normalized_market})")
    return track_new_tickers(get_markminervini_advanced_financial_results_path(normalized_market), market=normalized_market)



def run_qullamaggie_strategy_task(
    setups: Optional[list[str]] | None = None,
    skip_data: bool = False,
    *,
    market: str = "us",
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
    as_of_date: str | None = None,
) -> Any:
    normalized_market = require_market_key(market)
    if runtime_context is None and as_of_date:
        runtime_context = RuntimeContext(market=normalized_market, as_of_date=as_of_date)
    elif runtime_context is not None and as_of_date:
        runtime_context.set_as_of_date(as_of_date)
    effective_standalone = _resolve_effective_standalone(
        normalized_market,
        standalone=standalone,
        runtime_context=runtime_context,
        explicit_as_of=as_of_date,
    )
    try:
        from screeners.qullamaggie.screener import run_qullamaggie_screening
    except Exception as exc:
        print(f"[Task] Qullamaggie import failed ({normalized_market}): {exc}")
        return {"error": str(exc), "market": normalized_market}

    setup_type = None
    if setups:
        setup_type = setups[0] if len(setups) == 1 else None

    try:
        print(f"\n[Task] Qullamaggie screening started ({normalized_market})")
        result = run_qullamaggie_screening(
            setup_type=setup_type,
            market=normalized_market,
            standalone=effective_standalone,
            runtime_context=runtime_context,
            enable_earnings_filter=not skip_data,
        )
        print(f"[Task] Qullamaggie screening completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Qullamaggie screening failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}


def _empty_upcoming_earnings_fetcher(
    market: str,
    as_of_date: str,
    days: int,
) -> pd.DataFrame:
    _ = (market, as_of_date, days)
    return pd.DataFrame(columns=["symbol", "earnings_date"])


class _LocalOnlyEarningsCollector:
    def get_earnings_surprise(self, symbol: str) -> None:
        _ = symbol
        return None

    def provider_diagnostics_rows(self) -> list[dict[str, Any]]:
        return []

    def log_provider_summary(self) -> None:
        return None



def run_tradingview_preset_screeners(
    *,
    market: str,
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
    as_of_date: str | None = None,
) -> Any:
    normalized_market = require_market_key(market)
    _ = standalone
    if runtime_context is None and as_of_date:
        runtime_context = RuntimeContext(market=normalized_market, as_of_date=as_of_date)
    elif runtime_context is not None and as_of_date:
        runtime_context.set_as_of_date(as_of_date)
    try:
        from screeners.tradingview.screener import run_tradingview_preset_screeners

        print(f"\n[Task] TradingView preset screeners started ({normalized_market})")
        results = run_tradingview_preset_screeners(
            market=normalized_market,
            runtime_context=runtime_context,
        )
        preset_count = len(results)
        candidate_count = sum(len(frame) for frame in results.values())
        print(
            f"[Task] TradingView preset screeners completed ({normalized_market}) - "
            f"presets={preset_count}, candidates={candidate_count}"
        )
        return results
    except Exception as exc:
        print(f"[Task] TradingView preset screeners failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}


def _signal_row_brief(row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or row.get("code") or "").strip()
    action = str(row.get("action_type") or row.get("action") or "").strip()
    signal_code = str(row.get("signal_code") or row.get("signal") or "").strip()
    score = row.get("signal_score")
    if score is None:
        score = row.get("conviction_score")
    if score is None:
        score = row.get("entry_score")
    stop = row.get("stop_level")
    if stop is None:
        stop = row.get("risk_stop")
    sizing = row.get("position_size_pct")
    if sizing is None:
        sizing = row.get("sizing")
    parts = [part for part in (symbol, action, signal_code) if part]
    if score is not None:
        parts.append(f"score={score}")
    if stop is not None:
        parts.append(f"stop={stop}")
    if sizing is not None:
        parts.append(f"sizing={sizing}")
    return " | ".join(str(part) for part in parts)


def _print_signal_public_output_summary(
    market: str,
    result: Any,
    *,
    runtime_context: RuntimeContext | None = None,
) -> None:
    if not isinstance(result, dict):
        return
    signal_summary = result.get("signal_summary")
    if not isinstance(signal_summary, dict):
        return
    counts = signal_summary.get("counts") if isinstance(signal_summary.get("counts"), dict) else {}
    as_of_date = str(signal_summary.get("as_of_date") or "").strip()
    freshness_status = ""
    if runtime_context is not None:
        freshness_status = str(runtime_context.runtime_state.get("freshness_status") or "").strip()
    results_dir = get_signal_engine_results_dir(market)
    print(
        "[Task] Public signal outputs "
        f"({market}) - as_of={as_of_date}, freshness={freshness_status or 'unknown'}, "
        f"buy_all={counts.get('buy_signals_all_symbols_v1', 0)}, "
        f"sell_all={counts.get('sell_signals_all_symbols_v1', 0)}, "
        f"buy_screened={counts.get('buy_signals_screened_symbols_v1', 0)}, "
        f"sell_screened={counts.get('sell_signals_screened_symbols_v1', 0)}"
    )
    for name in (
        "buy_signals_all_symbols_v1",
        "sell_signals_all_symbols_v1",
        "buy_signals_screened_symbols_v1",
        "sell_signals_screened_symbols_v1",
    ):
        print(f"[Task] Public signal file ({market}) - {os.path.join(results_dir, name + '.csv')}")
    highlighted_rows: list[dict[str, Any]] = []
    for name in (
        "buy_signals_screened_symbols_v1",
        "sell_signals_screened_symbols_v1",
        "buy_signals_all_symbols_v1",
        "sell_signals_all_symbols_v1",
    ):
        rows = result.get(name)
        if isinstance(rows, list):
            highlighted_rows.extend(row for row in rows if isinstance(row, dict))
        if len(highlighted_rows) >= 10:
            break
    for row in highlighted_rows[:10]:
        brief = _signal_row_brief(row)
        if brief:
            print(f"[Task] Public signal ({market}) - {brief}")



def run_signal_engine_task(
    *,
    market: str,
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
    as_of_date: str | None = None,
    local_only: bool = False,
) -> Any:
    normalized_market = require_market_key(market)
    effective_standalone = _resolve_effective_standalone(
        normalized_market,
        standalone=standalone,
        runtime_context=runtime_context,
        explicit_as_of=as_of_date,
    )
    try:
        from screeners.signals import run_multi_screener_signal_scan

        print(f"\n[Task] Multi-screener signal engine started ({normalized_market})")
        kwargs = {
            "market": normalized_market,
            "standalone": effective_standalone,
        }
        if as_of_date:
            kwargs["as_of_date"] = as_of_date
        if runtime_context is not None:
            kwargs["runtime_context"] = runtime_context
        if local_only:
            kwargs["upcoming_earnings_fetcher"] = _empty_upcoming_earnings_fetcher
            kwargs["earnings_collector"] = _LocalOnlyEarningsCollector()
        result = run_multi_screener_signal_scan(**kwargs)
        _print_signal_public_output_summary(
            normalized_market,
            result,
            runtime_context=runtime_context,
        )
        print(f"[Task] Multi-screener signal engine completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Multi-screener signal engine failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def run_signal_engine_processes(
    markets: Optional[list[str]] = None,
    *,
    standalone: bool = False,
    runtime_contexts: dict[str, RuntimeContext] | None = None,
    as_of_date: str | None = None,
    local_only: bool = False,
) -> dict[str, Any]:
    target_markets = _normalize_markets(markets)
    if runtime_contexts is not None:
        missing_as_of_markets: list[str] = []
        for market in target_markets:
            runtime_context = runtime_contexts.get(market)
            if runtime_context is None:
                runtime_contexts[market] = RuntimeContext(market=market)
                missing_as_of_markets.append(market)
                continue
            if not str(runtime_context.as_of_date or "").strip():
                missing_as_of_markets.append(market)
        if as_of_date or missing_as_of_markets:
            _initialize_runtime_context_as_of(
                runtime_contexts,
                target_markets if as_of_date else missing_as_of_markets,
                explicit_as_of=as_of_date,
            )
    elif as_of_date:
        runtime_contexts = {
            market: RuntimeContext(market=market) for market in target_markets
        }
        _initialize_runtime_context_as_of(
            runtime_contexts,
            target_markets,
            explicit_as_of=as_of_date,
        )
    print("\n[Task] Signal engine process started")
    started_perf = time.perf_counter()
    try:
        outcomes: list[TaskStepOutcome] = []
        total_steps = len(target_markets)
        for index, market in enumerate(target_markets, start=1):
            runtime_context = _get_runtime_context(runtime_contexts, market)
            effective_standalone = _resolve_effective_standalone(
                market,
                standalone=standalone,
                runtime_context=runtime_context,
                explicit_as_of=(
                    runtime_context.as_of_date
                    if runtime_context is not None and runtime_context.as_of_date
                    else as_of_date
                ),
            )
            _record_outcome(
                outcomes,
                label="Signal engine process",
                outcome=_run_timed_step(
                    index,
                    total_steps,
                    "Multi-screener signal engine",
                    market,
                    lambda market=market, runtime_context=runtime_context: run_signal_engine_task(
                        market=market,
                        standalone=effective_standalone,
                        **({"local_only": True} if local_only else {}),
                        **(
                            {
                                "as_of_date": (
                                    runtime_context.as_of_date
                                    if runtime_context is not None
                                    else as_of_date
                                )
                            }
                            if (
                                runtime_context is not None
                                and runtime_context.as_of_date
                            )
                            or as_of_date
                            else {}
                        ),
                        **(
                            {"runtime_context": runtime_context}
                            if runtime_context is not None
                            else {}
                        ),
                    ),
                    runtime_context=runtime_context,
                ),
            )

        summary = _build_process_summary("Signal engine process", outcomes)
        _flush_runtime_state_writes(_runtime_state_paths_for_markets(target_markets))
        return _set_process_wall_elapsed(
            summary,
            wall_elapsed_seconds=time.perf_counter() - started_perf,
        )
    except Exception as exc:
        return _unexpected_process_summary("Signal engine process", exc)



def run_screening_augment_task(
    *,
    market: str,
    runtime_context: RuntimeContext | None = None,
) -> Any:
    normalized_market = require_market_key(market)
    try:
        from screeners.augment import run_screening_augment

        print(f"\n[Task] Screening augment started ({normalized_market})")
        kwargs = {"market": normalized_market}
        if runtime_context is not None:
            kwargs["runtime_context"] = runtime_context
        result = run_screening_augment(**kwargs)
        print(f"[Task] Screening augment completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Screening augment failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}


def run_screening_augment_processes(
    markets: Optional[list[str]] = None,
    *,
    runtime_contexts: dict[str, RuntimeContext] | None = None,
) -> dict[str, Any]:
    target_markets = _normalize_markets(markets)
    print("\n[Task] Screening augment process started")
    started_perf = time.perf_counter()
    try:
        outcomes: list[TaskStepOutcome] = []
        total_steps = len(target_markets)
        for index, market in enumerate(target_markets, start=1):
            runtime_context = _get_runtime_context(runtime_contexts, market)
            _record_outcome(
                outcomes,
                label="Screening augment process",
                outcome=_run_timed_step(
                    index,
                    total_steps,
                    "Screening augment",
                    market,
                    lambda market=market, runtime_context=runtime_context: run_screening_augment_task(
                        market=market,
                        **(
                            {"runtime_context": runtime_context}
                            if runtime_context is not None
                            else {}
                        ),
                    ),
                    runtime_context=runtime_context,
                ),
            )

        summary = _build_process_summary("Screening augment process", outcomes)
        _flush_runtime_state_writes(_runtime_state_paths_for_markets(target_markets))
        return _set_process_wall_elapsed(
            summary,
            wall_elapsed_seconds=time.perf_counter() - started_perf,
        )
    except Exception as exc:
        return _unexpected_process_summary("Screening augment process", exc)


def run_weinstein_stage2_screening(
    *,
    market: str,
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
    as_of_date: str | None = None,
) -> Any:
    normalized_market = require_market_key(market)
    if runtime_context is None and as_of_date:
        runtime_context = RuntimeContext(market=normalized_market, as_of_date=as_of_date)
    elif runtime_context is not None and as_of_date:
        runtime_context.set_as_of_date(as_of_date)
    effective_standalone = _resolve_effective_standalone(
        normalized_market,
        standalone=standalone,
        runtime_context=runtime_context,
        explicit_as_of=as_of_date,
    )
    try:
        from screeners.weinstein_stage2.screener import run_weinstein_stage2_screening

        print(f"\n[Task] Weinstein Stage 2 screening started ({normalized_market})")
        result = run_weinstein_stage2_screening(
            market=normalized_market,
            standalone=effective_standalone,
            runtime_context=runtime_context,
        )
        print(f"[Task] Weinstein Stage 2 screening completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Weinstein Stage 2 screening failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def run_leader_lagging_screening(
    *,
    market: str,
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
    as_of_date: str | None = None,
) -> Any:
    normalized_market = require_market_key(market)
    if runtime_context is None and as_of_date:
        runtime_context = RuntimeContext(market=normalized_market, as_of_date=as_of_date)
    elif runtime_context is not None and as_of_date:
        runtime_context.set_as_of_date(as_of_date)
    effective_standalone = _resolve_effective_standalone(
        normalized_market,
        standalone=standalone,
        runtime_context=runtime_context,
        explicit_as_of=as_of_date,
    )
    try:
        from screeners.leader_lagging.screener import run_leader_lagging_screening

        print(f"\n[Task] Leader / lagging screening started ({normalized_market})")
        result = run_leader_lagging_screening(
            market=normalized_market,
            standalone=effective_standalone,
            runtime_context=runtime_context,
        )
        print(f"[Task] Leader / lagging screening completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Leader / lagging screening failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def run_all_screening_processes(
    skip_data: bool = False,
    markets: Optional[list[str]] = None,
    *,
    standalone: bool = False,
    runtime_contexts: dict[str, RuntimeContext] | None = None,
) -> dict[str, Any]:
    target_markets = _normalize_markets(markets)
    print("\n[Task] Full screening process started")
    started_perf = time.perf_counter()
    try:
        outcomes: list[TaskStepOutcome] = []
        for market in target_markets:
            runtime_context = _get_runtime_context(runtime_contexts, market)
            market_standalone = _resolve_effective_standalone(
                market,
                standalone=standalone,
                runtime_context=runtime_context,
            )
            print(f"\n[Task] Market pipeline started ({market})")

            def _record_step(outcome: TaskStepOutcome) -> None:
                _record_outcome(
                    outcomes,
                    label="Full screening process",
                    outcome=outcome,
                )

            _record_step(
                _run_timed_step(
                    1,
                    8,
                    "Mark Minervini technical",
                    market,
                    lambda market=market, runtime_context=runtime_context: run_markminervini_screening(
                        market=market,
                        standalone=market_standalone,
                        **(
                            {"runtime_context": runtime_context}
                            if runtime_context is not None
                            else {}
                        ),
                    ),
                    runtime_context=runtime_context,
                )
            )
            wait_for_yahoo_phase_handoff("Advanced financial")
            _record_step(
                _run_timed_step(
                    2,
                    8,
                    "Advanced financial",
                    market,
                    lambda market=market, runtime_context=runtime_context: run_advanced_financial_screening(
                        market=market,
                        skip_data=skip_data,
                        standalone=market_standalone,
                        **(
                            {"runtime_context": runtime_context}
                            if runtime_context is not None
                            else {}
                        ),
                    ),
                    runtime_context=runtime_context,
                )
            )
            _record_step(
                _run_timed_step(
                    3,
                    8,
                    "Integrated screening",
                    market,
                    lambda market=market, runtime_context=runtime_context: run_integrated_screening(
                        market=market,
                        standalone=market_standalone,
                        **(
                            {"runtime_context": runtime_context}
                            if runtime_context is not None
                            else {}
                        ),
                    ),
                    runtime_context=runtime_context,
                )
            )
            _record_step(
                _run_timed_step(
                    4,
                    8,
                    "New ticker tracking",
                    market,
                    lambda market=market: run_new_ticker_tracking(
                        market=market,
                        standalone=market_standalone,
                    ),
                    runtime_context=runtime_context,
                )
            )

            def _weinstein_action(child_context: RuntimeContext | None) -> Any:
                return run_weinstein_stage2_screening(
                    market=market,
                    standalone=market_standalone,
                    runtime_context=child_context,
                )

            def _leader_action(child_context: RuntimeContext | None) -> Any:
                return run_leader_lagging_screening(
                    market=market,
                    standalone=market_standalone,
                    runtime_context=child_context,
                )

            def _qullamaggie_action(child_context: RuntimeContext | None) -> Any:
                return run_qullamaggie_strategy_task(
                    skip_data=skip_data,
                    market=market,
                    standalone=market_standalone,
                    runtime_context=child_context,
                )

            def _tradingview_action(child_context: RuntimeContext | None) -> Any:
                return run_tradingview_preset_screeners(
                    market=market,
                    standalone=market_standalone,
                    runtime_context=child_context,
                )

            stage_specs = [
                _ScreeningStageSpec(
                    5,
                    8,
                    "Weinstein Stage 2",
                    market,
                    _weinstein_action,
                    runtime_context,
                ),
                _ScreeningStageSpec(
                    6,
                    8,
                    "Leader / lagging",
                    market,
                    _leader_action,
                    runtime_context,
                ),
            ]
            if skip_data:
                stage_specs.append(
                    _ScreeningStageSpec(
                        7,
                        8,
                        "Qullamaggie",
                        market,
                        _qullamaggie_action,
                        runtime_context,
                    )
                )
            stage_specs.append(
                _ScreeningStageSpec(
                    8,
                    8,
                    "TradingView presets",
                    market,
                    _tradingview_action,
                    runtime_context,
                )
            )

            if _screening_stage_parallel_enabled():
                _preload_shared_screening_ohlcv_cache(market, runtime_context)
                local_outcomes = _run_screening_stage_specs(
                    stage_specs,
                    parallel=True,
                )
                if not skip_data:
                    wait_for_yahoo_phase_handoff("Qullamaggie")
                    qullamaggie_outcome = _run_timed_step(
                        7,
                        8,
                        "Qullamaggie",
                        market,
                        lambda: _qullamaggie_action(runtime_context),
                        runtime_context=runtime_context,
                    )
                    local_outcomes.append(qullamaggie_outcome)
                stage_order = {
                    "Weinstein Stage 2": 5,
                    "Leader / lagging": 6,
                    "Qullamaggie": 7,
                    "TradingView presets": 8,
                }
                for outcome in sorted(
                    local_outcomes,
                    key=lambda item: stage_order.get(item.label, 99),
                ):
                    _record_step(outcome)
            else:
                for spec in stage_specs[:2]:
                    outcome = _run_timed_step(
                        spec.step_number,
                        spec.total_steps,
                        spec.label,
                        spec.market,
                        lambda spec=spec: spec.action(runtime_context),
                        runtime_context=runtime_context,
                    )
                    _record_step(outcome)
                wait_for_yahoo_phase_handoff("Qullamaggie")
                _record_step(
                    _run_timed_step(
                        7,
                        8,
                        "Qullamaggie",
                        market,
                        lambda: _qullamaggie_action(runtime_context),
                        runtime_context=runtime_context,
                    )
                )
                _record_step(
                    _run_timed_step(
                        8,
                        8,
                        "TradingView presets",
                        market,
                        lambda: _tradingview_action(runtime_context),
                        runtime_context=runtime_context,
                    )
                )

            try:
                _build_and_store_source_registry_snapshot(market, runtime_context)
            except Exception as exc:
                print(
                    f"[Task] Source registry snapshot build skipped ({market}) - {exc}"
                )
            print(f"[Task] Market pipeline completed ({market})")

        summary = _build_process_summary("Full screening process", outcomes)
        _flush_runtime_state_writes(_runtime_state_paths_for_markets(target_markets))
        return _set_process_wall_elapsed(
            summary,
            wall_elapsed_seconds=time.perf_counter() - started_perf,
        )
    except Exception as exc:
        return _unexpected_process_summary("Full screening process", exc)



def run_market_analysis_pipeline(
    *,
    skip_data: bool = False,
    markets: Optional[list[str]] = None,
    include_signals: bool = False,
    enable_augment: bool = False,
    standalone: bool = False,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    target_markets = _normalize_markets(markets)
    started_perf = time.perf_counter()
    runtime_contexts = {
        market: RuntimeContext(market=market) for market in target_markets
    }
    _initialize_runtime_context_as_of(
        runtime_contexts,
        target_markets,
        explicit_as_of=as_of_date,
    )
    print(
        "\n[Task] Market analysis pipeline started - "
        f"markets={target_markets}, skip_data={skip_data}, include_signals={include_signals}, "
        f"enable_augment={enable_augment}, as_of_date={as_of_date or 'auto'}"
    )
    screening_summary = run_all_screening_processes(
        skip_data=skip_data,
        markets=target_markets,
        standalone=standalone,
        runtime_contexts=runtime_contexts,
    )
    summaries = [screening_summary]
    if enable_augment:
        summaries.append(
            run_screening_augment_processes(
                markets=target_markets,
                runtime_contexts=runtime_contexts,
            )
        )
    if include_signals:
        summaries.append(
            run_signal_engine_processes(
                markets=target_markets,
                standalone=standalone,
                runtime_contexts=runtime_contexts,
                as_of_date=as_of_date,
                local_only=skip_data,
            )
        )
    combined = _combine_process_summaries("Market analysis pipeline", summaries)
    _flush_runtime_state_writes(_runtime_state_paths_for_markets(target_markets))
    write_full_run_summaries(
        "Market analysis pipeline",
        combined,
        markets=target_markets,
    )
    return _set_process_wall_elapsed(
        combined,
        wall_elapsed_seconds=time.perf_counter() - started_perf,
    )



_SCHED_CONF = {"full_time": "14:30", "interval": 1}



def setup_scheduler(full_run_time: str = "14:30", keep_alive_interval: int = 1) -> None:
    _SCHED_CONF["full_time"] = full_run_time
    _SCHED_CONF["interval"] = keep_alive_interval
    print(
        "[Task] Scheduler configured - "
        f"daily full run after {full_run_time} KST, keep-alive interval {keep_alive_interval} min"
    )



def run_scheduler() -> None:
    try:
        import pytz
    except Exception:
        pytz = None

    full_time = datetime.strptime(_SCHED_CONF["full_time"], "%H:%M").time()
    interval = _SCHED_CONF["interval"]
    kst_tz = pytz.timezone("Asia/Seoul") if pytz else None
    last_full_date = None

    print("[Task] Scheduler started (Ctrl+C to stop)")
    try:
        while True:
            run_market_analysis_pipeline(skip_data=True, markets=["us"], include_signals=True)
            now = datetime.now(kst_tz)
            if now.time() >= full_time and (last_full_date != now.date()):
                time.sleep(interval * 60)
                subprocess.run([sys.executable, "main.py"], check=False)
                last_full_date = datetime.now(kst_tz).date() if kst_tz else datetime.now().date()
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        print("\n[Task] Scheduler stopped")
