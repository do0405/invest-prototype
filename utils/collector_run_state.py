"""Shared collector run-state helpers for long-running OHLCV refreshes."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Iterable, Mapping


def collector_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def default_collector_run_state(
    *,
    market: str,
    as_of_date: str,
    cooldown_snapshot: Mapping[str, object] | None = None,
) -> dict:
    now = collector_timestamp()
    normalized_market = str(market or "").strip().lower()
    normalized_as_of = str(as_of_date or "").strip()
    return {
        "market": normalized_market,
        "as_of_date": normalized_as_of,
        "run_id": f"{normalized_market}:{normalized_as_of}:{now}",
        "started_at": now,
        "last_progress_at": now,
        "last_symbol": "",
        "completed_symbols": [],
        "retry_queue": [],
        "failed_symbols": [],
        "status_counts": {},
        "cooldown_snapshot": dict(cooldown_snapshot or {}),
        "symbol_statuses": {},
    }


def write_collector_run_state(path: str, state: dict, *, warned_paths: set[str] | None = None) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
    except OSError as exc:
        if warned_paths is None or path not in warned_paths:
            if warned_paths is not None:
                warned_paths.add(path)
            print(f"[Collector] State write skipped - path={path}, error={exc}")


def load_collector_run_state(
    path: str,
    *,
    market: str,
    as_of_date: str,
    cooldown_snapshot: Mapping[str, object] | None = None,
) -> dict:
    default_state = default_collector_run_state(
        market=market,
        as_of_date=as_of_date,
        cooldown_snapshot=cooldown_snapshot,
    )
    if not os.path.exists(path):
        return default_state
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return default_state
    if not isinstance(payload, dict):
        return default_state
    if str(payload.get("market") or "").strip().lower() != str(market or "").strip().lower():
        return default_state
    if str(payload.get("as_of_date") or "").strip() != str(as_of_date or "").strip():
        return default_state

    state = dict(default_state)
    state.update(payload)
    state["market"] = str(market or "").strip().lower()
    state["as_of_date"] = str(as_of_date or "").strip()
    state["run_id"] = str(state.get("run_id") or default_state["run_id"]).strip()
    state["started_at"] = str(state.get("started_at") or default_state["started_at"]).strip()
    state["last_progress_at"] = str(state.get("last_progress_at") or default_state["last_progress_at"]).strip()
    state["completed_symbols"] = _normalize_symbol_list(state.get("completed_symbols") or [])
    state["retry_queue"] = _normalize_symbol_list(state.get("retry_queue") or [])
    state["failed_symbols"] = _normalize_symbol_list(state.get("failed_symbols") or [])
    raw_statuses = state.get("symbol_statuses") or {}
    if not isinstance(raw_statuses, dict):
        raw_statuses = {}
    state["symbol_statuses"] = {
        str(symbol).strip().upper(): str(status).strip()
        for symbol, status in raw_statuses.items()
        if str(symbol).strip() and str(status).strip()
    }
    state["status_counts"] = {
        str(status).strip(): int(count)
        for status, count in dict(state.get("status_counts") or {}).items()
        if str(status).strip()
    }
    state["cooldown_snapshot"] = dict(state.get("cooldown_snapshot") or {})
    return state


def collector_tickers_for_run(
    tickers: Iterable[str],
    state: dict,
    *,
    skip_completed: bool = True,
    terminal_statuses: set[str] | None = None,
) -> list[str]:
    requested = _normalize_symbol_list(tickers)
    completed = set(state.get("completed_symbols") or [])
    failed = set(state.get("failed_symbols") or [])
    terminal_statuses = {
        str(status or "").strip()
        for status in set(terminal_statuses or set())
        if str(status or "").strip()
    }
    symbol_statuses = {
        str(symbol).strip().upper(): str(status).strip()
        for symbol, status in dict(state.get("symbol_statuses") or {}).items()
        if str(symbol).strip() and str(status).strip()
    }
    terminal = {
        symbol
        for symbol, status in symbol_statuses.items()
        if status in terminal_statuses
    }
    retry_queue = [
        symbol
        for symbol in list(state.get("retry_queue") or [])
        if symbol in requested
        and symbol not in terminal
        and (not skip_completed or symbol not in completed)
    ]
    pending = [
        symbol
        for symbol in requested
        if symbol not in terminal
        and symbol not in retry_queue
        and (not skip_completed or (symbol not in completed and symbol not in failed))
    ]
    return retry_queue + pending


def record_collector_symbol_status(
    state: dict,
    *,
    symbol: str,
    status: str,
    completed_statuses: set[str],
    retryable_statuses: set[str],
    cooldown_snapshot: Mapping[str, object] | None = None,
) -> dict:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_status = str(status or "").strip()
    symbol_statuses = {
        str(key).strip().upper(): str(value).strip()
        for key, value in dict(state.get("symbol_statuses") or {}).items()
        if str(key).strip()
    }
    completed = set(state.get("completed_symbols") or [])
    retry_queue = set(state.get("retry_queue") or [])
    failed = set(state.get("failed_symbols") or [])

    if normalized_symbol:
        state["last_symbol"] = normalized_symbol
    if normalized_symbol and normalized_status:
        symbol_statuses[normalized_symbol] = normalized_status
        if normalized_status in completed_statuses:
            completed.add(normalized_symbol)
            retry_queue.discard(normalized_symbol)
            failed.discard(normalized_symbol)
        elif normalized_status in retryable_statuses:
            retry_queue.add(normalized_symbol)
            completed.discard(normalized_symbol)
            failed.discard(normalized_symbol)
        else:
            failed.add(normalized_symbol)
            completed.discard(normalized_symbol)
            retry_queue.discard(normalized_symbol)

    state["symbol_statuses"] = symbol_statuses
    state["completed_symbols"] = sorted(completed)
    state["retry_queue"] = sorted(retry_queue)
    state["failed_symbols"] = sorted(failed)
    state["status_counts"] = collector_status_counts(symbol_statuses)
    state["last_progress_at"] = collector_timestamp()
    if cooldown_snapshot is not None:
        state["cooldown_snapshot"] = dict(cooldown_snapshot)
    return state


def collector_status_counts(symbol_statuses: Mapping[str, str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in symbol_statuses.values():
        normalized = str(status or "").strip()
        if normalized:
            counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def build_collector_summary(
    *,
    market: str,
    as_of: str,
    requested_total: int,
    run_state: dict,
    elapsed_seconds: float,
    failed_samples: list[dict[str, str]] | None = None,
    retryable_statuses: set[str] | None = None,
    hard_failure_statuses: set[str] | None = None,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    status_counts = dict(run_state.get("status_counts") or {})
    retry_queue = list(run_state.get("retry_queue") or [])
    failed_symbols = list(run_state.get("failed_symbols") or [])
    retryable_statuses = set(retryable_statuses or set())
    hard_failure_statuses = set(hard_failure_statuses or {"failed"})
    retryable = bool(retry_queue) or any(int(status_counts.get(status, 0) or 0) > 0 for status in retryable_statuses)
    hard_failed = bool(failed_symbols) or any(
        int(status_counts.get(status, 0) or 0) > 0 for status in hard_failure_statuses
    )
    ok = not retryable and not hard_failed
    summary: dict[str, object] = {
        "schema_version": "1.0",
        "market": str(market or "").strip().lower(),
        "as_of": as_of,
        "total": int(requested_total),
        "processed": len(run_state.get("symbol_statuses") or {}),
        "status_counts": status_counts,
        "saved": int(status_counts.get("saved", 0) or 0),
        "latest": int(status_counts.get("latest", 0) or 0),
        "kept_existing": int(status_counts.get("kept_existing", 0) or 0),
        "soft_unavailable": int(status_counts.get("soft_unavailable", 0) or 0),
        "delisted": int(status_counts.get("delisted", 0) or 0),
        "rate_limited": int(status_counts.get("rate_limited", 0) or 0),
        "failed": int(status_counts.get("failed", 0) or 0),
        "retry_queue_size": len(retry_queue),
        "failed_symbol_count": len(failed_symbols),
        "failed_samples": list(failed_samples or []),
        "elapsed_seconds": round(float(elapsed_seconds), 6),
        "ok": ok,
        "status": "ok" if ok else "degraded",
        "retryable": retryable,
        "run_state": {
            "run_id": run_state.get("run_id", ""),
            "path_status": "written",
            "completed_symbols": len(run_state.get("completed_symbols") or []),
            "retry_queue": len(retry_queue),
            "failed_symbols": len(failed_symbols),
            "last_symbol": run_state.get("last_symbol", ""),
        },
    }
    if extra:
        summary.update(dict(extra))
    return summary


def _normalize_symbol_list(values: Iterable[str]) -> list[str]:
    return [
        str(symbol).strip().upper()
        for symbol in list(values or [])
        if str(symbol).strip()
    ]
