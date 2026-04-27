from __future__ import annotations

import os
from typing import Any, Callable, Iterable, Mapping


DEFAULT_SCOPE = "screened"

_HISTORY_KEY_FIELDS = (
    "scope",
    "signal_date",
    "symbol",
    "engine",
    "family",
    "signal_code",
    "action_type",
)


def _normalize_scope(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text or DEFAULT_SCOPE


def _load_filtered_signal_history(
    results_dir: str,
    *,
    safe_csv_rows: Callable[[str], list[dict[str, Any]]],
    safe_text: Callable[[Any], str],
    history_prefix: str,
    signal_kinds: Iterable[str],
) -> list[dict[str, Any]]:
    allowed_signal_kinds = {
        safe_text(kind).upper() for kind in signal_kinds if safe_text(kind)
    }
    path = os.path.join(results_dir, f"{history_prefix}.csv")
    rows: list[dict[str, Any]] = []
    for row in safe_csv_rows(path):
        if safe_text(row.get("signal_kind")).upper() not in allowed_signal_kinds:
            continue
        normalized = dict(row)
        normalized["scope"] = _normalize_scope(normalized.get("scope"))
        rows.append(normalized)
    return rows


def _persist_filtered_signal_history(
    results_dir: str,
    *,
    existing_rows: Iterable[Mapping[str, Any]],
    new_rows: Iterable[Mapping[str, Any]],
    history_merge_rows: Callable[..., list[dict[str, Any]]],
    safe_text: Callable[[Any], str],
    write_records: Callable[[str, str, list[dict[str, Any]]], None],
    history_prefix: str,
    signal_kinds: Iterable[str],
    action_types: Iterable[str],
) -> list[dict[str, Any]]:
    allowed_signal_kinds = {
        safe_text(kind).upper() for kind in signal_kinds if safe_text(kind)
    }
    allowed_action_types = {
        safe_text(action).upper() for action in action_types if safe_text(action)
    }
    merged = history_merge_rows(
        existing_rows,
        [
            row
            for row in new_rows
            if safe_text(row.get("signal_kind")).upper() in allowed_signal_kinds
            and safe_text(row.get("action_type")).upper() in allowed_action_types
        ],
        key_fields=_HISTORY_KEY_FIELDS,
    )
    write_records(results_dir, history_prefix, merged)
    return merged


def load_active_cycles(
    results_dir: str,
    *,
    safe_csv_rows: Callable[[str], list[dict[str, Any]]],
    safe_text: Callable[[Any], str],
    hydrate_loaded_cycle: Callable[[Mapping[str, Any]], dict[str, Any]],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    path = os.path.join(results_dir, "open_family_cycles.csv")
    cycles: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in safe_csv_rows(path):
        symbol = safe_text(row.get("symbol")).upper()
        engine = safe_text(row.get("engine"))
        family = safe_text(row.get("family"))
        scope = _normalize_scope(row.get("scope"))
        if not symbol or not engine or not family:
            continue
        hydrated = hydrate_loaded_cycle(row)
        hydrated["scope"] = scope
        cycles[(scope, engine, family, symbol)] = hydrated
    return cycles


def load_signal_history(
    results_dir: str,
    *,
    safe_csv_rows: Callable[[str], list[dict[str, Any]]],
    safe_text: Callable[[Any], str],
    signal_history_prefix: str,
) -> list[dict[str, Any]]:
    return _load_filtered_signal_history(
        results_dir,
        safe_csv_rows=safe_csv_rows,
        safe_text=safe_text,
        history_prefix=signal_history_prefix,
        signal_kinds=("EVENT",),
    )


def persist_signal_history(
    results_dir: str,
    *,
    existing_rows: Iterable[Mapping[str, Any]],
    new_event_rows: Iterable[Mapping[str, Any]],
    history_merge_rows: Callable[..., list[dict[str, Any]]],
    safe_text: Callable[[Any], str],
    write_records: Callable[[str, str, list[dict[str, Any]]], None],
    signal_history_prefix: str,
) -> list[dict[str, Any]]:
    return _persist_filtered_signal_history(
        results_dir,
        existing_rows=existing_rows,
        new_rows=new_event_rows,
        history_merge_rows=history_merge_rows,
        safe_text=safe_text,
        write_records=write_records,
        history_prefix=signal_history_prefix,
        signal_kinds=("EVENT",),
        action_types=("BUY", "SELL", "WATCH", "ALERT", "TRIM", "EXIT"),
    )


def load_state_history(
    results_dir: str,
    *,
    safe_csv_rows: Callable[[str], list[dict[str, Any]]],
    safe_text: Callable[[Any], str],
    state_history_prefix: str,
) -> list[dict[str, Any]]:
    return _load_filtered_signal_history(
        results_dir,
        safe_csv_rows=safe_csv_rows,
        safe_text=safe_text,
        history_prefix=state_history_prefix,
        signal_kinds=("STATE", "AUX"),
    )


def persist_state_history(
    results_dir: str,
    *,
    existing_rows: Iterable[Mapping[str, Any]],
    new_state_rows: Iterable[Mapping[str, Any]],
    history_merge_rows: Callable[..., list[dict[str, Any]]],
    safe_text: Callable[[Any], str],
    write_records: Callable[[str, str, list[dict[str, Any]]], None],
    state_history_prefix: str,
) -> list[dict[str, Any]]:
    return _persist_filtered_signal_history(
        results_dir,
        existing_rows=existing_rows,
        new_rows=new_state_rows,
        history_merge_rows=history_merge_rows,
        safe_text=safe_text,
        write_records=write_records,
        history_prefix=state_history_prefix,
        signal_kinds=("STATE", "AUX"),
        action_types=("STATE", "ALERT"),
    )


def load_peg_event_history(
    results_dir: str,
    *,
    safe_csv_rows: Callable[[str], list[dict[str, Any]]],
    peg_event_history_prefix: str,
) -> list[dict[str, Any]]:
    path = os.path.join(results_dir, f"{peg_event_history_prefix}.csv")
    return safe_csv_rows(path)


def latest_peg_event_map(
    rows: Iterable[Mapping[str, Any]],
    *,
    safe_text: Callable[[Any], str],
    date_to_str: Callable[[Any], str | None],
) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = safe_text(row.get("symbol")).upper()
        event_date = date_to_str(row.get("event_date"))
        if not symbol or event_date is None:
            continue
        current = latest.get(symbol)
        if current is None or event_date > safe_text(current.get("event_date")):
            latest[symbol] = dict(row)
    return latest


def persist_peg_event_history(
    results_dir: str,
    *,
    market: str,
    existing_rows: Iterable[Mapping[str, Any]],
    peg_context_map: Mapping[str, Mapping[str, Any]],
    history_merge_rows: Callable[..., list[dict[str, Any]]],
    write_records: Callable[[str, str, list[dict[str, Any]]], None],
    peg_event_history_prefix: str,
) -> list[dict[str, Any]]:
    confirmed_rows: list[dict[str, Any]] = []
    for symbol, context in peg_context_map.items():
        if not (context.get("event_day") and context.get("event_confirmed")):
            continue
        confirmed_rows.append(
            {
                "symbol": symbol,
                "market": market.upper(),
                "earnings_date": context.get("earnings_date"),
                "event_date": context.get("event_date"),
                "gap_pct": context.get("gap_pct"),
                "gap_low": context.get("gap_low"),
                "half_gap": context.get("half_gap"),
                "event_high": context.get("event_high"),
            }
        )

    merged = history_merge_rows(
        existing_rows,
        confirmed_rows,
        key_fields=("symbol", "event_date"),
    )
    write_records(results_dir, peg_event_history_prefix, merged)
    return merged
