from __future__ import annotations

import json
import os
from typing import Any, Callable



def write_records(
    output_dir: str,
    filename_prefix: str,
    rows: list[dict[str, Any]],
    *,
    save_screening_results_fn: Callable[..., Any],
) -> None:
    save_screening_results_fn(
        results=rows,
        output_dir=output_dir,
        filename_prefix=filename_prefix,
        include_timestamp=False,
        incremental_update=False,
    )



def write_signal_outputs(
    results_dir: str,
    *,
    trend_event_rows: list[dict[str, Any]],
    trend_state_rows: list[dict[str, Any]],
    ug_event_rows: list[dict[str, Any]],
    ug_state_rows: list[dict[str, Any]],
    ug_combo_rows: list[dict[str, Any]],
    all_signal_rows: list[dict[str, Any]],
    open_cycle_rows: list[dict[str, Any]],
    diagnostics: list[dict[str, Any]],
    signal_universe_rows: list[dict[str, Any]],
    source_registry_summary: dict[str, Any],
    signal_summary: dict[str, Any],
    write_records_fn: Callable[[str, str, list[dict[str, Any]]], None],
) -> None:
    write_records_fn(results_dir, "trend_following_events_v2", trend_event_rows)
    write_records_fn(results_dir, "trend_following_states_v2", trend_state_rows)
    write_records_fn(results_dir, "ultimate_growth_events_v2", ug_event_rows)
    write_records_fn(results_dir, "ultimate_growth_states_v2", ug_state_rows)
    write_records_fn(results_dir, "ug_strategy_combos_v2", ug_combo_rows)
    write_records_fn(results_dir, "all_signals_v2", all_signal_rows)
    write_records_fn(results_dir, "open_family_cycles", open_cycle_rows)
    write_records_fn(results_dir, "screen_signal_diagnostics", diagnostics)
    write_records_fn(results_dir, "signal_universe_snapshot", signal_universe_rows)

    with open(
        os.path.join(results_dir, "source_registry_summary.json"),
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(source_registry_summary, handle, ensure_ascii=False, indent=2)

    with open(
        os.path.join(results_dir, "signal_summary.json"),
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(signal_summary, handle, ensure_ascii=False, indent=2)
