from __future__ import annotations

import os
from typing import Any, Callable

from utils.io_utils import write_json_with_fallback


PUBLIC_TODAY_ONLY_SIGNAL_OUTPUTS: tuple[str, ...] = (
    "buy_signals_all_symbols_v1",
    "sell_signals_all_symbols_v1",
    "buy_signals_screened_symbols_v1",
    "sell_signals_screened_symbols_v1",
)

INTERNAL_SIGNAL_DIAGNOSTIC_OUTPUTS: tuple[str, ...] = (
    "trend_following_events_v2",
    "trend_following_states_v2",
    "ultimate_growth_events_v2",
    "ultimate_growth_states_v2",
    "ug_strategy_combos_v2",
    "all_signals_v2",
    "screen_signal_diagnostics",
    "earnings_provider_diagnostics",
    "signal_universe_snapshot",
)

SIGNAL_RECORD_OUTPUT_PREFIXES: tuple[str, ...] = (
    "trend_following_events_v2",
    "trend_following_states_v2",
    "ultimate_growth_events_v2",
    "ultimate_growth_states_v2",
    "ug_strategy_combos_v2",
    "all_signals_v2",
    *PUBLIC_TODAY_ONLY_SIGNAL_OUTPUTS,
    "open_family_cycles",
    "screen_signal_diagnostics",
    "earnings_provider_diagnostics",
    "signal_universe_snapshot",
)


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
    buy_signals_all_rows: list[dict[str, Any]],
    sell_signals_all_rows: list[dict[str, Any]],
    buy_signals_screened_rows: list[dict[str, Any]],
    sell_signals_screened_rows: list[dict[str, Any]],
    open_cycle_rows: list[dict[str, Any]],
    diagnostics: list[dict[str, Any]],
    earnings_provider_diagnostics: list[dict[str, Any]],
    signal_universe_rows: list[dict[str, Any]],
    source_registry_summary: dict[str, Any],
    signal_summary: dict[str, Any],
    write_records_fn: Callable[[str, str, list[dict[str, Any]]], None],
) -> None:
    rows_by_prefix: dict[str, list[dict[str, Any]]] = {
        "trend_following_events_v2": trend_event_rows,
        "trend_following_states_v2": trend_state_rows,
        "ultimate_growth_events_v2": ug_event_rows,
        "ultimate_growth_states_v2": ug_state_rows,
        "ug_strategy_combos_v2": ug_combo_rows,
        "all_signals_v2": all_signal_rows,
        "buy_signals_all_symbols_v1": buy_signals_all_rows,
        "sell_signals_all_symbols_v1": sell_signals_all_rows,
        "buy_signals_screened_symbols_v1": buy_signals_screened_rows,
        "sell_signals_screened_symbols_v1": sell_signals_screened_rows,
        "open_family_cycles": open_cycle_rows,
        "screen_signal_diagnostics": diagnostics,
        "earnings_provider_diagnostics": earnings_provider_diagnostics,
        "signal_universe_snapshot": signal_universe_rows,
    }
    for prefix in SIGNAL_RECORD_OUTPUT_PREFIXES:
        write_records_fn(results_dir, prefix, rows_by_prefix[prefix])

    write_json_with_fallback(
        source_registry_summary,
        os.path.join(results_dir, "source_registry_summary.json"),
        ensure_ascii=False,
        indent=2,
    )
    write_json_with_fallback(
        signal_summary,
        os.path.join(results_dir, "signal_summary.json"),
        ensure_ascii=False,
        indent=2,
    )
