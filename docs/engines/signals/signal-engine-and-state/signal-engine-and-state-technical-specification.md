# Signal Engine And State Technical Specification

## Canonical Code Surfaces

- `screeners/signals/engine.py`
- `screeners/signals/metrics.py`
## Output Contract

- `trend_following_events_v2.csv`, `trend_following_states_v2.csv`, `ultimate_growth_events_v2.csv`, `ultimate_growth_states_v2.csv`, `ug_strategy_combos_v2.csv`, `all_signals_v2.csv`, `open_family_cycles.csv`, `screen_signal_diagnostics.csv`, `signal_universe_snapshot.csv`, `signal_summary.json`, `source_registry_summary.json`.

## Fidelity Status

- Medium-high fidelity because regression coverage is deep, but semantics remain partly heuristic and proxy-backed.
- Explicit semantic status, better family arbitration, clearer calibration lineage.

## Closest Tests

- `tests/test_signal_engine_restoration.py`
- `tests/test_signals_package.py`
- `tests/test_market_intel_bridge.py`
