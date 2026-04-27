# Signal Engine And State ERD

## Entities

- signal-engine-and-state
- output artifacts described below

## Producers

- `screeners/signals/engine.py`
- `screeners/signals/metrics.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| signal-engine-and-state outputs | `screeners/signals/engine.py, screeners/signals/metrics.py` | `trend_following_events_v2.csv`, `trend_following_states_v2.csv`, `ultimate_growth_events_v2.csv`, `ultimate_growth_states_v2.csv`, `ug_strategy_combos_v2.csv`, `all_signals_v2.csv`, `open_family_cycles.csv`, `screen_signal_diagnostics.csv`, `signal_universe_snapshot.csv`, `signal_summary.json`, `source_registry_summary.json`. | operators/tests |

## Closest Tests

- `tests/test_signal_engine_restoration.py`
- `tests/test_signals_package.py`
- `tests/test_market_intel_bridge.py`
