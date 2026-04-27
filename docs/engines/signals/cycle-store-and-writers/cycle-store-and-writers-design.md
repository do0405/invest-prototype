# Cycle Store And Writers Design

## Source Of Truth

This design doc records the stable boundary for `cycle-store-and-writers` under `signals`.

## Runtime Surface

- `screeners/signals/cycle_store.py`
- `screeners/signals/writers.py`
## Output Contract

- `signal_history.csv`, `signal_state_history.csv`, `peg_event_history.csv`, `open_family_cycles.csv`, plus writer-emitted v2 artifact files.

## Current Fidelity And Higher-End Target

- High fidelity; regression tests cover separation and round-trip persistence.
- Richer lifecycle provenance rather than simple CSV merge history.

## Closest Tests

- `tests/test_signal_engine_restoration.py`
