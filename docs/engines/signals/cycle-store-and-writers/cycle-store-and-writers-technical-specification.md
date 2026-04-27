# Cycle Store And Writers Technical Specification

## Canonical Code Surfaces

- `screeners/signals/cycle_store.py`
- `screeners/signals/writers.py`
## Output Contract

- `signal_history.csv`, `signal_state_history.csv`, `peg_event_history.csv`, `open_family_cycles.csv`, plus writer-emitted v2 artifact files.

## Fidelity Status

- High fidelity; regression tests cover separation and round-trip persistence.
- Richer lifecycle provenance rather than simple CSV merge history.

## Closest Tests

- `tests/test_signal_engine_restoration.py`
