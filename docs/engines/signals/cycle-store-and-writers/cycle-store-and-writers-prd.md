# Cycle Store And Writers PRD

## Purpose

Persistence of event/state histories, PEG event history, open cycles, and writer helper outputs.

## Canonical Code Surfaces

- `screeners/signals/cycle_store.py`
- `screeners/signals/writers.py`
## Output Contract

- `signal_history.csv`, `signal_state_history.csv`, `peg_event_history.csv`, `open_family_cycles.csv`, plus writer-emitted v2 artifact files.

## Closest Tests

- `tests/test_signal_engine_restoration.py`
## Current Fidelity And High-End Target

- Current fidelity: High fidelity; regression tests cover separation and round-trip persistence.
- Higher-end target: Richer lifecycle provenance rather than simple CSV merge history.

## Related Raw Sources

- `docs/audits/archive/2026-03-31-buy-sell-signal-audit.md`
