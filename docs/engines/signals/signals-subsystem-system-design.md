# Signals Subsystem System Design

## Source Of Truth

This subsystem doc family is the stable design boundary for `signals`.

## Runtime Flow

- `run_multi_screener_signal_scan()` emits v2 event/state outputs plus summary JSON and cycle/history artifacts.
- `signal_state_history.csv` is intentionally separate from event history.
## Module Boundaries

- `signal-engine-and-state`: Multi-screener signal engine, event/state/combo semantics, update overlays, and output summaries.
- `cycle-store-and-writers`: Persistence of event/state histories, PEG event history, open cycles, and writer helper outputs.
- `screening-augment`: Optional post-screening diagnostics built on merged candidate pools, per-source STUMPY summaries, global lag diagnostics, Chronos-2 reranking, TimesFM comparator reranking, and a shared augment run summary.
## Boundary Rule

- stable docs describe current code first
- runtime audits capture live-vs-design-vs-high-end gaps
- raw sources do not override live contracts

## Closest Tests

- `tests/test_signal_engine_restoration.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_market_intel_bridge.py`
