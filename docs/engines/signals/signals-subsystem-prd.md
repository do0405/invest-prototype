# Signals Subsystem PRD

## Purpose

Local multi-screener signal engine, cycle store, writer outputs, and augment diagnostics.

## Canonical Code Surfaces

- `screeners/signals/engine.py`
- `screeners/signals/cycle_store.py`
- `screeners/signals/metrics.py`
- `screeners/signals/writers.py`
- `screeners/signals/__init__.py`
- `screeners/augment/pipeline.py`
- `screeners/augment/stumpy_sidecar.py`
- `screeners/augment/chronos_rerank.py`
- `screeners/augment/timesfm_rerank.py`
- `screeners/augment/lag_diagnostics.py`
- `screeners/augment/run_summary.py`
## Module Set

- `signal-engine-and-state`: Multi-screener signal engine, event/state/combo semantics, update overlays, and output summaries.
- `cycle-store-and-writers`: Persistence of event/state histories, PEG event history, open cycles, and writer helper outputs.
- `screening-augment`: Optional post-screening diagnostics built on merged candidate pools, per-source STUMPY summaries, global lag diagnostics, Chronos-2 reranking, TimesFM comparator reranking, and a shared augment run summary.
## Closest Tests

- `tests/test_signal_engine_restoration.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_market_intel_bridge.py`
## Live Default

- `run_multi_screener_signal_scan()` emits v2 event/state outputs plus summary JSON and cycle/history artifacts.
- `MultiScreenerSignalEngine` and `run_multi_screener_signal_scan()` are canonical public names; shorter signal package aliases are compatibility shims, not separate engines.
- `signal_state_history.csv` is intentionally separate from event history.
## Higher-End Posture

- Higher-end target: clearer family arbitration, typed state transitions, stronger calibration lineage, explicit semantic status.
## Related Raw Sources

- `docs/audits/archive/2026-03-31-buy-sell-signal-audit.md`
- `docs/archive/raw-sources/docs/2026-03-31-invest-prototype-no-label-ai-augmentation-shortlist.md`
