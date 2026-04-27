# Signals Runtime Audit

## Scope

Code-grounded audit of the current `signals` runtime in `invest-prototype-main`.

## Code-Grounded Runtime Path

- `screeners/signals/engine.py`
- `screeners/signals/cycle_store.py`
- `screeners/signals/metrics.py`
- `screeners/signals/writers.py`
- `screeners/signals/__init__.py`
- `screeners/augment/pipeline.py`
- `screeners/augment/stumpy_sidecar.py`
- `screeners/augment/chronos_rerank.py`
- `screeners/augment/timesfm_rerank.py`
- `screeners/augment/tsfm_metrics.py`
## Live Baseline

- `run_multi_screener_signal_scan()` emits v2 event/state outputs plus summary JSON and cycle/history artifacts.
- `signal_state_history.csv` is intentionally separate from event history.
## Artifact Surface

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| signal-engine-and-state | `screeners/signals/engine.py, screeners/signals/metrics.py` | `trend_following_events_v2.csv`, `trend_following_states_v2.csv`, `ultimate_growth_events_v2.csv`, `ultimate_growth_states_v2.csv`, `ug_strategy_combos_v2.csv`, `all_signals_v2.csv`, `open_family_cycles.csv`, `screen_signal_diagnostics.csv`, `signal_universe_snapshot.csv`, `signal_summary.json`, `source_registry_summary.json`. | operators/tests |
| cycle-store-and-writers | `screeners/signals/cycle_store.py, screeners/signals/writers.py` | `signal_history.csv`, `signal_state_history.csv`, `peg_event_history.csv`, `open_family_cycles.csv`, plus writer-emitted v2 artifact files. | operators/tests |
| screening-augment | `screeners/augment/pipeline.py, screeners/augment/stumpy_sidecar.py, screeners/augment/chronos_rerank.py, screeners/augment/timesfm_rerank.py, screeners/augment/tsfm_metrics.py` | `merged_candidate_pool.csv`, per-source `*_stumpy_summary.csv`, `stumpy_global_pairs.csv`, `chronos2_rerank.csv`, `timesfm2p5_rerank.csv`, and `augment_run_summary.json`. Diagnostic-only artifacts; not signal-gating inputs. | operators/tests |
## Test Evidence

- `tests/test_signal_engine_restoration.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_market_intel_bridge.py`
## Known Gaps

- The engine is a large monolith with heuristic overlays and open policy choices around family arbitration and sizing.
- Calibrated/model-backed semantics remain deferred; current labels are mostly heuristic or proxy-backed.
## Higher-End Reference Target

- Higher-end target: clearer family arbitration, typed state transitions, stronger calibration lineage, explicit semantic status.
