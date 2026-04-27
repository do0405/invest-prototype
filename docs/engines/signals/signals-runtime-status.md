# Signals Runtime Status

## Live Default

- `run_multi_screener_signal_scan()` emits v2 event/state outputs plus summary JSON and cycle/history artifacts.
- `signal_state_history.csv` is intentionally separate from event history.
- Default mode reads strict compat truth and leader-core overlay inputs.
- `--standalone` emits signals from local screener registry outputs only, adds `market_truth_source=local_standalone`, and sets `core_overlay_applied=false`.
## Module Status

| Module | Implementation status | Current fidelity | Higher-end target |
| --- | --- | --- | --- |
| `signal-engine-and-state` | `implemented`, `heuristic`, `proxy-backed`, `high-end deferred` | Medium-high fidelity because regression coverage is deep, but semantics remain partly heuristic and proxy-backed; standalone local truth is intentionally heuristic. | Explicit semantic status, better family arbitration, clearer calibration lineage. |
| `cycle-store-and-writers` | `implemented` | High fidelity; regression tests cover separation and round-trip persistence. | Richer lifecycle provenance rather than simple CSV merge history. |
| `screening-augment` | `implemented`, `heuristic`, `high-end deferred` | Medium fidelity; explicit output contract, but intentionally optional and experimental. | Stronger provenance and clearer separation between experimental reranking and canonical runtime truth. |

## Known Gaps

- The engine is a large monolith with heuristic overlays and open policy choices around family arbitration and sizing.
- Calibrated/model-backed semantics remain deferred; current labels are mostly heuristic or proxy-backed.
- Standalone mode preserves continuity honestly, but it does not attempt `market-intel-core` parity for breadth, rotation, or leader provenance.
- `screening-augment` keeps `Chronos-2` and `TimesFM 2.5` optional and experimental; constrained local machines should default to imports-only readiness checks instead of full live inference.
## Closest Tests

- `tests/test_signal_engine_restoration.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_market_intel_bridge.py`
