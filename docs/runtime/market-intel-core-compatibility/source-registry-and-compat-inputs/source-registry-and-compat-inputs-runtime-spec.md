# Source Registry And Compat Inputs Runtime Spec

## Runtime Rules

- The live behavior is owned by the listed code paths and nearest tests.
- Higher-end targets in this doc are not live unless code and tests move.
- Default signal/runtime merges may include `MIC_LEADER_CORE` registry overlays.
- Standalone signal/runtime reads only local screener registry inputs and must fail fast if those local outputs are absent.

## Current Fidelity Status

- Medium-high fidelity; merge rules are explicit, style bonuses remain heuristic.
- Standalone local-registry-only mode is implemented, but its market overlay is heuristic and high-end deferred.
- Higher-end deferred item: Clearer contract versioning and less downstream duplication.

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
