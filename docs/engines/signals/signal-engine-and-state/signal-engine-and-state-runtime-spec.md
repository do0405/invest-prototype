# Signal Engine And State Runtime Spec

## Runtime Rules

- The live behavior is owned by the listed code paths and nearest tests.
- Higher-end targets in this doc are not live unless code and tests move.
- Default runtime requires compat truth and may merge leader-core registry overlays.
- Standalone runtime ignores compat truth entirely, requires local screener outputs, and applies a local benchmark-only market truth overlay.

## Current Fidelity Status

- Medium-high fidelity because regression coverage is deep, but semantics remain partly heuristic and proxy-backed.
- Standalone market truth is implemented for continuity, but it is heuristic and high-end deferred rather than canonical.
- Higher-end deferred item: Explicit semantic status, better family arbitration, clearer calibration lineage.

## Closest Tests

- `tests/test_signal_engine_restoration.py`
- `tests/test_signals_package.py`
- `tests/test_market_intel_bridge.py`
