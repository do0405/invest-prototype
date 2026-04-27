# Financial And Integrated Screening Runtime Spec

## Runtime Rules

- The live behavior is owned by the listed code paths and nearest tests.
- Higher-end targets in this doc are not live unless code and tests move.

## Current Fidelity Status

- Medium fidelity; real and test-backed, but not fully calibrated.
- Higher-end deferred item: Explicit calibration lineage and cleaner separation of raw metrics vs derived rank semantics.

## Closest Tests

- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
