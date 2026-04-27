# OHLCV Normalization And Storage Runtime Spec

## Runtime Rules

- The live behavior is owned by the listed code paths and nearest tests.
- Higher-end targets in this doc are not live unless code and tests move.

## Current Fidelity Status

- High fidelity for the live storage contract.
- Higher-end deferred item: Explicit schema/version manifest instead of convention-only CSV rules.

## Closest Tests

- `tests/test_market_data_contract.py`
- `tests/test_market_runtime.py`
- `tests/test_symbol_normalization.py`
