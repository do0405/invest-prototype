# Cycle Store And Writers Runtime Spec

## Runtime Rules

- The live behavior is owned by the listed code paths and nearest tests.
- Higher-end targets in this doc are not live unless code and tests move.

## Current Fidelity Status

- High fidelity; regression tests cover separation and round-trip persistence.
- Higher-end deferred item: Richer lifecycle provenance rather than simple CSV merge history.

## Closest Tests

- `tests/test_signal_engine_restoration.py`
