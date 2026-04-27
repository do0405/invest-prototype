# Leader Core Consume Seam Runtime Spec

## Runtime Rules

- The live behavior is owned by the listed code paths and nearest tests.
- Higher-end targets in this doc are not live unless code and tests move.
- Default runtime consumes `leader_core_v1` and `market_context_v1` strictly.
- Standalone runtime does not consume this seam; affected engines receive local benchmark-only truth or empty leader-core annotation instead.

## Current Fidelity Status

- High fidelity; the seam is narrow and strict.
- Standalone bypass is implemented as an explicit non-parity mode, so high-end consume semantics remain deferred outside the default path.
- Higher-end deferred item: Typed manifest plus broader provenance bundle.

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_leader_lagging_screener.py`
- `tests/test_weinstein_stage2_screener.py`
