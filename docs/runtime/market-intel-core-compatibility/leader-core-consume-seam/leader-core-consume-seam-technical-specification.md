# Leader Core Consume Seam Technical Specification

## Canonical Code Surfaces

- `screeners/leader_core_bridge.py`
- `utils/market_runtime.py`
## Output Contract

- Validated leader-core snapshot and market-truth snapshot.

## Fidelity Status

- High fidelity; the seam is narrow and strict.
- Typed manifest plus broader provenance bundle.

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_leader_lagging_screener.py`
- `tests/test_weinstein_stage2_screener.py`
