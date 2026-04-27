# Leader Core Consume Seam PRD

## Purpose

Load and validate leader-core summary, leaders, groups, and market-context artifacts locally.

## Canonical Code Surfaces

- `screeners/leader_core_bridge.py`
- `utils/market_runtime.py`
## Output Contract

- Validated leader-core snapshot and market-truth snapshot.

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_leader_lagging_screener.py`
- `tests/test_weinstein_stage2_screener.py`
## Current Fidelity And High-End Target

- Current fidelity: High fidelity; the seam is narrow and strict.
- Higher-end target: Typed manifest plus broader provenance bundle.

## Related Raw Sources

- `docs/audits/2026-04-02-cross-repo-intent-overlap-audit.md`
