# Source Registry And Compat Inputs PRD

## Purpose

Candidate-source merge semantics for local screeners, leader-core entries, metadata, and financial caches.

## Canonical Code Surfaces

- `screeners/signals/source_registry.py`
- `screeners/source_contracts.py`
- `screeners/augment/pipeline.py`
## Output Contract

- Merged source registry, canonical source taxonomy, buy-eligible source-spec contract.

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium-high fidelity; merge rules are explicit, style bonuses remain heuristic.
- Higher-end target: Clearer contract versioning and less downstream duplication.

## Related Raw Sources

- `docs/audits/2026-04-02-cross-repo-intent-overlap-audit.md`
