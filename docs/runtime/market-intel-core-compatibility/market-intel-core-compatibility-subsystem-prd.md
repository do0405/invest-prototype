# Market Intel Core Compatibility Subsystem PRD

## Purpose

Consume-side compatibility boundary between this repo and `market-intel-core` exported artifacts.

## Canonical Code Surfaces

- `screeners/leader_core_bridge.py`
- `screeners/signals/source_registry.py`
- `screeners/source_contracts.py`
- `utils/market_runtime.py`
## Module Set

- `leader-core-consume-seam`: Load and validate leader-core summary, leaders, groups, and market-context artifacts locally.
- `source-registry-and-compat-inputs`: Candidate-source merge semantics for local screeners, leader-core entries, metadata, and financial caches.
## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
## Live Default

- Compatibility inputs must be same-day and schema-matched.
- `leader_core_v1` and `market_context_v1` are the active consume-side contracts.
## Higher-End Posture

- Higher-end target: typed manifests, compatibility warnings, provenance for partial overlays.
## Related Raw Sources

- `docs/audits/2026-04-02-cross-repo-intent-overlap-audit.md`
