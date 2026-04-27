# Market Intel Core Compatibility Subsystem System Design

## Source Of Truth

This subsystem doc family is the stable design boundary for `market-intel-core-compatibility`.

## Runtime Flow

- Compatibility inputs must be same-day and schema-matched.
- `leader_core_v1` and `market_context_v1` are the active consume-side contracts.
## Module Boundaries

- `leader-core-consume-seam`: Load and validate leader-core summary, leaders, groups, and market-context artifacts locally.
- `source-registry-and-compat-inputs`: Candidate-source merge semantics for local screeners, leader-core entries, metadata, and financial caches.
## Boundary Rule

- stable docs describe current code first
- runtime audits capture live-vs-design-vs-high-end gaps
- raw sources do not override live contracts

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
