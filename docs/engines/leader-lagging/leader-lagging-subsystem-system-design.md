# Leader Lagging Subsystem System Design

## Source Of Truth

This subsystem doc family is the stable design boundary for `leader-lagging`.

## Runtime Flow

- `run_leader_lagging_screening()` persists pattern pools, leader/follower subsets, pair outputs, group dashboards, and `market_summary.json`.
## Module Boundaries

- `leader-lagging-screening`: Leader/follower scoring, market-context overlays, and persisted leader-family outputs.
## Boundary Rule

- stable docs describe current code first
- runtime audits capture live-vs-design-vs-high-end gaps
- raw sources do not override live contracts

## Closest Tests

- `tests/test_leader_lagging_screener.py`
- `tests/test_market_intel_bridge.py`
