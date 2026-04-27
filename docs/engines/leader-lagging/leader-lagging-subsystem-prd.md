# Leader Lagging Subsystem PRD

## Purpose

Local leader/follower screening engine and market-context overlays.

## Canonical Code Surfaces

- `screeners/leader_lagging/screener.py`
- `screeners/leader_core_bridge.py`
## Module Set

- `leader-lagging-screening`: Leader/follower scoring, market-context overlays, and persisted leader-family outputs.
## Closest Tests

- `tests/test_leader_lagging_screener.py`
- `tests/test_market_intel_bridge.py`
## Live Default

- `run_leader_lagging_screening()` persists pattern pools, leader/follower subsets, pair outputs, group dashboards, and `market_summary.json`.
## Higher-End Posture

- Higher-end target: tighter separation between tactical overlays and canonical upstream truth.
## Related Raw Sources

- `docs/archive/raw-sources/PRD/leader stock/leader_lagging_multifactor_prd.md`
- `docs/archive/raw-sources/PRD/leader stock/leader_lagging_rs_screener_prd.md`
