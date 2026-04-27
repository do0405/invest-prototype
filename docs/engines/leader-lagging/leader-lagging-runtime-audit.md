# Leader Lagging Runtime Audit

## Scope

Code-grounded audit of the current `leader-lagging` runtime in `invest-prototype-main`.

## Code-Grounded Runtime Path

- `screeners/leader_lagging/screener.py`
- `screeners/leader_core_bridge.py`
## Live Baseline

- `run_leader_lagging_screening()` persists pattern pools, leader/follower subsets, pair outputs, group dashboards, and `market_summary.json`.
## Artifact Surface

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| leader-lagging-screening | `screeners/leader_lagging/screener.py` | `pattern_excluded_pool.csv`, `pattern_included_candidates.csv`, `leaders.csv`, `followers.csv`, `leader_follower_pairs.csv`, `group_dashboard.csv`, `leader_quality_diagnostics.csv`, `leader_quality_diagnostics.json`, `leader_quality_summary.json`, `leader_candidate_quality_diagnostics.csv`, `leader_candidate_quality_diagnostics.json`, `leader_candidate_quality_summary.json`, `leader_threshold_tuning_report.csv`, `leader_threshold_tuning_report.json`, `market_summary.json`, `actual_data_calibration.json`. | operators/tests |
## Test Evidence

- `tests/test_leader_lagging_screener.py`
- `tests/test_market_intel_bridge.py`
## Known Gaps

- Local leader heuristics still coexist with canonical core truth downstream.
- Higher-end group semantics are not yet versioned.
## Higher-End Reference Target

- Higher-end target: tighter separation between tactical overlays and canonical upstream truth.
