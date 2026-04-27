# Leader Lagging Screening Technical Specification

## Canonical Code Surfaces

- `screeners/leader_lagging/screener.py`
## Output Contract

- `pattern_excluded_pool.csv`, `pattern_included_candidates.csv`, `leaders.csv`, `followers.csv`, `leader_follower_pairs.csv`, `group_dashboard.csv`, `leader_quality_diagnostics.csv`, `leader_quality_diagnostics.json`, `leader_quality_summary.json`, `leader_candidate_quality_diagnostics.csv`, `leader_candidate_quality_diagnostics.json`, `leader_candidate_quality_summary.json`, `leader_threshold_tuning_report.csv`, `leader_threshold_tuning_report.json`, `market_summary.json`, `actual_data_calibration.json`.

## Fidelity Status

- Medium-high fidelity; current behavior is clear, but tactical scoring is not canonical cross-repo truth.
- Stronger decomposition between tactical overlays, canonical truth, and calibration lineage.

## Closest Tests

- `tests/test_leader_lagging_screener.py`
- `tests/test_market_intel_bridge.py`
