# Leader Lagging Screening ERD

## Entities

- leader-lagging-screening
- output artifacts described below

## Producers

- `screeners/leader_lagging/screener.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| leader-lagging-screening outputs | `screeners/leader_lagging/screener.py` | `pattern_excluded_pool.csv`, `pattern_included_candidates.csv`, `leaders.csv`, `followers.csv`, `leader_follower_pairs.csv`, `group_dashboard.csv`, `leader_quality_diagnostics.csv`, `leader_quality_diagnostics.json`, `leader_quality_summary.json`, `leader_candidate_quality_diagnostics.csv`, `leader_candidate_quality_diagnostics.json`, `leader_candidate_quality_summary.json`, `leader_threshold_tuning_report.csv`, `leader_threshold_tuning_report.json`, `market_summary.json`, `actual_data_calibration.json`. | operators/tests |

## Closest Tests

- `tests/test_leader_lagging_screener.py`
- `tests/test_market_intel_bridge.py`
