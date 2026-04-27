# Core Screening ERD

## Entities

- core-screening
- output artifacts described below

## Producers

- `screeners/qullamaggie/screener.py`
- `screeners/qullamaggie/core.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| core-screening outputs | `screeners/qullamaggie/screener.py, screeners/qullamaggie/core.py` | `breakout_results.*`, `episode_pivot_results.*`, `parabolic_short_results.*`, `candidate_snapshots.*`, `pattern_excluded_pool.*`, `pattern_included_candidates.*`, `market_summary.json`, `actual_data_calibration.json`. | operators/tests |

## Closest Tests

- `tests/test_qullamaggie_screener.py`
