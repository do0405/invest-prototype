# Stage2 Screening ERD

## Entities

- stage2-screening
- output artifacts described below

## Producers

- `screeners/weinstein_stage2/screener.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| stage2-screening outputs | `screeners/weinstein_stage2/screener.py` | `all_results.csv`, `pattern_excluded_pool.csv`, `primary_candidates.csv`, `secondary_candidates.csv`, `breakout_week_candidates.csv`, `fresh_stage2_candidates.csv`, `retest_candidates.csv`, `market_summary.json`. | operators/tests |

## Closest Tests

- `tests/test_weinstein_stage2_screener.py`
