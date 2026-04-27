# Stage2 Screening Design

## Source Of Truth

This design doc records the stable boundary for `stage2-screening` under `weinstein-stage2`.

## Runtime Surface

- `screeners/weinstein_stage2/screener.py`
## Output Contract

- `all_results.csv`, `pattern_excluded_pool.csv`, `primary_candidates.csv`, `secondary_candidates.csv`, `breakout_week_candidates.csv`, `fresh_stage2_candidates.csv`, `retest_candidates.csv`, `market_summary.json`.

## Current Fidelity And Higher-End Target

- Medium-high fidelity for live code.
- Clearer timing-state lineage and more formal stage-transition semantics.

## Closest Tests

- `tests/test_weinstein_stage2_screener.py`
