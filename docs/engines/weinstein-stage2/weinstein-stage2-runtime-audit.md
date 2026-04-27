# Weinstein Stage2 Runtime Audit

## Scope

Code-grounded audit of the current `weinstein-stage2` runtime in `invest-prototype-main`.

## Code-Grounded Runtime Path

- `screeners/weinstein_stage2/screener.py`
## Live Baseline

- `run_weinstein_stage2_screening()` persists `all_results`, candidate subsets, group rankings, and market summary artifacts.
## Artifact Surface

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| stage2-screening | `screeners/weinstein_stage2/screener.py` | `all_results.csv`, `pattern_excluded_pool.csv`, `primary_candidates.csv`, `secondary_candidates.csv`, `breakout_week_candidates.csv`, `fresh_stage2_candidates.csv`, `retest_candidates.csv`, `market_summary.json`. | operators/tests |
## Test Evidence

- `tests/test_weinstein_stage2_screener.py`
## Known Gaps

- Timing-state labels and cutoffs are heuristic.
- Higher-end state taxonomies in legacy notes exceed current code.
## Higher-End Reference Target

- Higher-end target: clearer state machine and richer benchmark-context explanation.
