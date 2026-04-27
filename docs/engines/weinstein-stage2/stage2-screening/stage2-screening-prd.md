# Stage2 Screening PRD

## Purpose

Local market context computation, timing-state classification, and multi-bucket Weinstein candidate outputs.

## Canonical Code Surfaces

- `screeners/weinstein_stage2/screener.py`
## Output Contract

- `all_results.csv`, `pattern_excluded_pool.csv`, `primary_candidates.csv`, `secondary_candidates.csv`, `breakout_week_candidates.csv`, `fresh_stage2_candidates.csv`, `retest_candidates.csv`, `market_summary.json`.

## Closest Tests

- `tests/test_weinstein_stage2_screener.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium-high fidelity for live code.
- Higher-end target: Clearer timing-state lineage and more formal stage-transition semantics.

## Related Raw Sources

- `docs/archive/raw-sources/PRD/stage2 analysis/weinstein_stage2_quant_screener_prd.md`
