# Weinstein Stage2 Subsystem PRD

## Purpose

Local Weinstein early-stage and breakout-week screener.

## Canonical Code Surfaces

- `screeners/weinstein_stage2/screener.py`
## Module Set

- `stage2-screening`: Local market context computation, timing-state classification, and multi-bucket Weinstein candidate outputs.
## Closest Tests

- `tests/test_weinstein_stage2_screener.py`
## Live Default

- `run_weinstein_stage2_screening()` persists `all_results`, candidate subsets, group rankings, and market summary artifacts.
## Higher-End Posture

- Higher-end target: clearer state machine and richer benchmark-context explanation.
## Related Raw Sources

- `docs/archive/raw-sources/PRD/stage2 analysis/weinstein_stage2_quant_screener_prd.md`
