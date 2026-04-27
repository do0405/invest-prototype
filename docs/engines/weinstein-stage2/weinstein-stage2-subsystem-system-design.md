# Weinstein Stage2 Subsystem System Design

## Source Of Truth

This subsystem doc family is the stable design boundary for `weinstein-stage2`.

## Runtime Flow

- `run_weinstein_stage2_screening()` persists `all_results`, candidate subsets, group rankings, and market summary artifacts.
## Module Boundaries

- `stage2-screening`: Local market context computation, timing-state classification, and multi-bucket Weinstein candidate outputs.
## Boundary Rule

- stable docs describe current code first
- runtime audits capture live-vs-design-vs-high-end gaps
- raw sources do not override live contracts

## Closest Tests

- `tests/test_weinstein_stage2_screener.py`
