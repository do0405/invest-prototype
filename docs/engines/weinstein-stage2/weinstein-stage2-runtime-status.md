# Weinstein Stage2 Runtime Status

## Live Default

- `run_weinstein_stage2_screening()` persists `all_results`, candidate subsets, group rankings, and market summary artifacts.
## Module Status

| Module | Implementation status | Current fidelity | Higher-end target |
| --- | --- | --- | --- |
| `stage2-screening` | `implemented`, `heuristic` | Medium-high fidelity for live code. | Clearer timing-state lineage and more formal stage-transition semantics. |

## Known Gaps

- Timing-state labels and cutoffs are heuristic.
- Higher-end state taxonomies in legacy notes exceed current code.
## Closest Tests

- `tests/test_weinstein_stage2_screener.py`
