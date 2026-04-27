# Leader Lagging Runtime Status

## Live Default

- `run_leader_lagging_screening()` persists pattern pools, leader/follower subsets, pair outputs, group dashboards, and `market_summary.json`.
## Module Status

| Module | Implementation status | Current fidelity | Higher-end target |
| --- | --- | --- | --- |
| `leader-lagging-screening` | `implemented`, `heuristic`, `high-end deferred` | Medium-high fidelity; current behavior is clear, but tactical scoring is not canonical cross-repo truth. | Stronger decomposition between tactical overlays, canonical truth, and calibration lineage. |

## Known Gaps

- Local leader heuristics still coexist with canonical core truth downstream.
- Higher-end group semantics are not yet versioned.
## Closest Tests

- `tests/test_leader_lagging_screener.py`
- `tests/test_market_intel_bridge.py`
