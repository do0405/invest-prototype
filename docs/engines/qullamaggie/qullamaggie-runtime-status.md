# Qullamaggie Runtime Status

## Live Default

- `run_qullamaggie_screening()` persists setup-specific result files, watchlists, pattern pools, and market summaries.
## Module Status

| Module | Implementation status | Current fidelity | Higher-end target |
| --- | --- | --- | --- |
| `core-screening` | `implemented`, `heuristic`, `high-end deferred` | Medium fidelity; regression-tested, but the notes describe richer higher-end intent than the live rules capture. | Cleaner setup grammar and better lineage between universe, pattern pool, weekly focus, and daily focus artifacts. |
| `earnings-enrichment` | `implemented`, `proxy-backed` | Medium fidelity; useful and well-tested, but not a richer event-modeling contract. | Stronger event provenance and clearer unavailable/stale/confirmed state separation. |

## Known Gaps

- Higher-end strategy semantics remain richer in legacy notes than in the current rule set.
- Earnings semantics are operational and cache-backed rather than a full event model.
## Closest Tests

- `tests/test_qullamaggie_screener.py`
- `tests/test_qullamaggie_earnings_cache.py`
