# Qullamaggie Runtime Audit

## Scope

Code-grounded audit of the current `qullamaggie` runtime in `invest-prototype-main`.

## Code-Grounded Runtime Path

- `screeners/qullamaggie/screener.py`
- `screeners/qullamaggie/core.py`
- `screeners/qullamaggie/earnings_data_collector.py`
## Live Baseline

- `run_qullamaggie_screening()` persists setup-specific result files, pattern pools, and market summaries.
## Artifact Surface

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| core-screening | `screeners/qullamaggie/screener.py, screeners/qullamaggie/core.py` | `breakout_results.*`, `episode_pivot_results.*`, `parabolic_short_results.*`, `candidate_snapshots.*`, `pattern_excluded_pool.*`, `pattern_included_candidates.*`, `market_summary.json`, `actual_data_calibration.json`. | operators/tests |
| earnings-enrichment | `screeners/qullamaggie/earnings_data_collector.py, screeners/qullamaggie/screener.py` | Provider-backed earnings payloads and status markers used during screening. | operators/tests |
## Test Evidence

- `tests/test_qullamaggie_screener.py`
- `tests/test_qullamaggie_earnings_cache.py`
## Known Gaps

- Higher-end strategy semantics remain richer in legacy notes than in the current rule set.
- Earnings semantics are operational and cache-backed rather than a full event model.
## Higher-End Reference Target

- Higher-end target: clearer episode-event lineage and cleaner candidate lineage semantics.
