# Qullamaggie Subsystem System Design

## Source Of Truth

This subsystem doc family is the stable design boundary for `qullamaggie`.

## Runtime Flow

- `run_qullamaggie_screening()` persists setup-specific result files, watchlists, pattern pools, and market summaries.
## Module Boundaries

- `core-screening`: Breakout, episode-pivot, parabolic-short, watchlist, and priority-tier logic for the Qullamaggie family.
- `earnings-enrichment`: Cached earnings-event enrichment and gating semantics inside Qullamaggie screening.
## Boundary Rule

- stable docs describe current code first
- runtime audits capture live-vs-design-vs-high-end gaps
- raw sources do not override live contracts

## Closest Tests

- `tests/test_qullamaggie_screener.py`
- `tests/test_qullamaggie_earnings_cache.py`
