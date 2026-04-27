# Qullamaggie Subsystem PRD

## Purpose

Breakout, episode-pivot, parabolic-short, watchlist, and earnings-enrichment logic in the local Qullamaggie family.

## Canonical Code Surfaces

- `screeners/qullamaggie/screener.py`
- `screeners/qullamaggie/core.py`
- `screeners/qullamaggie/earnings_data_collector.py`
## Module Set

- `core-screening`: Breakout, episode-pivot, parabolic-short, watchlist, and priority-tier logic for the Qullamaggie family.
- `earnings-enrichment`: Cached earnings-event enrichment and gating semantics inside Qullamaggie screening.
## Closest Tests

- `tests/test_qullamaggie_screener.py`
- `tests/test_qullamaggie_earnings_cache.py`
## Live Default

- `run_qullamaggie_screening()` persists setup-specific result files, watchlists, pattern pools, and market summaries.
## Higher-End Posture

- Higher-end target: clearer episode-event lineage and richer watchlist priority semantics.
## Related Raw Sources

- `docs/archive/raw-sources/PRD/Q style/qullamaggie_quant_screener_prd_v2.md`
- `docs/archive/raw-sources/screeners/qullamaggie/qullamaggie-algorithm.md`
- `docs/archive/raw-sources/screeners/qullamaggie/pattern.md`
