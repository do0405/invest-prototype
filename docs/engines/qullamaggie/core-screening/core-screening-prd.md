# Core Screening PRD

## Purpose

Breakout, episode-pivot, parabolic-short, and priority-tier logic for the Qullamaggie family.

## Canonical Code Surfaces

- `screeners/qullamaggie/screener.py`
- `screeners/qullamaggie/core.py`
## Output Contract

- `breakout_results.*`, `episode_pivot_results.*`, `parabolic_short_results.*`, `candidate_snapshots.*`, `pattern_excluded_pool.*`, `pattern_included_candidates.*`, `market_summary.json`, `actual_data_calibration.json`.

## Closest Tests

- `tests/test_qullamaggie_screener.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium fidelity; regression-tested, but the notes describe richer higher-end intent than the live rules capture.
- Higher-end target: Cleaner setup grammar and better lineage between universe, pattern pool, and canonical candidate artifacts.

## Related Raw Sources

- `docs/archive/raw-sources/PRD/Q style/qullamaggie_quant_screener_prd_v2.md`
- `docs/archive/raw-sources/screeners/qullamaggie/qullamaggie-algorithm.md`
- `docs/archive/raw-sources/screeners/qullamaggie/pattern.md`
