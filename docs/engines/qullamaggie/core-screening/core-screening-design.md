# Core Screening Design

## Source Of Truth

This design doc records the stable boundary for `core-screening` under `qullamaggie`.

## Runtime Surface

- `screeners/qullamaggie/screener.py`
- `screeners/qullamaggie/core.py`
## Output Contract

- `breakout_results.*`, `episode_pivot_results.*`, `parabolic_short_results.*`, `candidate_snapshots.*`, `pattern_excluded_pool.*`, `pattern_included_candidates.*`, `market_summary.json`, `actual_data_calibration.json`.

## Current Fidelity And Higher-End Target

- Medium fidelity; regression-tested, but the notes describe richer higher-end intent than the live rules capture.
- Cleaner setup grammar and better lineage between universe, pattern pool, and canonical candidate artifacts.

## Closest Tests

- `tests/test_qullamaggie_screener.py`
