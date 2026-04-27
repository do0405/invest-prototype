# Financial And Integrated Screening Design

## Source Of Truth

This design doc records the stable boundary for `financial-and-integrated-screening` under `markminervini`.

## Runtime Surface

- `screeners/markminervini/data_fetching.py`
- `screeners/markminervini/financial_calculators.py`
- `screeners/markminervini/advanced_financial.py`
- `screeners/markminervini/integrated_screener.py`
## Output Contract

- `advanced_financial_results.csv`, `integrated_results.csv`, `integrated_actionable_patterns.csv`, pattern split variants, calibration JSON.

## Current Fidelity And Higher-End Target

- Medium fidelity; real and test-backed, but not fully calibrated.
- Explicit calibration lineage and cleaner separation of raw metrics vs derived rank semantics.

## Closest Tests

- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
