# Financial And Integrated Screening Technical Specification

## Canonical Code Surfaces

- `screeners/markminervini/data_fetching.py`
- `screeners/markminervini/financial_calculators.py`
- `screeners/markminervini/advanced_financial.py`
- `screeners/markminervini/integrated_screener.py`
## Output Contract

- `advanced_financial_results.csv`, `integrated_results.csv`, `integrated_actionable_patterns.csv`, pattern split variants, calibration JSON.

## Fidelity Status

- Medium fidelity; real and test-backed, but not fully calibrated.
- Explicit calibration lineage and cleaner separation of raw metrics vs derived rank semantics.

## Closest Tests

- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
