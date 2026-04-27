# Financial And Integrated Screening PRD

## Purpose

Provider-backed financial collection, advanced financial scoring, and integrated technical-financial-pattern ranking.

## Canonical Code Surfaces

- `screeners/markminervini/data_fetching.py`
- `screeners/markminervini/financial_calculators.py`
- `screeners/markminervini/advanced_financial.py`
- `screeners/markminervini/integrated_screener.py`
## Output Contract

- `advanced_financial_results.csv`, `integrated_results.csv`, `integrated_actionable_patterns.csv`, pattern split variants, calibration JSON.

## Closest Tests

- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium fidelity; real and test-backed, but not fully calibrated.
- Higher-end target: Explicit calibration lineage and cleaner separation of raw metrics vs derived rank semantics.

## Related Raw Sources

- `docs/archive/raw-sources/PRD/VCP & cup and handle/minervini_vcp_cup_handle_quant_screener_prd_v2.md`
