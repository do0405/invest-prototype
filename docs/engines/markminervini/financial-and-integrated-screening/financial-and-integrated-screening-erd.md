# Financial And Integrated Screening ERD

## Entities

- financial-and-integrated-screening
- output artifacts described below

## Producers

- `screeners/markminervini/data_fetching.py`
- `screeners/markminervini/financial_calculators.py`
- `screeners/markminervini/advanced_financial.py`
- `screeners/markminervini/integrated_screener.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| financial-and-integrated-screening outputs | `screeners/markminervini/data_fetching.py, screeners/markminervini/financial_calculators.py, screeners/markminervini/advanced_financial.py, screeners/markminervini/integrated_screener.py` | `advanced_financial_results.csv`, `integrated_results.csv`, `integrated_actionable_patterns.csv`, pattern split variants, calibration JSON. | operators/tests |

## Closest Tests

- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
