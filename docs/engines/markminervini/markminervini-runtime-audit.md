# Markminervini Runtime Audit

## Scope

Code-grounded audit of the current `markminervini` runtime in `invest-prototype-main`.

## Code-Grounded Runtime Path

- `screeners/markminervini/screener.py`
- `screeners/markminervini/enhanced_pattern_analyzer.py`
- `screeners/markminervini/advanced_financial.py`
- `screeners/markminervini/financial_calculators.py`
- `screeners/markminervini/data_fetching.py`
- `screeners/markminervini/integrated_screener.py`
- `screeners/markminervini/ticker_tracker.py`
## Live Baseline

- `with_rs.csv` is the base technical artifact.
- Integrated screening writes pattern-aware and patternless variants plus actionable subsets.
## Artifact Surface

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| technical-and-pattern-screening | `screeners/markminervini/screener.py, screeners/markminervini/enhanced_pattern_analyzer.py` | `with_rs.csv` plus VCP/cup-handle evidence and state buckets. | operators/tests |
| financial-and-integrated-screening | `screeners/markminervini/data_fetching.py, screeners/markminervini/financial_calculators.py, screeners/markminervini/advanced_financial.py, screeners/markminervini/integrated_screener.py` | `advanced_financial_results.csv`, `integrated_results.csv`, `integrated_actionable_patterns.csv`, pattern split variants, calibration JSON. | operators/tests |
| ticker-tracking-and-artifacts | `screeners/markminervini/ticker_tracker.py, utils/market_runtime.py` | `new_tickers.csv` / `.json` and previous snapshot update. | operators/tests |
## Test Evidence

- `tests/test_markminervini_screener.py`
- `tests/test_markminervini_pattern_analyzer.py`
- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
## Known Gaps

- Pattern and financial evidence remain heuristic/cache-backed.
- Legacy PRDs describe higher-end semantics beyond current code.
## Higher-End Reference Target

- Higher-end target: clearer pattern grammar, richer evidence provenance, explicit calibration status.
