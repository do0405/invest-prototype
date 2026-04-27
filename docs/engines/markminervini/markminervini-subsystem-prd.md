# Markminervini Subsystem PRD

## Purpose

Trend-template screening, pattern analysis, financial enrichment, integrated ranking, and ticker-tracking artifacts.

## Canonical Code Surfaces

- `screeners/markminervini/screener.py`
- `screeners/markminervini/enhanced_pattern_analyzer.py`
- `screeners/markminervini/advanced_financial.py`
- `screeners/markminervini/financial_calculators.py`
- `screeners/markminervini/data_fetching.py`
- `screeners/markminervini/integrated_screener.py`
- `screeners/markminervini/ticker_tracker.py`
## Module Set

- `technical-and-pattern-screening`: Trend-template screening, RS handling, and pattern-analysis surfaces for Minervini-style candidates.
- `financial-and-integrated-screening`: Provider-backed financial collection, advanced financial scoring, and integrated technical-financial-pattern ranking.
- `ticker-tracking-and-artifacts`: Previous/current candidate snapshot tracking and new-ticker artifacts.
## Closest Tests

- `tests/test_markminervini_screener.py`
- `tests/test_markminervini_pattern_analyzer.py`
- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
## Live Default

- `with_rs.csv` is the base technical artifact.
- Integrated screening writes pattern-aware and patternless variants plus actionable subsets.
## Higher-End Posture

- Higher-end target: clearer pattern grammar, richer evidence provenance, explicit calibration status.
## Related Raw Sources

- `docs/archive/raw-sources/PRD/VCP & cup and handle/`
- `docs/archive/raw-sources/screeners/markminervini/pattern.md`
