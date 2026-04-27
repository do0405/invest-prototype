# Markminervini Subsystem System Design

## Source Of Truth

This subsystem doc family is the stable design boundary for `markminervini`.

## Runtime Flow

- `with_rs.csv` is the base technical artifact.
- Integrated screening writes pattern-aware and patternless variants plus actionable subsets.
## Module Boundaries

- `technical-and-pattern-screening`: Trend-template screening, RS handling, and pattern-analysis surfaces for Minervini-style candidates.
- `financial-and-integrated-screening`: Provider-backed financial collection, advanced financial scoring, and integrated technical-financial-pattern ranking.
- `ticker-tracking-and-artifacts`: Previous/current candidate snapshot tracking and new-ticker artifacts.
## Boundary Rule

- stable docs describe current code first
- runtime audits capture live-vs-design-vs-high-end gaps
- raw sources do not override live contracts

## Closest Tests

- `tests/test_markminervini_screener.py`
- `tests/test_markminervini_pattern_analyzer.py`
- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
