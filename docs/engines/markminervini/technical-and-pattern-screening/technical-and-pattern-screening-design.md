# Technical And Pattern Screening Design

## Source Of Truth

This design doc records the stable boundary for `technical-and-pattern-screening` under `markminervini`.

## Runtime Surface

- `screeners/markminervini/screener.py`
- `screeners/markminervini/enhanced_pattern_analyzer.py`
## Output Contract

- `with_rs.csv` plus VCP/cup-handle evidence and state buckets.

## Current Fidelity And Higher-End Target

- Medium-high fidelity for live code; strategy notes are higher-end reference only.
- Clearer pattern grammar with stronger explainability and state lineage.

## Closest Tests

- `tests/test_markminervini_screener.py`
- `tests/test_markminervini_pattern_analyzer.py`
