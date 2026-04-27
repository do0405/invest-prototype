# Technical And Pattern Screening ERD

## Entities

- technical-and-pattern-screening
- output artifacts described below

## Producers

- `screeners/markminervini/screener.py`
- `screeners/markminervini/enhanced_pattern_analyzer.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| technical-and-pattern-screening outputs | `screeners/markminervini/screener.py, screeners/markminervini/enhanced_pattern_analyzer.py` | `with_rs.csv` plus VCP/cup-handle evidence and state buckets. | operators/tests |

## Closest Tests

- `tests/test_markminervini_screener.py`
- `tests/test_markminervini_pattern_analyzer.py`
