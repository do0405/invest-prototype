# Ticker Tracking And Artifacts ERD

## Entities

- ticker-tracking-and-artifacts
- output artifacts described below

## Producers

- `screeners/markminervini/ticker_tracker.py`
- `utils/market_runtime.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| ticker-tracking-and-artifacts outputs | `screeners/markminervini/ticker_tracker.py, utils/market_runtime.py` | `new_tickers.csv` / `.json` and previous snapshot update. | operators/tests |

## Closest Tests

- `tests/test_orchestrator_tasks.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
