# Ticker Tracking And Artifacts Design

## Source Of Truth

This design doc records the stable boundary for `ticker-tracking-and-artifacts` under `markminervini`.

## Runtime Surface

- `screeners/markminervini/ticker_tracker.py`
- `utils/market_runtime.py`
## Output Contract

- `new_tickers.csv` / `.json` and previous snapshot update.

## Current Fidelity And Higher-End Target

- Medium fidelity; operationally useful but simple.
- Richer candidate lineage and state-transition history.

## Closest Tests

- `tests/test_orchestrator_tasks.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
