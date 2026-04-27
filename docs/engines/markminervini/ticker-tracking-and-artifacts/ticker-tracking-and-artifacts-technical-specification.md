# Ticker Tracking And Artifacts Technical Specification

## Canonical Code Surfaces

- `screeners/markminervini/ticker_tracker.py`
- `utils/market_runtime.py`
## Output Contract

- `new_tickers.csv` / `.json` and previous snapshot update.

## Fidelity Status

- Medium fidelity; operationally useful but simple.
- Richer candidate lineage and state-transition history.

## Closest Tests

- `tests/test_orchestrator_tasks.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
