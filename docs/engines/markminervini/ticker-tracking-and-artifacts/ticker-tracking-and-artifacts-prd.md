# Ticker Tracking And Artifacts PRD

## Purpose

Previous/current candidate snapshot tracking and new-ticker artifacts.

## Canonical Code Surfaces

- `screeners/markminervini/ticker_tracker.py`
- `utils/market_runtime.py`
## Output Contract

- `new_tickers.csv` / `.json` and previous snapshot update.

## Closest Tests

- `tests/test_orchestrator_tasks.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium fidelity; operationally useful but simple.
- Higher-end target: Richer candidate lineage and state-transition history.

## Related Raw Sources

- None
