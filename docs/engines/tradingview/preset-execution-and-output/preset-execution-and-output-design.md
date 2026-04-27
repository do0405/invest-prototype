# Preset Execution And Output Design

## Source Of Truth

This design doc records the stable boundary for `preset-execution-and-output` under `tradingview`.

## Runtime Surface

- `screeners/tradingview/screener.py`
- `utils/market_runtime.py`
## Output Contract

- Market-specific preset CSV files under `results/{market}/screeners/tradingview`.

## Current Fidelity And Higher-End Target

- Medium-high fidelity; explicit and tested, but intentionally thin.
- Registry-backed preset governance and easier market-to-market diffability.

## Closest Tests

- `tests/test_tradingview_metrics.py`
- `tests/test_orchestrator_tasks.py`
