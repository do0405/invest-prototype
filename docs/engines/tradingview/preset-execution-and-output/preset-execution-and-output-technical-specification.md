# Preset Execution And Output Technical Specification

## Canonical Code Surfaces

- `screeners/tradingview/screener.py`
- `utils/market_runtime.py`
## Output Contract

- Market-specific preset CSV files under `results/{market}/screeners/tradingview`.

## Fidelity Status

- Medium-high fidelity; explicit and tested, but intentionally thin.
- Registry-backed preset governance and easier market-to-market diffability.

## Closest Tests

- `tests/test_tradingview_metrics.py`
- `tests/test_orchestrator_tasks.py`
