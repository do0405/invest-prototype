# Preset Execution And Output PRD

## Purpose

Preset selection, market-specific preset counts, and persisted TradingView-style output files.

## Canonical Code Surfaces

- `screeners/tradingview/screener.py`
- `utils/market_runtime.py`
## Output Contract

- Market-specific preset CSV files under `results/{market}/screeners/tradingview`.

## Closest Tests

- `tests/test_tradingview_metrics.py`
- `tests/test_orchestrator_tasks.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium-high fidelity; explicit and tested, but intentionally thin.
- Higher-end target: Registry-backed preset governance and easier market-to-market diffability.

## Related Raw Sources

- None
