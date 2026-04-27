# Preset Metric Contracts Design

## Source Of Truth

This design doc records the stable boundary for `preset-metric-contracts` under `tradingview`.

## Runtime Surface

- `screeners/tradingview/screener.py`
- `utils/indicator_helpers.py`
- `utils/market_data_contract.py`
## Output Contract

- EMA, ATR/ADR, traded-value, RVOL, breakout strength, and distance-from-low metric rows.

## Current Fidelity And Higher-End Target

- High fidelity because key computations are pinned by tests.
- Reusable metric schema and preset-provenance layer.

## Closest Tests

- `tests/test_tradingview_metrics.py`
