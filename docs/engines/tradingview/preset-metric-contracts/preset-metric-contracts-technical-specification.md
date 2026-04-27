# Preset Metric Contracts Technical Specification

## Canonical Code Surfaces

- `screeners/tradingview/screener.py`
- `utils/indicator_helpers.py`
- `utils/market_data_contract.py`
## Output Contract

- EMA, ATR/ADR, traded-value, RVOL, breakout strength, and distance-from-low metric rows.

## Fidelity Status

- High fidelity because key computations are pinned by tests.
- Reusable metric schema and preset-provenance layer.

## Closest Tests

- `tests/test_tradingview_metrics.py`
