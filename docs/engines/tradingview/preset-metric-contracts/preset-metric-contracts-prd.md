# Preset Metric Contracts PRD

## Purpose

Metric derivation for TradingView-style presets.

## Canonical Code Surfaces

- `screeners/tradingview/screener.py`
- `utils/indicator_helpers.py`
- `utils/market_data_contract.py`
## Output Contract

- EMA, ATR/ADR, traded-value, RVOL, breakout strength, and distance-from-low metric rows.

## Closest Tests

- `tests/test_tradingview_metrics.py`
## Current Fidelity And High-End Target

- Current fidelity: High fidelity because key computations are pinned by tests.
- Higher-end target: Reusable metric schema and preset-provenance layer.

## Related Raw Sources

- None
