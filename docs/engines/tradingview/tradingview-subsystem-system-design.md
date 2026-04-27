# Tradingview Subsystem System Design

## Source Of Truth

This subsystem doc family is the stable design boundary for `tradingview`.

## Runtime Flow

- US and KR preset sets are hard-coded in `screeners/tradingview/screener.py`.
## Module Boundaries

- `preset-metric-contracts`: Metric derivation for TradingView-style presets.
- `preset-execution-and-output`: Preset selection, market-specific preset counts, and persisted TradingView-style output files.
## Boundary Rule

- stable docs describe current code first
- runtime audits capture live-vs-design-vs-high-end gaps
- raw sources do not override live contracts

## Closest Tests

- `tests/test_tradingview_metrics.py`
- `tests/test_orchestrator_tasks.py`
