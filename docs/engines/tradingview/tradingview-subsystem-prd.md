# Tradingview Subsystem PRD

## Purpose

Local TradingView-style preset metric engine and persisted preset outputs.

## Canonical Code Surfaces

- `screeners/tradingview/screener.py`
- `utils/indicator_helpers.py`
- `utils/market_runtime.py`
## Module Set

- `preset-metric-contracts`: Metric derivation for TradingView-style presets.
- `preset-execution-and-output`: Preset selection, market-specific preset counts, and persisted TradingView-style output files.
## Closest Tests

- `tests/test_tradingview_metrics.py`
- `tests/test_orchestrator_tasks.py`
## Live Default

- US and KR preset sets are hard-coded in `screeners/tradingview/screener.py`.
## Higher-End Posture

- Higher-end target: registry-backed preset catalog with stronger metric provenance.
## Related Raw Sources

- None
