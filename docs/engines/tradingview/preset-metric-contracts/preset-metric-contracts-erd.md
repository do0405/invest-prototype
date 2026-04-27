# Preset Metric Contracts ERD

## Entities

- preset-metric-contracts
- output artifacts described below

## Producers

- `screeners/tradingview/screener.py`
- `utils/indicator_helpers.py`
- `utils/market_data_contract.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| preset-metric-contracts outputs | `screeners/tradingview/screener.py, utils/indicator_helpers.py, utils/market_data_contract.py` | EMA, ATR/ADR, traded-value, RVOL, breakout strength, and distance-from-low metric rows. | operators/tests |

## Closest Tests

- `tests/test_tradingview_metrics.py`
