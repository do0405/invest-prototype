# Preset Execution And Output ERD

## Entities

- preset-execution-and-output
- output artifacts described below

## Producers

- `screeners/tradingview/screener.py`
- `utils/market_runtime.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| preset-execution-and-output outputs | `screeners/tradingview/screener.py, utils/market_runtime.py` | Market-specific preset CSV files under `results/{market}/screeners/tradingview`. | operators/tests |

## Closest Tests

- `tests/test_tradingview_metrics.py`
- `tests/test_orchestrator_tasks.py`
