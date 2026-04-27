# OHLCV Normalization And Storage ERD

## Entities

- ohlcv-normalization-and-storage
- output artifacts described below

## Producers

- `utils/market_data_contract.py`
- `utils/market_runtime.py`
- `utils/symbol_normalization.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| ohlcv-normalization-and-storage outputs | `utils/market_data_contract.py, utils/market_runtime.py, utils/symbol_normalization.py` | Canonical OHLCV frame with raw/adjusted lineage fields and market-aware benchmark loading. | operators/tests |

## Closest Tests

- `tests/test_market_data_contract.py`
- `tests/test_market_runtime.py`
- `tests/test_symbol_normalization.py`
