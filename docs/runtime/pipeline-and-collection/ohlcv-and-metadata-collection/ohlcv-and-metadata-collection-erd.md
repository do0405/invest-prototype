# OHLCV And Metadata Collection ERD

## Entities

- ohlcv-and-metadata-collection
- output artifacts described below

## Producers

- `data_collector.py`
- `data_collectors/kr_ohlcv_collector.py`
- `data_collectors/stock_metadata_collector.py`
- `data_collectors/symbol_universe.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| ohlcv-and-metadata-collection outputs | `data_collector.py, data_collectors/kr_ohlcv_collector.py, data_collectors/stock_metadata_collector.py, data_collectors/symbol_universe.py` | `data/us/*.csv`, `data/kr/*.csv`, `data/stock_metadata.csv`, `data/stock_metadata_kr.csv`. | operators/tests |

## Closest Tests

- `tests/test_kr_ohlcv_collector.py`
- `tests/test_stock_metadata_collector.py`
- `tests/test_data_collector_symbol_universe.py`
- `tests/test_data_collector_rate_limit.py`
