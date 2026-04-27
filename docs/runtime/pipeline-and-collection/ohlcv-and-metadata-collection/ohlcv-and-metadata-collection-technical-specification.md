# OHLCV And Metadata Collection Technical Specification

## Canonical Code Surfaces

- `data_collector.py`
- `data_collectors/kr_ohlcv_collector.py`
- `data_collectors/stock_metadata_collector.py`
- `data_collectors/symbol_universe.py`
## Output Contract

- `data/us/*.csv`, `data/kr/*.csv`, `data/stock_metadata.csv`, `data/stock_metadata_kr.csv`.

## Fidelity Status

- Medium-high fidelity; provider semantics still include heuristics.
- Stronger lineage and typed provider status.

## Closest Tests

- `tests/test_kr_ohlcv_collector.py`
- `tests/test_stock_metadata_collector.py`
- `tests/test_data_collector_symbol_universe.py`
- `tests/test_data_collector_rate_limit.py`
