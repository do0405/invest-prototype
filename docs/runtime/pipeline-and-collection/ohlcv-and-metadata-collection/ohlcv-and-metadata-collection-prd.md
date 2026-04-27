# OHLCV And Metadata Collection PRD

## Purpose

Repo-owned US/KR OHLCV collection plus stock-metadata refresh and cache handling.

## Canonical Code Surfaces

- `data_collector.py`
- `data_collectors/kr_ohlcv_collector.py`
- `data_collectors/stock_metadata_collector.py`
- `data_collectors/symbol_universe.py`
## Output Contract

- `data/us/*.csv`, `data/kr/*.csv`, `data/stock_metadata.csv`, `data/stock_metadata_kr.csv`.

## KR Intake Ownership

- `FinanceDataReader` is the primary KR intake source for listing, OHLCV, and benchmark/index fetch.
- `FinanceDatabase` is the primary KR metadata enrichment source for additive gap fill.
- yfinance remains a fallback path for KR provider-symbol fetches where the FDR primary path is unavailable.
- `pykrx` is not part of the live KR runtime dependency path.

## Closest Tests

- `tests/test_kr_ohlcv_collector.py`
- `tests/test_stock_metadata_collector.py`
- `tests/test_data_collector_symbol_universe.py`
- `tests/test_data_collector_rate_limit.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium-high fidelity; provider semantics still include heuristics.
- Higher-end target: Stronger lineage and typed provider status.

## Related Raw Sources

- None
