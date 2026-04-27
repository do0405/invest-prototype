# Pipeline And Collection Subsystem System Design

## Source Of Truth

This subsystem doc family is the stable design boundary for `pipeline-and-collection`.

## Runtime Flow

- US data collection is default; KR is explicit.
- Invalid markets fail fast.
- `split_adjusted` is the default local price policy.
## Module Boundaries

- `task-runner-and-market-contracts`: CLI task selection, scheduler wiring, fail-fast market contract, and output-root resolution.
- `ohlcv-and-metadata-collection`: Repo-owned US/KR OHLCV collection plus stock-metadata refresh and cache handling.
- `ohlcv-normalization-and-storage`: Canonical OHLCV normalization, price-adjustment policy, local cache precedence, and storage-path safety.
## Boundary Rule

- stable docs describe current code first
- runtime audits capture live-vs-design-vs-high-end gaps
- raw sources do not override live contracts

## Closest Tests

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
- `tests/test_market_data_contract.py`
- `tests/test_kr_ohlcv_collector.py`
- `tests/test_stock_metadata_collector.py`
