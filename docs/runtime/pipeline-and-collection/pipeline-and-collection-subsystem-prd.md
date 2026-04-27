# Pipeline And Collection Subsystem PRD

## Purpose

CLI task dispatch, market-aware collection orchestration, OHLCV and metadata collection, and local normalized storage owned by this repo.

## Canonical Code Surfaces

- `main.py`
- `orchestrator/tasks.py`
- `data_collector.py`
- `data_collectors/kr_ohlcv_collector.py`
- `data_collectors/stock_metadata_collector.py`
- `utils/market_runtime.py`
- `utils/market_data_contract.py`
## Module Set

- `task-runner-and-market-contracts`: CLI task selection, scheduler wiring, fail-fast market contract, and output-root resolution.
- `ohlcv-and-metadata-collection`: Repo-owned US/KR OHLCV collection plus stock-metadata refresh and cache handling.
- `ohlcv-normalization-and-storage`: Canonical OHLCV normalization, price-adjustment policy, local cache precedence, and storage-path safety.
## Closest Tests

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
- `tests/test_market_data_contract.py`
- `tests/test_kr_ohlcv_collector.py`
- `tests/test_stock_metadata_collector.py`
## Live Default

- US data collection is default; KR is explicit.
- Invalid markets fail fast.
- `split_adjusted` is the default local price policy.
## Higher-End Posture

- Higher-end target: typed task registry, explicit lineage manifests, cleaner provider/readiness model.
## Related Raw Sources

- `README.md`
- `docs/archive/raw-sources/PRD/`
- `docs/archive/raw-sources/Reference/`
