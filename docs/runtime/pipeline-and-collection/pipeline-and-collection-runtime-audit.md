# Pipeline And Collection Runtime Audit

## Scope

Code-grounded audit of the current `pipeline-and-collection` runtime in `invest-prototype-main`.

## Code-Grounded Runtime Path

- `main.py`
- `orchestrator/tasks.py`
- `data_collector.py`
- `data_collectors/kr_ohlcv_collector.py`
- `data_collectors/stock_metadata_collector.py`
- `utils/market_runtime.py`
- `utils/market_data_contract.py`
## Live Baseline

- US data collection is default; KR is explicit.
- Invalid markets fail fast.
- `split_adjusted` is the default local price policy.
## Artifact Surface

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| task-runner-and-market-contracts | `main.py, orchestrator/tasks.py, utils/market_runtime.py` | Task summaries, parser errors, `results/{market}` / `data/{market}` path contract. | operators/tests |
| ohlcv-and-metadata-collection | `data_collector.py, data_collectors/kr_ohlcv_collector.py, data_collectors/stock_metadata_collector.py, data_collectors/symbol_universe.py` | `data/us/*.csv`, `data/kr/*.csv`, `data/stock_metadata.csv`, `data/stock_metadata_kr.csv`. | operators/tests |
| ohlcv-normalization-and-storage | `utils/market_data_contract.py, utils/market_runtime.py, utils/symbol_normalization.py` | Canonical OHLCV frame with raw/adjusted lineage fields and market-aware benchmark loading. | operators/tests |
## Test Evidence

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
- `tests/test_market_data_contract.py`
- `tests/test_kr_ohlcv_collector.py`
- `tests/test_stock_metadata_collector.py`
## Known Gaps

- Task registry is still conditional-code driven.
- Lineage/readiness is not yet a first-class manifest.
- Scheduler path is still effectively US-first.
## Higher-End Reference Target

- Higher-end target: typed task registry, explicit lineage manifests, cleaner provider/readiness model.
