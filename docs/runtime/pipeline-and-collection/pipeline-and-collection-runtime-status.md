# Pipeline And Collection Runtime Status

## Live Default

- US data collection is default; KR is explicit.
- Invalid markets fail fast.
- `split_adjusted` is the default local price policy.
- Default screening and signals runs keep strict `market-intel-core` compatibility semantics.
- `--standalone` is an explicit temporary mode for `all`, `screening`, `signals`, `leader`, `qullamaggie`, and `weinstein`; it uses local benchmark-only market truth and does not auto-fallback.
## Module Status

| Module | Implementation status | Current fidelity | Higher-end target |
| --- | --- | --- | --- |
| `task-runner-and-market-contracts` | `implemented` | High fidelity; current contract is pinned by tests. | Declarative task registry and typed schedule profiles. |
| `ohlcv-and-metadata-collection` | `implemented`, `heuristic` | Medium-high fidelity; KR now uses `FinanceDataReader` primary intake plus `FinanceDatabase` metadata enrichment, but provider semantics still include fallback heuristics. | Stronger lineage and typed provider status. |
| `ohlcv-normalization-and-storage` | `implemented` | High fidelity for the live storage contract. | Explicit schema/version manifest instead of convention-only CSV rules. |

## Known Gaps

- Task registry is still conditional-code driven.
- Lineage/readiness is not yet a first-class manifest.
- Scheduler path is still effectively US-first.
- KR runtime still depends on fallback heuristics when `FinanceDataReader` primary fetch returns unavailable or sparse outputs.
- `--standalone` preserves continuity, but its market truth is heuristic and intentionally below `market-intel-core` parity.
## Closest Tests

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
- `tests/test_market_data_contract.py`
- `tests/test_kr_ohlcv_collector.py`
- `tests/test_stock_metadata_collector.py`
