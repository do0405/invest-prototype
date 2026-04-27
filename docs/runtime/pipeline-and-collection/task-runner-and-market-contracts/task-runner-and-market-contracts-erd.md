# Task Runner And Market Contracts ERD

## Entities

- task-runner-and-market-contracts
- output artifacts described below

## Producers

- `main.py`
- `orchestrator/tasks.py`
- `utils/market_runtime.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| task-runner-and-market-contracts outputs | `main.py, orchestrator/tasks.py, utils/market_runtime.py` | Task summaries, parser errors, `results/{market}` / `data/{market}` path contract. | operators/tests |

## Closest Tests

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
