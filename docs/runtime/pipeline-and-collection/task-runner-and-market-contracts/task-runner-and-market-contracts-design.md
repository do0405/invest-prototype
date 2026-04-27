# Task Runner And Market Contracts Design

## Source Of Truth

This design doc records the stable boundary for `task-runner-and-market-contracts` under `pipeline-and-collection`.

## Runtime Surface

- `main.py`
- `orchestrator/tasks.py`
- `utils/market_runtime.py`
## Output Contract

- Task summaries, parser errors, `results/{market}` / `data/{market}` path contract.

## Current Fidelity And Higher-End Target

- High fidelity; current contract is pinned by tests.
- Declarative task registry and typed schedule profiles.

## Closest Tests

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
