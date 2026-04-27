# Task Runner And Market Contracts Technical Specification

## Canonical Code Surfaces

- `main.py`
- `orchestrator/tasks.py`
- `utils/market_runtime.py`
## Output Contract

- Task summaries, parser errors, `results/{market}` / `data/{market}` path contract.

## Fidelity Status

- High fidelity; current contract is pinned by tests.
- Declarative task registry and typed schedule profiles.

## Closest Tests

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
