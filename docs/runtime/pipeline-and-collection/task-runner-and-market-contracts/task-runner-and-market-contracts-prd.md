# Task Runner And Market Contracts PRD

## Purpose

CLI task selection, scheduler wiring, fail-fast market contract, and output-root resolution.

## Canonical Code Surfaces

- `main.py`
- `orchestrator/tasks.py`
- `utils/market_runtime.py`
## Output Contract

- Task summaries, parser errors, `results/{market}` / `data/{market}` path contract.

## Closest Tests

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
## Current Fidelity And High-End Target

- Current fidelity: High fidelity; current contract is pinned by tests.
- Higher-end target: Declarative task registry and typed schedule profiles.

## Related Raw Sources

- None
