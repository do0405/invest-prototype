# Task Runner And Market Contracts Runtime Spec

## Runtime Rules

- The live behavior is owned by the listed code paths and nearest tests.
- Higher-end targets in this doc are not live unless code and tests move.
- `--standalone` is opt-in only; the runner never auto-switches into local fallback semantics.
- Default mode keeps strict compat reads and failures for missing or stale upstream artifacts.
- Market runtime artifacts remain additive: `runtime_state.json` and `runtime_profile.json` persist step-level stop points, and US OHLCV collection now persists same-day `collector_run_state.json` under `results/{market}/runtime/` to skip completed symbols and retry only `partial` / `rate_limited` / `soft_unavailable` work.

## Current Fidelity Status

- High fidelity; current contract is pinned by tests.
- Standalone activation is implemented, but the market-truth layer it selects is heuristic and high-end deferred.
- Higher-end deferred item: Declarative task registry and typed schedule profiles.

## Closest Tests

- `tests/test_main_market_resolution.py`
- `tests/test_orchestrator_tasks.py`
- `tests/test_market_runtime.py`
