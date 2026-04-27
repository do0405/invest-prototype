# Tradingview Runtime Status

## Live Default

- US and KR preset sets are hard-coded in `screeners/tradingview/screener.py`.
## Module Status

| Module | Implementation status | Current fidelity | Higher-end target |
| --- | --- | --- | --- |
| `preset-metric-contracts` | `implemented` | High fidelity because key computations are pinned by tests. | Reusable metric schema and preset-provenance layer. |
| `preset-execution-and-output` | `implemented` | Medium-high fidelity; explicit and tested, but intentionally thin. | Registry-backed preset governance and easier market-to-market diffability. |

## Known Gaps

- Preset definitions are code-driven, not registry-driven.
- Metric surfaces are useful but lighter-weight than a reusable contract layer.
## Closest Tests

- `tests/test_tradingview_metrics.py`
- `tests/test_orchestrator_tasks.py`
