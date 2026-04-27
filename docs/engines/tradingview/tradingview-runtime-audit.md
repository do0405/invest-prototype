# Tradingview Runtime Audit

## Scope

Code-grounded audit of the current `tradingview` runtime in `invest-prototype-main`.

## Code-Grounded Runtime Path

- `screeners/tradingview/screener.py`
- `utils/indicator_helpers.py`
- `utils/market_runtime.py`
## Live Baseline

- US and KR preset sets are hard-coded in `screeners/tradingview/screener.py`.
## Artifact Surface

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| preset-metric-contracts | `screeners/tradingview/screener.py, utils/indicator_helpers.py, utils/market_data_contract.py` | EMA, ATR/ADR, traded-value, RVOL, breakout strength, and distance-from-low metric rows. | operators/tests |
| preset-execution-and-output | `screeners/tradingview/screener.py, utils/market_runtime.py` | Market-specific preset CSV files under `results/{market}/screeners/tradingview`. | operators/tests |
## Test Evidence

- `tests/test_tradingview_metrics.py`
- `tests/test_orchestrator_tasks.py`
## Known Gaps

- Preset definitions are code-driven, not registry-driven.
- Metric surfaces are useful but lighter-weight than a reusable contract layer.
## Higher-End Reference Target

- Higher-end target: registry-backed preset catalog with stronger metric provenance.
