# Market Intel Core Compatibility Runtime Audit

## Scope

Code-grounded audit of the current `market-intel-core-compatibility` runtime in `invest-prototype-main`.

## Code-Grounded Runtime Path

- `screeners/leader_core_bridge.py`
- `screeners/signals/source_registry.py`
- `screeners/source_contracts.py`
- `utils/market_runtime.py`
## Live Baseline

- Compatibility inputs must be same-day and schema-matched.
- `leader_core_v1` and `market_context_v1` are the active consume-side contracts.
## Artifact Surface

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| leader-core-consume-seam | `screeners/leader_core_bridge.py, utils/market_runtime.py` | Validated leader-core snapshot and market-truth snapshot. | operators/tests |
| source-registry-and-compat-inputs | `screeners/signals/source_registry.py, screeners/source_contracts.py, screeners/augment/pipeline.py` | Merged source registry, canonical source taxonomy, buy-eligible source-spec contract. | operators/tests |
## Test Evidence

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
## Known Gaps

- No manifest-style version handshake yet.
- Downstream local heuristics still coexist with canonical upstream truth.
## Higher-End Reference Target

- Higher-end target: typed manifests, compatibility warnings, provenance for partial overlays.
