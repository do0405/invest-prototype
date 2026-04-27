# Source Registry And Compat Inputs Design

## Source Of Truth

This design doc records the stable boundary for `source-registry-and-compat-inputs` under `market-intel-core-compatibility`.

## Runtime Surface

- `screeners/signals/source_registry.py`
- `screeners/source_contracts.py`
- `screeners/augment/pipeline.py`
## Output Contract

- Merged source registry, canonical source taxonomy, buy-eligible source-spec contract.

## Current Fidelity And Higher-End Target

- Medium-high fidelity; merge rules are explicit, style bonuses remain heuristic.
- Clearer contract versioning and less downstream duplication.

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
