# Source Registry And Compat Inputs Technical Specification

## Canonical Code Surfaces

- `screeners/signals/source_registry.py`
- `screeners/source_contracts.py`
- `screeners/augment/pipeline.py`
## Output Contract

- Merged source registry, canonical source taxonomy, buy-eligible source-spec contract.

## Fidelity Status

- Medium-high fidelity; merge rules are explicit, style bonuses remain heuristic.
- Clearer contract versioning and less downstream duplication.

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
