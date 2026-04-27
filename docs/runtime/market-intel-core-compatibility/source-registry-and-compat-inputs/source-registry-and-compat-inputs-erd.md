# Source Registry And Compat Inputs ERD

## Entities

- source-registry-and-compat-inputs
- output artifacts described below

## Producers

- `screeners/signals/source_registry.py`
- `screeners/source_contracts.py`
- `screeners/augment/pipeline.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| source-registry-and-compat-inputs outputs | `screeners/signals/source_registry.py, screeners/source_contracts.py, screeners/augment/pipeline.py` | Merged source registry, canonical source taxonomy, buy-eligible source-spec contract. | operators/tests |

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
