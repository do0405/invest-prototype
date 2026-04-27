# OHLCV Normalization And Storage Design

## Source Of Truth

This design doc records the stable boundary for `ohlcv-normalization-and-storage` under `pipeline-and-collection`.

## Runtime Surface

- `utils/market_data_contract.py`
- `utils/market_runtime.py`
- `utils/symbol_normalization.py`
## Output Contract

- Canonical OHLCV frame with raw/adjusted lineage fields and market-aware benchmark loading.

## Current Fidelity And Higher-End Target

- High fidelity for the live storage contract.
- Explicit schema/version manifest instead of convention-only CSV rules.

## Closest Tests

- `tests/test_market_data_contract.py`
- `tests/test_market_runtime.py`
- `tests/test_symbol_normalization.py`
