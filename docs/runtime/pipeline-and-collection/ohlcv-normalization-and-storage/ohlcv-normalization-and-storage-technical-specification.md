# OHLCV Normalization And Storage Technical Specification

## Canonical Code Surfaces

- `utils/market_data_contract.py`
- `utils/market_runtime.py`
- `utils/symbol_normalization.py`
## Output Contract

- Canonical OHLCV frame with raw/adjusted lineage fields and market-aware benchmark loading.

## Fidelity Status

- High fidelity for the live storage contract.
- Explicit schema/version manifest instead of convention-only CSV rules.

## Closest Tests

- `tests/test_market_data_contract.py`
- `tests/test_market_runtime.py`
- `tests/test_symbol_normalization.py`
