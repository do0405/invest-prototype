# OHLCV Normalization And Storage PRD

## Purpose

Canonical OHLCV normalization, price-adjustment policy, local cache precedence, and storage-path safety.

## Canonical Code Surfaces

- `utils/market_data_contract.py`
- `utils/market_runtime.py`
- `utils/symbol_normalization.py`
## Output Contract

- Canonical OHLCV frame with raw/adjusted lineage fields and market-aware benchmark loading.

## Closest Tests

- `tests/test_market_data_contract.py`
- `tests/test_market_runtime.py`
- `tests/test_symbol_normalization.py`
## Current Fidelity And High-End Target

- Current fidelity: High fidelity for the live storage contract.
- Higher-end target: Explicit schema/version manifest instead of convention-only CSV rules.

## Related Raw Sources

- None
