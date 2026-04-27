# Earnings Enrichment PRD

## Purpose

Cached earnings-event enrichment and gating semantics inside Qullamaggie screening.

## Canonical Code Surfaces

- `screeners/qullamaggie/earnings_data_collector.py`
- `screeners/qullamaggie/screener.py`
## Output Contract

- Provider-backed earnings payloads and status markers used during screening.

## Closest Tests

- `tests/test_qullamaggie_earnings_cache.py`
- `tests/test_qullamaggie_screener.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium fidelity; useful and well-tested, but not a richer event-modeling contract.
- Higher-end target: Stronger event provenance and clearer unavailable/stale/confirmed state separation.

## Related Raw Sources

- `docs/archive/raw-sources/PRD/Q style/qullamaggie_quant_screener_prd_v2.md`
