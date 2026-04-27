# Earnings Enrichment Design

## Source Of Truth

This design doc records the stable boundary for `earnings-enrichment` under `qullamaggie`.

## Runtime Surface

- `screeners/qullamaggie/earnings_data_collector.py`
- `screeners/qullamaggie/screener.py`
## Output Contract

- Provider-backed earnings payloads and status markers used during screening.

## Current Fidelity And Higher-End Target

- Medium fidelity; useful and well-tested, but not a richer event-modeling contract.
- Stronger event provenance and clearer unavailable/stale/confirmed state separation.

## Closest Tests

- `tests/test_qullamaggie_earnings_cache.py`
- `tests/test_qullamaggie_screener.py`
