# Earnings Enrichment ERD

## Entities

- earnings-enrichment
- output artifacts described below

## Producers

- `screeners/qullamaggie/earnings_data_collector.py`
- `screeners/qullamaggie/screener.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| earnings-enrichment outputs | `screeners/qullamaggie/earnings_data_collector.py, screeners/qullamaggie/screener.py` | Provider-backed earnings payloads and status markers used during screening. | operators/tests |

## Closest Tests

- `tests/test_qullamaggie_earnings_cache.py`
- `tests/test_qullamaggie_screener.py`
