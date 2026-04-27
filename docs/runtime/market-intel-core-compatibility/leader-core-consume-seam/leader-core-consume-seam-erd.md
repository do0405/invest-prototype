# Leader Core Consume Seam ERD

## Entities

- leader-core-consume-seam
- output artifacts described below

## Producers

- `screeners/leader_core_bridge.py`
- `utils/market_runtime.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| leader-core-consume-seam outputs | `screeners/leader_core_bridge.py, utils/market_runtime.py` | Validated leader-core snapshot and market-truth snapshot. | operators/tests |

## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_leader_lagging_screener.py`
- `tests/test_weinstein_stage2_screener.py`
