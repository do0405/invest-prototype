# Screening Augment ERD

## Entities

- screening-augment
- output artifacts described below

## Producers

- `screeners/augment/pipeline.py`
- `screeners/augment/stumpy_sidecar.py`
- `screeners/augment/chronos_rerank.py`
- `screeners/augment/timesfm_rerank.py`
- `screeners/augment/lag_diagnostics.py`
- `screeners/augment/run_summary.py`
## Consumers

- operators
- downstream runtime consumers
- regression tests

## Artifact Lineage Matrix

| Artifact | Producer | Main contents | Main consumers |
| --- | --- | --- | --- |
| screening-augment outputs | `screeners/augment/pipeline.py, screeners/augment/stumpy_sidecar.py, screeners/augment/chronos_rerank.py, screeners/augment/timesfm_rerank.py, screeners/augment/lag_diagnostics.py, screeners/augment/run_summary.py` | `merged_candidate_pool.csv`, per-source `*_stumpy_summary.csv`, `stumpy_global_pairs.csv`, `chronos2_rerank.csv`, `timesfm2p5_rerank.csv`, and `augment_run_summary.json`. | operators/tests |

## Runtime Posture

- `Chronos-2` and `TimesFM 2.5` remain optional augment modules.
- Missing packages or model assets use soft-skip status rather than pipeline failure.
- On constrained local machines, the preferred readiness check is imports-only validation before any live model download or inference attempt.

## Closest Tests

- `tests/test_screening_augment.py`
- `tests/test_signals_package.py`
