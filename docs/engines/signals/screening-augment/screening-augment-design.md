# Screening Augment Design

## Source Of Truth

This design doc records the stable boundary for `screening-augment` under `signals`.

## Runtime Surface

- `screeners/augment/pipeline.py`
- `screeners/augment/stumpy_sidecar.py`
- `screeners/augment/chronos_rerank.py`
- `screeners/augment/timesfm_rerank.py`
- `screeners/augment/tsfm_metrics.py`
## Output Contract

- `merged_candidate_pool.csv`
- per-source `*_stumpy_summary.csv`
- `stumpy_global_pairs.csv`
- `chronos2_rerank.csv` with per-model risk-geometry and directional proxy fields
- `timesfm2p5_rerank.csv` with the same per-model risk-geometry and directional proxy fields
- `augment_run_summary.json` with module-level status for STUMPY, lag diagnostics, Chronos, and TimesFM

## Current Fidelity And Higher-End Target

- Medium fidelity; explicit output contract, but intentionally optional and experimental.
- Stronger provenance and clearer separation between experimental reranking and canonical runtime truth.

## Closest Tests

- `tests/test_screening_augment.py`
- `tests/test_signals_package.py`
