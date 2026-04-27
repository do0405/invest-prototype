# Screening Augment Technical Specification

## Canonical Code Surfaces

- `screeners/augment/pipeline.py`
- `screeners/augment/stumpy_sidecar.py`
- `screeners/augment/chronos_rerank.py`
- `screeners/augment/timesfm_rerank.py`
- `screeners/augment/tsfm_metrics.py`
## Output Contract

- `merged_candidate_pool.csv`
- per-source `*_stumpy_summary.csv`
- `stumpy_global_pairs.csv`
- `chronos2_rerank.csv` with additive `fm_model_*`, `up_close_prob_proxy_*`, `down_close_prob_proxy_*`, `support_breach_risk_proxy_*`, `follow_through_quality_*`, and `fragility_score_*`
- `timesfm2p5_rerank.csv` with the same schema category as `chronos2_rerank.csv`
- `augment_run_summary.json` with explicit module-level soft-skip/runtime status

## Fidelity Status

- Medium fidelity; explicit output contract, but intentionally optional and experimental.
- Stronger provenance and clearer separation between experimental reranking and canonical runtime truth.

## Closest Tests

- `tests/test_screening_augment.py`
- `tests/test_signals_package.py`
