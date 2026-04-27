# Screening Augment PRD

## Purpose

`screening-augment` is the optional post-screening diagnostics and rerank layer for `invest-prototype-main`.

Its job is to:

- consume existing screener outputs after first-pass screening
- emit experimental diagnostics and model-side scores before signals
- stay outside canonical screening and signal truth through Phase 3

This module is not a replacement for first-pass screeners and not a replacement for the canonical signal engine.

## Current Live Boundary

Current live code surfaces:

- `screeners/augment/pipeline.py`
- `screeners/augment/stumpy_sidecar.py`
- `screeners/augment/chronos_rerank.py`
- `screeners/augment/timesfm_rerank.py`
- `screeners/augment/lag_diagnostics.py`
- `screeners/augment/run_summary.py`
- `screeners/augment/tsfm_metrics.py`

Current live entry surface:

- `main.py --enable-augment`
- screening runs augment after screening and before signals only when explicitly enabled

Current live output contract:

- `merged_candidate_pool.csv`
- per-source `*_stumpy_summary.csv`
- `stumpy_global_pairs.csv`
- `chronos2_rerank.csv`
- `timesfm2p5_rerank.csv`
- `augment_run_summary.json`

## Product Direction

This PRD records the approved augment roadmap.

Fixed roadmap order:

1. `post-screener augment`
2. `data intake + metadata enrichment`
3. `research sidecar`
4. `agent/reporting UX`

Locked near-term rules:

- `OSS runtime first`
- `no training through Phase 3`
- `pykrx no-go`
- `OpenBB reference-only`
- augment remains optional and off by default
- augment remains diagnostics-only through Phase 3
- Chronos and TimesFM remain optional local experimental modules rather than base runtime requirements
- constrained machines should validate augment model readiness via imports only before any live model download or inference attempt

## Repo Decision Table

### Adopt In Roadmap

- `STUMPY`: Phase 1 core no-label augment engine
- `Chronos-2`: first TSFM rerank candidate because the repo already has a stub
- `TimesFM 2.5`: second TSFM comparator after Chronos
- `FinanceDataReader`: Phase 2 KR data and listing candidate
- `FinanceDatabase`: Phase 2 metadata and universe enrichment candidate
- `Qlib`: Phase 3 research sidecar candidate

### Watchlist / Later

- `Kronos`: finance-specific TSFM watch candidate after the main TSFM path is stable
- `QuantAgent`, `AI-Trader`, `Vibe-Trading`: Phase 4+ only for explanation, research, and operator UX

### Reference-Only / Excluded

- `OpenBB`: reference-only for future provider abstraction, API, and MCP ideas
- `pykrx`: excluded entirely
- `Awesome-finance-skills`: excluded from the execution roadmap
- `gbrain`: excluded from the execution roadmap

## Phase Roadmap

### Phase 1A: STUMPY First

Primary goal:

- promote STUMPY from experimental sidecar output into the primary augment diagnostic layer

Required outputs:

- shape similarity or motif score
- self-discord score
- cluster identity and exemplar symbol
- explicit status codes for missing OHLCV, short history, singleton, and runtime skip

Rules:

- keep augment behind `--enable-augment`
- keep the current screening-after and signals-before insertion point
- do not mutate canonical screener ranking
- do not mutate canonical signal ranking
- add module-scoped augment summary artifacts with statuses such as `OK`, `SKIPPED_MISSING_DEP`, `SKIPPED_MISSING_MODEL`, and `FAILED_RUNTIME`

### Phase 1B: Chronos First TSFM

Primary goal:

- productionize the existing Chronos rerank path as the first TSFM augment module

Required outputs:

- upside proxies
- breach-margin proxies
- forecast dispersion or fragility metrics
- model-local rerank score and model status

Rules:

- Chronos remains diagnostics-only through Phase 3
- Chronos scores must not alter canonical screening or signal ordering
- Chronos must follow the same module-summary and soft-skip contract as STUMPY

### Phase 1C: TimesFM Comparator

Primary goal:

- add `TimesFM 2.5` as a comparator TSFM, not as a Chronos replacement

Required behavior:

- run against the same merged candidate pool as Chronos
- use the same horizon family and output schema category as Chronos
- support side-by-side comparison without changing the canonical runtime

Rules:

- TimesFM is soft-skippable when missing
- TimesFM remains diagnostics-only through Phase 3

### Phase 2: Data Intake And Metadata Enrichment

Primary goal:

- improve upstream intake and metadata quality without replacing the repo with a new platform

Required direction:

- remove any planning dependence on `pykrx`
- expand KR intake with `FinanceDataReader` wherever it improves listing, index, or OHLCV resilience
- apply `FinanceDatabase` where it safely enriches symbol-universe, sector, industry, exchange, and market metadata across KR and US

Rules:

- new integrations must be additive and non-destructive
- existing symbol normalization and disk contracts remain canonical
- `OpenBB` stays reference-only

### Phase 3: Qlib Research Sidecar

Primary goal:

- create a research sidecar, not a runtime replacement

In-scope uses:

- event-study style evaluation of screener outputs
- backtest and validation workflows
- experiment tracking and recorder usage
- PIT-aware research discipline

Rules:

- Qlib consumes existing artifacts as inputs
- Qlib does not take ownership of production collection, screening, or signal generation
- no local training or fine-tuning is introduced in this phase

### Phase 4+: Agent And Reporting Layer

Primary goal:

- add explanation and operator-facing research UX only after Phase 1 through 3 are stable

In-scope candidates:

- `QuantAgent`
- `AI-Trader`
- `Vibe-Trading`

Rules:

- these are not near-term execution candidates
- they stay out of the canonical trading or signal path unless explicitly re-planned

## Public Interface And Runtime Rules

- `--enable-augment` remains the public switch
- no new top-level runtime mode is required for this roadmap
- augment stays optional and off by default
- augment remains diagnostics-only through Phase 3
- augment may emit scores, comparisons, and operator-facing rank suggestions
- augment must not alter canonical screening or canonical signal ordering through Phase 3
- missing augment dependencies or unavailable models use soft skip, not pipeline failure
- every augment module must emit explicit module-level status in a shared run summary
- no training or fine-tuning is allowed through Phase 3
- the default validation posture for constrained local machines is imports-only readiness, not full live inference

## Acceptance Criteria

### Phase 1

- screening and signals continue to behave identically when augment is disabled
- `--enable-augment` still runs augment between screening and signals
- STUMPY outputs are deterministic on fixed fixtures
- Chronos and TimesFM outputs are deterministic and schema-matched on fixed fixtures
- missing dependencies or models yield `SKIPPED_*` states instead of breaking the base pipeline
- augment outputs do not change canonical ranking or final signal order

### Phase 2

- KR collection remains compatible with the current disk contract
- metadata enrichment is additive and non-destructive
- KR and US metadata merges preserve downstream screener assumptions
- no `pykrx` dependency or fallback remains in the roadmap implementation

### Phase 3

- Qlib reads existing artifacts without changing production runtime behavior
- event-study and backtest outputs are reproducible from saved repo artifacts
- experiment records remain separate from the production `results/{market}` contract
- no training or fine-tuning workflow is introduced

## Current Fidelity And High-End Target

- Current fidelity: Medium fidelity; explicit output contract, but intentionally optional and experimental.
- Higher-end target: stronger provenance, explicit module-level augment summaries, comparator TSFM diagnostics, and a clean separation between experimental reranking and canonical runtime truth.

## Related Raw Sources

- `docs/archive/raw-sources/docs/2026-03-31-invest-prototype-no-label-ai-augmentation-shortlist.md`
