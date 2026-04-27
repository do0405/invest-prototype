# Leader Lagging Screening Upgrade Runtime Spec

- status: implemented V2 runtime contract
- as_of: 2026-04-22
- scope: implemented deterministic leader/follower runtime and additive diagnostics

## Runtime Status

This upgrade remains deterministic and local-data-first. The narrowed source policy removes direct dependency on existing 3대 screener families and general broker portals. Runtime must compute leader/follower evidence from local OHLCV, benchmark, metadata, optional `market-intel-core` overlay, and reviewed local source-context records.

## Source Runtime Policy

| Source type | Runtime policy | Allowed effect | Forbidden effect |
| --- | --- | --- | --- |
| RS / RS line / RS Rating | local deterministic calculation | `rs_quality_score`, `leadership_freshness_score`, `rs_inflection_score` | Treat proxy rank as true universe percentile. |
| Academic momentum / 52-week / lead-lag / PEAD | evidence-backed formula design | score families and pair logic | Per-symbol automatic BUY rule. |
| Hidden RS / RS Rotation | local taxonomy adapted to OHLCV + benchmark | weak-window resilience and leader state | Market regime claim. |
| Darvas / pivotal breakout | structure and confirmation layer | `structure_readiness_score`, `breakout_confirmation_score` | Standalone buy trigger. |
| Direct leader-attribute report | local/manual source context | `attribute_evidence`, `leader_lifecycle_evidence`, `rotation_evidence` | Signal override, target price, or Top Pick import. |
| Existing 3대 screeners | `excluded-existing-screener` | optional separate subsystem context only | Direct algorithm source in this upgrade. |

## Excluded Existing Screener Runtime Boundary

Mark Minervini, Weinstein/Weinstain, Mansfield, and Qullamaggie are already covered by separate local subsystems. Runtime for this upgrade must not recreate their named rules or import their output as direct score truth.

Allowed:

- consume separate subsystem outputs only as optional external context if a future contract explicitly defines it;
- keep generic RS line, benchmark-relative strength, structure preservation, and breakout terms when not tied to excluded named systems.

Forbidden:

- `mansfield_rs_state`, `minervini_template`, `qullamaggie_ep`, or named Stage Analysis states as output fields in this upgrade;
- treating excluded subsystem scores as canonical leader/follower truth.

## Broker Runtime Policy

Allowed direct report evidence:

- Macrend Invest `주도주의 생로병사` style material that describes 주도주 attributes, lifecycle, rotation, and common properties.
- Fields: `leader_lifecycle_evidence`, `rotation_evidence`, `attribute_evidence`.

Excluded broker discovery terms: FnGuide, Hana, NH Research, Kiwoom, Top Pick, 산업분류, industry classification. Runtime should not ingest generic broker portals, sector classifications, top picks, target prices, or single-name “주도주 가능성” report headlines for this upgrade.

## Determinism Rules

- No live web, GitHub, TradingView, or broker-report fetch during screening.
- Same local input data and parameters must produce the same outputs.
- Cross-symbol percentile ranks and pair correlations must use explicit universe, market, date, and tie-breaking policies.
- Public BUY/SELL filenames and today-only behavior remain unchanged.

## Non-Parallelization Contract

Do not naively parallelize these without deterministic reduce:

- `rs_rank_true` cross-sectional percentile.
- group/industry rank context.
- leader/follower pair generation.
- lagged correlation over 1/2/3/5 days.
- final ranking and tie-breakers.

Symbol-local rolling features may be parallelized if final ordering is deterministic.

## Runtime Flow

1. Load OHLCV, benchmark, symbol metadata, group metadata, optional core overlay.
2. Compute RS profile: `rs_line`, `rs_slope`, `rs_rank_true`, `rs_rank_proxy`, `rs_new_high_before_price`.
3. Compute momentum/52-week profile: weighted 3/6/9/12 month score, `near_high_leadership_score`, extension flags.
4. Compute Hidden RS weak-window resilience.
5. Compute Darvas/pivotal structure state and volume confirmation.
6. Compute `leader_rs_state`, leader/follower confidence, and reason-code diagnostics from RS, momentum, structure, and lead-lag evidence.
7. Score leaders.
8. Build follower pool and compute lead-lag pair features.
9. Attach source context with conservative policies.
10. Emit additive fields while preserving public output contracts.

## Runtime Field Defaults

| Field | Default | Reason |
| --- | --- | --- |
| `rs_rank_true` | null | Do not fake universe percentile. |
| `rs_rank_proxy` | null | Proxy only when explicitly computed. |
| `hidden_rs_score` | 0 | Missing weak window should not inflate leader quality. |
| `leader_rs_state` | `unknown` | Avoid forced state. |
| `peer_lead_score` | 0 | No pair evidence means no lead-lag lift. |
| `source_policy` | `reference-only` | Conservative default. |

Prior-cycle and lifecycle report ideas are `reference-only/deferred`; they do not create runtime fields in V2.

## Test Plan

- RS new high before price: RS line makes 65/252d high before close makes same-window high.
- True rank vs proxy: `rs_rank_true` and `rs_rank_proxy` differ and are labeled separately.
- Hidden RS: candidate holds up during benchmark down days.
- RS state: rising/stable/fading/weakening states derive from RS rank level, delta, slope, and structure.
- Darvas structure: valid box, confirmed breakout, and invalid breakdown are distinct.
- Lead-lag follower: same peer group leader return leads follower return at 1/2/3/5 day lag.
- Direct attribute report remains reference-only/deferred and does not populate runtime score fields.
- Excluded families: Mark Minervini, Weinstein/Weinstain, Mansfield, Qullamaggie cannot populate direct algorithm fields.

## Runtime Non-Goals

- No live web/report ingestion.
- No broker portal ingestion.
- No Top Pick import.
- No industry classification ingestion from reports.
- No recreation of existing 3대 screener algorithms.
