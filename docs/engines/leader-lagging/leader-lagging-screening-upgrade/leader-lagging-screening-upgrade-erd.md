# Leader Lagging Screening Upgrade ERD

- status: implemented V2 conceptual data model
- as_of: 2026-04-22
- scope: implemented output artifact and diagnostic lineage contract

## Entity Model

The upgraded conceptual model separates price/RS truth, independent research evidence, direct leader-attribute report evidence, and excluded existing screener families.

## Input Entities

| Entity | Owner | Key fields | Notes |
| --- | --- | --- | --- |
| `ohlcv_bar` | this repo | `market`, `symbol`, `date`, `open`, `high`, `low`, `close`, `volume` | Primary input for RS, momentum, 52-week high, hidden RS, Darvas/pivotal structure. |
| `benchmark_series` | this repo/config | `market`, `benchmark_symbol`, `date`, `close`, `return_1d` | Required for `rs_line`, `benchmark_relative_strength`, `hidden_rs_score`. |
| `market_metadata` | this repo | `market`, `symbol`, `name`, `exchange`, `liquidity_bucket`, `listing_status` | Prevents US/KR universe leakage. |
| `sector_industry_group` | this repo or core overlay | `symbol`, `sector`, `industry`, `group_id`, `as_of_date` | Used for group context and follower peer links, not broker industry classification. |
| `leader_core_overlay` | `market-intel-core` | `symbol`, `group_rank`, `leader_health`, `regime_context`, `confidence` | Optional canonical group/regime truth. |
| `external_reference_register` | docs/source registry future | `source_id`, `source_family`, `source_url`, `source_policy`, `logic_extracted`, `algorithm_target`, `as_of_date` | Holds academic, GitHub, Pine, IBD/O'Neil, Darvas references. |
| `leader_attribute_report_registry` | local/manual docs evidence | `report_id`, `report_title`, `source_url`, `as_of_date`, `attribute_evidence`, `leader_lifecycle_evidence`, `rotation_evidence`, `source_policy` | Replaces broad `broker_report_catalog`. |
| `excluded_existing_screener_registry` | docs/source registry future | `family`, `local_subsystem`, `exclusion_reason`, `allowed_generic_carryover` | Records Mark Minervini, Weinstein/Weinstain, Mansfield, Qullamaggie as excluded direct sources. |

## Derived Entities

| Entity | Derived from | Key fields | Purpose |
| --- | --- | --- | --- |
| `rs_profile` | `ohlcv_bar`, `benchmark_series`, universe returns | `rs_line`, `rs_line_slope`, `rs_rank_true`, `rs_rank_proxy`, `rs_line_new_high_65d`, `rs_line_new_high_252d`, `rs_new_high_before_price` | Independent relative-strength layer. |
| `momentum_profile` | `ohlcv_bar` | `perf_3m`, `perf_6m`, `perf_9m`, `perf_12m`, `weighted_rs_score`, `momentum_persistence_score`, `near_high_leadership_score` | Academic momentum and 52-week high layer. |
| `hidden_rs_profile` | `ohlcv_bar`, `benchmark_series` | `weak_market_resilience`, `drawdown_resilience`, `hidden_rs_score` | Early leader evidence during weak benchmark windows. |
| `structure_profile` | `ohlcv_bar` | `box_high`, `box_low`, `box_valid`, `breakout_confirmed`, `volume_ratio`, `structure_readiness_score`, `breakout_confirmation_score` | Darvas/pivotal structure layer. |
| `rs_state_profile` | `rs_profile`, `momentum_profile`, `structure_profile` | `leader_rs_state`, `fading_risk_score` | Runtime RS-state layer. Lifecycle/prior-cycle ideas are deferred reference-only evidence. |
| `leader_candidate_profile` | all profiles | `symbol`, `early_leader_score`, `leader_quality_score`, `leader_rs_state`, `leader_tier`, `entry_suitability`, `extension_risk_score` | Final leader candidate facts. |
| `lead_lag_pair_profile` | leaders, followers, returns | `leader_symbol`, `follower_symbol`, `lag_days`, `lagged_corr`, `lag_profile_sample_count`, `lag_profile_stability_score`, `propagation_ratio`, `propagation_state`, `pair_confidence`, `pair_evidence_confidence` | Pair explainability for follower output. |
| `follower_candidate_profile` | pairs, RS, structure | `peer_link_score`, `peer_lead_score`, `underreaction_score`, `rs_inflection_score`, `structure_preservation_score`, `sympathy_freshness_score`, `follower_confidence_score`, `follower_reject_reason_codes` | Linked underreaction layer. |
| `leader_quality_profile` | leaders, feature table | `leader_confidence_score`, `confidence_bucket`, `reject_reason_codes`, `extended_reason_codes`, `threshold_proximity_codes` | Final leader diagnostics. |
| `leader_candidate_quality_profile` | pre-final leader candidates, feature table | same diagnostic fields plus rejected candidates | Candidate universe diagnostics before final persisted leader filtering. |

## Output Entities

| Output entity | Added fields | Contract |
| --- | --- | --- |
| `leaders` | `rs_quality_score`, `leadership_freshness_score`, `momentum_persistence_score`, `near_high_leadership_score`, `hidden_rs_score`, `leader_rs_state`, `leader_tier`, `entry_suitability`, `leader_confidence_score`, `confidence_bucket`, `extension_risk_score` | Additive only. Existing output naming and BUY/SELL behavior unchanged. |
| `followers` | `peer_lead_score`, `underreaction_score`, `rs_inflection_score`, `structure_preservation_score`, `sympathy_freshness_score`, `follower_confidence_score`, `pair_evidence_confidence`, `propagation_state`, `follower_reject_reason_codes`, `link_evidence_tags` | BUY-capable source policy remains; existing signal gate required. |
| `leader_follower_pairs` | `lead_lag_profile`, `lag_days`, `lag_profile_sample_count`, `lag_profile_stability_score`, `propagation_ratio`, `propagation_state`, `connection_type`, `pair_confidence`, `pair_evidence_confidence` | Explains follower linkage. |
| `leader_quality_diagnostics` | same symbols as final `leaders.csv` | Final leader explainability. |
| `leader_candidate_quality_diagnostics` | pre-final candidates including rejects | Candidate/reject explainability. |
| `source_context` | score/state/reason-code summaries only | Source evidence remains internal/additive. |

## Relationship Map

```text
ohlcv_bar + benchmark_series
  -> rs_profile
  -> hidden_rs_profile
  -> leader_candidate_profile

ohlcv_bar
  -> momentum_profile
  -> structure_profile
  -> rotation_profile
  -> leader_candidate_profile

leader_candidate_profile + peer returns + sector_industry_group
  -> lead_lag_pair_profile
  -> follower_candidate_profile

leader_attribute_report_registry
  -> attribute_evidence / leader_lifecycle_evidence / rotation_evidence
  -> source_context only

excluded_existing_screener_registry
  -> source_context exclusion metadata only
```

## Score-To-Output Field Matrix

| Score / evidence | Leader output | Follower output | Pair output | Source context |
| --- | --- | --- | --- | --- |
| `rs_quality_score` | yes | via `rs_inflection_score` | no | IBD/O'Neil RS, RS Rating, RS line. |
| `leadership_freshness_score` | yes | no | no | RS New High, RS New High Before Price. |
| `momentum_persistence_score` | yes | no | no | Jegadeesh-Titman. |
| `near_high_leadership_score` | yes | structure context | no | George-Hwang 52-week high. |
| `hidden_rs_score` | yes | no | no | Hidden RS / weak-market resilience. |
| `leader_rs_state` | yes | leader context | no | RS-state model. |
| prior-cycle/lifecycle report evidence | no | no | no | `주도주의 생로병사` remains reference-only/deferred source context. |
| `structure_readiness_score` | yes | yes | no | Darvas/pivotal breakout. |
| `breakout_confirmation_score` | yes | yes | no | Darvas/pivotal breakout. |
| `peer_lead_score` | no | yes | yes | Hou, Lo-MacKinlay. |
| `underreaction_score` | no | yes | yes | PEAD, limited attention. |
| `sympathy_freshness_score` | no | yes | yes | PEAD / limited attention / catalyst context. |

## Excluded And Broker Scope

- `excluded-existing-screener`: Mark Minervini, Weinstein/Weinstain, Mansfield, Qullamaggie.
- Excluded broker discovery terms: FnGuide, Hana, NH Research, Kiwoom, Top Pick, 산업분류, industry classification.
- These excluded sources do not populate score fields except for explicit exclusion metadata.

## Acceptance Checks

- `broker_report_catalog`, `theme_hint`, and `catalyst_hint` are replaced by direct attribute evidence fields.
- Direct leader-attribute reports never own price/RS truth.
- Existing 3대 screener families do not appear as direct algorithm sources.
