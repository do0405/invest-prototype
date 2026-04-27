# PineScript Buy/Sell Signal Accuracy Upgrade ERD

- status: conceptual data model
- as_of: 2026-04-22
- scope: future documentation and implementation planning; no schema migration in this pass

## Entity Model

이 ERD는 Pine/reference 기반 정확도 개선을 위한 conceptual model이다. 현재 runtime schema를
바꾸지 않으며, 향후 구현 시 additive fields와 diagnostics를 어디에 둘지 명확히 한다.

## Input Entities

| Entity | Owner | Key fields | Notes |
| --- | --- | --- | --- |
| `ohlcv_bar` | this repo | `market`, `symbol`, `date`, `open`, `high`, `low`, `close`, `volume` | 모든 pattern feature의 primary input. |
| `benchmark_series` | this repo/config | `market`, `benchmark_symbol`, `date`, `close`, `return_1d` | RS, market-relative context, hidden weakness checks에 사용. |
| `source_registry_entry` | `screeners/signals/source_registry.py` | `symbol`, `source_tags`, `source_disposition`, `screen_stage`, `source_priority_score`, `source_fit_score` | 기존 source and scope gating context. |
| `market_truth_overlay` | `market-intel-core` bridge or local fallback | `market_condition_state`, `breadth_support_score`, `rotation_support_score`, `leader_health_score` | 이 repo는 소비만 한다. 산식 소유권은 외부 market-intel layer. |
| `financial_row` | local financial cache | `symbol`, growth fields, earnings fields | PEG/UG dashboard context. |
| `open_family_cycle` | `screeners/signals/cycle_store.py` | `engine`, `family`, `symbol`, `opened_on`, `entry_price`, `support_zone_low`, `trailing_level`, `tp_plan`, `current_position_units` | Existing lifecycle truth. |
| `signal_event_history` | `screeners/signals/cycle_store.py` | `signal_date`, `symbol`, `signal_code`, `action_type`, `family_cycle_id` | Cooldown and lifecycle provenance. |
| `peg_event_history` | `screeners/signals/cycle_store.py` | `symbol`, `event_high`, `gap_low`, `half_gap`, `followup_bars` | PEG follow-up context. |

## Reference Entities

| Entity | Key fields | Purpose |
| --- | --- | --- |
| `pine_reference` | `reference_id`, `title`, `url`, `script_access`, `source_family`, `published_or_updated`, `reference_confidence`, `repaint_risk`, `logic_summary` | Public Pine reference catalog. |
| `official_reference` | `reference_id`, `title`, `url`, `source_family`, `logic_summary` | TradingView official docs or original strategy concept reference. |
| `reference_decision` | `engine_part`, `reference_id`, `decision`, `adopted_formula`, `rejected_reason` | Links references to engine changes. |

## Derived Entities

| Entity | Derived from | Key fields | Purpose |
| --- | --- | --- | --- |
| `pattern_feature` | `ohlcv_bar`, `pine_reference`, `official_reference` | `w_quality_score`, `vcp_quality_score`, `squeeze_state`, `tight_range_score`, `pocket_pivot_score`, `pullback_quality_score`, `breakout_quality_score` | Pattern truth layer. |
| `band_reversion_feature` | `ohlcv_bar` | `bb_percent`, `z_score`, `band_reclaim`, `mid_band_reject`, `mean_reversion_pressure` | PBB/PBS/MR long/MR short layer. |
| `trend_quality_feature` | `ohlcv_bar`, `benchmark_series` | `ma_stack_score`, `ma_slope_score`, `rs_score`, `stage_context`, `momentum_context` | Trend template/stage/RS context layer. |
| `volume_quality_feature` | `ohlcv_bar` | `rvol20`, `vol2x`, `dry_volume_score`, `pocket_pivot_volume`, `breakout_volume_quality` | Volume confirmation and dry-up layer. |
| `exit_pressure_feature` | `ohlcv_bar`, `open_family_cycle` | `support_failure_score`, `pbs_score`, `mr_short_score`, `trailing_break_score`, `resistance_reject_score` | Sell/trim pressure layer. |
| `decision_score` | all features and source overlays | `decision_score`, `decision_grade`, `pattern_quality_score`, `exit_pressure_score`, `decision_rule_ids`, `reference_confidence` | Additive final decision layer. |
| `replay_case` | local OHLCV and expected state | `case_id`, `market`, `symbol`, `as_of_date`, `expected_signal_code`, `expected_action_type`, `expected_cycle_effect` | Deterministic golden/replay validation. |
| `replay_metric` | old/new outputs and future bars | `signal_count_delta`, `duplicate_rate`, `forward_return_5d`, `forward_return_10d`, `forward_return_20d`, `mae`, `fakeout_rate` | Accuracy acceptance evidence. |

## Output Entities

| Output entity | Producer | Candidate additive fields | Contract |
| --- | --- | --- | --- |
| `trend_following_events_v2` | `screeners/signals/writers.py` | `decision_score`, `decision_grade`, `pattern_quality_score`, `exit_pressure_score`, `decision_rule_ids`, `reference_confidence` | Internal diagnostic/event output; additive only. |
| `trend_following_states_v2` | `screeners/signals/writers.py` | pattern state quality and reference provenance fields | State/level provenance; no cycle mutation by itself. |
| `ultimate_growth_events_v2` | `screeners/signals/writers.py` | `pattern_quality_score`, `band_reversion_score`, `exit_pressure_score`, `reference_confidence` | Internal event output; additive only. |
| `ultimate_growth_states_v2` | `screeners/signals/writers.py` | `w_quality_score`, `vcp_quality_score`, `squeeze_state`, `traffic_light`, `decision_score` | UG state diagnostics; Green is not automatic BUY. |
| `all_signals_v2` | `screeners/signals/writers.py` | all additive diagnostics | Full internal trace surface. |
| `buy_signals_*` | `screeners/signals/writers.py` | only reviewed additive fields after compatibility check | Today-only public BUY projection; filenames unchanged. |
| `sell_signals_*` | `screeners/signals/writers.py` | only reviewed additive fields after compatibility check | Today-only public SELL/TRIM/EXIT projection; filenames unchanged. |
| `open_family_cycles` | `screeners/signals/writers.py`, `cycle_store.py` | no unreviewed reference fields | Runtime lifecycle state remains canonical. |

## Relationship Map

```text
pine_reference + official_reference
  -> reference_decision
  -> adopted_formula
  -> pattern_feature / band_reversion_feature / trend_quality_feature / volume_quality_feature
  -> decision_score
  -> signal_event
  -> cycle_effect
  -> public artifact projection

ohlcv_bar + benchmark_series + source_registry_entry + market_truth_overlay
  -> current metrics
  -> candidate enhanced features
  -> signal rows
  -> replay_metric

open_family_cycle + signal_event
  -> lifecycle mutation
  -> OPEN / ADD / TRIM / CLOSE
  -> persisted open_family_cycles
```

## Source Confidence Model

| Field | Values | Meaning |
| --- | --- | --- |
| `reference_confidence` | `A`, `B`, `C`, `D` | PRD reference policy grade. |
| `script_access` | `official`, `open-source`, `protected`, `invite-only`, `article-only` | How auditable the source is. |
| `repaint_risk` | `none-known`, `confirmed-bar-required`, `pivot-lag`, `htf-risk`, `unknown`, `reject` | Runtime trust and test requirement. |
| `adoption_decision` | `direct-replace-candidate`, `enhance-before-replace`, `reference-only`, `defer`, `reject` | Future implementation action. |

## Field Candidate Matrix

| Field | Entity | Public output candidate | Notes |
| --- | --- | --- | --- |
| `decision_score` | `decision_score` | maybe | Must not replace existing `signal_score` until replay acceptance. |
| `decision_grade` | `decision_score` | maybe | S/A/B/C/D derived from `decision_score`. |
| `pattern_quality_score` | `pattern_feature` | maybe | Applies to W, VCP, squeeze, pullback, breakout. |
| `exit_pressure_score` | `exit_pressure_feature` | maybe | Applies to PBS, breakdown, trailing, resistance reject. |
| `decision_rule_ids` | `decision_score` | maybe | Complements existing `indicator_dog_rule_ids`. |
| `reference_confidence` | `reference_decision` | internal first | Public exposure requires compatibility review. |
| `replay_metric_id` | `replay_metric` | no | Diagnostic only. |

## Invariants

- `signal_code`, `action_type`, `cycle_effect`, and position-unit fields remain lifecycle truth.
- `signal_score`, `gp_score`, and `sigma_score` keep current UG compatibility semantics unless a later implementation explicitly changes them.
- A reference cannot own signal truth unless it has an `adoption_decision` and deterministic local formula.
- Market regime fields remain context overlays. This repo must not derive broad regime truth from Pine references.

## Acceptance Checks

- The ERD contains all entities requested by the plan:
  `ohlcv_bar`, `benchmark_series`, `source_registry_entry`, `pine_reference`,
  `official_reference`, `pattern_feature`, `decision_score`, `signal_event`,
  `open_family_cycle`, `replay_case`.
- The relationship chain explicitly includes:
  reference -> adopted formula -> pattern feature -> signal trigger -> cycle effect -> public artifact.
- Candidate output fields include:
  `decision_score`, `decision_grade`, `pattern_quality_score`, `exit_pressure_score`,
  `decision_rule_ids`, `reference_confidence`.

