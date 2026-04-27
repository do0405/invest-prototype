# PineScript Buy/Sell Signal Accuracy Upgrade Runtime Spec

- status: future runtime contract
- as_of: 2026-04-22
- scope: no runtime code change in this documentation pass

## Runtime Position

This upgrade is documentation-first. Future runtime implementation must remain
deterministic, local-data-first, and additive-compatible until replay evidence justifies
direct trigger replacement.

## Hard Runtime Contracts

| Contract | Requirement |
| --- | --- |
| Local deterministic runtime | Screening must not fetch TradingView, GitHub, broker, or web sources at runtime. |
| Public output stability | `buy_signals_all_symbols_v1`, `sell_signals_all_symbols_v1`, `buy_signals_screened_symbols_v1`, `sell_signals_screened_symbols_v1` filenames remain unchanged. |
| Today-only public artifacts | Public buy/sell outputs continue to include only current `as_of_date` rows. |
| Lifecycle truth | Existing `cycle_effect`, `position_units_before`, `position_units_after`, `position_delta_units`, and open-cycle persistence remain authoritative. |
| Weak-market policy | Weak market remains warning/downgrade context only; it does not suppress BUY artifacts by itself. |
| Market regime ownership | Broader regime/breadth/rotation truth stays in market-intel layer. This repo consumes fields. |
| No source code copying | Pine code is not copied verbatim. Adopted formulas are independent local implementations. |

## Runtime Flow For Future Implementation

1. Load local OHLCV, metadata, financial rows, source registry snapshot, active cycles, and histories.
2. Build current metrics with existing `_build_metrics()` behavior.
3. Compute enhanced pattern features from deterministic helpers:
   - W/double-bottom quality.
   - VCP/contraction quality.
   - squeeze/tight/consolidation quality.
   - pocket pivot and volume confirmation.
   - PBB/PBS/MR band reversion features.
   - breakout/pullback/momentum/PEG quality.
   - trailing/exit pressure features.
4. Compute additive `decision_score` and `decision_grade`.
5. Emit existing signal rows with additive diagnostics only.
6. Run cycle lifecycle using existing OPEN/ADD/TRIM/CLOSE logic.
7. Project public BUY/SELL artifacts with current today-only filters.
8. Persist internal histories and diagnostics.

## Source Runtime Policy

| Source type | Runtime effect | Forbidden effect |
| --- | --- | --- |
| TradingView official docs/library | Defines non-repaint, confirmed-bar, ensemble score constraints. | Runtime network fetch. |
| Open-source Pine reference | Provides candidate formula after independent translation. | Verbatim code copy or untested trigger swap. |
| Protected Pine with detailed description | Provides B-grade concept and thresholds to validate against original source. | Treating hidden implementation as exact truth. |
| Original/trader method | Validates whether a Pine implementation matches known concept. | Overriding local replay evidence. |
| Market-intel overlay | Supplies market context fields. | Moving regime formulas into this repo. |

## Repaint And Confirmation Policy

| Risk | Handling |
| --- | --- |
| `barstate.isconfirmed` requirement | Future logic must use closed local daily bars or explicit closure diagnostics. |
| Pivot-based patterns | Pivot lag is accepted only if tests assert delayed confirmation semantics. |
| HTF `request.security` references | Reject or convert to local confirmed daily/weekly aggregation before adoption. |
| `calc_on_every_tick` strategy behavior | Reject for daily screening semantics. |
| Backpainting or future leak | Reject as D-grade reference. |

## Market Regime Assumption

- Horizon: daily and swing-trading signal horizon.
- Markets: US and KR must be evaluated separately when market context is used.
- Observable inputs: `market_condition_state`, `market_alignment_score`,
  `breadth_support_score`, `rotation_support_score`, `leader_health_score`.
- Ownership: broad regime computation is outside this repo. This repo may attach warnings,
  conviction downgrades, and sizing changes from already supplied context.
- Validation needed: replay must report weak-market rows separately, but weak market cannot be
  used as a silent hard suppressor unless a later user-approved policy changes that contract.

## Direct Replacement Gate

Future trigger replacement cannot happen only because a Pine reference looks cleaner. It needs
the following evidence.

| Gate | Requirement |
| --- | --- |
| Golden case | Synthetic fixture proves expected emit/non-emit behavior for the family. |
| Old-vs-new replay | Local OHLCV replay compares signal counts, duplicates, forward returns, MAE, fakeouts. |
| Lifecycle compatibility | Existing cycle effects remain correct after trigger changes. |
| Scope compatibility | `all` and `screened` scopes remain independent. |
| Public artifact compatibility | Public today-only outputs remain unchanged except additive reviewed fields. |

## Replay Metrics

| Metric | Definition |
| --- | --- |
| `old_signal_count` / `new_signal_count` | Number of emitted signal rows by family and action. |
| `duplicate_rate` | Same symbol/family/day duplicate signal pressure. |
| `forward_return_5d` | Close-to-close return 5 trading days after signal. |
| `forward_return_10d` | Close-to-close return 10 trading days after signal. |
| `forward_return_20d` | Close-to-close return 20 trading days after signal. |
| `max_adverse_excursion` | Worst close/low excursion after signal before evaluation horizon. |
| `fakeout_rate` | Breakout/reversal invalidation within configured bars. |
| `stop_proxy_hit_rate` | Whether family stop/support level was breached. |
| `tp_proxy_hit_rate` | Whether family TP or reference target was reached. |

## Runtime Defaults

| Field | Default | Reason |
| --- | --- | --- |
| `decision_score` | null | Do not imply final score until helper exists. |
| `decision_grade` | empty | Compatibility-safe missing value. |
| `pattern_quality_score` | null | Pattern-specific only. |
| `exit_pressure_score` | null | Sell/trim/exit-specific only. |
| `decision_rule_ids` | empty list | Additive provenance only. |
| `reference_confidence` | empty | Internal source mapping must exist first. |

## Parallelization Contract

Symbol-local feature computation can be parallelized if final ordering is deterministic.

Do not parallelize without deterministic reduce:

- Cross-symbol replay metrics.
- Family-level old-vs-new comparison.
- Public output sorting.
- Cycle lifecycle mutation for same symbol/family.
- Shared source registry and market overlay application.

## Runtime Test Plan

| Scenario | Expected result |
| --- | --- |
| Public outputs remain today-only | `buy_signals_*` and `sell_signals_*` contain only current `as_of_date` rows. |
| W confirmed vs pending | W quality can be pending without BUY; neckline confirmation is distinct. |
| VCP forming vs breakout | VCP state emits as context; BUY requires concrete breakout trigger. |
| PBB Green vs Orange | Green can BUY when PBB event is valid; Orange remains WATCH unless future policy changes. |
| PBS mid-band reject | PBS close emits EXIT only after high touches/reclaims and close rejects below band. |
| MR short trim limit | Two trims max before final close path. |
| Trailing never-down | Lower recalculated level cannot reduce persisted protected level. |
| Weak market warning-only | BUY row remains visible with warning/downgrade/sizing context. |
| Replay no future leakage | Explicit `as_of_date` cannot use future rows for signal or evaluation input. |

## Runtime Non-Goals

- No live web lookup during screening.
- No Pine script execution inside the production screening path.
- No public past-N-day buy/sell lookup restoration.
- No hard market-regime suppressor in this repo.
- No direct code change in this documentation batch.

