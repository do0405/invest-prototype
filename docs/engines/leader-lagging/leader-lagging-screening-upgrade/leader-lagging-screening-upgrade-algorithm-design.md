# Leader Lagging Screening Upgrade Algorithm Design

- status: implemented V2 algorithm design
- as_of: 2026-04-22
- scope: implemented pure calculation modules, screener orchestration, and diagnostic outputs

## Design Goal

This algorithm design keeps only the methods left after excluding the existing 3대 screener families. It strengthens leader/follower detection with independent RS, academic momentum, direct leader-attribute report logic, lead-lag evidence, Hidden RS, RS rotation, and Darvas/pivotal structure.

## Excluded Existing Screener Boundary

Mark Minervini, Weinstein/Weinstain, Mansfield, and Qullamaggie are `excluded-existing-screener`. They are not algorithm sources in this upgrade. Their names may appear only in this exclusion boundary and source-policy metadata.

Generic concepts retained:

- `RS line`, `RS Rating`, `RS New High`, `RS New High Before Price`.
- `benchmark_relative_strength`, `rs_slope`, `relative performance`.
- generic structure preservation, box breakout, pivotal breakout, and extension risk.

## Remaining Method-To-Algorithm Matrix

| Remaining method | Source / evidence | Policy | Logic extracted | Formula / pseudocode | Output target |
| --- | --- | --- | --- | --- | --- |
| RS weighted performance | RS Rating 1-99, iArpanK RS Screener, skyte relative strength | `adapt` | Recent performance weighted more than older momentum | `weighted_rs_score = 0.40*perf_3m + 0.20*perf_6m + 0.20*perf_9m + 0.20*perf_12m` | `rs_quality_score`, `rs_rank_true`, `rs_rank_proxy` |
| RS line | IBD/O'Neil RS line | `adapt` | Stock outperformance vs benchmark | `rs_line = close / benchmark_close`; `rs_line_slope = slope(rs_line, n)` | `rs_quality_score`, `rs_inflection_score` |
| RS New High Before Price | IBD/O'Neil, RS screener repos | `adapt` | RS leadership appears before price high | `rs_new_high_before_price = rs_line >= rolling_max(rs_line,w) and close < rolling_max(close,w)` | `leadership_freshness_score`, `early_leader_score` |
| IBD/O'Neil leadership subset | IBD RS line/RS Rating articles | `adapt` | Use relative-strength leadership and new-high leadership, not full CANSLIM | `leader_candidate = rs_rank_true >= threshold and rs_line_slope > 0 and near_52w_high` | `leader_phase`, `early_leader_score` |
| Academic momentum | Jegadeesh-Titman | `adopt` | 3-12 month winners persist | `intermediate_momentum = rank(return_3m, return_6m, return_12m)` | `momentum_persistence_score` |
| 52-week high | George-Hwang | `adopt` | Near-high anchoring and momentum persistence | `near_high_score = close / high_252` | `near_high_leadership_score` |
| Hidden RS | Pine/RS taxonomy, weak-market resilience | `adapt` | Leaders hold up in weak benchmark windows | `hidden_rs_score = mean(stock_return - benchmark_return on weak_market_days) + drawdown_resilience` | `hidden_rs_score`, `early_leader_score` |
| RS state | RS rotation map taxonomy | `adapt` | Leadership transitions through rising/stable/fading/weakening states | state machine from `rs_rank_level`, `rs_rank_delta`, `rs_line_slope`, `price_structure_state` | `leader_rs_state`, `fading_risk_score` |
| Lead-lag follower | Hou, Lo-MacKinlay | `adopt` | Related/visible leaders can lead peers | `peer_lead_score = max(corr(leader_return[t], follower_return[t+lag]))`, `lag in [1,2,3,5]` | `peer_lead_score`, `lead_lag_profile` |
| Underreaction / attention | Bernard-Thomas PEAD, DellaVigna-Pollet | `adapt` | Events can drift; attention delay creates catch-up | `sympathy_freshness_score = event_window_active * rs_inflection * not_extended` | `underreaction_score`, `sympathy_freshness_score` |
| Darvas / pivotal breakout | Darvas Box Theory | `adapt` | Boxes define structure; volume confirms breakout | `box_valid = range_contraction and repeated_resistance_tests and no_breakdown`; `breakout_confirmed = close > box_high and volume_ratio > threshold` | `structure_readiness_score`, `breakout_confirmation_score` |
| Direct leader attribute report | Macrend Invest `주도주의 생로병사` | `watchlist-only` | Prior leaders rarely repeat; price momentum can precede earnings; leaders have lifecycle phases | reference-only/deferred until 504+ bars and prior-cycle evidence are available | no runtime score field |

## RS / RS Line Algorithm

```text
perf_3m  = close / close_63d_ago  - 1
perf_6m  = close / close_126d_ago - 1
perf_9m  = close / close_189d_ago - 1
perf_12m = close / close_252d_ago - 1

weighted_rs_score =
    0.40 * perf_3m
  + 0.20 * perf_6m
  + 0.20 * perf_9m
  + 0.20 * perf_12m

rs_rank_true  = percentile_rank(weighted_rs_score, same_market_universe)
rs_rank_proxy = historical_distribution_rank(weighted_rs_score, own_symbol_history)

rs_line = close / benchmark_close
rs_line_slope = slope(rs_line, rs_slope_window)
```

Rules:

- `rs_rank_true` is the primary rank when the full market universe is available.
- `rs_rank_proxy` is allowed only as a labeled proxy.
- `rs_quality_score` combines rank level, rank slope, RS line slope, and RS new-high evidence.

## RS New High Before Price

```text
rs_line_new_high_w = rs_line >= rolling_max(rs_line, w)
price_new_high_w = close >= rolling_max(close, w)

rs_new_high_before_price_w =
    rs_line_new_high_w and not price_new_high_w

leadership_freshness_score =
    0.45 * max(rs_new_high_before_price_65, rs_new_high_before_price_252)
  + 0.30 * positive(rs_line_slope)
  + 0.25 * group_rank_improvement
```

Use this to identify early leaders before price breakout screens become obvious.

## IBD / O'Neil Independent Subset

CANSLIM as a full system is not copied. Only independent leader-screening primitives remain:

```text
leader_candidate =
    rs_rank_true >= rs_leader_threshold
    and rs_line_slope > 0
    and near_52w_high_score >= near_high_threshold

early_leader_bonus =
    rs_new_high_before_price_bonus
    + group_rank_improvement_bonus

late_chase_penalty =
    distance_from_recent_structure
    + excessive_short_term_return

early_leader_score =
    rs_quality_score
    + leadership_freshness_score
    + early_leader_bonus
    - late_chase_penalty
```

## Academic Momentum And 52-Week High

```text
intermediate_momentum =
    rank(return_3m)
  + rank(return_6m)
  + rank(return_12m)

momentum_persistence_score = normalize(intermediate_momentum)

near_high_leadership_score = close / high_252

overextension_flag =
    close / ma20 - 1 > ma20_extension_limit
    or close / recent_structure_high - 1 > structure_extension_limit
```

Interpretation:

- High `near_high_leadership_score` supports leadership.
- `overextension_flag` does not erase leadership; it marks fresh-entry risk.

## Hidden RS

```text
weak_market_days = benchmark_return_1d < 0
weak_window = rolling_sum(benchmark_return_1d, w) < weak_window_threshold

relative_down_day_return =
    mean(stock_return_1d - benchmark_return_1d on weak_market_days)

drawdown_resilience =
    max_drawdown(benchmark, w) - max_drawdown(stock, w)

hidden_rs_score =
    normalize(relative_down_day_return)
  + normalize(drawdown_resilience)
  + bonus_if(close_above_key_structure_during_weak_window)
```

Hidden RS is leader evidence, not a market regime claim.

## RS State

```text
if rs_rank_level high and rs_rank_delta > 0 and rs_line_slope > 0 and structure_intact:
    leader_rs_state = "rising"
elif rs_rank_level high and abs(rs_rank_delta) <= flat_band and structure_intact:
    leader_rs_state = "stable"
elif rs_rank_delta < 0 and rs_line_slope < 0 and structure_intact:
    leader_rs_state = "fading"
elif structure_breakdown or heavy_volume_failed_breakout:
    leader_rs_state = "weakening"
else:
    leader_rs_state = "unknown"

fading_risk_score =
    normalize(-rs_rank_delta)
  + normalize(-rs_line_slope)
  + structure_damage_penalty
```

This state model replaces excluded named stage systems and is not a lifecycle model.

## Lead-Lag Follower

```text
lags = [1, 2, 3, 5]

for lag in lags:
    lagged_corr[lag] =
        corr(leader_return[t-lookback:t],
             follower_return[t-lookback+lag:t+lag])

best_lag = argmax(lagged_corr)
peer_lead_score = max(lagged_corr[best_lag], 0) * peer_link_score

propagation_ratio =
    follower_event_return / leader_event_return
    if abs(leader_event_return) >= min_event_return

catchup_room = max(0, target_propagation_ratio - propagation_ratio)

underreaction_score =
    catchup_room
    * structure_preservation_score
    * peer_link_score
```

Follower output must include the leader pair. A follower without pair evidence is not a linked follower.

## PEAD / Limited Attention

```text
event_window_active =
    days_since_leader_event <= max_catchup_window

rs_inflection =
    rs_rank_delta > min_rank_delta
    or rs_line_slope_turns_positive

not_extended =
    extension_risk_score <= follower_extension_limit

sympathy_freshness_score =
    event_window_active
    * rs_inflection
    * not_extended
```

Event/catalyst evidence can raise review priority only. It cannot create automatic BUY.

## Darvas / Pivotal Breakout

```text
box_high = rolling_resistance(high, box_window)
box_low = rolling_support(low, box_window)

box_valid =
    range_contraction
    and repeated_resistance_tests >= min_tests
    and no_breakdown_below_box_low

breakout_confirmed =
    close > box_high
    and volume / avg_volume_50d > volume_threshold

structure_readiness_score =
    normalize(box_valid)
  + normalize(close_distance_to_box_high)
  + support_integrity_bonus

breakout_confirmation_score =
    breakout_confirmed
    * volume_confirmation
```

Darvas/pivotal structure is a quality and preservation layer, not standalone BUY logic.

## Direct Leader Attribute Report Logic

Core source: Macrend Invest `주도주의 생로병사`.

Extracted logic:

- prior-cycle leaders are less likely to become next-cycle leaders immediately.
- early price momentum can be more useful than current earnings growth for identifying leadership.
- lifecycle phases can exist conceptually, but they are not runtime fields in V2.
- leader rotation is not automatically a broad market breakdown.

```text
runtime_field = none
carryover_policy = reference-only/deferred
reason = current local OHLCV history is not long enough for durable prior-cycle classification
```

Report evidence is `watchlist-only`; price/RS/structure calculations still own the score.

## Score-To-Output Field Matrix

| Score / feature | `leaders` | `followers` | `leader_follower_pairs` | Source context |
| --- | --- | --- | --- | --- |
| `weighted_rs_score` | yes | yes | no | RS Rating / RS screener. |
| `rs_rank_true` | yes | yes | no | local universe rank. |
| `rs_rank_proxy` | optional | optional | no | Pine/IBD-style proxy. |
| `rs_line` / `rs_line_slope` | yes | yes | no | generic RS line. |
| `rs_new_high_before_price` | yes | no | no | IBD/O'Neil / RS New High. |
| `momentum_persistence_score` | yes | no | no | Jegadeesh-Titman. |
| `near_high_leadership_score` | yes | yes as structure context | no | George-Hwang. |
| `hidden_rs_score` | yes | no | no | Hidden RS. |
| `leader_rs_state` | yes | leader context | no | RS Rotation. |
| `fading_risk_score` | yes | leader context | yes | RS Rotation. |
| prior-cycle/lifecycle report evidence | no | no | no | `주도주의 생로병사` is reference-only/deferred. |
| `structure_readiness_score` | yes | yes | no | Darvas/pivotal breakout. |
| `breakout_confirmation_score` | yes | yes | no | Darvas/pivotal breakout. |
| `peer_lead_score` | no | yes | yes | Hou / Lo-MacKinlay. |
| `underreaction_score` | no | yes | yes | PEAD / limited attention. |
| `sympathy_freshness_score` | no | yes | yes | PEAD / DellaVigna-Pollet. |
| `attribute_evidence` | context | context | context | direct leader attribute report only. |

## Validation Scenarios

| Scenario | Expected result |
| --- | --- |
| RS leads price | High `leadership_freshness_score`; lifecycle remains deferred. |
| Strong 3-12M momentum near 52w high | High `momentum_persistence_score` and `near_high_leadership_score`. |
| Prior-cycle leader fading | Reference-only note; no runtime score until sufficient prior-cycle history exists. |
| New rotation candidate | Use `leader_rs_state`, hidden RS, and structure evidence; no lifecycle score field. |
| Hidden RS in weak benchmark | High `hidden_rs_score`, not a regime claim. |
| Linked follower | Positive `peer_lead_score`, `underreaction_score`, and pair row. |
| Darvas breakout without RS | Structure evidence exists, but not enough for leader without RS/momentum. |
| Direct report only | `attribute_evidence` present, no BUY and no score override. |

## Source Grounding

- RS / implementation references: [`iArpanK/RS-Screener`](https://github.com/iArpanK/RS-Screener), [`arpankundu4/rs-line`](https://github.com/arpankundu4/rs-line), [`skyte/relative-strength`](https://github.com/skyte/relative-strength), [TradingView RS Rating 1-99](https://www.tradingview.com/script/vYn2nyIW-RS-Rating-1-99/), [IBD RS line](https://www.investors.com/how-to-invest/investors-corner/stock-market-leaders-bullish-relative-strength-lines/).
- Academic references: [Jegadeesh-Titman](https://ideas.repec.org/a/bla/jfinan/v48y1993i1p65-91.html), [George-Hwang](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1104491), [Moskowitz-Grinblatt](https://ideas.repec.org/a/bla/jfinan/v54y1999i4p1249-1290.html), [Hou](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1151155), [Lo-MacKinlay](https://academic.oup.com/rfs/article/3/2/175/1595488), [Bernard-Thomas](https://ideas.repec.org/a/bla/joares/v27y1989ip1-36.html), [DellaVigna-Pollet](https://www.nber.org/papers/w11683).
- Structure reference: [Darvas Box Theory](https://www.investopedia.com/terms/d/darvasboxtheory.asp).
- Direct leader attribute report: [Macrend Invest `주도주의 생로병사`](https://ssl.pstatic.net/imgstock/upload/research/invest/1539740471781.pdf).
