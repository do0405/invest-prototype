# Indicator Dog Signal Algorithm Fidelity Contract

## Purpose

This document is the implementation contract for buy/sell signal semantic fidelity.
It does not replace the active-signal inventory audit in
`docs/audits/archive/2026-03-31-buy-sell-signal-audit.md`.

The archive audit answers whether active `signal_code` values are generated, persisted,
and regression-tested. This contract answers a different question:

- Does each `signal_code` algorithm reflect the intended Indicator Dog document rule?
- If not, is the implementation an acceptable proxy, a partial implementation, missing,
  or an intentional repo-level divergence?
- Which gaps are part of the accepted implementation batch, and which remain deferred?

This document is also the implementation contract for the 2026-04-21 fidelity batch.
Runtime changes must remain limited to the additive fields and semantic resets described
below.

## Scope And Assumptions

- Scope is `screeners/signals/engine.py` signal semantics for Trend Following and Ultimate Growth.
- Source material is under `docs/archive/raw-sources/Reference/indicator-dog/`.
- `docs/archive/raw-sources/Reference/indicator-dog/` remains untouched; durable repo interpretation lives under `docs/engines/signals/`.
- Current output filenames and today-only buy/sell split are preserved by this implementation.
- Additive output fields are allowed: `cycle_effect`, `position_units_before`,
  `position_units_after`, `position_delta_units`, and `indicator_dog_rule_ids`.
- `gp_score` and `sigma_score` intentionally reset semantics for UG rows:
  they are Indicator Dog raw subtotals, not the previous repo proxy health scores.
- Weak-market BUY behavior intentionally follows the repo policy selected by the user:
  weak market context must warn, downgrade conviction/sizing, and remain visible. It must not
  suppress BUY artifacts by itself.
- Market regime logic remains outside this repository except for consuming existing market
  context fields and source-registry overlays.

## Fidelity Status

| Status | Meaning |
| --- | --- |
| `implemented` | Current algorithm directly represents the document rule. |
| `proxy-backed` | Current algorithm represents the document concept through an explicit technical proxy. |
| `partial` | Core concept is present, but important document behavior is missing or ambiguous. |
| `missing` | Document rule has no current runtime equivalent. |
| `intentional-divergence` | Current repo policy intentionally differs from the Indicator Dog document. |

## Rule ID Namespace

Rule IDs are stable identifiers for additive output fields such as
`indicator_dog_rule_ids`.

| Rule ID | Indicator Dog source | Document intent |
| --- | --- | --- |
| `IDOG.TF.REGULAR_PULLBACK` | `Trend following/1-1. Regular Buy.md` | Buy only after trend, controlled pullback, and action/reversal confirmation. |
| `IDOG.TF.BREAKOUT_BB_VOL` | `Trend following/1-2. Breakout.md` | Buy a real breakout through resistance/band with volume and strength confirmation. |
| `IDOG.TF.BUILDUP_VCP` | `Trend following/1-3 Build-up.md` | Treat VCP/build-up as a pre-buy watch state, not an immediate buy. |
| `IDOG.TF.AGGRESSIVE_ALERT` | `Trend following/1-4. aggressive.md` | High-risk early/counter-trend opportunity; small-size/tight-risk concept. |
| `IDOG.TF.MARKET_FILTER` | `Trend following/2-1. market filter.md` | Market weakness should constrain new buys. |
| `IDOG.TF.SUPPORT_FAIL_EXIT` | `Trend following/1-1. Regular Buy.md`, `Trend following/2-3. Trailing Stop Loss.md` | Exit when the family/cycle support or stop reference fails. |
| `IDOG.TF.PARTIAL_EXIT` | `Trend following/2-2 partial exit.md` | Sell 30-50% when short trend support weakens, keep the rest until final exit. |
| `IDOG.TF.TRAILING_RATCHET` | `Trend following/2-3. Trailing Stop Loss.md` | Trailing stop rises with price and never moves down; final exit on break. |
| `IDOG.TF.CHANNEL_BREAK_EXIT` | `Trend following/2-3. Trailing Stop Loss.md` | Exit immediately when a range/channel floor breaks. |
| `IDOG.TF.R_MULTIPLE_TP` | `Trend following/2-4. R multiple TP.md` | TP1/TP2 are based on entry minus stop risk multiples. |
| `IDOG.TF.FAILED_RECLAIM_EXIT` | Repo proxy related to Trend pullback/partial-exit docs | Exit/defensive sell when price rejects a reclaim zone and closes weak. |
| `IDOG.TF.PEG_R50_REBREAK` | `Trend following/3-1. Power Earnings Gap.md` | PEG follow-up buys use gap-low/R50 pullback and event-high rebreak. |
| `IDOG.TF.PYRAMID_WINNER` | `Trend following/3-2. Pyramiding.md` | Add only to winning positions with improved protection. |
| `IDOG.TF.ADDON_READY` | `Trend following/3-2. Pyramiding.md` | State provenance that an existing winner is eligible for an add-on. |
| `IDOG.TF.LEVEL_PROVENANCE` | `Trend following/2-3. Trailing Stop Loss.md`, `Trend following/2-4. R multiple TP.md` | State provenance for trailing, break-even, and R-multiple target levels. |
| `IDOG.TF.MOMENTUM_CHASE` | `Trend following/3-3. momentum chasing.md` | Tactical momentum entry/exit based on RSI, MACD, and breakout pressure. |
| `IDOG.UG.STATE_TRAFFIC_LIGHT` | `Ultimate growth/1-3. dashboard.md` | Green/Orange/Red technical state describes action readiness. |
| `IDOG.UG.GP_NH60` | `Ultimate growth/1-4. GP-engine.md` | New-high context indicates leadership/ceiling break. |
| `IDOG.UG.GP_VOL2X` | `Ultimate growth/1-4. GP-engine.md` | 2x volume confirms energy/fuel. |
| `IDOG.UG.GP_W` | `Ultimate growth/1-4. GP-engine.md` | W/double-bottom structure supports constructive recovery. |
| `IDOG.UG.SIGMA_BO` | `Ultimate growth/1-5. sigma-engine.md` | Band breakout means trend-following launch. |
| `IDOG.UG.SIGMA_PBB` | `Ultimate growth/1-5. sigma-engine.md` | Pullback buy after support near lower/mid Sigma band. |
| `IDOG.UG.SIGMA_PBS` | `Ultimate growth/1-5. sigma-engine.md` | Pullback sell after rebound failure near middle/upper band. |
| `IDOG.UG.SIGMA_MR_LONG` | `Ultimate growth/1-5. sigma-engine.md` | Mean-reversion long after excessive downside. |
| `IDOG.UG.SIGMA_MR_SHORT` | `Ultimate growth/1-5. sigma-engine.md` | Mean-reversion short/trim after excessive upside. |
| `IDOG.UG.SIGMA_BREAKDOWN` | `Ultimate growth/1-5. sigma-engine.md` | Immediate exit when lower/support band breaks. |
| `IDOG.UG.VALIDATION_SCORE` | `Ultimate growth/3-1. validation.md` | Validation score follows the documented point table: Vol +30, W +25, NH +15, PBB +15, BO +10, MR +5, Breakdown -20. |

## Trend Sell Tags

| signal_code | Current algorithm summary | Fidelity | Gap | Proposed handling | Test target |
| --- | --- | --- | --- | --- | --- |
| `TF_SELL_BREAKDOWN` | `_trend_sell_events()` emits when `close <= cycle.support_zone_low`; row reason includes `SUPPORT_FAIL`; `_update_cycles()` closes cycle. | `proxy-backed` | Document describes support/stop failure broadly. Current support can come from trend zone, breakout zone, PEG zone, or loaded cycle state; support-origin provenance remains coarser than the document. | Keep close behavior. Emit `IDOG.TF.SUPPORT_FAIL_EXIT` and `cycle_effect=CLOSE`; support-origin detail can be improved later without changing lifecycle behavior. | Support-only failure emits breakdown and closes cycle without requiring channel break. |
| `TF_SELL_CHANNEL_BREAK` | Emits when `close <= metrics.channel_low8`; `_TF_CHANNEL_LOOKBACK` is 8; `_update_cycles()` closes cycle. | `implemented` | The document says channel floor break is immediate exit. Current proxy is an 8-bar rolling prior-low channel floor, which is explicit but not documented as a configurable setting. | Keep algorithm. Emit `IDOG.TF.CHANNEL_BREAK_EXIT` and `cycle_effect=CLOSE`. | Channel-only failure emits channel break, closes cycle, and does not require support failure. |
| `TF_SELL_TRAILING_BREAK` | Emits when not `in_channel8` and `close <= max(cycle.trailing_level, cycle.protected_stop_level)`; closes cycle. Active-cycle refresh keeps trailing/protected stops from moving down. | `implemented` | Exact trailing source remains a repo proxy, but the never-down invariant is enforced and tested. | Keep `IDOG.TF.TRAILING_RATCHET` and `cycle_effect=CLOSE`. | Reloaded cycle whose new MA is lower must keep prior trailing/protected level. |
| `TF_SELL_TP1` | Emits when `high >= cycle.tp1_level` and `tp1_hit` is false. `_update_cycles()` sets `tp1_hit`, `trim_count >= 1`, `risk_free_armed=True`, and trims current units by 50%. | `implemented` | Output `action_type` stays `SELL` for compatibility; lifecycle truth comes from `cycle_effect=TRIM`. | Keep authoritative `cycle_effect=TRIM`, before/after/delta fields, and open cycle. | TP1 reduces units and leaves the cycle open with risk-free state armed. |
| `TF_SELL_TP2` | Emits when `high >= cycle.tp2_level` and `tp2_hit` is false. `_update_cycles()` sets `tp2_hit`, `trim_count >= 2`, and trims current units by 50%. | `implemented` | Output `action_type` stays compatibility-oriented while lifecycle truth comes from `cycle_effect=TRIM`. | Keep cycle open for runner until final close. | TP2 reduces units after TP1 and leaves the cycle open. |
| `TF_SELL_MOMENTUM_END` | For `TF_MOMENTUM`, emits if RSI14 < 55, MACD histogram < 0, close <= EMA10, or after 10 business days close < fast ref; closes cycle. | `proxy-backed` | Document describes momentum fade conceptually through RSI/MACD/Donchian pressure and short swing timing. Current exact thresholds are repo heuristics. | Keep thresholds as current proxy. Emit `IDOG.TF.MOMENTUM_CHASE` and `cycle_effect=CLOSE`; threshold recalibration remains deferred. | Each fade component can independently produce a momentum-end close row. |
| `TF_SELL_RESISTANCE_REJECT` | Emits when not in 8-bar channel, high touches/exceeds BB mid, close finishes below BB mid, and daily return is negative; closes cycle. | `proxy-backed` | No directly named Indicator Dog Trend sell rule matches this tag. It is a practical failed-reclaim / pullback-sell proxy. | Keep but document as proxy-backed. Emit `IDOG.TF.FAILED_RECLAIM_EXIT`, not a direct Indicator Dog source claim. | Mid-band rejection emits only when high touched the level and close rejected below it. |

## Ultimate Growth Sell Tags

| signal_code | Current algorithm summary | Fidelity | Gap | Proposed handling | Test target |
| --- | --- | --- | --- | --- | --- |
| `UG_SELL_MR_SHORT` | Emits when `metrics.ug_mr_short_ready`, reference exit allows it, and `trim_count < 2`; condition is upper band touch plus negative return or `risk_heat`; `_update_cycles()` trims 50% then 25% of base units. | `implemented` | Row output now exposes `cycle_effect=TRIM` and before/after/delta fields. | Keep algorithm and `IDOG.UG.SIGMA_MR_SHORT`. | Two MR Short trims reduce units, third is suppressed until PBS/breakdown close. |
| `UG_SELL_PBS` | Emits when `metrics.ug_pbs_ready` and BB mid exists; current PBS is high >= BB mid, close < BB mid, and negative return; closes cycle. | `partial` | Sigma PBS is implemented, but the UG Trend document's Red + PBS condition is not a hard gate. Current behavior treats traffic light as context only. | Keep current behavior: do not add a Red hard gate. Emit `IDOG.UG.SIGMA_PBS` and `cycle_effect=CLOSE`; Red remains context, not a prerequisite. | PBS exits when rebound fails at BB mid, with traffic-light context visible. |
| `UG_SELL_BREAKDOWN` | Emits when `close <= cycle.support_zone_low`; reason includes `SIGMA_BREAKDOWN`; closes cycle. | `proxy-backed` | Current support can be Sigma lower/mid band for new UG cycles, but restored cycles may only expose generic support. It does not require `ug_breakdown_risk`. | Keep final close behavior. Emit `IDOG.UG.SIGMA_BREAKDOWN` and `cycle_effect=CLOSE`; richer support-origin provenance remains optional. | Breakdown closes restored and newly opened UG cycles deterministically. |

## Trend Buy, State, And Add-On Tags

| signal_code | Current algorithm summary | Fidelity | Gap | Proposed handling | Test target |
| --- | --- | --- | --- | --- | --- |
| `TF_BUY_REGULAR` | Requires liquidity, bullish alignment, ADX >= 20, MA gap, rising support trend, setup active, pullback profile pass, reversal at support, and not in channel. | `proxy-backed` | Document core trend + pullback + action exists, but repo adds conservative gates not stated in the document. | Emit `IDOG.TF.REGULAR_PULLBACK`; `cycle_effect=OPEN` only when final action remains `BUY`. Document the extra gates as repo risk filters. | Regular buy opens only on action-confirmed pullback. |
| `TF_BUY_BREAKOUT` | Requires breakout context, `breakout_ready`, setup/build-up, VCP/build-up/NH context, and not in channel. `breakout_ready` includes BB upper clear, RVOL, strong close/body, and energy. | `proxy-backed` | Document core breakout + volume/strength exists, but repo adds conservative setup, trend, and channel gates. | Emit `IDOG.TF.BREAKOUT_BB_VOL`; `cycle_effect=OPEN` only when final action remains `BUY`. Document the extra gates as repo risk filters. | Breakout buy requires BB/anchor clear plus volume/strength. |
| `TF_BUILDUP_READY` | Emits STATE when setup/build-up is active, with VCP/squeeze/tight/dry-volume/near-high reasons when present. | `implemented` | Correctly remains state/watch context rather than BUY. | Emit `IDOG.TF.BUILDUP_VCP` and `cycle_effect=STATE`. | Build-up/VCP does not open a cycle by itself. |
| `TF_VCP_ACTIVE` | Emits AUX STATE when VCP is active from compression, dry volume, and tight range. | `implemented` | VCP compression proxy is explicit; exact VCP geometry is heuristic. | Keep as proxy-backed implementation detail under build-up rule. | VCP row carries dry-volume/tight-range reasons. |
| `TF_AGGRESSIVE_ALERT` | Emits ALERT when `aggressive_ready` is true, with below-200MA alert/reversal/rising EMA reasons. | `intentional-divergence` | Document allows aggressive small/tight-risk action. Repo currently keeps it as alert only, which avoids promoting high-risk counter-trend rows into BUY artifacts. | Keep alert-only unless a later explicit feature flag promotes it. | Aggressive alert never opens a cycle. |
| `TF_BUY_PEG_PULLBACK` | During active PEG follow-up, emits when low trades between gap low and half gap, and close reclaims/holds half gap. | `implemented` | Directly maps R50/half-gap pullback concept. | Emit `IDOG.TF.PEG_R50_REBREAK`; `cycle_effect=OPEN` only when final action remains `BUY`. | PEG pullback opens only inside gap-low to half-gap structure. |
| `TF_BUY_PEG_REBREAK` | During active PEG follow-up, emits when close >= PEG event high. | `implemented` | Directly maps event-high rebreak concept. | Emit `IDOG.TF.PEG_R50_REBREAK`; `cycle_effect=OPEN` only when final action remains `BUY`. | PEG rebreak opens on event-high reclaim. |
| `TF_PEG_EVENT` | Emits ALERT/WATCH for confirmed or missed PEG event day. | `implemented` | Event-day tracking is informational; it does not open cycle until follow-up trigger. | Keep as event provenance. | Confirmed PEG event emits alert/watch only. |
| `TF_BUY_MOMENTUM` | Emits when `momentum_ready` is true and not in channel; momentum uses liquidity, rising support trend, RSI, MACD, Donchian/high pressure, strong close. | `proxy-backed` | Document concept maps well, but thresholds are repo heuristics. | Emit `IDOG.TF.MOMENTUM_CHASE`; threshold recalibration remains deferred. | Momentum buy and momentum end share rule lineage. |
| `TF_ADDON_PYRAMID` | Existing cycle add-on path requires family-specific ready state, not in channel, profitable position, blended entry protected, trailing ratcheted, and protection improved. Slot sizes are 0.50 then 0.30. | `partial` | Winner-only and protection-improved constraints are strong. `3-2. Pyramiding.md` gives a 100/50/30 example that matches current code, while `4-2. Settings.md` says 1st add 50% and 2nd add 25%; this contract keeps current 0.50/0.30 and records the settings mismatch. | Keep current tranche behavior. Emit `IDOG.TF.PYRAMID_WINNER`, `cycle_effect=ADD`, and position delta fields. | Add-on cannot average down and must improve protection. |

## Trend State And Level Tags

These tags do not open, trim, or close cycles. They are state/level provenance rows that carry rule IDs without changing cycle state.

| signal_code group | Current role | Fidelity | Proposed handling | Test target |
| --- | --- | --- | --- | --- |
| `TF_SETUP_ACTIVE` | Setup state row for aligned trend/support context before a buy trigger. | `proxy-backed` | Map to `IDOG.TF.BUILDUP_VCP` or `IDOG.TF.REGULAR_PULLBACK` context depending on row reasons; keep `cycle_effect=STATE`. | Setup state emits without opening a cycle. |
| `TF_ADDON_READY`, `TF_ADDON_SLOT1_READY`, `TF_ADDON_SLOT2_READY` | Add-on readiness state for an existing winning cycle. | `partial` | Map to `IDOG.TF.ADDON_READY` and `IDOG.TF.PYRAMID_WINNER`; keep current 0.50/0.30 tranche policy and expose readiness as provenance only. | Add-on readiness rows do not mutate cycle until `TF_ADDON_PYRAMID` event. |
| `TF_TRAILING_LEVEL`, `TF_PROTECTED_STOP_LEVEL`, `TF_BREAKEVEN_LEVEL` | Active-cycle stop/protection level rows. | `partial` | Map to `IDOG.TF.LEVEL_PROVENANCE` and `IDOG.TF.TRAILING_RATCHET`; never-down refresh behavior is enforced and tested. | Level rows describe current protection and do not close cycles by themselves. |
| `TF_TP1_LEVEL`, `TF_TP2_LEVEL` | Active-cycle R-multiple target level rows. | `partial` | Map to `IDOG.TF.LEVEL_PROVENANCE` and `IDOG.TF.R_MULTIPLE_TP`; TP trim accounting is handled by sell rows. | TP level rows describe target levels and do not trim cycles by themselves. |

## Ultimate Growth Buy, State, And Aux Tags

| signal_code | Current algorithm summary | Fidelity | Gap | Proposed handling | Test target |
| --- | --- | --- | --- | --- | --- |
| `UG_STATE_GREEN`, `UG_STATE_ORANGE`, `UG_STATE_RED` | Derived from `signal_score` built from the documented validation table. `gp_score` is raw Vol2x +30, W +25, NH +15. `sigma_score` is raw PBB +15, BO +10, MR Long +5, Breakdown -20. `signal_score = clamp(gp_score + sigma_score, 0, 100)`. | `implemented` | `gp_score`/`sigma_score` semantics intentionally reset from repo proxy health scores to Indicator Dog raw subtotals. Negative `sigma_score` is allowed and means disqualification pressure; final score clamps at 0. | Keep `IDOG.UG.VALIDATION_SCORE`; `traffic_light` is score-based with Breakdown hard-RED. | State rows expose validation, GP, Sigma, and health context; tests compare score components to the document table. |
| `UG_NH60` | AUX STATE when current close is near or above prior 60-day high. | `implemented` | Direct match to GP new-high concept. | Emit `IDOG.UG.GP_NH60`. | New-high context emits aux row. |
| `UG_VOL2X` | AUX STATE when RVOL20 >= 2.0. | `implemented` | Direct match to 2x volume concept. | Emit `IDOG.UG.GP_VOL2X`. | 2x volume emits aux row and supports squeeze breakout reason. |
| `UG_W` | AUX STATE when double-bottom detector is active. | `proxy-backed` | W pattern is detected by repo heuristic. | Keep heuristic and document as proxy. | W detector emits aux row without opening a cycle. |
| `UG_VCP`, `UG_SQUEEZE`, `UG_TIGHT` | AUX STATE from compression, Bollinger width, ATR, tightness, and recent squeeze context. | `proxy-backed` | Matches concept through volatility/compression proxies, not full visual pattern semantics. | Keep as state context with squeeze/VCP provenance rule IDs. | Squeeze/VCP/tight rows stay state-only until BO trigger. |
| `UG_BUY_BREAKOUT` | Requires score-based Green, NH60, `breakout_ready`, and no EMA turn-down. | `implemented` | BUY is not created by Green alone; a concrete Sigma BO event is required. | Keep `IDOG.UG.STATE_TRAFFIC_LIGHT`, `IDOG.UG.GP_NH60`, `IDOG.UG.SIGMA_BO`, and `cycle_effect=OPEN`. | Breakout BUY requires Green + NH + BO. |
| `UG_BUY_SQUEEZE_BREAKOUT` | Same as UG breakout plus recent Orange context, recent squeeze context, and RVOL20 >= 2.0. | `proxy-backed` | Squeeze/VCP context remains a repo heuristic, but BUY still requires Green + concrete BO. | Keep squeeze/VOL rule IDs, `cycle_effect=OPEN`, and validation-score provenance. | Squeeze breakout requires squeeze context and Vol2x confirmation. |
| `UG_BUY_PBB` | Requires `ug_pbb_ready`, pullback profile pass, no EMA turn-down; BUY artifact is allowed only in Green. Orange remains WATCH. Stop/support use BB lower to BB mid. | `implemented` | Pullback quality remains a repo proxy, but Green/Orange BUY distinction is now explicit to reduce false BUYs. | Keep `IDOG.UG.SIGMA_PBB`, `cycle_effect=OPEN` only when action is BUY, and reference target/exit rule IDs. | PBB is BUY in Green and WATCH in Orange. |
| `UG_BUY_MR_LONG` | Requires `ug_mr_long_ready`, bullish alignment, no EMA turn-down; MR long is lower-band proximity, RSI <= 40, positive reversal, strong close. Red state cannot become aggressive BUY. | `proxy-backed` | Sigma MR Long is a tactical mean-reversion concept; bullish-alignment gate remains more restrictive than pure oversold bounce. | Keep as conservative short-swing proxy with downgraded conviction. | MR Long remains short-swing entry/watch with downgraded conviction. |
| `UG_COMBO_TREND`, `UG_COMBO_PULLBACK`, `UG_COMBO_SQUEEZE` | Strategy combo state rows combine UG state/GP/Sigma contexts. | `proxy-backed` | Combo labels are repo presentation helpers, not direct Indicator Dog source rules. | Keep as derived state, not cycle mutation. | Combo rows never open/close cycles. |

## Market Filter Contract

| Current policy | Indicator Dog intent | Fidelity | Proposed handling | Test target |
| --- | --- | --- | --- | --- |
| Weak market states `RISK_OFF`, `WEAK`, `BEARISH`, and `RED` keep BUY rows visible while warning/downgrading conviction/sizing; `MARKET_WEAK` is a warning/reason code, not the market state itself. | Trend market filter document says weak/bearish market should block or strongly constrain new buys. | `intentional-divergence` | Preserve user-selected warning-only policy. Emit `IDOG.TF.MARKET_FILTER` plus repo policy reason such as `MARKET_FILTER_WARNING_ONLY` where applicable. | Weak-market BUY remains in artifacts with warning summary, conviction reason, and downgraded sizing. |

## Implementation Candidates After Acceptance

These are the accepted implementation items for the 2026-04-21 fidelity batch.

### Correctness Gaps

- Add lifecycle provenance fields:
  - `cycle_effect`
  - `position_units_before`
  - `position_units_after`
  - `position_delta_units`
  - `indicator_dog_rule_ids`
- Make Trend TP1/TP2 actual trims:
  - TP1 and TP2 each reduce current `current_position_units` by 50%.
  - Cycle remains open until final close signal.
  - Rows should be clearly distinguishable from final exits.
- Enforce and test non-decreasing Trend trailing/protected stops across active cycle refresh.
- Preserve same-day emission detail while making persisted cycle state deterministic:
  final close wins over same-day trim for open-cycle persistence.
- Add persisted open-cycle restore tests for `all` and `screened` scopes independently.
- Implement `IDOG.UG.VALIDATION_SCORE` from `docs/archive/raw-sources/Reference/indicator-dog/Ultimate growth/3-1. validation.md` exactly:
  - Vol +30
  - W +25
  - NH +15
  - PBB +15
  - BO +10
  - MR +5
  - Breakdown -20
- Use score-based UG state while preventing automatic BUY:
  - Green is state readiness, not a BUY by itself.
  - `UG_BUY_*` requires the family-specific entry event.
  - `UG_BUY_PBB` is BUY only in Green and WATCH in Orange.

### Semantic/Calibration Deferred

- Do not silently change pyramiding tranche sizes in the lifecycle batch.
- Do not promote `TF_AGGRESSIVE_ALERT` to BUY without an explicit feature flag and separate tests.
- Do not move market-regime ownership into this repo.

## Batch Acceptance Criteria

- Existing public artifact filenames remain unchanged.
- Existing CSV/JSON contracts remain additive-compatible.
- Today-only buy/sell artifact behavior remains unchanged.
- `all` and `screened` open cycles remain independently scoped.
- `docs/archive/raw-sources/Reference/indicator-dog/` remains unchanged.
- Focused verification target remains:
  `.\.venv\Scripts\python -m pytest tests\test_signal_engine_restoration.py tests\test_signals_package.py -q`
- Full verification target remains:
  `.\.venv\Scripts\python -m pytest -q`
