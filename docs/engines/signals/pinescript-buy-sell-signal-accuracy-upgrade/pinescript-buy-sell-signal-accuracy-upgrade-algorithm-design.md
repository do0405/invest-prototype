# PineScript Buy/Sell Signal Accuracy Upgrade Algorithm Design

- status: reference-to-algorithm design
- as_of: 2026-04-22
- scope: documentation only; no trigger replacement in this pass

## Design Goal

This document maps every current functional signal part to Pine-first references,
official/original validation sources, future adopted formulas, repaint risk, tests, and
replay metrics. It is the implementation decision layer for a future trigger replacement batch.

## Current Signal Architecture

```text
OHLCV + metadata + source registry + market context
  -> _build_metrics()
  -> Trend / UG / PEG event and state builders
  -> _update_cycles()
  -> internal diagnostics
  -> today-only public buy/sell projection
```

Current high-risk concentration is `_build_metrics()`, where many pattern booleans are
computed inline. Future implementation should extract these calculations only after tests are
written.

## Source Register

| Reference ID | Source | URL | Grade | Use |
| --- | --- | --- | --- | --- |
| `TV.TECHNICAL_RATINGS` | TradingView Technical Ratings | https://www.tradingview.com/support/solutions/43000614331/ | `A` | Final ensemble scoring reference. |
| `TV.TECHNICAL_RATING_LIB` | TradingView TechnicalRating Pine library | https://www.tradingview.com/script/jDWyb5PG-TechnicalRating/ | `A` | MA/oscillator ensemble structure. |
| `TV.REPAINTING` | TradingView Pine repainting docs | https://www.tradingview.com/pine-script-docs/concepts/repainting/ | `A` | Non-repaint and confirmed-bar rules. |
| `TV.BAR_STATES` | TradingView Pine bar states docs | https://www.tradingview.com/pine-script-docs/v5/concepts/bar-states/ | `A` | `barstate.isconfirmed` equivalent policy. |
| `PINE.VCP_PP` | VCP with Pocket Pivots | https://www.tradingview.com/script/filyLJ1l-VCP-Pattern-with-Pocket-Pivots-by-Mark-Minervini/ | `A` | VCP + pocket pivot concepts. |
| `PINE.VCP_AMPHIBIAN` | Volatility Contraction Pattern | https://www.tradingview.com/script/J1tqSCqR-Volatility-Contraction-Pattern/ | `B` | Detailed VCP leg and breakout description. |
| `PINE.MINERVINI_TEMPLATE` | Minervini Trend Template Pine Screener Safe | https://www.tradingview.com/script/3Dp2SnUT-Minervini-Trend-Template-Pine-Screener-Safe/ | `A` | MA stack and 52w high/low trend context. |
| `PINE.WEINSTEIN` | Improved Weinstein Stage Analysis | https://www.tradingview.com/script/R3y6mh9a-Improved-Weinstein-Stage-Analysis/ | `A` | Stage/MA slope/volume/RS context. |
| `PINE.RS` | Relative Strength | https://www.tradingview.com/script/A4WyMCKM-Relative-Strength/ | `A` | Benchmark-relative strength context. |
| `PINE.DOUBLE_PATTERNS` | DoublePatterns Pine library | https://www.tradingview.com/script/3ZJsdqnl-DoublePatterns/ | `A` | W/double-bottom structure, quality, breakout. |
| `PINE.DOUBLE_QUALITY` | Double Top / Bottom Quality | https://www.tradingview.com/script/xzJbwxHc-Double-Top-Bottom-Quality-AGPro-Series/ | `A` | W quality score and lifecycle states. |
| `PINE.BB_Z_RSI` | Mean Reversion BB + Z-score + RSI + EMA200 | https://www.tradingview.com/script/XkjJJIJ2-Mean-Reversion-BB-Z-Score-RSI-EMA200-TP-at-Opposite-Z/ | `A` | PBB/PBS/MR candidate features. |
| `PINE.BB_RSI_MR` | Bollinger Bands Mean Reversion using RSI | https://www.tradingview.com/script/XRPeqEdA-Bollinger-Bands-Mean-Reversion-using-RSI-Krishna-Peri/ | `A` | Band entry and mid-band exit concepts. |
| `PINE.POCKET_PIVOTS` | Pocket Pivots | https://www.tradingview.com/script/Y8HdDerB-Pocket-Pivots/ | `A` | 10-day down-volume signature. |
| `PINE.POCKET_PIVOT_INDICATOR` | Pocket Pivot Indicator | https://www.tradingview.com/script/GrVbLlPy-Pocket-Pivot-Indicator/ | `A` | Pocket pivot with MA/base context. |
| `PINE.PRICE_VOLUME_BREAKOUT` | Price Volume Breakout | https://www.tradingview.com/script/6i904TTF-Price-Volume-Breakout/ | `A` | Breakout volume multiple. |
| `PINE.CONSOLIDATION_BREAKOUT` | Consolidation zones + Breakout | https://www.tradingview.com/script/K50esXWb/ | `A` | ATR-based consolidation zone and breakout. |
| `PINE.DONCHIAN_BREAKOUT` | Breakout Signals Donchian + EMA + Volume + ATR | https://www.tradingview.com/script/rEDxnKxn/ | `B` | Donchian breakout with trend, volume, ATR buffer. |
| `PINE.BGU` | Buyable Gap Ups Screener | https://www.tradingview.com/script/7r6Oc9fE-Buyable-Gap-Ups-BGU-Screener/ | `A` | Gap percentage, volume, ATR-based gap criteria. |
| `PINE.EARNINGS_GAP` | Earnings Gap Ups | https://www.tradingview.com/script/KWTJ9jeC-Earnings-Gap-Ups/ | `B` | PEG/monster gap concepts. |
| `PINE.CHANDELIER` | Chandelier Exit | https://www.tradingview.com/script/AqXxNS7j-Chandelier-Exit/ | `A` | ATR trailing stop reference. |
| `PINE.ATR_TRAIL` | ATR Trailing Stop | https://www.tradingview.com/script/p9DJbLgJ-ATR-Trailing-Stop/ | `A` | ATR-based long trailing stop. |
| `PINE.SETUP_SCORECARD` | Setup Quality Scorecard | https://www.tradingview.com/script/VLxfdJfT-Setup-Quality-Scorecard-AGPro-Series/ | `A` | Multi-factor setup quality score. |
| `PINE.COMPOSITE_SCORE` | Composite Buy/Sell Score | https://www.tradingview.com/script/DFgw9vm5-Composite-Buy-Sell-Score-100-to-100-by-LM/ | `A` | Momentum/oscillator composite score. |

## Method-To-Algorithm Matrix

| Functional part | Current formula / behavior | Pine reference | Official / original reference | Adopted formula candidate | Repaint risk | Decision | Test case | Replay metric |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| W / double bottom | `_detect_double_bottom()` compares left/right 60-bar lows within 8%; no neckline confirmation. | `PINE.DOUBLE_PATTERNS`, `PINE.DOUBLE_QUALITY` | Classical double-bottom pivot + neckline confirmation. | Detect two pivot lows within tolerance, require meaningful interim neckline high, track pending/confirmed/invalid states, score symmetry/depth/volume/breakout. | `pivot-lag`; confirmed only after neckline close. | `direct-replace-candidate` | Pending W emits state only; neckline close confirms `UG_W`; invalidation suppresses quality. | W-confirmed forward return and false W invalidation rate. |
| VCP | `compression_score >= 68`, `volume_dry`, `tight_active`. | `PINE.VCP_PP`, `PINE.VCP_AMPHIBIAN` | Minervini VCP: prior uptrend, 2-5 contractions, higher lows, shrinking depth, dry volume. | Score prior advance, contraction count, higher lows, decreasing pullback depth, dry volume, final tightness, pivot breakout level. | `pivot-lag`; no intrabar breakout. | `direct-replace-candidate` | VCP forming is STATE; breakout through pivot with volume can feed BUY context. | VCP breakout fakeout rate and 10/20d forward return. |
| Squeeze | BB width <= 12% and ATR% <= 2.5; recent squeeze/tight lookback. | LazyBear squeeze lineage, consolidation references | BB/KC squeeze concept and volatility compression. | Keep BB/ATR local proxy but add release state: compression active, release close above range, volume confirmation. | `confirmed-bar-required`. | `enhance-before-replace` | Squeeze without release is state; release + BO can become squeeze breakout. | Squeeze breakout duplicate rate and fakeout rate. |
| Tight range | ATR% <= 2.5 or compression_score >= 72. | `PINE.CONSOLIDATION_BREAKOUT` | Consolidation range vs ATR. | `range_high - range_low < ATR * multiplier` for minimum bars; expose tight duration and range boundaries. | `none-known` with closed bars. | `direct-replace-candidate` | 2-3 bars do not count; min duration counts as tight state. | Tight-state to breakout conversion rate. |
| Dry volume | Recent 10-day average volume below prior 20-day average proxy. | `PINE.POCKET_PIVOTS`, Simple Volume references | VCP dry-up / volume contraction. | Separate `dry_volume_score` from `pocket_pivot_volume`: dry-up uses recent volume percentile vs 50d average; pocket pivot uses up-day volume above highest down-volume in last 10 bars. | `none-known`. | `direct-replace-candidate` | Dry-up can support VCP/PBB; pocket pivot can support breakout. | Breakout quality with/without pocket pivot. |
| Pocket pivot | Not explicit; RVOL and Vol2x proxy institutional volume. | `PINE.POCKET_PIVOTS`, `PINE.POCKET_PIVOT_INDICATOR` | Kacher/Morales pocket pivot volume signature. | `close > open` and `volume > max(volume on down days over lookback)` with optional MA/base context. | `none-known`. | `direct-replace-candidate` | Pocket pivot emits volume quality but not standalone BUY. | Pocket pivot breakout 5/10d return and fakeout rate. |
| NH60 / near high | Close near prior 60d high; 52w proximity from feature row. | `PINE.MINERVINI_TEMPLATE` | Minervini trend template and 52w high proximity. | Keep `nh60`, add 52w high/low context score and separate from breakout trigger. | `none-known`. | `enhance-before-replace` | Near-high context alone cannot BUY. | Near-high context hit-rate by family. |
| Vol2x | RVOL20 >= 2.0. | Simple Volume / volume breakout references | Volume confirmation convention. | Keep threshold but expose `rvol_source`, `avg_volume_window`, and `vol2x_confirmed`. | `none-known`. | `reference-only` | Vol2x supports UG GP and squeeze breakout. | Signal quality split by RVOL bucket. |
| Trend template | Current trend uses close above 200MA, bullish MA alignment, MA gap, ADX, slopes. | `PINE.MINERVINI_TEMPLATE` | Minervini trend template. | Add `trend_template_score`: price > 50/150/200, 50 > 150 > 200, 200 rising, near 52w high, above 52w low. | `none-known`. | `enhance-before-replace` | Trend score downgrades weak setup but does not alone create BUY. | False-positive reduction by trend score. |
| Stage / RS | Some source/leader context exists; no Pine RS score in signals. | `PINE.WEINSTEIN`, `PINE.RS` | Weinstein stage, benchmark-relative strength. | Add optional `stage_rs_context`: MA slope, volume, RS line/rank. Keep market regime external. | `htf-risk` if weekly data; use local confirmed aggregation. | `enhance-before-replace` | RS context can improve conviction but not override trigger. | Forward return by RS context bucket. |
| Regular pullback | Prior expansion, 1-12% pullback depth, dry volume, rising support, EMA/BB zone touch, reversal confirmation. | EMA pullback and setup quality references | Trend pullback with action confirmation. | Score prior expansion, controlled depth, support touch/reclaim, dry-up, bullish close position/body, trend quality. Require score threshold. | `confirmed-bar-required`. | `direct-replace-candidate` | Pullback without reversal remains WATCH/state; confirmed reversal emits buy candidate. | Pullback MAE and stop-proxy hit rate. |
| Breakout | Donchian/prior high + BB upper clear + RVOL >= 1.5 + close/body strength + energy. | `PINE.PRICE_VOLUME_BREAKOUT`, `PINE.CONSOLIDATION_BREAKOUT`, `PINE.DONCHIAN_BREAKOUT` | Donchian breakout, consolidation breakout, volume confirmation. | Require structural level, close above level with ATR buffer, volume quality, close/body strength, optional consolidation/VCP context. | `confirmed-bar-required`. | `direct-replace-candidate` | Wick-only breakout fails; close+ATR buffer passes. | Breakout fakeout rate and 20d forward return. |
| Momentum chase | RSI >= 60, MACD positive, Donchian high, RVOL >= 1.2, strong candle, not risk heat. | `PINE.COMPOSITE_SCORE`, Technical Ratings | Momentum oscillator/MA ensemble. | Replace fixed single gate with momentum sub-score: RSI zone/delta, MACD histogram, Donchian pressure, volume, candle quality, risk heat penalty. | `confirmed-bar-required`. | `enhance-before-replace` | Momentum score below threshold suppresses chase; high risk heat downgrades. | Momentum end rate and forward return. |
| PEG / BGU | Event day alert, gap-low/R50 pullback, event-high rebreak. | `PINE.BGU`, `PINE.EARNINGS_GAP` | Buyable gap-up / Power Earnings Gap concepts. | Add PEG quality: gap percent or ATR gap, volume multiple, close range, low holds prior high/gap low, follow-up R50/rebreak. | `confirmed-bar-required`; earnings data stale risk. | `enhance-before-replace` | Event alert never opens cycle; follow-up trigger opens only on R50 reclaim or event-high rebreak. | PEG follow-up return and gap-fill failure rate. |
| PBB | UG PBB uses pullback profile pass and BB lower/mid support/reclaim. | `PINE.BB_Z_RSI`, `PINE.BB_RSI_MR` | Bollinger pullback/reversion. | Score BB%, z-score, RSI, band touch/reclaim, trend quality, close position. Green BUY, Orange WATCH remains. | `confirmed-bar-required`. | `direct-replace-candidate` | Green PBB BUY; Orange WATCH; Red suppressed. | PBB MAE and mid-band target hit rate. |
| PBS | High >= BB mid, close < BB mid, negative return. | `PINE.BB_Z_RSI`, mid-band failure descriptions | Bollinger mid-band failure / pullback sell. | Score mid/upper band touch, failed reclaim close, negative candle quality, RSI fade, z-score normalization, support risk. | `confirmed-bar-required`. | `direct-replace-candidate` | Mid-band wick touch without close rejection fails. | PBS exit avoided drawdown and false exit rate. |
| MR long | Close near lower BB, RSI <= 40, positive return, strong close. | `PINE.BB_Z_RSI`, `PINE.BB_RSI_MR` | Mean reversion from lower band with oscillator extreme. | Add BB%, z-score <= negative threshold, RSI oversold, close back inside band, optional ADX/chop/trend filter. | `confirmed-bar-required`. | `enhance-before-replace` | MR long downgraded conviction; never Red aggressive BUY. | MR long target hit and stop-proxy rate. |
| MR short | High >= upper BB and negative return or risk heat; trims max twice. | `PINE.BB_Z_RSI`, upper-band MR references | Mean reversion trim from upper band. | Add z-score/RSI overbought and close-back-inside or failed upper-band hold. Keep two-trim lifecycle. | `confirmed-bar-required`. | `enhance-before-replace` | Two trims max; third suppressed until PBS/breakdown. | Trim benefit vs missed upside. |
| Breakdown | Close <= support zone low closes cycle. | Donchian/channel/support references | Support failure / stop breach. | Keep close-confirmed support breach; add support origin and ATR buffer diagnostics. | `none-known`. | `enhance-before-replace` | Restored cycle breakdown closes deterministically. | Avoided drawdown after breakdown. |
| Resistance reject | High touches BB mid, close below BB mid, negative return; closes Trend cycle. | Failed reclaim / break-retest quality references | Pullback sell / failed reclaim proxy. | Score failed reclaim: touch/reclaim attempt, close rejection, candle body, volume, trend damage. | `confirmed-bar-required`. | `enhance-before-replace` | Channel-active rows do not emit resistance reject. | False exit rate and avoided drawdown. |
| Trailing / ATR stop | Family-specific MA/Donchian levels, never-down protected stop. | `PINE.CHANDELIER`, `PINE.ATR_TRAIL` | Chandelier/ATR trailing stop. | Add optional ATR/Chandelier candidate level and compare to current MA stop; keep never-down max with protected stop. | `none-known`. | `enhance-before-replace` | Recalculated lower stop cannot reduce persisted level. | Exit efficiency and giveback metric. |
| Channel break | Close <= 8-bar channel low closes cycle. | Consolidation/channel break references | Range floor failure. | Keep 8-bar channel as explicit repo parameter; add optional ATR-based channel width diagnostics. | `none-known`. | `reference-only` | Channel-only failure closes without support failure. | Avoided drawdown after channel break. |
| TP | R multiple TP1/TP2 trims 50% each and leaves runner. | Quality-Controlled Trend Strategy risk separation | R-multiple partial exit. | Keep current lifecycle. Future docs may tune R multiples by family after replay. | `none-known`. | `reference-only` | TP1/TP2 trim units and keep cycle open. | Trim benefit vs runner performance. |
| Pyramiding | Add to winners only; slots 0.50 then 0.30; protection improved. | Minervini/SEPA add-to-winner concepts | Add only to winner with risk controlled. | Keep current add-on lifecycle; add setup quality score requirement before add-on if replay supports it. | `none-known`. | `enhance-before-replace` | Cannot average down; add-on improves protected stop. | Add-on incremental return and MAE. |
| UG validation | GP/Sigma point table: Vol +30, W +25, NH +15, PBB +15, BO +10, MR +5, Breakdown -20. | Indicator Dog current contract; quality score references | UG validation score. | Keep point table. Feed improved W/PBB/BO/MR/breakdown truth into same score after tests. | depends on component. | `enhance-before-replace` | Score components match documented table. | State-to-entry conversion quality. |
| Final decision score | Trend conviction and UG dashboard exist; no unified final score. | `TV.TECHNICAL_RATINGS`, `TV.TECHNICAL_RATING_LIB`, `PINE.SETUP_SCORECARD`, `PINE.COMPOSITE_SCORE` | Ensemble scoring and setup-quality score. | Add additive score: hard gates + weighted trend/pattern/volume/momentum/risk/source/market components. Do not replace existing output ordering until replay acceptance. | component dependent. | `enhance-before-replace` | Score is bounded and does not create BUY without signal event. | Family-level precision and ranking lift. |
| Source fit | `_family_source_profile()` maps source style to family fit. | Setup quality scorecard as confluence model | Source/strategy fit. | Keep current family fit; optionally add `reference_confidence` and `decision_rule_ids`. | `none-known`. | `reference-only` | Low source fit can WATCH but not silently delete diagnostics. | Signal quality by source fit bucket. |
| Market overlay | Weak market warns/downgrades sizing; no hard suppress. | TradingView ratings are context, not regime owner | Repo policy from current contract. | Preserve warning-only policy. Future score can subtract market component but not hide BUY solely due weak market. | external data freshness risk. | `reference-only` | Weak-market BUY remains visible with warning. | Weak-market performance split. |
| Cycle effect | `_update_cycles()` records OPEN/ADD/TRIM/CLOSE/STATE and position units. | Risk-management strategy references | Lifecycle accounting. | Preserve exactly; pattern changes only alter event emission, not lifecycle semantics. | `none-known`. | `reference-only` | Same-day trim+close persists closed; rows still explain conditions. | Lifecycle correctness, not alpha metric. |

## Final Decision Score Draft

The score is additive and diagnostic first.

```text
hard_gate_fail =
    missing_ohlcv
    or stale_as_of
    or invalid_price
    or disallowed_source_scope

pattern_component =
    max(w_quality, vcp_quality, pullback_quality, breakout_quality, band_reversion_quality)

trend_component =
    ma_stack_score
  + ma_slope_score
  + rs_context_score
  - ema_turn_down_penalty

volume_component =
    rvol_score
  + pocket_pivot_score
  + dry_volume_context_score

momentum_component =
    rsi_context
  + macd_context
  + donchian_pressure
  - risk_heat_penalty

exit_pressure_component =
    pbs_score
  + breakdown_score
  + resistance_reject_score
  + trailing_break_score

decision_score =
    0 if hard_gate_fail else clamp(
        0.25 * trend_component
      + 0.25 * pattern_component
      + 0.20 * volume_component
      + 0.15 * momentum_component
      + 0.10 * source_fit_component
      + 0.05 * market_context_component
      - exit_pressure_component,
      0,
      100
    )
```

Draft grades:

| Grade | Range | Meaning |
| --- | --- | --- |
| `S` | `>= 90` | High quality and low conflict. |
| `A` | `>= 80` | Strong setup. |
| `B` | `>= 68` | Valid but with imperfections. |
| `C` | `>= 50` | Watch/diagnostic quality. |
| `D` | `< 50` | Low quality or high conflict. |

## Replacement Order

1. W/double-bottom helper and tests.
2. VCP/consolidation/squeeze/tight helper and tests.
3. Volume quality and pocket pivot helper.
4. Pullback and breakout quality scores.
5. PBB/PBS/MR band reversion scores.
6. Trailing/ATR stop candidate diagnostics.
7. Additive final decision score.
8. Local replay evaluator.
9. Trigger replacement only after replay acceptance.

## Rejected Or Deferred Ideas

| Idea | Reason | Decision |
| --- | --- | --- |
| Copying Pine source code directly | House Rules/copyright and maintainability risk. | `reject` |
| Using intraday projected volume in daily screening | Partial-bar and reproducibility risk. | `defer` |
| Hard blocking BUY on weak market | Current repo policy intentionally warning-only. | `reject` unless user changes policy |
| Pivot pattern that plots in the past without confirmation | Backpainting/future-leak risk. | `reject` |
| Protected script as exact formula source | Not auditable. | `reference-only` or `enhance-before-replace` |

## Acceptance Checks

- Every PRD functional part appears in the matrix.
- Every row has a decision and a replay metric.
- Every direct-replace candidate has a deterministic test case.
- Repaint risk is explicit for pivot, HTF, and confirmed-bar sensitive methods.
- Cycle lifecycle and public artifact contracts are preserved.

