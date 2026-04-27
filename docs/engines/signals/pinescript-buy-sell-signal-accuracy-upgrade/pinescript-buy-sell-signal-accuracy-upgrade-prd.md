# PineScript Buy/Sell Signal Accuracy Upgrade PRD

- status: documentation-only planning contract
- as_of: 2026-04-22
- scope: `screeners/signals` buy/sell signal semantics; no runtime code change in this pass

## Purpose

현재 buy/sell signal engine은 Trend Following, Ultimate Growth, PEG follow-up,
cycle lifecycle, public artifact projection까지 동작한다. 그러나 일부 기능 파트는
Indicator Dog 문서나 일반 트레이딩 개념을 repo-local heuristic으로 proxy한 상태다.

이 문서 묶음의 목적은 트리거를 바로 바꾸기 전에 모든 기능 파트를 Pine Script
reference와 공식/원전 자료 기준으로 재검토하고, 이후 구현자가 어떤 조건식을
채택하거나 버릴지 결정할 수 있는 근거를 고정하는 것이다.

## Product Thesis

정확도 개선은 단일 "좋은 Pine script"를 복사하는 작업이 아니다. 각 신호는 다음
세 층을 분리해서 보완해야 한다.

- Pattern truth: W, VCP, squeeze, pullback, breakout, PBS 같은 구조가 실제로 성립했는가.
- Decision truth: 그 구조가 BUY, WATCH, TRIM, EXIT 중 무엇을 의미하는가.
- Lifecycle truth: 이미 열린 cycle, trailing, TP, add-on, weak-market context와 어떻게 결합되는가.

따라서 이번 문서화는 Pine-first이지만 Pine-only는 아니다. 공개 Pine 조건식을 우선
수집하고, TradingView 공식 문서와 Minervini, Weinstein, Bollinger, Donchian,
ATR/Chandelier, Pocket Pivot 같은 원전성 기준으로 신뢰도를 판정한다.

## Current Runtime Boundary

주요 현재 구현 표면은 다음과 같다.

| Area | Current owner | Notes |
| --- | --- | --- |
| Metric construction | `screeners/signals/engine.py::_build_metrics()` | EMA/SMA/ADX/RSI/MACD/BB/Donchian/ATR, VCP, squeeze, pullback, breakout, UG band states를 계산한다. |
| Trend buy events | `screeners/signals/engine.py::_trend_buy_events()` | Regular pullback, breakout, momentum, PEG pullback/rebreak, add-on 후보를 만든다. |
| Trend sell events | `screeners/signals/engine.py::_trend_sell_events()` | Support breakdown, channel break, trailing break, TP1/TP2, momentum end, resistance reject를 만든다. |
| UG state/buy/sell | `screeners/signals/engine.py::_ug_state_rows()`, `_ug_buy_events()`, `_ug_sell_events()` | UG validation score, traffic light, BO/PBB/MR long/PBS/MR short/breakdown을 담당한다. |
| Cycle lifecycle | `screeners/signals/engine.py::_update_cycles()` | OPEN, ADD, TRIM, CLOSE, position units, trailing/protected stop persistence를 담당한다. |
| Public outputs | `screeners/signals/writers.py` | `buy_signals_*`, `sell_signals_*` today-only public artifacts를 유지한다. |

## Goals

- 현재 엔진의 모든 기능 파트를 reference matrix에 포함한다.
- 각 파트마다 현재 산식, Pine reference, 공식/원전 reference, 채택 후보 산식,
  repaint risk, 구현 결정, 테스트 후보, replay metric을 고정한다.
- 직접 교체 전 검증 기준을 명확히 한다.
- 이후 구현이 public artifact 계약과 cycle lifecycle을 깨지 않도록 제한을 명시한다.

## Non-Goals

- 이번 배치에서 buy/sell trigger를 바로 교체하지 않는다.
- live TradingView, GitHub, broker, data-provider network call을 screening runtime이나 tests에 추가하지 않는다.
- market-intel regime 산식을 이 repo로 가져오지 않는다.
- `docs/archive/raw-sources/Reference/indicator-dog/`, `data/`, `results/`, `docs/archive/raw-sources/PRD/`를 수정하지 않는다.
- 공개 Pine Script 코드를 그대로 복사하지 않는다. 독립 구현 가능한 조건식과 개념만 문서화한다.

## Functional Parts To Cover

| Functional part | Current signal impact | Documentation requirement |
| --- | --- | --- |
| W / double bottom | `UG_W`, `gp_score`, UG state quality | Pivot, neckline, confirmation, invalidation, quality score 기준 필요. |
| VCP | `TF_VCP_ACTIVE`, `UG_VCP`, build-up, breakout context | Prior uptrend, 2-5 contraction legs, higher lows, shrinking depth, dry volume, pivot breakout 기준 필요. |
| Squeeze | `UG_SQUEEZE`, `UG_BUY_SQUEEZE_BREAKOUT` | BB/KC 또는 BB width/ATR compression 기준과 release 기준 필요. |
| Tight range | `UG_TIGHT`, setup/build-up | ATR%, range compression, consolidation duration 기준 필요. |
| Dry volume / pocket pivot | VCP, pullback, breakout energy, Vol2x | Dry-up과 institutional volume signature를 분리해야 한다. |
| NH60 / near high | UG GP and breakout gating | 60d/52w high proximity와 breakout trigger를 분리해야 한다. |
| Trend template / stage / RS | Trend conviction and quality flags | MA stack, 200MA slope, RS line/rank를 final decision context로 검토한다. |
| Regular pullback | `TF_BUY_REGULAR` | Prior expansion, EMA/BB zone touch, dry volume, reversal confirmation을 재검토한다. |
| Breakout | `TF_BUY_BREAKOUT`, `UG_BUY_BREAKOUT` | Donchian/pivot/consolidation breakout, ATR buffer, RVOL, close/body strength를 재검토한다. |
| Momentum chase | `TF_BUY_MOMENTUM`, `TF_SELL_MOMENTUM_END` | RSI/MACD/Donchian/volume entry와 fade exit threshold를 재검토한다. |
| PEG / BGU | `TF_PEG_EVENT`, `TF_BUY_PEG_PULLBACK`, `TF_BUY_PEG_REBREAK` | Gap size, volume, close range, gap-low/R50/event-high follow-up 기준을 보완한다. |
| PBB / PBS | `UG_BUY_PBB`, `UG_SELL_PBS` | Bollinger position, z-score, RSI, band reclaim/reject, mid-band failure 기준을 재검토한다. |
| MR long / MR short | `UG_BUY_MR_LONG`, `UG_SELL_MR_SHORT` | Mean-reversion entry/trim을 trend/chop regime and risk heat와 분리한다. |
| Breakdown / resistance reject | `TF_SELL_BREAKDOWN`, `UG_SELL_BREAKDOWN`, `TF_SELL_RESISTANCE_REJECT` | Support origin, close confirmation, failed reclaim quality를 보완한다. |
| Trailing / ATR stop | `TF_SELL_TRAILING_BREAK`, active level rows | Chandelier/ATR stop과 current never-down invariant를 비교한다. |
| TP / pyramiding | `TF_SELL_TP1`, `TF_SELL_TP2`, `TF_ADDON_PYRAMID` | R multiple, trim units, add-to-winner protection 기준을 유지/보완한다. |
| UG validation | `UG_STATE_GREEN/ORANGE/RED` | 기존 Indicator Dog point table은 유지하되 pattern truth 개선 영향을 검토한다. |
| Final decision score | public BUY/SELL ranking and diagnostics | Technical Ratings-style ensemble and setup quality score를 additive로 설계한다. |

## Reference Policy

| Grade | Meaning | Implementation use |
| --- | --- | --- |
| `A` | TradingView official docs/library, or open-source Pine with clear source and auditable conditions. | 구현 후보. |
| `B` | Protected/closed source but description is specific and aligned with official/original references. | 보조 구현 후보. Formula는 원전 기준으로 재작성한다. |
| `C` | Useful idea but weak implementation detail, insufficient source, or narrow market fit. | 참고만. |
| `D` | Repaint/future leak/overfit/unclear logic risk. | 채택하지 않고 rejected rationale에 남긴다. |

## Source Families

| Source family | Examples | Role |
| --- | --- | --- |
| TradingView official | Pine repainting docs, bar states docs, Technical Ratings docs/library | Non-repaint, confirmed-bar, ensemble score 기준. |
| VCP / Minervini | VCP with Pocket Pivots, VCP detector, Minervini Trend Template | VCP and trend-template validation criteria. |
| Weinstein / RS | Weinstein Stage Analysis, Relative Strength scripts | Stage/RS context, not direct market-regime ownership. |
| Chart pattern quality | DoublePatterns library, Double Top/Bottom Quality | W/double-bottom structure and quality scoring. |
| Mean reversion | BB + z-score + RSI + EMA200, BB RSI mean reversion | PBB/PBS/MR long/short formula candidates. |
| Volume and pocket pivot | Pocket Pivots, Simple Volume, Price Volume Breakout | Dry volume, Vol2x, pocket pivot, breakout volume confirmation. |
| Stop / risk | Chandelier Exit, ATR trailing stop, Quality-Controlled Trend Strategy | Trailing stop, confirmed-bar, risk separation criteria. |
| Final score | Technical Ratings, Setup Quality Scorecard, Composite Buy/Sell Score | Additive decision score design. |

## Product Acceptance Criteria

- Five documents exist under `docs/engines/signals/pinescript-buy-sell-signal-accuracy-upgrade/`.
- The documents use the same `Reference Policy`, source names, and field candidates.
- Every functional part in this PRD appears in the algorithm design matrix.
- Each functional part has one decision: `direct-replace-candidate`, `enhance-before-replace`,
  `reference-only`, or `defer`.
- Runtime code, public artifact names, today-only behavior, weak-market warning-only policy, and
  cycle lifecycle semantics are explicitly preserved.

## Future Implementation Acceptance Criteria

- Focused tests pass:

```powershell
.\.venv\Scripts\python -m pytest tests\test_signal_engine_restoration.py tests\test_signals_package.py -q
```

- Replay comparison reports old-vs-new signal count, duplicate rate, forward returns over 5/10/20
  trading days, max adverse excursion, and fakeout rate.
- A direct trigger replacement is accepted only if replay quality is not worse, or if lower signal
  count is explained by measurable false-positive reduction.

## Sources

- TradingView Technical Ratings: https://www.tradingview.com/support/solutions/43000614331/
- TradingView TechnicalRating Pine library: https://www.tradingview.com/script/jDWyb5PG-TechnicalRating/
- TradingView Pine repainting docs: https://www.tradingview.com/pine-script-docs/concepts/repainting/
- TradingView Pine bar states docs: https://www.tradingview.com/pine-script-docs/v5/concepts/bar-states/
- VCP with Pocket Pivots: https://www.tradingview.com/script/filyLJ1l-VCP-Pattern-with-Pocket-Pivots-by-Mark-Minervini/
- Volatility Contraction Pattern: https://www.tradingview.com/script/J1tqSCqR-Volatility-Contraction-Pattern/
- Minervini Trend Template Pine Screener Safe: https://www.tradingview.com/script/3Dp2SnUT-Minervini-Trend-Template-Pine-Screener-Safe/
- Improved Weinstein Stage Analysis: https://www.tradingview.com/script/R3y6mh9a-Improved-Weinstein-Stage-Analysis/
- Relative Strength: https://www.tradingview.com/script/A4WyMCKM-Relative-Strength/
- DoublePatterns library: https://www.tradingview.com/script/3ZJsdqnl-DoublePatterns/
- Double Top / Bottom Quality: https://www.tradingview.com/script/xzJbwxHc-Double-Top-Bottom-Quality-AGPro-Series/
- Mean Reversion BB/Z-score/RSI/EMA200: https://www.tradingview.com/script/XkjJJIJ2-Mean-Reversion-BB-Z-Score-RSI-EMA200-TP-at-Opposite-Z/
- Pocket Pivots: https://www.tradingview.com/script/Y8HdDerB-Pocket-Pivots/
- Pocket Pivot Indicator: https://www.tradingview.com/script/GrVbLlPy-Pocket-Pivot-Indicator/
- Chandelier Exit: https://www.tradingview.com/script/AqXxNS7j-Chandelier-Exit/
- Setup Quality Scorecard: https://www.tradingview.com/script/VLxfdJfT-Setup-Quality-Scorecard-AGPro-Series/

