# Signal Algorithm Audit - Consolidated

Date: `2026-04-26`

Consolidates:
- `docs/audits/2026-04-26-signal-algorithm-audit.md`
- `docs/audits/2026-04-26-signal-algorithm-audit-v2.md`
- `docs/audits/2026-04-26-signal-output-segmentation-amendment.md`

Prompt applied in order:
- `docs/audits/codex-signal-audit-prompt.md`
  1. False Positive 탐색
  2. Signal Scarcity 탐색
  3. 수치 안정성 및 데이터 경계 조건
  4. 운영 상태 관리 및 사이클 로직
  5. 스크리너별 로직 검증
  6. 종합 알고리즘 강건성 점검

## 0. Audit Baseline

이 문서는 코드 변경 계획이 아니라, 신호/패턴 알고리즘에서 위양성, 과잉 신호, 과소 신호, 운영 상태 불일치가 발생할 수 있는 지점을 한 곳에 모은 감사 기준 문서다.

현재 검증 기준:
- Market: `US`
- Horizon: 일봉/주봉 기반 swing screening
- Universe: local OHLCV-backed universe
- Output snapshot: `results/us/signals/multi_screener`, `as_of_date=2026-04-22`
- Market truth: `local_standalone`, `core_overlay_applied=false`
- Regime assumption: 코드 감사는 특정 강세/약세장 판단을 전제로 하지 않는다. 단, 현재 산출물 해석은 로컬 standalone 결과에 한정한다.

Repository ownership:
- 이 repo는 OHLCV collection, screening runtime, signal projection을 소유한다.
- 시장 regime, breadth, rotation의 durable truth는 market-intel layer 책임이다.
- 따라서 regime 자체를 바꾸는 로직은 이 repo에 넣지 않고, 이 repo에서는 입력 필드의 사용 방식과 output labeling만 검증한다.

## 1. Output Contract And Segmentation

가장 중요한 해석 보정:

- `buy_signals_all_symbols_v1`은 추천 전용 파일이 아니다. 전체 OHLCV-backed universe에 대한 today-only BUY 탐색 출력이다.
- `buy_signals_screened_symbols_v1`은 그중 source/screener로 걸러진 subset이다.
- `all + is_screened=false + source_buy_eligible=false + source_fit_label=NONE` row는 즉시 "추천 위양성"으로 보지 않고 `discovery_only` universe discovery 후보로 분류한다.
- 추천/우선 검토 surface는 `buy_signals_screened_symbols_v1`, 또는 `buy_signals_all_symbols_v1`에서 `is_screened`, `source_buy_eligible`, `source_fit_label`, `source_disposition`으로 필터링한 view다.

Implemented additive fields:
- Public buy/sell projection row에 `source_disposition`을 추가했다.
- 값 의미:
  - `buy_eligible`: source/screener 또는 PEG context가 BUY eligibility를 지지.
  - `watch_only`: source context는 있으나 BUY eligible은 아님.
  - `discovery_only`: all-scope universe scan에서 source/screener eligibility 없이 나온 탐색 후보.

Implemented summary:
- `signal_summary.json`에 `buy_signal_segments`를 추가했다.
- 포함 필드:
  - `all_total`
  - `screened_total`
  - `all_only_discovery_total`
  - `all_source_disposition_counts`
  - `all_source_fit_label_counts`
  - `screened_source_fit_label_counts`
  - `all_signal_code_counts`
  - `screened_signal_code_counts`

Current observed output counts from `2026-04-22` snapshot:

| Surface | Rows | Main observation |
| --- | ---: | --- |
| `buy_signals_all_symbols_v1` | 109 | `TF_BUY_MOMENTUM` 91, `source_fit_label=NONE` 32 |
| `buy_signals_screened_symbols_v1` | 77 | `TF_BUY_MOMENTUM` 63, all `is_screened=true` |
| `all_signals_v2` | 39,832 | `UG_STATE_RED` 13,586, `UG_W` 7,074 |
| `open_family_cycles` | 190 | `TF_MOMENTUM` cycles 158 in prior inspection |

Segmentation-aware interpretation:
- The 32 all-only/no-source BUY rows are discovery output, not contract violations.
- Over-signal risk should still be evaluated separately for `screened` and `discovery_only`.
- The screened surface still shows momentum concentration and fakeout warnings, so the risk is not limited to all-only discovery rows.

## 2. System Overview

Main signal engines:

| Engine | Families | Main BUY codes |
| --- | --- | --- |
| TREND (TF) | `TF_REGULAR_PULLBACK`, `TF_BREAKOUT`, `TF_PEG`, `TF_MOMENTUM` | `TF_BUY_REGULAR`, `TF_BUY_BREAKOUT`, `TF_BUY_MOMENTUM`, `TF_ADDON_PYRAMID` |
| ULTIMATE GROWTH (UG) | `UG_BREAKOUT`, `UG_PULLBACK`, `UG_MEAN_REVERSION` | `UG_BUY_BREAKOUT`, `UG_BUY_SQUEEZE_BREAKOUT`, `UG_BUY_PBB`, `UG_BUY_MR_LONG` |

Runtime flow:

```text
source_registry -> peg_screen -> active_cycles -> financial_map
    -> OHLCV frames -> feature_map -> metrics_map (_build_metrics per symbol)
        -> _run_scope_scan("all" / "screened")
            -> state rows + buy candidate rows + sell rows
                -> public projections + internal diagnostics
                -> write_signal_outputs()
```

Scope contract:
- `all` and `screened` scopes are scanned independently.
- Public today-only files keep stable names.
- Internal diagnostics such as `all_signals_v2`, family event/state files, diagnostics, history, and open cycles remain traceability surfaces, not the public recommendation contract.

## 3. Section 1 Applied - False Positive And Over-Signal Risks

### A-1. RSI `None` bypass in mean reversion gates

Location:
- `screeners/signals/patterns.py:478`
- `screeners/signals/patterns.py:498`

Risk:
- `mr_long_ready` passes when RSI is missing because the condition allows `rsi is None or rsi <= 40.0`.
- `mr_short_ready` has the same bypass pattern with `rsi is None or rsi >= 65.0 ...`.
- Missing/insufficient RSI data can become a pass condition instead of an unknown/blocked condition.

Recommended fix:
- Treat RSI `None` as not-ready for MR_LONG/MR_SHORT, unless an explicit fallback value is documented and tested.
- Add regression tests where Bollinger conditions pass but RSI is `None`; expected result: no MR signal.

### A-2. VCP contraction allows worsening depth

Location:
- `screeners/signals/patterns.py:222`

Risk:
- `shrinking_depth = depths[i] <= depths[i - 1] * 1.05` allows up to 5% deeper contraction and still labels it shrinking.
- Example: `15.0% -> 15.7% -> 16.4%` can pass even though contraction depth is worsening.

Recommended fix:
- Change tolerance to `1.00` or at most `1.02`.
- Add fixture with slightly increasing contraction depths; expected result: VCP inactive.

### A-3. W pattern uses half-window global minima instead of local swing lows

Location:
- `screeners/signals/patterns.py:145`

Risk:
- The detector splits the last 80 bars into left/right halves and uses `idxmin()` in each half.
- This can identify arbitrary half-window lows rather than two local swing lows.
- Current `all_signals_v2` evidence is concerning: `UG_W` appears 7,074 times in the current US snapshot, larger than a normally selective W-pattern state should be.

Recommended fix:
- Detect local lows using swing windows.
- Require second low timing separation, neckline between lows, neckline reclaim, and volume/close confirmation.
- Report `UG_W` separately for `screened` and `discovery_only` after the fix.

### A-4. PBS sell fade threshold is too weak

Location:
- `screeners/signals/patterns.py:462`

Risk:
- `rsi_fade = rsi <= 50.0` treats neutral RSI as bearish fade confirmation.
- Combined with failed mid/upper band logic, this may overproduce PBS sell/trim signals in sideways conditions.

Recommended fix:
- Use stricter threshold such as `rsi <= 45.0`, or require RSI slope/failed reclaim confirmation.
- Add test where RSI is 49-50 but close context is ambiguous; expected result: no PBS sell.

### A-5. Squeeze breakout context contains redundant or weakly discriminating conditions

Location:
- `screeners/signals/engine.py:6107`

Risk:
- `squeeze_breakout_condition` requires both `recent_orange_context` and `recent_squeeze_context`.
- Prior audit noted that one context can already imply the other depending on upstream construction, so the additional condition may add little filtering while making the logic harder to reason about.

Recommended fix:
- Document the intended set relationship between orange and squeeze context.
- If one is a superset, remove the redundant gate or replace it with a materially different confirmation such as volatility expansion or close-through-pivot behavior.

### A-6. PBB/MR_LONG refined band gate is conditional on diagnostics presence

Location:
- `screeners/signals/engine.py:6114`
- `screeners/signals/engine.py:6356`

Risk:
- When refined band diagnostics are present, PBB/MR gates require refined confirmation.
- When diagnostics are absent, legacy `ug_pbb_ready` or `ug_mr_long_ready` can pass.
- This creates two quality regimes depending on feature availability.

Recommended fix:
- Decide whether refined band scoring is mandatory for public BUY.
- If mandatory, missing diagnostics should block or downgrade to WATCH.
- If optional, output should include a quality flag such as `BAND_REVERSION_LEGACY_GATE`.

### A-7. `TF_BUY_MOMENTUM` dominates BUY output and fakeout warnings are not blocking

Evidence:
- Current `buy_signals_all_symbols_v1`: `TF_BUY_MOMENTUM` 91/109.
- Current `buy_signals_screened_symbols_v1`: `TF_BUY_MOMENTUM` 63/77.
- Current `buy_all` rows with `BREAKOUT_FAKEOUT_RISK`: 54.
- Current `buy_screened` rows with `BREAKOUT_FAKEOUT_RISK`: 37.

Risk:
- Momentum BUY is not only a discovery-scope effect; it dominates screened/source output too.
- `BREAKOUT_FAKEOUT_RISK` is emitted as a quality flag but does not suppress or downgrade public BUY.

Recommended fix:
- Add segment-level monitoring thresholds for momentum share and fakeout share.
- Consider downgrading `TF_BUY_MOMENTUM` with fakeout risk to WATCH or lower conviction unless a stronger close/volume confirmation exists.

### A-8. Mark Minervini pattern labels overlap heavily

Evidence:
- Current `integrated_actionable_patterns.csv`: 104 rows.
- `vcp_detected=true`: 89.
- `cup_handle_detected=true`: 87.
- Both true: 72.

Risk:
- The actionable count is not excessive, but pattern labeling may be over-broad.
- A row can appear to satisfy multiple pattern archetypes without a primary/secondary distinction.

Recommended fix:
- Add a `primary_pattern` field and demote the other matched pattern to `secondary_pattern_tags`.
- Keep actionable count unchanged unless pattern overlap is later shown to produce poor forward results.

## 4. Section 2 Applied - Signal Scarcity Risks

### B-1. UG cooldown ignores `signal_code`

Location:
- `screeners/signals/engine.py:6035`

Risk:
- `_is_ug_cooldown_blocked(symbol, signal_code, ...)` accepts `signal_code` but immediately discards it.
- Any recent UG BUY can block all UG BUY families for the symbol during the cooldown window.

Recommended fix:
- Use code-level or family-level cooldown buckets.
- Example: `UG_BUY_BREAKOUT` should not necessarily block `UG_BUY_PBB` or `UG_BUY_MR_LONG` if the strategy intent differs.

### B-2. `breakout_ready` requires many simultaneous conditions

Location:
- `screeners/signals/engine.py:3006`

Current gate requires:
- liquidity pass
- support trend rising
- no EMA turn down
- breakout anchor clear
- close above upper band
- bullish RVOL >= 1.5
- close position >= 0.70
- body strength >= 0.55
- daily return >= `max(2.0, ADR20 * 0.45)`

Risk:
- This may suppress valid breakouts, especially in lower volatility or stepwise breakout behavior.

Recommended fix:
- Split into mandatory gates and scoring gates.
- Track rejection counts per gate before relaxing thresholds.

### B-3. VCP final depth may be too strict

Location:
- `screeners/signals/patterns.py:240`

Risk:
- `final_depth <= min(8.0, first_depth * 0.5)` forces the last contraction to be both shallow and at most half the first contraction.
- This can under-detect constructive VCP patterns that contract but do not halve perfectly.

Recommended fix:
- Replay VCP candidates with `0.6`, `0.7`, and `8/10/12%` final-depth caps.
- Compare count, fakeout rate, and forward return.

### B-4. Weinstein actionable scarcity

Evidence:
- Current `weinstein_stage2/all_results.csv`: 12,233 rows.
- `EXCLUDE`: 6,569.
- `BASE`: 5,622.
- `FAIL`: 42.
- Current primary/retest actionable count: 0 in prior inspection.

Risk:
- `BASE` is broad, but actionable primary/retest output is absent.
- This may be intentional strictness, but it needs rejection-funnel diagnostics to prove.

Recommended fix:
- Add rejection counts after base detection: close proximity, RS pass, volume ratio, weekly alignment, resistance breakout, retest validity.
- Do not relax thresholds until the dominant rejection reason is known.

### B-5. Leader/Lagging zero-output behavior

Evidence:
- Current leaders: 0.
- Current followers: 0.
- Current `market_summary.json` showed `leader_health_score=14.29` while market alignment/breadth/rotation were not all weak.

Risk:
- Core overlay gating, strict RS thresholds, or empty diagnostics can eliminate all output.
- If diagnostics are also empty, auto-tuning cannot identify a relaxation path.

Recommended fix:
- Add broad-pool and rejection-stage counts.
- Separate "no eligible leaders" from "core overlay unavailable or too strict."

### B-6. Qullamaggie pattern-included output can be stale or inconsistent

Evidence:
- Current `breakout_results.csv`: 74 rows.
- Current `pattern_included_candidates.csv`: 0 rows and older timestamp in prior inspection.

Risk:
- Downstream screeners that consume `pattern_included_candidates` may see no Qullamaggie candidates even when breakout results exist.

Recommended fix:
- Add consistency check: if breakout/episode candidates exist, `pattern_included_candidates` should be generated from the same run or explicitly marked stale.
- Include source file timestamp and run id in the market summary.

## 5. Section 3 Applied - Numerical Stability And Data Boundary Conditions

### C-1. `base_start_idx = 0` falsy bug in Qullamaggie prior window

Location:
- `screeners/qullamaggie/core.py:356`
- `screeners/qullamaggie/core.py:357`

Risk:
- `(base_start_idx or len(daily))` treats `0` as missing and falls back to `len(daily)`.
- IPO/short-history cases where a base starts at index 0 can compute prior-run features from the wrong window.

Recommended fix:

```python
base = base_start_idx if base_start_idx is not None else len(daily)
prior_window_start = max(0, base - 60)
prior_window_end = base
```

### C-2. Low `min_periods` can create low-quality early indicators

Locations:
- `screeners/qullamaggie/core.py:313-324`
- `screeners/leader_lagging/screener.py:387-401`
- `screeners/markminervini/enhanced_pattern_analyzer.py:197-212`
- `screeners/weinstein_stage2/screener.py:284-288`

Risk:
- Several indicators allow values before most of the intended window is available.
- Early moving averages, ATR, ADV, and traded-value estimates can enter screening as if fully mature.

Recommended fix:
- Add explicit `indicator_quality` or `warmup_complete` fields.
- For public BUY eligibility, require maturity for key indicators or downgrade to discovery/watch.

### C-3. `detect_tight_range` uses overlapping compressed windows and mixed ATR semantics

Location:
- `screeners/signals/patterns.py:106`

Risk:
- It ranks current ATR10 against ATR10 history, then compares recent ranges against ATR14.
- The five compressed windows overlap heavily; `compressed_bars >= 5` is stricter than it appears and not equivalent to five independent compressed bars.

Recommended fix:
- Rename the measure to reflect overlapping range checks, or change to explicit per-bar compression.
- Add fixtures where only overlapping windows pass but individual bar compression does not.

### C-4. Weinstein weekly bar boundary handling needs explicit verification

Location:
- `screeners/weinstein_stage2/screener.py:247-280`

Risk:
- Weekly bar construction removes an incomplete latest week based on exchange-calendar logic.
- Boundary conditions around holidays and KR/US weekly final sessions can affect stage and breakout timing.

Recommended fix:
- Add calendar fixtures for US holiday weeks and KR final-session mapping.
- Verify whether the latest daily bar should be included in weekly analysis for each market.

### C-5. Date ordering assumptions in pattern screeners

Locations:
- Mark Minervini daily pattern calculations and integrated output paths.
- Qullamaggie feature extraction and `pattern_included_candidates`.

Risk:
- If input frames are not sorted or have duplicate dates, latest-row pattern classification can be wrong.

Recommended fix:
- Add a common precondition check for sorted unique dates after `normalize_indicator_frame`.
- Add tests with unsorted rows and duplicate dates.

## 6. Section 4 Applied - Operational State And Cycle Logic

### D-1. Same cycle can emit multiple SELL rows

Original risk:
- Trend following sell logic can emit `TF_SELL_BREAKDOWN`, `TF_SELL_CHANNEL_BREAK`, and `TF_SELL_TRAILING_BREAK` in the same cycle.

Impact:
- Multiple exit rows can be correct as audit evidence, but public position effects must dedupe or prioritize final close.

Recommended fix:
- Keep diagnostic sell rows if useful.
- Public projection should apply deterministic close priority and cycle-effect resolution.

### D-2. Same symbol can produce BUY and SELL in the same run

Location:
- `_run_scope_scan` buy candidates are generated from scope symbols.
- Sell candidates are generated from active cycles.

Risk:
- A symbol with an existing cycle can satisfy a new BUY condition and a SELL condition in the same scan.
- Current lifecycle fields reduce ambiguity, but the audit should check same-run conflicts explicitly.

Recommended fix:
- Add summary counts for same-symbol same-day BUY/SELL conflicts.
- Define priority: final close should resolve the cycle before a new open unless same-day re-entry is explicitly supported.

### D-3. TF cooldown policy is less explicit than UG cooldown

Risk:
- UG has a dedicated cooldown path, but TF momentum/breakout can continue to dominate open cycles.
- Current `open_family_cycles` inspection showed `TF_MOMENTUM` concentration.

Recommended fix:
- Add TF family-level cycle spacing metrics before introducing cooldown.
- Avoid suppressing true trend continuation without replay evidence.

### D-4. Source disposition labeling has been corrected

Implemented:
- `source_disposition` now appears in public projection rows.
- `discovery_only` labels all-scope no-source rows.
- `signal_summary.buy_signal_segments` separates all, screened, and discovery counts.

Regression coverage:
- `tests/test_signal_engine_restoration.py::test_run_signal_scan_emits_today_only_buy_sell_scope_outputs`
- `tests/test_signals_package.py`

## 7. Section 5 Applied - Screener-Specific Logic Review

### E-1. Signals engine

Highest-risk issues:
- `TF_BUY_MOMENTUM` concentration.
- `BREAKOUT_FAKEOUT_RISK` not blocking or downgrading public BUY.
- `UG_W` broad state detection.
- UG cooldown ignores requested `signal_code`.
- Band-reversion gates behave differently when refined diagnostics are missing.
- Internal event-generation semantics can still mix scope membership with source eligibility before public rows are labeled.

### E-2. Qullamaggie

Highest-risk issues:
- `base_start_idx or len(daily)` bug.
- Low `min_periods` warmup quality.
- `pattern_included_candidates` can be stale/empty while breakout results exist.
- Missing universe prefilter fields can default to scanning, increasing false-positive risk.
- Missing fundamentals and group strength can receive neutral-positive defaults.
- Short history can pass the "not excessive run" check when `ret_3m` is unavailable.
- Episode/gap thresholds may create signal scarcity, but this needs rejection counts before threshold changes.

### E-3. Mark Minervini

Highest-risk issues:
- VCP/cup-handle overlap is high in actionable rows.
- Enhanced analyzer permits several early indicator estimates with low `min_periods`.
- Percentile scoring can give all rows 100 when a metric has only one distinct value.
- RS-score calculation failures collapse to 0 and can remove otherwise valid candidates.

Interpretation:
- Not currently an obvious over-signal by count because actionable rows are 104.
- Stronger issue is label precision and primary/secondary pattern classification.

### E-4. Weinstein Stage 2

Highest-risk issues:
- Large BASE population with no actionable primary/retest output.
- Weekly boundary and volume-ratio references need deterministic calendar tests.
- Recent breakout selection returns the first valid breakout in the lookback window, not necessarily the latest or highest-quality one.

Interpretation:
- This is a signal scarcity / funnel visibility problem before it is a threshold problem.

### E-5. Leader/Lagging

Highest-risk issues:
- Zero leaders/followers in current output.
- Core overlay replacement and strict RS filters can eliminate candidates.
- Non-standalone core overlay can overwrite local leader scores and filter blank overlay states out.
- Empty or sparse diagnostics make tuning ineffective.

Interpretation:
- Add rejection funnels first; do not relax leader thresholds blindly.

## 8. Additional Findings From Second Pass

The following issues were found by reapplying the six prompt sections against existing screener/signal logic only. These are not requests to import new features or external regime logic.

### F-1. Qullamaggie universe prefilter missing-field bypass

Location:
- `screeners/qullamaggie/screener.py:565`
- `screeners/qullamaggie/screener.py:570`

Risk:
- Breakout and episode-pivot scans run when `feature_row is None`, or when `breakout_universe_pass` / `ep_universe_pass` is missing.
- A stale or incomplete feature map can bypass the intended prefilter and expand candidates.

Recommended fix:
- Treat missing universe-pass fields as an explicit diagnostic state.
- Decide per setup whether missing prefilter should block, downgrade, or run as discovery-only.

### F-2. Scope membership can still affect source eligibility during event generation

Location:
- `screeners/signals/engine.py:7954`
- `screeners/signals/engine.py:4189`

Risk:
- `_scoped_source_entry()` sets `buy_eligible = symbol in scope_symbols`.
- `_family_source_profile()` later treats `buy_eligible` as a source-disposition signal through `_source_disposition()`.
- Public rows are now labeled with `discovery_only`, but event-generation `BUY/WATCH` semantics can still be harder to audit because scope inclusion and source eligibility are coupled internally.

Recommended fix:
- Keep `scope_buy_eligible` and `source_buy_eligible` separate internally.
- Public compatibility fields can remain additive, but family-level permission checks should use source disposition only.

### F-3. Weinstein recent breakout returns first valid signal, not best/latest

Location:
- `screeners/weinstein_stage2/screener.py:860`
- `screeners/weinstein_stage2/screener.py:948`

Risk:
- `_detect_recent_breakout()` scans the recent window and stores only the first valid breakout.
- A later, stronger, or cleaner breakout in the lookback window can be ignored.
- This can create stale actionable context or suppress the better current setup.

Recommended fix:
- Rank valid breakout candidates by recency, quality, volume ratio, and resistance confidence.
- Add a test with two valid breakouts where the later one should win.

### F-4. Qullamaggie missing fundamentals default to neutral-positive scores

Location:
- `screeners/qullamaggie/core.py:563`
- `screeners/qullamaggie/core.py:673`

Risk:
- Missing earnings growth, revenue growth, ROE, and market cap score as `70.0`.
- Missing `group_strength_score` is filled with `70.0`.
- Data absence can support candidate quality instead of being neutral, unknown, or penalized.

Recommended fix:
- Split value score from data-coverage score.
- Add `fundamental_data_status` / `group_data_status` to downstream outputs.

### F-5. Mark Minervini percentile scoring gives uniform metrics a perfect score

Location:
- `screeners/markminervini/integrated_screener.py:139`

Risk:
- `_percentile_score()` returns `100.0` for all rows when the input metric has one distinct value.
- A low-quality metric with a constant value can become a perfect percentile component.

Recommended fix:
- Use `50.0` for single-valued distributions, or report `NO_CROSS_SECTIONAL_SPREAD`.
- Add a regression test for a constant liquidity series.

### F-6. Qullamaggie short history can pass the excessive-run filter

Location:
- `screeners/qullamaggie/core.py:440`

Risk:
- `no_excessive_run = ret_3m is None or ret_3m < 1.0`.
- If a symbol has too little history to compute 3-month return, it passes the excessive-run filter.

Recommended fix:
- Treat missing `ret_3m` as unknown and require a separate short-history classification.
- If short-history candidates should be allowed, mark them as discovery-only or lower confidence.

### F-7. Leader/Lagging non-standalone overlay can erase local leaders

Location:
- `screeners/leader_lagging/screener.py:2248`
- `screeners/leader_lagging/screener.py:2255`

Risk:
- In non-standalone mode, local `leader_score` is replaced by `core_leader_score`.
- Rows are then filtered to `leader_state in ["CONFIRMED", "EMERGING"]` and `breakdown_status == "OK"`.
- If core overlay fields are blank, stale, or unavailable, local leader candidates can disappear.

Recommended fix:
- Count overlay-missing and overlay-filtered leaders separately.
- Preserve local score as an audit field and only replace ranking when overlay freshness is valid.

### F-8. Mark Minervini RS calculation failure collapses candidates to zero

Location:
- `screeners/markminervini/screener.py:314`

Risk:
- Missing RS scores are filled with `0.0`.
- Benchmark alignment failures or RS calculation gaps can make all candidates fail `cond8`, producing artificial signal scarcity.

Recommended fix:
- Distinguish true low RS from `RS_UNAVAILABLE`.
- Add summary counts for RS-missing candidates and a test for benchmark/symbol alignment failure.

## 9. Section 6 Applied - Quick Robustness Scan Summary

Critical / high priority:

| Priority | Finding | Type | First action |
| --- | --- | --- | --- |
| P0 | Public output segmentation | Interpretation / contract | Implemented via `source_disposition` and `buy_signal_segments` |
| P1 | RSI `None` bypass | False positive | Block MR signals when RSI missing |
| P1 | `TF_BUY_MOMENTUM` dominance plus fakeout flags | Over-signal | Add downgrade/suppression replay and segment metrics |
| P1 | `UG_W` broad detection | False positive / over-state | Require local swing-low W structure |
| P1 | Qullamaggie stale `pattern_included_candidates` | Operational consistency | Add same-run consistency check |
| P1 | Scope/source eligibility coupling inside signal event generation | Contract ambiguity | Separate scope membership from source permission |
| P1 | Qullamaggie missing prefilter fields default to scan | False positive | Add missing-prefilter diagnostics and policy |
| P2 | UG cooldown ignores `signal_code` | Signal scarcity | Use code/family cooldown buckets |
| P2 | Weinstein actionable zero | Signal scarcity | Add rejection funnel |
| P2 | Weinstein first-valid breakout selection | Stale/low-quality actionability | Rank valid breakouts by recency and quality |
| P2 | Leader/Lagging zero output | Signal scarcity | Add broad-pool/rejection diagnostics |
| P2 | Leader/Lagging overlay can erase local leaders | Signal scarcity | Count overlay-missing/filter effects |
| P2 | Mark Minervini RS failures become zero score | Signal scarcity | Separate unavailable RS from low RS |
| P2 | VCP contraction tolerance/final depth | False positive + scarcity | Replay parameter sensitivity |
| P3 | Qullamaggie neutral-positive missing data defaults | False positive | Split data coverage from quality score |
| P3 | Mark Minervini uniform percentile equals 100 | Score distortion | Use neutral value for no cross-sectional spread |
| P3 | Qullamaggie missing `ret_3m` passes excessive-run check | False positive | Add short-history state |
| P3 | Minervini VCP/cup overlap | Label precision | Add primary/secondary pattern fields |

Monitoring metrics to add or track:
- `buy_signal_segments.all_only_discovery_total`
- `buy_signal_segments.screened_total`
- `TF_BUY_MOMENTUM` share by segment
- `BREAKOUT_FAKEOUT_RISK` share by segment
- `UG_W` state count by segment
- Same-symbol same-day BUY/SELL conflict count
- Screener rejection counts by stage
- Stale-output timestamp/run-id mismatches
- Missing-prefilter scan counts
- Overlay-missing and overlay-filtered candidate counts
- RS-unavailable candidate counts
- Constant-distribution percentile flags

## 10. Validation Plan

Signal engine:
- Add tests for RSI `None` MR_LONG/MR_SHORT suppression.
- Add tests for W pattern local-low requirement.
- Add tests for VCP contraction worsening and final-depth sensitivity.
- Add tests for `BREAKOUT_FAKEOUT_RISK` downgrade policy once chosen.
- Add tests for UG cooldown code/family scope.
- Add tests proving scope membership does not grant source-level family buy permission.

Output contract:
- Keep all/screened scope tests.
- Keep public file names unchanged.
- Verify `source_disposition` is present in buy/sell projections.
- Verify all-only no-source rows remain in `buy_signals_all_symbols_v1` as `discovery_only`.
- Verify `signal_summary.buy_signal_segments` row counts match public files.

Screener consistency:
- Qullamaggie: if `breakout_results` or `episode_pivot_results` has pattern-included candidates, `pattern_included_candidates` should reflect the same run or be marked stale.
- Qullamaggie: add missing-prefilter, missing-fundamentals, and short-history fixtures.
- Weinstein: add funnel diagnostics from `BASE` to actionable state.
- Weinstein: add a two-breakout fixture where the later/higher-quality breakout wins.
- Leader/Lagging: add broad-pool count, RS rejection count, overlay gating count.
- Leader/Lagging: add overlay-missing and stale-overlay tests.
- Mark Minervini: assert one `primary_pattern` per actionable row.
- Mark Minervini: add RS-unavailable and constant-percentile fixtures.

Replay:
- Compare old/new on local deterministic OHLCV only.
- Evaluate counts, duplicate signals, same-day conflicts, fakeout rate, forward return, MAE/MFE by segment.
- Do not use live network dependency in unit tests.

## 11. Reference Files

Primary code:
- `screeners/signals/engine.py`
- `screeners/signals/patterns.py`
- `screeners/signals/writers.py`
- `screeners/signals/source_registry.py`

Screeners:
- `screeners/qullamaggie/core.py`
- `screeners/qullamaggie/screener.py`
- `screeners/weinstein_stage2/screener.py`
- `screeners/leader_lagging/screener.py`
- `screeners/leader_lagging/algorithms.py`
- `screeners/leader_lagging/quality.py`
- `screeners/markminervini/screener.py`
- `screeners/markminervini/enhanced_pattern_analyzer.py`
- `screeners/markminervini/integrated_screener.py`

Current outputs used as evidence:
- `results/us/signals/multi_screener/signal_summary.json`
- `results/us/signals/multi_screener/buy_signals_all_symbols_v1.csv`
- `results/us/signals/multi_screener/buy_signals_screened_symbols_v1.csv`
- `results/us/signals/multi_screener/all_signals_v2.csv`
- `results/us/screeners/weinstein_stage2/all_results.csv`
- `results/us/screeners/leader_lagging/market_summary.json`
- `results/us/screeners/qullamaggie/breakout_results.csv`
- `results/us/screeners/qullamaggie/pattern_included_candidates.csv`
- `results/us/screeners/markminervini/integrated_actionable_patterns.csv`
