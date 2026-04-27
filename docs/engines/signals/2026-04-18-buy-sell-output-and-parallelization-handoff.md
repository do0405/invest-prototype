# Screening / Signals Correctness And Runtime Hardening Handoff

## Purpose

This file is the current operating index for the screening and signal hardening work
in `invest-prototype-main`.

Detailed follow-up work is split into linked documents:

- [Signal Artifact Inventory And Quarantine](./2026-04-21-signal-artifact-inventory-and-quarantine.md)
- [Daily Bar Closure Audit Plan](./2026-04-21-daily-bar-closure-audit-plan.md)
- [Buy/Sell Replay Golden Set Plan](./2026-04-21-buy-sell-replay-golden-set-plan.md)
- [Indicator Dog Signal Algorithm Fidelity Contract](./2026-04-21-indicator-dog-signal-algorithm-fidelity-contract.md)

The priority order remains:

1. Correctness and semantic clarity.
2. Repo-wide time / `as_of` discipline.
3. Deterministic and debuggable operation.
4. Safe cleanup of ambiguous or stale outputs.
5. Runtime efficiency.
6. Buy/sell signal accuracy evaluation and operational polish.

## Current Baseline

- Recent full-suite baseline: `415 passed` on 2026-04-23.
- Latest practical speed / safe parallelization baseline: `415 passed` on
  2026-04-23.
- The buy/sell signal fidelity batch is additive-compatible:
  - public artifact filenames are unchanged;
  - public buy/sell outputs are today-only projections for the current `as_of_date`;
  - `cycle_effect`, position unit delta fields, and `indicator_dog_rule_ids` are additive row fields.
- Historical signal files are internal lifecycle / diagnostic artifacts, not public
  recommendation outputs:
  - `open_family_cycles.csv` restores open-cycle state;
  - `signal_event_history.csv` supports event history and cooldown behavior;
  - `signal_state_history.csv` preserves state / aux provenance separately;
  - `peg_event_history.csv` supports PEG follow-up after event day.
- Public/operator-facing "look back several days to see whether BUY/SELL fired"
  behavior must stay removed. Current public buy/sell artifacts must not expose past
  recommendation rows.

## Done / Validated Enough

- Source disposition and `watch_only` are canonicalized through:
  - `screeners/source_contracts.py`
  - `screeners/signals/source_registry.py`
  - `screeners/augment/pipeline.py`
  - signal universe output tests
- Main screener paths now pass `as_of` into local symbol-frame loads:
  - Weinstein
  - leader-lagging
  - Mark Minervini
  - TradingView
  - signals
- Weinstein weekly analysis uses completed-week semantics.
- Augment is diagnostic-only and must not become a ranking, gating, or selection authority.
- Runtime state basics exist through `runtime_state.json`, `runtime_profile.json`, and
  collector run state where wired.
- Buy/sell signal fidelity semantics are covered by the 2026-04-21 Indicator Dog contract:
  - `cycle_effect` is authoritative lifecycle meaning;
  - Trend TP1/TP2 trim current units while keeping the cycle open;
  - final close signals remove remaining open-cycle units;
  - Trend trailing/protected stops refresh with never-down behavior;
  - UG validation scoring follows the Indicator Dog raw table;
  - Green state alone does not create BUY;
  - PBB Orange emits WATCH rather than BUY.
- The stale ETF fixture failures were test-fixture issues, not signal regressions.
  ETF/fund metadata skip remains intentional behavior.
- The Pine/Indicator Dog gate refinement batch is implemented:
  - `screeners/signals/patterns.py` contains the deterministic helper layer for
    pocket pivot, band reversion, and exit pressure;
  - `UG_BUY_PBB`, `UG_SELL_PBS`, `UG_SELL_MR_SHORT`,
    `UG_SELL_BREAKDOWN`, `TF_SELL_BREAKDOWN`,
    `TF_SELL_RESISTANCE_REJECT`, and `TF_SELL_TRAILING_BREAK`
    keep their public codes while using refined daily-close gates;
  - helper coverage lives in `tests/test_signal_patterns.py`;
  - replay/public projection coverage lives in `tests/test_signal_replay_accuracy.py`.
- Direct signal-engine default `as_of` now follows completed-session discipline when
  no explicit replay date is provided. Explicit `as_of_date` remains replay mode.
- Public buy/sell projections strip internal gate diagnostics such as BB z-score,
  pocket-pivot, and band-reversion helper fields.
- Results-root isolation contract is now explicit:
  - when `INVEST_PROTO_RESULTS_DIR` is set, screening/signal/runtime artifacts resolve
    inside that override root rather than mixing with default `results/{market}`;
  - latest CSV/JSON and stage summary JSON writes use fallback-safe writer helpers;
  - isolated `signals` and `augment` runs fail fast if the same root does not contain
    prerequisite screening artifacts or a compatible source-registry snapshot;
  - per-screener/per-signal artifact families and column schemas remain strategy-local
    and are not flattened into one shared schema.
- Repo-wide non-signal daily-bar freshness diagnostics are implemented for:
  - Mark Minervini technical screening;
  - TradingView preset screening;
  - leader/lagging feature builds;
  - Qullamaggie frame loads;
  - Weinstein Stage 2 daily-to-weekly analysis.
- Freshness diagnostics are runtime-only under `data_freshness`; public screener and
  buy/sell output schemas are unchanged.
- `--skip-data` now means local-only for adjunct provider paths as well as OHLCV
  collection:
  - Advanced Financial does not call `collect_financial_data_hybrid()` when
    `skip_data=True`; it reuses cached `advanced_financial_results.csv` rows where
    present and emits degraded placeholders otherwise;
  - Qullamaggie task orchestration passes `enable_earnings_filter=False` when
    `skip_data=True`;
  - signal orchestration supports `local_only=True` and injects empty upcoming
    earnings and no-op earnings-surprise providers;
  - `main.py --task signals --skip-data` and `main.py --task all --skip-data`
    dispatch signal runs through that local-only path.
- Live/local processing speed batch is implemented without public schema changes:
  - `RuntimeContext` now has same-run OHLCV frame caching with copy-on-read/write
    protection;
  - `utils.market_data_contract.load_local_ohlcv_frames_ordered()` provides
    deterministic ordered parallel frame loads;
  - Mark Minervini, TradingView, Weinstein Stage 2, and Leader/Lagging use the
    ordered loader after symbol universe selection, so RS/percentile/cross-symbol
    logic remains sequential;
  - Weinstein and Leader/Lagging parallelize independent symbol-level analysis while
    preserving input-order materialization before ranking and file output;
  - runtime profiles now include internal timings such as frame load, benchmark load,
    symbol/feature analysis, relationship/context work, and output persistence where
    wired;
  - KR OHLCV collection separates FDR primary from Yahoo fallback: FDR primary can
    use a small worker pool, and only FDR failures enter the Yahoo-throttled lane;
  - US OHLCV collection prefetches same-window existing/new symbols through a
    yfinance batch candidate and falls back to existing single-symbol
    retry/rate-limit handling.
- Safe speed / live collector calibration batch is implemented without changing
  algorithms, rankings, signal lifecycle, or public output schemas:
  - Weinstein group context keeps metadata member counts but performs expensive daily
    normalization / weekly builds only for symbols with loaded `daily_frames`;
  - Weinstein market/group/symbol analysis reuses a precomputed `benchmark_weekly`
    frame where available, while preserving fallback behavior;
  - Qullamaggie uses `load_local_ohlcv_frames_ordered()` and runtime OHLCV cache for
    frame loading, preserving `as_of` and candidate order;
  - runtime timings now include `weinstein.group_context_seconds`,
    `qullamaggie.frame_load_seconds`, `qullamaggie.context_build_seconds`,
    `qullamaggie.setup_scan_seconds`, and related output-persistence timings;
  - Yahoo shared throttle telemetry now tracks attempts, successes, success streaks,
    rate-limit counts, and AIMD interval scale by source;
  - `INVEST_PROTO_YAHOO_MIN_INTERVAL_SCALE`,
    `INVEST_PROTO_YAHOO_SUCCESS_DECAY_STEP`, and
    `INVEST_PROTO_YAHOO_FAILURE_BACKOFF_STEP` tune the adaptive throttle internally;
  - US OHLCV yfinance batch prefetch is used only for same-window groups with at
    least two symbols; one-symbol groups stay on the existing single-symbol path;
  - batch omissions still fall back to the existing single-symbol retry path;
  - Yahoo success telemetry is wired into US/KR OHLCV, Mark Minervini financials,
    Qullamaggie/signal earnings, and metadata collectors; rate-limit telemetry remains
    shared through `extend_yahoo_cooldown()`.
- Follow-up speed pass on 2026-04-23:
  - Leader/Lagging RS proxy now converts each stock/benchmark series to numeric once
    per profile instead of once per offset/span component; ranking semantics and RS
    formula are unchanged;
  - signal frame loading now uses the shared ordered/cache-aware OHLCV loader;
  - signal runtime profile now records internal timings for source registry, PEG,
    history load/persist, financial load, frame load, feature map, metrics map, scope
    scans, and output persistence.
- Live collector bottleneck reduction pass on 2026-04-23:
  - `collect_data_main()` no longer applies Yahoo phase handoff waits before KR-only
    FDR/reference stages; KR OHLCV Yahoo fallback still uses the shared Yahoo
    throttle internally when FDR cannot serve a symbol;
  - US OHLCV same-window batch grouping reuses each symbol's prepared local frame and
    fetch window instead of reading the same CSV again in `process_ticker()`;
  - US and KR collector run-state checkpoints now record symbol outcomes in memory
    and write `collector_run_state.json` at chunk boundaries instead of per symbol;
  - KR still writes one final diagnostics snapshot after benchmark collection, so
    the operator state remains useful without restoring per-symbol disk writes.
- Additional practical bottleneck reduction pass on 2026-04-23:
  - Qullamaggie context feature rows are computed with ordered symbol-level
    parallelism via `INVEST_PROTO_SYMBOL_ANALYSIS_WORKERS`; materialization order
    remains the existing symbol order before ranking/output;
  - Qullamaggie setup scan no longer copies the symbol DataFrame before breakout,
    EP, or parabolic-short analyzer calls, and breakout/EP universe hard-fails
    short-circuit before heavy setup analyzers;
  - Leader/Lagging reuses the already normalized benchmark frame during symbol
    feature analysis, avoiding per-symbol benchmark normalization while preserving
    RS/ranking formulas and final row order;
  - signal `_build_metrics()` now returns minimal empty metrics before indicator
    normalization when a symbol frame is empty;
  - US OHLCV defaults align chunk size with the same-window yfinance batch size,
    removes the unreachable duplicate batch-start CSV read, and skips chunk pause
    for chunks that were entirely already-latest/local with no provider request;
  - KR OHLCV skips inter-chunk pause only when every symbol in the chunk is already
    latest; any FDR/Yahoo fetch or rate-limit chunk keeps existing pacing;
  - KR metadata pre-fills the missing universe once from reference sources in
    `main()`, immediately merges complete reference records into cache/output, and
    sends only incomplete records into the Yahoo-backed metadata queue;
  - metadata batch pause is skipped only when diagnostics prove the batch had zero
    provider-fetch symbols. TTL policy, Yahoo shared throttle, OHLCV merge/dedup
    schema, public output schemas, signal lifecycle, and ranking semantics are
    unchanged.
- Safe screener / signal parallelization pass on 2026-04-23:
  - `run_all_screening_processes()` keeps the dependent chain
    Mark Minervini technical → Advanced Financial → Integrated → New ticker
    tracking sequential;
  - Weinstein Stage 2, Leader/Lagging, TradingView presets, and Qullamaggie
    when `skip_data=True` now run as a safe local fan-out controlled by
    `INVEST_PROTO_SCREENING_STAGE_PARALLEL` and
    `INVEST_PROTO_SCREENING_STAGE_WORKERS`;
  - Qullamaggie with live earnings/provider possibility remains in the
    Yahoo-handoff sequential lane;
  - each parallel screener stage uses a child `RuntimeContext`; parent context
    receives deterministic timing/cache/row/freshness merges only after child
    completion;
  - signal all-symbol and screened-symbol scope scans now run in parallel via
    `INVEST_PROTO_SIGNAL_SCOPE_WORKERS`, while `_update_cycles()`, history merge,
    final projection, and file persistence remain serial;
  - signal scope-internal state-row and sell-candidate generation uses ordered
    parallel pure calculation via `INVEST_PROTO_SIGNAL_EVENT_WORKERS`;
  - public screener/signal filenames, CSV/JSON schemas, signal codes, ranking
    formulas, and lifecycle mutation semantics are unchanged.

## Partial / Needs Audit Before More Implementation

- Artifact/output semantics need inventory and classification before deletion or rename.
  See the artifact inventory document.
- Daily-bar closure assumptions for the main non-signal screener paths now have
  runtime diagnostics. Collector/provider write-time closure behavior still needs a
  separate audit before any hard blocking or collector rewrite.
- Replay/evaluation coverage has deterministic synthetic signal cases. Any live-data
  evaluation remains local-only operator validation, not a unit-test oracle.
- Runtime resumability exists but operator-grade runbook polish remains lower priority
  than measured collection/processing bottlenecks:
  - what finished;
  - what is pending;
  - what can be retried;
  - what is blocked by rate limits or degraded external artifacts;
  - whether rerunning will repeat expensive provider calls.
- Collector/provider closure audit is also lower priority while runtime freshness
  diagnostics remain non-blocking and the immediate bottleneck is local processing
  time.

## Next Safe Order

1. Keep full-suite baseline green.
2. Maintain the artifact inventory / quarantine document.
3. Improve measured local processing bottlenecks one stage at a time. Weinstein
   context/group work, Qullamaggie duplicate frame loading, Leader/Lagging RS proxy
   repeated numeric conversion, signal frame-load cache usage, collector handoff
   waits, duplicate US OHLCV prepare reads, and per-symbol collector state writes
   have been narrowed; use the next bounded runtime profile to select the next stage
   rather than guessing.
4. Continue collector/provider closure audit only where runtime diagnostics show stale
   or future/partial row risk.
5. Expand replay/evaluation only after deterministic local cases identify a gap.
6. Polish runtime resumability and bounded-run operator guidance.

Do not collapse these into one broad refactor.

## Do Not Reopen Unless Evidence Changes

- Do not rename or remove public buy/sell output files:
  - `buy_signals_all_symbols_v1`
  - `sell_signals_all_symbols_v1`
  - `buy_signals_screened_symbols_v1`
  - `sell_signals_screened_symbols_v1`
- Do not reintroduce public past-N-day BUY/SELL lookup fields or reports.
- Do not delete internal history files that support cycle restore, cooldown, PEG
  follow-up, or state provenance.
- Do not undo Indicator Dog additive fields.
- Do not treat `action_type` as lifecycle-authoritative when `cycle_effect` is present.
- Do not hard-block BUY artifacts solely because market context is weak.
- Do not make augment a ranking, gating, or selection authority.
- Do not parallelize RS / percentile / cross-symbol relative logic, history merge, cycle
  mutation, or file writing unless a separate design proves determinism. The current
  parallelism is limited to independent local stages and ordered pure signal
  candidate calculation before lifecycle mutation.
- Do not change `market-intel-core` ownership from this repository.
- Do not reorganize or delete `docs/archive/raw-sources/Reference/indicator-dog/`.

## Verification Commands

Focused handoff suite:

```powershell
.\.venv\Scripts\python -m pytest tests/test_signal_patterns.py tests/test_signal_replay_accuracy.py tests/test_signal_engine_restoration.py tests/test_signals_package.py -q
```

Full suite:

```powershell
.\.venv\Scripts\python -m pytest -q
```

Latest verification:

- Safe screener/signal parallelization targeted suite:
  `3 passed` for orchestrator safe fan-out and signal scope fan-out.
- Orchestrator plus signal restoration suite:
  `97 passed`.
- Focused signal compatibility suite:
  `19 passed` for signal patterns, replay accuracy, and package contract tests.
- Focused parallelized screener suite:
  `64 passed` for Qullamaggie, Leader/Lagging, Weinstein Stage 2, and TradingView.
- Full suite after safe parallelization:
  `415 passed`.
- Bounded local `--skip-data --standalone` smoke with scratch
  `INVEST_PROTO_RESULTS_DIR=.runtime_eval/parallel_screening_signal_20260423_after_merge`,
  `INVEST_PROTO_RUNTIME_SYMBOL_LIMIT=25`, and system Python 3.12:
  - `.venv` Python 3.13 was blocked by the repo unstable-runtime guard;
  - system Python 3.12 completed screening+signals successfully from
    `20:21:47` to `20:22:10` local time, about `23s` wall-clock;
  - summary `elapsed_seconds` remains a sum of stage elapsed values, so it reports
    about `51.0s` even though safe stage fan-out overlaps independent work;
  - screening stages 5-8 started together; Qullamaggie was included because
    `skip_data=True`;
  - signal engine completed in about `4.4s`;
  - public signal counts were `buy_all=0`, `sell_all=0`, `buy_screened=0`,
    `sell_screened=0`.
- Practical bottleneck reduction focused suite:
  `110 passed` for Qullamaggie, Leader/Lagging, and signal restoration.
- Collector optimization focused suite:
  `65 passed` for US OHLCV, KR OHLCV, and stock metadata collectors.
- Focused signal compatibility suite after practical speed changes:
  `19 passed`.
- Full suite after practical speed / collector optimization:
  `412 passed`.
- Bounded local `--skip-data --standalone` smoke with scratch
  `INVEST_PROTO_RESULTS_DIR=.runtime_eval/speed_followup_20260423`,
  `INVEST_PROTO_RUNTIME_SYMBOL_LIMIT=25`, and system Python 3.12:
  - `.venv` Python 3.13 was blocked by the repo unstable-runtime guard;
  - system Python 3.12 completed screening+signals successfully in about `16.1s`;
  - screening completed in about `12.2s`, signal engine in about `3.9s`;
  - Weinstein Stage 2 completed in about `3.4s` with
    `weinstein.group_context_seconds=0.395634` and
    `weinstein.symbol_analysis_seconds=1.911188`;
  - Leader/Lagging completed in about `2.7s` with
    `leader_lagging.feature_analysis_seconds=1.040527`;
  - Qullamaggie completed in about `3.3s` with
    `qullamaggie.context_build_seconds=0.843926` and
    `qullamaggie.setup_scan_seconds=0.490446`;
  - public signal counts remained `buy_all=0`, `sell_all=0`,
    `buy_screened=0`, `sell_screened=0`.
- Live collector bottleneck reduction focused suite:
  `68 passed` for US/KR collectors and orchestrator handoff/state-write behavior.
- Adjacent collector cache/provider compatibility suite:
  `49 passed`.
- Focused signal suite after collector bottleneck changes:
  `84 passed`.
- Full suite after live collector bottleneck changes:
  `404 passed`.
- Safe speed / live collector calibration focused suite:
  `73 passed` for Yahoo throttle, US/KR collectors, MarkMinervini advanced financial,
  Qullamaggie, and Weinstein.
- Orchestrator and adjacent screener compatibility suite:
  `64 passed`.
- Focused signal suite after speed/calibration changes:
  `84 passed`.
- Full suite after speed/calibration changes:
  `398 passed`.
- Additional speed follow-up focused suite:
  `112 passed` for Leader/Lagging plus signal regression.
- Additional collector/screener/orchestrator compatibility suite:
  `110 passed`.
- Full suite after additional speed follow-up:
  `399 passed`.
- Bounded local speed smoke with scratch
  `INVEST_PROTO_RESULTS_DIR=.runtime_eval/speed_bottleneck_20260423_signal_timing`,
  `INVEST_PROTO_RUNTIME_SYMBOL_LIMIT=25`, and system Python 3.12:
  - completed screening+signals in about `15.9s`;
  - previous same-session pre-follow-up smoke was about `19.7s`;
  - screening was about `12.2s`, signal engine about `3.7s`;
  - Leader/Lagging feature analysis improved from about `2.38s` to about `1.55s`;
  - signal profile showed `signals.frame_load_seconds=0.301643`,
    `signals.feature_map_seconds=0.778507`,
    `signals.metrics_map_seconds=0.686458`,
    `signals.persist_outputs_seconds=0.669816`;
  - remaining top bounded stages are signal output/feature/metrics work, Weinstein
    symbol analysis, Qullamaggie setup/context/persist work, and Leader/Lagging
    feature analysis; no further I/O frame-load bottleneck was visible.
- No live provider calibration smoke was run in this restricted session; live provider
  behavior remains covered by deterministic fake-provider and monkeypatch tests, with
  optional operator smoke left as a scratch-only run.
- Live/local processing speed focused suite:
  `82 passed` for KR/US collector and MarkMinervini/TradingView/Weinstein/Leader.
- Runtime/signal compatibility suite:
  `112 passed`.
- Weinstein/Leader/orchestrator timing regression suite:
  `68 passed`.
- Full suite after live/local processing speed changes:
  `391 passed`.
- Bounded local `--skip-data --standalone` smoke with scratch
  `INVEST_PROTO_RESULTS_DIR=.runtime_eval/live_speed_20260422`,
  `INVEST_PROTO_RUNTIME_SYMBOL_LIMIT=25`, and system Python 3.12:
  - `.venv` Python 3.13 was blocked by the repo unstable-runtime guard;
  - system Python 3.12 completed screening+signals successfully in about `29.2s`;
  - screening completed in about `25.0s`, signal engine in about `4.3s`;
  - public signal counts were `buy_all=0`, `sell_all=0`, `buy_screened=0`,
    `sell_screened=0`;
  - slowest screening stage was Weinstein Stage 2 at about `14.5s`;
  - runtime timing captured `markminervini.frame_load_seconds=0.501026`,
    `tradingview.frame_load_seconds=0.009269`,
    `weinstein.benchmark_load_seconds=0.027356`,
    `weinstein.frame_load_seconds=0.008421`,
    `weinstein.market_context_seconds=10.96345`,
    `weinstein.symbol_analysis_seconds=2.7256`,
    `weinstein.persist_outputs_seconds=0.33406`,
    `leader_lagging.benchmark_load_seconds=0.027944`,
    `leader_lagging.frame_load_seconds=0.006927`,
    `leader_lagging.feature_analysis_seconds=2.461214`,
    `leader_lagging.relationship_analysis_seconds=0.082241`, and
    `leader_lagging.persist_outputs_seconds=0.350303`.
- Local-only speed focused suite:
  `59 passed`.
- Focused signal suite after local-only speed changes:
  `84 passed`.
- Full suite after local-only speed changes:
  `380 passed`.
- Bounded local `--skip-data --standalone` smoke with scratch
  `INVEST_PROTO_RESULTS_DIR=.runtime_eval/local_only_speed_20260422`,
  `INVEST_PROTO_RUNTIME_SYMBOL_LIMIT=25`, and system Python 3.12:
  - completed successfully in about `52.2s` for screening+signals;
  - the requested `.venv` command was blocked by the repo's Windows Python 3.13
    venv guard, so the stable Python 3.12 runtime was used for the smoke;
  - Advanced Financial completed in `0.2s` using local-only placeholders
    (`cached=0`, `placeholders=2`);
  - Qullamaggie completed in `5.0s` with earnings filter disabled by
    `skip_data=True`;
  - signal engine completed in `5.6s`; public buy/sell counts were all `0`;
  - `earnings_provider_diagnostics` rows were `0`, consistent with the no-op
    local-only earnings collector;
  - largest remaining stage was Weinstein Stage 2 at about `28.7s`.
- Non-signal closure/runtime diagnostics focused suite:
  `92 passed`.
- Focused signal suite: `84 passed`.
- Full suite: `373 passed`.
- Bounded local TradingView smoke with scratch
  `INVEST_PROTO_RESULTS_DIR=.runtime_eval/closure_freshness_tradingview_20260422`
  and `INVEST_PROTO_RUNTIME_SYMBOL_LIMIT=4`:
  - produced the six US TradingView preset files in scratch output;
  - `data_freshness` counts were `closed=4`, `stale=0`,
    `future_or_partial=0`, `empty=0`.
- Bounded local signal replay with scratch
  `INVEST_PROTO_RESULTS_DIR=.runtime_eval/closure_freshness_signal_20260422`,
  stubbed earnings provider, existing local source-registry snapshot, and
  `as_of_date=2026-04-17`:
  - public buy/sell counts were all `0`;
  - `all_signals_v2=12`, `signal_state_history=12`, `signal_universe=2`;
  - no internal gate diagnostic leaks were found in public buy/sell outputs.
- Bounded local dry evaluation with local data, no provider fetcher, stubbed earnings
  collector, and no-op writers:
  - `as_of_date=2026-04-17`;
  - public buy/sell counts were all `0` for the bounded symbol set;
  - public output dates were empty and no internal diagnostic-field leaks were found.
  - A normal writer-enabled local evaluation hit a permission lock on
    `results/us/screeners/peg_imminent/peg_imminent_raw.csv`, so the local validation
    used no-op writers to avoid mutating operator artifacts.
- A wider 25-symbol scratch screening+signals smoke exceeded the 10-minute command
  limit; use smaller symbol limits or run long operator evaluations outside unit-test
  verification.

Windows runtime smoke, only when needed:

```powershell
$env:INVEST_PROTO_RUNTIME_SYMBOL_LIMIT='4'
& "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe" main.py --task all --market us
```

If this smoke reports a missing `market-intel-core` compatibility artifact, treat that
as a degraded external dependency unless local runtime state shows a local task failure.
