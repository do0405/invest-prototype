# Daily Bar Closure Audit Plan

## Purpose

`as_of` scoping prevents future-date leakage, but it does not prove that the latest
daily row is a closed candle.

Many screeners and signal calculations use the latest row through `iloc[-1]`. If that
row is partial intraday data, a BUY/SELL signal can become a false positive or false
negative even when the date filter is correct.

This audit started as documentation-first for the broader repo. Direct signal-engine
defaults now use completed-session `as_of` resolution when no explicit replay date is
supplied, and the main non-signal screeners now emit runtime-only daily-row freshness
diagnostics.

## Audit Questions

- For each local OHLCV source, when is a row considered closed?
- Can the collector write today's partial daily row before market close?
- Can a provider lag one session without making the operator aware?
- Does `as_of_date` mean "calendar date requested" or "latest confirmed closed bar" for
  each path?
- Do downstream screeners distinguish partial, stale, and closed data?

## Paths To Audit

- Mark Minervini technical screening.
- TradingView preset screening.
- Leader/lagging feature builds.
- Weinstein completed-week conversion from daily bars.
- Signal metrics and buy/sell candidate formation.
- Standalone local market-truth builders.
- US and KR OHLCV collectors, including provider-specific end-date behavior.

## Expected Findings Format

For each path, record:

- owning module;
- data source and local file path shape;
- relevant `as_of` handling;
- latest-row closure assumption;
- stale-row behavior;
- partial-row risk;
- current tests;
- proposed handling.

## Handling Defaults

- Do not block screeners solely because an audit has not yet been completed.
- Prefer explicit diagnostics before changing behavior.
- If a path can ingest partial daily rows, add a closure/staleness flag before changing
  BUY/SELL gating.
- If a provider is one-day-lagged, surface that as operator context instead of silently
  treating it as fresh.
- Keep replay tests deterministic and local.

## Initial Acceptance Criteria

- The audit identifies every runtime path that feeds latest daily rows into signal or
  screener decisions.
- The audit distinguishes future leakage, partial current-day rows, and stale latest rows.
- The next implementation batch has clear candidate changes, if any, without requiring
  broad collector rewrites.

## Implemented For Signals

- `MultiScreenerSignalEngine` default `as_of` now delegates to the same completed-session
  discipline used by orchestrator runtime context.
- Explicit `as_of_date` remains replay mode and is not clipped.
- Standalone signal market-truth benchmark loading is scoped to the resolved
  `as_of_date`.
- Regression coverage is in `tests/test_signal_replay_accuracy.py`.

## Implemented For Non-Signal Screeners

- `utils.market_data_contract.describe_ohlcv_freshness()` classifies loaded OHLCV
  frames as `closed`, `stale`, `future_or_partial`, or `empty` against the active
  target date.
- `RuntimeContext.update_data_freshness(stage_key, summary)` stores diagnostics under
  `runtime_state["data_freshness"]`; public screener output schemas are unchanged.
- Orchestrator `runtime_profile.json` preserves top-level and step-level
  `data_freshness` summaries for operator review.
- Regression coverage is in:
  - `tests/test_market_data_contract.py`;
  - `tests/test_orchestrator_tasks.py`;
  - screener-focused tests for Mark Minervini, TradingView, leader/lagging,
    Qullamaggie, and Weinstein Stage 2.

## Audited Runtime Paths

| Path | Owner | Source Shape | `as_of` Handling | Latest-Row Handling |
| --- | --- | --- | --- | --- |
| Mark Minervini technical | `screeners/markminervini/screener.py` | `data/{market}/{symbol}.csv` via `load_local_ohlcv_frame()` | Uses explicit/runtime `as_of_date`, otherwise benchmark latest date | Emits `markminervini_technical` freshness summary after symbol frame loads |
| TradingView presets | `screeners/tradingview/screener.py` | `data/{market}/{symbol}.csv` via `load_local_ohlcv_frame()` | Uses runtime `as_of_date`, otherwise benchmark latest date | Emits `tradingview_presets` freshness summary after metric frame loads |
| Leader/lagging | `screeners/leader_lagging/screener.py` | local OHLCV files listed from market data dir | Uses runtime `as_of_date`, otherwise benchmark latest date | Emits `leader_lagging` freshness summary after frame load stage |
| Qullamaggie | `screeners/qullamaggie/screener.py` | market symbol list plus local OHLCV frames | Uses benchmark latest date for frame scoping | Emits `qullamaggie` freshness summary after threaded frame load |
| Weinstein Stage 2 | `screeners/weinstein_stage2/screener.py` | local daily bars converted into completed weekly bars | Uses runtime `as_of_date`, otherwise benchmark latest date | Emits `weinstein_stage2` freshness summary before weekly analysis |

## Operator Notes

- These diagnostics are non-blocking. A stale or future/partial latest row does not
  suppress screener output in this batch.
- If the default `results/` tree is locked by another process, run bounded validation
  with `INVEST_PROTO_RESULTS_DIR` set to a writable scratch root.
- Local live-data smoke remains an operator validation aid, not a deterministic unit
  test oracle.
