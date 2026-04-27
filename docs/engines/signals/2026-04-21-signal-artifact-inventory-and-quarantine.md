# Signal Artifact Inventory And Quarantine

## Purpose

This document classifies signal and adjacent runtime artifacts before any cleanup.
It exists to prevent accidental removal of files that are still part of lifecycle,
diagnostic, compatibility, or operator workflows.

No artifact deletion is approved by this document. Removal requires a separate change
after proving that no orchestration, test, runtime, or operator workflow reads the file.

## Classification Model

- `canonical`: Current public or runtime contract. Keep stable.
- `compatibility`: Kept for old readers or transitional workflows. Quarantine before removal.
- `diagnostic/internal`: Useful for debugging, state restore, provenance, or operator audits.
  Not a public recommendation surface.
- `ambiguous-needs-quarantine`: Externally visible or historically used, but ownership is not
  clear enough for deletion.
- `stale/orphaned-candidate`: No known current reader. Still requires proof before removal.

## Current Signal Artifact Inventory

| Artifact | Producer | Classification | Handling |
| --- | --- | --- | --- |
| `buy_signals_all_symbols_v1` | `screeners/signals/writers.py` | `canonical` | Public today-only BUY projection for all OHLCV-backed symbols. Filename stays stable. |
| `sell_signals_all_symbols_v1` | `screeners/signals/writers.py` | `canonical` | Public today-only SELL/TRIM/EXIT projection for all OHLCV-backed symbols. Filename stays stable. |
| `buy_signals_screened_symbols_v1` | `screeners/signals/writers.py` | `canonical` | Public today-only BUY projection for screened symbols. Filename stays stable. |
| `sell_signals_screened_symbols_v1` | `screeners/signals/writers.py` | `canonical` | Public today-only SELL/TRIM/EXIT projection for screened symbols. Filename stays stable. |
| `all_signals_v2` | `screeners/signals/writers.py` | `diagnostic/internal` | Full same-run signal surface for debugging and traceability. Not the public buy/sell recommendation contract. |
| `trend_following_events_v2` | `screeners/signals/writers.py` | `diagnostic/internal` | Family-specific event audit. Keep additive. |
| `trend_following_states_v2` | `screeners/signals/writers.py` | `diagnostic/internal` | Family-specific state/level provenance. Keep additive. |
| `ultimate_growth_events_v2` | `screeners/signals/writers.py` | `diagnostic/internal` | Family-specific event audit. Keep additive. |
| `ultimate_growth_states_v2` | `screeners/signals/writers.py` | `diagnostic/internal` | Family-specific state/level provenance. Keep additive. |
| `ug_strategy_combos_v2` | `screeners/signals/writers.py` | `diagnostic/internal` | UG combo provenance, not a standalone BUY authority. |
| `open_family_cycles` | `screeners/signals/writers.py` and `cycle_store.py` | `canonical` | Runtime lifecycle state. Do not delete or quarantine. |
| `signal_event_history` | `screeners/signals/cycle_store.py` | `diagnostic/internal` | Event history for cooldown and lifecycle provenance. Not a public recommendation output. |
| `signal_state_history` | `screeners/signals/cycle_store.py` | `diagnostic/internal` | State/aux history. Separate from events by design. |
| `peg_event_history` | `screeners/signals/cycle_store.py` | `diagnostic/internal` | PEG event follow-up support. Not a past BUY/SELL lookup surface. |
| `screen_signal_diagnostics` | `screeners/signals/writers.py` | `diagnostic/internal` | Debugging and source explanation. Keep additive. |
| `earnings_provider_diagnostics` | `screeners/signals/writers.py` | `diagnostic/internal` | Provider diagnostics. Keep out of recommendation semantics. |
| `signal_universe_snapshot` | `screeners/signals/writers.py` | `diagnostic/internal` | Universe/source audit. Not BUY/SELL authority. |
| `source_registry_summary.json` | `screeners/signals/writers.py` | `diagnostic/internal` | Run summary for source disposition. Keep additive. |
| `signal_summary.json` | `screeners/signals/writers.py` | `diagnostic/internal` | Run counts and metadata. Keep additive. |
| `source_registry_snapshot.json` | `orchestrator/tasks.py` | `canonical` | Runtime handoff between screening, signals, and augment. |
| `peg_imminent_raw` | `screeners/signals/engine.py` | `diagnostic/internal` | PEG input audit and current-run context source. Keep; not a public recommendation surface. |
| `peg_ready` | `screeners/signals/engine.py` | `diagnostic/internal` | PEG eligibility source consumed by the same signal run through return maps. Keep stable while PEG integration exists. |
| `runtime_state.json` | `orchestrator/tasks.py` | `canonical` | Operator stop-point state. Keep stable/additive. |
| `runtime_profile.json` | `orchestrator/tasks.py` | `diagnostic/internal` | Runtime profile and step timing. Keep additive. |
| `collector_run_state.json` | `data_collector.py` | `canonical` | Collector resumability state. Keep stable/additive. |

## Past Signal Lookup Policy

Public/operator-facing past-N-day BUY/SELL lookup is retired.

- `buy_signals_*` and `sell_signals_*` must contain only rows where `signal_date`
  equals the current scan `as_of_date`.
- Fields that imply public past signal lookup, such as `lead_buy_found_10d` or
  `lead_buy_found_15d`, must not appear in public buy/sell artifacts.
- Historical files may remain only as internal lifecycle, cooldown, PEG follow-up,
  state provenance, or diagnostic artifacts.
- Internal history must not be presented as a current recommendation surface.

Current code search did not find an active public producer for the retired past-N-day
BUY/SELL lookup. Existing regression coverage asserts the public buy/sell artifacts are
today-only and do not include the old `lead_buy_found_*` fields.

## Quarantine Rules

- Compatibility and ambiguous artifacts may be documented as quarantined before removal.
- Quarantine requires a reader inventory:
  - Python imports and path reads;
  - tests;
  - orchestration handoffs;
  - documented operator workflows;
  - known downstream scripts.
- Removal requires a follow-up implementation plan and focused regression coverage.
- Runtime-generated `results/` files are operator data. Do not bulk delete them in a
  normal cleanup batch.

## 2026-04-22 Reader Inventory Result

- No signal artifact was proven stale enough for same-batch producer removal.
- `peg_imminent_raw` and `peg_ready` are not deletion candidates: `PEGImminentScreener`
  returns both maps to `MultiScreenerSignalEngine._load_or_run_peg_screen()`, and
  `peg_ready_map` feeds universe scope, source disposition, signal universe rows, and
  PEG-aware buy context.
- `open_family_cycles`, `signal_event_history`, `signal_state_history`, and
  `peg_event_history` remain runtime/cycle state or lifecycle provenance and must not be
  removed.
- Screening and augment outputs such as `pattern_included_candidates`,
  `pattern_excluded_pool`, `merged_candidate_pool`, `chronos2_rerank`, and
  `timesfm2p5_rerank` have active readers or documented operator/test usage. They are
  outside this signal artifact deletion batch.
- Proven-stale removals in this batch: none.

## Next Review Targets

- Revisit whether PEG CSV outputs should be kept as operator diagnostics or folded into
  a future source-registry contract. Do not remove the current-run return-map behavior.
- Inventory legacy screener timestamped outputs separately from signal runtime outputs.
- Add a small artifact-manifest test only if writer drift becomes a recurring problem.
