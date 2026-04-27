# Leader/Lagging Screener V2 Operational Validation Review

Date: 2026-04-22
Branch: `codex/pipeline-remediation-base`
Base commit observed: `a83875a19`
Scope: V2/P0/P2 operational validation, documentation consistency, and handoff record.

## Summary

The leader/lagging screener V2 implementation is validated at the unit, package, local-data smoke, import, CLI smoke, and full-regression levels. The implemented runtime contract now centers on:

- `leader_rs_state`, `leader_tier`, `entry_suitability`, canonical `label`, `legacy_label`.
- RS/Hidden RS/structure diagnostics and confidence fields.
- follower/pair confidence diagnostics without changing BUY-capable policy.
- final-leader quality artifacts separated from pre-final candidate quality artifacts.
- conservative leader-threshold tuning diagnostics, emitted as internal artifacts and calibration metadata.

No BUY/SELL contract change was made in this validation pass. Conservative tuning is leader-only, bounded, diagnostic-led, and does not change follower thresholds or public source eligibility.

## Test Results

| Check | Command | Result |
| --- | --- | --- |
| Leader/lagging targeted tests | `.\.venv\Scripts\python -m pytest tests/test_leader_lagging_screener.py -q` | `24 passed in 20.83s` |
| Signals package/source registry tests | `.\.venv\Scripts\python -m pytest tests/test_signals_package.py -q` | `9 passed in 1.49s` |
| Local-data leader smoke tests | `.\.venv\Scripts\python -m pytest tests/test_leader_lagging_screener.py -m local_data -q` | `2 passed, 22 deselected in 3.88s` |
| Import check | `.\.venv\Scripts\python -B -c "... import modules ..."` | `ok` |
| Runtime/bridge/leader targeted tests | `.\.venv\Scripts\python -m pytest tests/test_market_runtime.py tests/test_io_utils.py tests/test_market_intel_bridge.py tests/test_leader_lagging_screener.py -q` | `53 passed` |
| Orchestrator/main dispatch tests | `.\.venv\Scripts\python -m pytest tests/test_orchestrator_tasks.py tests/test_main_market_resolution.py -q` | `49 passed` |
| Market data/runtime context regression check | `.\.venv\Scripts\python -m pytest tests/test_market_data_contract.py tests/test_orchestrator_tasks.py -q` | `32 passed in 3.30s` |
| Full regression | `.\.venv\Scripts\python -m pytest -q` | `373 passed in 226.26s` |

## Contract Searches

| Check | Result |
| --- | --- |
| stale planning wording in `docs/engines/leader-lagging` | no matches for legacy planning-only phrases after the cleanup |
| lifecycle/prior-cycle/rotation candidate fields | only negative contract references remain in tests/docs; no runtime/source context re-entry found |
| V2 diagnostics fields | expected implementation, test, and documentation matches found for `leader_candidate_quality`, `follower_confidence_score`, `pair_evidence_confidence`, `structure_reject_reason_codes` |

## Runtime Smoke

Requested bounded smoke now uses the isolated output-root override added in the stabilization pass:

```powershell
$env:INVEST_PROTO_RESULTS_DIR='data\_runtime_smoke\results'
$env:INVEST_PROTO_RUNTIME_SYMBOL_LIMIT='4'
python main.py --task screening --skip-data --market both --standalone
```

Result: US/KR standalone screening completed with redirected runtime output. Public filenames and V2 diagnostic artifacts were present under `data\_runtime_smoke\results\{market}\screeners\leader_lagging`.

Direct standalone leader smoke:

```powershell
$env:INVEST_PROTO_RESULTS_DIR='data\_runtime_smoke\results'
$env:INVEST_PROTO_RUNTIME_SYMBOL_LIMIT='4'
python main.py --task leader --skip-data --market both --standalone
```

Result: US/KR standalone leader task completed with redirected runtime output. The previous default `results\` permission/file-lock issue is now avoidable through `INVEST_PROTO_RESULTS_DIR`; default paths remain unchanged for normal operation.

Default non-standalone leader mode still intentionally requires current `market-intel-core` compatibility artifacts. Missing compat files now fail fast with expected paths and remediation guidance for `--standalone` or `MARKET_INTEL_COMPAT_RESULTS_ROOT`.

## Artifact Sanity

Validated via isolated local-data runtime artifact:

Path: `data\_test\runtime\leader\leader_lagging_run`

Required public and diagnostic files present:

- `leaders.csv/json`
- `followers.csv/json`
- `leader_follower_pairs.csv/json`
- `group_dashboard.csv/json`
- `pattern_excluded_pool.csv/json`
- `pattern_included_candidates.csv/json`
- `market_summary.json`
- `actual_data_calibration.json`
- `leader_quality_diagnostics.csv/json`
- `leader_quality_summary.json`
- `leader_candidate_quality_diagnostics.csv/json`
- `leader_candidate_quality_summary.json`
- `leader_threshold_tuning_report.csv/json`

Observed quality split:

- `leaders.csv` rows: 1
- `leader_quality_diagnostics.csv` rows: 1
- `leader_candidate_quality_diagnostics.csv` rows: 4
- `market_summary.leader_quality.leader_count`: 1
- `market_summary.leader_candidate_quality.leader_count`: 4

This satisfies the P0 contract: `leader_quality_*` is final-leaders-only, while `leader_candidate_quality_*` preserves rejected/pre-final candidates and reason-code diagnostics.

## Documentation Consistency

Updated `docs/engines/leader-lagging/leader-lagging-screening-upgrade/*` from planning-only language to implemented-runtime language.

Current negative contract:

- `leader_lifecycle_phase`
- `prior_cycle_exclusion_score`
- `rotation_candidate_score`
- `rotation_state`

These remain excluded/deferred and are not runtime fields or source-registry context. `leader_rs_state` is the implemented RS-state field.

Current artifact contract:

- final persisted leaders: `leader_quality_diagnostics.csv/json`, `leader_quality_summary.json`
- pre-final candidates: `leader_candidate_quality_diagnostics.csv/json`, `leader_candidate_quality_summary.json`

## Regression Fixes

No new leader/lagging or source-registry behavioral regression was found. A transient full-suite collection failure around `utils.market_data_contract` was rechecked after inspecting the dirty worktree; the expected symbols were present, and the closest regression set now passes (`32 passed`).

## Non-Blocking Risks

- Default `results\...` paths may still be locked or permission-restricted in the local operator environment. `INVEST_PROTO_RESULTS_DIR` is now the supported non-destructive bypass for runtime smoke and alternate deployments.
- Default non-standalone leader task requires `market-intel-core` compat artifacts. Missing artifacts are expected to block overlay mode.
- Documentation still contains some older non-ASCII/encoding artifacts in legacy sections. They were not expanded or rewritten because the validation scope was stale-contract cleanup, not broad documentation normalization.
- Backtest-quality review is still deferred. Current validation proves contract, diagnostics, bounded tuning mechanics, and runtime behavior under tests, not investment performance.

## Recommended Next Work

1. Generate fresh `market-intel-core` compat artifacts, then rerun non-standalone leader task.
2. Review real US/KR leader outputs by label, confidence bucket, reject reason, extended reason, and tuning recommendation.
3. Compare pre/post tuning distributions on several trading dates before accepting any more aggressive threshold changes.
4. Keep BUY/SELL source policy unchanged until replay/backtest evidence supports a change.

## Stabilization Addendum

Same-day follow-up implemented the operational stabilization path:

- `INVEST_PROTO_RESULTS_DIR` can redirect market runtime output under a writable root while preserving default `results\` behavior.
- CLI startup now preflights market runtime/screener/signal output directories before long-running work.
- write fallback errors now report both the primary and timestamped fallback paths when both are blocked.
- missing `market-intel-core` compat errors now include the expected path plus `--standalone` and `MARKET_INTEL_COMPAT_RESULTS_ROOT` remediation.
- `leader_threshold_tuning_report.csv/json` is emitted as an internal diagnostic artifact.
- `actual_data_calibration.json` includes additive leader tuning metadata: `leader_tuning_applied`, `leader_tuning_eligible`, `leader_tuning_reason_codes`, and `leader_tuning_adjustments`.

Validation after the addendum:

| Check | Result |
| --- | --- |
| Targeted runtime/bridge/leader tests | `53 passed` |
| Signals package tests | `9 passed` |
| Orchestrator/main dispatch tests | `49 passed` |
| Full regression | `373 passed in 226.26s` |
| `--task leader --standalone` smoke with `INVEST_PROTO_RESULTS_DIR=data\_runtime_smoke\results` | US/KR completed |
| `--task screening --standalone` smoke with `INVEST_PROTO_RESULTS_DIR=data\_runtime_smoke\results` | US/KR completed |

The default non-standalone path still intentionally requires current `market-intel-core` compatibility artifacts.
