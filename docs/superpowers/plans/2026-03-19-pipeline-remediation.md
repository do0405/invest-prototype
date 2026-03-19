# Pipeline Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve operational safety, correctness, runtime efficiency, and architectural clarity of the data -> screening -> signal pipeline without breaking current market-specific output contracts.

**Architecture:** Fix correctness and operations first, then reduce avoidable runtime, then clean up orchestration seams, and only then split the signal engine internally. Preserve current output folders and CSV/JSON contracts while introducing stricter market validation and more honest pipeline status reporting.

**Tech Stack:** Python, pandas, pytest, yfinance, yahooquery, existing CLI/orchestrator modules, market-specific runtime helpers.

---

## Priority Order

1. Stop unsafe destructive behavior in the default CLI path.
2. Make orchestration status honest and machine-readable.
3. Make metadata refresh incremental so runtime scales with change, not universe size.
4. Enforce fail-fast market validation at public entrypoints.
5. Make scheduler freshness consistent with the new signal phase.
6. Split the signal engine into smaller internal seams after behavior is stabilized.

## Invariants

- Preserve explicit `us` and `kr` contracts. Invalid market values must fail fast.
- Keep `results/{market}/screeners/...` and `results/{market}/signals/...` output locations stable unless a migration is explicitly planned.
- Keep tests deterministic and local; do not introduce live-network dependencies into the test suite.
- Do not change screener selection semantics while performing orchestration and runtime cleanups.
- Preserve the current `screeners.signals` public aliases while refactoring internals.

### Task 1: Make Result Cleanup Explicit And Safe

**Files:**
- Modify: `main.py`
- Modify: `utils/file_cleanup.py`
- Test: `tests/test_main_market_resolution.py`
- Test: `tests/test_file_cleanup.py`

- [ ] **Step 1: Write the failing tests**
Add tests that prove normal `main.py` runs do not perform destructive cleanup by default, and that destructive cleanup only happens behind an explicit operator action such as a dedicated flag or task.

- [ ] **Step 2: Run the cleanup-focused tests to verify they fail**
Run: `.\.venv\Scripts\python -m pytest tests/test_main_market_resolution.py tests/test_file_cleanup.py -q`
Expected: FAIL because the current default path calls destructive cleanup with `dry_run=False`.

- [ ] **Step 3: Implement explicit cleanup control**
Change the default runtime path to non-destructive behavior.
Recommended shape:
`main.py` only performs a dry-run or informational scan during normal execution.
Add an explicit operator-only path such as `--cleanup-results` or a dedicated cleanup task for destructive pruning.

- [ ] **Step 4: Re-run the same tests**
Run: `.\.venv\Scripts\python -m pytest tests/test_main_market_resolution.py tests/test_file_cleanup.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**
Run:
```bash
git add main.py utils/file_cleanup.py tests/test_main_market_resolution.py tests/test_file_cleanup.py
git commit -m "fix: make result cleanup explicit and non-destructive by default"
```

### Task 2: Propagate Stage Failures Honestly Through Orchestration

**Files:**
- Modify: `orchestrator/tasks.py`
- Modify: `main.py`
- Test: `tests/test_orchestrator_tasks.py`
- Test: `tests/test_main_market_resolution.py`

- [ ] **Step 1: Write the failing tests**
Add tests that prove failed steps are surfaced as failed, not printed as completed, and that the top-level run summary does not report full success when one or more stages failed.

- [ ] **Step 2: Run the orchestration tests to verify they fail**
Run: `.\.venv\Scripts\python -m pytest tests/test_orchestrator_tasks.py tests/test_main_market_resolution.py -q`
Expected: FAIL because `_run_timed_step()` always prints completed and many task wrappers swallow failures into `None`.

- [ ] **Step 3: Implement structured step outcomes**
Introduce a small durable abstraction in `orchestrator/tasks.py`, for example a `TaskStepOutcome` dataclass or dict with:
`ok`, `label`, `market`, `elapsed_seconds`, `summary`, `error`.
Update `_run_timed_step()` and the public orchestration flows to aggregate outcomes and print an honest final summary.
Do not break existing task helpers that return dataframes or dicts; wrap them at the orchestration boundary.

- [ ] **Step 4: Decide and implement failure policy**
Recommended policy:
Single-stage failures should not crash the whole market pipeline immediately.
The overall process should return a degraded summary and top-level messaging should not print unconditional success.

- [ ] **Step 5: Re-run the orchestration tests**
Run: `.\.venv\Scripts\python -m pytest tests/test_orchestrator_tasks.py tests/test_main_market_resolution.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**
Run:
```bash
git add orchestrator/tasks.py main.py tests/test_orchestrator_tasks.py tests/test_main_market_resolution.py
git commit -m "fix: report pipeline failures honestly"
```

### Task 3: Make Metadata Refresh Incremental

**Files:**
- Modify: `data_collectors/stock_metadata_collector.py`
- Test: `tests/test_stock_metadata_collector.py`

- [ ] **Step 1: Write the failing tests**
Add tests for `get_missing_symbols()` and the top-level metadata flow so that:
cached fresh complete symbols are skipped,
missing symbols are fetched,
stale or incomplete rows are retried,
and stale cache reuse remains deterministic.

- [ ] **Step 2: Run the metadata tests to verify they fail**
Run: `.\.venv\Scripts\python -m pytest tests/test_stock_metadata_collector.py -q`
Expected: FAIL because `get_missing_symbols()` currently returns all symbols regardless of cache state.

- [ ] **Step 3: Implement cache-aware symbol selection**
Use `fetch_status`, `last_attempted_at`, and symbol presence to build a true delta set.
Recommended rule:
skip `complete` rows within freshness window,
retry `failed`, `pending`, `rate_limited`, and stale `partial_fast_info`,
always fetch missing symbols.

- [ ] **Step 4: Add runtime instrumentation**
Emit a summary like:
`total`, `cached_fresh`, `stale_retry`, `missing`, `to_fetch`.
Keep this at the collector boundary so runtime improvements are visible in logs.

- [ ] **Step 5: Re-run the metadata tests**
Run: `.\.venv\Scripts\python -m pytest tests/test_stock_metadata_collector.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**
Run:
```bash
git add data_collectors/stock_metadata_collector.py tests/test_stock_metadata_collector.py
git commit -m "perf: make metadata refresh incremental"
```

### Task 4: Enforce Fail-Fast Market Validation

**Files:**
- Modify: `main.py`
- Modify: `utils/market_runtime.py`
- Modify: `orchestrator/tasks.py`
- Test: `tests/test_main_market_resolution.py`
- Test: `tests/test_market_runtime.py`
- Test: `tests/test_orchestrator_tasks.py`

- [ ] **Step 1: Write the failing tests**
Add tests that invalid market inputs such as `jp`, `foo`, or mixed invalid CSV values fail explicitly instead of silently becoming `us`.

- [ ] **Step 2: Run the market validation tests to verify they fail**
Run: `.\.venv\Scripts\python -m pytest tests/test_main_market_resolution.py tests/test_market_runtime.py tests/test_orchestrator_tasks.py -q`
Expected: FAIL because current behavior falls back to `us`.

- [ ] **Step 3: Introduce an explicit validation seam**
Recommended structure:
add a strict helper such as `require_market_key()` in `utils/market_runtime.py` that raises on unsupported values.
Use it at public entrypoints in `main.py` and public task helpers.
Keep tolerant internal normalization only where legacy internal code genuinely needs it, and document those cases.

- [ ] **Step 4: Re-run the validation tests**
Run: `.\.venv\Scripts\python -m pytest tests/test_main_market_resolution.py tests/test_market_runtime.py tests/test_orchestrator_tasks.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**
Run:
```bash
git add main.py utils/market_runtime.py orchestrator/tasks.py tests/test_main_market_resolution.py tests/test_market_runtime.py tests/test_orchestrator_tasks.py
git commit -m "fix: enforce fail-fast market validation"
```

### Task 5: Unify Screening And Signal Freshness In Scheduler And CLI

**Files:**
- Modify: `orchestrator/tasks.py`
- Modify: `main.py`
- Test: `tests/test_orchestrator_tasks.py`
- Test: `tests/test_main_market_resolution.py`

- [ ] **Step 1: Write the failing tests**
Add tests that prove the scheduler keep-alive path and the `all` task both run the same post-screening signal phase, or both intentionally opt out with explicit naming.

- [ ] **Step 2: Run the freshness tests to verify they fail**
Run: `.\.venv\Scripts\python -m pytest tests/test_orchestrator_tasks.py tests/test_main_market_resolution.py -q`
Expected: FAIL because the scheduler currently updates screeners without updating signals.

- [ ] **Step 3: Introduce a durable market pipeline seam**
Recommended structure:
extract a shared helper such as `run_market_analysis_pipeline(markets, *, skip_data, include_signals)` or `run_post_screening_processes(markets)` and have both `main.py` and `run_scheduler()` call it.
This reduces orchestration duplication and keeps freshness rules consistent.

- [ ] **Step 4: Re-run the freshness tests**
Run: `.\.venv\Scripts\python -m pytest tests/test_orchestrator_tasks.py tests/test_main_market_resolution.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**
Run:
```bash
git add main.py orchestrator/tasks.py tests/test_orchestrator_tasks.py tests/test_main_market_resolution.py
git commit -m "fix: keep signals fresh across scheduler and cli flows"
```

### Task 6: Split The Signal Engine Internals Without Breaking Its Public Contract

**Files:**
- Modify: `screeners/signals/engine.py`
- Create: `screeners/signals/source_registry.py`
- Create: `screeners/signals/metrics.py`
- Create: `screeners/signals/cycle_store.py`
- Create: `screeners/signals/writers.py`
- Test: `tests/test_signal_engine_restoration.py`

- [ ] **Step 1: Write the failing regression coverage**
Before moving code, extend regression tests to lock down:
public aliases,
output filenames,
signal history persistence,
cycle persistence,
and representative `all_signals_v2` fields.

- [ ] **Step 2: Run the signal engine regression tests to verify baseline behavior**
Run: `.\.venv\Scripts\python -m pytest tests/test_signal_engine_restoration.py -q`
Expected: PASS before refactor, establishing the protection net.

- [ ] **Step 3: Extract one seam at a time**
Recommended order:
move source-registry loading into `source_registry.py`,
move metric construction into `metrics.py`,
move cycle read/write helpers into `cycle_store.py`,
move record writing and overlay/snapshot transforms into `writers.py`.
Keep `engine.py` as the orchestration shell and public import surface.

- [ ] **Step 4: Re-run regression tests after each extraction**
Run: `.\.venv\Scripts\python -m pytest tests/test_signal_engine_restoration.py -q`
Expected: PASS after every extraction step.

- [ ] **Step 5: Commit**
Run:
```bash
git add screeners/signals/engine.py screeners/signals/source_registry.py screeners/signals/metrics.py screeners/signals/cycle_store.py screeners/signals/writers.py tests/test_signal_engine_restoration.py
git commit -m "refactor: split signal engine into internal modules"
```

### Task 7: Add Runtime Baselines And Post-Change Verification

**Files:**
- Modify: `orchestrator/tasks.py`
- Modify: `data_collectors/stock_metadata_collector.py`
- Test: `tests/test_orchestrator_tasks.py`

- [ ] **Step 1: Record the current baseline manually**
Capture wall-clock timings for:
`main.py --task screening --market us --skip-data`
`main.py --task signals --market us`
metadata collection only for `us` and `kr`.
Store the measurements in the PR or implementation notes.

- [ ] **Step 2: Add lightweight timing summaries**
Ensure orchestration emits per-stage elapsed time and overall totals in a consistent, grep-friendly format.

- [ ] **Step 3: Run focused verification**
Run:
`.\.venv\Scripts\python -m pytest tests/test_signal_engine_restoration.py tests/test_orchestrator_tasks.py tests/test_main_market_resolution.py tests/test_market_runtime.py tests/test_stock_metadata_collector.py -q`
Expected: PASS.

- [ ] **Step 4: Run manual end-to-end smoke checks**
Run:
`.\.venv\Scripts\python main.py --task signals --market us`
`.\.venv\Scripts\python main.py --task screening --market us --skip-data`
`.\.venv\Scripts\python main.py --task all --market us --skip-data`
Verify:
screeners output is updated,
signals output is updated,
and log summaries reflect success or degraded status honestly.

- [ ] **Step 5: Commit**
Run:
```bash
git add orchestrator/tasks.py data_collectors/stock_metadata_collector.py tests/test_orchestrator_tasks.py
git commit -m "chore: add pipeline timing and verification baselines"
```

## Recommended Execution Notes

- Execute Tasks 1 through 5 before attempting Task 6. The signal engine refactor should not start until cleanup, failure propagation, market validation, and freshness rules are stable.
- The highest expected runtime win is Task 3. The highest operational risk reduction is Task 1 plus Task 2.
- If Task 4 reveals too many internal callers depending on fallback behavior, introduce `require_market_key()` first and migrate public entrypoints before removing legacy tolerance deeper inside the codebase.
