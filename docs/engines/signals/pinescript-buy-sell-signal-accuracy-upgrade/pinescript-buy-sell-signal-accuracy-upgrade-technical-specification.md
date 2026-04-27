# PineScript Buy/Sell Signal Accuracy Upgrade Technical Specification

- status: future implementation specification
- as_of: 2026-04-22
- scope: documentation only; no code change in this pass

## Future Code Surface

Future implementation should keep edits scoped to the signals subsystem and closest tests.

| Surface | Future role |
| --- | --- |
| `screeners/signals/engine.py` | Keep orchestration, row creation, cycle lifecycle, and public projection. Move dense pattern calculations out of `_build_metrics()` only when tests cover the extracted helpers. |
| `screeners/signals/patterns.py` | Candidate new pure helper module for W, VCP, squeeze, tight, pocket pivot, pullback, breakout, band reversion, and exit pressure features. |
| `screeners/signals/decision_score.py` | Candidate new pure helper module for additive `decision_score`, `decision_grade`, and `decision_rule_ids`. |
| `tests/test_signal_engine_restoration.py` | Main regression target for signal semantics, lifecycle, and golden fixtures. |
| `tests/test_signals_package.py` | Package/writer/public artifact contract tests. |
| `tests/test_signal_replay_accuracy.py` | Candidate future replay comparison tests if replay helper becomes large enough. |

## Module Boundary

`engine.py` should continue to own:

- source registry loading and scoping;
- market truth overlay consumption;
- event/state row creation;
- public output projection;
- cycle mutation and persistence;
- compatibility fields.

Future helper modules should own only pure calculations from local inputs:

- no file IO;
- no network;
- no runtime context mutation;
- no cycle persistence;
- no public artifact writes.

## Candidate APIs

```python
def build_pattern_features(frame, metrics_seed, *, market, symbol) -> dict[str, object]:
    """Return deterministic local pattern and quality features."""

def build_decision_score(metrics, row_context) -> dict[str, object]:
    """Return additive decision_score, decision_grade, and rule provenance."""

def compare_signal_replay(old_rows, new_rows, frames, *, horizons=(5, 10, 20)) -> list[dict[str, object]]:
    """Return local old-vs-new replay diagnostics."""
```

These APIs are intentionally conceptual. Exact signatures should follow the local style after
the tests are written.

## Source Confidence Enum

| Value | Meaning | Storage note |
| --- | --- | --- |
| `A` | Official TradingView reference or auditable open-source Pine with clear conditions. | Safe as `reference_confidence`. |
| `B` | Protected/closed source with specific description aligned to original concept. | Use only after independent formula rewrite. |
| `C` | Useful idea with insufficient implementation detail. | Reference-only. |
| `D` | Repaint/future leak/overfit/unclear. | Reject. |

## Adoption Decision Enum

| Value | Meaning |
| --- | --- |
| `direct-replace-candidate` | Strong enough for a future trigger replacement after replay passes. |
| `enhance-before-replace` | Useful but needs additional filters, source confirmation, or tests. |
| `reference-only` | Keep for context or explanation, not implementation. |
| `defer` | Potentially useful but not in near-term scope. |
| `reject` | Do not use because of repaint/future leak/overfit/source weakness. |

## Additive Field Contract

| Field | Type | First output surface | Meaning |
| --- | --- | --- | --- |
| `decision_score` | float or null | internal rows first | Composite score across trend, pattern, volume, risk, source, and market context. |
| `decision_grade` | string | internal rows first | S/A/B/C/D score label. |
| `pattern_quality_score` | float or null | internal rows first | Pattern-specific quality for W/VCP/squeeze/pullback/breakout. |
| `exit_pressure_score` | float or null | internal rows first | Sell/trim/exit pressure score. |
| `decision_rule_ids` | list[str] | internal rows first | Pine/reference-derived rule provenance. |
| `reference_confidence` | string | internal rows first | A/B/C/D reference confidence. |
| `repaint_risk` | string | diagnostics | Confirmation/repaint risk classification. |

Public `buy_signals_*` and `sell_signals_*` should receive these fields only after a
compatibility review confirms downstream consumers tolerate them.

## Current-To-Future Mapping

| Current area | Current function | Future helper target |
| --- | --- | --- |
| W detector | `_detect_double_bottom()` | `patterns.detect_w_pattern()` |
| VCP/squeeze/tight | `_build_metrics()` | `patterns.detect_compression_patterns()` |
| Dry volume and pocket pivot | `_dry_volume()`, `_build_metrics()` | `patterns.detect_volume_quality()` |
| Pullback profile | `_build_metrics()` | `patterns.score_pullback_quality()` |
| Breakout readiness | `_build_metrics()` | `patterns.score_breakout_quality()` |
| PBB/PBS/MR | `_build_metrics()` | `patterns.score_band_reversion()` |
| Momentum | `_build_metrics()`, `_trend_buy_events()`, `_trend_sell_events()` | `patterns.score_momentum_context()` |
| Exit pressure | `_trend_sell_events()`, `_ug_sell_events()` | `patterns.score_exit_pressure()` |
| Final score | `_trend_conviction()`, `_ug_dashboard_profile()`, overlays | `decision_score.build_decision_score()` |

## Technical Compatibility Rules

- Do not rename existing `signal_code` values.
- Do not remove `indicator_dog_rule_ids`.
- Do not change `cycle_effect` semantics:
  - BUY opens cycle;
  - add-on uses `ADD`;
  - TP/MR short uses `TRIM`;
  - breakdown/trailing/PBS/final sell uses `CLOSE`;
  - state rows use `STATE`.
- Do not restore public past-N-day BUY/SELL lookup.
- Do not make UG Green state an automatic BUY.
- Do not make weak market an unapproved hard suppressor.
- Do not put live network calls in tests.

## Reference Extraction Rules

- Prefer descriptions and formulas over copying source code.
- If a Pine script uses pivot detection, document confirmation lag.
- If a Pine script uses higher timeframe requests, require a local confirmed-bar equivalent.
- If a Pine script describes backtest results without risk logic, do not use its score as an accuracy claim.
- If a reference is protected or invite-only, grade at most `B` and rewrite from original concept.
- Every adopted formula needs a deterministic local fixture.

## Future Test Placement

| Test group | File | Cases |
| --- | --- | --- |
| Pattern helpers | `tests/test_signal_engine_restoration.py` or future helper test | W pending/confirmed/invalid, VCP contraction sequence, squeeze release, pocket pivot. |
| Signal rows | `tests/test_signal_engine_restoration.py` | TF regular, TF breakout, TF momentum, PEG pullback/rebreak, UG BO/PBB/MR/PBS. |
| Cycle lifecycle | `tests/test_signal_engine_restoration.py` | OPEN, ADD, TRIM, CLOSE, same-run trim+close, restored cycle. |
| Artifact contract | `tests/test_signals_package.py` | Public filenames, today-only, no retired past lookup fields. |
| Replay | future `tests/test_signal_replay_accuracy.py` | Old-vs-new metrics on deterministic local fixtures. |

## Verification Commands

Focused verification:

```powershell
.\.venv\Scripts\python -m pytest tests\test_signal_engine_restoration.py tests\test_signals_package.py -q
```

Full repo verification after implementation:

```powershell
.\.venv\Scripts\python -m pytest -q
```

Documentation-only verification for this batch:

```powershell
rg --line-number "direct-replace-candidate|enhance-before-replace|reference-only|defer|reject" docs/engines/signals/pinescript-buy-sell-signal-accuracy-upgrade
```

## Non-Goals

- No implementation in this pass.
- No schema migration in this pass.
- No docs-driven automatic runtime behavior.
- No general-purpose Pine interpreter.
- No broad refactor of `screeners/signals/engine.py` before helper tests exist.

