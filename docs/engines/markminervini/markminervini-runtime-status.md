# Markminervini Runtime Status

## Live Default

- `with_rs.csv` is the base technical artifact.
- Integrated screening writes pattern-aware and patternless variants plus actionable subsets.
## Module Status

| Module | Implementation status | Current fidelity | Higher-end target |
| --- | --- | --- | --- |
| `technical-and-pattern-screening` | `implemented`, `heuristic` | Medium-high fidelity for live code; strategy notes are higher-end reference only. | Clearer pattern grammar with stronger explainability and state lineage. |
| `financial-and-integrated-screening` | `implemented`, `proxy-backed`, `high-end deferred` | Medium fidelity; real and test-backed, but not fully calibrated. | Explicit calibration lineage and cleaner separation of raw metrics vs derived rank semantics. |
| `ticker-tracking-and-artifacts` | `implemented` | Medium fidelity; operationally useful but simple. | Richer candidate lineage and state-transition history. |

## Known Gaps

- Pattern and financial evidence remain heuristic/cache-backed.
- Legacy PRDs describe higher-end semantics beyond current code.
## Closest Tests

- `tests/test_markminervini_screener.py`
- `tests/test_markminervini_pattern_analyzer.py`
- `tests/test_markminervini_financial_cache.py`
- `tests/test_markminervini_integrated_ohlcv_cache.py`
