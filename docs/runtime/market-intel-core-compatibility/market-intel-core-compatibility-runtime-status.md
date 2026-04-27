# Market Intel Core Compatibility Runtime Status

## Live Default

- Compatibility inputs must be same-day and schema-matched.
- `leader_core_v1` and `market_context_v1` are the active consume-side contracts.
- Default mode fails fast on missing, stale, or schema-mismatched compatibility artifacts.
- `--standalone` is an explicit bypass mode; it does not read compatibility artifacts even when they exist.
## Module Status

| Module | Implementation status | Current fidelity | Higher-end target |
| --- | --- | --- | --- |
| `leader-core-consume-seam` | `implemented` | High fidelity; the seam is narrow and strict. | Typed manifest plus broader provenance bundle. |
| `source-registry-and-compat-inputs` | `implemented`, `heuristic` | Medium-high fidelity; merge rules are explicit, style bonuses remain heuristic. | Clearer contract versioning and less downstream duplication. |

## Known Gaps

- No manifest-style version handshake yet.
- Downstream local heuristics still coexist with canonical upstream truth.
- Standalone bypass keeps screening and signals continuous, but it is heuristic and high-end deferred rather than canonical.
## Closest Tests

- `tests/test_market_intel_bridge.py`
- `tests/test_signals_package.py`
- `tests/test_screening_augment.py`
- `tests/test_signal_engine_restoration.py`
