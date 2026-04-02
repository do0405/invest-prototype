# Cross-Repo Intent Overlap Audit

Date: `2026-04-02`

## Scope
- This audit reviews `invest-prototype-main` internally and `invest-prototype-main <-> market-intel-core` jointly for places where the same decision intent is implemented in different ways.
- The audit is read-only. It is grounded in current code, tests, config defaults, runtime entrypoints, compat bridges, and the small set of docs that still shape current semantics.
- `industry_key` is treated as the v1 canonical group unit. Finer peer-group or theme redesign is explicitly out of scope for this audit.

## Method
- The unit of analysis is an `intent cluster`, not a file.
- `Same intent` means one of the following:
  - the same user or operator judgment is being made in two places
  - the same state is expressed under different names, scores, or thresholds
  - one repo computes a canonical state while the other recomputes a local heuristic truth for the same purpose
  - the same contract is translated twice across bridge or compat layers
- Each overlap item is classified with:
  - `status`: `identical | partially-overlapping | divergent | bridge-only`
  - `risk`: `low | medium | high`
  - `recommended action`: `unify-core | keep-separate | bridge | deprecate`
  - `decision`: `Must Unify | Should Bridge | Safe To Keep Separate | Doc/Test Drift Only`
- Every item included in the matrix is backed by at least two evidence surfaces from:
  - live code
  - tests
  - config/default registry
  - compat/export/bridge contract
  - docs or research briefs when they still shape behavior

## Live Surface Inventory
### invest-prototype-main
- Runtime entrypoints:
  - `main.py`
  - `orchestrator/tasks.py`
- Active trade-facing surfaces:
  - `screeners/leader_lagging/screener.py`
  - `screeners/weinstein_stage2/screener.py`
  - `screeners/qullamaggie/core.py`
  - `screeners/signals/engine.py`
- Cross-repo consume seam:
  - `screeners/leader_core_bridge.py`
  - `screeners/signals/source_registry.py`
- Stated repo contract:
  - `README.md` scopes this repo around OHLCV collection, screening, and results output.
  - The same README says broader `regime`, `hazard`, `theme`, and `alert` logic live in `market-intel-core`.

### market-intel-core
- Live defaults:
  - `config/market_intel.yaml`
  - `market_intel/runtime/pipeline.py`
  - `market_intel/runtime/registry.py`
- Current default runtime stack:
  - `regime: snapshot_v1`
  - `hazard: multifactor_v4`
  - `industry_rotation: industry_rotation_v2`
  - `theme_rotation: theme_rotation_v2`
  - `theme: graph_v2`
  - `leader: leader_kernel_v1`
- Cross-repo export seam:
  - `market_intel/runtime/compat/invest_prototype/exporters.py`
- Canonical owner candidates:
  - market regime and readout
  - breadth and concentration
  - industry and theme rotation
  - leader/group/deterioration truth

## Overlap Heatmap
| Bucket | Overlap | Risk | Why It Matters | Current Best Direction |
|---|---|---|---|---|
| Leader / Group / Deterioration | High | High | Same leadership truth still exists as both canonical core state and local screener heuristics | `market-intel-core` owns truth, prototype consumes and overlays |
| Market State / Regime / Risk-On-Off | High | High | Four local prototype dialects compete with core regime state | Core owns global truth; prototype keeps tactical overlays only |
| Breadth / Participation / Concentration | High | High | Breadth is recomputed locally even though core already owns a breadth axis | Core owns market-level breadth truth |
| Rotation / Ranking | High | High | Group and rotation ranking still exist both locally and canonically | Core owns group/rotation truth |
| Setup / Actionability / Conviction | Medium | Medium | Similar words, different layer responsibilities | Keep separate by layer |
| Theme / Group Semantics | Medium | Medium | Local naming suggests theme logic where only sector/group heuristics exist | Keep theme ownership in core; deprecate misleading local names |
| Risk / Hazard / Breakdown / Sell-quality | Low | Low | These look similar but mostly serve different decisions | Keep separate; bridge only high-level context |
| Data Contract / Compat / Bridge | Medium | Medium | Necessary overlap, but only safe if versioned and explicit | Keep as explicit bridge surface |

## Audit Matrix
The full row-level matrix lives in:
- `docs/audits/2026-04-02-cross-repo-intent-overlap-matrix.csv`

High-signal summary:

| Bucket | Intent | Status | Risk | Recommended Action | Decision |
|---|---|---|---|---|---|
| Leader / Group / Deterioration | Leader group truth | partially-overlapping | high | unify-core | Must Unify |
| Leader / Group / Deterioration | Leader stock truth | divergent | high | unify-core | Must Unify |
| Leader / Group / Deterioration | Leader deterioration truth | bridge-only | medium | bridge | Should Bridge |
| Market State / Regime / Risk-On-Off | Global market aggressiveness gate | divergent | high | unify-core | Must Unify |
| Breadth / Participation / Concentration | Breadth and participation truth | partially-overlapping | high | unify-core | Must Unify |
| Rotation / Ranking | Industry and group rotation ranking | partially-overlapping | high | unify-core | Must Unify |
| Setup / Actionability / Conviction | Tradability and actionability | divergent | medium | keep-separate | Safe To Keep Separate |
| Setup / Actionability / Conviction | Position sizing recommendation | divergent | medium | deprecate | Should Bridge |
| Theme / Group Semantics | Theme-like grouping semantics | divergent | medium | deprecate | Doc/Test Drift Only |
| Risk / Hazard / Breakdown / Sell-quality | Risk warning and failure semantics | divergent | low | keep-separate | Safe To Keep Separate |
| Data Contract / Compat / Bridge | Leader compat export and consume seam | bridge-only | medium | bridge | Should Bridge |
| Data Contract / Compat / Bridge | Market normalization and path contract | partially-overlapping | medium | bridge | Should Bridge |
| Data Contract / Compat / Bridge | OHLCV normalization contract | partially-overlapping | medium | bridge | Should Bridge |

## Canonical Ownership Table
| Intent | Canonical Owner | Consumer / Boundary Note |
|---|---|---|
| US/KR OHLCV collection and upstream disk contract | `invest-prototype-main` | Core should consume through thin runtime/compat shims only |
| Stock metadata and raw screener candidate generation | `invest-prototype-main` | Core may read upstream artifacts but should not duplicate collector ownership |
| Entry timing, setup formation, breakout lifecycle, sell/trim execution state | `invest-prototype-main` | Trading-engine semantics, not canonical market truth |
| Global market regime and market-health interpretation | `market-intel-core` | Prototype should consume context rather than rebuild competing truth |
| Breadth, participation, concentration, leadership axes | `market-intel-core` | Prototype may keep tactical overlays but not alternate canonical truth |
| Industry rotation, theme rotation, leader group truth | `market-intel-core` | Prototype should consume canonical state |
| Leader stock truth and deterioration truth | `market-intel-core` | Prototype should consume canonical `leader_state` and `breakdown_status` |
| Cross-repo compatibility row shape | Shared seam | Core owns export shape; prototype owns consume path and fail-fast validation |
| Theme graph and theme taxonomy | `market-intel-core` | Local pseudo-theme heuristics should not present as canonical theme truth |
| User-facing sizing recommendation inside prototype | `invest-prototype-main` | Prototype still needs one canonical surface internally |

## Bucket Notes
### 1. Leader / Group / Deterioration
Evidence:
- Prototype:
  - `screeners/leader_lagging/screener.py`
  - `screeners/weinstein_stage2/screener.py`
  - `screeners/leader_core_bridge.py`
  - `tests/test_market_intel_bridge.py`
- Core:
  - `market_intel/core/engines/leader/leader_kernel_v1.py`
  - `market_intel/runtime/compat/invest_prototype/exporters.py`
  - `tests/test_market_intel_leader_kernel_v1.py`
  - `tests/test_market_intel_invest_prototype_exports.py`

Assessment:
- This is the strongest overlap seam in the system.
- `leader_lagging` and `weinstein` still carry local group or leader-facing overlays, but `market-intel-core` now exports canonical `group_state`, `leader_state`, and `breakdown_status`.
- This seam should converge on a single truth surface, which current code already points toward: `leader_kernel_v1`.

### 2. Market State / Regime / Risk-On-Off
Evidence:
- Prototype:
  - `screeners/leader_lagging/screener.py` local `Risk-On/Neutral/Risk-Off`
  - `screeners/weinstein_stage2/screener.py` local `MARKET_STAGE2_FAVORABLE`
  - `screeners/qullamaggie/core.py` local `RISK_ON_AGGRESSIVE/RISK_OFF`
  - `screeners/signals/engine.py` local `market_condition_state`
- Core:
  - `market_intel/core/engines/regime/snapshot_axes_v1.py`
  - `market_intel/core/engines/regime/snapshot_v1.py`
  - `market_intel/core/engines/regime/leader_health_overlay.py`
  - `config/market_intel.yaml`

Assessment:
- Prototype still contains multiple tactical market-state dialects.
- Core already has a proper regime owner and a leadership overlay path.
- The correct cut is: core owns global truth; prototype may keep screener-local tactical overlays if they are explicitly downstream consumers rather than competing truth generators.

### 3. Breadth / Participation / Concentration
Evidence:
- Prototype:
  - `screeners/leader_lagging/screener.py`
  - `screeners/weinstein_stage2/screener.py`
  - `screeners/qullamaggie/core.py`
- Core:
  - `market_intel/core/engines/breadth`
  - `market_intel/core/engines/regime/snapshot_axes_v1.py`

Assessment:
- Prototype breadth metrics are still valuable as local opportunity overlays.
- They should not remain alternate market-level truth if core already owns breadth and concentration state.

### 4. Rotation / Ranking
Evidence:
- Prototype:
  - `screeners/leader_lagging/screener.py` local `group_strength_score`, `group_rank`, `group_overlay_score`
  - `screeners/weinstein_stage2/screener.py` local `group_overlay_score`
- Core:
  - `market_intel/core/engines/industry_rotation/v2.py`
  - `market_intel/core/engines/leader/leader_kernel_v1.py`

Assessment:
- Local rotation and group overlay numbers are still useful.
- Canonical group ranking should stay in core; local overlays should remain explicitly local.

### 5. Setup / Actionability / Conviction
Evidence:
- Prototype:
  - `screeners/signals/engine.py`
  - `screeners/weinstein_stage2/screener.py`
- Core:
  - `market_intel/core/engines/leader/setup_v2.py`
  - `docs/research/inputs/2026-03-30-leader-setup-v2-research-brief.md`

Assessment:
- These are adjacent but not identical intents.
- Core `setup_v2` is an intelligence-layer actionability and ranking surface.
- Prototype conviction and timing are execution-layer entry surfaces.
- They should remain separate unless both repos are redesigned around a single actionability contract, which is not justified yet.

### 6. Theme / Group Semantics
Evidence:
- Prototype:
  - `screeners/leader_lagging/screener.py` local `same_theme` variable is sector equality, not canonical theme logic
  - local `group_name` and `industry_key` display fields
- Core:
  - `market_intel/core/engines/theme/graph_v2.py`
  - `market_intel/core/engines/theme_rotation/v2.py`

Assessment:
- This is mostly a naming drift problem.
- The local heuristic should not be called `theme` when core owns actual theme semantics.

### 7. Risk / Hazard / Breakdown / Sell-quality
Evidence:
- Prototype:
  - `screeners/signals/engine.py` warning and sell-quality fields
  - breakout failure and sell lifecycle in screeners
- Core:
  - `market_intel/core/engines/hazard/multifactor_v4.py`
  - `market_intel/core/engines/hazard/hazard_v5.py`

Assessment:
- These are not the same intent.
- Hazard is market/event risk.
- Prototype breakdown, warning, and sell-quality logic are trade-lifecycle hygiene.

### 8. Data Contract / Compat / Bridge
Evidence:
- Prototype:
  - `utils/market_data_contract.py`
  - `utils/market_runtime.py`
  - `screeners/leader_core_bridge.py`
  - `screeners/signals/source_registry.py`
- Core:
  - `market_intel/runtime/ohlcv_schema.py`
  - `market_intel/runtime/market_registry.py`
  - `market_intel/runtime/snapshot_inputs.py`
  - `market_intel/runtime/compat/invest_prototype/exporters.py`

Assessment:
- This overlap is intentional and unavoidable.
- The right answer is not single ownership of every helper, but explicit bridging and fail-fast versioning.

## Conflict Register
### Must Unify
- `Market state` is expressed in multiple local prototype dialects while core already owns a regime state machine.
- `Breadth / participation` is recomputed locally even though core already owns market-level breadth state.
- `Industry/group rotation` still has local truth producers despite existing canonical core rotation.
- `Leader stock truth` still exists as both local screener labels and canonical core leader states.

### Should Bridge
- `Leader deterioration` now has a clear canonical core surface, but local failure semantics still coexist in prototype outputs.
- `Compat export <-> source registry` is an intentional overlap seam and should remain explicit, versioned, and fail-fast.
- `Market normalization / runtime roots / OHLCV schema` are duplicated because both repos must read files, but upstream ownership should remain explicit.
- `Position sizing` exists in two prototype surfaces and should converge on one canonical user-facing path inside prototype.

### Safe To Keep Separate
- `setup_v2 actionability` vs `signals conviction` vs `weinstein timing_state`
- `hazard` vs local `risk_flag`, `buy_warning_summary`, and sell-quality state
- follower sympathy or pair-link logic vs market-intel leader truth

### Doc/Test Drift Only
- `market-intel-core/README.md` still says the default leader runtime is `setup_v2`.
- `market-intel-core/runtime/run_readiness.py` still describes `setup_v2` as the default leader path.
- `market-intel-core/docs/status/algorithm-readiness-matrix.md` still says live leader default is `setup_v2`.
- Current live truth is elsewhere:
  - `config/market_intel.yaml` -> `leader: leader_kernel_v1`
  - `market_intel/runtime/pipeline.py` -> default `leader` algorithm is `leader_kernel_v1`
  - `tests/test_market_intel_phase_d_defaults.py` asserts the default is `leader_kernel_v1`
- `invest-prototype-main/README.md` says market-state logic lives in core, but prototype still computes local tactical market-state heuristics. The implementation is defensible; the wording is too absolute.
- `leader_lagging` still uses `same_theme` for what is effectively sector equality.

## False Overlap Register
- `leader.setup_v2` is not the same thing as `signals conviction`.
  - Both rate tradability, but one is an intel-layer ranking/filter surface and the other is an execution overlay.
- `hazard` is not the same thing as `breakdown_status`.
  - Hazard is market/event risk.
  - Breakdown is leader deterioration.
- `hazard` is not the same thing as `sell-quality`.
  - Sell-quality is trade-management logic inside prototype.
- `theme_rotation` is not the same thing as prototype `industry_key/group_name`.
  - Prototype fields are grouping/display aids.
  - Core theme engines are canonical theme semantics.

## Priority Memo
### Phase A
- Freeze canonical ownership for:
  - market state
  - breadth
  - industry/group rotation
  - leader group / leader stock / deterioration
- Treat local prototype calculations on those seams as overlays only, not truth.

### Phase B
- Harden bridge surfaces:
  - leader compat export contract
  - schema and `as_of` validation
  - source registry merge rules
  - eventual read-only market-state and breadth bridge if prototype needs canonical context directly

### Phase C
- Keep separate:
  - `setup_v2 actionability`
  - `signals conviction`
  - `weinstein timing_state`
  - `TF/UG lifecycle`
  - follower sympathy logic
- These belong to different layers and should not be forced into one score.

### Phase D
- Later research:
  - finer peer-group discovery beyond `industry_key`
  - theme and peer taxonomy redesign
  - deeper contract convergence only if the upstream/downstream repo split changes

## Migration Recommendations
1. Keep `market-intel-core` as canonical owner for:
   - regime
   - breadth
   - rotation
   - leader/group/deterioration
2. Keep `invest-prototype-main` as canonical owner for:
   - OHLCV collection and raw upstream disk contract
   - candidate generation
   - entry timing
   - signal lifecycle
   - trade-facing overlays
3. Inside `invest-prototype-main`, do not retain multiple user-facing sizing surfaces long term.
   - `signals` sizing is currently the better canonical surface.
   - `qullamaggie` sizing hint should either become explicitly screener-local advisory output or be deprecated.
4. Fix doc/runtime drift in `market-intel-core` before further cross-repo integrations.
   - Otherwise future audits will keep finding conflicting statements about the live leader owner.
5. Rename misleading local fields where needed.
   - Especially `same_theme` in `leader_lagging`.

## Bottom Line
- The system should not become identical everywhere.
- The system should become identical where the same truth is being claimed:
  - global market state
  - breadth
  - rotation
  - leader/group/deterioration
- The system should stay intentionally different where the consumer layer is different:
  - entry timing
  - conviction
  - setup lifecycle
  - sell-quality and local trade warnings