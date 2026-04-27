# Buy/Sell Replay Golden Set Plan

## Purpose

The replay golden set should reduce false positives and lock signal semantics with
deterministic local cases before any live-data evaluation.

This follows the artifact inventory and daily-bar closure audit. It assumes public
buy/sell artifacts are today-only and that internal history remains available for cycle,
cooldown, and PEG follow-up behavior.

## Golden Case Categories

- All-symbol vs screened-symbol projection consistency.
- Restored open cycle -> trim -> close.
- Same-run trim plus final close:
  - emitted rows may explain all detected conditions;
  - persisted cycle state must resolve closed when a final close exists.
- Trend TP1 and TP2 position-unit accounting.
- Trend trailing/protected stop never-down refresh.
- UG Green state without BO/PBB/MR Long entry event does not create BUY.
- UG breakout requires Green + NH + breakout readiness + no EMA turn-down.
- UG PBB is BUY on Green, WATCH on Orange, and not BUY on Red.
- Negative Sigma score clamps final `signal_score` at zero and produces Red state.
- Breakdown forces Red even when raw GP components are strong.
- Weak market keeps BUY visible but records `MARKET_WEAK`, conviction warning, and
  sizing downgrade.
- Explicit `as_of_date` replay cannot see future symbol rows.
- Public buy/sell outputs do not expose past-N-day lookup fields.

## Data Rules

- Use synthetic OHLCV, source registry, earnings, and market-context fixtures.
- Do not use live provider or network calls.
- Keep fixtures minimal enough that the expected signal is obvious from the fixture.
- Use separate cases for algorithmic recent-context rules and public past-signal lookup.
  Recent-context calculations such as squeeze or Orange readiness are valid strategy
  inputs; public past BUY/SELL lookup is not.

## Test Placement

- Put signal lifecycle and artifact projection cases near existing signal engine tests.
- Put package/writer contract cases near existing signal package tests.
- Add daily closure cases only after the closure audit identifies the exact local
  contract to enforce.

## Acceptance Criteria

- Public buy/sell artifacts remain today-only.
- Internal histories continue to support open-cycle restore, UG cooldown, and PEG
  follow-up.
- Each major Trend/UG lifecycle effect has a deterministic replay case.
- False-positive sensitive gates, especially UG state-vs-entry and PBB Orange WATCH,
  are covered without live data.

## Implemented Deterministic Coverage

- `tests/test_signal_replay_accuracy.py` covers default completed-session clipping,
  explicit replay `as_of`, standalone benchmark scoping, public today-only projection,
  and public stripping of internal gate diagnostics.
- `tests/test_signal_patterns.py` covers pocket pivot, PBB/PBS/MR band reversion, and
  ATR/Chandelier exit-pressure helpers.
- Live-data evaluation remains local-only operator validation. It must not become a
  fixed unit-test oracle because local `data/` and `results/` are operator state.
