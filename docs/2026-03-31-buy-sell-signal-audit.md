# Buy/Sell Signal Audit

Date: 2026-03-31

## Scope
- 기준은 문서가 아니라 현재 코드 계약이다.
- 주 조사 대상은 `screeners/signals/engine.py`, `screeners/signals/cycle_store.py`, `screeners/signals/writers.py`, `tests/test_signal_engine_restoration.py`다.
- 주문/체결 계층은 범위 밖이다. 여기서는 시그널 생성, cycle 상태전이, persisted output, 회귀테스트만 본다.

## Execution Summary
- 관련 회귀테스트 baseline과 최종 확인을 모두 실행했다.
- 최종 검증 명령:
  - `.\.venv\Scripts\python -m pytest -q tests\test_signal_engine_restoration.py tests\test_main_market_resolution.py tests\test_orchestrator_tasks.py tests\test_market_runtime.py`
  - 결과: `65 passed`
- 직접 확인은 테스트와 별도로 엔진 메서드를 수동 호출해서 진행했다.
  - 확인한 대표 경로:
    - `TF_BUY_BREAKOUT`, `TF_BUY_MOMENTUM`
    - `TF_PEG_EVENT`, `TF_BUY_PEG_PULLBACK`, `TF_BUY_PEG_REBREAK`
    - `TF_SELL_BREAKDOWN`, `TF_SELL_CHANNEL_BREAK`, `TF_SELL_TRAILING_BREAK`
    - `TF_ADDON_SLOT2_READY`
    - `UG_STATE_ORANGE`, `UG_STATE_RED`
    - `UG_COMBO_SQUEEZE`

## Executive Summary
- 현재 active inventory는 `_LABELS` 기준 45개 코드다.
- 이 45개 코드 안에서 `미구현`이나 `dead`인 신호는 찾지 못했다.
- 45개 코드 모두 생성 경로가 있고, output 경로가 있고, 이번 audit 기준으로 직접 회귀테스트까지 붙였다.
- 현재 부족한 부분은 `신호 미구현`보다 `정책 결정이 아직 열려 있는 부분`이다.
  - 같은 symbol에서 같은 엔진 내 여러 family 신호가 동시에 나올 수 있다.
  - `signal_history`는 `EVENT`만 저장하므로 state/aux/combo는 이력 CSV에 안 남는다.
  - active inventory 밖에는 helper/legacy 문자열이 남아 있다.

## Important Behavior Notes
- `TF`는 family 간 배타성이 없다. 직접 확인 결과 breakout snapshot 하나에서 `TF_BUY_BREAKOUT`와 `TF_BUY_MOMENTUM`이 같이 나왔다.
- `TF_PEG`도 배타성이 없다. PEG pullback/rebreak 조건이 맞으면 `TF_BUY_PEG_*`가 추가되고, 같은 snapshot에서 breakout/momentum이 같이 남을 수 있다.
- `UG`도 family 간 배타성이 없다. 직접 확인 결과 한 snapshot에서 `UG_BUY_SQUEEZE_BREAKOUT`, `UG_BUY_PBB`, `UG_BUY_MR_LONG`이 함께 성립할 수 있다.
- `signal_history`는 `cycle_store.persist_signal_history()`에서 `signal_kind == "EVENT"`만 저장한다. 따라서 `TF_AGGRESSIVE_ALERT`, `UG_STATE_*`, `UG_COMBO_*`, level/add-on state는 output CSV에는 남지만 history CSV에는 남지 않는다.
- `reference_exit_signal`은 현재 `UG_PULLBACK -> UG_SELL_MR_SHORT_OR_PBS` 한 경우만 의미 있게 강제된다. 다른 family에선 정규화 과정에서 빈 문자열로 버린다.

## Signal Matrix

Output legend:
- `TE`: `trend_following_events` + `trend_following_events_v2`
- `TS`: `trend_following_states` + `trend_following_states_v2`
- `UE`: `ultimate_growth_events` + `ultimate_growth_events_v2`
- `US`: `ultimate_growth_states` + `ultimate_growth_states_v2`
- `UC`: `ug_strategy_combos_v2`
- `ALL`: `all_signals` + `all_signals_v2`
- `OC`: `open_family_cycles`
- `HIST`: `signal_history`

Cycle legend:
- `open`: 새 cycle 생성
- `update`: 기존 cycle 상태 업데이트
- `close`: cycle 종료
- `none`: cycle에는 영향 없음

### TREND Event Signals

| engine | family | signal_code | action_type | 생성 조건 위치 | cycle 영향 | persisted output | 테스트 상태 | 판정 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TREND | TF_REGULAR_PULLBACK | TF_BUY_REGULAR | BUY/WATCH | `_trend_buy_events()` regular pullback trigger | open | TE, ALL, HIST, OC | direct | 구현완료 |
| TREND | TF_BREAKOUT | TF_BUY_BREAKOUT | BUY/WATCH | `_trend_buy_events()` breakout trigger | open | TE, ALL, HIST, OC | direct | 구현완료 |
| TREND | TF_PEG | TF_BUY_PEG_PULLBACK | BUY/WATCH | `_trend_buy_events()` PEG pullback trigger | open | TE, ALL, HIST, OC | direct | 구현완료 |
| TREND | TF_PEG | TF_BUY_PEG_REBREAK | BUY/WATCH | `_trend_buy_events()` PEG rebreak trigger | open | TE, ALL, HIST, OC | direct | 구현완료 |
| TREND | TF_MOMENTUM | TF_BUY_MOMENTUM | BUY/WATCH | `_trend_buy_events()` momentum trigger | open | TE, ALL, HIST, OC | direct | 구현완료 |
| TREND | active TF cycle | TF_ADDON_PYRAMID | BUY/WATCH | `_trend_buy_events()` + `_trend_addon_context()` ready path | update | TE, ALL, HIST, OC | direct | 구현완료 |
| TREND | TF_PEG | TF_PEG_EVENT | ALERT/WATCH | `_trend_buy_events()` event-day confirmed/missed path | none | TE, ALL, HIST | direct | 구현완료 |
| TREND | active TF cycle | TF_SELL_BREAKDOWN | SELL | `_trend_sell_events()` support_low fail | close | TE, ALL, HIST | direct | 구현완료 |
| TREND | active TF cycle | TF_SELL_CHANNEL_BREAK | SELL | `_trend_sell_events()` channel_low8 fail | close | TE, ALL, HIST | direct | 구현완료 |
| TREND | active TF cycle | TF_SELL_TRAILING_BREAK | SELL | `_trend_sell_events()` trailing/protected stop fail | close | TE, ALL, HIST | direct | 구현완료 |
| TREND | active TF cycle | TF_SELL_TP1 | SELL | `_trend_sell_events()` `high >= tp1_level` and not hit | update | TE, ALL, HIST, OC | direct | 구현완료 |
| TREND | active TF cycle | TF_SELL_TP2 | SELL | `_trend_sell_events()` `high >= tp2_level` and not hit | update | TE, ALL, HIST, OC | direct | 구현완료 |
| TREND | TF_MOMENTUM | TF_SELL_MOMENTUM_END | SELL | `_trend_sell_events()` momentum fade path | close | TE, ALL, HIST | direct | 구현완료 |
| TREND | active TF cycle | TF_SELL_RESISTANCE_REJECT | SELL | `_trend_sell_events()` mid-band reject path | close | TE, ALL, HIST | direct | 구현완료 |

### TREND State, Level, Add-on Signals

| engine | family | signal_code | action_type | 생성 조건 위치 | cycle 영향 | persisted output | 테스트 상태 | 판정 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TREND | TF_BREAKOUT | TF_SETUP_ACTIVE | STATE | `_trend_state_rows()` `setup_active` | none | TS, ALL | direct | 구현완료 |
| TREND | TF_BREAKOUT | TF_VCP_ACTIVE | STATE | `_trend_state_rows()` `vcp_active` | none | TS, ALL | direct | 구현완료 |
| TREND | TF_BREAKOUT | TF_BUILDUP_READY | STATE | `_trend_state_rows()` setup/build-up ready | none | TS, ALL | direct | 구현완료 |
| TREND | TF_BREAKOUT | TF_AGGRESSIVE_ALERT | ALERT | `_trend_state_rows()` `aggressive_ready` | none | TS, ALL | direct | 구현완료 |
| TREND | active TF cycle | TF_ADDON_READY | STATE | `_build_trend_addon_state_rows()` `next_addon_allowed` | none | TS, ALL | direct | 구현완료 |
| TREND | active TF cycle | TF_ADDON_SLOT1_READY | STATE | `_build_trend_addon_state_rows()` `addon_next_slot == 1` | none | TS, ALL | direct | 구현완료 |
| TREND | active TF cycle | TF_ADDON_SLOT2_READY | STATE | `_build_trend_addon_state_rows()` `addon_next_slot == 2` | none | TS, ALL | direct | 구현완료 |
| TREND | active TF cycle | TF_TRAILING_LEVEL | STATE | `_build_level_state_rows()` active cycle trailing | none | TS, ALL | direct | 구현완료 |
| TREND | active TF cycle | TF_PROTECTED_STOP_LEVEL | STATE | `_build_level_state_rows()` risk-free/add-on protected stop | none | TS, ALL | direct | 구현완료 |
| TREND | active TF cycle | TF_BREAKEVEN_LEVEL | STATE | `_build_level_state_rows()` risk-free armed | none | TS, ALL | direct | 구현완료 |
| TREND | active TF cycle | TF_TP1_LEVEL | STATE | `_build_level_state_rows()` active TP1 level | none | TS, ALL | direct | 구현완료 |
| TREND | active TF cycle | TF_TP2_LEVEL | STATE | `_build_level_state_rows()` active TP2 level | none | TS, ALL | direct | 구현완료 |

### UG Event Signals

| engine | family | signal_code | action_type | 생성 조건 위치 | cycle 영향 | persisted output | 테스트 상태 | 판정 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| UG | UG_BREAKOUT | UG_BUY_BREAKOUT | BUY/WATCH | `_ug_buy_events()` breakout condition | open | UE, ALL, HIST, OC | direct | 구현완료 |
| UG | UG_BREAKOUT | UG_BUY_SQUEEZE_BREAKOUT | BUY/WATCH | `_ug_buy_events()` squeeze breakout condition | open | UE, ALL, HIST, OC | direct | 구현완료 |
| UG | UG_PULLBACK | UG_BUY_PBB | BUY/WATCH | `_ug_buy_events()` PBB pullback condition | open | UE, ALL, HIST, OC | direct | 구현완료 |
| UG | UG_MEAN_REVERSION | UG_BUY_MR_LONG | BUY/WATCH | `_ug_buy_events()` MR-long condition | open | UE, ALL, HIST, OC | direct | 구현완료 |
| UG | active UG cycle | UG_SELL_MR_SHORT | TRIM | `_ug_sell_events()` MR-short and `trim_count < 2` | update | UE, ALL, HIST, OC | direct | 구현완료 |
| UG | active UG cycle | UG_SELL_PBS | EXIT | `_ug_sell_events()` PBS-ready and allowed | close | UE, ALL, HIST | direct | 구현완료 |
| UG | active UG cycle | UG_SELL_BREAKDOWN | EXIT | `_ug_sell_events()` support_low fail | close | UE, ALL, HIST | direct | 구현완료 |

### UG State and Combo Signals

| engine | family | signal_code | action_type | 생성 조건 위치 | cycle 영향 | persisted output | 테스트 상태 | 판정 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| UG | UG_STATE | UG_STATE_GREEN | STATE | `_ug_state_rows()` traffic-light green | none | US, ALL | direct | 구현완료 |
| UG | UG_STATE | UG_STATE_ORANGE | STATE | `_ug_state_rows()` traffic-light orange | none | US, ALL | direct | 구현완료 |
| UG | UG_STATE | UG_STATE_RED | STATE | `_ug_state_rows()` traffic-light red | none | US, ALL | direct | 구현완료 |
| UG | UG_STATE | UG_NH60 | STATE | `_ug_state_rows()` `nh60` | none | US, ALL | direct | 구현완료 |
| UG | UG_STATE | UG_VOL2X | STATE | `_ug_state_rows()` `rvol20 >= 2.0` | none | US, ALL | direct | 구현완료 |
| UG | UG_STATE | UG_W | STATE | `_ug_state_rows()` `w_active` | none | US, ALL | direct | 구현완료 |
| UG | UG_STATE | UG_VCP | STATE | `_ug_state_rows()` `vcp_active` | none | US, ALL | direct | 구현완료 |
| UG | UG_STATE | UG_SQUEEZE | STATE | `_ug_state_rows()` `squeeze_active` | none | US, ALL | direct | 구현완료 |
| UG | UG_STATE | UG_TIGHT | STATE | `_ug_state_rows()` `tight_active` | none | US, ALL | direct | 구현완료 |
| UG | strategy combo | UG_COMBO_TREND | STATE | `_ug_strategy_combo_rows()` green breakout combo | none | UC, ALL | direct | 구현완료 |
| UG | strategy combo | UG_COMBO_PULLBACK | STATE | `_ug_strategy_combo_rows()` PBB combo | none | UC, ALL | direct | 구현완료 |
| UG | strategy combo | UG_COMBO_SQUEEZE | STATE | `_ug_strategy_combo_rows()` orange squeeze combo | none | UC, ALL | direct | 구현완료 |

## State Transition Table

| engine/family | buy path | possible sells / trims | close rule | surviving cycle fields |
| --- | --- | --- | --- | --- |
| TREND / TF_REGULAR_PULLBACK | `TF_BUY_REGULAR` or `TF_ADDON_PYRAMID` | `TF_SELL_TP1`, `TF_SELL_TP2`, `TF_SELL_BREAKDOWN`, `TF_SELL_CHANNEL_BREAK`, `TF_SELL_TRAILING_BREAK`, `TF_SELL_RESISTANCE_REJECT` | breakdown/channel/trailing/resistance reject | entry, stop, trailing, TP plan, TP levels, trim_count, risk_free_armed, add-on state, blended entry, protected stop |
| TREND / TF_BREAKOUT | `TF_BUY_BREAKOUT` or `TF_ADDON_PYRAMID` | `TF_SELL_TP1`, `TF_SELL_TP2`, `TF_SELL_BREAKDOWN`, `TF_SELL_CHANNEL_BREAK`, `TF_SELL_TRAILING_BREAK`, `TF_SELL_RESISTANCE_REJECT` | breakdown/channel/trailing/resistance reject | same as above |
| TREND / TF_MOMENTUM | `TF_BUY_MOMENTUM` or `TF_ADDON_PYRAMID` | `TF_SELL_TP1`, `TF_SELL_TP2`, `TF_SELL_BREAKDOWN`, `TF_SELL_CHANNEL_BREAK`, `TF_SELL_TRAILING_BREAK`, `TF_SELL_RESISTANCE_REJECT`, `TF_SELL_MOMENTUM_END` | breakdown/channel/trailing/resistance reject/momentum end | same as above |
| TREND / TF_PEG | `TF_BUY_PEG_PULLBACK`, `TF_BUY_PEG_REBREAK`, `TF_ADDON_PYRAMID`; plus `TF_PEG_EVENT` alert/watch | `TF_SELL_TP1`, `TF_SELL_TP2`, `TF_SELL_BREAKDOWN`, `TF_SELL_CHANNEL_BREAK`, `TF_SELL_TRAILING_BREAK`, `TF_SELL_RESISTANCE_REJECT` | breakdown/channel/trailing/resistance reject | same as above |
| UG / UG_BREAKOUT | `UG_BUY_BREAKOUT`, `UG_BUY_SQUEEZE_BREAKOUT` | `UG_SELL_MR_SHORT`, `UG_SELL_PBS`, `UG_SELL_BREAKDOWN` | PBS or breakdown | entry, support zone, stop, trim_count, last_trim_date, partial_exit_active, base/current units |
| UG / UG_PULLBACK | `UG_BUY_PBB` | `UG_SELL_MR_SHORT`, `UG_SELL_PBS`, `UG_SELL_BREAKDOWN` | PBS or breakdown | same as above plus canonical `reference_exit_signal=UG_SELL_MR_SHORT_OR_PBS` |
| UG / UG_MEAN_REVERSION | `UG_BUY_MR_LONG` | `UG_SELL_MR_SHORT`, `UG_SELL_PBS`, `UG_SELL_BREAKDOWN` | PBS or breakdown | same as above |
| UG / STATE | none | none | none | no cycle |
| UG / combo | none | none | none | no cycle |

## Coverage Table

| coverage status | signal codes |
| --- | --- |
| direct regression-tested | `TF_SETUP_ACTIVE`, `TF_VCP_ACTIVE`, `TF_BUILDUP_READY`, `TF_AGGRESSIVE_ALERT`, `TF_ADDON_READY`, `TF_ADDON_SLOT1_READY`, `TF_ADDON_SLOT2_READY`, `TF_TRAILING_LEVEL`, `TF_PROTECTED_STOP_LEVEL`, `TF_BREAKEVEN_LEVEL`, `TF_TP1_LEVEL`, `TF_TP2_LEVEL`, `TF_BUY_REGULAR`, `TF_BUY_BREAKOUT`, `TF_BUY_PEG_PULLBACK`, `TF_BUY_PEG_REBREAK`, `TF_BUY_MOMENTUM`, `TF_ADDON_PYRAMID`, `TF_SELL_RESISTANCE_REJECT`, `TF_SELL_BREAKDOWN`, `TF_SELL_CHANNEL_BREAK`, `TF_SELL_TRAILING_BREAK`, `TF_SELL_TP1`, `TF_SELL_TP2`, `TF_SELL_MOMENTUM_END`, `TF_PEG_EVENT`, `UG_STATE_GREEN`, `UG_STATE_ORANGE`, `UG_STATE_RED`, `UG_NH60`, `UG_VOL2X`, `UG_W`, `UG_VCP`, `UG_SQUEEZE`, `UG_TIGHT`, `UG_BUY_BREAKOUT`, `UG_BUY_SQUEEZE_BREAKOUT`, `UG_BUY_PBB`, `UG_BUY_MR_LONG`, `UG_SELL_PBS`, `UG_SELL_BREAKDOWN`, `UG_SELL_MR_SHORT`, `UG_COMBO_TREND`, `UG_COMBO_PULLBACK`, `UG_COMBO_SQUEEZE` |
| indirect only | none |
| untested | none |

## Manual Verification Notes
- 직접 호출 결과 breakout snapshot에서 `TF_BUY_BREAKOUT`와 `TF_BUY_MOMENTUM`이 동시에 발생했다.
- 직접 호출 결과 PEG pullback/rebreak context에서 각각 `TF_BUY_PEG_PULLBACK`, `TF_BUY_PEG_REBREAK`가 정상 발생했다.
- 직접 호출 결과 support와 channel을 동시에 깨면 `TF_SELL_BREAKDOWN`, `TF_SELL_CHANNEL_BREAK`, `TF_SELL_TRAILING_BREAK`가 함께 발생할 수 있다.
- 직접 호출 결과 orange-oriented UG metrics에서는 `UG_STATE_ORANGE`와 `UG_COMBO_SQUEEZE`가 발생했다.
- 직접 호출 결과 red-oriented UG metrics에서는 `UG_STATE_RED`가 발생했다.
- 직접 호출 결과 add-on second slot context에서 `TF_ADDON_SLOT2_READY`가 발생했다.

## Prioritized Backlog

### 즉시 수정 필요
- 없음. 현재 active inventory 안에서는 생성 경로, cycle 연결, persisted output, 회귀테스트가 모두 확인됐다.

### 다음 배치
- family 간 동시 buy 허용 정책을 명시적으로 결정할 것.
  - 현재는 같은 symbol에서 같은 엔진 안에서도 family별 cycle을 동시에 열 수 있다.
  - 이 동작이 의도라면 유지하고, 의도가 아니라면 family arbitration 또는 priority table을 도입해야 한다.
- `signal_history`에 state/aux/combo를 남길지 결정할 것.
  - 현재는 `EVENT`만 저장되므로 `TF_AGGRESSIVE_ALERT`, `UG_STATE_*`, `UG_COMBO_*`, level/add-on state는 history CSV에서 추적되지 않는다.
- `UG_COMBO_*`를 legacy output에도 넣을지 결정할 것.
  - 현재 combo는 v2 output 전용이다.

### 정리만 하면 되는 legacy/helper
- `TF_SELL_PBS`
  - active inventory에 없는 과거 이름이다.
  - 현재는 label fallback과 test assertion에서만 보인다.
- `TF_SELL_SUB200`
  - active inventory에 없는 label fallback 이름이다.
  - 현재 sell contract에는 포함되지 않는다.
- `UG_SELL_MR_SHORT_OR_PBS`
  - active signal code가 아니라 `UG_PULLBACK` 전용 `reference_exit_signal` helper literal이다.

## Bottom Line
- 현재 코드 계약 기준으로 buy/sell signal inventory는 구현이 닫혀 있다.
- 이번 audit 기준으로 active signal code 45개는 전부 live path, persisted output, direct regression, direct manual verification이 확인됐다.
- 남은 일은 `신호 미구현`이 아니라 `다중 family 동시 허용`, `history 범위`, `legacy/helper 정리` 같은 운영 정책 확정이다.