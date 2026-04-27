# Signal Algorithm Audit

Date: `2026-04-26`
Scope: `screeners/signals/engine.py`, `screeners/signals/patterns.py`

---

## 범위

- 시그널 생성 파이프라인 전반 (TF / UG 양 엔진)
- 위양성(false positive) 발생 원인 및 신호 과잉 억제(signal scarcity) 원인 식별
- 운영·데이터 일관성 문제 식별
- 코드 변경 제안 포함 (설계 의도 변경 없이 현재 동작 기준)

---

## 시스템 개요

두 개의 메인 신호 엔진이 병렬 운영된다.

| 엔진 | 계열 | 주요 BUY 코드 |
|------|------|--------------|
| TREND (TF) | `TF_REGULAR_PULLBACK`, `TF_BREAKOUT`, `TF_PEG` | `TF_BUY_REGULAR`, `TF_BUY_BREAKOUT`, `TF_BUY_MOMENTUM`, `TF_ADDON_PYRAMID` |
| ULTIMATE GROWTH (UG) | `UG_BREAKOUT`, `UG_PULLBACK`, `UG_MEAN_REVERSION` | `UG_BUY_BREAKOUT`, `UG_BUY_SQUEEZE_BREAKOUT`, `UG_BUY_PBB`, `UG_BUY_MR_LONG` |

신호 생성 흐름:

```
source_registry → peg_screen → active_cycles → financial_map
    → frames → feature_map → metrics_map (_build_metrics per symbol)
        → _run_scope_scan("all" / "screened")
            → state rows (병렬)  +  buy candidate rows (병렬)  +  sell rows (병렬)
                → write_signal_outputs()
```

---

## 발견 사항

### A. 위양성(False Positive) 원인

#### A-1. `mr_long_ready` / `mr_short_ready`의 RSI None bypass

**위치**: `screeners/signals/patterns.py:477, 496`

```python
# mr_long_ready
and (rsi is None or rsi <= 40.0)

# mr_short_ready
and (rsi is None or rsi >= 65.0 or risk_heat or (z_score or 0.0) >= 1.0)
```

RSI 계산 실패 또는 데이터 부족 심볼에서 RSI 게이트를 통과한다. oversold/overheated 판정이 BB 기준으로 잡혀도 RSI 확인 없이 신호가 발생한다.

**권장 수정**: `rsi is None` 분기를 제거하고 None 시 신호 억제, 또는 fallback 임계값(예: `rsi = 50.0`)을 명시적으로 지정.

---

#### A-2. VCP `shrinking_depth` 허용 오차 5%

**위치**: `screeners/signals/patterns.py:222`

```python
shrinking_depth = all(depths[i] <= depths[i - 1] * 1.05 ...)
```

직전 수축 대비 5% 깊어져도 "수축 중"으로 판정된다. 예: `15% → 15.7% → 16.4%`도 VCP 통과. 진짜 VCP는 깊이가 단조 감소해야 한다.

**권장 수정**: `* 1.05` → `* 1.02` (소폭 오차 허용) 또는 `* 1.0` (flat 허용, 증가 불허).

---

#### A-3. W 패턴이 국소 최저점이 아닌 전역 최저점 사용

**위치**: `screeners/signals/patterns.py:153-164`

```python
left = lows.iloc[:split]    # 40바 전반
right = lows.iloc[split:]   # 40바 후반
left_idx = left.idxmin()    # 전반 전체 최저점
right_idx = right.idxmin()  # 후반 전체 최저점
```

각 절반 구간의 전역 최저점을 찾으므로, W 형태(두 근접 저점 + 중간 반등)가 아닌 단순 하락 추세에서도 두 저점이 8% 이내면 W로 인식된다.

**권장 수정**: `idxmin()` 전에 rolling local minimum 필터를 적용하거나, 두 저점 사이 반등폭 최소 조건(예: `neckline >= floor * 1.10`)을 추가.

---

#### A-4. PBS 매도 신호의 약한 RSI 기준

**위치**: `screeners/signals/patterns.py:462`

```python
rsi_fade = bool(rsi is not None and rsi <= 50.0)
```

RSI 50은 중립점이다. `failed_mid or failed_upper` + 약한 클로즈 + 음봉만 있어도 RSI가 중립(50)이면 매도 신호가 발생한다. 반등 초기 조정바에서 오발 가능성이 높다.

**권장 수정**: `rsi <= 45.0` 또는 `rsi <= 40.0`으로 기준 강화.

---

#### A-5. `squeeze_breakout_condition`의 중복 조건

**위치**: `screeners/signals/engine.py:6014-6018`

```python
squeeze_breakout_condition = bool(
    breakout_condition
    and recent_orange_context    # recent_orange_ready10
    and recent_squeeze_context   # recent_squeeze_ready10
    and buy_volume_rvol >= 2.0
)
```

`recent_orange_ready10`의 정의:
```python
recent_orange_ready10 = bool(
    recent_squeeze_ready10    # ← recent_squeeze_ready10의 상위집합
    or build_up_ready
    or vcp_active
    or squeeze_active
    or tight_active
)
```

`recent_orange_ready10 AND recent_squeeze_ready10` = `recent_squeeze_ready10`이므로 `recent_orange_context` 조건이 실질적 필터 역할을 하지 못한다.

**권장 수정**: `recent_orange_context` 제거 후 `recent_squeeze_context`만 유지, 또는 두 변수의 역할을 재정의.

---

#### A-6. PBB / MR_LONG 게이트의 이중 품질 기준

**위치**: `screeners/signals/engine.py:6022-6035`

```python
pbb_gate_pass = bool(
    metrics.get("ug_pbb_ready")
    and (
        not refined_band_gate_present       # 진단 데이터 없으면 단일 조건으로 통과
        or band_reversion_profile.get("pbb_ready")
    )
)
```

`refined_band_gate_present`가 False인 심볼(진단 데이터 없음)은 `ug_pbb_ready`만으로 신호가 발생하고, True인 심볼은 이중 검증을 거친다. 데이터 가용성에 따라 신호 기준이 달라진다.

**권장 수정**: `refined_band_gate_present`가 False일 때도 band_reversion 계산을 강제 실행하거나, 단일 기준으로 통일.

---

### B. 신호 과잉 억제(Signal Scarcity) 원인

#### B-1. UG 쿨다운이 family-agnostic

**위치**: `screeners/signals/engine.py:5942-5973`

```python
def _is_ug_cooldown_blocked(self, symbol, signal_code, active_cycles, signal_history):
    _ = signal_code   # ← 파라미터 즉시 버림, 실제로 미사용
    for cycle in active_cycles.values():
        if _safe_text(cycle.get("engine")) != "UG":
            continue
        # family 구분 없이 UG 사이클이 존재하면 모두 차단
        bars = _business_days_between(cycle.get("opened_on"), self.as_of_date)
        if bars is not None and 1 <= bars < _UG_COOLDOWN_BUSINESS_DAYS:
            return True
```

`UG_BUY_BREAKOUT`(추세 돌파 진입)과 `UG_BUY_PBB`(밴드 풀백 진입)은 서로 다른 전략인데, 어느 한 family의 사이클이 열려있으면 다른 모든 UG BUY를 15 영업일 동안 차단한다. 돌파 후 Pullback to Band 재진입 기회가 억제된다.

**권장 수정**: `family`별 쿨다운 적용. 예: `UG_BREAKOUT` 사이클은 `UG_BUY_BREAKOUT`/`UG_BUY_SQUEEZE_BREAKOUT`만 차단하고, `UG_PULLBACK`/`UG_MEAN_REVERSION` 신호는 독립적으로 평가.

---

#### B-2. `breakout_ready` 8가지 조건 동시 충족

**위치**: `screeners/signals/engine.py:2913-2923`

```python
breakout_ready = bool(
    liquidity_pass
    and support_trend_rising
    and not ema_turn_down
    and breakout_anchor_clear    # close >= 20일 또는 60일 최고가
    and breakout_band_clear      # close >= BB 상단
    and rvol50 >= 1.5
    and close_position >= 0.70
    and body_strength >= 0.55
    and daily_return >= max(2.0, adr20 * 0.45)
)
```

`breakout_anchor_clear`(이전 고점 돌파)와 `breakout_band_clear`(BB 상단 돌파) 동시 요구는 특히 엄격하다. ATR이 좁은 저변동성 구간에서 진행되는 돌파는 BB 상단과 이전 고점이 일치하지 않아 둘 다 충족하기 어렵다. 이 조건이 `UG_BUY_BREAKOUT`의 주요 진입 게이트(`breakout_signal_ready`)이기도 하다.

**검토 사항**: `vcp_pivot_breakout`이 대안 경로로 존재하므로 실제 억제 규모는 backtesting으로 확인 필요.

---

#### B-3. VCP의 엄격한 `final_depth <= first_depth * 0.5` 조건

**위치**: `screeners/signals/patterns.py:240`

```python
setup_active = bool(
    ...
    and final_depth <= min(8.0, first_depth * 0.5)
    ...
)
```

초기 수축 깊이가 크면 마지막 수축이 절반 이하여야 한다. 예: 첫 수축 20% → 마지막 수축 <= 10% (그리고 <= 8%). `tight_active`와 `volume_dry` 동시 요구와 결합되어 유효한 VCP 중 상당수가 필터링될 수 있다.

**권장 수정**: `first_depth * 0.5` → `first_depth * 0.6`으로 완화 검토. 단, 백테스트 기반 결정 필요.

---

### C. 운영 문제

#### C-1. 동일 사이클에서 다중 SELL 신호 동시 발생

**위치**: `screeners/signals/engine.py:5467-5589`

```
TF_SELL_BREAKDOWN     ← close <= support_low
TF_SELL_CHANNEL_BREAK ← close <= channel_floor
TF_SELL_TRAILING_BREAK ← close <= trailing_level
```

세 조건이 동시에 참인 경우(갭다운 등) 같은 사이클에서 하루에 3개 SELL 신호가 출력 파일에 독립 행으로 쌓인다. 신호 간 우선순위 로직이 없다.

**권장 수정**: 우선순위 체인 적용 — `TF_SELL_BREAKDOWN > TF_SELL_CHANNEL_BREAK > TF_SELL_TRAILING_BREAK`, 가장 강한 신호 하나만 emit. 또는 복수 신호를 단일 행으로 병합하고 `reason_codes`에 전체 트리거를 기록.

---

#### C-2. `signal_code` 파라미터 서명에만 존재, 실제 미사용

**위치**: `screeners/signals/engine.py:5949`

```python
def _is_ug_cooldown_blocked(self, symbol, signal_code, active_cycles, signal_history):
    _ = signal_code   # 의도적 무시
```

파라미터 서명은 code-specific 쿨다운이 구현된 것처럼 보이지만 내부적으로 완전히 무시된다. B-1 문제의 근본 원인이며, 추후 family 분리 구현 시 버그 유입 지점이 된다.

**권장 수정**: B-1 개선 시 `signal_code` 또는 `family` 파라미터를 실제로 활용. 또는 파라미터를 제거하고 의도를 주석으로 명시.

---

#### C-3. `detect_tight_range`에서 ATR10과 ATR14 혼용

**위치**: `screeners/signals/patterns.py:112-141`

```python
atr10 = true_range.rolling(10, min_periods=10).mean()
atr14 = true_range.rolling(14, min_periods=14).mean()
percentile_rank = float((atr_window <= latest_atr10).mean())  # ATR10 기준
...
range_value <= atr_value * 1.5   # ATR14 기준 (atr14.iloc[-offset])
```

percentile rank 계산은 ATR10으로, 개별 바 압축 검사는 ATR14로 수행한다. 동일 함수 내에서 다른 기간의 ATR을 혼용하면 tight 판정 결과의 일관성이 떨어진다.

**권장 수정**: 하나의 기간으로 통일. ATR14가 표준이면 percentile rank도 ATR14 기반으로 변경.

---

## 우선순위 요약

| 순위 | 항목 | 분류 | 예상 효과 |
|------|------|------|----------|
| 1 | A-1: RSI None bypass 수정 | 위양성 | MR_LONG/MR_SHORT 오발 즉시 감소 |
| 2 | B-1 + C-2: UG 쿨다운 family 분리 | 신호 억제 | PBB/MR_LONG 정당한 기회 복원 |
| 3 | A-2: VCP shrinking_depth 1.02로 강화 | 위양성 | VCP 오인식 감소 |
| 4 | A-4: PBS rsi_fade <= 45.0으로 강화 | 위양성 | 매도 신호 오발 감소 |
| 5 | C-1: TF SELL 신호 우선순위 적용 | 운영 | 중복 출력 행 제거 |
| 6 | A-3: W 패턴 국소 최저점 검증 추가 | 위양성 | W 오인식 감소 |
| 7 | C-3: ATR 기간 통일 | 운영 | tight 판정 일관성 |
| 8 | B-2: breakout_ready 조건 검토 | 신호 억제 | 백테스트 후 결정 |

---

## 참고 파일

| 파일 | 관련 항목 |
|------|----------|
| `screeners/signals/engine.py` | A-5, A-6, B-1, B-2, C-1, C-2 |
| `screeners/signals/patterns.py` | A-1, A-2, A-3, A-4, B-3, C-3 |
| `screeners/signals/cycle_store.py` | B-1 (쿨다운 히스토리 로드) |
| `config.py` | `ADVANCED_FINANCIAL_CRITERIA` (재무 게이트) |
