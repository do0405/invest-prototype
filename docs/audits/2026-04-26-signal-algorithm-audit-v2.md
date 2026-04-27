# Signal Algorithm Audit — v2 (Extended)

Date: `2026-04-26`
Predecessor: `2026-04-26-signal-algorithm-audit.md`
Scope: `screeners/signals/engine.py`, `screeners/signals/patterns.py`, `screeners/qullamaggie/core.py`, `screeners/leader_lagging/algorithms.py`, `screeners/markminervini/screener.py`, `screeners/weinstein_stage2/screener.py`, `utils/indicator_helpers.py`

---

## 이전 audit에서 다룬 항목

`2026-04-26-signal-algorithm-audit.md` 참조. 본 문서는 해당 항목(A-1~A-6, B-1~B-3, C-1~C-3)을 제외한 **추가 발견 사항**만 기록한다.

---

## 추가 발견 사항

### D. Python 언어 함정 (Language Pitfall)

#### D-1. `base_start_idx = 0`이 falsy로 처리되어 prior_window 계산 오류

**위치**: `screeners/qullamaggie/core.py:356-357`

```python
prior_window_start = max(0, (base_start_idx or len(daily)) - 60)
prior_window_end   = max(1, base_start_idx or len(daily))
```

`base_start_idx`가 정수 `0`일 때 Python의 `or` 연산자가 이를 falsy로 평가하여 `len(daily)`로 대체된다.

| `base_start_idx` 값 | 의도된 prior_window | 실제 prior_window |
|---------------------|---------------------|-------------------|
| `None` | `daily[-60:]` (fallback, 정상) | `daily[-60:]` |
| `5` | `daily[0:5]` | `daily[0:5]` |
| `0` | `daily[0:0]` (빈 윈도우) | `daily[-60:]` ← **오류** |

`base_start_idx = 0`은 데이터 시작점부터 베이스가 잡힌 경우(IPO 초기 등)에 발생한다. 이때 `prior_low`가 최근 60바의 종가 최저값으로 계산되어 `prior_run_pct`(베이스 돌입 전 상승폭)이 과장된다.

**권장 수정**:
```python
_base = base_start_idx if base_start_idx is not None else len(daily)
prior_window_start = max(0, _base - 60)
prior_window_end   = _base
```

---

### E. 지표 품질 문제

#### E-1. `min_periods`가 너무 낮아 초기 지표 품질 저하

**위치**: `screeners/qullamaggie/core.py:313-324`

```python
daily["sma10"]   = rolling_sma(daily["close"], 10, min_periods=5)
daily["sma20"]   = rolling_sma(daily["close"], 20, min_periods=10)
daily["sma50"]   = rolling_sma(daily["close"], 50, min_periods=20)
daily["sma200"]  = rolling_sma(daily["close"], 200, min_periods=60)
daily["adr20_pct"] = daily["adr_pct"].rolling(20, min_periods=5).mean()
daily["adv20"]   = rolling_average_volume(daily, 20, min_periods=5)
```

`sma10`은 5바부터, `adr20_pct`는 5바부터 유효값을 반환한다. 이후 `.iloc[-1]`로 최신 바의 피처를 추출할 때 심볼의 데이터가 최소 요구량을 간신히 충족한 경우 저품질 추정값이 스크리닝 입력으로 사용된다.

`min_periods`와 실제 필요 데이터량 불일치 요약:

| 지표 | window | min_periods | 실제 신뢰 가능 기준 |
|------|--------|-------------|---------------------|
| sma20 | 20 | 10 | 20 |
| adr20_pct | 20 | 5 | 15 이상 |
| adv20 | 20 | 5 | 15 이상 |
| sma200 | 200 | 60 | 200 |

**권장 수정**: `min_periods`를 `window * 0.75` 이상으로 상향 조정하거나, 초기 바에서 명시적으로 None을 반환하도록 처리.

---

#### E-2. `detect_tight_range`에서 compressed_bars 계산에 off-by-one 가능성

**위치**: `screeners/signals/patterns.py:126-141`

```python
for offset in range(5, 0, -1):
    end = None if offset == 1 else -offset + 1
    high_window = high.iloc[-offset - 9 : end]
    low_window  = low.iloc[-offset - 9 : end]
    atr_value   = _safe_float(atr14.iloc[-offset])
    range_value = _safe_float(high_window.max() - low_window.min())
```

`offset=1`일 때 `end=None`이므로 `high.iloc[-10:]` (최신 10바). `offset=2`일 때 `end=-1`이므로 `high.iloc[-11:-1]` (2~11바 전). 이 슬라이싱은 정상처럼 보이지만, `atr14.iloc[-offset]`도 같은 위치여야 한다.

실제 검토:
- `offset=1`: 가격 윈도우 = 최신 10바, ATR = `atr14.iloc[-1]` (최신) ✓
- `offset=2`: 가격 윈도우 = 2~11바 전, ATR = `atr14.iloc[-2]` (2바 전) ✓
- `offset=5`: 가격 윈도우 = 5~14바 전, ATR = `atr14.iloc[-5]` (5바 전) ✓

정렬은 맞지만, 모든 5개 윈도우가 독립적이지 않고 **서로 4~9바 겹친다**. `compressed_bars >= 5` 판정이 실질적으로 "최신 14바 내에서 9바 이상 압축" 판정과 동일하여, 독립적인 5회 압축 확인이라는 의미가 아님.

**운영 위험**: 큰 문제는 아니지만 `detect_tight_range`의 실제 동작이 코드 독해 시 기대되는 것과 다름. 문서화 필요.

---

### F. 상태 관리 문제

#### F-1. 동일 심볼에서 BUY와 SELL 신호 동시 발생 가능

**위치**: `screeners/signals/engine.py:7960-8036`

`_run_scope_scan` 에서 buy와 sell이 독립적으로 처리된다:

```python
# buy: scope_symbols 내 모든 심볼
buy_candidate_symbols = sorted(symbol for symbol in scope_symbols if symbol in metrics_map)

# sell: 활성 사이클 보유 심볼
cycle_items = list(scope_active_cycles.items())
```

활성 UG 사이클이 15 영업일을 초과한 심볼은 cooldown이 해제되어 `_ug_buy_events`에서 새 BUY 신호가 발생할 수 있다. 동시에 `_ug_sell_events`는 기존 사이클에 대한 SELL 신호를 발생시킨다. 결과적으로 같은 날 동일 심볼에서:

```
UG_BUY_BREAKOUT  (action_type=BUY)
UG_SELL_PBS      (action_type=EXIT)
```

두 신호가 동시에 출력 파일에 기록된다.

**운영 위험**: 신호 소비자(알림, 대시보드)가 같은 심볼의 BUY+SELL을 동일 스캔에서 받으면 처리 우선순위가 불명확하다.

**권장 수정**: 출력 단계에서 같은 `(signal_date, symbol, engine)` 내에 BUY와 EXIT/SELL이 공존하면 SELL을 우선하거나, 소비자 계층에서 명시적으로 처리 순서를 정의.

---

#### F-2. TF 사이클에 명시적 쿨다운 없음

**위치**: `screeners/signals/engine.py:4674-4750`

UG 엔진에는 `_is_ug_cooldown_blocked()`가 있지만, TF 엔진의 `_trend_buy_events()`에는 동등한 쿨다운 로직이 없다. TF는 활성 사이클이 있으면 `TF_ADDON_PYRAMID`로 전환하는 방식으로 중복을 방지하지만:

```python
active = active_cycles.get(("TREND", family, symbol))
addon_context = self._trend_addon_context(...) if active else {}
if active and not addon_context.get("ready"):
    pass  # ← 애드온 준비 안됐으면 아무것도 안 함
else:
    # BUY 또는 ADDON 신호 발생
```

`TF_REGULAR_PULLBACK` 사이클이 있고 `addon_context.ready = False`이면 신호가 발생하지 않는다. 그러나 `TF_BREAKOUT` 패밀리와 `TF_REGULAR_PULLBACK` 패밀리는 키가 다르므로(`("TREND", "TF_BREAKOUT", symbol)` vs `("TREND", "TF_REGULAR_PULLBACK", symbol)`), 동일 심볼에서 두 패밀리가 동시에 BUY 신호를 발생시킬 수 있다.

**운영 위험**: 같은 날 같은 심볼에 `TF_BUY_REGULAR`와 `TF_BUY_BREAKOUT`이 동시에 출력될 수 있다.

---

### G. 스크리너 간 데이터 품질 문제

#### G-1. `leader_lagging` RS 계산의 최소 데이터 부트스트랩 저품질

**위치**: `screeners/leader_lagging/algorithms.py`

RS 계산에 사용되는 성분들:

```python
RS_COMPONENTS = ((21, 0.25), (65, 0.45), (126, 0.30))
```

최장 126바(약 6개월)가 필요하지만, `max_offset = min(history_offsets, len - 64)`에 의해 64바 이상이면 RS 계산이 시작된다. IPO 후 3~4개월 주식의 RS 점수는 126바 성분 없이 단기 성분(21, 65바)만으로 계산되어 과대평가될 수 있다.

이 점수가 `source_registry`를 통해 `source_priority_score`로 전달되고, 이것이 다시 `_ug_conviction` 계산에 영향을 주기 때문에 신규 상장주의 conviction grade가 인위적으로 높아질 수 있다.

**권장 수정**: RS 신뢰도(`rs_proxy_confidence`)가 임계값(예: 0.7) 미만인 심볼은 `source_priority_score` 적용 시 가중치를 낮추거나 별도 플래그 처리.

---

#### G-2. `markminervini/screener.py`의 날짜 정렬 묵시적 가정

**위치**: `screeners/markminervini/screener.py:51, 73-74`

```python
df = df.sort_values("date").reset_index(drop=True)  # 라인 51
...
ma150_60d_ago = to_float_or_none(df["ma150"].iloc[-60]) if len(df) >= 210 else None
ma200_20d_ago = to_float_or_none(df["ma200"].iloc[-20]) if len(df) >= 220 else None
```

`sort_values("date")`로 정렬 후 `iloc[-60]`, `iloc[-20]`을 사용하므로, 정렬이 제대로 된다면 안전하다.

그러나 `len(df) >= 210`은 210 거래일 이상의 데이터가 있어야만 "60일 전 MA150"을 조회한다는 의미다. 실제로는 200바 이동평균(`ma200`)이 수렴하려면 200바 이상이 필요한데, 이 조건은 `ma150_60d_ago`용 최소값(210)과 `ma200_20d_ago`용 최소값(220)만 체크하고 있다. MA200 자체 계산 시 충분한 데이터가 있는지 별도 확인이 없다.

**운영 위험**: 낮은 위험. 하지만 데이터가 정확히 210~219바인 심볼에서 `ma150_60d_ago`는 유효하지만 `ma200_20d_ago`는 None이 되어 스크리닝 조건이 비대칭적으로 평가된다.

---

#### G-3. Weinstein 주간 봉 계산에서 불완전한 주 제거 로직 경계 조건

**위치**: `screeners/weinstein_stage2/screener.py` (weekly bar 생성)

현재 주의 불완전한 봉은 제거된다:

```python
if last_week_key == current_week_key and last_bar_end < expected_final_session:
    weekly = weekly.iloc[:-1].copy()
```

이 로직은 한국 시장(`kr`)에서 `W-SUN` 분할을 그대로 사용할 경우, 한국 공휴일 주의 마지막 거래일이 금요일이 아닌 경우 `expected_final_session` 계산이 틀릴 수 있다.

더 큰 문제는 `last_week_key == current_week_key` 비교에서 `current_week_key`가 `self.as_of_date` 기준으로 계산되는데, `as_of_date`가 비거래일(주말 또는 공휴일)이면 현재 주 key 계산이 다음 주를 가리킬 수 있다. 이 경우 불완전한 주를 제거하지 않고 최종 주간 봉에 포함된다.

---

## 종합 우선순위 (기존 audit 포함 전체)

| 순위 | 항목 | 위치 | 분류 |
|------|------|------|------|
| 1 | A-1: RSI None bypass (mr_long/mr_short) | patterns.py:477,496 | 위양성 |
| 2 | B-1+C-2: UG 쿨다운 family 분리 | engine.py:5942 | 신호 억제 |
| 3 | D-1: base_start_idx=0 falsy 버그 | qullamaggie/core.py:356 | 데이터 오염 |
| 4 | A-2: VCP shrinking_depth 허용 오차 | patterns.py:222 | 위양성 |
| 5 | A-4: PBS rsi_fade 임계값 | patterns.py:462 | 위양성 |
| 6 | F-1: BUY+SELL 동시 발생 | engine.py:7960 | 운영 |
| 7 | F-2: TF 패밀리 간 중복 BUY | engine.py:4674 | 운영 |
| 8 | C-1: TF 다중 SELL 우선순위 | engine.py:5467 | 운영 |
| 9 | G-1: 신규 상장주 RS 과대평가 | leader_lagging/algorithms.py | 위양성 |
| 10 | A-3: W 패턴 global minimum | patterns.py:153 | 위양성 |
| 11 | E-1: min_periods 저품질 지표 | qullamaggie/core.py:313 | 데이터 품질 |
| 12 | A-5: squeeze_breakout 중복 조건 | engine.py:6014 | 로직 |
| 13 | E-2: tight_range 윈도우 오버랩 | patterns.py:126 | 문서화 필요 |
| 14 | G-2: Minervini MA 비대칭 | markminervini/screener.py:73 | 낮은 위험 |
| 15 | G-3: Weinstein 주간 봉 경계 | weinstein_stage2/screener.py | 낮은 위험 |

---

## 참고 파일

| 파일 | 관련 항목 |
|------|----------|
| `screeners/signals/engine.py` | A-5, A-6, B-1~B-2, C-1~C-2, F-1~F-2 |
| `screeners/signals/patterns.py` | A-1~A-4, B-3, C-3, E-2 |
| `screeners/qullamaggie/core.py` | D-1, E-1 |
| `screeners/leader_lagging/algorithms.py` | G-1 |
| `screeners/markminervini/screener.py` | G-2 |
| `screeners/weinstein_stage2/screener.py` | G-3 |
