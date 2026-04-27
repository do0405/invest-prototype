# Codex Signal Algorithm Audit Prompt

이 프롬프트는 Codex(또는 유사 AI 코드 탐색 도구)에게 이 프로젝트의 시그널/스크리닝 알고리즘을 감사할 때 사용한다.

---

## 사용 방법

아래 섹션 중 필요한 것을 복사해서 Codex에 붙여넣는다. 전체를 한 번에 쓰면 탐색 범위가 너무 넓어진다. **섹션별로 분리해서 사용**하는 것을 권장한다.

---

## 섹션 1: False Positive 탐색

```
이 Python 투자 스크리닝 프로젝트(e:\side project\invest-prototype-main)의
screeners/signals/ 디렉토리에서 위양성(false positive) 시그널을 유발할 수 있는
코드를 찾아라. 다음 패턴을 집중적으로 탐색해라:

1. Boolean 조건에서 None 허용 bypass
   - `(x is None or x <= threshold)` 형태의 조건 찾기
   - `(x is None or x >= threshold)` 형태의 조건 찾기
   - 이런 패턴은 x가 None일 때 게이트를 무조건 통과시킨다
   - 파일: screeners/signals/patterns.py, screeners/signals/engine.py

2. 지표 임계값이 너무 느슨한 조건
   - 볼린저 밴드 관련: rsi_fade = rsi <= 50.0 같이 중립값을 "페이딩"으로 정의하는 경우
   - 패턴 수축: shrinking 조건이 실제로는 증가를 허용하는 경우 (e.g., * 1.05)
   - 파일: screeners/signals/patterns.py (score_band_reversion, detect_vcp_features)

3. 패턴 감지의 구조적 약점
   - W 패턴, Head & Shoulders 등에서 local minimum이 아닌 global minimum 사용
   - 시간 윈도우를 반으로 나누어 각각의 최솟값을 찾는 방식
   - 파일: screeners/signals/patterns.py (detect_w_pattern)

4. 조건 집합의 논리적 중복
   - `A AND B` 조건에서 B가 A의 상위집합(superset)인 경우 B가 필터 역할을 못 함
   - 파일: screeners/signals/engine.py (squeeze_breakout_condition 근처)

각 발견에 대해:
- 파일 경로와 라인 번호
- 문제 코드 스니펫
- 어떤 입력 조건에서 위양성이 발생하는지
- 수정 제안
```

---

## 섹션 2: Signal Scarcity 탐색

```
이 Python 투자 스크리닝 프로젝트(e:\side project\invest-prototype-main)의
시그널 엔진에서 정상적인 신호가 과도하게 억제되는 원인을 찾아라.

1. 쿨다운 로직의 과잉 적용
   - 파일: screeners/signals/engine.py
   - 찾을 것: `_is_ug_cooldown_blocked` 또는 유사한 쿨다운 함수
   - 확인: 쿨다운이 signal_code 또는 family별로 분리되어 있는가,
     아니면 모든 코드에 동일하게 적용되는가?
   - signal_code 파라미터가 함수 내에서 실제로 사용되는가?

2. AND 조건 과잉으로 신호 생성 희소화
   - 파일: screeners/signals/engine.py (_build_metrics, breakout_ready 계산)
   - 찾을 것: 8개 이상의 조건을 AND로 결합하는 bool 계산
   - 특히 `breakout_anchor_clear AND breakout_band_clear` 같이
     거의 같은 조건을 중복으로 요구하는 경우

3. 정상 범위 데이터에서 지표가 None을 반환하여 게이트 실패
   - 파일: utils/indicator_helpers.py
   - rolling 계산에서 min_periods가 window의 50% 미만인 경우
   - 이 경우 유효한 심볼이 지표 부재로 스크리닝에서 탈락

4. 서로 다른 전략 간 불필요한 상호 차단
   - 예: TF_REGULAR_PULLBACK과 TF_BREAKOUT이 같은 심볼에서
     독립적으로 동작해야 하지만 한쪽이 다른 쪽을 차단하는 경우
   - 파일: screeners/signals/engine.py (_trend_buy_events)

각 발견에 대해:
- 파일 경로와 라인 번호
- 어떤 심볼 유형 또는 시장 조건에서 정당한 신호가 억제되는가
- 억제 규모 추정 (전체 스캔 중 몇 %가 영향받는가)
```

---

## 섹션 3: 수치 안정성 및 데이터 경계 조건

```
이 Python 투자 스크리닝 프로젝트(e:\side project\invest-prototype-main)에서
수치 안정성 문제와 데이터 경계 조건 버그를 찾아라.

1. Python `or` 연산자와 0(정수) 충돌
   - 패턴: `(x or fallback)` 형태에서 x가 정수 0이 될 수 있는 경우
   - Python에서 `0 or fallback`은 fallback을 반환한다 (0이 falsy)
   - 이 패턴이 인덱스, 카운터, 길이 계산에 사용될 때 버그 발생
   - 파일: screeners/qullamaggie/core.py (base_start_idx 관련 계산)
   - 파일: screeners/signals/engine.py 전체

2. 분모 0 보호 불완전
   - `/ x` 또는 `/ (x - y)` 형태에서 분모가 0이 될 수 있는 경우
   - `_safe_float(x)` 반환값이 0일 때 후속 나눗셈이 무한대 반환
   - 파일: screeners/signals/patterns.py, utils/indicator_helpers.py

3. 최소 데이터 요구량 불일치
   - 함수 A가 50바를 요구하고 함수 B가 80바를 요구하는데
     동일 코드 경로에서 순서 없이 호출되는 경우
   - 각 함수의 len(frame) 체크가 일관된가?
   - 파일: screeners/signals/patterns.py의 모든 detect_* 함수들

4. rolling 윈도우에서 iloc[-N] 안전성
   - `frame.iloc[-N]` 사용 시 `len(frame) >= N` 체크가 있는가
   - lookback이 frame 크기보다 클 때의 처리
   - 파일: screeners/signals/engine.py (_build_metrics 함수)

각 발견에 대해:
- 파일 경로와 라인 번호
- 버그를 유발하는 최소 재현 시나리오
- 방어 코드 추가 제안
```

---

## 섹션 4: 운영 상태 관리 및 사이클 로직

```
이 Python 투자 스크리닝 프로젝트(e:\side project\invest-prototype-main)의
시그널 사이클 상태 관리에서 운영 문제를 찾아라.

1. 동일 심볼에서 BUY와 SELL 신호 동시 발생
   - 파일: screeners/signals/engine.py (_run_scope_scan)
   - 확인: buy candidate 처리와 sell 처리가 같은 심볼에 대해
     독립적으로 실행되는가?
   - 활성 사이클의 쿨다운이 만료된 경우 이 현상이 발생할 수 있는가?
   - 출력 파일에 같은 날 동일 심볼의 BUY+SELL 행이 공존하면
     신호 소비자(알림, 대시보드)가 어떻게 처리해야 하는가?

2. 동일 사이클에서 여러 SELL 신호 중복 발생
   - 파일: screeners/signals/engine.py (_trend_sell_events, _ug_sell_events)
   - 확인: 같은 날 같은 사이클에서 BREAKDOWN + CHANNEL_BREAK + TRAILING_BREAK가
     동시에 발생할 수 있는가?
   - 각 SELL 신호 간 우선순위 또는 상호 배제 로직이 있는가?

3. 스코프 간 상태 격리
   - 파일: screeners/signals/engine.py (_run_scope_scan)
   - "all" 스코프와 "screened" 스코프가 각각 독립적인 active_cycles와
     signal_history를 사용하는가?
   - 같은 심볼이 두 스코프에서 서로 다른 사이클 상태를 가질 수 있는가?

4. 시그널 히스토리 범위 쿼리 정확성
   - 파일: screeners/signals/cycle_store.py
   - `_history_rows_for_scope()` 함수가 scope를 정확히 필터링하는가?
   - scope 필드가 없거나 비어있는 히스토리 행은 어떻게 처리되는가?

각 발견에 대해:
- 발생 시나리오 (어떤 시장 조건에서)
- 실제 영향 (잘못된 포지션 진입/청산으로 이어질 수 있는가)
- 해결 방향
```

---

## 섹션 5: 스크리너별 로직 검증

```
이 Python 투자 스크리닝 프로젝트(e:\side project\invest-prototype-main)의
각 스크리너 알고리즘을 원저자의 방법론과 비교하여 이탈 사항을 찾아라.

1. Qullamaggie (screeners/qullamaggie/core.py)
   - base window 감지 로직: 베이스 시작점이 0일 때 prior_window 계산이
     올바른가? (`base_start_idx or len(daily)` 표현식 확인)
   - prior_run_pct가 베이스 돌입 전 상승폭을 정확히 반영하는가?
   - _find_base_window의 반환값이 None인 경우 처리

2. Leader-Lagging (screeners/leader_lagging/algorithms.py)
   - RS(Relative Strength) 점수 계산에서 126바 미만 데이터를 가진
     심볼(IPO 초기)이 어떻게 처리되는가?
   - confidence 점수가 낮을 때 RS 점수가 상위 시스템에 미치는 영향
   - `rs_proxy_confidence`가 source_priority_score에 반영되는가?

3. Mark Minervini (screeners/markminervini/screener.py)
   - 7가지 기술적 조건이 Minervini 원저 기준과 일치하는가?
   - ma150_60d_ago, ma200_20d_ago 계산에서 데이터 길이 조건이 일관된가?

4. Weinstein Stage 2 (screeners/weinstein_stage2/screener.py)
   - 주간 봉 계산에서 `W-SUN` 주기 분할이 미국/한국 시장 모두에서 올바른가?
   - as_of_date가 비거래일(주말, 공휴일)일 때 current_week_key 계산이 정확한가?
   - Stage 2 진입 조건이 30주 이동평균 기반인가?

각 발견에 대해:
- 어떤 방법론 기준에서 이탈했는가
- 이탈이 의도적인 수정인지 버그인지 판단 근거
- 원저 기준으로 수정할 경우 예상 신호 빈도 변화
```

---

## 섹션 6: 종합 알고리즘 강건성 점검 (빠른 스캔)

```
이 Python 투자 스크리닝 프로젝트(e:\side project\invest-prototype-main)를
다음 체크리스트로 빠르게 스캔하라:

[ ] screeners/signals/patterns.py: 모든 `rsi is None or` 패턴 목록화
[ ] screeners/signals/engine.py: 모든 `_ = signal_code` 또는 파라미터 즉시 버리는 패턴
[ ] screeners/signals/engine.py: `_UG_COOLDOWN_BUSINESS_DAYS`가 family별로 다르게 적용되는가
[ ] screeners/qullamaggie/core.py: `x or len(daily)` 형태의 표현식 목록화
[ ] 모든 screener 파일: `rolling(N, min_periods=M)`에서 M < N/2 인 경우
[ ] 모든 screener 파일: `iloc[-1]` 사용 전 len 체크 누락 위치
[ ] screeners/signals/engine.py: BUY와 SELL 신호가 같은 날 같은 symbol로 emit되는 경로
[ ] screeners/signals/patterns.py: 두 신호가 수학적으로 동시에 True가 될 수 없는 경우
    (bullish_close와 weak_close 등 상호 배제 조건 확인)
[ ] 전체: `_safe_float(x) or 0.0` 패턴에서 음수값이 0.0으로 대체되는 경우

결과를 체크리스트 형식으로 반환하고, 각 항목에 대해
파일 경로와 라인 번호, 이슈 유무를 명시하라.
```

---

## 주의 사항

- 이미 알려진 이슈(아래)는 중복 보고하지 않아도 된다:
  - `mr_long_ready` / `mr_short_ready`의 RSI None bypass (patterns.py:477,496)
  - VCP `shrinking_depth * 1.05` (patterns.py:222)
  - W 패턴 `idxmin()` global minimum (patterns.py:153)
  - PBS `rsi_fade <= 50.0` (patterns.py:462)
  - UG 쿨다운 family-agnostic (engine.py:5942)
  - `breakout_ready` 8조건 (engine.py:2913)
  - TF 다중 SELL 동시 발생 (engine.py:5467)
  - `squeeze_breakout_condition`의 중복 조건 (engine.py:6014)
  - `base_start_idx = 0` falsy 버그 (qullamaggie/core.py:356)
  - `min_periods=5` 저품질 지표 (qullamaggie/core.py:313)
  - BUY+SELL 동시 발생 (engine.py:7960)

- 전체 이슈 목록은 `docs/audits/2026-04-26-signal-algorithm-audit.md`와
  `docs/audits/2026-04-26-signal-algorithm-audit-v2.md` 참조
