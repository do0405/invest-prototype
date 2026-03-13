# Minervini VCP + Cup-with-Handle Quant Screener PRD v2

작성일: 2026-03-12
대상 데이터: KR / US 다종목 일봉 OHLCV (최근 1년)

## 1. 문서 목적

이 문서는 Mark Minervini 계열의 `VCP(Volatility Contraction Pattern)`와 `Cup-with-Handle`를 `KR/US 다종목 일봉 OHLCV 1년치`만으로 스크리닝하기 위한 `제품 요구사항 문서(PRD)`다.

이번 버전의 목적은 단순 아이디어 문서가 아니라 아래를 모두 포함한 `실행 가능한 제품 설계`로 정리하는 데 있다.

1. 입력 데이터 계약을 명확히 정의한다.
2. KR/US 혼합 유니버스에서 동작 가능한 공통 규칙과 시장별 예외를 분리한다.
3. VCP, Cup-with-Handle, 최근 5영업일 돌파를 하나의 상태 머신으로 통합한다.
4. 규칙 기반 탐지, 점수화, 출력 스키마, 검증 프레임워크까지 포함한다.
5. 이후 바로 Python 스크리너 구현으로 연결할 수 있게 한다.

본 문서는 “원문 요약”보다 `핵심 알고리즘`, `데이터 설계`, `출력 계약`, `검증 절차`에 집중한다.

## 2. 제품 목표

### 2.1 핵심 문제

사용자는 다수 종목의 일봉 CSV만 가지고도 아래 3종류를 안정적으로 찾고 싶다.

- `형성 중인 VCP / 컵앤핸들`
- `완성되었지만 아직 돌파하지 않은 패턴`
- `최근 5영업일 이내 돌파한 패턴`

문제는 VCP와 컵앤핸들이 재량적 설명에 많이 의존한다는 점이다. 따라서 본 제품의 핵심 가치는 “사람이 보던 패턴을 기계가 반복 가능하게 찾는 것”이다.

### 2.2 타깃 사용자

- discretionary swing trader
- pattern 기반 watchlist를 관리하는 리서처
- 추후 백테스트/자동화로 확장하려는 퀀트 개발자

### 2.3 제품 산출물

기본 산출물은 아래 3개 리스트다.

1. `forming watchlist`
2. `completed / ready watchlist`
3. `recent breakout list (5 trading days)`

각 종목은 단순 통과 여부가 아니라 `패턴 유형`, `상태`, `피벗 가격`, `무효화 가격`, `점수`, `설명 필드`까지 함께 반환한다.

## 3. 성공 기준

### 3.1 기능 성공 기준

- KR/US 혼합 입력을 같은 파이프라인에서 처리할 수 있어야 한다.
- 종목별로 `VCP`, `Cup-with-Handle`, `상태`, `pivot`, `breakout 여부`가 결정돼야 한다.
- 출력은 사람이 재검토 가능한 설명 필드를 포함해야 한다.

### 3.2 품질 성공 기준

- 상위 50개 `READY` 후보의 수작업 시각 검토 precision이 `70% 이상`이면 1차 합격으로 본다.
- 상위 30개 `BROKEOUT_RECENT` 후보에서 “돌파 구조가 명확하지 않다”는 오탐이 `30% 미만`이어야 한다.
- 분할/권리락 등 데이터 이상으로 인한 명백한 가짜 패턴은 전체 후보의 `5% 미만`이어야 한다.

### 3.3 운영 성공 기준

- 일봉 1년치 기준 `3,000~8,000 종목` 배치를 노트북 환경에서 수 분 내 처리할 수 있어야 한다.
- 결과는 동일 입력에 대해 결정론적으로 재현되어야 한다.

## 4. 범위 정의

### 4.1 반드시 구현할 것

- KR/US 공통 입력 스키마 처리
- 가격 보정 가능 시 adjusted 기반 구조 판정
- 유동성 필터
- Trend Template Lite
- 상대강도 percentile
- pivot extraction
- VCP detector
- Cup-with-Handle detector
- `FORMING / COMPLETED / BROKEOUT_RECENT` 상태 분류
- confidence score와 설명 필드

### 4.2 나중에 붙일 것

- fundamentals 성장 필터
- 섹터/업종 리더십 엔진
- intraday ORB / opening range breakout 확인
- 알림 및 자동 주문
- 차트 이미지 리포트 자동 생성

### 4.3 의도적으로 제외할 것

- 딥러닝 이미지 분류기 기반 블랙박스 탐지
- intraday 데이터 없이 판단 불가능한 execution logic
- 단일 파라미터 최적화로 만든 overfit 전략

## 5. 입력 데이터 계약

### 5.1 필수 컬럼

- `ticker`
- `date`
- `open`
- `high`
- `low`
- `close`
- `volume`

### 5.2 강력 권장 컬럼

- `market` (`KR` / `US`)
- `adj_close`
- `exchange`
- `currency`
- `name`
- `sector`
- `industry`

### 5.3 최소 데이터 길이

- 종목별 최소 `220 bars`
- 권장 최소 `252 bars`

### 5.4 시장 혼합 처리 원칙

KR/US가 같이 들어올 때는 아래 원칙을 쓴다.

1. `최근 5영업일`은 시장 공통 달력이 아니라 `각 종목의 최신 5개 일봉`으로 정의한다.
2. 유동성 임계값은 KR과 US를 같은 절대값으로 비교하지 않고 `market별 threshold 또는 percentile`로 처리한다.
3. 상대강도 percentile은 기본적으로 `market 내부 percentile`을 사용한다.
4. 글로벌 통합 랭킹이 필요하면 `market 내 z-score 정규화 후 결합`을 사용한다.

### 5.5 권장 입력 형태

long-format DataFrame:

- `ticker, market, date, open, high, low, close, volume, adj_close(optional)`

## 6. 데이터 전처리 요구사항

### 6.1 정렬과 무결성

- `ticker, date` 기준 오름차순 정렬
- 중복 날짜 제거 또는 병합 로그 기록
- `high < low`, 음수 가격, 음수 거래량 같은 비정상 row 제거
- 장기 결측이 많은 종목은 `data_quality_flag`를 남기고 제외 가능하게 한다.

### 6.2 결측 처리

- 가격 결측은 해당 row 제거
- 거래량 결측은 0으로 채우지 않고 `NaN` 유지
- `volume_ma`는 충분한 `min_periods`를 요구
- 최근 구간 결측이 많으면 패턴 판단 불가로 제외

### 6.3 가격 보정

우선순위는 아래와 같다.

1. `adj_close`가 있으면 `adj_factor = adj_close / close`를 계산해 OHLC까지 일관되게 조정한다.
2. 조정주가가 없으면 raw 가격으로 진행하되 `adjustment_confidence = low` 플래그를 남긴다.
3. split/권리락으로 추정되는 이상 갭이 보이면 후보에서 강한 감점을 준다.

권장 조정식:

- `adj_open  = open  * adj_factor`
- `adj_high  = high  * adj_factor`
- `adj_low   = low   * adj_factor`
- `adj_close = adj_close`
- `adj_volume = volume / adj_factor`

### 6.4 통화와 거래대금

KRW와 USD는 절대 가격이 다르므로, 유동성은 `price`보다 `거래대금` 기준으로 본다.

- `traded_value = close * volume`
- `tv_ma20`, `tv_median20` 계산

기본 게이트는 아래 두 방식 중 하나로 구현한다.

- `market별 절대 threshold`
- `market별 traded_value percentile gate`

MVP에서는 `market별 percentile gate`를 추천한다.

## 7. 보조지표와 파생 피처

### 7.1 필수 지표

- `SMA20, SMA50, SMA150, SMA200`
- `ATR14`, `ATR% = ATR14 / close * 100`
- `vol_ma20`, `vol_ma50`
- `range_5p`, `range_10p`
- `ret_21d`, `ret_63d`, `ret_126d`, `ret_252d`
- `tv_median20`, `tv_ma20`

### 7.2 권장 지표

- `log_return_vol20`
- `distance_to_52w_high`
- `distance_from_sma50`
- `distribution_day_count_20`
- `up_down_volume_ratio_20`

### 7.3 상대강도 점수

기본식:

`RS_raw = 0.4 * ret_63d + 0.3 * ret_126d + 0.3 * ret_252d`

`RS_percentile_market = pct_rank(RS_raw within same market)`

선택식:

- `RS_percentile_global = pct_rank(RS_raw across all symbols)`

## 8. 공통 선행 필터

### 8.1 유동성 필터

기본 게이트:

- `bars >= 220`
- `tv_median20 >= market_threshold`
- 최근 60일 안에 극단적 결측/거래정지 없음

권장 기본값:

- 절대값보다 `market 내 상위 60% 이상` 또는 `하위 40% 제외`

### 8.2 Trend Template Lite

일봉 기반 Minervini식 전처리 게이트다.

필수 조건:

- `close > SMA50`
- `close > SMA150`
- `close > SMA200`
- `SMA50 > SMA150 >= SMA200`
- `SMA200_t > SMA200_t-20`
- `close >= 0.75 * rolling_252d_high`
- `close >= 1.25 * rolling_252d_low`

권장 조건:

- `distance_to_52w_high <= 15%`
- `RS_percentile_market >= 70`

### 8.3 prior run-up 조건

패턴은 상승 추세 이후 형성되는 연속형 베이스를 우선한다.

권장 게이트:

- base 시작 이전 `40~120일` 구간의 저점 대비 고점 상승률 `>= 25%`
- 또는 `ret_63d >= 20%`

## 9. Pivot Extraction Engine

### 9.1 기본 철학

패턴 탐지의 핵심은 raw 시계열이 아니라 `swing structure`를 얻는 것이다.

### 9.2 권장 접근

1. 구조 판정용으로 `adjusted close` 또는 `smoothed close`를 사용한다.
2. `Savitzky-Golay` 또는 `kernel regression`으로 스무딩한다.
3. `5-bar` 또는 `7-bar` 국소 극값으로 pivot 후보를 추출한다.
4. 최종 pivot 가격은 raw `high/low`에 다시 매핑한다.

### 9.3 노이즈 억제 규칙

- 인접 pivot 간 가격 차가 `max(4%, 1.5 * ATR% 수준)`보다 작으면 merge
- pivot prominence가 너무 작은 경우 제거
- 지나치게 민감한 극값 탐지는 오탐을 키우므로 `strict / relaxed` 모드 제공

### 9.4 출력

각 pivot은 아래 속성을 가진다.

- `pivot_date`
- `pivot_type` (`H` / `L`)
- `pivot_price`
- `prominence_pct`
- `swing_index`

## 10. VCP Detector

### 10.1 핵심 정의

VCP는 `2~4회`의 점진적 수축이 우측으로 갈수록 타이트해지는 패턴으로 본다.

핵심 구조:

`H0 -> L1 -> H1 -> L2 -> H2 ... -> Lk -> Hk`

### 10.2 탐색 창

- base search window: 최근 `120 bars`
- 실제 VCP base 길이: `15~80 bars`
- `H0`는 최근 90 bars 내 유의미한 고점

### 10.3 정량 규칙

- 수축 개수 `k = 2~4`, 기본 권장 `3`
- 첫 수축 깊이 `8% ~ 35%`
- 마지막 수축 깊이 `<= 12%`
- 감소 규칙:
  - `depth_{i+1} <= 0.85 * depth_i`
  - 또는 `depth_{i+1} <= depth_i - 2%p`
- 각 수축 길이 `>= 5 bars`
- 마지막 5일 타이트함:
  - `range_5p <= 8%` 권장
  - strict mode는 `<= 5%`
- 저항 정렬:
  - 마지막 두 고점 차이 `<= 3%`
  - 전체 저항대 band `<= 6%`

### 10.4 변동성/거래량 건조 규칙

- `ATR%_last10 <= 0.7 * ATR%_first10`
- `vol_ma10_last <= 0.8 * vol_ma50_current` 또는 contraction별 평균 거래량 감소
- 최근 15 bars의 median range가 base 초반보다 작아야 함

### 10.5 VCP pivot 정의

기본값:

- `pivot_price = max(highs of final resistance band)`

실전형 대안:

- `pivot_price = 마지막 1~2개 수축 상단의 최댓값`

### 10.6 상태 정의

- `FORMING_VCP`: 수축 구조는 일부 충족하나 최종 타이트 구간 또는 우측 회복이 아직 진행 중
- `COMPLETED_VCP`: 구조와 pivot이 확정되었지만 아직 유효 돌파 없음
- `BREAKOUT_VCP_RECENT`: 최근 5개 일봉 안에 유효 돌파 있음

## 11. Cup-with-Handle Detector

### 11.1 핵심 정의

컵앤핸들은 `좌림(A) -> 바닥(B) -> 우림(C) -> 핸들 저점(D)` 구조를 가진 연속형 베이스다.

### 11.2 탐색 창

- cup search window: 최근 `160 bars`
- cup length: `30~120 bars`
- extended mode: `<= 160 bars`
- handle length: `5~20 bars`

### 11.3 정량 규칙

- 림 높이 일치:
  - `abs(rim_L - rim_R) / rim_L <= 5%`
- cup depth:
  - 기본 `12% ~ 35%`
  - relaxed mode `<= 45%`
- U-shape 완만함:
  - bottom zone width `>= 5 bars`
  - 저점 부근 체류 `>= cup_len * 0.1`
  - 저점 직후 3 bars 내 급반등으로 depth의 50% 이상 회복 시 V-shape 감점
- handle depth:
  - `<= 10~12%`
  - 또는 `<= 1/3 of cup advance`
- handle 위치:
  - `handle_low >= (cup_low + rim_level) / 2`
- handle 거래량:
  - handle 평균 거래량 `< vol_ma50` 또는 cup 우측 상승 구간 평균보다 낮아야 함

### 11.4 Cup pivot 정의

- `pivot_price = handle 구간 고점`
- handle이 불완전하면 `state = FORMING_HANDLE`로 유지하고 `pivot_price = rim_R`를 임시값으로 둘 수 있다.

### 11.5 상태 정의

- `FORMING_CUP`: 컵 구조는 진행 중이나 우림 미완성
- `FORMING_HANDLE`: 컵은 완성, 핸들만 형성 중
- `COMPLETED_CWH`: 컵과 핸들이 완성, 아직 돌파 전
- `BREAKOUT_CWH_RECENT`: 최근 5개 일봉 안에 유효 돌파 있음

## 12. 돌파 판정 모듈

### 12.1 공통 breakout 정의

보수적 기본값:

- `close_t > pivot * (1 + eps)`
- `volume_t >= k * vol_ma50_t`

권장 기본값:

- `eps = 0.003` (0.3%)
- `k = 1.4`

공격적 대안:

- `high_t > pivot * (1 + eps)` and `close_t >= pivot`

### 12.2 최근 5영업일 정의

- `breakout_date`가 종목별 최신 5개 row 안에 존재하면 `BROKEOUT_RECENT`
- KR/US 달력 차이 때문에 글로벌 business day가 아니라 종목별 row 기준을 쓴다.

### 12.3 breakout 품질 유지 조건

최근 돌파 리스트에 남기기 위한 보조 조건:

- 최신 종가 `>= pivot * 0.97`
- breakout 이후 최고가 대비 되밀림 `<= 8%`
- 이미 pivot 대비 `+10% 이상` 멀어진 종목은 `extended` 태그

## 13. 상태 머신

제품 수준에서는 `state_bucket`과 `state_detail`을 분리한다.

### 13.1 state_bucket

- `FORMING`
- `COMPLETED`
- `BROKEOUT_RECENT`
- `STALE`
- `FAILED`

### 13.2 state_detail

- `FORMING_VCP`
- `COMPLETED_VCP`
- `BREAKOUT_VCP_RECENT`
- `FORMING_CUP`
- `FORMING_HANDLE`
- `COMPLETED_CWH`
- `BREAKOUT_CWH_RECENT`

이 구조를 쓰면 제품 UI와 알고리즘 디버깅을 동시에 단순화할 수 있다.

## 14. 점수 체계

### 14.1 상위 점수 구조

- `LeaderScore` 0~25
- `PatternQualityScore` 0~35
- `TightnessDryupScore` 0~20
- `BreakoutReadinessScore` 0~20

`FinalScore = LeaderScore + PatternQualityScore + TightnessDryupScore + BreakoutReadinessScore`

### 14.2 LeaderScore

- Trend Template 충족도
- RS percentile
- 52주 고점 근접도
- 유동성 안정성

### 14.3 PatternQualityScore

VCP:

- 수축 횟수와 감소 일관성
- 저항 정렬
- prior run-up 존재
- base 길이/깊이 건강도

Cup-with-Handle:

- cup depth/length 적정성
- 림 정렬
- U-shape 완만함
- handle upper-half 유지 여부

### 14.4 TightnessDryupScore

- ATR% 감소
- 최근 5일/10일 range 축소
- 거래량 건조
- distribution day 억제

### 14.5 BreakoutReadinessScore

FORMING:

- pivot 근접도
- 구조 진행률

COMPLETED:

- pivot 바로 아래 위치
- 최근 타이트 구간
- volume dry-up

BROKEOUT_RECENT:

- volume multiple
- breakout 후 hold 여부
- pivot 대비 과확장 여부

## 15. 출력 데이터 계약

### 15.1 필수 출력 필드

- `ticker`
- `market`
- `pattern_type`
- `state_bucket`
- `state_detail`
- `pattern_start`
- `pattern_end`
- `pivot_price`
- `breakout_date`
- `breakout_price`
- `breakout_volume`
- `volume_multiple`
- `invalidation_price`
- `final_score`

### 15.2 권장 출력 필드

- `base_length_bars`
- `base_depth_pct`
- `cup_depth_pct`
- `handle_depth_pct`
- `contraction_count`
- `range_5p`
- `atrp14`
- `rs_percentile_market`
- `tv_median20`
- `trend_score`
- `tightness_score`
- `volume_dryup_score`
- `quality_flags`
- `notes`

### 15.3 notes 예시

- `VCP(3 contractions: 24%-13%-7%, range_5=4.1%, vol_dryup=0.64x)`
- `CWH(cup_depth=22%, handle_depth=7%, handle upper-half ok)`

## 16. 기능 요구사항

### 16.1 필수 기능

- 다종목 batch run
- 시장별 그룹화 처리
- 종목당 다중 패턴 후보 지원
- 동일 종목의 VCP/CWH 중복 탐지 허용
- 결과 CSV 또는 DataFrame 반환

### 16.2 설명 가능성 요구사항

모든 상위 후보는 사람이 왜 뽑혔는지 이해할 수 있어야 한다.

필수 설명 요소:

- 어떤 패턴으로 분류됐는지
- pivot이 얼마인지
- 최근 돌파인지 아닌지
- 타이트함/건조도가 어느 정도인지
- 무효화 가격이 어디인지

### 16.3 성능 요구사항

- lookahead bias가 없어야 한다.
- 종목 간 독립 처리가 가능해야 한다.
- 결과는 deterministic해야 한다.

## 17. 검증 프레임워크

### 17.1 1차 검증: 시각 감사

- 상위 후보를 패턴별 50개씩 샘플링
- `정탐 / 오탐 / 애매`로 수작업 라벨링
- bucket별 precision 측정

### 17.2 2차 검증: 이벤트 성과

`BROKEOUT_RECENT`에 대해 아래를 본다.

- 이후 5일 / 10일 / 20일 수익률
- 손절 기준 하회 비율
- 패턴별 hit rate

### 17.3 라벨 정의 예시

- 성공: 돌파일 이후 20거래일 내 `+10%` 이상 상승 and `-7%` 이상 깨지지 않음
- 실패: 돌파일 이후 짧은 기간 내 invalidation price 하회

### 17.4 파라미터 튜닝 원칙

- 단일 최적값보다 `robust region`을 찾는다.
- 워크포워드 방식으로 튜닝한다.
- 시간 누수를 피하기 위해 일반 K-fold 대신 시계열 검증을 쓴다.

### 17.5 현실적 제약

입력 데이터가 1년뿐이면 “정식 검증”이 아니라 “개발용 sanity check” 수준에 가깝다. 제품 검증은 추후 `3~5년 이상` 데이터 확장 후 수행하는 것이 바람직하다.

## 18. 구현 전개 순서

### Phase 1

- 데이터 계약 확정
- 전처리
- 조정주가 처리
- 지표 계산
- 유동성 필터와 Trend Template Lite

### Phase 2

- pivot extraction 엔진
- VCP detector
- VCP 상태 분류

### Phase 3

- Cup-with-Handle detector
- 공통 breakout 모듈
- 최근 5영업일 분류

### Phase 4

- 점수 체계
- 출력 스키마
- notes/quality flags
- top candidate 시각 검증 도구

### Phase 5

- 워크포워드 검증
- 민감도 분석
- 시장별 threshold 보정

## 19. 핵심 의사코드

```python
for market, df_market in ohlcv_all.groupby("market"):
    df_market = preprocess(df_market)
    df_market = add_indicators(df_market)
    rs_table = build_rs_percentile(df_market)

    for ticker, df in df_market.groupby("ticker"):
        if not liquidity_ok(df, market):
            continue
        if not trend_template_ok(df, rs_table):
            continue

        pivots = extract_pivots(df)
        vcp_candidates = detect_vcp(df, pivots)
        cwh_candidates = detect_cup_handle(df, pivots)

        for candidate in vcp_candidates + cwh_candidates:
            breakout = check_breakout_last_5_rows(df, candidate.pivot)
            state_bucket, state_detail = classify(candidate, breakout)
            score = score_candidate(df, candidate, breakout, rs_table)
            emit(candidate, breakout, state_bucket, state_detail, score)
```

## 20. 리스크와 오픈 이슈

- `adj_close`가 없는 raw 데이터는 split/권리락으로 가짜 패턴이 생길 수 있다.
- KR/US를 완전히 하나의 랭킹으로 합칠지, 시장별 랭킹으로 분리할지 제품 정책이 필요하다.
- 1년 데이터만으로는 장기 컵 일부를 놓칠 수 있다.
- fundamentals 없이 technical만으로는 Minervini SEPA 전체를 재현하지 못한다.

## 21. 참고 소스

- Lo, Mamaysky, Wang, *Foundations of Technical Analysis*  
  [https://www.nber.org/papers/w7613](https://www.nber.org/papers/w7613)
- Zaib et al., *Pattern Recognition in Financial Time Series Data Using Perceptually Important Points*  
  [https://arxiv.org/abs/cs/0412003](https://arxiv.org/abs/cs/0412003)
- Fidelity, *Cup with Handle*  
  [https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cup-with-handle](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cup-with-handle)
- HumanRupert, *marketsmith_pattern_recognition*  
  [https://github.com/HumanRupert/marketsmith_pattern_recognition](https://github.com/HumanRupert/marketsmith_pattern_recognition)
- marco-hui-95, *vcp_screener.github.io*  
  [https://github.com/marco-hui-95/vcp_screener.github.io](https://github.com/marco-hui-95/vcp_screener.github.io)
- BennyThadikaran, *stock-pattern wiki / Pattern Algorithms*  
  [https://github.com/BennyThadikaran/stock-pattern/wiki/Pattern-Algorithms](https://github.com/BennyThadikaran/stock-pattern/wiki/Pattern-Algorithms)

위 레퍼런스에 더해, 사용자 제공 메모 `VCP, Cup and handle.md`의 데이터 전처리, adjusted price 처리, 출력 필드, 검증 프레임워크 아이디어를 본 PRD에 통합했다.