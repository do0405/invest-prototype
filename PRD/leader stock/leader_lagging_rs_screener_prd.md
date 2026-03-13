# Leader-Lagging RS Screener PRD

작성일: 2026-03-12

## 1. 문서 목적

이 문서는 `주도주(leader stock) 스크리너`와 `후행 2등주(lagging follower) 스크리너`를 하나의 정량 시스템으로 설계하기 위한 PRD 초안이다.

핵심 목표는 아래 4가지다.

1. RS와 RS Line을 핵심 고려 요소로 포함하되, 추세/그룹/거래량/이벤트와 함께 `지금 시장의 진짜 리더`를 찾는다.
2. 단순 약세주가 아니라 `강한 그룹 안에서 뒤늦게 반응할 가능성이 높은 follower`를 찾는다.
3. 논문에서 확인된 `모멘텀`, `산업 리더십`, `정보 확산 지연`, `연결된 종목 간 spillover`를 스크리너 규칙으로 번역한다.
4. 실전 트레이더 계보인 `O'Neil/IBD -> Weinstein -> Minervini -> Qullamaggie` 흐름과 충돌하지 않는 형태로 제품화를 설계한다.

본 문서는 종목 추천이 아니라 `PRD 작성용 핵심 알고리즘과 전개`에 초점을 둔다. 따라서 아래 공식, 점수, 컷오프는 원전의 질적 원칙을 정량화한 `설계 추론`이 포함된다.

즉, RS/RS Line은 이 문서에서 매우 중요한 feature이지만, 스크리너 전체를 대표하는 단독 선별 엔진으로 취급하지 않는다.

## 2. 소스 정책

### 2.1 허용 소스

- RS, Relative Performance, Mansfield RS 관련 공식/직접 계보 자료
- 모멘텀과 lead-lag를 다룬 학술 논문
- RS 스크리너 구현에 직접 연결되는 오픈소스 저장소
- 트레이더 본인 공식 사이트 또는 공식적으로 배포된 자료

### 2.2 의도적으로 제외한 것

- 출처 불명 블로그 요약
- 임의의 SNS 쓰레드
- invite-only 스크립트
- "유명하니까 맞다" 식의 비검증 룰

### 2.3 설계 순도 원칙

이 PRD의 중심축은 아래 순서다.

`시장/그룹 리더십 -> RS/RS Line -> 개별 리더 판정 -> lagging follower 판정 -> 이벤트 재정렬`

즉, 이 시스템은 단순히 강한 종목과 약한 종목을 나누는 것이 아니라, `리더십`과 `지연 확산`을 함께 다루는 멀티팩터 스크리너다. RS/RS Line은 그 안의 핵심 모듈 중 하나다.

## 3. 전략 테제

참고 자료를 종합하면 아래 명제가 일관되게 나온다.

1. 강한 수익은 대체로 `과거 3-12개월 winner`에서 이어진다.
2. 그 winner 효과의 상당 부분은 `산업/그룹 리더십`으로 설명된다.
3. 주도주는 종종 `RS Line이 가격보다 먼저 신고가`를 만들며 나타난다.
4. 후행 2등주는 보통 아무 약세주가 아니라 `강한 그룹, 강한 연결성, 느린 정보 확산`의 산물이다.
5. 따라서 리더 스크리너와 2등주 스크리너는 같은 유니버스에서 돌리되, `랭킹 로직`은 분리해야 한다.

## 4. 핵심 정의

### 4.1 주도주

본 PRD에서 주도주는 아래를 동시에 만족하는 종목이다.

- 시장 또는 섹터 대비 상대성과가 높다.
- RS Line이 상승 중이며 대개 신고가 또는 신고가 근처다.
- 같은 그룹 내부에서도 상위권이다.
- 가격 구조가 손상되지 않았고, 돌파 또는 재가속 준비가 되어 있다.

### 4.2 후행 2등주

본 PRD에서 후행 2등주는 아래를 의미한다.

- 이미 검증된 리더와 같은 산업, 테마, 공급망, 공통 애널리스트 커버리지 등의 연결고리가 있다.
- 그룹은 강하지만 본인은 아직 leader만큼 가격 반응이 다 반영되지 않았다.
- 그러나 구조적으로는 망가지지 않았고, RS Line은 개선 초기다.
- 즉 `약한 쓰레기주`가 아니라 `지연 반응하는 follower`다.

### 4.3 RS, RS Line, Mansfield RS

- `RS Line`: 기준지수 대비 상대가격선. 기본 구현은 `stock_close / benchmark_close`.
- `RS Percentile`: 유니버스 내 상대성과 순위 점수.
- `Mansfield RS`: RS Line을 장기 평균 대비 정규화한 오실레이터형 표현.

Stage Analysis 공식 자료에 따르면 Weinstein 계열의 상대강도는 본질적으로 `종목 가격을 시장 평균 가격으로 나누는 것`이다. 본 문서의 상세 점수화는 이 정의를 운영용으로 확장한 것이다.

### 4.4 멀티팩터 원칙

이 스크리너는 RS-only 설계가 아니라 아래 6축 결합을 기본으로 한다.

- Leader 판단: `Trend -> Group -> RS -> Volume -> Structure -> Catalyst`
- Follower 판단: `Linkage -> Group -> Underreaction -> Structure -> Event -> RS Inflection`

RS/RS Line은 leader와 follower 모두에서 중요한 확인 도구지만, 단독 선별 기준으로 쓰지 않는다.

## 5. 제품 범위

### 5.1 반드시 구현할 것

- Benchmark 대비 RS/RS Line 엔진
- 산업/섹터 RS 엔진
- 추세/구조 품질 엔진
- 거래량/수급 품질 엔진
- catalyst/event 엔진
- `Leader Screener`
- `Lagging Follower Screener`
- leader와 follower를 연결하는 `Pair/Peer Link Engine`
- 이벤트 기반 재정렬 모듈
- 일간/주간 랭킹 출력

### 5.2 나중에 붙일 것

- 공급망/공통 애널리스트/세그먼트 기반 네트워크 연결 강화
- 실시간 장중 경보
- 자동 실행 엔진
- 뉴스/NLP 기반 catalyst 분류기

### 5.3 의도적으로 제외할 것

- 저RS 가치주 발굴
- mean reversion 시스템
- 바닥주 턴어라운드 전용 스크리너
- 단기 초고빈도 execution

## 6. 연구에서 추출한 핵심 원칙

### 6.1 모멘텀은 기본 팩터다

- Jegadeesh-Titman 계열 연구는 과거 3-12개월 winner가 이후에도 초과성과를 보인다는 점을 보여준다.
- 따라서 leader 스크리너는 `중기 성과 우위`를 핵심 축 중 하나로 가져야 한다.

### 6.2 개별 종목보다 산업/그룹이 먼저다

- Moskowitz-Grinblatt는 개별 모멘텀의 큰 부분이 산업 모멘텀으로 설명된다고 보였다.
- 따라서 `그룹 RS`는 보조지표가 아니라 선행 게이트여야 한다.

### 6.3 lead-lag는 산업 내부에서 특히 강하다

- Hou는 lead-lag가 주로 산업 내부의 느린 정보 확산에서 발생한다고 봤다.
- Brennan-Jegadeesh-Swaminathan은 애널리스트가 많이 붙은 종목이 적게 붙은 종목을 선행할 수 있음을 보였다.
- Ali-Hirshleifer는 shared analyst coverage가 여러 spillover 현상을 상당 부분 통합한다고 제시했다.

### 6.4 후행 2등주는 "약한 주식"이 아니라 "덜 반영된 연결 종목"이다

- Cen 외 연구는 산업 leader의 다중 세그먼트 정보가 다른 종목에 느리게, 때로는 왜곡되어 확산됨을 보였다.
- 따라서 2등주 스크리너는 `절대 약세`보다 `연결성 + 지연 반응`을 핵심으로 봐야 한다.

### 6.5 RS Line은 가격보다 앞서 리더십을 드러낼 수 있다

- IBD 계열과 관련 오픈소스 구현은 `RS New High`, `RS New High Before Price`를 핵심 신호로 취급한다.
- 따라서 leader 판정에서 `RS Line 선행성`은 별도 점수 항목이어야 한다.

### 6.6 52주 신고가 근처와 구조 보존이 중요하다

- 52주 신고가/근접성 연구는 모멘텀과 앵커링 효과의 연결을 보여준다.
- Minervini 계열 규칙도 `52주 고점 근처`, `고RS`, `상승 추세 템플릿`을 강조한다.

### 6.7 이벤트는 2등주 전개를 가속할 수 있다

- PEAD와 제한적 주의 연구는 earnings/news 이후에도 반응이 천천히 이어질 수 있음을 보여준다.
- 따라서 leader의 실적 갭업, 섹터 뉴스, 원자재 급등 등은 follower 재정렬 트리거가 될 수 있다.

## 7. 시스템 아키텍처

### 7.1 전체 파이프라인

1. `Universe Builder`
2. `Benchmark/Group RS Engine`
3. `Leader Classification Engine`
4. `Peer Link Engine`
5. `Lagging Follower Engine`
6. `Event Re-Ranking Engine`
7. `Output Layer`

### 7.2 실행 주기

- 주간:
  - 유니버스 정리
  - 산업/섹터 랭킹 갱신
  - leader 후보군 갱신
- 일간:
  - RS/RS Line 재계산
  - leader/follower 점수 갱신
  - earnings/gap/news 기반 재정렬
- 이벤트 발생 시:
  - 특정 leader 기준 peer universe 재랭킹

## 8. 데이터 요구사항

### 8.1 필수 데이터

- Daily OHLCV
- 조정종가
- 기준지수 종가
- 섹터/산업 분류
- ADV20, ADV50
- ATR20, ADR20
- 52주 고점/저점
- earnings calendar와 실적 발표일

### 8.2 있으면 좋은 데이터

- 애널리스트 커버리지 수
- 공급망/고객사/벤더 관계
- 사업 세그먼트 매출 비중
- 공통 ETF/테마 바스켓 소속 정보
- 공매도/옵션/기관 수급

### 8.3 벤치마크 체계

벤치마크는 고정 하나보다 계층형이 좋다.

- 1차: broad market benchmark
  - 예: `SPY`, `QQQ`, `KOSPI200`, `KOSDAQ150`
- 2차: sector benchmark
  - 예: sector ETF 또는 업종지수
- 3차: group benchmark
  - 동일 산업 basket equal-weight index

## 9. 공통 Relative Strength Module

이 섹션은 스크리너 전체가 아니라 `relative strength 계열 feature 묶음`을 정의하는 부분이다. 실제 최종 선별은 이 모듈을 그룹, 추세, 구조, 거래량, 이벤트 모듈과 결합해 수행한다.

### 9.1 기본 RS Line

가장 기본 구현은 아래다.

```text
RS_Line_t = Close_stock_t / Close_benchmark_t
```

### 9.2 운영용 RS Score

IBD 스타일 구현을 참고한 실무용 RS Score는 아래처럼 둔다.

```text
Raw_RS = 0.40 * Return_3M
       + 0.20 * Return_6M
       + 0.20 * Return_9M
       + 0.20 * Return_12M

RS_Percentile = PercentileRank(Raw_RS within universe)
```

설명:

- 최근 분기에 40% 가중치를 주는 방식은 IBD 스타일 오픈소스 구현과 일치한다.
- 실전에서는 `3M, 6M, 12M` 3축만 써도 되지만, PRD 기준 기본안은 위 4분기 가중식이 가장 직관적이다.

### 9.3 RS Line 상태 변수

필수 상태값:

- `rs_pct_252`
- `rs_line_20d_slope`
- `rs_line_65d_high_flag`
- `rs_line_250d_high_flag`
- `rs_new_high_before_price_flag`
- `rs_line_distance_to_high`
- `mansfield_rs`
- `mansfield_rs_slope`

### 9.4 RS New High Before Price 정의

운영 정의:

```text
rs_new_high_before_price_flag =
    (RS_Line_t >= max(RS_Line_{t-250:t-1}))
    and
    (Close_t < max(Close_{t-250:t-1}))
```

의미:

- 가격은 아직 52주 고점을 돌파하지 않았는데 RS Line이 먼저 신고가를 찍는 경우다.
- 이 상태는 `리더십 선행성`을 의미하므로 leader 쪽에서 높은 점수를 준다.

### 9.5 Mansfield RS 운영 정의

Stage Analysis 계열의 취지를 따라, 운영용 정규화 버전은 아래처럼 둔다.

```text
mansfield_rs = 100 * (RS_Line / SMA(RS_Line, 52w) - 1)
```

이는 원전의 개념을 구현 가능한 수식으로 풀어쓴 설계 추론이다. 실제 배포 시 주봉/일봉 버전을 분리해 둘 수 있다.

## 10. Market / Group Leadership Engine

### 10.1 그룹 강도는 선행 게이트다

leader와 follower 모두 아래 그룹 게이트를 통과해야 한다.

- `industry_rs_pct >= 80`
- 또는 `industry_rank <= 상위 20%`
- `group_mansfield_rs > 0`
- `group_rs_slope_20d > 0`

### 10.2 시장 상태 게이트

강한 leader/follower 스크리너는 시장 환경의 영향을 크게 받으므로 최소한 아래는 반영한다.

- `benchmark_close > 50dma > 200dma`
- `200dma_slope > 0`
- `상위 20% 그룹 비중` 증가 중
- `52주 고점 종목 수 / 52주 저점 종목 수` 우호적

MVP에서는 hard gate보다 `position sizing multiplier`로 쓰는 것이 더 실용적이다.

## 11. Leader Screener

### 11.1 리더주 철학

리더주는 대체로 아래 순서를 보인다.

1. 강한 그룹 내부에서 먼저 튄다.
2. RS Percentile이 빠르게 상위권으로 진입한다.
3. RS Line이 가격보다 먼저 신고가를 만든다.
4. 이후 가격이 base를 만들거나 바로 재가속한다.

다만 실전 판정은 RS만으로 내리지 않고, 그룹 강도, 추세 무결성, 구조 품질, 거래량 확대, catalyst를 함께 본다.

### 11.2 하드 게이트

- `close >= 10`
- `ADV20 >= 유동성 기준`
- `close > 50dma > 150dma > 200dma`
- `slope(200dma, 20d) > 0`
- `close >= 0.75 * 52w_high`
- `close >= 1.30 * 52w_low`
- `rs_pct_252 >= 90`
- `industry_rs_pct >= 80`
- `mansfield_rs > 0`

### 11.3 강한 leader 추가 게이트

`Tier 1 leader`는 아래 중 2개 이상을 추가 충족해야 한다.

- `rs_pct_252 >= 95`
- `rs_line_250d_high_flag = true`
- `rs_new_high_before_price_flag = true`
- `price breakout on volume >= 1.5x ADV20`
- `최근 20일 distribution day <= 2`

### 11.4 리더 점수

```text
LeadershipScore =
    0.20 * GroupScore
  + 0.20 * TrendTemplateScore
  + 0.15 * RSScore
  + 0.10 * RSLineScore
  + 0.15 * BaseOrBreakoutScore
  + 0.10 * VolumeAccumulationScore
  + 0.10 * CatalystScore
```

RS 관련 항목 합산 비중은 25%로 두고, 나머지는 그룹/추세/구조/거래량/이벤트가 결정하게 설계한다.

세부 정의:

- `RSScore`: RS percentile, 3/6/12M 성과
- `RSLineScore`: RS 신고가, RS 선행 신고가, slope
- `GroupScore`: 산업/섹터 랭킹
- `TrendTemplateScore`: Minervini/Weinstein 계열 추세 품질
- `BaseOrBreakoutScore`: 수축, 타이트클로즈, pivot 근접성
- `VolumeAccumulationScore`: OBV/PVT 대신 단순 거래대금 확대와 up-volume dominance로도 충분
- `CatalystScore`: 실적, 가이던스, 섹터 뉴스, 규제/가격 변수

### 11.5 추천 라벨

- `Confirmed Leader`: score 상위 5-10%, RS Line 신고가형
- `Emerging Leader`: RS는 높고 구조는 좋은데 가격 돌파 전
- `Extended Leader`: 리더이나 엔트리 우위는 약화

## 12. Lagging Follower Screener

### 12.1 철학

후행 2등주는 아래를 찾는 시스템이어야 한다.

- `리더와 같은 물을 먹는데`
- `아직 가격 반영은 덜 되었고`
- `구조는 아직 멀쩡하며`
- `RS Line은 막 돌기 시작한 종목`

즉 `강한 업종의 늦은 반응주`를 찾는 것이지, `약해서 덜 오른 종목`을 찾는 것이 아니다.

여기서 RS/RS Line은 follower의 전환 초기 여부를 확인하는 요소이고, 후보군 생성 자체는 연결성, 그룹 강도, 구조 보존, 이벤트 확산이 먼저 결정한다.

### 12.2 하드 프리컨디션

후행 2등주 후보는 먼저 아래를 만족해야 한다.

- 같은 산업 또는 강한 테마 안에 `Confirmed Leader`가 최소 1개 존재
- `industry_rs_pct >= 80`
- `close > 50dma`
- `close >= 0.80 * 52w_high`
- `rs_pct_252`가 너무 낮지 않음
  - 권장 기본값: `70 <= rs_pct_252 < 90`
- 최근 20일 중 대형 악재/급락이 없음
- 거래대금과 유동성이 최소 기준 통과

### 12.3 "너무 약한 가짜 laggard" 제거 규칙

아래 중 하나라도 해당하면 follower 후보에서 제거한다.

- `close < 200dma`
- `50dma < 150dma < 200dma`처럼 하락 배열
- `drawdown from 52w high > 25%`
- `distribution day count 20d >= 4`
- `earnings gap down unresolved`
- 동종 leader 대비 구조 손상 명확

### 12.4 Catch-up Gap 정의

2등주의 핵심은 leader와의 성과 격차가 `너무 작지도, 너무 크지도 않아야` 한다는 점이다.

운영 정의:

```text
LeaderGap_20d = Return_leader_20d - Return_candidate_20d
LeaderGap_60d = Return_leader_60d - Return_candidate_60d
```

권장 범위:

- `LeaderGap_20d > 0`
- `LeaderGap_60d > 0`
- 단, gap이 너무 커서 구조가 망가진 종목은 제외

실무 초기값 예시:

- `0.05 <= LeaderGap_20d <= 0.25`
- `0.10 <= LeaderGap_60d <= 0.35`

### 12.5 Propagation Ratio 정의

leader의 최근 상승을 follower가 얼마나 덜 반영했는지 본다.

```text
PropagationRatio_20d = Return_candidate_20d / Return_leader_20d
```

권장 범위:

- `0.25 <= PropagationRatio_20d <= 0.80`

해석:

- 0.25 미만이면 너무 약하거나 깨졌을 수 있다.
- 0.80 초과면 이미 충분히 동행해서 "2등주" 의미가 약하다.

### 12.6 RS Inflection 규칙

후행 2등주는 절대 RS가 낮기만 해서는 안 된다. 핵심은 `개선 초기`다.

필수 조건:

- `rs_line_20d_slope > 0`
- `mansfield_rs_slope > 0`
- `rs_line_distance_to_high`가 축소 중

가산 조건:

- `rs_line_65d_high_flag = true`
- `rs_new_high_before_price_flag = true`
- `mansfield_rs`가 음수에서 양수로 전환

### 12.7 Pair / Peer Link Score

MVP에서는 연결성을 아래처럼 단순화한다.

```text
PeerLinkScore =
    0.50 * SameIndustry
  + 0.20 * SameSubIndustry
  + 0.15 * SameThemeBasket
  + 0.15 * EventCoMoveScore
```

확장 버전에서는 아래를 추가한다.

- shared analyst coverage
- 공급망 상류/하류 연결
- 사업 세그먼트 중첩
- 공통 ETF 편입과 flow 노출

### 12.8 이벤트 기반 follower 탐색

아래 leader 이벤트가 발생하면 follower 랭킹을 별도 재계산한다.

- earnings gap up
- guidance 상향
- 섹터 정책/규제 호재
- 원자재/상품 가격 급등
- 업계 1위 기업의 강한 breakout

이벤트 후 탐색 윈도:

- `t+1 ~ t+10` 거래일
- 필요시 `t+20`까지 확장

### 12.9 후행 2등주 점수

```text
LaggingFollowerScore =
    0.25 * PeerLinkScore
  + 0.20 * GroupStrengthScore
  + 0.20 * CatchUpPotentialScore
  + 0.15 * TrendIntegrityScore
  + 0.10 * CatalystSympathyScore
  + 0.10 * RSInflectionScore
```

해석:

- `PeerLinkScore`: 연결성
- `GroupStrengthScore`: 그룹 자체의 강도
- `CatchUpPotentialScore`: leader와의 성과 gap이 적절한지
- `RSInflectionScore`: follower의 상대강도 개선이 실제 시작됐는지
- `TrendIntegrityScore`: 망가진 차트가 아닌지
- `CatalystSympathyScore`: leader 이벤트가 같은 흐름으로 확산될 가능성

### 12.10 추천 라벨

- `High-Quality Follower`: leader와 연결되고 구조/RS 개선이 동시 확인
- `Early Sympathy Candidate`: 아직 가격은 눌려 있으나 RS 선행 개선 시작
- `Too Weak, Reject`: group은 강하지만 본인은 훼손

## 13. 선택형 고급 모듈: Rolling Lead-Lag Beta

논문 취지를 더 직접적으로 반영하려면 leader의 과거 수익률이 candidate의 미래 수익률을 얼마나 설명하는지 rolling하게 본다.

간단 구현 예시:

```text
PeerLeadScore(i, j) =
    0.40 * corr(r_i[t-1], r_j[t])
  + 0.30 * corr(r_i[t-2], r_j[t])
  + 0.20 * corr(r_i[t-3], r_j[t])
  + 0.10 * corr(r_i[t-5], r_j[t])
```

여기서:

- `i`: confirmed leader
- `j`: follower candidate

주의:

- 이 모듈은 pair 수가 많아지면 계산비용이 커진다.
- MVP에서는 `same-industry + event window + RS inflection`만으로도 충분하다.

## 14. 실전 트레이더 계보와의 연결

### 14.1 O'Neil / IBD 계열

- 높은 RS Rating
- RS Line의 선행 신고가
- 강한 산업 그룹 우선

즉 leader 스크리너의 본체에 가장 직접 연결된다.

### 14.2 Weinstein 계열

- 시장 대비 Relative Performance
- Mansfield RS
- Forest to the Trees, 즉 그룹 우선 접근

즉 `그룹 RS -> 종목 RS` 구조에 직접 연결된다.

### 14.3 Minervini 계열

- Stage 2 uptrend
- 52주 고점 근처
- 높은 RS ranking

즉 leader의 `trend integrity`와 follower의 `구조 보존` 필터에 직접 연결된다.

### 14.4 Qullamaggie 계열

- 1/3/6개월 강한 상승으로 현재 리더를 찾음
- 강한 move 후 orderly pullback, tightening, breakout
- sector EP와 sympathy move를 중요하게 봄

즉 leader 탐색과 event-driven follower 재정렬에 자연스럽게 연결된다.

## 15. 출력 스키마

### 15.1 Leader 출력

- ticker
- sector / industry
- rs_pct_252
- rs_line_20d_slope
- rs_new_high_before_price_flag
- mansfield_rs
- industry_rank
- breakout_state
- leadership_score
- label

### 15.2 Follower 출력

- ticker
- linked_leader_ticker
- connection_type
- industry_rank
- rs_pct_252
- rs_line_20d_slope
- leader_gap_20d
- leader_gap_60d
- propagation_ratio_20d
- lagging_follower_score
- label

## 16. 구현 전개 순서

### 16.1 MVP

1. broad benchmark 대비 RS Line 계산
2. 산업/섹터 RS 랭킹 계산
3. leader 하드 게이트와 점수 구현
4. 같은 산업 내 follower 후보군 생성
5. catch-up gap, propagation ratio, RS inflection 점수 구현

### 16.2 V2

1. sector benchmark와 industry benchmark 동시 사용
2. leader 이벤트 기반 follower 재랭킹
3. 주봉 Mansfield RS 추가
4. industry breadth 및 distribution count 개선

### 16.3 V3

1. shared analyst coverage 반영
2. 공급망/세그먼트 연결 반영
3. rolling lead-lag beta 및 event-study 자동화

## 17. 검증 계획

### 17.1 leader 검증

- leader 선정일 이후 `5d / 10d / 20d / 60d` 초과수익
- benchmark 대비 초과성과
- breakout 성공률
- RS 신고가 선행 신호의 hit rate

### 17.2 follower 검증

- linked leader 이벤트 이후 `5d / 10d / 20d` 성과
- 같은 그룹 평균 대비 초과성과
- leader 대비 beta-adjusted catch-up 성과
- rejected laggards 제거 전후 성과 차이

### 17.3 이벤트 스터디

- leader earnings gap up 당일을 `t0`로 놓고
- peer universe의 누적 비정상수익률을 측정
- industry, market cap, analyst coverage 수준별로 분해

## 18. 핵심 리스크와 함정

### 18.1 가장 큰 함정

- `lagging`을 `약한 주식`으로 오해하는 것

이 문서의 follower는 반드시 `강한 그룹 내부의 delayed winner candidate`여야 한다.

### 18.2 RS 해석 오류

- RSI와 RS를 혼동하면 안 된다.
- RS Percentile이 높아도 RS Line slope가 꺾이면 실전 우선순위가 떨어질 수 있다.

### 18.3 그룹 분류 오류

- 산업 분류가 너무 거칠면 엉뚱한 peer가 섞인다.
- 초기엔 sector보다 sub-industry가 더 중요할 수 있다.

### 18.4 이벤트 없는 follower 남발

- leader와의 연결성 없이 "덜 오른 종목"만 고르면 junk laggard가 많이 섞인다.

## 19. 명시적 비목표

- 저점 매수 시스템
- 리밸런싱형 팩터 포트폴리오 최적화
- pure statistical arbitrage
- 뉴스 sentiment only 시스템

## 20. 핵심 알고리즘 요약

### 20.1 Leader

```text
if GroupStrong
and TrendHealthy
and RS_Percentile >= 90
and MansfieldRS > 0
and (RS_Line_NewHigh or RS_NewHighBeforePrice):
    classify as leader candidate
rank by LeadershipScore
```

### 20.2 Lagging Follower

```text
if LinkedToConfirmedLeader
and GroupStrong
and TrendNotBroken
and 70 <= RS_Percentile < 90
and LeaderGap positive but not excessive
and PropagationRatio in healthy lagging zone
and RS_Line improving:
    classify as lagging follower candidate
rank by LaggingFollowerScore
```

## 21. 오픈소스 구현 참고

- `iArpanK/RS-Screener`
  - RS New High, RS New High Before Price, IBD 스타일 가중 RS Score 구현 참고
- `skyte/relative-strength`
  - IBD 스타일 RS percentile 산출과 산업/종목 출력 구조 참고
- `ktshen/screener`
  - RS 계산과 Minervini trend template 결합 방식 참고
- `starboi-63/growth-stock-screener`
  - RS, 유동성, 추세 템플릿, 성장 필터를 순차 스크리닝으로 엮는 구조 참고

## 22. 논문에서 설계로 연결되는 부분

- `Jegadeesh-Titman`
  - leader의 3-12개월 winner bias 정당화
- `Moskowitz-Grinblatt`
  - group RS를 선행 게이트로 두는 근거
- `Hou`
  - intra-industry lead-lag와 follower 설계의 핵심 근거
- `Brennan-Jegadeesh-Swaminathan`
  - analyst coverage가 높은 종목이 낮은 종목을 선행할 수 있다는 근거
- `Ali-Hirshleifer`
  - shared analyst coverage를 고급 연결 점수로 확장하는 근거
- `52주 신고가 계열 연구`
  - leader의 고점 근접성 조건 정당화
- `PEAD / limited attention`
  - leader 이벤트 이후 follower 재정렬 윈도의 근거

## 23. 최종 설계 원칙

1. 이 스크리너는 `멀티팩터 시스템`이고, RS/RS Line은 그 안의 핵심 feature이지 단독 엔진이 아니다.
2. `Leader`와 `Follower`를 같은 점수로 섞지 않는다.
3. leader는 `그룹 강도 + 추세 무결성 + 구조 품질 + 거래량/수급 + catalyst + RS`의 결합으로 본다.
4. follower는 `연결성 + 그룹 강도 + underreaction + 구조 보존 + 이벤트 확산 + RS inflection`의 결합으로 본다.
5. RS Percentile만 보지 말고 `RS Line의 상태 변화`를 별도 엔진으로 둔다.
6. 실전 우선순위는 언제나 `강한 그룹의 강한 leader`가 먼저고, follower는 그 다음이다.

## 24. 참고 자료

- Stage Analysis, "How to create the Mansfield Relative Strength Indicator"
  - https://www.stageanalysis.net/blog/4266/how-to-create-the-mansfield-relative-performance-indicator
- Stage Analysis, "New Features: Relative Strength Section Added to the Website"
  - https://www.stageanalysis.net/blog/1251720/new-features-relative-strength-section-added
- Qullamaggie, "3 TIMELESS setups that have made me TENS OF MILLIONS!"
  - https://qullamaggie.com/my-3-timeless-setups-that-have-made-me-tens-of-millions/
- Qullamaggie, "How to master a setup: Episodic Pivots"
  - https://qullamaggie.com/how-to-master-a-setup-episodic-pivots/
- Minervini.com / Stocks & Commodities review PDF
  - https://www.minervini.com/1MTPreview.pdf
- Minervini Markets 360
  - https://www.minervini.com/
- GitHub, `iArpanK/RS-Screener`
  - https://github.com/iArpanK/RS-Screener
- GitHub, `skyte/relative-strength`
  - https://github.com/skyte/relative-strength
- GitHub, `ktshen/screener`
  - https://github.com/ktshen/screener
- GitHub, `starboi-63/growth-stock-screener`
  - https://github.com/starboi-63/growth-stock-screener
- Jegadeesh & Titman, 1993, Journal of Finance
  - https://ideas.repec.org/a/bla/jfinan/v48y1993i1p65-91.html
- Moskowitz & Grinblatt, 1999, Journal of Finance
  - https://ideas.repec.org/a/bla/jfinan/v54y1999i4p1249-1290.html
- Brennan, Jegadeesh & Swaminathan, 1993, Review of Financial Studies
  - https://ideas.repec.org/a/oup/rfinst/v6y1993i4p799-824.html
- Hou, 2007, Review of Financial Studies
  - https://ideas.repec.org/a/oup/rfinst/v20y2007i4p1113-1138.html
- Ali & Hirshleifer, NBER Working Paper 25201
  - https://www.nber.org/papers/w25201
- Cen et al., 2013, Management Science
  - https://ideas.repec.org/a/inm/ormnsc/v59y2013i11p2566-2585.html
- Bernard & Thomas, 1989, Journal of Accounting Research
  - https://ideas.repec.org/a/bla/joares/v27y1989ip1-36.html
- DellaVigna & Pollet, NBER Working Paper 11683
  - https://www.nber.org/papers/w11683
- 52-week high related evidence
  - https://ideas.repec.org/a/eee/jimfin/v30y2011i1p180-204.html
  - https://ideas.repec.org/a/eee/finana/v57y2018icp167-183.html