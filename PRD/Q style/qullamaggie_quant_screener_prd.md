# Qullamaggie-Style Quant Screener PRD

작성일: 2026-03-11

## 1. 문서 목적

이 문서는 Qullamaggie 스타일을 최대한 오염 없이 정량 스크리너로 구현하기 위한 PRD 초안이다.
핵심 목표는 아래 4가지를 하나의 파이프라인으로 연결하는 것이다.

1. 이 기법이 잘 먹히는 시장 상황을 정량적으로 판별한다.
2. 전체 시장에서 Qullamaggie 스타일에 맞는 유니버스를 만든다.
3. Breakout / Episodic Pivot(EP) 중심으로 setup readiness를 점수화한다.
4. 최종적으로 `A++ 종목`과 `5-star setup`을 분리해서 산출한다.

본 문서는 "원문 요약"보다 "핵심 알고리즘과 전개"에 초점을 둔다. 따라서 아래 점수화와 게이트 규칙은 원문에 흩어진 질적 기준을 PRD용으로 정량화한 추론 설계다.

## 2. 소스 정책

### 2.1 허용 소스

- Qullamaggie 본인 사이트 `qullamaggie.com`
- `qullamaggie.net` 및 그 안에 게시된 Qullamaggie 관련 정리글
- `qullamaggie.net`에 게시된 follower workflow 글
- 이 기법 구현에 직접 연결되는 오픈소스 프로젝트
- 이 기법의 핵심 현상을 뒷받침하는 관련 논문

### 2.2 의도적으로 제외한 것

- 일반적인 TA 패턴 모음
- Qullamaggie와 직접 연결되지 않은 임의의 breakout/VCP 체계
- 유료/비공개 invite-only 스크립트
- 출처가 불분명한 재배포 PDF/노트

### 2.3 전략 순도 원칙

이 PRD는 Qullamaggie의 3개 setup 중 long-side에서 가장 재현 가능한 `Breakout`과 `EP`를 핵심으로 삼는다. `Parabolic Short`는 옵션 모듈로 다루되, MVP에서는 제외하는 것이 적절하다.

## 3. 전략 테제

Qullamaggie 자료를 종합하면 시스템의 중심축은 아래와 같다.

1. 시장이 momentum을 보상하는 구간인지 먼저 본다.
2. 그 다음 전체 시장에서 리더와 강한 테마를 좁힌다.
3. 그 안에서 tight한 구조와 명확한 catalyst가 있는 종목만 남긴다.
4. 실행은 대개 opening range break, stop은 low of day 또는 day structure, 수익 극대화는 10/20일선 추적에 둔다.

즉, 이 시스템의 본질은 "아무 breakout이나 찾는 스캐너"가 아니라 아래 순서다.

`시장 상태 -> 리더십/테마 -> 종목 품질 -> setup readiness -> intraday 실행 가능성`

## 4. 제품 범위

### 4.1 반드시 구현할 것

- 시장 레짐 점수와 권장 노출도
- Universe / Wide List / Focus List / Daily Focus List 파이프라인
- Breakout 스크리너
- EP 스크리너
- A++ 종목 점수
- 5-star setup 점수
- 장전/장중 alert 기준

### 4.2 나중에 붙일 것

- Parabolic Short 모듈
- 실제 포지션 sizing 엔진
- 자동 execution
- 종목별 deep-dive notebook 자동화

## 5. 소스에서 추출한 핵심 원칙

### 5.1 시장

- 시장 건강도와 rotation을 같이 봐야 한다.
- 좋은 시장에서는 setup 수가 늘어나고, focus list 품질도 올라간다.
- 장기 추세보다 "지금 momentum이 보상되는가"가 더 중요하다.
- 노출은 on/off가 아니라 dimmer switch처럼 단계적으로 조절한다.

### 5.2 Universe / Watch / Focus

- Universe는 대략 300-600개 수준이 적절하다.
- Wide List는 50-100개 정도의 고품질 아이디어로 압축한다.
- Weekly Focus List는 5-20개, Daily Focus List는 1-5개가 적절하다.
- Focus List를 만들 때는 `Quality`와 `Readiness`를 구분해야 한다.

### 5.3 Breakout

- 최근 1-3개월 내 강한 상승이 먼저 있어야 한다.
- 이후 2주-2개월의 orderly pullback / consolidation이 나와야 한다.
- 10/20일선을 타고 오르며, 변동성이 줄고 구조가 타이트해질수록 좋다.
- 실행은 opening range high 돌파가 중심이고, stop은 low of day 기준이 기본이다.

### 5.4 EP

- 핵심은 `예상 밖의 뉴스로 인한 재평가`다.
- 보통 10% 이상 gap, 강한 volume, 명확한 catalyst가 필요하다.
- 특히 earnings EP는 EPS/매출 성장, analyst beat, guidance 상향이 중요하다.
- 이미 3-6개월 크게 달린 종목보다 오랫동안 쉬거나 소외된 종목에서 더 잘 작동한다.

### 5.5 A++ / 5-star

- A++는 "지금 시장에서 가장 강한 top tier leadership" 개념에 가깝다.
- 5-star는 A++ 종목 중에서도 "지금 바로 실행 가능한 setup quality"까지 충족한 상태다.
- 즉 A++는 종목 품질, 5-star는 setup 품질이다.

## 6. 시스템 아키텍처

### 6.1 전체 파이프라인

1. `Market Regime Engine`
2. `Universe Builder`
3. `Wide List Builder`
4. `Focus List Builder`
5. `Setup Scoring Engine`
6. `Intraday Execution Layer`

### 6.2 일간/주간 동작

- 주간 배치:
  - broad universe 생성
  - leadership / sector rotation 분석
  - weekly wide list, focus list 생성
- 장전 배치:
  - premarket gap / earnings / news 스캔
  - daily focus list 재정렬
  - EP 우선순위 갱신
- 장중:
  - ORH trigger 감시
  - RVOL / DCR / % from open / VWAP 기준 재정렬
  - 이미 ADR을 과도하게 소모한 종목 제거

## 7. 데이터 요구사항

### 7.1 가격/수급

- Daily OHLCV
- 1m / 5m / 60m intraday bars
- Premarket / afterhours price and volume
- ADV20, ADV50
- ADR20, ATR14, ATR20
- VWAP
- DCR
- Volume Run Rate / Relative Volume

### 7.2 펀더멘털/이벤트

- EPS YoY growth
- Sales YoY growth
- EPS surprise
- Revenue surprise
- Guidance revision
- Analyst estimate revision
- Earnings calendar
- News / catalyst taxonomy
- Short interest
- Float / market cap
- Industry group / sector classification

### 7.3 시장 내부지표

- NDX / SPX / IWM / QQQ / SPY / IWM daily bars
- 지수별 10/20/50/200DMA
- 종목의 50DMA/200DMA 상회 비율
- Net new highs / net new lows
- Advance / decline
- Leading sectors count
- Weekly focus list density

## 8. 핵심 상태값 정의

### 8.1 레짐 상태

- `RISK_OFF`
- `RISK_NEUTRAL`
- `RISK_ON`
- `RISK_ON_AGGRESSIVE`

### 8.2 리스트 단계

- `UniverseList`
- `WideList`
- `FocusList`
- `DailyFocusList`

### 8.3 종목/셋업 레이블

- 종목 등급: `A++`, `A+`, `B`, `Discard`
- setup 등급: `5-star`, `4-star`, `3-star`, `Monitor`

## 9. Market Regime Engine

Qullamaggie식 스크리너는 시장 상황 판별이 제일 먼저 와야 한다. 여기서는 follower 자료의 노출 규칙과 Qullamaggie의 situational awareness 개념을 정량 엔진으로 합친다.

### 9.1 입력 피처

- 주요 지수 가격이 50DMA 위에 있는지
- 10DMA > 20DMA 인지
- 20DMA slope가 상승 중인지
- 50DMA 위 종목 비율
- 200DMA 위 종목 비율
- Net New Highs - Net New Lows
- 최근 5거래일 신규 breakout 성공률
- leading groups 개수
- weekly focus list의 고품질 setup 개수

### 9.2 지수 추세 점수

각 지수마다 아래 4개 조건을 점수화한다.

- close > 50DMA
- 10DMA > 20DMA
- 20DMA slope > 0
- close >= 20DMA

지수 점수:

```text
index_trend_score = mean([
  close_gt_50dma,
  sma10_gt_sma20,
  slope20_positive,
  close_gt_20dma
]) * 100
```

사용 지수:

- QQQ
- SPY
- IWM

최종 지수 점수:

```text
market_trend_score = average(QQQ, SPY, IWM index_trend_score)
```

### 9.3 breadth 점수

```text
breadth_score =
  0.35 * pct_above_50dma +
  0.25 * pct_above_200dma +
  0.20 * scaled_net_new_highs +
  0.20 * scaled_adv_decline
```

모든 입력은 0-100으로 정규화한다.

### 9.4 opportunity density 점수

Qullamaggie 계열 자료에서 반복적으로 나오는 아이디어는 "좋은 시장은 setup이 많아진다"는 것이다. 이를 직접 엔진에 넣는다.

```text
opportunity_density_score =
  0.50 * pct_successful_breakouts_5d +
  0.25 * count_a_plus_candidates_norm +
  0.25 * count_leading_groups_norm
```

### 9.5 최종 레짐 점수

```text
regime_score =
  0.45 * market_trend_score +
  0.35 * breadth_score +
  0.20 * opportunity_density_score
```

### 9.6 레짐 분류와 권장 노출도

| Regime | 조건 | 권장 총 노출 |
|---|---|---:|
| `RISK_OFF` | `regime_score < 35` | 0-10% |
| `RISK_NEUTRAL` | `35 <= regime_score < 55` | 10-40% |
| `RISK_ON` | `55 <= regime_score < 75` | 40-75% |
| `RISK_ON_AGGRESSIVE` | `regime_score >= 75` | 75-100% |

### 9.7 운영 규칙

- `RISK_OFF`에서는 breakout 신규 진입을 거의 금지한다.
- `RISK_NEUTRAL`에서는 EP와 top leadership만 허용한다.
- `RISK_ON` 이상에서만 5-star breakout을 적극 허용한다.
- focus list 개수와 breakout 성공률이 줄면 regime 하향 조정 신호로 본다.

## 10. Universe Builder

Universe는 하나가 아니라 `Breakout Universe`와 `EP Universe`를 병렬로 만든다. 이유는 EP는 종종 "소외 -> 재평가" 형태라서 순수 추세 필터만으로 놓치기 쉽기 때문이다.

### 10.1 Breakout Universe 규칙

필수 조건:

- price >= 10
- avg_dollar_volume_20 >= 20M USD
- avg_volume_20 >= 300k
- close > 50DMA
- 20DMA > 50DMA
- 1M, 3M, 6M RS percentile이 모두 상위 구간

권장 조건:

- close > 200DMA
- industry group strength 상위 구간
- 최근 3개월 신고가 또는 52주 고점 근접

타깃 개수:

- 300-600개

### 10.2 EP Universe 규칙

필수 조건:

- price >= 5
- event가 존재
- 장전 gap >= 5% 또는 강한 뉴스 후 거래대금 급증
- 장전 또는 개장 직후 거래 가능 유동성 확보

권장 조건:

- avg_dollar_volume_20 >= 10M USD
- catalyst가 earnings / guidance / contract / FDA / regulation / sector revaluation 중 하나
- 최근 3-6개월 과도한 선행 상승이 없음

타깃 개수:

- 평시 매우 적음
- earnings season에는 집중적으로 증가

## 11. Wide List / Focus List Builder

### 11.1 Wide List 구성 규칙

Wide List는 아래 3개 그룹으로 구성한다.

1. `Leadership`
2. `Strong Movers`
3. `Actionable Ideas`

#### Leadership

- 현재 시장의 top tier names
- leading group에 속함
- 아직 setup이 완성되지 않았더라도 유지

#### Strong Movers

- 강한 breakout
- 강한 earnings gap
- power trend

#### Actionable Ideas

- 며칠 내 실제 진입 가능성이 있는 구조
- pivot과 stop이 정의되는 종목

타깃 개수:

- 50-100개

### 11.2 Focus List 구성 규칙

Focus List는 아래 2개 차원을 동시에 본다.

- `Quality`
- `Readiness`

Quality 질문:

- leader인가
- 기관이 실을 만한 유동성인가
- RS가 강한가
- trend가 tight한가
- EPS / sales / theme / short interest가 유의미한가
- leading group인가

Readiness 질문:

- 며칠 내 entry tactic이 가능한가
- pivot 근처인가
- 최근 액션이 tight한가
- catalyst가 임박했는가
- 손절이 ADR/ATR 안쪽에서 정의되는가

타깃 개수:

- weekly: 5-20개
- daily: 1-5개

## 12. A++ 종목 점수

A++는 "setup 전" 종목 품질 등급이다.

### 12.1 A++ 점수 구성

```text
a_pp_score =
  0.30 * leadership_score +
  0.20 * trend_quality_score +
  0.20 * fundamental_or_catalyst_score +
  0.15 * liquidity_score +
  0.15 * group_theme_score
```

### 12.2 하위 점수 정의

#### leadership_score

- 1M RS percentile
- 3M RS percentile
- 6M RS percentile
- 52주 고점 근접도

#### trend_quality_score

- 10DMA > 20DMA > 50DMA 구조
- higher lows 지속 여부
- 최근 20일 range compression
- high DCR 빈도

#### fundamental_or_catalyst_score

- EPS growth
- Sales growth
- analyst beat / estimate raise
- 명확한 catalyst 존재 여부

#### liquidity_score

- ADV
- average dollar volume
- spread proxy

#### group_theme_score

- leading sector / industry 소속
- group 내부 동반 breakout 수
- 동일 테마 종목 동조도

### 12.3 A++ 판정

| 등급 | 조건 |
|---|---|
| `A++` | `a_pp_score >= 85` and no fatal flaw |
| `A+` | `75 <= a_pp_score < 85` |
| `B` | `60 <= a_pp_score < 75` |
| `Discard` | `< 60` |

Fatal flaw 예시:

- 유동성 부족
- stop이 너무 넓음
- 이미 과도하게 확장
- sector/group가 꺾임

## 13. Breakout Setup Engine

### 13.1 철학

Qullamaggie breakout은 "강한 prior run 이후의 타이트한 재출발"이다. 따라서 탐지 알고리즘은 `prior expansion -> compression -> range expansion` 순서를 강제해야 한다.

### 13.2 Breakout 게이트 규칙

#### G1. Prior run

- 최근 20-60거래일 내 저점 대비 현재가 또는 base high가 30% 이상 상승
- 이상적 구간은 30-100%+

```text
prior_run = (base_high / lowest_close_last_60d) - 1
pass if prior_run >= 0.30
```

#### G2. Base length

- base length는 10-40거래일
- 너무 짧으면 noise, 너무 길면 다른 전략에 가까워짐

#### G3. Orderly pullback / tightness

- 최근 base 구간의 ATR10 / ATR60 감소
- higher low 구조
- 10DMA, 20DMA 주변에서 수렴

정량 예시:

```text
compression_score =
  0.40 * (1 - atr10_over_atr60) +
  0.30 * higher_lows_score +
  0.30 * close_to_ma_surf_score
```

#### G4. MA surfing

- base 구간에서 close가 10DMA 위에 머무는 비율이 높아야 함
- 10DMA > 20DMA가 유지될수록 가점

#### G5. 52주 고점 근접

- breakout 후보는 신고가 근처일수록 우선

```text
high_proximity = close / rolling_252d_high
pass if high_proximity >= 0.85
```

#### G6. Actionability

- entry trigger에서 stop까지 거리가 ADR/ATR 이내

```text
risk_unit = entry_price - stop_price
pass if risk_unit <= ADR20
```

### 13.3 장중 실행 조건

Qullamaggie 원문 기준 execution은 ORH 중심이다.

허용 trigger:

- 1분 opening range high
- 5분 opening range high
- 60분 opening range high
- 일봉 pivot break

장중 확인 항목:

- RVOL / volume run rate가 높을수록 우선
- 이미 당일 ADR 대부분을 소모했으면 제외
- low of day stop이 비정상적으로 멀면 제외

### 13.4 Breakout 점수

```text
breakout_setup_score =
  0.30 * a_pp_score +
  0.25 * compression_score +
  0.20 * readiness_score +
  0.15 * intraday_confirmation_score +
  0.10 * regime_alignment_score
```

#### intraday_confirmation_score

- ORH break 발생 여부
- 장중 RVOL
- DCR
- VWAP 상방 유지
- % from open

### 13.5 Breakout 5-star 판정

`5-star breakout` 조건:

- `A++`
- `breakout_setup_score >= 90`
- `regime in {RISK_ON, RISK_ON_AGGRESSIVE}`
- `risk_unit <= ADR20`
- 당일 과확장 아님

`4-star breakout` 조건:

- `breakout_setup_score >= 80`
- 또는 A+인데 구조가 매우 타이트함

## 14. Episodic Pivot Engine

### 14.1 철학

EP의 본질은 `뉴스가 예상치를 크게 벗어나며 시장이 종목을 재평가하는 순간`이다. 따라서 EP 엔진은 기술적 패턴보다 `event intensity`와 `volume shock`를 더 크게 본다.

### 14.2 EP 게이트 규칙

#### G1. Gap

원문 기준의 엄격한 EP:

```text
gap_pct = open / prev_close - 1
pass if gap_pct >= 0.10
```

운영상 watch tier:

- `Core EP`: gap >= 10%
- `EP Watch`: 5% <= gap < 10% and event intensity very high

#### G2. Volume shock

이상적인 조건:

- 장전 거래량이 이미 비정상적으로 큼
- 또는 개장 후 15-30분 내 평균 일 거래량에 근접

정량 예시:

```text
volume_shock_score =
  max(
    premarket_volume / adv20,
    first_15m_volume / adv20,
    first_30m_volume / adv20
  )
```

강한 EP 기준:

- `first_15m_volume / adv20 >= 0.50`
- 또는 `first_30m_volume / adv20 >= 1.00`

#### G3. Catalyst taxonomy

허용 catalyst:

- earnings beat
- earnings + guidance raise
- contract / order
- FDA / biotech result
- regulation / political revaluation
- sector EP

#### G4. Earnings quality

earnings EP는 아래를 우선한다.

- EPS YoY growth 중상 이상
- Sales YoY growth 중상 이상
- analyst beat
- guidance raise

정량 예시:

```text
earnings_quality_score =
  0.30 * eps_growth_score +
  0.30 * sales_growth_score +
  0.20 * surprise_score +
  0.20 * guidance_score
```

#### G5. Neglect / sideways prior condition

원문상 best EP는 이미 크게 달리지 않은 종목이다.

정량 예시:

- 최근 60거래일 상승률이 과도하지 않음
- 최근 3-6개월 range width가 제한적
- 3-6개월 내 대형 EP 2회 이상 반복이면 감점

```text
neglect_score =
  0.50 * low_prior_extension +
  0.50 * sideways_base_score
```

### 14.3 EP 실행 조건

허용 entry:

- 1분 ORH
- 5분 ORH
- 60분 ORH

기본 stop:

- low of day

추가 제약:

- stop distance <= 1.5 * ADR20
- 너무 illiquid하면 제외

### 14.4 EP 점수

```text
ep_setup_score =
  0.30 * volume_shock_score +
  0.25 * event_intensity_score +
  0.20 * earnings_quality_score +
  0.15 * neglect_score +
  0.10 * regime_alignment_score
```

### 14.5 EP 5-star 판정

`5-star EP` 조건:

- `gap >= 10%`
- `ep_setup_score >= 90`
- `volume_shock_score`가 최고 구간
- catalyst가 명확
- prior extension 과도하지 않음

참고:

- EP는 breakout보다 시장 레짐 민감도가 약간 낮다.
- 다만 `RISK_OFF`에서는 follow-through 실패율이 커지므로 size 축소가 필요하다.

## 15. Optional: Parabolic Short Module

이 모듈은 Qullamaggie의 3대 setup 중 하나지만, 본 PRD의 핵심 목표인 long-side end-to-end screener와는 우선순위가 다르다.

후속 모듈 규칙 초안:

- 5-20거래일 내 비정상 급등
- 3-5일 연속 상승
- VWAP fail / ORL 기반 진입
- 10/20DMA까지 mean reversion 목표

MVP에서는 제외하는 것이 맞다.

## 16. 5-star 최종 랭킹 엔진

최종 daily focus 정렬은 아래 4개 축으로 구성한다.

```text
final_trade_priority =
  0.35 * setup_score +
  0.30 * a_pp_score +
  0.20 * regime_score +
  0.15 * execution_quality_score
```

### 16.1 execution_quality_score

- stop distance / ADR
- opening range clarity
- spread / liquidity
- intraday RVOL
- VWAP location
- already extended 여부

### 16.2 최종 우선순위 해석

| 우선순위 | 의미 |
|---|---|
| `Tier 1` | 오늘 바로 실행 가능한 5-star |
| `Tier 2` | 매우 좋지만 하루 이틀 더 필요 |
| `Tier 3` | 구조는 좋으나 시장/유동성/확장도 문제 |

## 17. 스크리너 출력물 정의

### 17.1 주간 출력

- 시장 레짐 리포트
- sector rotation 리포트
- UniverseList
- WideList
- Weekly FocusList

### 17.2 장전 출력

- gap / news / earnings 스캔
- EP candidates
- 당일 Daily FocusList
- ORH alert 레벨

### 17.3 장중 출력

- high RVOL movers
- high DCR leaders
- strong from open names
- Above VWAP leaders
- 이미 ADR 초과한 종목 제거 리스트

## 18. 구현 우선순위

### Phase 1: MVP

- Market Regime Engine
- Breakout Universe
- EP Universe
- A++ score
- Breakout / EP 5-star score
- Daily FocusList 생성

### Phase 2

- intraday resorting
- ORH alerting
- sector rotation dashboard
- setup success tracker

### Phase 3

- exposure engine
- portfolio heat / progressive exposure
- execution integration

## 19. 기술 구현 메모

### 19.1 추천 테이블 구조

- `daily_bars`
- `intraday_bars_1m`
- `intraday_bars_5m`
- `intraday_bars_60m`
- `earnings_events`
- `news_events`
- `analyst_estimates`
- `fundamental_quarters`
- `market_breadth_daily`
- `symbol_scores_daily`
- `watchlists_daily`

### 19.2 배치 스케줄

- 주말: full rebuild
- 매일 장후: daily rebuild
- 장전: EP / gap refresh
- 개장 후 5분 / 15분 / 30분 / 60분: execution refresh

## 20. 논문과의 연결

아래 논문들은 Qullamaggie 방법론 전체를 직접 검증한 것은 아니지만, 핵심 메커니즘을 지지한다.

### 20.1 Momentum / leadership

- Jegadeesh and Titman (1993)
  - 3-12개월 winner가 이후에도 outperform하는 경향을 보여준다.
  - Breakout universe를 1M/3M/6M 상대강도 중심으로 만드는 근거가 된다.

- George and Hwang (2004)
  - 52주 고점 근접성이 momentum forecasting에 강한 설명력을 가진다고 본다.
  - 신고가 근처 leadership과 breakout 후보 우선순위에 직접 연결된다.

### 20.2 Earnings surprise / EP

- Bernard and Thomas (1989)
  - PEAD를 보여주며, earnings surprise 이후 즉시 정보가 완전히 반영되지 않는다는 점을 시사한다.
  - EP의 "뉴스 후 수개월 추세" 논리와 연결된다.

- Livnat and Jegadeesh (2006)
  - revenue surprise가 earnings surprise와 같은 방향일 때 drift가 더 강하다고 본다.
  - EP 점수에서 EPS만 아니라 sales surprise를 크게 반영해야 하는 근거다.

- DellaVigna and Pollet (2009)
  - 투자자 주의력 한계가 announcement 반응 지연에 영향을 준다는 근거를 제공한다.
  - surprise + volume + follow-through 관찰 로직과 맞닿아 있다.

## 21. 오픈소스 구현 참고

아래는 "정답"이 아니라 구현 참고용이다.

### 21.1 직접 연관 스크립트

- TradingView `Qullamaggie Breakout`
  - 작은 timeframe breakout + 상위 timeframe 10/20MA trailing이라는 구조를 보여준다.

- TradingView `Episodic Pivot`
  - gap, volume, liquidity 기반의 단순 EP 필터 구현 예시다.

- TradingView `SMA 10/20 Trend Info Table - Qullamaggie`
  - 지수/종목의 10/20 추세 필터를 단순화한 참고 구현이다.

### 21.2 인프라성 오픈소스

- `tradingview-screener`
  - RS, MA, fundamental 필드를 조합한 universe/wide list 쿼리 구현에 적합하다.

- `small-caps-scanner`
  - premarket / regular market movers, 뉴스, 기본 펀더멘털 ingestion 파이프라인 참고용이다.

## 22. 명시적 비목표

- mean reversion 전략 추가
- multi-factor long-term investing 모델로 확장
- 일반적인 candlestick 라이브러리 중심 설계
- black-box AI score로 핵심 규칙 대체

## 23. 최종 제안

가장 Qullamaggie스럽고 spoil이 적은 설계는 아래다.

1. `Breakout + EP` 두 엔진만 우선 만든다.
2. 모든 스캔 결과 위에 `Market Regime Engine`을 먼저 올린다.
3. 종목 품질과 setup 품질을 분리해 `A++`와 `5-star`를 따로 낸다.
4. list pipeline을 `Universe -> Wide -> Focus -> Daily Focus`로 강제한다.
5. 장중 실행은 ORH, RVOL, ADR 소모량, VWAP 유지 여부만 최소한으로 본다.

이렇게 하면 Qullamaggie 자료의 구조를 유지하면서도 실제 정량 시스템으로 구현 가능한 형태가 된다.

## 24. 참고 링크

### 24.1 Qullamaggie / direct

- [Qullamaggie FAQ](https://qullamaggie.com/faq/)
- [Qullamaggie Home](https://qullamaggie.com/)
- [Qullamaggie Resources Home](https://qullamaggie.net/)
- [3 TIMELESS setups that have made me TENS OF MILLIONS!](https://qullamaggie.net/3-timeless-setups-that-have-made-me-tens-of-millions/)
- [How to master a setup: Episodic Pivots](https://qullamaggie.net/how-to-master-a-setup-episodic-pivots/)
- [Catalysts that create explosive moves](https://qullamaggie.net/catalysts-that-create-explosive-moves/)
- [What drives momentum](https://qullamaggie.net/what-drives-momentum/)
- [How these momentum stocks perform in a short-term market pullback](https://qullamaggie.net/how-these-momentum-stocks-perform-in-a-short-term-market-pullback/)

### 24.2 Workflow / follower materials hosted on qullamaggie.net

- [Creating a Weekly Routine](https://qullamaggie.net/creating-a-weekly-routine/)
- [The Daily Routine Template](https://qullamaggie.net/the-daily-routine-template/)
- [How to Consistently find Top Trade Ideas – Creating a Focus List](https://qullamaggie.net/how-to-consistently-find-top-trade-ideas-creating-a-focus-list/)
- [Situational Awareness post](https://qullamaggie.net/situational-awareness-post/)
- [Alex Desjardins’s method](https://qullamaggie.net/alex-desjardinss-method/)
- [Jeff Sun’s method and flow](https://qullamaggie.net/jeff-suns-method-and-flow/)
- [PaulStifler‘s Method](https://qullamaggie.net/paulstiflers-method/)

### 24.3 오픈소스

- [TradingView Qullamaggie Breakout](https://www.tradingview.com/script/cDCAPrd1-Qullamaggie-Breakout/)
- [TradingView Episodic Pivot](https://www.tradingview.com/script/PZghP0Uq-Episodic-Pivot/)
- [TradingView SMA 10/20 Trend Info Table - Qullamaggie](https://www.tradingview.com/script/vArzZItD/)
- [tradingview-screener](https://github.com/shner-elmo/TradingView-Screener)
- [small-caps-scanner](https://github.com/gianpierreba/small-caps-scanner)

### 24.4 논문

- [Jegadeesh and Titman (1993) - Returns to Buying Winners and Selling Losers](https://ideas.repec.org/a/bla/jfinan/v48y1993i1p65-91.html)
- [George and Hwang (2004) - The 52-Week High and Momentum Investing](https://repository.hkust.edu.hk/ir/Record/1783.1-27926)
- [Bernard and Thomas (1989) - Post-Earnings-Announcement Drift](https://www.jstor.org/stable/2491062)
- [Livnat and Jegadeesh (2006) - The Role of Revenue Surprises](https://www.tandfonline.com/doi/abs/10.2469/faj.v62.n2.4081)
- [DellaVigna and Pollet (2005/2009) - Investor Inattention and Friday Earnings Announcements](https://www.nber.org/papers/w11683)