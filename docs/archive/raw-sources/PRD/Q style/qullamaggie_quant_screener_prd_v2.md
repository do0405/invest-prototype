# Qullamaggie-Style Multi-Market Quant Screener PRD v2

작성일: 2026-03-12

이 문서는 기존 [qullamaggie_quant_screener_prd.md] 내용을 바탕으로, `Q style.md`의 연구 정리와 운영 규칙을 흡수해 실제 제품 명세 수준으로 확장한 PRD다.
핵심 방향은 `Qullamaggie 스타일 보존`, `OHLCV-first`, `KR/US 멀티마켓`, `데이터 공급자 비종속`, `설명 가능한 스코어링`, `이벤트 드리븐 백테스트 가능성`이다.

## 1. 제품 목표

### 1.1 문제 정의

우리가 만들고 싶은 것은 “브레이크아웃 몇 개를 보여주는 단순 스캐너”가 아니다. 목표는 아래 전체 흐름을 하나의 시스템으로 묶는 것이다.

1. 시장이 Qullamaggie식 momentum을 보상하는지 판단한다.
2. KR/US 전체 종목에서 리더십과 액셔너블 setup을 좁힌다.
3. Breakout / Episodic Pivot(EP)를 정량 규칙으로 검출한다.
4. 종목 품질(`A++`)과 setup 품질(`5-star`)을 분리 산출한다.
5. 장전/장중 ORH 기반 실행 가능 상태까지 연결한다.
6. 나중에 백테스트와 리뷰 시스템으로 이어질 수 있게 모든 판단을 기록한다.

### 1.2 최종 산출물

시스템은 아래 산출물을 생성해야 한다.

- 시장별 `Market Regime`
- `UniverseList`
- `WideList`
- `Weekly FocusList`
- `Daily FocusList`
- `Breakout Candidates`
- `EP Candidates`
- `A++ Stock Scores`
- `5-star Setup Scores`
- `ORH Trigger Alerts`
- `Signal Snapshot Logs`

## 2. 제품 원칙

### 2.1 원칙 1: OHLCV-first

### 2.1.1 가격 시계열 정책

- 기본 기술 시계열은 `split-adjusted OHLC`다.
- dividend를 포함한 `Adj Close` back-adjusted 시계열은 기본값이 아니라 별도 정책으로만 사용한다.
- split 근거가 없는 캐시는 `raw`로 계산하되, source metadata를 남겨 false precision을 피한다.
- `EMA`와 `SMA`는 공용 helper를 쓰더라도 서로 다른 지표로 유지한다.

가장 중요한 입력은 KR/US 종목의 OHLCV다. 시스템은 최소한 일봉 OHLCV만으로도 다음을 생성할 수 있어야 한다.

- 시장 레짐 기본 판정
- 리더십 랭킹
- breakout universe
- weekly focus list
- A++의 가격/거래량 기반 버전

즉, `OHLCV만 있어도 돌아가야 한다`. 재무/이벤트/뉴스는 정확도를 높이는 enrichment다.

### 2.2 원칙 2: enrich-later

재무데이터, 실적 캘린더, 가이던스, analyst revision, short interest, float, 뉴스, sector taxonomy는 붙일 수 있으면 붙인다. 없으면 시스템이 멈추면 안 된다.

규칙:

- enrichment 부재는 `unknown`으로 처리한다.
- `unknown`을 `negative`로 취급하지 않는다.
- 대신 `data_confidence_score`를 낮춘다.
- EP는 enrichment가 있을수록 정밀해지고, breakout은 OHLCV만으로도 충분히 동작해야 한다.

### 2.3 원칙 3: hard rules와 soft rules를 분리한다

Qullamaggie 자료에서 상대적으로 강한 하드 룰은 별도 계층으로 고정한다.

하드 룰 예시:

- breakout 진입은 ORH 중심
- 손절은 LOD 중심
- breakout 손절폭은 ADR/ATR 이내
- EP는 gap >= 10%가 core 정의
- EP 손절폭은 대략 1.0~1.5x ADR/ATR 이내
- 트레이드당 계좌 리스크는 대체로 0.25~1%
- 일반 포지션 크기는 대체로 10~20%
- 1종목 오버나잇 익스포저는 30% 초과 금지

반면 아래는 configurable한 soft rules로 둔다.

- 레짐 점수 가중치
- 시장 breadth 임계치
- 유동성 하한
- A++ 가중치
- 5-star 컷오프
- KR/US별 세부 percentile 컷

### 2.4 원칙 4: 멀티마켓이지만 시장별 레짐은 분리한다

US 장세가 좋아도 KR 장세가 나쁠 수 있고, 반대도 가능하다. 따라서 시스템은 `시장별 regime`를 별도로 계산해야 한다.

- US는 US benchmark set으로 평가
- KR은 KR benchmark set으로 평가
- cross-market ranking은 가능하지만, gating은 기본적으로 market-local하게 처리

### 2.5 원칙 5: 스코어는 설명 가능해야 한다

각 후보는 반드시 아래를 함께 출력해야 한다.

- 왜 포함되었는지
- 어떤 게이트를 통과했는지
- 어떤 데이터가 없었는지
- A++가 왜 높은지
- 5-star가 왜 높은지
- 어느 규칙 때문에 탈락했는지

## 3. 범위

### 3.1 In Scope

- KR/US 주식 및 ETF
- 일봉/분봉 OHLCV 기반 스캐닝
- Breakout setup
- Episodic Pivot setup
- 시장 레짐 산출
- A++ / 5-star 점수화
- watchlist/focus list 파이프라인
- 장전/장중 alert
- signal snapshot 저장
- 이벤트 드리븐 백테스트를 위한 signal replay 데이터 저장

### 3.2 Out of Scope for MVP

- 완전 자동매매
- 포트폴리오 최적화
- Parabolic Short 본격 모듈
- 옵션/선물/코인 지원
- 비정형 뉴스 요약 AI를 핵심 룰로 사용하는 것

## 4. 사용자와 Job To Be Done

### 4.1 Primary User

- Qullamaggie 스타일을 정량 시스템으로 구현하려는 연구자/트레이더
- KR/US 양 시장을 동시에 스캔하려는 운영자

### 4.2 JTBD

- “오늘 어느 시장에서 momentum이 보상되는지 알고 싶다.”
- “US/KR에서 지금 strongest leaders를 보고 싶다.”
- “브레이크아웃 후보와 EP 후보를 분리해서 보고 싶다.”
- “A++ 종목과 5-star setup을 구분해 보고 싶다.”
- “OHLCV만으로도 먼저 돌리고, 나중에 재무/이벤트 데이터를 붙이고 싶다.”
- “나중에 백테스트로 검증할 수 있게 signal trace를 남기고 싶다.”

## 5. 성공 기준

### 5.1 제품 성공 기준

- 일봉 OHLCV만으로 daily regime, leader rank, breakout universe를 생성할 수 있다.
- 분봉이 들어오면 ORH/LOD 기반 intraday trigger를 생성할 수 있다.
- event/fundamental data가 들어오면 EP precision이 올라간다.
- 각 후보는 `reason_codes`, `data_flags`, `score_breakdown`을 반드시 가진다.
- KR/US 모두 동일 엔진으로 돌되 market profile만 바꿔 동작한다.

### 5.2 연구 성공 기준

- 과거 snapshot replay가 가능하다.
- 후보 생성 시점의 피처와 스코어를 재현할 수 있다.
- 이벤트 드리븐 백테스트로 ORH/LOD/ADR 규칙 검증이 가능하다.

## 6. 데이터 전략

## 6.1 최소 입력

MVP에서 반드시 필요한 입력은 아래뿐이다.

- 종목 마스터
- 거래소/시장 정보
- 일봉 OHLCV

이 세 가지로 기본 universe, market regime, leader ranking, breakout scan은 동작해야 한다.

## 6.2 권장 입력

정확도를 위해 권장하는 입력:

- 1m / 5m / 60m intraday OHLCV
- corporate actions
- sector / industry mapping
- benchmark index OHLCV

## 6.3 선택적 enrichment

선택적으로 붙일 수 있는 입력:

- quarterly fundamentals
- earnings calendar
- EPS / revenue surprise
- guidance data
- analyst revision
- news / catalyst tagging
- float / short interest / market cap
- premarket / afterhours prints
- KR DART / US SEC filing derived features

## 6.4 시스템 요구사항

- enrichment는 비동기적으로 backfill 가능해야 한다.
- 데이터 공급원은 adapter pattern으로 분리해야 한다.
- 특정 벤더 API를 PRD 수준에서 하드코딩하지 않는다.
- 동일 종목에 대해 raw series와 adjusted series를 둘 다 보관할 수 있어야 한다.

## 7. 멀티마켓 추상화

## 7.1 Market Profile

시스템은 각 시장마다 `market_profile`을 가져야 한다.

필수 필드:

- `market_code` (`US`, `KR`)
- `timezone`
- `currency`
- `session_open`
- `session_close`
- `supports_premarket`
- `supports_afterhours`
- `benchmark_set`
- `default_sector_taxonomy`
- `orh_windows`

예시:

- US: premarket 지원, ORH 1m/5m/60m
- KR: regular open 기준 ORH, premarket 없음 또는 제한적 취급, 장초반 5m/15m/30m volume shock 중요

## 7.2 Currency / Liquidity 처리 원칙

절대 가격이나 절대 거래대금만으로 US와 KR을 한 테이블에서 비교하면 왜곡된다. 따라서 기본 원칙은 아래와 같다.

- 시장 내부 순위는 local-currency 기반 percentile로 계산
- cross-market 비교는 return / volatility / volume percentile 위주로 계산
- 필요 시 FX로 환산한 공통 liquidity proxy를 추가

## 7.3 KR 특수 처리

KR 시장은 US처럼 premarket gap/volume을 넓게 활용하기 어렵다. 따라서 EP 감지는 아래처럼 정의한다.

- 공식 시초가 기준 gap
- 첫 5분 / 15분 / 30분 거래량 런레이트
- 장초반 range expansion
- DCR / VWAP / opening drive 유지

즉 KR의 EP는 `premarket-aware EP`가 아니라 `open-drive EP`에 가깝게 구현한다.

## 8. 도메인 모델

핵심 테이블/컬렉션은 아래를 권장한다.

- `symbols`
- `market_profiles`
- `daily_bars`
- `intraday_bars_1m`
- `intraday_bars_5m`
- `intraday_bars_60m`
- `corporate_actions`
- `fundamentals_quarterly`
- `event_facts`
- `analyst_estimates`
- `news_events`
- `sector_membership`
- `market_breadth_snapshots`
- `candidate_snapshots`
- `signal_events`
- `review_tags`

## 8.1 candidate_snapshots 필수 필드

- `as_of_ts`
- `symbol`
- `market_code`
- `setup_family`
- `candidate_stage`
- `stock_grade`
- `setup_grade`
- `a_pp_score`
- `setup_score`
- `final_priority_score`
- `regime_state`
- `regime_score`
- `reason_codes`
- `fail_codes`
- `data_flags`
- `data_confidence_score`
- `pivot_price`
- `stop_price`
- `risk_unit_pct`
- `entry_timeframe`

## 9. 제품 기능 요구사항

## 9.1 Data Ingestion Engine

시스템은 아래를 지원해야 한다.

- KR/US 심볼 로딩
- 일봉 OHLCV 적재
- 분봉 OHLCV 적재
- benchmark index 적재
- 데이터 소스별 freshness 추적
- 누락/이상치 감지

필수 동작:

- 동일 날짜 재실행 시 deterministic rebuild 가능
- split/dividend 적용 여부를 명시적으로 관리
- symbol rename / delisting 추적 가능

## 9.2 Feature Store

필수 피처:

- `ret_1m`, `ret_3m`, `ret_6m`, `ret_12m`
- `adr20`, `atr14`, `atr20`, `natr20`
- `high_52w_proximity`
- `sma10`, `sma20`, `sma50`, `sma200`
- `pct_above_50dma`, `pct_above_200dma`
- `dcr`
- `vwap_position`
- `rvol`
- `volume_run_rate`
- `gap_pct`
- `base_length`
- `prior_run_pct`
- `compression_score`
- `higher_low_score`

필수 요구사항:

- OHLCV만으로 계산 가능한 피처는 daily batch에서 생성
- intraday 피처는 opening snapshots(5m/15m/30m/60m)로 저장
- 피처 계산은 시장 프로필에 따라 session-aware해야 함

## 9.3 Market Regime Engine

레짐은 시장별로 계산한다.

### 입력

- benchmark trend
- breadth
- opportunity density
- breakout success proxy
- focus list density

### 기본 수식

```text
market_trend_score = weighted_mean(
  close_gt_50dma,
  sma10_gt_sma20,
  sma20_slope,
  close_gt_20dma
)

breadth_score = weighted_mean(
  pct_above_50dma,
  pct_above_200dma,
  net_new_highs_lows,
  adv_decline
)

opportunity_score = weighted_mean(
  valid_breakout_count,
  top_candidate_count,
  leading_group_count,
  recent_breakout_followthrough
)

regime_score =
  0.45 * market_trend_score +
  0.35 * breadth_score +
  0.20 * opportunity_score
```

### 출력 상태

- `RISK_OFF`
- `RISK_NEUTRAL`
- `RISK_ON`
- `RISK_ON_AGGRESSIVE`

### 요구사항

- KR과 US는 별도 score를 가져야 한다.
- focus list 수와 breakout follow-through가 줄면 regime_score를 자동 하향 조정한다.
- regime는 hard blocker가 아니라 exposure guidance와 setup grading modifier로도 사용한다.

## 9.4 Universe Builder

Universe는 하나가 아니라 아래 두 개가 기본이다.

- `Breakout Universe`
- `EP Universe`

### Breakout Universe

필수 조건:

- 기본 유동성 필터 통과
- close > 50DMA 또는 그 근처의 강한 구조
- 1M/3M/6M 수익률 percentile 상위
- 52주 고점 근접 또는 신고가 흐름

권장 타깃:

- 300-600 종목

### EP Universe

필수 조건:

- 장전 또는 시초 gap 탐지 가능
- 장초반 거래량 이상 감지 가능
- 거래 가능한 유동성 확보

권장 타깃:

- 평시 적고, 실적 시즌 확대

요구사항:

- breakout universe는 OHLCV-only로 구축 가능해야 한다.
- EP universe는 OHLCV+intraday만으로 1차 구축 가능해야 한다.
- event/fundamental data가 생기면 EP universe precision이 올라가야 한다.

## 9.5 Watchlist Pipeline

리스트 파이프라인은 아래 단계를 강제한다.

1. `UniverseList`
2. `WideList`
3. `Weekly FocusList`
4. `Daily FocusList`

권장 크기:

- Universe: 300-600
- WideList: 50-100
- Weekly Focus: 5-20
- Daily Focus: 1-5

분류 축:

- `Leadership`
- `Strong Movers`
- `Actionable Ideas`

## 9.6 Explainability Engine

모든 후보는 최소 3개 이상의 `reason_codes`를 가져야 한다.

예시:

- `TOP_RS_3M`
- `TOP_RS_6M`
- `NEAR_52W_HIGH`
- `TIGHT_BASE`
- `HIGHER_LOWS`
- `GAP_GE_10`
- `VOL_SHOCK_15M`
- `EARNINGS_CATALYST`
- `RISK_WITHIN_ADR`
- `REGIME_SUPPORTIVE`

또한 실패 사유도 저장해야 한다.

예시:

- `STOP_TOO_WIDE`
- `TOO_EXTENDED`
- `LOW_LIQUIDITY`
- `NO_EVENT_CONFIRMATION`
- `REGIME_HEADWIND`
- `ADR_ALREADY_CONSUMED`

## 10. 핵심 알고리즘

## 10.1 A++ Stock Score

A++는 setup 전 종목 품질 점수다.

```text
a_pp_score =
  0.30 * leadership_score +
  0.25 * trend_quality_score +
  0.15 * liquidity_score +
  0.15 * group_strength_score +
  0.15 * catalyst_or_fundamental_score
```

### 세부 정의

`leadership_score`
- `ret_1m`, `ret_3m`, `ret_6m` percentile
- 52주 고점 근접도

`trend_quality_score`
- 10DMA > 20DMA > 50DMA 여부
- higher lows
- range tightening
- high DCR frequency

`liquidity_score`
- avg volume
- avg turnover
- spread proxy

`group_strength_score`
- 동종 그룹 동반 상승
- 섹터 상대강도

`catalyst_or_fundamental_score`
- fundamentals가 있으면 EPS/sales/guidance 반영
- 없으면 reweight하지 말고 `unknown` 비중을 confidence에 반영

### 등급

- `A++`: `a_pp_score >= 85`
- `A+`: `75 <= score < 85`
- `B`: `60 <= score < 75`
- `Discard`: `< 60`

## 10.2 Data Confidence Score

OHLCV-first 원칙 때문에 score와 confidence를 분리한다.

```text
data_confidence_score = weighted_mean(
  has_daily_bars,
  has_intraday_bars,
  has_corp_actions,
  has_sector_mapping,
  has_event_data,
  has_fundamentals,
  has_estimate_data
)
```

규칙:

- OHLCV만 있는 breakout 후보도 A++까지는 갈 수 있다.
- 하지만 `5-star EP`는 event/fundamental confidence가 낮으면 제한될 수 있다.
- 점수는 높아도 confidence가 낮으면 `needs_review` 플래그를 켠다.

## 10.3 Breakout Setup Engine

### Core Thesis

Breakout은 `prior expansion -> orderly consolidation -> range expansion`이어야 한다.

### 게이트

`G1. prior run`

```text
prior_run_pct = (base_high / lowest_close_last_60d) - 1
pass if prior_run_pct >= 0.30
```

`G2. base length`

- 기본 범위: 10-40 거래일
- 확장 허용 범위: 5-60 거래일

`G3. compression`

```text
compression_score =
  0.40 * (1 - atr10_over_atr60) +
  0.30 * higher_low_score +
  0.30 * ma_surf_score
```

`G4. high proximity`

```text
high_proximity = close / rolling_252d_high
pass if high_proximity >= 0.85
```

`G5. risk geometry`

```text
risk_unit_pct = (entry_price - stop_price) / entry_price
pass if risk_unit_pct <= adr20_pct
```

### 실행 트리거

- daily pivot break
- 1m ORH break
- 5m ORH break
- 60m ORH break

### Breakout Setup Score

```text
breakout_setup_score =
  0.30 * a_pp_score +
  0.25 * compression_score +
  0.20 * readiness_score +
  0.15 * intraday_confirmation_score +
  0.10 * regime_alignment_score
```

### 5-star Breakout

필수 조건:

- `A++`
- `breakout_setup_score >= 90`
- `risk_unit_pct <= adr20_pct`
- `regime in {RISK_ON, RISK_ON_AGGRESSIVE}`
- `not too_extended`

## 10.4 Episodic Pivot Engine

### Core Thesis

EP는 기술적 패턴보다 `event intensity + gap + volume shock + revaluation`이 본질이다.

### EP를 두 단계로 나눈다

1. `EP_CORE`
2. `EP_PRICE_VOLUME`

`EP_CORE`는 Qullamaggie 원문과 가장 가깝다.
`EP_PRICE_VOLUME`은 event feed가 부족할 때 price/volume proxy로 잡는 하위 계층이다.

### EP_CORE 게이트

`G1. gap`

```text
gap_pct = open / prev_close - 1
pass if gap_pct >= 0.10
```

`G2. volume shock`

```text
volume_shock_score = max(
  premarket_volume / adv20,
  first_15m_volume / adv20,
  first_30m_volume / adv20
)
```

강한 조건 예시:

- `first_15m_volume / adv20 >= 0.50`
- 또는 `first_30m_volume / adv20 >= 1.00`

`G3. catalyst`

허용 예시:

- earnings beat
- earnings + guidance raise
- contract
- approval / regulatory
- sector revaluation

`G4. neglected base`

- 최근 3-6개월 sideways base
- 최근 60일 과도한 선행 상승 없음

### EP_PRICE_VOLUME 게이트

event feed가 없으면 아래 proxy로만 포착한다.

- `gap >= 10%`
- 장초반 비정상 volume
- DCR 높음
- VWAP 상방 유지
- 이전 3-6개월 과도한 연장 아님

주의:

- `EP_PRICE_VOLUME`은 `5-star EP`가 아니라 기본적으로 `EP Watch`로 분류한다.
- event 확인이 들어오면 `EP_CORE`로 승격한다.

### KR/US 구현 차이

US:
- premarket + regular session gap 모두 사용
- afterhours news와 premarket volume을 반영

KR:
- 공식 시초가 gap을 사용
- 첫 5/15/30분 volume shock이 핵심
- premarket 필드는 기본적으로 비활성

### EP Score

```text
ep_setup_score =
  0.30 * volume_shock_score +
  0.25 * event_intensity_score +
  0.20 * earnings_quality_score +
  0.15 * neglected_base_score +
  0.10 * regime_alignment_score
```

### 5-star EP

필수 조건:

- `EP_CORE`
- `gap >= 10%`
- `ep_setup_score >= 90`
- `stop_width <= 1.5 * adr20`
- `clear catalyst`

## 10.5 Final Priority Score

최종 daily focus 정렬은 아래를 사용한다.

```text
final_priority_score =
  0.35 * setup_score +
  0.25 * a_pp_score +
  0.20 * regime_score +
  0.10 * execution_quality_score +
  0.10 * data_confidence_score
```

### 출력 등급

- `Tier 1`: 오늘 바로 실행 가능한 5-star
- `Tier 2`: A++이지만 하루 이틀 더 필요한 구조
- `Tier 3`: 구조는 괜찮으나 데이터/레짐/리스크가 부족

## 11. 리스크 엔진 요구사항

이 시스템은 screener가 중심이지만, Qullamaggie 스타일 특성상 risk hints를 함께 내야 한다.

필수 출력:

- `suggested_entry`
- `suggested_stop`
- `risk_unit_pct`
- `position_size_hint`
- `overnight_exposure_flag`

기본 하드 룰:

- `max_overnight_exposure_per_symbol <= 30%`
- `typical_position_size_range = 10~20%`
- `risk_per_trade = 0.25~1.0%`
- breakout stop은 ADR/ATR 이내
- EP stop은 1.0~1.5x ADR/ATR 이내

주의:

- MVP에서는 실제 주문 계산기를 간단한 힌트 수준으로만 제공해도 충분하다.
- 자동 주문은 범위 밖이다.

## 12. 일간 운영 플로우

### 12.1 주말/주간 배치

- 종목 마스터 갱신
- 일봉 OHLCV 적재
- 시장별 regime 재계산
- leader ranking 생성
- universe / wide / weekly focus 생성

### 12.2 장전 배치

US:
- premarket movers
- earnings / gap / news enrichment
- EP candidates 생성

KR:
- 당일 시초 예상보다는 전일 장후/당일 공시, 전일 강한 D1 setup 정렬
- 장초반 open-drive candidate 준비

### 12.3 개장 후

- 5분 snapshot
- 15분 snapshot
- 30분 snapshot
- 60분 snapshot
- ORH break / invalidation / stop width 재평가

### 12.4 장후

- signal snapshot 저장
- focus list 품질 평가
- breakout follow-through 추적
- regime feedback 업데이트

## 13. 출력 UX / API 요구사항

각 후보는 UI와 API에서 동일한 구조를 가져야 한다.

```json
{
  "symbol": "NVDA",
  "market": "US",
  "setup_family": "BREAKOUT",
  "candidate_stage": "DAILY_FOCUS",
  "stock_grade": "A++",
  "setup_grade": "5-star",
  "regime_state": "RISK_ON",
  "scores": {
    "a_pp_score": 91.2,
    "setup_score": 92.8,
    "final_priority_score": 90.4,
    "data_confidence_score": 84.0
  },
  "execution": {
    "entry_timeframe": "5m_ORH",
    "pivot_price": 123.45,
    "stop_price": 119.80,
    "risk_unit_pct": 2.96
  },
  "reason_codes": [
    "TOP_RS_3M",
    "TIGHT_BASE",
    "NEAR_52W_HIGH",
    "REGIME_SUPPORTIVE"
  ],
  "data_flags": [
    "HAS_INTRADAY",
    "NO_EARNINGS_DATA"
  ]
}
```

## 14. 백테스트 요구사항

### 14.1 필수 조건

- 이벤트 드리븐 분봉 백테스트 지원
- ORH 진입 시점 재현 가능
- LOD 손절 재현 가능
- partial take profit / break-even 이동 재현 가능
- trailing by 10/20DMA close break 재현 가능

### 14.2 검증 모드

1. `Daily Approximation Mode`
2. `Intraday Exact Mode`

`Daily Approximation Mode`는 빠른 연구용이다.
`Intraday Exact Mode`는 ORH/LOD/volume shock의 정식 검증용이다.

### 14.3 필수 로그

- signal generation timestamp
- feature snapshot hash
- regime snapshot
- candidate score breakdown
- entry/exit simulated reason
- slippage assumption

## 15. 비기능 요구사항

- 동일 입력에 대해 재실행하면 동일 결과가 나와야 한다.
- 각 score는 구성 항목이 분해 가능해야 한다.
- 데이터 지연/누락이 발생하면 후보를 조용히 누락시키지 말고 `data_flags`로 노출해야 한다.
- 시장별 시계열과 타임존이 분리 저장되어야 한다.
- KR/US를 동시에 돌려도 시장 캘린더 충돌이 없어야 한다.

## 16. MVP 정의

### Phase 1: OHLCV-first MVP

- KR/US daily OHLCV ingest
- market regime
- leader rank
- breakout universe
- wide/focus list
- A++ score (price-volume only)
- breakout 5-star

### Phase 2: Intraday MVP

- 1m/5m/60m bars
- ORH alerts
- LOD stop geometry
- intraday candidate resorting
- KR open-drive EP lite

### Phase 3: Enriched EP

- earnings/event/fundamental adapters
- EP_CORE detection
- catalyst scoring
- estimate revision features
- data confidence scoring

### Phase 4: Research Platform

- event-driven backtest
- replay viewer
- signal journal
- post-trade review tagging

## 17. 명시적 리스크와 오픈 이슈

- KR/US sector taxonomy를 어떻게 통일할지
- KR에서 EP proxy를 어디까지 허용할지
- premarket 없는 시장에서 EP와 breakout의 경계 정의
- corporate action adjustment 기준
- short interest / float 데이터 커버리지
- event/fundamental scraper의 운영 비용
- intraday vendor latency와 replay fidelity

## 18. 최종 제안

가장 현실적이고 순도 높은 제품 전략은 아래다.

1. 먼저 `OHLCV-only Breakout Engine`으로 KR/US를 모두 커버한다.
2. 그 위에 `market-local regime engine`을 얹는다.
3. 그 다음 `intraday ORH layer`를 붙인다.
4. 마지막으로 `EP enrichment layer`를 추가한다.
5. 모든 후보는 점수보다 `하드 룰 통과 여부 + reason codes + data confidence`가 먼저 보이게 한다.

이 순서가 좋은 이유는, Qullamaggie 스타일의 핵심인 `리더십`, `타이트한 구조`, `레짐`, `ORH 실행`은 OHLCV와 분봉만으로도 상당 부분 재현되기 때문이다. 재무/이벤트 데이터는 특히 EP의 정밀도를 크게 올리지만, 시스템의 출발 조건이 되어서는 안 된다.

## 19. 참고 메모

- 이 PRD는 기존 연구 초안과 `Q style.md`의 연구 내용을 제품 관점으로 재배열한 것이다.
- `Q style.md`에서 가져온 강한 포인트는 하드 리스크 룰, EP 정의, 이벤트 드리븐 백테스트, data-latency 경계, 오픈소스 역할 분리다.
- 기존 source purity 원칙은 유지한다. 즉, Qullamaggie 본인 자료와 `qullamaggie.net` 연결 자료를 뼈대로 두고, 제품 구현에 필요한 정량 설계만 추가했다.
