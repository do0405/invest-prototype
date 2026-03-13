# Minervini VCP + Cup-with-Handle Quant Screener PRD

작성일: 2026-03-12  
개정: v2

## 1. 문서 목적

이 문서는 Mark Minervini 계열의 `VCP(Volatility Contraction Pattern)`와 `Cup-with-Handle`를 중심으로, `KR/US 멀티마켓 주식 스크리너`를 설계하기 위한 정식 PRD다.

핵심 목표는 아래 6가지를 하나의 제품/파이프라인으로 연결하는 것이다.

1. `1년치 일봉 OHLCV`만 있어도 작동하는 기술적 스크리너를 만든다.
2. 데이터가 더 생기면 `fundamentals`, `earnings`, `news`, `sector/group`, `benchmark`까지 자연스럽게 확장되게 설계한다.
3. VCP와 Cup-with-Handle을 `형성 중`, `완성 직전(Ready)`, `최근 5영업일 이내 돌파` 상태로 분리한다.
4. 단순 패턴 탐지기가 아니라 `리더십 + 베이스 품질 + 돌파 품질 + 확장 데이터`를 종합 평가하는 랭킹 엔진을 만든다.
5. KR/US 시장 모두에 적용 가능하도록 `달력`, `유동성`, `통화`, `거래량 분포` 차이를 흡수한다.
6. 나중에 실제 Python 스크리너, 백테스트, 리포팅 시스템으로 바로 연결될 수 있게 한다.

본 문서는 원문 요약보다 `핵심 알고리즘`, `데이터 전략`, `시스템 구조`, `검증 프레임워크`, `구현 전개 순서`에 집중한다.

## 2. 제품 비전

이 제품의 본질은 단순히 “차트 모양을 찾아주는 도구”가 아니다. 더 정확하게는 아래 순서를 따르는 `idea-generation engine`이다.

`유니버스 정제 -> 추세/리더십 평가 -> 베이스 구조 판정 -> 피벗/돌파 판정 -> 우선순위 랭킹 -> 사람 검토`

즉, 목표 산출물은 아래 4종이다.

- `Forming Watchlist`: 아직 구조가 진행 중인 후보
- `Ready Watchlist`: 베이스는 완성됐고 피벗이 정의됐지만 돌파는 아직 없는 후보
- `Recent Breakout List`: 최근 5영업일 내 돌파한 후보
- `Rejected/Debug List`: 왜 탈락했는지 설명 가능한 후보

## 3. 전략 테제

공개 구현체, 정통 base-pattern 설명, 사용자 메모를 종합하면 이 스크리너의 핵심 논리는 아래와 같다.

1. 좋은 VCP/Cup-with-Handle은 대개 `이미 강한 추세 또는 리더 성격을 보인 종목` 위에 형성된다.
2. 패턴은 `52주 고점 근처` 또는 장기 추세 위에서 형성될수록 품질이 높다.
3. VCP는 `수축 폭 감소`, `우측 타이트닝`, `거래량 건조`, `명확한 피벗`이 본질이다.
4. Cup-with-Handle은 `둥근 컵`, `상단부 핸들`, `우측 회복`, `핸들 돌파`가 본질이다.
5. 좋은 돌파는 대개 `가격이 피벗을 넘고`, `거래량 또는 거래대금이 확장`되며, `돌파 후 피벗 위에 남는다`.
6. 패턴 자체만으로 충분하지 않을 수 있으므로, `상대강도`, `섹터 리더십`, `실적/이벤트`, `시장 상태`를 나중에 얹을 수 있어야 한다.

따라서 이 제품은 `technical-only mode`와 `enriched mode` 두 가지를 동시에 지원하도록 설계한다.

## 4. 제품 범위

### 4.1 반드시 구현할 것

- KR/US 멀티마켓 일봉 OHLCV ingestion
- 데이터 표준화 및 corporate action adjustment 지원
- 공통 feature engine
- VCP detector
- Cup-with-Handle detector
- `FORMING / READY / BREAKOUT_5D` 상태 분류
- explainable ranking output
- 결과 CSV/DataFrame 출력

### 4.2 빠르게 붙일 것

- benchmark / sector / industry relative strength
- earnings calendar / earnings result ingestion
- fundamentals enrichment
- chart snapshot generation
- 간단한 walk-forward validation harness

### 4.3 나중에 붙일 것

- intraday breakout refinement
- alerting system
- portfolio construction / execution
- semi-automatic report generation
- ML reranker

### 4.4 의도적으로 제외할 것

- 블랙박스 이미지 분류기 단독 의존
- intraday 체결까지 포함한 자동매매 엔진
- 전략 성과를 보장하는 “완성형 매매 시스템” 포장

## 5. 핵심 설계 원칙

### 5.1 최소 입력과 확장 입력을 분리한다

- `Minimum viable input`: 종목별 1년 이상 일봉 OHLCV
- `Preferred input`: 조정주가, 벤치마크, 섹터/산업, fundamentals, earnings, news
- 설계는 OHLCV만으로 작동하되, 더 좋은 데이터가 들어오면 같은 엔진 위에 점수만 추가되게 한다.

### 5.2 hard gate와 score를 분리한다

- 패턴 구조 자체는 hard rule 중심으로 본다.
- 시장별 차이와 데이터 품질 차이는 score로 흡수한다.
- 파라미터 민감도가 큰 조건은 처음부터 hard filter로 고정하지 않는다.

### 5.3 멀티마켓 차이는 상대지표로 흡수한다

KR과 US는 거래량, 가격단위, 휴장일, 유동성 분포가 다르므로 아래 원칙을 따른다.

- 절대 거래량보다 `자기 이력 대비 상대 거래량`을 우선 사용한다.
- 절대 가격보다 `ATR%`, `range%`, `distance to pivot %`처럼 정규화된 지표를 우선 사용한다.
- 거래소 달력은 심볼별 `market/exchange` 메타데이터를 통해 처리한다.

### 5.4 사람 검토를 전제로 explainability를 넣는다

패턴 스크리닝은 본질적으로 사람 눈 검토가 중요하므로, 출력은 “왜 잡혔는지”를 설명할 수 있어야 한다.

## 6. 데이터 전략

### 6.1 필수 입력 데이터

최소 요구 스키마:

- `symbol`
- `market` (`KR`, `US` 등)
- `exchange`
- `date`
- `open`
- `high`
- `low`
- `close`
- `volume`

권장 추가 필드:

- `adj_close`
- `currency`
- `sector`
- `industry`
- `shares_outstanding`
- `free_float`
- `market_cap`
- `benchmark_symbol`

### 6.2 확장 가능한 enrichment 데이터

OHLCV 외에 새롭게 긁을 수 있는 데이터는 아래와 같이 별도 adapter로 붙인다.

- `Corporate actions`: split, reverse split, dividend, rights issue
- `Fundamentals`: EPS, sales, operating margin, ROE, debt, FCF, share dilution
- `Earnings`: earnings date, surprise, guidance revision
- `News/Catalyst`: news count, filing events, analyst revisions, product launch, FDA/contract 등
- `Market context`: benchmark OHLCV, sector ETF/index OHLCV, breadth proxies
- `FX`: KRW/USD 변환용 환율

### 6.3 운영 모드

`Technical-only mode`

- OHLCV와 기본 메타데이터만으로 동작
- 패턴 탐지와 기본 랭킹은 가능
- 초기 MVP와 가장 잘 맞음

`Enriched mode`

- OHLCV 위에 fundamentals, earnings, sector, benchmark, news를 얹음
- Minervini/SEPA 스타일에 더 가까운 후보 랭킹 가능
- 기술적 패턴은 같고, 랭킹과 우선순위가 고도화됨

### 6.4 데이터 정규화

필수 정규화 규칙:

- 종목별 날짜 오름차순 정렬
- 중복 날짜 제거
- `high < low`, 음수 가격, 비정상 volume row 제거 또는 quarantine
- 최근 장기 결측이 심한 종목은 패턴 탐지 대상에서 제외

결측 처리 원칙:

- 가격 결측은 보간하지 않고 해당 row 제거
- 거래량 결측은 0으로 강제하지 않고 결측으로 유지
- 이동평균/ATR 계산 시 `min_periods`를 명시

### 6.5 corporate action adjustment

우선순위는 아래와 같다.

1. `adj_close`가 있으면 이를 기준으로 OHLC를 역산 조정한다.
2. `adj_close`가 없으면 외부 corporate action source를 붙인다.
3. 조정이 불가능한 원시 데이터는 `low-confidence` 플래그를 달고 결과를 별도 관리한다.

조정 예시:

- `adj_factor = adj_close / close`
- `adj_open = open * adj_factor`
- `adj_high = high * adj_factor`
- `adj_low = low * adj_factor`
- `adj_volume = volume / adj_factor`

### 6.6 멀티마켓 정규화 규칙

- `최근 5영업일`은 각 종목의 실제 최근 5개 거래 row 기준으로 계산한다.
- 유동성은 `close * volume`의 로컬 통화 기준 또는 FX 변환 기준으로 계산한다.
- KR/US를 한 랭킹에 섞을 때는 가능하면 `usd_traded_value` 또는 percentile rank로 정규화한다.
- 거래량 confirm은 고정 배수 하나보다 `volume_multiple`과 `volume_percentile`을 같이 쓴다.

## 7. 시스템 아키텍처

### 7.1 전체 파이프라인

1. `Universe Loader`
2. `Normalizer / Adjuster`
3. `Feature Store Builder`
4. `Trend & Leadership Engine`
5. `Pivot Extraction Engine`
6. `VCP Detector`
7. `Cup-with-Handle Detector`
8. `Breakout / State Engine`
9. `Enrichment Scoring Engine`
10. `Ranking / Output Layer`
11. `Validation / Audit Layer`

### 7.2 배치 흐름

기본 daily batch:

1. 전일 또는 최신 일봉 데이터 적재
2. 조정주가/corporate action 반영
3. feature 재계산
4. 패턴 탐지 및 상태 갱신
5. 랭킹 산출
6. 결과 CSV/Parquet/DB 적재
7. 상위 후보 차트/리포트 생성

### 7.3 상태 모델

제품 용어는 아래를 기준으로 통일한다.

- `FORMING`: 구조 일부는 맞지만 아직 피벗 완성 전
- `READY`: 패턴은 완성되었고 피벗 정의 가능, 아직 유효 돌파는 없음
- `BREAKOUT_5D`: 최근 5영업일 내 유효 돌파 발생
- `STALE`: READY 또는 BREAKOUT 이후 시간이 지나 freshness가 낮아짐
- `FAILED`: invalidation level 이탈 또는 돌파 실패

참고로 사용자 메모의 `COMPLETED`는 제품 용어상 `READY`와 동일 개념으로 본다.

## 8. 공통 feature engine

### 8.1 가격/추세 지표

필수 계산 지표:

- `SMA20, SMA50, SMA150, SMA200`
- `EMA21` optional
- `ATR14`, `ATR%`
- `range_5_pct`, `range_10_pct`
- `return_21d`, `return_63d`, `return_126d`, `return_252d`
- `distance_to_52w_high`, `distance_from_52w_low`

### 8.2 거래량/유동성 지표

- `vol_ma20`, `vol_ma50`
- `traded_value = close * volume`
- `traded_value_ma20`
- `volume_multiple = volume / vol_ma50`
- `volume_percentile_50`
- `turnover_rate` optional

### 8.3 상대강도 지표

OHLCV-only 환경에서는 universe 내부 cross-sectional RS를 기본으로 쓴다.

- `RS_cross = pct_rank(0.4*r63 + 0.3*r126 + 0.3*r252)`

benchmark가 있으면 아래를 추가한다.

- `RS_bm_63 = stock_return_63 - benchmark_return_63`
- `RS_bm_126 = stock_return_126 - benchmark_return_126`
- `Mansfield-style RS` optional

sector/group 데이터가 있으면 아래를 추가한다.

- `sector_RS_percentile`
- `industry_RS_percentile`
- `group_breadth_score`

### 8.4 fundamentals / catalyst feature

이 레이어는 없어도 제품이 돌아가지만, 있으면 후보 품질이 좋아진다.

- `sales_growth_yoy`
- `eps_growth_yoy`
- `margin_trend`
- `ROE`
- `share_dilution_1y`
- `earnings_surprise`
- `guidance_revision_score`
- `news_catalyst_score`

### 8.5 feature 사용 원칙

- feature가 없다고 후보를 무조건 버리지 않는다.
- 다만 `available feature based renormalization`으로 점수를 재가중한다.
- 즉, fundamentals가 없는 종목은 `기술적 점수만으로 평가`하고, fundamentals가 있는 종목은 `추가 가점/감점`이 붙는다.

## 9. 공통 선행 게이트

### 9.1 유동성 게이트

기본 조건:

- `bars >= 220`
- `median(traded_value, 20) >= threshold`
- `close >= min_price_threshold`
- 최근 60영업일 중 결측/정지 비중이 낮아야 함

threshold는 시장별 또는 통화별로 설정 가능해야 한다.

### 9.2 Trend Template Lite

기본 기술적 게이트:

- `close > SMA50`
- `close > SMA150`
- `SMA50 > SMA150`
- `SMA150 >= SMA200` 또는 `SMA150 slope > 0`
- `SMA200_t > SMA200_t-20`
- `distance_to_52w_high <= 15%` 권장
- `close >= 1.25 * 52w_low` 권장

### 9.3 prior run-up 조건

VCP/CWH 모두 base 이전 선행 상승이 있어야 한다.

- `base_start` 이전 40~120영업일 사이 상승폭 `>= 25%`
- 또는 `63일 수익률 >= 20%`

### 9.4 시장/섹터 컨텍스트 게이트

MVP에서는 optional이지만, benchmark와 sector가 있으면 아래를 추가한다.

- benchmark가 하락 추세일 때 long breakout candidate 감점
- sector/industry RS가 낮은 종목 감점
- earnings season 직전/직후 상태별 태그 부여

## 10. Price Simplification / Pivot Extraction

### 10.1 목적

사람은 노이즈 많은 원시 시계열이 아니라 구조를 본다. 따라서 rule-based 탐지도 먼저 `가격 단순화 -> pivot 추출` 단계를 가져가야 한다.

### 10.2 권장 방식

우선순위는 아래와 같다.

1. `Savitzky-Golay` 또는 `kernel regression`으로 close를 smoothing
2. smoothed close에서 local high/low 탐지
3. raw high/low/close에 다시 매핑해 실제 피벗 가격 확정

대안:

- `ZigZag threshold`
- `Perceptually Important Points (PIP)`
- `argrelextrema`류 국소 극값 탐지

### 10.3 기본 파라미터

- `savgol window = 9~11`
- `polyorder = 2`
- `local extrema window = 5 or 7`
- `pivot prominence >= max(4%, 1.5 * ATR20/close)`

### 10.4 pivot object

각 pivot은 아래 정보를 가진다.

- `pivot_date`
- `pivot_type` (`H`, `L`)
- `pivot_price`
- `prominence_pct`
- `source_window`

## 11. VCP Detector

### 11.1 패턴 정의

VCP는 `2~4회 이상의 점진적 수축 + 우측 타이트닝 + 거래량 건조 + 피벗 돌파` 구조로 본다.

핵심 구조:

`H0 -> L1 -> H1 -> L2 -> H2 -> ... -> Lk -> Hk`

### 11.2 후보 윈도우

- 기본 base 길이: `15~80` trading days
- 엄격 모드: `20~65` trading days
- 확장 모드: 큰 종목/긴 베이스용 `<= 100` bars

### 11.3 수축 규칙

수축 깊이:

- `d_i = (H_{i-1} - L_i) / H_{i-1}`

기본 조건:

- `k in [2, 4]` hard, `5`는 soft 허용
- `d1`은 대체로 `8% ~ 35%`
- `d_last <= 12%`
- 감소 조건:
  - `d_{i+1} <= d_i - 0.02` 또는
  - `d_{i+1} <= 0.85 * d_i`

### 11.4 구조 품질 규칙

- 수축 기간은 각 `5~25` bars
- 마지막 수축은 `5~15` bars 권장
- 저항 정렬: `max(Hs) - min(Hs) <= 6% of pivot`
- 마지막 두 고점 차이 `<= 3%`
- higher lows는 가점 요소, hard gate는 아님

### 11.5 건조(dry-up) 규칙

- `median(volume, last 10 bars) <= 0.7 * median(volume, first half of base)`
- `NATR10(last 10) <= 0.8 * NATR10(first 10)`
- `range_5_pct` 또는 `range_10_pct`가 base 초반보다 작아야 함

### 11.6 VCP pivot과 무효화

- `pivot_price = max(H0..Hk)`
- `invalidation_price = last_contraction_low`

### 11.7 VCP 상태 분류

`FORMING_VCP`

- 최소 2개 수축은 보이나
- 마지막 수축 또는 회복이 진행 중이고
- `close < pivot_price`

`READY_VCP`

- 수축/건조/저항 정렬이 유효하고
- `distance_to_pivot`가 `0% ~ 2%` 아래
- 최근 5영업일 내 유효 돌파는 없음

`BREAKOUT_VCP_5D`

- 최근 5영업일 내 유효 돌파 발생
- 최신 종가가 `pivot_price * 0.97` 위 유지

## 12. Cup-with-Handle Detector

### 12.1 패턴 정의

Cup-with-Handle은 `left rim(A) -> bottom(B) -> right rim(C) -> handle(D)` 구조로 본다.

### 12.2 컵 규칙

기본 조건:

- `cup_len = 35~130` bars
- 확장 모드: `<= 180` bars
- `cup_depth = (A - B) / A`
- 기본 `cup_depth = 12% ~ 35%`
- 완화 모드 `<= 45%`
- `C >= 0.95 * A`

### 12.3 roundness 규칙

- `bottom zone width >= 5 bars`
- 최저가 근처 25% band 체류 시간이 `cup_len * 0.1` 이상
- 저점 후 3 bars 안에 깊이의 50% 이상 회복하면 V-shape로 감점/제외
- 좌측 하락 기간과 우측 회복 기간 비율이 `0.4 ~ 2.5`

### 12.4 handle 규칙

- `handle_len = 5~20` bars
- `handle_depth <= 12%`
- 그리고 `handle_depth <= cup_depth / 3`
- `handle_low >= B + 0.5 * (A - B)`
- handle 평균 거래량은 cup 우측 상승 구간보다 낮은 편이 바람직

### 12.5 CWH pivot과 무효화

- `pivot_price = max(highs during handle)`
- handle이 아직 불명확하면 임시 `pivot = C`, 상태는 `FORMING_HANDLE`
- `invalidation_price = handle_low`

### 12.6 Cup-with-Handle 상태 분류

`FORMING_CUP`

- `A`, `B`는 식별되었지만 `C`가 아직 충분히 회복되지 않음

`FORMING_HANDLE`

- 컵은 거의 완성됐고 우측 림까지 회복했지만 handle이 진행 중

`READY_CWH`

- 컵/핸들 규칙이 충족되고
- `close`가 `pivot_price` 아래 `0% ~ 2%`
- 최근 5영업일 내 유효 돌파는 없음

`BREAKOUT_CWH_5D`

- 최근 5영업일 내 유효 돌파 발생
- 최신 종가가 `pivot_price * 0.97` 위 유지

## 13. 공통 breakout / state engine

### 13.1 유효 돌파 정의

멀티마켓 이식성을 위해 가격 조건과 거래량 조건을 분리한다.

가격 조건:

- 보수형: `close_t >= pivot_price * 1.005`
- 공격형: `high_t >= pivot_price * 1.005` and `close_t >= pivot_price`

거래량 조건:

- `volume_t >= 1.4 * vol_ma50`
- 또는 `volume_percentile_50 >= 80`
- 또는 `traded_value_multiple >= threshold` optional

### 13.2 recent breakout 유지 조건

- `days_since_breakout <= 5`
- 최신 종가 `>= pivot_price * 0.97`
- breakout 이후 최고가 대비 하락폭 `<= 8%`
- pivot 대비 +10% 이상 멀어졌으면 `extended` 태그

### 13.3 stale / failed 조건

`STALE`

- READY 후 일정 기간 동안 돌파 없음
- BREAKOUT 후 freshness window 초과

`FAILED`

- invalidation price 이탈
- breakout 후 pivot 아래 재진입 및 유지

### 13.4 overlap resolution

같은 종목이 VCP와 Cup-with-Handle을 동시에 만족할 수 있다.

- `primary_pattern = higher pattern score`
- `secondary_pattern = lower pattern score`
- output에는 두 태그를 모두 보존

## 14. 점수 체계

### 14.1 상위 구조

- `TechnicalContextScore` 0~25
- `PatternQualityScore` 0~35
- `BreakoutQualityScore` 0~20
- `LeadershipScore` 0~10
- `FundamentalCatalystScore` 0~10

`FinalScore = available-score weighted sum`

### 14.2 TechnicalContextScore

- Trend Template 충족도
- 52주 고점 근접도
- ATR% 안정성
- 유동성 적합도

### 14.3 PatternQualityScore

VCP:

- 수축 횟수와 감소 일관성
- 저항 정렬
- 우측 타이트닝
- 거래량 건조

Cup-with-Handle:

- cup depth / duration 건강도
- roundness
- handle upper-half / depth quality
- 거래량 건조

### 14.4 BreakoutQualityScore

- pivot proximity 또는 actual breakout
- breakout volume quality
- post-breakout hold
- 최근 5일 narrow-range strength

### 14.5 LeadershipScore

technical-only 환경:

- RS_cross
- liquidity percentile
- sector data 없으면 생략 후 재정규화

enriched 환경:

- benchmark-relative RS
- sector RS
- industry RS

### 14.6 FundamentalCatalystScore

데이터가 있을 때만 활성화한다.

- EPS / sales growth
- earnings surprise
- guidance/news catalyst
- dilution penalty

## 15. 출력 스키마

### 15.1 필수 출력 컬럼

- `symbol`
- `market`
- `exchange`
- `date`
- `pattern_primary`
- `pattern_secondary`
- `state`
- `pivot_price`
- `invalidation_price`
- `breakout_date`
- `days_since_breakout`
- `final_score`
- `technical_context_score`
- `pattern_quality_score`
- `breakout_quality_score`
- `leadership_score`
- `fundamental_catalyst_score`
- `distance_to_pivot_pct`
- `base_start_date`
- `base_length_bars`
- `base_depth_pct`
- `volume_multiple`
- `volume_dryup_ratio`
- `atr_tightening_ratio`
- `notes`

### 15.2 권장 추가 컬럼

- `currency`
- `sector`
- `industry`
- `benchmark_symbol`
- `rs_cross_percentile`
- `rs_benchmark`
- `earnings_date`
- `catalyst_tag`
- `data_quality_flag`

### 15.3 explainable notes 예시

- `VCP(3 contractions: 24%-13%-7%, last range_5=4.2%, vol dry-up 0.63x)`
- `CWH(cup 78 bars, depth 23%, handle 8 bars / 6.1%, breakout 1.6x vol)`

## 16. 검증 프레임워크

### 16.1 개발용 검증과 제품 검증을 분리한다

- `1년 데이터`는 개발용 sanity check에는 유효
- 정식 검증은 최소 `3~5년 이상`이 권장
- 가능하면 delisted 포함 universe를 써 survivorship bias를 줄인다

### 16.2 1차 검증: 수작업 차트 감사

- 상위 100개 후보를 수작업으로 검토
- `정탐 / 오탐 / 애매` 라벨링
- VCP와 CWH를 별도 precision 측정

### 16.3 2차 검증: 이벤트 성과 검증

예시 라벨:

- breakout 후 20거래일 내 +10% 도달
- 동시에 -7% ~ -10% 손절 이탈은 없음

또는:

- fixed holding period 수익률
- ATR trailing stop 수익률

### 16.4 평가 지표

분류 지표:

- Precision
- Recall
- F1

트레이딩 지표:

- 평균/중앙값 수익률
- 승률
- Profit factor
- Sharpe
- Max drawdown

### 16.5 파라미터 검증 방식

권장 방식:

- walk-forward validation
- rolling train/tune -> next period validate
- 시계열 누수 방지
- 단일 best point보다 `robust region`을 찾는 튜닝

### 16.6 민감도 분석

- VCP contraction minimum
- depth 감소 임계값
- handle depth
- breakout volume multiple
- range_5 / ATR% cutoff

결과가 특정 파라미터 하나에 과도하게 민감하면 hard gate를 score로 내리는 것이 적절하다.

## 17. 구현 전개 순서

### 17.1 Phase 1: Technical MVP

- KR/US OHLCV ingestion
- 조정주가 지원
- feature engine
- VCP/CWH detector
- state engine
- CSV/DataFrame output

목표:

- OHLCV만으로 `FORMING / READY / BREAKOUT_5D`를 안정적으로 산출

### 17.2 Phase 2: Market Context

- benchmark adapter
- sector/industry metadata
- cross-sectional RS + benchmark RS
- 멀티마켓 랭킹 정규화

목표:

- “좋은 패턴”과 “좋은 리더”를 분리해서 볼 수 있게 함

### 17.3 Phase 3: Enrichment

- earnings/fundamentals/news adapter
- catalyst score
- enriched ranking

목표:

- Minervini/SEPA에 더 가까운 후보 정렬

### 17.4 Phase 4: Validation & Reporting

- walk-forward harness
- chart snapshot generator
- debug report
- top candidate report

목표:

- 패턴 엔진을 제품 수준으로 고도화

## 18. MVP 의사코드

```python
for symbol, df in universe:
    df = normalize(df)
    df = adjust_if_possible(df)
    if not passes_liquidity_gate(df):
        continue

    features = compute_features(df)
    context = compute_context(df, benchmark=None, sector=None, fundamentals=None)

    if not passes_trend_template_lite(features):
        continue

    pivots = extract_pivots(df, features)

    vcp_candidates = detect_vcp(df, pivots, features, context)
    cwh_candidates = detect_cup_handle(df, pivots, features, context)

    candidates = vcp_candidates + cwh_candidates
    for c in candidates:
        breakout = check_breakout_last_5_days(df, c.pivot_price, features)
        state = classify_state(c, breakout, df)
        score = score_candidate(c, state, features, context)
        emit(symbol, c, breakout, state, score)
```

핵심은 `탐지`, `상태 분류`, `점수화`, `검증`을 분리하는 것이다.

## 19. 주요 리스크와 의사결정 포인트

- `1년 데이터`는 긴 컵 패턴에는 부족할 수 있다.
- KR과 US를 한 시스템으로 묶을 때 절대 거래량 임계값은 쉽게 깨진다.
- corporate action 조정이 없으면 패턴 탐지가 심하게 왜곡될 수 있다.
- VCP와 CWH는 재량 패턴이므로 100% 객관식 정의는 불가능하다.
- 따라서 이 제품은 “정답기”가 아니라 “고품질 후보 생성기”로 포지셔닝하는 편이 정직하다.

## 20. 참고 소스

### 20.1 개념적 원전

- Mark Minervini, `Trade Like a Stock Market Wizard`
- Mark Minervini, `Think and Trade Like a Champion`
- William J. O'Neil, `How to Make Money in Stocks`

### 20.2 패턴/구현/연구 참고 링크

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

### 20.3 이 PRD에서의 해석 원칙

- VCP 수치 임계값은 공개 구현체와 Minervini 계열 설명을 토대로 한 `정량화 추론`이다.
- Cup-with-Handle 규칙은 정통 O'Neil 계열 설명과 공개 구현체를 종합한 `실무형 규칙`이다.
- 검증 프레임워크는 “백테스트 과최적화 방지”를 우선 목표로 둔다.