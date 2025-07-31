# CNN Fear & Greed + Relative Strength 기반 ‘Market Reversal Leader’ 스크리너 설계 매뉴얼

본 문서는 Trae AI IDE가 **코드 없이 자연어만으로** 시장 반전 구간에 ‘가장 먼저 튀어오르는’ 주도주를 자동 발굴·랭킹·관리할 수 있게 하는 완전 명세서다. CNN Fear & Greed Index(이하 FGI)로 시장 국면을 식별하고, **RS Line 선행 신고가·RS Rating·Pocket Pivot** 등을 조합해 초기 리더(Leader) 주식을 실시간 스크리닝한다.

## 개요
침체장 끝머리에서 FGI가 극단적 공포(Extreme Fear) → 중립 구간으로 회복될 때, 주가보다 **RS Line**이 먼저 신고가를 돌파하며 강세 전환을 시사하는 종목이 등장한다[1][2]. 이러한 ‘선행 리더’를 조기에 포착해 **분할 매수 → FTD 확인 → 포지션 확대**로 연결하는 것이 전략의 핵심이다[3][4].

## 1 시장 국면 탐지 로직

| 구분 | FGI 범위 | 의미 | 트리거 액션 |
|------|---------|------|-------------|
|Extreme Fear|0–24[5] | 패닉·바닥 탐색 | 관망·워치리스트 구축 |
|Fear|25–44[5] | 반등 시도 | **RS Line 선행 신고가 탐색** |
|Neutral|45–55[5] | 추세 확인기 | FTD 유무 판정[3] |
|Greed|56–74[5] | 랠리 확정 | 리더 집중 매수 |
|Extreme Greed|75–100[5] | 과열·익절 | 분할 매도·현금화 |

**Market State Detector**

```pseudo
INPUT: FGI_today
IF FGI_today ≤24 → STATE=EXTREME_FEAR
ELSE IF 25≤FGI_today70 & FGI ≥80[18]|

## 5 지표 계산 세부

### 5.1 RS Line New High Before Price

```pseudo
RS_Line = Close / SPX_Close * 100
IF HHV(RS_Line, lookback)=RS_Line_today AND
   Price_today  Close_yesterday AND
   Volume_today > MAX(DownVolume_last10):
       PocketPivot = TRUE[12]
```

## 6 AI IDE 노드 매핑

| Trae 노드 | 입력 | 출력 | 설명 |
|-----------|------|------|------|
|FGI Fetcher|API Key|FGI_today|시장심리 값|
|Market State Detector|FGI_today|STATE|FSM 로직|
|Price Hub|OHLCV|Price/Volume DF|종목·지수 feed|
|RS Engine|Price/Benchmark|RS_Line, RS_Rating|신고가·랭크 산출|
|Pivot Detector|Price/Volume DF|Pocket_Pivot_Flag|10일 비교|
|Leader Scorer|RS, Pivot, Price|Score, Rank|조합 스코어|
|Portfolio Bot|STATE, Rank|Orders|포지션 관리|

## 7 검증 시나리오

| 케이스 | FGI 추이 | 기대 시그널 |
|--------|----------|-------------|
|A. 2020 3월 저점|8→30[5] |수일 내 RS Line 선행 신고가 다수 탐지|
|B. 2022 6월 반등 실패|15→34→신저점|FTD 미발생·매수 보류|
|C. AI 열풍(2023)|FGI 45→70|Leader 종목 NVIDIA·AMD 등 상위 랭크 예상[1]|

## 8 출력 포맷

```json
{
  "date": "YYYY-MM-DD",
  "market_state": "FEAR",
  "FTD_confirmed": false,
  "leaders": [
     {"ticker":"NVDA","RS_line_new_high":true,"RS_rating":97,"pivot":true,"score":4},
     ...
  ]
}
```

## 9 위험 관리 규칙
- 종목당 10%·섹터당 30% 초과 불가[14].  
- **Drawdown ≥ 15%**면 전량 정리 후 관망.  
- 일일 변동성 2× 평균 초과 시 비중 50% 감축.

## 10 확장 제안
- **Crypto FGI** 연동으로 알트코인 리더 탐색[19].  
- **RS Line Blue Dot Alert** API 연결로 실시간 알람[6][8].  
- **Minervini Trend Template** 병합으로 패턴 필터링 강화[20].

## 결론
FGI로 ‘공포→중립’ 구간을 감지한 뒤 **RS Line 선행 신고가 + RS Rating 90+**를 핵심 엔진으로 삼으면 시장 반전 초기에 기관 매집이 몰린 주도주를 높은 확률로 갈라낼 수 있다[1][2]. Trae AI IDE는 본 매뉴얼의 모듈·규칙·FSM을 그대로 배치함으로써 **완전 자동 스크리너**를 구현할 수 있다.