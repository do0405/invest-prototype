# Rule-Based 정량화 투자 전략: AI 구현 가이드

## 1. 주도주 투자 전략 (Leader Stock Investment)

### 1.1 Forest to Trees 접근법 (우선순위)
- **Market Trend Check**: S&P 500이 200일 MA 위에 위치 AND VIX < 30
- **Sector Selection**: 섹터 RS(Relative Strength) 점수 ≥ 70 (상위 30%)
- **Individual Stock**: 해당 섹터 내 RS 점수 상위 10% 종목

### 1.2 Stage별 정량화 조건

#### Stage 1: 공포 완화 시점 (Fear Relief Phase)
**진입 조건:**
- 주간차트: 종가 > 30주 SMA 연속 3일
- RSI(14) > 30 (과매도 탈출)
- 거래량 ≥ 20일 평균의 200%
- 섹터 ETF RS ≥ 70

**포지션 관리:**
- 매수 비중: 총 자산의 20-30%
- 개별 종목 한도: 5%
- 손절선: -10%

#### Stage 2: 본격 상승장 (Full Bull Market)
**진입 조건:**
- 30주 SMA 위 연속 3주 유지
- 10주 SMA > 30주 SMA (정배열)
- 볼린저밴드(20,2) 상단 돌파
- 거래량 ≥ 일평균의 150%

**포지션 관리:**
- 매수 비중: 총 자산의 40-50%
- 개별 종목 한도: 8%
- 손절선: -15%

#### Stage 3: 과열 구간 (Overheating Phase)
**진입 조건:**
- 소형주 (시가총액 < $2B) OR IPO (상장 후 < 5년)
- 거래량 ≥ 일평균의 500%
- RSI(14) ≥ 70
- 모멘텀 지표 ≥ 90

**포지션 관리:**
- 매수 비중: 총 자산의 60-70%
- 개별 종목 한도: 10%
- 손절선: -20%

#### Stage 4: 어깨 구간 (Shoulder Zone)
**매도 조건:**
- P/E ≥ 40 (고평가 주식)
- 매출 성장률 < 15% (성장 둔화)
- 52주 신고가 대비 -5% ~ -10% 하락

**포지션 관리:**
- 매수 비중: 총 자산의 10-20%
- 이익 실현: +30% 도달시 50% 매도

---

## 2. 상승 모멘텀 시그널 전략 (Uptrend Momentum Signals)

### 2.1 Stan Weinstein Stage Analysis 기반 조건

#### 핵심 진입 시그널
- **30주 이평선 돌파**: 종가 > 30주 SMA 3일 연속 (신뢰도: 85%)
- **Stage 2A 확인**: 10주 SMA 상승 기울기 + 30주 SMA 수평/상승 (신뢰도: 90%)
- **거래량 확인**: 돌파일 거래량 ≥ 20일 평균의 150% (신뢰도: 80%)

#### 보조 확인 지표
- **상대강도 순위**: RS 점수 ≥ 70 (전체 시장 대비 상위 30%)
- **차트 패턴**: 컵앤핸들, 역삼각형, VCP 패턴 돌파
- **섹터 강도**: 해당 섹터 ETF RS ≥ 60

### 2.2 빠른 확인용 기술적 지표 (30주 대신 사용 가능)

#### 단기 모멘텀 지표 (1-5일 확인)
- **EMA 정배열**: 5일 EMA > 10일 EMA > 20일 EMA
- **MACD 시그널**: MACD 라인 > 시그널 라인 + 히스토그램 > 0
- **RSI 브레이크아웃**: RSI(14) > 50 이후 55 돌파
- **볼린저밴드**: 상단밴드 터치 후 재돌파

#### 거래량 기반 확인
- **VWAP 돌파**: 종가 > VWAP + 거래량 ≥ 평균의 150%
- **OBV 상승**: On-Balance Volume 3일 연속 상승
- **Accumulation/Distribution**: A/D 라인 신고점 경신

---

## 3. IPO 투자 전략 (IPO Investment)

### 3.1 Track 1: 상장가 하회 매집형 (Conservative Accumulation)

#### 진입 조건
- **가격 조건**: 상장가 대비 -10% ~ -30% 하락
- **기술적 조건**: RSI(14) < 30 AND 지지선 터치 확인
- **거래량 조건**: 일평균 거래량 < 50% (관심 소멸)
- **시장 환경**: VIX < 25 AND 해당 섹터 RS ≥ 50

#### 펀더멘털 체크리스트
- P/S 배수 < 동종업계 평균
- 매출 성장률 > 20% (최근 4분기)
- 자기자본비율 > 30%
- 현금/매출 비율 > 15%

#### 분할 매수 전략
- 1차 매수: 30% (진입 조건 만족시)
- 2차 매수: 40% (RSI 25 이하 도달시)
- 3차 매수: 30% (지지선 재확인시)

#### 목표 및 손절
- 목표가: 상장가 기준 +50%
- 손절선: 진입가 기준 -15%

### 3.2 Track 2: 빠른 상승 추종형 (Aggressive Momentum)

#### 진입 조건 (빠른 지표 사용)
- **가격 모멘텀**: 상장 후 5일 내 +20% 이상
- **단기 지표**: MACD(12,26,9) 시그널선 상향 돌파
- **거래량 폭증**: 일평균 거래량 ≥ 300%
- **기관 관심**: 기관 순매수 3일 연속 확인

#### 빠른 기술적 확인 (30주 이평선 대체)
- **5일 EMA 돌파**: 종가 > 5일 EMA 2일 연속
- **단기 RSI**: RSI(7) > 70 (강한 모멘텀)
- **스토캐스틱**: %K > %D AND %K > 80
- **Price Rate of Change**: ROC(5) > 15%

#### 포지션 관리
- 즉시 매수: 20% (조건 확인시)
- 확인 후 추가: 50% (모든 지표 정렬시)
- 최종 추가: 30% (브레이크아웃 재확인시)

#### 목표 및 손절
- 목표가: +100% (단기 목표)
- 손절선: 5일 EMA 이탈시 즉시
- 분할 매도: +50% 도달시 50% 매도

---

## 4. 리스크 관리 통합 규칙

### 4.1 포지션 사이징 매트릭스
```
전체 포트폴리오 = 100%
├── 현금 보유: 10-30% (시장 상황에 따라)
├── 주도주 투자: 40-70%
├── IPO 투자: 10-30%
└── 헷지 포지션: 5-15%
```

### 4.2 손절 및 이익실현 규칙
- **개별 종목 손절**: -20% 도달시 무조건 매도
- **포트폴리오 손절**: -15% 도달시 전체 검토
- **이익실현**: +50% 도달시 25% 매도, +100% 도달시 50% 매도

### 4.3 AI 구현용 코드 구조
```python
class QuantifiedTradingSystem:
    def __init__(self):
        self.fear_greed_index = 0
        self.market_stage = ""
        self.sector_rs_threshold = 70
        
    def check_leader_stock_conditions(self, stock_data):
        conditions = {
            'price_above_30w_ma': stock_data['close'] > stock_data['sma_30w'],
            'volume_confirmation': stock_data['volume'] >= stock_data['avg_volume_20d'] * 1.5,
            'rsi_oversold_exit': stock_data['rsi_14'] > 30,
            'sector_strength': self.get_sector_rs(stock_data['sector']) >= 70
        }
        return all(conditions.values())
        
    def check_ipo_track1_conditions(self, ipo_data):
        return {
            'price_below_ipo': ipo_data['current_price'] < ipo_data['ipo_price'] * 0.9,
            'rsi_oversold': ipo_data['rsi_14'] < 30,
            'low_volume': ipo_data['volume'] < ipo_data['avg_volume'] * 0.5,
            'market_vix': self.get_vix() < 25
        }
        
    def check_ipo_track2_conditions(self, ipo_data):
        return {
            'momentum_breakout': ipo_data['price_change_5d'] > 0.2,
            'macd_signal': ipo_data['macd'] > ipo_data['macd_signal'],
            'volume_surge': ipo_data['volume'] >= ipo_data['avg_volume'] * 3,
            'institutional_buying': ipo_data['institution_net_buy_3d'] > 0
        }
```

---

## 5. 백테스팅 및 성과 측정

### 5.1 핵심 성과 지표 (KPI)
- **샤프 비율**: > 1.5 목표
- **최대 낙폭**: < 25%
- **승률**: > 60%
- **평균 보유 기간**: 주도주 90일, IPO 30일

### 5.2 실시간 모니터링 체크리스트
- 매일 시장 개장 전: Fear & Greed Index 확인
- 매주 주말: 섹터별 RS 순위 업데이트
- 매월 말: 포트폴리오 리밸런싱
- 분기별: 전략 성과 리뷰 및 조정