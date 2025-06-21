# Xing et al.(2010) 기반 변동성 스큐 역전 전략 종목 스크리너

## 프로젝트 개요
Xing et al.(2010) 논문 "What Does Individual Option Volatility Smirk Tell Us About Future Equity Returns?"의 핵심 발견을 기반으로 **낮은 변동성 스큐를 가진 상승 가능성이 높은 종목을 찾는 스크리너**를 개발해주세요.

## 핵심 전략 논리 (논문 기반)

### 연구 결과 요약
- **높은 변동성 스큐 → 낮은 수익률**: 가장 높은 스큐를 가진 종목들이 가장 낮은 스큐를 가진 종목들보다 **연간 10.9% 낮은 수익률**
- **예측력 지속성**: 최소 6개월간 지속되는 예측력
- **펀더멘털 연관성**: 높은 스큐 = 나쁜 실적 서프라이즈 예상

### 투자 전략
**"낮은 변동성 스큐를 가진 종목을 매수하라"** - 이들이 향후 상승할 가능성이 높음

## 1. 핵심 스크리닝 로직

### 1.1 변동성 스큐 지수 계산
```python
# 논문의 정확한 정의에 따라
skew_index = otm_put_iv - atm_call_iv

# 여기서:
# - otm_put_iv: OTM 풋옵션 내재변동성 (행사가/주가 비율 0.80~0.95)
# - atm_call_iv: ATM 콜옵션 내재변동성 (행사가/주가 비율 0.95~1.05)
```

### 1.2 1차 스크리닝: 기본 조건 (논문 Table 1 기준)
```python
# 조건 1: 옵션이 거래되는 대형주 (논문 sample과 일치)
market_cap_filter = market_cap > 1_billion  # 평균 $10.22B in 논문

# 조건 2: 충분한 유동성
liquidity_filter = (
    (monthly_turnover > 0.1) &  # 월 거래회전율 10% 이상
    (avg_daily_volume > 500_000)  # 일평균 거래량 50만주 이상
)

# 조건 3: 옵션 데이터 유효성
option_data_filter = (
    (option_volume > 0) &  # 옵션 거래량 존재
    (open_interest > 0) &  # 미결제약정 존재
    (days_to_expiration >= 10) & (days_to_expiration <= 60)  # 만기 10-60일
)
```

### 1.3 2차 스크리닝: 상승 후보 선별
```python
# 핵심: 낮은 변동성 스큐를 가진 종목 선별
# 논문에서 lowest quintile이 highest quintile보다 10.9% 더 높은 수익률

# 방법 1: 절대 기준
low_skew_filter = skew_index < 2.4  # 논문 Table 1의 25 percentile

# 방법 2: 상대 기준 (더 정확함)
skew_rank = skew_index.rank(pct=True)  # 백분위 순위
top_candidates = skew_rank <= 0.2  # 하위 20% (lowest quintile)

# 추가 조건: 최근 모멘텀 (논문 Table 2 결과 반영)
momentum_filter = past_6m_return > 0  # 과거 6개월 수익률 양수
```

## 2. 구현 요구사항

### 2.1 데이터 수집 및 처리
```python
class VolatilitySkewScreener:
    def __init__(self):
        self.target_stocks = self.get_large_cap_stocks()  # S&P 500 또는 대형주
        
    def calculate_skew_index(self, symbol):
        """논문의 정확한 정의에 따른 스큐 지수 계산"""
        # ATM 콜옵션 IV: 행사가/주가 비율이 1에 가장 가까운 것
        atm_call_iv = self.get_atm_call_iv(symbol, moneyness_target=1.0)
        
        # OTM 풋옵션 IV: 행사가/주가 비율이 0.95에 가장 가까운 것
        otm_put_iv = self.get_otm_put_iv(symbol, moneyness_target=0.95)
        
        return otm_put_iv - atm_call_iv
    
    def screen_stocks(self):
        """전체 스크리닝 프로세스"""
        results = []
        
        for symbol in self.target_stocks:
            # 1단계: 기본 조건 체크
            if not self.meets_basic_criteria(symbol):
                continue
                
            # 2단계: 스큐 지수 계산
            skew = self.calculate_skew_index(symbol)
            
            # 3단계: 상승 후보 판별 (낮은 스큐)
            if self.is_bullish_candidate(symbol, skew):
                results.append({
                    'symbol': symbol,
                    'skew_index': skew,
                    'expected_return': self.estimate_expected_return(skew),
                    'confidence_score': self.calculate_confidence(symbol, skew)
                })
        
        # 스큐 지수 기준 오름차순 정렬 (낮은 스큐 = 높은 상승 가능성)
        return sorted(results, key=lambda x: x['skew_index'])
```

### 2.2 수익률 예측 로직 (논문 Table 3 기반)
```python
def estimate_expected_performance(self, skew_index):
    """논문 결과를 기반으로 예상 성과 계산"""
    
    # 논문 Table 3 결과 활용
    # Lowest quintile (skew < 2.4%): 연간 13.18% 수익률
    # Highest quintile (skew > 8.43%): 연간 3.99% 수익률
    
    if skew_index < 2.4:  # 25 percentile 이하
        expected_annual_return = 0.13  # 13% 
        confidence = "높음"
    elif skew_index < 4.76:  # median 이하  
        expected_annual_return = 0.08  # 8%
        confidence = "중간"
    elif skew_index < 8.43:  # 75 percentile 이하
        expected_annual_return = 0.05  # 5%
        confidence = "낮음"
    else:  # 75 percentile 초과
        expected_annual_return = 0.02  # 2% (피해야 할 구간)
        confidence = "매우 낮음"
        
    return expected_annual_return, confidence
```

### 2.3 옵션 데이터 처리 전략 (우선순위별)

**🎯 권장 접근법: 계층적 데이터 소싱**

```python
class OptimalDataStrategy:
    def __init__(self):
        # 우선순위별 데이터 소스 설정
        self.data_sources = [
            "yfinance_options",       # 1순위: yfinance 옵션 체인
            "exclusion_approach"      # 2순위: 해당 종목 제외
        ]
    
    def get_options_data(self, symbol):
        """계층적 옵션 데이터 수집"""
        
        # 방법 1: yfinance 옵션 체인 (무료이지만 불안정)
        try:
            options_data = self.get_yfinance_options(symbol)  
            if self.validate_options_data(options_data):
                return options_data, "yfinance"
        except Exception as e:
            print(f"yfinance 실패 ({symbol}): {e}")
        
        # 방법 3: 데이터 없으면 해당 종목 제외
        return None, "excluded"
    
    
    def validate_options_data(self, data):
        """옵션 데이터 품질 검증"""
        if not data or 'data' not in data:
            return False
            
        # ATM 콜과 OTM 풋 데이터 존재 확인
        has_atm_calls = any(
            0.95 <= float(opt.get('strike', 0)) / float(opt.get('underlying_price', 1)) <= 1.05
            for opt in data.get('data', [])
            if opt.get('type') == 'call'
        )
        
        has_otm_puts = any(
            0.80 <= float(opt.get('strike', 0)) / float(opt.get('underlying_price', 1)) <= 0.95  
            for opt in data.get('data', [])
            if opt.get('type') == 'put'
        )
        
        return has_atm_calls and has_otm_puts
```

**🚫 합성 데이터 생성을 권장하지 않는 이유:**

1. **정확성 문제**: 실제 IV는 시장 참가자들의 기대를 반영하며, 단순한 통계적 모델로는 복제 불가능
2. **논문 무효화**: Xing et al.(2010)의 핵심은 실제 옵션 시장의 정보 우위인데, 가짜 데이터로는 의미 없음
3. **백피팅 위험**: 과거 데이터로 만든 합성 IV는 미래 예측력이 없음

**✅ 실용적 대안 전략:**

```python
def practical_screening_approach(self):
    """실용적 스크리닝 접근법"""
    
    # 전략 A: 옵션 데이터 있는 종목만 대상
    def data_available_only_strategy(self):
        """가장 정확한 방법: 데이터 있는 종목만"""
        screened_stocks = []
        
        for symbol in self.candidate_stocks:
            options_data, source = self.get_options_data(symbol)
            
            if options_data:  # 옵션 데이터가 있는 경우만
                skew = self.calculate_skew_from_options(options_data)
                screened_stocks.append({
                    'symbol': symbol,
                    'skew': skew,
                    'data_source': source,
                    'reliability': 'high'
                })
            else:
                print(f"{symbol}: 옵션 데이터 없음 - 제외")
                
        return screened_stocks
    
    # 전략 B: 하이브리드 접근법 (권장)
    def hybrid_strategy(self):
        """핵심 종목은 유료 데이터, 나머지는 제외"""
        
        # 1단계: S&P 100 대형주는 프리미엄 데이터 사용
        sp100_results = []
        for symbol in self.get_sp100_symbols():
            options_data = self.get_premium_options_data(symbol)
            if options_data:
                sp100_results.append(self.process_options_data(symbol, options_data))
        
        # 2단계: 나머지는 무료 소스에서 가능한 것만
        remaining_results = []
        for symbol in self.get_remaining_symbols():
            options_data, source = self.get_options_data(symbol)
            if options_data and source != 'excluded':
                remaining_results.append(self.process_options_data(symbol, options_data))
        
        return sp100_results + remaining_results
```

**💡 최종 권장사항:**

```python
# 프롬프트에 포함할 실제 구현 방침
RECOMMENDED_APPROACH = """
옵션 데이터 처리 우선순위:

1. **yfinance 옵션 체인** (1순위)
   - 불안정하지만 무료
   - 데이터 검증 후 사용
2. **종목 제외** (2순위)
   - 옵션 데이터 없으면 과감히 제외
   - 합성 데이터는 생성하지 않음

4. **결과 표시**
   - 데이터 소스별 신뢰도 표시
   - 제외된 종목 수 리포트
   - 실제 사용 가능한 종목 수 명시
"""
```

## 3. 출력 형식

### 3.1 스크리닝 결과 출력
```python
def generate_screening_report(self, results):
    """논문 결과를 반영한 스크리닝 리포트 생성"""
    
    print("=== Xing et al.(2010) 변동성 스큐 역전 전략 스크리너 ===")
    print(f"실행일: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"스크리닝 대상: {len(self.target_stocks)}개 종목")
    print()
    
    # 상위 추천 종목 (낮은 스큐 = 높은 상승 가능성)
    print("📈 상승 유망 종목 TOP 10 (낮은 변동성 스큐 기준)")
    print("=" * 80)
    print(f"{'순위':<4} {'종목':<6} {'회사명':<20} {'스큐':<8} {'예상수익률':<10} {'신뢰도':<8}")
    print("-" * 80)
    
    for i, stock in enumerate(results[:10], 1):
        expected_return, confidence = self.estimate_expected_performance(stock['skew_index'])
        print(f"{i:<4} {stock['symbol']:<6} {stock['company_name'][:18]:<20} "
              f"{stock['skew_index']:<8.2f}% {expected_return:<10.1%} {confidence:<8}")
    
    print()
    print("📊 스크리닝 통계")
    print(f"• 기본 조건 통과: {len(results)}개 종목")
    print(f"• 낮은 스큐 (상승 유망): {len([r for r in results if r['skew_index'] < 4.76])}개")
    print(f"• 높은 스큐 (주의 필요): {len([r for r in results if r['skew_index'] > 8.43])}개")
    
    # 논문 근거 설명
    print()
    print("📋 전략 근거 (Xing et al. 2010)")
    print("• 낮은 스큐 종목이 높은 스큐 종목보다 연간 10.9% 높은 수익률")
    print("• 예측력은 최소 6개월간 지속")
    print("• 높은 스큐 = 나쁜 실적 서프라이즈 예상")
```

### 3.2 상세 분석 결과
```python
# 예상 출력 예시
"""
=== Xing et al.(2010) 변동성 스큐 역전 전략 스크리너 ===
실행일: 2025-05-29 10:30
스크리닝 대상: 500개 종목

📈 상승 유망 종목 TOP 10 (낮은 변동성 스큐 기준)
================================================================================
순위  종목    회사명              스큐      예상수익률   신뢰도
--------------------------------------------------------------------------------
1    AAPL   Apple Inc.          1.2%     13.0%       높음
2    MSFT   Microsoft Corp      1.8%     13.0%       높음  
3    GOOGL  Alphabet Inc        2.1%     13.0%       높음
4    AMZN   Amazon.com          2.9%     8.0%        중간
5    NVDA   NVIDIA Corp         3.4%     8.0%        중간
...

📊 스크리닝 통계
• 기본 조건 통과: 234개 종목
• 낮은 스큐 (상승 유망): 47개 종목
• 높은 스큐 (주의 필요): 39개 종목

📋 전략 근거 (Xing et al. 2010)
• 낮은 스큐 종목이 높은 스큐 종목보다 연간 10.9% 높은 수익률
• 예측력은 최소 6개월간 지속  
• 높은 스큐 = 나쁜 실적 서프라이즈 예상
"""
```

## 4. 핵심 주의사항

### 4.1 논문과의 정확한 일치
- **스큐 정의**: OTM 풋 IV - ATM 콜 IV (논문 equation 1)
- **머니니스**: 풋 0.80~0.95, 콜 0.95~1.05 (논문 데이터 기준)
- **만기**: 10~60일 (논문과 동일)
- **상승 종목**: 낮은 스큐 지수를 가진 종목 선별

### 4.2 실용적 구현
- **수동 실행**: 자동 스케줄링 없이 사용자가 직접 실행
- **명확한 순위**: 스큐 지수 오름차순으로 정렬 (낮은 스큐 = 1순위)
- **실제 투자 가능**: 유동성 있는 대형주 위주 선별

**최종 목표**: 논문에서 입증된 변동성 스큐 역전 현상을 활용하여 **낮은 스큐를 가진 상승 유망 종목을 찾는 실용적 스크리너** 개발