class SkewCalculationsMixin:
    def calculate_skew_index(self, symbol: str) -> Optional[float]:
        """논문의 정확한 정의에 따른 스큐 지수 계산"""
        options_data, source = self.get_options_data(symbol)
        
        if not options_data:
            return None
        
        try:
            if source == "yfinance":
                return self.calculate_skew_from_yfinance(options_data)
            elif source == "alpha_vantage":
                return self.calculate_skew_from_alpha_vantage(options_data)
        except Exception as e:
            print(f"스큐 계산 오류 ({symbol}): {e}")
            return None
        
        return None
    
    def calculate_skew_from_yfinance(self, options_data: Dict) -> Optional[float]:
        """yfinance 데이터로부터 스큐 지수 계산"""
        calls = options_data['calls']
        puts = options_data['puts']
        underlying_price = options_data['underlying_price']
        
        # ATM 콜옵션 IV: 행사가/주가 비율이 1에 가장 가까운 것
        atm_call_iv = None
        min_distance = float('inf')
        
        for call in calls:
            if 'impliedVolatility' not in call or call['impliedVolatility'] <= 0:
                continue
            
            moneyness = call['strike'] / underlying_price
            if 0.95 <= moneyness <= 1.05:
                distance = abs(moneyness - 1.0)
                if distance < min_distance:
                    min_distance = distance
                    atm_call_iv = call['impliedVolatility']
        
        # OTM 풋옵션 IV: 행사가/주가 비율이 0.95에 가장 가까운 것
        otm_put_iv = None
        min_distance = float('inf')
        
        for put in puts:
            if 'impliedVolatility' not in put or put['impliedVolatility'] <= 0:
                continue
            
            moneyness = put['strike'] / underlying_price
            if 0.80 <= moneyness <= 0.95:
                distance = abs(moneyness - 0.95)
                if distance < min_distance:
                    min_distance = distance
                    otm_put_iv = put['impliedVolatility']
        
        if atm_call_iv is not None and otm_put_iv is not None:
            # 스큐 지수 = OTM 풋 IV - ATM 콜 IV (백분율로 변환)
            skew_index = (otm_put_iv - atm_call_iv) * 100
            return skew_index
        
        return None
    
    def calculate_skew_from_alpha_vantage(self, options_data: Dict) -> Optional[float]:
        """Alpha Vantage 데이터로부터 스큐 지수 계산"""
        options_list = options_data.get('data', [])
        
        atm_call_iv = None
        otm_put_iv = None
        min_call_distance = float('inf')
        min_put_distance = float('inf')
        
        for opt in options_list:
            try:
                strike = float(opt.get('strike', 0))
                underlying_price = float(opt.get('underlying_price', 1))
                iv = float(opt.get('implied_volatility', 0))
                opt_type = opt.get('type', '')
                
                if iv <= 0:
                    continue
                
                moneyness = strike / underlying_price
                
                # ATM 콜옵션 찾기
                if opt_type == 'call' and 0.95 <= moneyness <= 1.05:
                    distance = abs(moneyness - 1.0)
                    if distance < min_call_distance:
                        min_call_distance = distance
                        atm_call_iv = iv
                
                # OTM 풋옵션 찾기
                elif opt_type == 'put' and 0.80 < moneyness < 0.95:
                    distance = abs(moneyness - 0.95)
                    if distance < min_put_distance:
                        min_put_distance = distance
                        otm_put_iv = iv
                        
            except (ValueError, TypeError):
                continue
        
        if atm_call_iv is not None and otm_put_iv is not None:
            # 스큐 지수 = OTM 풋 IV - ATM 콜 IV (이미 백분율)
            skew_index = otm_put_iv - atm_call_iv
            return skew_index
        
        return None
    
    def meets_basic_criteria(self, symbol: str) -> bool:
        """기본 조건 체크 (논문 Table 1 기준)"""
        try:
            ticker = yf.Ticker(symbol)
            
            # 기본 정보 가져오기
            info = ticker.info
            hist = ticker.history(period="6mo")
            
            if len(hist) < 100:  # 충분한 거래 데이터 필요
                return False
            
            # 조건 1: 대형주 (시가총액 10억 달러 이상)
            market_cap = info.get('marketCap', 0)
            if market_cap < 1_000_000_000:  # 10억 달러
                return False
            
            # 조건 2: 충분한 유동성
            avg_volume = hist['Volume'].tail(30).mean()
            if avg_volume < 500_000:  # 일평균 거래량 50만주 이상
                return False
            
            # 월 거래회전율 계산 (근사치)
            shares_outstanding = info.get('sharesOutstanding', 0)
            if shares_outstanding > 0:
                monthly_volume = avg_volume * 21  # 월 거래일 수
                monthly_turnover = monthly_volume / shares_outstanding
                if monthly_turnover < 0.1:  # 월 거래회전율 10% 이상
                    return False
            
            return True
            
        except Exception as e:
            print(f"기본 조건 체크 오류 ({symbol}): {e}")
            return False
    
    def is_bullish_candidate(self, symbol: str, skew_index: float) -> bool:
        """상승 후보 판별 (음수 스큐만 선별)"""
        if skew_index is None:
            return False
        
        try:
            # 핵심: 음수 변동성 스큐를 가진 종목만 선별
            # 음수 스큐 = put IV < call IV = 상승 신호
            if skew_index >= 0:  # 양수 또는 0인 스큐는 제외
                return False
            
            # 추가 조건: 최근 모멘텀 (논문 Table 2 결과 반영)
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="6mo")
            
            if len(hist) < 120:
                return False
            
            # 과거 6개월 수익률 양수
            past_6m_return = (hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1)
            if past_6m_return <= 0:
                return False
            
            return True
            
        except Exception as e:
            print(f"상승 후보 판별 오류 ({symbol}): {e}")
            return False

    def estimate_expected_performance(self, skew_index: float) -> Tuple[float, str]:
        """음수 스큐 값에 따른 예상 성과 계산"""
        
        # 음수 스큐 값을 절댓값으로 변환하여 구간 분류
        abs_skew = abs(skew_index)
        
        if abs_skew >= 5.0:  # 매우 낮은 스큐 (강한 상승 신호)
            expected_annual_return = 0.15  # 15%
            confidence = "매우 높음"
        elif abs_skew >= 2.0:  # 낮은 스큐 (상승 신호)
            expected_annual_return = 0.13  # 13%
            confidence = "높음"
        elif abs_skew >= 1.0:  # 중간 스큐
            expected_annual_return = 0.10  # 10%
            confidence = "중간"
        else:  # 약한 음수 스큐
            expected_annual_return = 0.08  # 8%
            confidence = "낮음"
        
        return expected_annual_return, confidence
    
    def calculate_confidence(self, symbol: str, skew_index: float) -> str:
        """신뢰도 계산"""
        if skew_index < 2.4:
            return "높음"
        elif skew_index < 4.76:
            return "중간"
        else:
            return "낮음"
    
    def calculate_confidence_with_quality_adjustment(self, symbol: str, skew_index: float, data_source: str) -> Tuple[str, str, float]:
        """데이터 품질을 고려한 신뢰도 계산"""
        # 기본 신뢰도 계산
        base_confidence = self.calculate_confidence(symbol, skew_index)
        
        # 데이터 품질 등급 가져오기
        quality_info = self.data_quality_grades.get(data_source, {
            "grade": "D", 
            "confidence_multiplier": 0.5, 
            "description": "알 수 없는 데이터 소스"
        })
        
        # 신뢰도 점수를 숫자로 변환
        confidence_score_map = {"높음": 100, "중간": 70, "낮음": 40}
        base_score = confidence_score_map.get(base_confidence, 40)
        
        # 데이터 품질에 따른 점수 조정
        adjusted_score = base_score * quality_info["confidence_multiplier"]
        
        # 조정된 점수를 다시 등급으로 변환
        if adjusted_score >= 85:
            adjusted_confidence = "높음"
        elif adjusted_score >= 60:
            adjusted_confidence = "중간"
        else:
            adjusted_confidence = "낮음"
        
        # 품질이 낮은 경우 신뢰도 등급 하향 조정
        if data_source == "yfinance_fallback":
            if adjusted_confidence == "높음":
                adjusted_confidence = "중간"
            elif adjusted_confidence == "중간":
                adjusted_confidence = "낮음"
        
        return adjusted_confidence, quality_info["grade"], adjusted_score

    def calculate_skew_index_with_source(self, symbol: str) -> Tuple[Optional[float], str]:
        """스큐 지수 계산 및 데이터 소스 반환"""
        options_data, source = self.get_options_data(symbol)
        
        if not options_data:
            return None, "excluded"
        
        try:
            if source in ["yfinance", "yfinance_fallback"]:
                skew = self.calculate_skew_from_yfinance(options_data)
            elif source == "alpha_vantage":
                skew = self.calculate_skew_from_alpha_vantage(options_data)
            else:
                return None, source
                
            return skew, source
        except Exception as e:
            print(f"스큐 계산 오류 ({symbol}): {e}")
            return None, source


