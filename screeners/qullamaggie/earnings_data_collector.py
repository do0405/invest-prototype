# -*- coding: utf-8 -*-
# 쿨라매기 매매법 - 실적 서프라이즈 데이터 수집 모듈

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import logging
import threading

# yahoo_fin 라이브러리 (실적 컨센서스 데이터용)
try:
    import yahoo_fin.stock_info as si
    YAHOO_FIN_AVAILABLE = True
except ImportError:
    YAHOO_FIN_AVAILABLE = False
    print("⚠️ yahoo_fin 라이브러리가 설치되지 않았습니다. 'pip install yahoo_fin'으로 설치하면 더 정확한 실적 컨센서스 데이터를 사용할 수 있습니다.")

logger = logging.getLogger(__name__)

class EarningsDataCollector:
    """실적 서프라이즈 데이터 수집기 (thread-safe)"""
    
    def __init__(self):
        self.cache = {}  # 캐싱으로 API 호출 최소화
        self.cache_expiry = {}  # 캐시 만료 시간
        self.cache_duration = 3600  # 1시간 캐시 유지
        self._cache_lock = threading.Lock()  # 캐시 접근 동기화
    
    def get_earnings_surprise(self, symbol: str) -> Optional[Dict[str, Any]]:
        """실적 서프라이즈 데이터 수집
        
        Args:
            symbol: 종목 티커
            
        Returns:
            Dict: 실적 서프라이즈 정보 또는 None
        """
        # 캐시 확인 (thread-safe)
        if self._is_cached(symbol):
            with self._cache_lock:
                return self.cache[symbol]
        
        try:
            # 1순위: yahoo_fin을 통한 실제 컨센서스 데이터 수집
            earnings_data = self._fetch_yahoo_fin_earnings(symbol)
            
            # 2순위: yfinance를 통한 백업 데이터 수집 (추정치 사용)
            if earnings_data is None:
                earnings_data = self._fetch_yf_earnings(symbol)
            
            if earnings_data is not None:
                surprise_data = self._calculate_surprise(earnings_data)
                self._cache_data(symbol, surprise_data)
                return surprise_data
            
            logger.warning(f"{symbol}: 실적 데이터를 찾을 수 없습니다.")
            return None
            
        except Exception as e:
            logger.error(f"{symbol}: 실적 데이터 수집 중 오류 - {str(e)}")
            return None
    
    def _fetch_yf_earnings(self, symbol: str) -> Optional[pd.DataFrame]:
        """yfinance를 통한 실적 데이터 수집 (백업용)"""
        try:
            ticker = yf.Ticker(symbol)
            
            # 분기별 재무제표 데이터 가져오기 (최신 API 사용)
            quarterly_income = ticker.quarterly_income_stmt
            
            if quarterly_income is None or quarterly_income.empty:
                return None
            
            # Net Income과 Total Revenue 추출
            if "Net Income" not in quarterly_income.index or "Total Revenue" not in quarterly_income.index:
                return None
                
            # 최근 4분기 데이터만 사용
            net_income_data = quarterly_income.loc["Net Income"].head(4)
            revenue_data = quarterly_income.loc["Total Revenue"].head(4)
            
            # 주식 수로 나누어 EPS 계산 (간단한 추정)
            shares_outstanding = ticker.info.get('sharesOutstanding', 1)
            if shares_outstanding and shares_outstanding > 0:
                eps_data = net_income_data / shares_outstanding
            else:
                eps_data = net_income_data  # 주식 수 정보가 없으면 순이익 그대로 사용
            
            # 데이터 결합
            earnings_data = pd.DataFrame({
                'date': net_income_data.index,
                'eps_actual': eps_data.values,
                'revenue_actual': revenue_data.values
            })
            
            # ⚠️ yfinance 한계: 컨센서스 데이터 미제공으로 추정치 사용
            # 실제 구현에서는 yahoo_fin, Alpha Vantage, FMP 등 활용 권장
            earnings_data['eps_estimate'] = earnings_data['eps_actual'] * 0.95  # 5% 낮게 추정
            earnings_data['revenue_estimate'] = earnings_data['revenue_actual'] * 0.98  # 2% 낮게 추정
            earnings_data['data_source'] = 'yfinance_estimated'
            
            return earnings_data
            
        except Exception as e:
            logger.error(f"{symbol}: yfinance 실적 데이터 수집 실패 - {str(e)}")
            return None
    
    def _fetch_yahoo_fin_earnings(self, symbol: str) -> Optional[pd.DataFrame]:
        """yahoo_fin을 통한 실적 데이터 수집 (실제 컨센서스 포함)"""
        if not YAHOO_FIN_AVAILABLE:
            return None
            
        try:
            # yahoo_fin으로 실적 히스토리 가져오기 (실제 컨센서스 포함)
            earnings_history = si.get_earnings_history(symbol)
            
            if not earnings_history:
                return None
            
            # DataFrame으로 변환
            earnings_df = pd.DataFrame(earnings_history)
            
            # 최근 4분기 데이터만 사용
            recent_earnings = earnings_df.head(4)
            
            # 데이터 정리
            earnings_data = pd.DataFrame({
                'date': pd.to_datetime(recent_earnings['startdatetime']),
                'eps_actual': recent_earnings['epsactual'].astype(float),
                'eps_estimate': recent_earnings['epsestimate'].astype(float),
                'revenue_actual': [0] * len(recent_earnings),  # yahoo_fin에서 매출 데이터는 별도 처리 필요
                'revenue_estimate': [0] * len(recent_earnings)
            })
            
            earnings_data['data_source'] = 'yahoo_fin_actual'
            
            return earnings_data
            
        except Exception as e:
            logger.error(f"{symbol}: yahoo_fin 실적 데이터 수집 실패 - {str(e)}")
            return None
    
    def _calculate_surprise(self, earnings_data: pd.DataFrame) -> Dict[str, Any]:
        """실적 서프라이즈 계산"""
        if earnings_data.empty:
            return None
        
        # 최신 분기 데이터 사용
        latest = earnings_data.iloc[0]
        data_source = latest.get('data_source', 'unknown')
        
        # EPS 서프라이즈 계산
        eps_actual = latest.get('eps_actual', 0)
        eps_estimate = latest.get('eps_estimate', 0)
        
        if eps_estimate != 0 and not pd.isna(eps_estimate) and not pd.isna(eps_actual):
            eps_surprise_pct = ((eps_actual - eps_estimate) / abs(eps_estimate)) * 100
        else:
            eps_surprise_pct = 0
        
        # 매출 서프라이즈 계산
        revenue_actual = latest.get('revenue_actual', 0)
        revenue_estimate = latest.get('revenue_estimate', 0)
        
        if revenue_estimate != 0 and not pd.isna(revenue_estimate) and not pd.isna(revenue_actual):
            revenue_surprise_pct = ((revenue_actual - revenue_estimate) / revenue_estimate) * 100
        else:
            revenue_surprise_pct = 0
        
        # 전년 동기 대비 성장률 계산
        if len(earnings_data) >= 4:  # 4분기 데이터가 있는 경우
            prev_eps = earnings_data.iloc[3]['eps_actual']
            prev_revenue = earnings_data.iloc[3]['revenue_actual']
            
            # EPS 성장률 계산 (음수 EPS 상황 고려)
            if prev_eps != 0:
                if prev_eps > 0:
                    # 기존 양수 EPS: 일반적인 성장률 계산
                    yoy_eps_growth = ((eps_actual - prev_eps) / prev_eps) * 100
                else:
                    # 기존 음수 EPS: 손실 개선 여부로 판단
                    if eps_actual >= 0:
                        # 손실에서 흑자 전환: 매우 긍정적 (200%로 설정)
                        yoy_eps_growth = 200
                    else:
                        # 여전히 손실이지만 개선: 손실 감소율로 계산
                        # -3에서 -1로 개선 시: ((-1) - (-3)) / |-3| * 100 = 66.7% 개선
                        yoy_eps_growth = ((eps_actual - prev_eps) / abs(prev_eps)) * 100
            else:
                yoy_eps_growth = 0
            
            # 매출 성장률 계산 (일반적인 방식)
            if prev_revenue != 0:
                yoy_revenue_growth = ((revenue_actual - prev_revenue) / prev_revenue) * 100
            else:
                yoy_revenue_growth = 0
        else:
            yoy_eps_growth = 0
            yoy_revenue_growth = 0
        
        # 데이터 소스별 조건 완화
        if data_source == 'yfinance_estimated':
            # yfinance 추정치 사용 시 조건 완화
            eps_growth_condition = yoy_eps_growth >= 50  # 100% → 50%로 완화
            revenue_growth_condition = yoy_revenue_growth >= 10  # 20% → 10%로 완화
            eps_surprise_condition = eps_surprise_pct >= 10  # 20% → 10%로 완화
            revenue_surprise_condition = revenue_surprise_pct >= 10  # 20% → 10%로 완화
            data_quality = 'estimated'
        else:
            # yahoo_fin 실제 컨센서스 사용 시 원래 조건 적용
            eps_growth_condition = yoy_eps_growth >= 100
            revenue_growth_condition = yoy_revenue_growth >= 20
            eps_surprise_condition = eps_surprise_pct >= 20
            revenue_surprise_condition = revenue_surprise_pct >= 20
            data_quality = 'actual'
        
        # 종합 판단
        meets_criteria = (
            eps_growth_condition and 
            (revenue_growth_condition or data_source == 'yahoo_fin_actual') and  # yahoo_fin에서 매출 데이터 부족 시 완화
            (eps_surprise_condition or revenue_surprise_condition)  # 둘 중 하나라도 만족
        )
        
        return {
            'eps_actual': float(eps_actual) if not pd.isna(eps_actual) else 0,
            'eps_estimate': float(eps_estimate) if not pd.isna(eps_estimate) else 0,
            'eps_surprise_pct': float(eps_surprise_pct),
            'revenue_actual': float(revenue_actual) if not pd.isna(revenue_actual) else 0,
            'revenue_estimate': float(revenue_estimate) if not pd.isna(revenue_estimate) else 0,
            'revenue_surprise_pct': float(revenue_surprise_pct),
            'yoy_eps_growth': float(yoy_eps_growth),
            'yoy_revenue_growth': float(yoy_revenue_growth),
            'eps_growth_condition': eps_growth_condition,
            'revenue_growth_condition': revenue_growth_condition,
            'eps_surprise_condition': eps_surprise_condition,
            'revenue_surprise_condition': revenue_surprise_condition,
            'meets_criteria': meets_criteria,
            'data_source': data_source,
            'data_quality': data_quality,
            'earnings_date': latest.get('date', datetime.now()).strftime('%Y-%m-%d') if hasattr(latest.get('date', datetime.now()), 'strftime') else str(latest.get('date', datetime.now()))
        }
    
    def _is_cached(self, symbol: str) -> bool:
        """캐시 유효성 확인 (thread-safe)"""
        with self._cache_lock:
            if symbol not in self.cache:
                return False
            
            if symbol not in self.cache_expiry:
                return False
            
            return datetime.now() < self.cache_expiry[symbol]
    
    def _cache_data(self, symbol: str, data: Dict[str, Any]) -> None:
        """데이터 캐싱 (thread-safe)"""
        with self._cache_lock:
            self.cache[symbol] = data
            self.cache_expiry[symbol] = datetime.now() + timedelta(seconds=self.cache_duration)
    
    def clear_cache(self) -> None:
        """캐시 초기화 (thread-safe)"""
        with self._cache_lock:
            self.cache.clear()
            self.cache_expiry.clear()
    
    def is_earnings_season(self, days_threshold: int = 7) -> bool:
        """실적 발표 시즌 여부 확인
        
        Args:
            days_threshold: 실적 발표 전후 며칠을 실적 시즌으로 볼지
            
        Returns:
            bool: 실적 시즌 여부 (항상 True - 모든 월에 활성화)
        """
        # 모든 월에 실적 필터 활성화
        return True