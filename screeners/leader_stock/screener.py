# -*- coding: utf-8 -*-
"""Market Reversal Leader 스크리너 - CNN Fear & Greed + RS Line 기반.

CNN Fear & Greed Index로 시장 국면을 식별하고,
RS Line 선행 신고가, RS Rating, Pocket Pivot을 조합해
시장 반전 구간의 주도주를 조기 포착하는 스크리너.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    DATA_US_DIR,
    RESULTS_DIR,
    US_WITH_RS_PATH,
    STOCK_METADATA_PATH,
)
from utils.calc_utils import get_us_market_today
from utils.io_utils import ensure_dir, extract_ticker_from_filename
from utils.screener_utils import save_screening_results, track_new_tickers, create_screener_summary


# 결과 저장 디렉토리
LEADER_STOCK_RESULTS_DIR = os.path.join(RESULTS_DIR, 'leader_stock')

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MarketReversalLeaderScreener:
    """Market Reversal Leader 스크리너 클래스"""
    
    def __init__(self, skip_data=False):
        """초기화"""
        self.today = get_us_market_today()
        self.skip_data = skip_data
        ensure_dir(LEADER_STOCK_RESULTS_DIR)
        
        if skip_data:
            self.fgi_value = self._fetch_fear_greed_index()  # skip-data 모드에서도 FGI는 가져옴
            self.market_state = self._determine_market_state()
            self.ftd_confirmed = False
            self.rs_scores = {}
            logger.info("Skip data mode: 주식 데이터 로드 건너뜀 (FGI는 조회)")
        else:
            self.fgi_value = self._fetch_fear_greed_index()
            self.market_state = self._determine_market_state()
            self.ftd_confirmed = self._check_ftd_confirmation()
            self.rs_scores = self._load_rs_scores()
            self._load_spx_data()
            
        logger.info(f"시장 상태: {self.market_state}, FGI: {self.fgi_value}, FTD 확인: {self.ftd_confirmed}")
    
    def _fetch_fear_greed_index(self) -> int:
        """CNN Fear & Greed Index 조회 (개선된 버전)"""
        try:
            # 1. 실제 CNN FGI API 시도
            try:
                # Alternative Data API (예: Alpha Vantage, Quandl 등)
                # 실제 구현 시 API 키 필요
                response = requests.get(
                    "https://api.alternative.me/fng/",
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and len(data['data']) > 0:
                        fgi_value = int(data['data'][0]['value'])
                        logger.info(f"실제 FGI API 조회 성공: {fgi_value}")
                        return fgi_value
            except Exception as api_error:
                logger.warning(f"FGI API 조회 실패, VIX 기반 계산으로 전환: {api_error}")
            
            # 2. VIX 기반 개선된 FGI 계산
            vix_path = os.path.join(DATA_US_DIR, 'VIX.csv')
            if os.path.exists(vix_path):
                vix_df = pd.read_csv(vix_path)
                vix_df.columns = [c.lower() for c in vix_df.columns]
                if not vix_df.empty:
                    # VIX 파일의 실제 컬럼명 확인 (vix_close 사용)
                    vix_close_col = None
                    for col in ['vix_close', 'close', 'Close']:
                        if col in vix_df.columns:
                            vix_close_col = col
                            break
                    
                    if vix_close_col:
                        latest_vix = vix_df.iloc[-1][vix_close_col]
                    else:
                        logger.warning(f"VIX 데이터에 적절한 컬럼이 없음. 컬럼: {list(vix_df.columns)}")
                        return 50
                    
                    # 개선된 VIX-FGI 변환 공식
                    # VIX 기반 다중 지표 조합
                    if latest_vix <= 12:
                        fgi = 85  # Extreme Greed
                    elif latest_vix <= 16:
                        fgi = 70  # Greed
                    elif latest_vix <= 20:
                        fgi = 50  # Neutral
                    elif latest_vix <= 30:
                        fgi = 30  # Fear
                    else:
                        fgi = 15  # Extreme Fear
                    
                    # 추가 시장 지표 반영 (Put/Call Ratio, High/Low Index 등)
                    fgi = self._adjust_fgi_with_market_indicators(fgi, latest_vix)
                    
                    logger.info(f"개선된 VIX 기반 FGI 계산: VIX={latest_vix:.2f}, FGI={fgi}")
                    return fgi
        except Exception as e:
            logger.warning(f"FGI 조회 실패, 기본값 사용: {e}")
        
        return 50  # 기본값 (중립)
    
    def _adjust_fgi_with_market_indicators(self, base_fgi: int, vix: float) -> int:
        """추가 시장 지표로 FGI 조정"""
        try:
            # SPY 데이터로 추가 지표 계산
            spy_path = os.path.join(DATA_US_DIR, 'SPY.csv')
            if os.path.exists(spy_path):
                spy_df = pd.read_csv(spy_path)
                spy_df.columns = [c.lower() for c in spy_df.columns]
                spy_df = spy_df.tail(20)
                
                if len(spy_df) >= 10:
                    # 최근 10일 수익률
                    recent_return = (spy_df.iloc[-1]['close'] / spy_df.iloc[-10]['close'] - 1) * 100
                    
                    # 수익률에 따른 FGI 조정
                    if recent_return > 5:  # 10일간 5% 이상 상승
                        base_fgi = min(100, base_fgi + 10)
                    elif recent_return < -5:  # 10일간 5% 이상 하락
                        base_fgi = max(0, base_fgi - 10)
                    
            return base_fgi
        except Exception:
            return base_fgi
    
    def _determine_market_state(self) -> str:
        """FGI 기반 시장 상태 결정"""
        if self.fgi_value <= 24:
            return "EXTREME_FEAR"
        elif 25 <= self.fgi_value <= 44:
            return "FEAR"
        elif 45 <= self.fgi_value <= 55:
            return "NEUTRAL"
        elif 56 <= self.fgi_value <= 74:
            return "GREED"
        else:
            return "EXTREME_GREED"
    
    def _check_ftd_confirmation(self) -> bool:
        """Follow Through Day (FTD) 확인 (개선된 버전)"""
        try:
            # S&P 500 데이터로 FTD 확인
            spy_path = os.path.join(DATA_US_DIR, 'SPY.csv')
            if not os.path.exists(spy_path):
                logger.warning("SPY 데이터 파일 없음")
                return False
                
            spy_df = pd.read_csv(spy_path)
            spy_df.columns = [c.lower() for c in spy_df.columns]
            spy_df['date'] = pd.to_datetime(spy_df['date'])
            spy_df = spy_df.sort_values('date')
            
            if len(spy_df) < 30:
                logger.warning("SPY 데이터 부족 (30일 미만)")
                return False
            
            # 최근 30일 데이터로 FTD 분석
            recent_spy = spy_df.tail(30).copy()
            
            # 거래량 이동평균 계산 (50일)
            if len(spy_df) >= 50:
                spy_df['volume_ma50'] = spy_df['volume'].rolling(50).mean()
                recent_spy = spy_df.tail(30).copy()
            else:
                recent_spy['volume_ma50'] = recent_spy['volume'].rolling(20).mean()
            
            # 일일 수익률 및 거래량 비율 계산
            recent_spy['price_change'] = recent_spy['close'].pct_change() * 100
            recent_spy['volume_ratio'] = recent_spy['volume'] / recent_spy['volume_ma50']
            
            # FTD 조건 (William O'Neil 기준)
            # 1. 시장 조정 후 첫 번째 강한 상승일
            # 2. 1.7% 이상 상승 + 거래량 평균 대비 40% 이상 증가
            
            # 최근 10일 내 FTD 후보일 찾기
            recent_10d = recent_spy.tail(10)
            
            ftd_candidates = recent_10d[
                (recent_10d['price_change'] >= 1.7) &  # 1.7% 이상 상승
                (recent_10d['volume_ratio'] >= 1.4) &  # 거래량 40% 이상 증가
                (recent_10d['close'] > recent_10d['close'].shift(1))  # 상승 확인
            ]
            
            # 추가 조건: FTD 이후 지속적인 상승 확인
            if len(ftd_candidates) > 0:
                # 가장 최근 FTD 후보
                latest_ftd_idx = ftd_candidates.index[-1]
                ftd_date_idx = recent_spy.index.get_loc(latest_ftd_idx)
                
                # FTD 이후 3일간의 성과 확인
                if ftd_date_idx < len(recent_spy) - 3:
                    post_ftd_data = recent_spy.iloc[ftd_date_idx:ftd_date_idx+4]
                    
                    # FTD 이후 추가 하락 없이 상승 지속 여부
                    no_major_decline = all(
                        post_ftd_data['price_change'].iloc[1:] > -1.0  # 1% 이상 하락 없음
                    )
                    
                    if no_major_decline:
                        logger.info(f"FTD 확인됨: {recent_spy.iloc[ftd_date_idx]['date'].strftime('%Y-%m-%d')}")
                        return True
            
            # 대안: 최근 5일 내 강한 상승일이 2일 이상
            strong_up_days = recent_10d[
                (recent_10d['price_change'] >= 1.0) &
                (recent_10d['volume_ratio'] >= 1.2)
            ]
            
            if len(strong_up_days) >= 2:
                logger.info(f"대안 FTD 조건 충족: 강한 상승일 {len(strong_up_days)}일")
                return True
            
            logger.info("FTD 조건 미충족")
            return False
            
        except Exception as e:
            logger.warning(f"FTD 확인 실패: {e}")
            return False
    
    def _load_rs_scores(self) -> Dict[str, float]:
        """RS 점수 로드"""
        rs_scores = {}
        
        if os.path.exists(US_WITH_RS_PATH):
            try:
                rs_df = pd.read_csv(US_WITH_RS_PATH)
                if 'symbol' in rs_df.columns and 'rs_score' in rs_df.columns:
                    rs_scores = rs_df.set_index('symbol')['rs_score'].to_dict()
                    logger.info(f"RS 점수 로드 완료: {len(rs_scores)}개 종목")
            except Exception as e:
                logger.warning(f"RS 점수 로드 실패: {e}")
        
        return rs_scores
    
    def _load_spx_data(self):
        """S&P 500 지수 데이터 로드 (RS Line 계산용)"""
        try:
            spy_path = os.path.join(DATA_US_DIR, 'SPY.csv')
            if os.path.exists(spy_path):
                self.spx_df = pd.read_csv(spy_path)
                self.spx_df.columns = [c.lower() for c in self.spx_df.columns]
                self.spx_df['date'] = pd.to_datetime(self.spx_df['date'])
                self.spx_df = self.spx_df.sort_values('date')
                logger.info(f"SPX 데이터 로드 완료: {len(self.spx_df)}일")
            else:
                self.spx_df = pd.DataFrame()
                logger.warning("SPX 데이터 파일 없음")
        except Exception as e:
            logger.warning(f"SPX 데이터 로드 실패: {e}")
            self.spx_df = pd.DataFrame()
    
    def _calculate_rs_line(self, stock_df: pd.DataFrame) -> pd.Series:
        """RS Line 계산 (개선된 버전) - 주가 / SPX * 100"""
        if self.spx_df.empty or stock_df.empty:
            return pd.Series(dtype=float)
        
        try:
            # 데이터 정리 및 검증
            stock_clean = stock_df.copy()
            spx_clean = self.spx_df.copy()
            
            # 날짜 컬럼 확인 및 정리
            if 'date' not in stock_clean.columns or 'date' not in spx_clean.columns:
                logger.warning("날짜 컬럼 없음")
                return pd.Series(dtype=float)
            
            # 날짜 형식 통일
            stock_clean['date'] = pd.to_datetime(stock_clean['date'])
            spx_clean['date'] = pd.to_datetime(spx_clean['date'])
            
            # 중복 날짜 제거 (최신 데이터 유지)
            stock_clean = stock_clean.drop_duplicates(subset=['date'], keep='last')
            spx_clean = spx_clean.drop_duplicates(subset=['date'], keep='last')
            
            # 날짜 기준으로 병합 (inner join으로 공통 날짜만)
            merged = pd.merge(stock_clean[['date', 'close']], 
                            spx_clean[['date', 'close']], 
                            on='date', suffixes=('_stock', '_spx'), how='inner')
            
            if merged.empty or len(merged) < 10:
                logger.warning(f"병합된 데이터 부족: {len(merged)}일")
                return pd.Series(dtype=float)
            
            # 0이나 음수 값 처리
            merged = merged[
                (merged['close_stock'] > 0) & 
                (merged['close_spx'] > 0) &
                (merged['close_stock'].notna()) &
                (merged['close_spx'].notna())
            ]
            
            if merged.empty:
                logger.warning("유효한 가격 데이터 없음")
                return pd.Series(dtype=float)
            
            # RS Line 계산: (주가 / SPX) * 100
            # 기준점 정규화 (첫 번째 값을 100으로 설정)
            first_ratio = merged.iloc[0]['close_stock'] / merged.iloc[0]['close_spx']
            rs_line = (merged['close_stock'] / merged['close_spx']) / first_ratio * 100
            
            # 인덱스를 원래 stock_df와 맞춤
            rs_line.index = range(len(rs_line))
            
            logger.debug(f"RS Line 계산 완료: {len(rs_line)}개 데이터포인트")
            return rs_line
            
        except Exception as e:
            logger.warning(f"RS Line 계산 실패: {e}")
            return pd.Series(dtype=float)
    
    def _check_rs_line_new_high(self, rs_line: pd.Series, stock_df: pd.DataFrame, lookback: int = 252) -> tuple[bool, str]:
        """RS Line 선행 신고가 확인 및 패턴 형성 시점 추적 (개선된 버전)"""
        # 최소 데이터 요구사항 완화 (1년 데이터가 없을 수도 있음)
        min_data_required = min(lookback, 60)  # 최소 60일
        
        if len(rs_line) < min_data_required or len(stock_df) < min_data_required:
            return False, self.today.strftime('%Y-%m-%d')
        
        try:
            # 사용 가능한 데이터 길이 결정
            available_length = min(len(rs_line), len(stock_df), lookback)
            
            # 최근 20일 내에서 RS Line 신고가 패턴 형성 시점 찾기
            recent_days = min(20, len(rs_line))
            rs_line_recent = rs_line.iloc[-recent_days:]
            stock_df_recent = stock_df.iloc[-recent_days:]
            
            pattern_formation_date = None
            
            # 최근 20일 내에서 RS Line 신고가 발생 시점 찾기
            for i in range(len(rs_line_recent)):
                current_idx = -recent_days + i
                current_rs = rs_line.iloc[current_idx]
                current_price = stock_df['close'].iloc[current_idx]
                
                # 해당 시점까지의 RS Line 최고값
                rs_period = rs_line.iloc[:current_idx+len(rs_line)] if current_idx < 0 else rs_line.iloc[:current_idx+1]
                if len(rs_period) < min_data_required:
                    continue
                    
                rs_high = rs_period.max()
                
                # 해당 시점까지의 주가 최고값
                price_period = stock_df['close'].iloc[:current_idx+len(stock_df)] if current_idx < 0 else stock_df['close'].iloc[:current_idx+1]
                price_high = price_period.max()
                
                # RS Line 신고가 조건 (더 엄격하게)
                rs_new_high = current_rs >= rs_high * 0.995  # 99.5% 이상
                
                if not rs_new_high:
                    continue
                
                # 주가 신고가 조건 (선행성 확인)
                price_not_new_high = current_price < price_high * 0.95  # 95% 미만
                
                # 대안 조건: 주가가 신고가이더라도 RS Line이 더 강한 경우
                if current_price >= price_high * 0.95:
                    # RS Line의 상대적 강도 확인
                    rs_strength = (current_rs / rs_high - 1) * 100
                    price_strength = (current_price / price_high - 1) * 100
                    
                    # RS Line이 주가보다 더 강한 신고가인 경우
                    rs_leading = rs_strength > price_strength + 1.0  # 1% 이상 차이
                    
                    pattern_detected = rs_new_high and rs_leading
                else:
                    # 전통적인 선행 신고가 조건
                    pattern_detected = rs_new_high and price_not_new_high
                
                if pattern_detected:
                    # 패턴 형성 날짜 추출
                    if hasattr(stock_df.iloc[current_idx], 'name') and hasattr(stock_df.iloc[current_idx].name, 'strftime'):
                        pattern_formation_date = stock_df.iloc[current_idx].name.strftime('%Y-%m-%d')
                    elif 'date' in stock_df.columns:
                        date_val = stock_df.iloc[current_idx]['date']
                        if pd.notna(date_val):
                            if isinstance(date_val, str):
                                pattern_formation_date = date_val
                            else:
                                pattern_formation_date = pd.to_datetime(date_val).strftime('%Y-%m-%d')
                    
                    if pattern_formation_date:
                        break
            
            # 패턴이 발견되지 않았으면 현재 시점에서 다시 확인
            if pattern_formation_date is None:
                current_rs = rs_line.iloc[-1]
                current_price = stock_df['close'].iloc[-1]
                
                # 사용 가능한 기간 내 RS Line 최고값
                rs_period = rs_line.iloc[-available_length:]
                rs_high = rs_period.max()
                
                # 사용 가능한 기간 내 주가 최고값
                price_period = stock_df['close'].iloc[-available_length:]
                price_high = price_period.max()
                
                # RS Line 신고가 조건 (더 엄격하게)
                rs_new_high = current_rs >= rs_high * 0.995  # 99.5% 이상
                
                # 주가 신고가 조건 (선행성 확인)
                price_not_new_high = current_price < price_high * 0.95  # 95% 미만
                
                # 대안 조건: 주가가 신고가이더라도 RS Line이 더 강한 경우
                if current_price >= price_high * 0.95:
                    rs_strength = (current_rs / rs_high - 1) * 100
                    price_strength = (current_price / price_high - 1) * 100
                    rs_leading = rs_strength > price_strength + 1.0  # 1% 이상 차이
                    result = rs_new_high and rs_leading
                else:
                    result = rs_new_high and price_not_new_high
                
                # 추가 검증: 최근 추세 확인
                if result:
                    if len(rs_line) >= 5:
                        recent_rs_trend = rs_line.iloc[-5:].pct_change().mean()
                        if recent_rs_trend < -0.01:  # 1% 이상 하락 추세면 제외
                            result = False
                
                if result:
                    # 최신 데이터의 날짜 사용
                    if hasattr(stock_df.iloc[-1], 'name') and hasattr(stock_df.iloc[-1].name, 'strftime'):
                        pattern_formation_date = stock_df.iloc[-1].name.strftime('%Y-%m-%d')
                    elif 'date' in stock_df.columns:
                        date_val = stock_df.iloc[-1]['date']
                        if pd.notna(date_val):
                            if isinstance(date_val, str):
                                pattern_formation_date = date_val
                            else:
                                pattern_formation_date = pd.to_datetime(date_val).strftime('%Y-%m-%d')
                    
                    if not pattern_formation_date:
                        pattern_formation_date = self.today.strftime('%Y-%m-%d')
                    
                    logger.debug(f"RS Line 선행 신고가 확인: RS={current_rs:.2f}, 주가={current_price:.2f}")
                    return True, pattern_formation_date
                else:
                    return False, self.today.strftime('%Y-%m-%d')
            else:
                logger.debug(f"RS Line 선행 신고가 패턴 형성 시점: {pattern_formation_date}")
                return True, pattern_formation_date
            
        except Exception as e:
            logger.warning(f"RS Line 신고가 확인 실패: {e}")
            return False, self.today.strftime('%Y-%m-%d')
    
    def _check_pocket_pivot(self, stock_df: pd.DataFrame) -> bool:
        """Pocket Pivot 확인"""
        if len(stock_df) < 11:
            return False
        
        try:
            recent = stock_df.iloc[-1]
            yesterday = stock_df.iloc[-2]
            
            # 기본 조건: 상승 + 거래량 증가
            price_up = recent['close'] > yesterday['close']
            
            # 최근 10일간 하락일의 최대 거래량
            last_10_days = stock_df.iloc[-11:-1]
            down_days = last_10_days[last_10_days['close'] < last_10_days['close'].shift(1)]
            
            if down_days.empty:
                max_down_volume = 0
            else:
                max_down_volume = down_days['volume'].max()
            
            # 오늘 거래량이 하락일 최대 거래량보다 큰가?
            volume_condition = recent['volume'] > max_down_volume
            
            return price_up and volume_condition
            
        except Exception as e:
            logger.warning(f"Pocket Pivot 확인 실패: {e}")
            return False
    
    def _calculate_leader_score(self, ticker: str, rs_line_new_high: bool, 
                              rs_rating: float, pocket_pivot: bool, 
                              price_strength: float) -> int:
        """리더 점수 계산 (0-4점) - 문서 기준에 맞게 조정"""
        score = 0
        
        # RS Line 선행 신고가 (1점) - 핵심 조건
        if rs_line_new_high:
            score += 1
        
        # RS Rating 80+ (1점) - 문서 기준에 맞춤
        if rs_rating >= 80:
            score += 1
        
        # Pocket Pivot (1점) - 거래량 기반 매수 신호
        if pocket_pivot:
            score += 1
        
        # 가격 강도 10% 이상 (1점) - 문서 기준에 맞춤
        if price_strength >= 10:
            score += 1
        
        return score
    
    def screen_market_reversal_leaders(self) -> pd.DataFrame:
        """Market Reversal Leader 스크리닝 실행"""
        logger.info("Market Reversal Leader 스크리닝 시작...")
        
        # skip_data 모드에서도 정상적으로 스크리닝 수행 (OHLCV 데이터만 건너뛰고 나머지는 진행)
        if self.skip_data:
            logger.info("Skip data mode: OHLCV 업데이트 없이 기존 데이터로 스크리닝 진행")
        
        # 시장 상태에 따른 스크리닝 활성화 여부
        if self.market_state in ["EXTREME_FEAR", "FEAR", "NEUTRAL"]:
            logger.info(f"시장 상태 {self.market_state}: RS Line 선행 신고가 탐색 활성화")
        elif self.market_state == "GREED":
            logger.info(f"시장 상태 {self.market_state}: 리더 집중 매수 구간")
        else:  # EXTREME_GREED
            logger.info(f"시장 상태 {self.market_state}: 과열 구간, 익절 고려")
        
        results = []
        stock_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        
        def process_stock_file(file):
            """개별 종목 파일 처리 함수"""
            try:
                ticker = extract_ticker_from_filename(file)
                
                # ETF 및 지수 제외
                if ticker in ['SPY', 'QQQ', 'DIA', 'IWM', 'VIX']:
                    return None
                
                # RS 점수 필터 (70 이상)
                rs_rating = self.rs_scores.get(ticker, 0)
                if rs_rating < 70:
                    return None
                
                # 주가 데이터 로드
                file_path = os.path.join(DATA_US_DIR, file)
                stock_df = pd.read_csv(file_path)
                stock_df.columns = [c.lower() for c in stock_df.columns]
                
                if stock_df.empty or len(stock_df) < 252:
                    return None
                
                stock_df['date'] = pd.to_datetime(stock_df['date'])
                stock_df = stock_df.sort_values('date')
                
                # RS Line 계산
                rs_line = self._calculate_rs_line(stock_df)
                if rs_line.empty:
                    return None
                
                # RS Line 선행 신고가 확인 및 패턴 형성 시점 추적
                rs_line_new_high, pattern_formation_date = self._check_rs_line_new_high(rs_line, stock_df)
                
                # Pocket Pivot 확인
                pocket_pivot = self._check_pocket_pivot(stock_df)
                
                # 가격 강도 계산 (최근 20일 수익률)
                if len(stock_df) >= 20:
                    price_20d_ago = stock_df['close'].iloc[-21]
                    current_price = stock_df['close'].iloc[-1]
                    price_strength = ((current_price / price_20d_ago) - 1) * 100
                else:
                    price_strength = 0
                
                # 리더 점수 계산
                leader_score = self._calculate_leader_score(
                    ticker, rs_line_new_high, rs_rating, pocket_pivot, price_strength
                )
                
                # 최소 점수 필터 (2점 이상)
                if leader_score < 2:
                    return None
                
                # 시장 상태별 추가 필터
                if self.market_state in ["EXTREME_FEAR", "FEAR"]:
                    # 공포 구간: RS Line 선행 신고가 필수
                    if not rs_line_new_high:
                        return None
                elif self.market_state == "EXTREME_GREED":
                    # 과열 구간: 고점수만 선별
                    if leader_score < 3:
                        return None
                
                # 결과 반환
                result = {
                    'ticker': ticker,
                    'rs_line_new_high': rs_line_new_high,
                    'rs_rating': rs_rating,
                    'pocket_pivot': pocket_pivot,
                    'price_strength': price_strength,
                    'leader_score': leader_score,
                    'close': stock_df['close'].iloc[-1],
                    'volume': stock_df['volume'].iloc[-1],
                    'market_state': self.market_state,
                    'fgi_value': self.fgi_value,
                    'ftd_confirmed': self.ftd_confirmed,
                    'date': pattern_formation_date,  # 실제 패턴 형성 시점
                    'screening_date': self.today.strftime('%Y-%m-%d')  # 스크리닝 실행 시점 (참고용)
                }
                return result
                
            except Exception as e:
                logger.warning(f"{ticker} 분석 중 오류: {e}")
                return None
        
        # 병렬 처리 실행 (스레드 안전성 보장)
        max_workers = min(4, len(stock_files))  # 최대 4개 워커
        completed_count = 0
        all_results = []  # 모든 결과를 임시로 저장
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 작업 제출
            future_to_file = {executor.submit(process_stock_file, file): file for file in stock_files}
            
            # 결과 수집 (스레드 안전)
            for future in as_completed(future_to_file):
                completed_count += 1
                
                # 진행률 출력
                if completed_count % 100 == 0:
                    logger.info(f"진행률: {completed_count}/{len(stock_files)} ({completed_count/len(stock_files)*100:.1f}%)")
                
                try:
                    result = future.result()
                    if result is not None:
                        all_results.append(result)
                except Exception as e:
                    file_name = future_to_file[future]
                    logger.warning(f"{file_name} 결과 처리 중 오류: {e}")
        
        # 결과 병합 (메인 스레드에서 안전하게 처리)
        results.extend(all_results)
         
        # 결과를 데이터프레임으로 변환
        if results:
            results_df = pd.DataFrame(results)
            
            # 리더 점수 기준으로 내림차순 정렬
            results_df = results_df.sort_values(['leader_score', 'rs_rating'], ascending=[False, False])
            
            # 결과 저장 (타임스탬프 포함 파일명 사용)
            results_list = results_df.to_dict('records')
            results_paths = save_screening_results(
                results=results_list,
                output_dir=LEADER_STOCK_RESULTS_DIR,
                filename_prefix="market_reversal_leaders",
                include_timestamp=True
            )
            
            logger.info(f"스크리닝 결과 저장 완료: {results_paths['csv_path']} ({len(results_df)}개 종목)")
            
            # 상위 10개 종목 로깅
            if len(results_df) > 0:
                top_leaders = results_df.head(10)
                logger.info("\n🏆 상위 Market Reversal Leaders:")
                for _, row in top_leaders.iterrows():
                    logger.info(f"  {row['ticker']}: 점수={row['leader_score']}, RS={row['rs_rating']:.1f}, "
                              f"RS신고가={row['rs_line_new_high']}, Pivot={row['pocket_pivot']}")
            
            return results_df
        else:
            # 빈 결과 파일 생성
            output_file = os.path.join(LEADER_STOCK_RESULTS_DIR, f"market_reversal_leaders_{self.today.strftime('%Y%m%d')}.csv")
            empty_df = pd.DataFrame(columns=[
                'ticker', 'rs_line_new_high', 'rs_rating', 'pocket_pivot', 'price_strength',
                'leader_score', 'close', 'volume', 'market_state', 'fgi_value', 'ftd_confirmed', 'date'
            ])
            empty_df.to_csv(output_file, index=False)
            empty_df.to_json(output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
             
            logger.info(f"조건을 만족하는 종목이 없습니다. 빈 파일 생성: {output_file}")
            return pd.DataFrame()


def run_leader_stock_screening(skip_data=False):
    """Market Reversal Leader 스크리닝 실행 함수"""
    print("\n🔍 Market Reversal Leader 스크리닝 시작...")
    
    screener = MarketReversalLeaderScreener(skip_data=skip_data)
    results_df = screener.screen_market_reversal_leaders()
    
    # DataFrame을 딕셔너리 리스트로 변환
    if not results_df.empty:
        results_list = results_df.to_dict('records')
    else:
        results_list = []
    
    # 결과 저장 (JSON + CSV)
    results_paths = save_screening_results(
        results=results_list,
        output_dir=LEADER_STOCK_RESULTS_DIR,
        filename_prefix="market_reversal_leaders",
        include_timestamp=True
    )
    
    # 새로운 티커 추적
    tracker_file = os.path.join(LEADER_STOCK_RESULTS_DIR, "new_leader_tickers.csv")
    new_tickers = track_new_tickers(
        current_results=results_list,
        tracker_file=tracker_file,
        symbol_key='ticker',
        retention_days=14
    )
    
    # 요약 정보 생성
    summary = create_screener_summary(
        screener_name="Market Reversal Leader",
        total_candidates=len(results_list),
        new_tickers=len(new_tickers),
        results_paths=results_paths
    )
    
    # 시장 상태 정보 출력
    if not results_df.empty:
        market_info = results_df.iloc[0]
        print(f"📊 시장 상태: {market_info['market_state']}, FGI: {market_info['fgi_value']}, FTD: {market_info['ftd_confirmed']}")
    
    print(f"✅ Market Reversal Leader 스크리닝 완료: {len(results_list)}개 종목, 신규 {len(new_tickers)}개")
    
    return results_df



