# -*- coding: utf-8 -*-
"""
Stan Weinstein Stage 2 Breakout 전략 스크리너

이 모듈은 Stan Weinstein의 Stage Analysis를 기반으로 한 Stage 2 breakout 주식을 스크리닝합니다.
최근 6주 내에 발생한 Stage 2 breakout을 식별하여 이상적인 매수 시점을 포착합니다.
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
    MOMENTUM_SIGNALS_RESULTS_DIR,
)
from utils.calc_utils import get_us_market_today
from utils.io_utils import ensure_dir, extract_ticker_from_filename
# 섹터 관련 import 제거
from utils.screener_utils import save_screening_results, track_new_tickers, create_screener_summary

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StanWeinsteinStage2Screener:
    """Stan Weinstein Stage 2 Breakout 스크리너 클래스"""
    
    def __init__(self, skip_data=False):
        """초기화"""
        self.today = get_us_market_today()
        self.skip_data = skip_data
        ensure_dir(MOMENTUM_SIGNALS_RESULTS_DIR)
        
        # 6주 전 날짜 계산 (42일)
        self.six_weeks_ago = self.today - timedelta(days=42)
        
        if skip_data:
            self.rs_scores = self._load_rs_scores()  # skip_data 모드에서도 RS 점수 로드
            self._load_metadata()  # skip_data 모드에서도 메타데이터 로드
            self.strong_sectors = {}
            self.market_indices = self._load_market_indices()  # skip_data 모드에서도 시장 지수 로드
            logger.info("Skip data mode: 기존 메타데이터 확인")
        else:
            self.rs_scores = self._load_rs_scores()
            self._load_metadata()
            # 섹터 관련 기능 제거
            self.strong_sectors = {}
            self.market_indices = self._load_market_indices()
    
    def _load_rs_scores(self) -> Dict[str, float]:
        """RS 점수 로드"""
        if os.path.exists(US_WITH_RS_PATH):
            try:
                from utils.screener_utils import read_csv_flexible
                rs_df = read_csv_flexible(US_WITH_RS_PATH, required_columns=['symbol', 'rs_score'])
                if rs_df is not None:
                    return dict(zip(rs_df['symbol'], rs_df['rs_score']))
            except Exception as e:
                logger.warning(f"RS 점수 로드 실패: {e}")
        logger.warning(f"RS 점수 파일을 찾을 수 없습니다: {US_WITH_RS_PATH}")
        return {}
    
    def _load_metadata(self):
        """섹터 및 메타데이터 로드"""
        self.sector_map = {}
        
        if os.path.exists(STOCK_METADATA_PATH):
            try:
                from utils.screener_utils import read_csv_flexible
                meta = read_csv_flexible(STOCK_METADATA_PATH, required_columns=['symbol', 'sector'])
                if meta is not None:
                    self.sector_map = meta.set_index('symbol')['sector'].to_dict()
                    logger.info(f"섹터 정보 로드 완료: {len(self.sector_map)}개 종목")
            except Exception as e:
                logger.warning(f"메타데이터 로드 실패: {e}")
        else:
            logger.warning(f"메타데이터 파일이 없습니다: {STOCK_METADATA_PATH}")
    
    def _load_sector_strength(self) -> Dict[str, Dict]:
        """섹터별 상대 강도 계산 (비활성화)"""
        # 섹터 관련 기능 완전 제거
        return {}
    
    def _load_market_indices(self) -> Dict[str, pd.DataFrame]:
        """주요 시장 지수 데이터 로드"""
        indices = {}
        index_symbols = ['SPY', 'QQQ', 'DIA', 'IWM']  # S&P 500, NASDAQ, Dow, Russell 2000
        
        for symbol in index_symbols:
            file_path = os.path.join(DATA_US_DIR, f"{symbol}.csv")
            if os.path.exists(file_path):
                try:
                    from utils.screener_utils import read_csv_flexible
                    df = read_csv_flexible(file_path, required_columns=['date', 'close'])
                    if df is not None:
                        df['date'] = pd.to_datetime(df['date'], utc=True)
                        df = df.sort_values('date')
                        indices[symbol] = df
                        logger.info(f"{symbol} 지수 데이터 로드 성공: {len(df)}일")
                    else:
                        logger.warning(f"{symbol} 지수 데이터 로드 실패")
                except Exception as e:
                    logger.warning(f"{symbol} 지수 데이터 로드 실패: {e}")
            else:
                logger.warning(f"{symbol} 지수 데이터 파일 없음: {file_path}")
        
        if indices:
            logger.info(f"시장 지수 로드 완료: {list(indices.keys())}")
        else:
            logger.warning("시장 지수 데이터를 찾을 수 없습니다")
        return indices
    
    def _check_market_environment(self) -> bool:
        """시장 환경 조건 확인"""
        if not self.market_indices:
            logger.warning("시장 지수 데이터가 없어 시장 환경 확인 불가")
            return True  # 데이터가 없으면 통과
        
        # S&P 500 (SPY) 기준으로 시장 트렌드 확인
        if 'SPY' not in self.market_indices:
            return True
        
        spy_df = self.market_indices['SPY']
        if len(spy_df) < 150:
            return True
        
        # 30주 이동평균선 계산 (150일 ≈ 30주)
        spy_df['sma_150'] = spy_df['close'].rolling(window=150).mean()
        
        # 이동평균선 계산 후 최신 데이터 가져오기
        recent_spy = spy_df.iloc[-1]
        
        # 시장이 30주 이동평균선 위에 있고, 이동평균선이 상승 추세인지 확인
        current_sma = recent_spy['sma_150'] if 'sma_150' in spy_df.columns else None
        prev_sma = spy_df.iloc[-10]['sma_150'] if len(spy_df) >= 10 and 'sma_150' in spy_df.columns else None
        
        if current_sma is None or prev_sma is None:
            return True
        
        market_above_ma = recent_spy['close'] > current_sma
        ma_trending_up = current_sma > prev_sma
        
        logger.info(f"시장 환경 - SPY 가격: {recent_spy['close']:.2f}, 150일 MA: {current_sma:.2f}, MA 상승: {ma_trending_up}")
        
        return market_above_ma and ma_trending_up
    
    def _calculate_weekly_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """일간 데이터를 주간 데이터로 변환"""
        if df.empty:
            return df
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'], utc=True)
        df.set_index('date', inplace=True)
        
        # 주간 데이터로 리샘플링
        weekly = df.resample('W').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        return weekly.reset_index()
    
    def _calculate_moving_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """이동평균선 계산"""
        df = df.copy()
        
        # 30주 이동평균선 (150일 ≈ 30주)
        df['sma_30'] = df['close'].rolling(window=30).mean()
        df['sma_30w'] = df['sma_30']  # 호환성을 위한 별칭
        
        # 추가 이동평균선들
        df['sma_10w'] = df['close'].rolling(window=10).mean()
        df['sma_20w'] = df['close'].rolling(window=20).mean()
        df['sma_40w'] = df['close'].rolling(window=40).mean()
        
        return df
    
    def _calculate_volume_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """거래량 지표 계산"""
        df = df.copy()
        
        # 평균 주간 거래량 (20주)
        df['avg_volume_20w'] = df['volume'].rolling(window=20).mean()
        
        # 거래량 비율
        df['volume_ratio'] = df['volume'] / df['avg_volume_20w']
        
        # OBV (On-Balance Volume) 계산
        df['obv'] = 0.0
        for i in range(1, len(df)):
            if df.iloc[i]['close'] > df.iloc[i-1]['close']:
                df.iloc[i, df.columns.get_loc('obv')] = df.iloc[i-1]['obv'] + df.iloc[i]['volume']
            elif df.iloc[i]['close'] < df.iloc[i-1]['close']:
                df.iloc[i, df.columns.get_loc('obv')] = df.iloc[i-1]['obv'] - df.iloc[i]['volume']
            else:
                df.iloc[i, df.columns.get_loc('obv')] = df.iloc[i-1]['obv']
        
        # OBV 상승 확인 (단순화)
        df['obv_rising'] = df['obv'] > df['obv'].shift(1)
        
        return df
    
    def _calculate_relative_strength(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """상대 강도 계산 (vs S&P 500)"""
        if 'SPY' not in self.market_indices or df.empty:
            df['relative_strength'] = 1.0
            return df
        
        spy_df = self.market_indices['SPY']
        spy_weekly = self._calculate_weekly_data(spy_df)
        
        if spy_weekly.empty:
            df['relative_strength'] = 1.0
            return df
        
        # 날짜 기준으로 병합
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        spy_weekly['date'] = pd.to_datetime(spy_weekly['date'])
        
        merged = pd.merge_asof(
            df_copy.sort_values('date'),
            spy_weekly[['date', 'close']].rename(columns={'close': 'spy_close'}).sort_values('date'),
            on='date',
            direction='backward'
        )
        
        # 상대 강도 계산 (주식 가격 변화율 / SPY 가격 변화율)
        if len(merged) > 1:
            stock_return = merged['close'].pct_change()
            spy_return = merged['spy_close'].pct_change()
            merged['relative_strength'] = (1 + stock_return) / (1 + spy_return)
            merged['relative_strength'] = merged['relative_strength'].fillna(1.0)
        else:
            merged['relative_strength'] = 1.0
        
        return merged
    
    def _check_higher_highs_lows(self, df: pd.DataFrame, weeks: int = 6) -> bool:
        """
        Higher highs/Higher lows 패턴 확인
        
        패턴 형성 조건:
        - Higher Highs: 각 고점이 이전 고점보다 높아야 함
        - Higher Lows: 각 저점이 이전 저점보다 높아야 함
        - 이는 상승 추세의 건전한 패턴을 나타냄
        
        현재 비활성화됨 - 항상 True 반환
        """
        # Higher highs/Higher lows 패턴 확인 로직 주석 처리 - 항상 통과
        # if len(df) < weeks:
        #     return True  # 데이터 부족시 통과
        # 
        # recent_data = df.iloc[-weeks:]
        # highs = recent_data['high'].values
        # lows = recent_data['low'].values
        # 
        # # Higher highs 확인 (최소 2개의 상승하는 고점)
        # higher_highs = 0
        # for i in range(1, len(highs)):
        #     if highs[i] > highs[i-1]:
        #         higher_highs += 1
        # 
        # # Higher lows 확인 (최소 2개의 상승하는 저점)
        # higher_lows = 0
        # for i in range(1, len(lows)):
        #     if lows[i] > lows[i-1]:
        #         higher_lows += 1
        # 
        # return higher_highs >= 2 and higher_lows >= 1
        
        return True  # 항상 통과
    
    def _identify_entry_type(self, df: pd.DataFrame, resistance_level: float) -> str:
        """
        A형/B형 매수 포인트 구분 (비활성화)
        
        A형 매수점:
        - 저항선을 처음 돌파하는 시점
        - 일반적으로 더 안전하지만 수익률이 상대적으로 낮음
        - 돌파 후 즉시 매수하는 전략
        
        B형 매수점:
        - 저항선 돌파 후 재테스트(pullback) 후 다시 상승하는 시점
        - 더 높은 수익률을 기대할 수 있지만 위험도 높음
        - 재테스트 후 반등 확인 후 매수하는 전략
        
        현재 비활성화됨 - 항상 'A형' 반환
        """
        # A/B형 구분 로직 주석 처리 - 기본값 반환
        # if len(df) < 6:
        #     return 'A형'  # 기본값
        # 
        # recent_6weeks = df.iloc[-6:]
        # 
        # # A형: 동시 돌파 (저항선과 30주 MA를 거의 동시에 돌파)
        # breakout_week = None
        # for i, row in recent_6weeks.iterrows():
        #     if row['close'] > resistance_level and row['close'] > row['sma_30']:
        #         breakout_week = i
        #         break
        # 
        # if breakout_week is not None:
        #     breakout_idx = recent_6weeks.index.get_loc(breakout_week)
        #     
        #     # B형: 돌파 후 후퇴했다가 재반등
        #     if breakout_idx < len(recent_6weeks) - 2:  # 돌파 후 최소 2주 데이터 있음
        #         post_breakout = recent_6weeks.iloc[breakout_idx+1:]
        #         
        #         # 후퇴 확인 (돌파 레벨 근처로 하락)
        #         pullback_occurred = any(post_breakout['low'] <= resistance_level * 1.02)  # 2% 여유
        #         
        #         # 재반등 확인 (다시 상승)
        #         if pullback_occurred and len(post_breakout) >= 2:
        #             recent_close = post_breakout.iloc[-1]['close']
        #             if recent_close > resistance_level:
        #                 return 'B형'
        # 
        # return 'A형'
        
        return 'A형'  # 기본값 반환
    
    def _detect_momentum_signal(self, df: pd.DataFrame, symbol: str) -> Dict:
        """Stage 2 breakout 패턴 감지 및 실제 패턴 형성 시점 추적"""
        if len(df) < 40:  # 최소 40주 데이터 필요
            return {'detected': False, 'reason': 'insufficient_data'}
        
        # 최근 6주 데이터 확인 (42일 = 6주)
        six_weeks_data = df.iloc[-6:] if len(df) >= 6 else df
        pattern_formation_date = None
        breakout_week_data = None
        
        # 최근 6주 내에서 브레이크아웃 패턴 형성 시점 찾기
        for i in range(len(six_weeks_data)):
            week_data = six_weeks_data.iloc[i]
            
            # 1. 30주 이동평균선 위치 조건
            above_30w_ma = week_data['close'] > week_data['sma_30']
            if not above_30w_ma:
                continue
            
            # 2. 30주 이동평균선 방향 조건 (상승 추세)
            if i >= 4:  # 최소 5주 데이터 필요
                prev_week_idx = max(0, i - 4)
                ma_trending_up = week_data['sma_30'] > six_weeks_data.iloc[prev_week_idx]['sma_30']
            else:
                ma_trending_up = True
            
            if not ma_trending_up:
                continue
            
            # 3. 거래량 조건 확인 (MD 문서 원본 조건)
            volume_surge = week_data['volume_ratio'] >= 2.0  # 평균의 2배 이상
            
            # 4. 상대 강도 조건 (MD 문서 원본 조건)
            positive_rs = week_data['relative_strength'] >= 1.0
            
            # 5. OBV 상승 확인 (단순화)
            obv_rising = week_data.get('obv_rising', True)
            
            # 모든 기본 조건이 충족되면 패턴 형성 시점으로 기록
            if above_30w_ma and ma_trending_up and volume_surge and positive_rs and obv_rising:
                pattern_formation_date = week_data.name if hasattr(week_data, 'name') else week_data.get('date')
                breakout_week_data = week_data
                break
        
        # 패턴이 형성되지 않았으면 False 반환
        if pattern_formation_date is None or breakout_week_data is None:
            return {'detected': False, 'reason': 'no_pattern_in_6weeks'}
        
        # 최신 데이터로 추가 검증
        recent = df.iloc[-1]
        
        # 6. Higher highs/Higher lows 패턴 확인 (비활성화)
        # healthy_pattern = self._check_higher_highs_lows(df, weeks=6)
        healthy_pattern = True  # 항상 통과
        
        # 7. 지속성 확인 (삭제됨 - 사용자 요청)
        sustained_breakout = True  # 항상 통과
        
        # 8. 저항선 계산 (패턴 형성 시점 기준)
        if len(df) >= 10:
            resistance_weeks = df.iloc[-10:-1]  # 최근 주 제외한 과거 10주
            resistance_level = resistance_weeks['high'].max() * 0.95
            breakout_occurred = breakout_week_data['close'] > resistance_level
        else:
            breakout_occurred = True
            resistance_level = breakout_week_data['high']
        
        # 9. A형/B형 매수 포인트 구분 (비활성화)
        entry_type = self._identify_entry_type(df, resistance_level)
        
        # 모든 조건 종합
        all_conditions = [
            breakout_week_data['close'] > breakout_week_data['sma_30'],  # above_30w_ma
            True,  # ma_trending_up (이미 확인됨)
            breakout_week_data['volume_ratio'] >= 2.0,  # volume_surge
            breakout_occurred,
            breakout_week_data['relative_strength'] >= 1.0,  # positive_rs
            breakout_week_data.get('obv_rising', True),  # obv_rising
            healthy_pattern
        ]
        
        detected = all(all_conditions)
        
        # 패턴 형성 날짜 포맷팅
        if pattern_formation_date:
            if isinstance(pattern_formation_date, pd.Timestamp):
                pattern_date_str = pattern_formation_date.strftime('%Y-%m-%d')
            elif isinstance(pattern_formation_date, str):
                pattern_date_str = pattern_formation_date
            else:
                pattern_date_str = str(pattern_formation_date)
        else:
            pattern_date_str = recent.name.strftime('%Y-%m-%d') if hasattr(recent, 'name') else self.today.strftime('%Y-%m-%d')
        
        return {
            'detected': detected,
            'pattern_formation_date': pattern_date_str,  # 실제 패턴 형성 시점
            'above_30w_ma': breakout_week_data['close'] > breakout_week_data['sma_30'],
            'ma_trending_up': True,  # 이미 검증됨
            'volume_surge': breakout_week_data['volume_ratio'] >= 2.0,
            'breakout_occurred': breakout_occurred,
            'positive_rs': breakout_week_data['relative_strength'] >= 1.0,
            'obv_rising': breakout_week_data.get('obv_rising', True),
            'healthy_pattern': healthy_pattern,
            'sustained_breakout': sustained_breakout,
            'entry_type': entry_type,
            'volume_ratio': breakout_week_data['volume_ratio'],
            'relative_strength': breakout_week_data['relative_strength'],
            'close_price': breakout_week_data['close'],
            'sma_30': breakout_week_data['sma_30'],
            'resistance_level': resistance_level
        }
    
    def _check_sector_leadership(self, symbol: str) -> bool:
        """섹터 리더십 확인 (비활성화)"""
        # 섹터 관련 기능 완전 제거
        return True
    
    def _check_minimal_resistance(self, df: pd.DataFrame) -> bool:
        """최소한의 상단 저항 확인 (비활성화)"""
        # 저항선 확인 로직 주석 처리 - 항상 통과
        # if len(df) < 52:  # 1년 데이터가 없으면 통과
        #     return True
        # 
        # recent_price = df.iloc[-1]['close']
        # year_high = df.iloc[-52:]['high'].max()
        # 
        # # 현재 가격이 52주 최고가의 95% 이상이면 저항이 적다고 판단
        # return recent_price >= year_high * 0.95
        
        return True  # 항상 통과
    
    def screen_momentum_signals(self) -> pd.DataFrame:
        """Stage 2 breakout 스크리닝 실행"""
        logger.info("Stan Weinstein Stage 2 Breakout 스크리닝 시작...")
        
        # skip_data 모드에서도 정상적으로 스크리닝 수행 (OHLCV 데이터만 건너뛰고 나머지는 진행)
        if self.skip_data:
            logger.info("Skip data mode: OHLCV 업데이트 없이 기존 데이터로 스크리닝 진행")
        
        # 시장 환경 확인
        market_favorable = self._check_market_environment()
        if not market_favorable:
            logger.warning("시장 환경이 불리하여 스크리닝을 제한합니다.")
        
        results = []
        stock_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        
        logger.info(f"총 {len(stock_files)}개 종목 분석 시작")
        
        def process_stock_file(file):
            """개별 종목 파일 처리 함수"""
            try:
                ticker = extract_ticker_from_filename(file)
                
                # 주요 지수 제외
                if ticker in ['SPY', 'QQQ', 'DIA', 'IWM']:
                    return None
                
                # RS 점수 필터 (70 이상)
                rs_score = self.rs_scores.get(ticker, 0)
                if rs_score < 70:
                    return None
                
                # 데이터 로드
                file_path = os.path.join(DATA_US_DIR, file)
                from utils.screener_utils import read_csv_flexible
                df = read_csv_flexible(file_path, required_columns=['close', 'volume', 'date', 'high', 'low', 'open'])
                if df is None:
                    return None
                
                # 컬럼명 정규화
                df.columns = [c.lower() for c in df.columns]
                
                # 필수 컬럼 확인
                required_columns = ['close', 'volume', 'date', 'high', 'low', 'open']
                if not all(col in df.columns for col in required_columns):
                    return None
                
                if df.empty or len(df) < 150:  # 최소 150일 데이터 필요
                    return None
                
                # 주간 데이터로 변환
                weekly_df = self._calculate_weekly_data(df)
                if len(weekly_df) < 30:  # 최소 30주 데이터 필요
                    return None
                
                # 기술적 지표 계산
                weekly_df = self._calculate_moving_averages(weekly_df)
                weekly_df = self._calculate_volume_indicators(weekly_df)
                weekly_df = self._calculate_relative_strength(weekly_df, ticker)
                
                # 최소 저항 확인
                if not self._check_minimal_resistance(weekly_df):
                    return None
                
                # Stage 2 breakout 감지
                breakout_result = self._detect_momentum_signal(weekly_df, ticker)
                
                if breakout_result['detected']:
                    sector = self.sector_map.get(ticker, 'Unknown')
                    
                    result = {
                        'symbol': ticker,
                        'sector': sector,
                        'rs_score': rs_score,
                        'close_price': breakout_result['close_price'],
                        'sma_30': breakout_result['sma_30'],
                        'volume_ratio': breakout_result['volume_ratio'],
                        'relative_strength': breakout_result['relative_strength'],
                        'above_30w_ma': breakout_result['above_30w_ma'],
                        'ma_trending_up': breakout_result['ma_trending_up'],
                        'volume_surge': breakout_result['volume_surge'],
                        'breakout_occurred': breakout_result['breakout_occurred'],
                        'positive_rs': breakout_result['positive_rs'],
                        'sustained_breakout': breakout_result['sustained_breakout'],
                        'entry_type': breakout_result['entry_type'],
                        'obv_rising': breakout_result['obv_rising'],
                        'healthy_pattern': breakout_result['healthy_pattern'],
                        'resistance_level': breakout_result['resistance_level'],
                        'market_favorable': market_favorable,
                        'date': breakout_result['pattern_formation_date'],  # 실제 패턴 형성 시점
                        'screening_date': self.today.strftime('%Y-%m-%d')  # 스크리닝 실행 시점 (참고용)
                    }
                    
                    logger.info(f"Stage 2 breakout 감지: {ticker} (RS: {rs_score}, 섹터: {sector}, 타입: {breakout_result['entry_type']}, OBV상승: {breakout_result['obv_rising']})")
                    return result
                    
                return None
                    
            except Exception as e:
                logger.error(f"{ticker} 분석 중 오류: {e}")
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
                    logger.error(f"{file_name} 결과 처리 중 오류: {e}")
        
        # 결과 병합 (메인 스레드에서 안전하게 처리)
        results.extend(all_results)
        
        # 결과 DataFrame 생성
        if results:
            results_df = pd.DataFrame(results)
            
            # RS 점수 기준으로 내림차순 정렬
            results_df = results_df.sort_values('rs_score', ascending=False)
            
            logger.info(f"Stage 2 breakout 스크리닝 완료: {len(results_df)}개 종목 발견")
            
            # 상위 10개 종목 출력
            if len(results_df) > 0:
                top_10 = results_df.head(10)
                logger.info("상위 10개 Stage 2 breakout 종목:")
                for _, row in top_10.iterrows():
                    logger.info(f"  {row['symbol']}: RS {row['rs_score']:.1f}, 가격 {row['close_price']:.2f}, 섹터 {row['sector']}")
        else:
            results_df = pd.DataFrame()
            logger.info("Stage 2 breakout 조건을 만족하는 종목이 없습니다.")
        
        return results_df


def run_momentum_signals_screening(skip_data=False) -> pd.DataFrame:
    """Momentum signals 스크리닝 실행 함수"""
    screener = StanWeinsteinStage2Screener(skip_data=skip_data)
    results_df = screener.screen_momentum_signals()
    
    if not results_df.empty:
        # DataFrame을 딕셔너리 리스트로 변환
        results_list = results_df.to_dict('records')
        
        # 결과 저장 (날짜만 포함한 파일명 사용)
        results_paths = save_screening_results(
            results=results_list,
            output_dir=MOMENTUM_SIGNALS_RESULTS_DIR,
            filename_prefix="momentum_signals",
            include_timestamp=True,
            incremental_update=True
        )
        
        logger.info(f"결과 저장 완료: {results_paths['csv_path']}")
        
        # 새로운 티커 추적
        tracker_file = os.path.join(MOMENTUM_SIGNALS_RESULTS_DIR, "new_momentum_tickers.csv")
        new_tickers = track_new_tickers(
            current_results=results_list,
            tracker_file=tracker_file,
            symbol_key='symbol',
            retention_days=14
        )
        
        # 요약 정보 생성
        summary = create_screener_summary(
            screener_name="Momentum Signals",
            total_candidates=len(results_list),
            new_tickers=len(new_tickers),
            results_paths=results_paths
        )
        
        print(f"✅ 모멘텀 시그널 스크리닝 완료: {len(results_list)}개 종목, 신규 {len(new_tickers)}개")
    
    return results_df


if __name__ == "__main__":
    # 테스트 실행
    results = run_momentum_signals_screening()
    print(f"\n스크리닝 완료: {len(results)}개 종목 발견")


