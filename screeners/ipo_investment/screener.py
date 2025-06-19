# -*- coding: utf-8 -*-
"""IPO 투자 전략 (IPO Investment) 스크리너"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from config import DATA_US_DIR, RESULTS_DIR
from utils.calc_utils import get_us_market_today, calculate_rsi, calculate_atr
from utils.io_utils import ensure_dir, extract_ticker_from_filename

# 결과 저장 디렉토리
IPO_INVESTMENT_RESULTS_DIR = os.path.join(RESULTS_DIR, 'ipo_investment')

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class IPOInvestmentScreener:
    """IPO 투자 전략 스크리너 클래스"""
    
    def __init__(self):
        """초기화"""
        self.today = get_us_market_today()
        ensure_dir(IPO_INVESTMENT_RESULTS_DIR)
        
        # IPO 데이터 로드 (실제로는 외부 API나 데이터베이스에서 가져와야 함)
        # 여기서는 예시로 빈 데이터프레임 생성
        self.ipo_data = self._load_ipo_data()
    
    def _load_ipo_data(self):
        """IPO 데이터 로드 (예시)"""
        # 실제로는 IPO 데이터를 외부 소스에서 가져와야 함
        # 여기서는 예시로 빈 데이터프레임 생성
        ipo_data = pd.DataFrame({
            'ticker': [],
            'ipo_date': [],
            'ipo_price': [],
            'sector': [],
            'industry': [],
            'market_cap': [],
            'float': [],
            'revenue_growth': [],
            'profitable': []
        })
        return ipo_data
    
    def _get_recent_ipos(self, days=365):
        """최근 IPO 종목 가져오기 (예시)"""
        # 실제로는 IPO 데이터에서 최근 IPO 종목 필터링
        # 여기서는 예시로 모든 주식 파일을 검사하여 데이터 길이로 추정
        
        recent_ipos = []
        cutoff_date = self.today - timedelta(days=days)
        
        # 모든 주식 파일 가져오기
        stock_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        
        for file in stock_files:
            try:
                file_path = os.path.join(DATA_US_DIR, file)
                ticker = extract_ticker_from_filename(file)
                
                # 주요 지수 제외
                if ticker in ['SPY', 'QQQ', 'DIA', 'IWM']:
                    continue
                
                # 데이터 로드
                df = pd.read_csv(file_path)
                if df.empty:
                    continue
                    
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                
                # 데이터 시작일이 cutoff_date 이후인 경우 최근 IPO로 간주
                first_date = df['date'].min()
                if first_date >= cutoff_date:
                    # IPO 정보 추정 (실제로는 정확한 정보 필요)
                    ipo_info = {
                        'ticker': ticker,
                        'ipo_date': first_date,
                        'first_price': df.iloc[0]['close'],
                        'current_price': df.iloc[-1]['close'],
                        'days_since_ipo': (self.today - first_date).days,
                        'data_length': len(df)
                    }
                    recent_ipos.append(ipo_info)
            
            except Exception as e:
                logger.error(f"{ticker} IPO 정보 분석 중 오류 발생: {e}")
                continue
        
        return pd.DataFrame(recent_ipos) if recent_ipos else pd.DataFrame()
    
    def _calculate_base_pattern(self, df):
        """IPO 베이스 패턴 계산"""
        # 이동평균 계산
        df['sma_10'] = df['close'].rolling(window=10).mean()
        df['sma_20'] = df['close'].rolling(window=20).mean()
        df['sma_50'] = df['close'].rolling(window=50).mean()
        
        # 볼린저 밴드 계산
        df['std_20'] = df['close'].rolling(window=20).std()
        df['upper_band'] = df['sma_20'] + (df['std_20'] * 2)
        df['lower_band'] = df['sma_20'] - (df['std_20'] * 2)
        
        # ATR 계산
        df = calculate_atr(df)
        
        # RSI 계산
        df = calculate_rsi(df)
        
        # 거래량 지표
        df['volume_sma_20'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']
        
        # 고점과 저점 식별
        df['rolling_high'] = df['high'].rolling(window=20).max()
        df['rolling_low'] = df['low'].rolling(window=20).min()
        
        # 베이스 형성 여부 확인 (20일 동안 가격 변동이 작고 횡보하는 패턴)
        df['price_range'] = (df['rolling_high'] / df['rolling_low'] - 1) * 100
        
        return df
    
    def _check_ipo_base_pattern(self, df, min_days=30, max_days=200):
        """IPO 베이스 패턴 확인"""
        # 데이터가 충분하지 않은 경우
        if len(df) < min_days:
            return False, {}
            
        # 너무 오래된 IPO인 경우
        if len(df) > max_days:
            return False, {}
        
        # 기술적 지표 계산
        df = self._calculate_base_pattern(df)
        
        # 최근 데이터
        recent = df.iloc[-1]
        
        # 베이스 패턴 조건 확인
        # 1. 최근 20일 동안 가격 변동이 15% 이내 (베이스 형성)
        recent_range = df.iloc[-20:]['price_range'].max()
        base_formed = recent_range <= 15
        
        # 2. 거래량 감소 (베이스 형성 중 거래량 감소)
        volume_declining = df.iloc[-20:]['volume'].mean() < df.iloc[-40:-20]['volume'].mean()
        
        # 3. 종가가 20일 이동평균선 위에 있음
        above_20_sma = recent['close'] > recent['sma_20']
        
        # 4. RSI가 과매도 구간에서 벗어남 (RSI > 40)
        healthy_rsi = recent['rsi_14'] > 40
        
        # 5. 최근 가격이 IPO 이후 고점의 70% 이상
        ipo_high = df['high'].max()
        price_strength = recent['close'] >= ipo_high * 0.7
        
        # 베이스 패턴 점수 계산
        base_score = sum([base_formed, volume_declining, above_20_sma, healthy_rsi, price_strength])
        
        # 베이스 패턴 정보
        base_info = {
            'base_score': base_score,
            'base_formed': base_formed,
            'volume_declining': volume_declining,
            'above_20_sma': above_20_sma,
            'healthy_rsi': healthy_rsi,
            'price_strength': price_strength,
            'recent_range': recent_range,
            'ipo_high': ipo_high,
            'current_price': recent['close'],
            'price_to_high_ratio': recent['close'] / ipo_high
        }
        
        # 베이스 패턴 확인 (점수가 3점 이상이면 베이스 패턴으로 간주)
        return base_score >= 3, base_info
    
    def _check_ipo_breakout(self, df):
        """IPO 브레이크아웃 확인"""
        # 데이터가 충분하지 않은 경우
        if len(df) < 30:
            return False, {}
        
        # 기술적 지표 계산
        df = self._calculate_base_pattern(df)
        
        # 최근 데이터
        recent = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 브레이크아웃 조건 확인
        # 1. 종가가 20일 고점 돌파
        breakout_20d_high = recent['close'] > df.iloc[-21:-1]['high'].max()
        
        # 2. 거래량 급증 (20일 평균 대비 2배 이상)
        volume_surge = recent['volume_ratio'] >= 2.0
        
        # 3. 종가가 상단 밴드 돌파
        breakout_upper_band = recent['close'] > recent['upper_band']
        
        # 4. 당일 가격 상승률 2% 이상
        daily_gain = (recent['close'] / prev['close'] - 1) * 100 >= 2
        
        # 5. RSI가 50 이상 (상승 모멘텀)
        strong_rsi = recent['rsi_14'] >= 50
        
        # 브레이크아웃 점수 계산
        breakout_score = sum([breakout_20d_high, volume_surge, breakout_upper_band, daily_gain, strong_rsi])
        
        # 브레이크아웃 정보
        breakout_info = {
            'breakout_score': breakout_score,
            'breakout_20d_high': breakout_20d_high,
            'volume_surge': volume_surge,
            'breakout_upper_band': breakout_upper_band,
            'daily_gain': daily_gain,
            'strong_rsi': strong_rsi,
            'current_price': recent['close'],
            'volume_ratio': recent['volume_ratio'],
            'rsi': recent['rsi_14']
        }
        
        # 브레이크아웃 확인 (점수가 3점 이상이면 브레이크아웃으로 간주)
        return breakout_score >= 3, breakout_info
    
    def screen_ipo_investments(self):
        """IPO 투자 전략 스크리닝 실행"""
        logger.info("IPO 투자 전략 스크리닝 시작...")
        
        # 최근 IPO 종목 가져오기
        recent_ipos = self._get_recent_ipos(days=365)
        if recent_ipos.empty:
            logger.info("최근 1년 내 IPO 종목이 없습니다.")
            return pd.DataFrame()
            
        logger.info(f"최근 1년 내 IPO 종목 수: {len(recent_ipos)}")
        
        # 결과 저장용 데이터프레임
        base_results = []
        breakout_results = []
        
        # 각 IPO 종목 분석
        for _, ipo in recent_ipos.iterrows():
            try:
                ticker = ipo['ticker']
                file_path = os.path.join(DATA_US_DIR, f"{ticker}.csv")
                
                # 데이터 로드
                df = pd.read_csv(file_path)
                if df.empty:
                    continue
                    
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                
                # 베이스 패턴 확인
                has_base, base_info = self._check_ipo_base_pattern(df)
                
                # 브레이크아웃 확인
                has_breakout, breakout_info = self._check_ipo_breakout(df)
                
                # 결과 저장
                if has_base:
                    base_result = {
                        'ticker': ticker,
                        'ipo_date': ipo['ipo_date'],
                        'days_since_ipo': ipo['days_since_ipo'],
                        'pattern_type': 'base',
                        'current_price': base_info['current_price'],
                        'score': base_info['base_score'],
                        'date': self.today.strftime('%Y-%m-%d')
                    }
                    base_result.update(base_info)
                    base_results.append(base_result)
                
                if has_breakout:
                    breakout_result = {
                        'ticker': ticker,
                        'ipo_date': ipo['ipo_date'],
                        'days_since_ipo': ipo['days_since_ipo'],
                        'pattern_type': 'breakout',
                        'current_price': breakout_info['current_price'],
                        'score': breakout_info['breakout_score'],
                        'date': self.today.strftime('%Y-%m-%d')
                    }
                    breakout_result.update(breakout_info)
                    breakout_results.append(breakout_result)
            
            except Exception as e:
                logger.error(f"{ticker} 분석 중 오류 발생: {e}")
                continue
        
        # 결과를 데이터프레임으로 변환
        base_df = pd.DataFrame(base_results) if base_results else pd.DataFrame()
        breakout_df = pd.DataFrame(breakout_results) if breakout_results else pd.DataFrame()
        
        # 결과 저장
        if not base_df.empty:
            base_df = base_df.sort_values('score', ascending=False)
            base_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR, 
                                           f"ipo_base_{self.today.strftime('%Y%m%d')}.csv")
            base_df.to_csv(base_output_file, index=False)
            logger.info(f"IPO 베이스 패턴 결과 저장 완료: {base_output_file} ({len(base_df)}개 종목)")
        else:
            logger.info("IPO 베이스 패턴 조건을 만족하는 종목이 없습니다.")
        
        if not breakout_df.empty:
            breakout_df = breakout_df.sort_values('score', ascending=False)
            breakout_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR, 
                                              f"ipo_breakout_{self.today.strftime('%Y%m%d')}.csv")
            breakout_df.to_csv(breakout_output_file, index=False)
            logger.info(f"IPO 브레이크아웃 결과 저장 완료: {breakout_output_file} ({len(breakout_df)}개 종목)")
        else:
            logger.info("IPO 브레이크아웃 조건을 만족하는 종목이 없습니다.")
        
        # 통합 결과
        combined_results = pd.concat([base_df, breakout_df]) if not (base_df.empty and breakout_df.empty) else pd.DataFrame()
        
        return combined_results


def run_ipo_investment_screening():
    """IPO 투자 전략 스크리닝 실행 함수"""
    screener = IPOInvestmentScreener()
    return screener.screen_ipo_investments()


if __name__ == "__main__":
    run_ipo_investment_screening()