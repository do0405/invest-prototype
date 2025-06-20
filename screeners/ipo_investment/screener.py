# -*- coding: utf-8 -*-
"""IPO 투자 전략 (IPO Investment) 스크리너"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yfinance as yf
import logging
from typing import Dict, List, Tuple, Optional
import os
import sys

# 로컬 데이터 매니저 모듈 추가
from .data_manager import DataManager

from config import (
    IPO_INVESTMENT_RESULTS_DIR
)
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.calc_utils import get_us_market_today
from .indicators import (
    calculate_base_pattern,
    calculate_macd,
    calculate_stochastic,
    calculate_track2_indicators
)
from utils.market_utils import (
    get_vix_value,
    calculate_sector_rs,
    SECTOR_ETFS,
)


# 결과 저장 디렉토리는 config에서 제공됨



# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class IPOInvestmentScreener:
    """IPO 투자 전략 스크리너 클래스"""

    def __init__(self):
        """IPO Investment 스크리너 초기화"""
        self.logger = logging.getLogger(__name__)
        self.today = get_us_market_today()
        
        # 데이터 매니저 초기화
        self.data_manager = DataManager()
        
        # IPO 데이터 로드 (실제 외부 데이터 소스 연동)
        self.ipo_data = self._load_ipo_data()
        
        # VIX 계산 및 섹터 상대강도 계산
        self.vix = get_vix_value()
        self.sector_rs = calculate_sector_rs(SECTOR_ETFS)
    
    def _load_ipo_data(self):
        """IPO 데이터 로드 (실제 외부 데이터 소스 연동)"""
        try:
            # 데이터 매니저를 통해 실제 IPO 데이터 수집
            ipo_data = self.data_manager.get_ipo_data(days_back=365)

            if not ipo_data.empty:
                ipo_data['ipo_date'] = pd.to_datetime(ipo_data['ipo_date'], errors='coerce')
                self.logger.info(f"IPO 데이터 로드 완료: {len(ipo_data)}개 종목")
                return ipo_data
            else:
                self.logger.warning("IPO 데이터가 비어있음")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"IPO 데이터 로드 중 오류: {e}")
            return pd.DataFrame()
    
    def _get_recent_ipos(self, days: int = 365) -> pd.DataFrame:
        """최근 IPO 종목을 ipo_data에서 필터링"""
        if self.ipo_data.empty:
            return pd.DataFrame()

        cutoff_date = pd.Timestamp(self.today - timedelta(days=days))
        recent = self.ipo_data[self.ipo_data['ipo_date'] >= cutoff_date].copy()
        if recent.empty:
            return pd.DataFrame()


        # 상장가와 섹터 정보가 없는 경우 제외
        recent.dropna(subset=['ipo_price', 'sector'], inplace=True)
        recent = recent[recent['ipo_price'] > 0]
        if recent.empty:
            return pd.DataFrame()
        recent['days_since_ipo'] = (self.today - recent['ipo_date']).dt.days
        return recent
    
    
    def _check_ipo_base_pattern(self, df, min_days=30, max_days=200):
        """IPO 베이스 패턴 확인"""
        # 데이터가 충분하지 않은 경우
        if len(df) < min_days:
            return False, {}
            
        # 너무 오래된 IPO인 경우
        if len(df) > max_days:
            return False, {}
        
        # 기술적 지표 계산
        df = calculate_base_pattern(df)
        
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
        df = calculate_base_pattern(df)
        
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

    def check_track1(self, ticker, df):
        """Track 1 조건 확인"""
        ipo_row = self.ipo_data[self.ipo_data['ticker'] == ticker]
        if ipo_row.empty:
            return False, {}

        ipo_info = ipo_row.iloc[0]
        df = calculate_base_pattern(df)
        recent = df.iloc[-1]

        price_cond = ipo_info['ipo_price'] * 0.7 <= recent['close'] <= ipo_info['ipo_price'] * 0.9
        rsi_cond = recent['rsi_14'] < 30
        support_touch = recent['close'] <= recent['rolling_low'] * 1.02
        volume_cond = recent['volume'] < recent['volume_sma_20'] * 0.5

        sector_rs = self.sector_rs.get(ipo_info['sector'], {}).get('percentile', 0)
        environment_cond = self.vix < 25 and sector_rs >= 50

        fundamental_cond = (
            ipo_info.get('ps_ratio', np.inf) < ipo_info.get('industry_ps_ratio', np.inf) and
            ipo_info.get('revenue_growth', 0) > 20 and
            ipo_info.get('equity_ratio', 0) > 30 and
            ipo_info.get('cash_to_sales', 0) > 15
        )

        info = {
            'price_cond': price_cond,
            'rsi_cond': rsi_cond,
            'support_touch': support_touch,
            'volume_cond': volume_cond,
            'environment_cond': environment_cond,
            'fundamental_cond': fundamental_cond,
            'sector_rs': sector_rs,
            'vix': self.vix,
            'current_price': recent['close'],
            'ipo_price': ipo_info['ipo_price']
        }

        return all([price_cond, rsi_cond and support_touch, volume_cond, environment_cond, fundamental_cond]), info

    def check_track2(self, ticker, df):
        """Track 2 조건 확인"""
        ipo_row = self.ipo_data[self.ipo_data['ticker'] == ticker]
        if ipo_row.empty:
            return False, {}

        ipo_info = ipo_row.iloc[0]
        df = calculate_track2_indicators(df)
        recent = df.iloc[-1]

        price_momentum = len(df) >= 5 and (df['close'].iloc[4] / df['close'].iloc[0] - 1) >= 0.20
        macd_signal = recent['macd'] > recent['macd_signal']
        volume_surge = recent['volume'] >= recent['volume_sma_20'] * 3
        institutional_buy = self.data_manager.check_institutional_buying_streak(ticker, min_days=3)

        ema_break = df.iloc[-2]['close'] > df.iloc[-2]['ema_5'] and recent['close'] > recent['ema_5']
        rsi_strong = recent['rsi_7'] > 70
        stoch_cond = recent['stoch_k'] > recent['stoch_d'] and recent['stoch_k'] > 80
        roc_cond = recent['roc_5'] > 15

        sector_rs = self.sector_rs.get(ipo_info['sector'], {}).get('percentile', 0)
        environment_cond = self.vix < 25 and sector_rs >= 50

        info = {
            'price_momentum': price_momentum,
            'macd_signal': macd_signal,
            'volume_surge': volume_surge,
            'institutional_buy': institutional_buy,
            'ema_break': ema_break,
            'rsi_strong': rsi_strong,
            'stoch_cond': stoch_cond,
            'roc_cond': roc_cond,
            'environment_cond': environment_cond,
            'sector_rs': sector_rs,
            'vix': self.vix,
            'current_price': recent['close']
        }

        return all([price_momentum, macd_signal, volume_surge, institutional_buy,
                    ema_break, rsi_strong, stoch_cond, roc_cond, environment_cond]), info
    
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
        track1_results = []
        track2_results = []
        
        # 각 IPO 종목 분석
        for _, ipo in recent_ipos.iterrows():
            try:
                # 컬럼명 통일 (symbol 또는 ticker)
                ticker = ipo.get('ticker', ipo.get('symbol', ''))
                if not ticker:
                    continue
                    
                # 주가 데이터 로드 (yfinance 사용)
                data = yf.download(ticker, period="6mo", interval="1d")
                if data.empty:
                    continue
                
                # 데이터프레임 형식 변환
                df = data.reset_index()
                df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
                df = df.sort_values('date')
                
                # IPO 분석 데이터 가져오기
                ipo_analysis = self.data_manager.get_ipo_analysis(ticker)
                
                # 베이스 패턴 확인
                has_base, base_info = self._check_ipo_base_pattern(df)

                # 브레이크아웃 확인
                has_breakout, breakout_info = self._check_ipo_breakout(df)

                # Track1/Track2 조건 확인
                track1_pass, track1_info = self.check_track1(ticker, df)
                track2_pass, track2_info = self.check_track2(ticker, df)
                
                # 결과 저장
                if has_base:
                    base_result = {
                        'ticker': ticker,
                        'company_name': ipo.get('company_name', 'Unknown'),
                        'ipo_date': ipo['ipo_date'],
                        'ipo_price': ipo['ipo_price'],
                        'days_since_ipo': ipo['days_since_ipo'],
                        'pattern_type': 'base',
                        'current_price': base_info['current_price'],
                        'score': base_info['base_score'],
                        'institutional_interest': ipo_analysis.get('institutional_interest', {}).get('total_institutions', 0),
                        'date': self.today.strftime('%Y-%m-%d')
                    }
                    base_result.update(base_info)
                    base_results.append(base_result)
                
                if has_breakout:
                    breakout_result = {
                        'ticker': ticker,
                        'company_name': ipo.get('company_name', 'Unknown'),
                        'ipo_date': ipo['ipo_date'],
                        'ipo_price': ipo['ipo_price'],
                        'days_since_ipo': ipo['days_since_ipo'],
                        'pattern_type': 'breakout',
                        'current_price': breakout_info['current_price'],
                        'score': breakout_info['breakout_score'],
                        'institutional_flow': ipo_analysis.get('institutional_interest', {}).get('recent_flow', 0),
                        'date': self.today.strftime('%Y-%m-%d')
                    }
                    breakout_result.update(breakout_info)
                    breakout_results.append(breakout_result)

                if track1_pass:
                    t1 = {
                        'ticker': ticker,
                        'company_name': ipo.get('company_name', 'Unknown'),
                        'ipo_date': ipo['ipo_date'],
                        'ipo_price': ipo['ipo_price'],
                        'track': 'track1',
                        'days_since_ipo': ipo['days_since_ipo'],
                        'current_price': track1_info['current_price'],
                        'price_vs_ipo': (track1_info['current_price'] / ipo['ipo_price'] - 1) * 100,
                        'institutional_ownership': ipo_analysis.get('institutional_interest', {}).get('institutional_ownership', 0),
                        'date': self.today.strftime('%Y-%m-%d')
                    }
                    t1.update(track1_info)
                    track1_results.append(t1)

                if track2_pass:
                    t2 = {
                        'ticker': ticker,
                        'company_name': ipo.get('company_name', 'Unknown'),
                        'ipo_date': ipo['ipo_date'],
                        'ipo_price': ipo['ipo_price'],
                        'track': 'track2',
                        'days_since_ipo': ipo['days_since_ipo'],
                        'current_price': track2_info['current_price'],
                        'price_vs_ipo': (track2_info['current_price'] / ipo['ipo_price'] - 1) * 100,
                        'institutional_buying_streak': self.data_manager.check_institutional_buying_streak(ticker, min_days=3),
                        'date': self.today.strftime('%Y-%m-%d')
                    }
                    t2.update(track2_info)
                    track2_results.append(t2)
            
            except Exception as e:
                logger.error(f"{ticker} 분석 중 오류 발생: {e}")
                continue
        
        # 결과를 데이터프레임으로 변환
        base_df = pd.DataFrame(base_results) if base_results else pd.DataFrame()
        breakout_df = pd.DataFrame(breakout_results) if breakout_results else pd.DataFrame()
        track1_df = pd.DataFrame(track1_results) if track1_results else pd.DataFrame()
        track2_df = pd.DataFrame(track2_results) if track2_results else pd.DataFrame()
        
        # 결과 저장
        if not base_df.empty:
            base_df = base_df.sort_values('score', ascending=False)
            base_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR, 
                                           f"ipo_base_{self.today.strftime('%Y%m%d')}.csv")
            base_df.to_csv(base_output_file, index=False)
            base_df.to_json(base_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"IPO 베이스 패턴 결과 저장 완료: {base_output_file} ({len(base_df)}개 종목)")
        else:
            logger.info("IPO 베이스 패턴 조건을 만족하는 종목이 없습니다.")
        
        if not breakout_df.empty:
            breakout_df = breakout_df.sort_values('score', ascending=False)
            breakout_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR,
                                              f"ipo_breakout_{self.today.strftime('%Y%m%d')}.csv")
            breakout_df.to_csv(breakout_output_file, index=False)
            breakout_df.to_json(breakout_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"IPO 브레이크아웃 결과 저장 완료: {breakout_output_file} ({len(breakout_df)}개 종목)")
        else:
            logger.info("IPO 브레이크아웃 조건을 만족하는 종목이 없습니다.")

        if not track1_df.empty:
            track1_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR,
                                             f"ipo_track1_{self.today.strftime('%Y%m%d')}.csv")
            track1_df.to_csv(track1_output_file, index=False)
            track1_df.to_json(track1_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"Track1 결과 저장 완료: {track1_output_file} ({len(track1_df)}개 종목)")
        else:
            logger.info("Track1 조건을 만족하는 종목이 없습니다.")

        if not track2_df.empty:
            track2_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR,
                                             f"ipo_track2_{self.today.strftime('%Y%m%d')}.csv")
            track2_df.to_csv(track2_output_file, index=False)
            track2_df.to_json(track2_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"Track2 결과 저장 완료: {track2_output_file} ({len(track2_df)}개 종목)")
        else:
            logger.info("Track2 조건을 만족하는 종목이 없습니다.")

        # 통합 결과
        dfs = [base_df, breakout_df, track1_df, track2_df]
        dfs = [d for d in dfs if not d.empty]
        combined_results = pd.concat(dfs) if dfs else pd.DataFrame()
        
        return combined_results


def run_ipo_investment_screening():
    """IPO 투자 전략 스크리닝 실행 함수"""
    screener = IPOInvestmentScreener()
    return screener.screen_ipo_investments()


