# -*- coding: utf-8 -*-
"""IPO 투자 전략 (IPO Investment) 스크리너"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from config import DATA_US_DIR, RESULTS_DIR
from utils.calc_utils import get_us_market_today
from utils.io_utils import ensure_dir, extract_ticker_from_filename
from .indicators import (
    calculate_base_pattern,
    calculate_macd,
    calculate_stochastic,
    calculate_track2_indicators,
)

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

        # 시장 지표 및 섹터 RS 계산
        self.vix = self._get_vix()
        sector_etfs = {
            'Technology': 'XLK',
            'Healthcare': 'XLV',
            'Consumer Discretionary': 'XLY',
            'Financials': 'XLF',
            'Communication Services': 'XLC',
            'Industrials': 'XLI',
            'Consumer Staples': 'XLP',
            'Energy': 'XLE',
            'Utilities': 'XLU',
            'Real Estate': 'XLRE',
            'Materials': 'XLB'
        }
        self.sector_rs = self._calculate_sector_rs(sector_etfs)
    
    def _load_ipo_data(self):
        """IPO 데이터 로드"""
        dataset_path = os.path.join(DATA_US_DIR, 'ipo_fundamentals.csv')

        if os.path.exists(dataset_path):
            ipo_data = pd.read_csv(dataset_path)
            ipo_data['ipo_date'] = pd.to_datetime(ipo_data['ipo_date'])
            return ipo_data

        logger.warning(f"IPO fundamentals dataset not found: {dataset_path}")
        return pd.DataFrame({
            'ticker': [],
            'ipo_date': [],
            'ipo_price': [],
            'sector': [],
            'industry': [],
            'market_cap': [],
            'float': [],
            'revenue_growth': [],
            'ps_ratio': [],
            'industry_ps_ratio': [],
            'equity_ratio': [],
            'cash_to_sales': [],
            'institutional_buy_streak': []
        })

    def _get_vix(self):
        """VIX 지수의 최신 값을 반환"""
        vix_path = os.path.join(DATA_US_DIR, 'VIX.csv')
        if os.path.exists(vix_path):
            try:
                vix = pd.read_csv(vix_path)
                vix['date'] = pd.to_datetime(vix['date'])
                vix = vix.sort_values('date')
                if not vix.empty:
                    return float(vix.iloc[-1]['close'])
            except Exception as e:
                logger.error(f"VIX 데이터 로드 오류: {e}")
        return 20.0

    def _calculate_sector_rs(self, sector_etfs):
        """섹터별 상대 강도(RS) 계산"""
        sector_rs = {}
        try:
            sp500_path = os.path.join(DATA_US_DIR, 'SPY.csv')
            if not os.path.exists(sp500_path):
                return {}

            sp500 = pd.read_csv(sp500_path)
            sp500['date'] = pd.to_datetime(sp500['date'])
            sp500 = sp500.sort_values('date')

            last_date = sp500['date'].max()
            three_months_ago = last_date - timedelta(days=90)
            sp500_3m = sp500[sp500['date'] >= three_months_ago]
            if sp500_3m.empty or len(sp500_3m) < 2:
                return {}

            sp500_return = (sp500_3m['close'].iloc[-1] / sp500_3m['close'].iloc[0] - 1) * 100

            for sector, etf in sector_etfs.items():
                etf_path = os.path.join(DATA_US_DIR, f"{etf}.csv")
                if not os.path.exists(etf_path):
                    continue

                etf_data = pd.read_csv(etf_path)
                etf_data['date'] = pd.to_datetime(etf_data['date'])
                etf_data = etf_data.sort_values('date')
                etf_3m = etf_data[etf_data['date'] >= three_months_ago]
                if etf_3m.empty or len(etf_3m) < 2:
                    continue

                etf_return = (etf_3m['close'].iloc[-1] / etf_3m['close'].iloc[0] - 1) * 100
                if sp500_return > 0:
                    rs_score = (etf_return / sp500_return) * 100
                else:
                    rs_score = (1 - (etf_return / sp500_return)) * 100 if etf_return < 0 else 100

                sector_rs[sector] = {'rs_score': rs_score}

            if sector_rs:
                rs_scores = [v['rs_score'] for v in sector_rs.values()]
                for sector in sector_rs:
                    percentile = np.percentile(rs_scores,
                                               np.searchsorted(np.sort(rs_scores), sector_rs[sector]['rs_score']) / len(rs_scores) * 100)
                    sector_rs[sector]['percentile'] = percentile
            return sector_rs
        except Exception as e:
            logger.error(f"섹터 RS 계산 오류: {e}")
            return {}
    
    def _get_recent_ipos(self, days: int = 365) -> pd.DataFrame:
        """최근 IPO 종목을 ipo_data에서 필터링"""
        if self.ipo_data.empty:
            return pd.DataFrame()

        cutoff_date = self.today - timedelta(days=days)
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
        institutional_buy = ipo_info.get('institutional_buy_streak', 0) >= 3

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

                # Track1/Track2 조건 확인
                track1_pass, track1_info = self.check_track1(ticker, df)
                track2_pass, track2_info = self.check_track2(ticker, df)
                
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

                if track1_pass:
                    t1 = {
                        'ticker': ticker,
                        'ipo_date': ipo['ipo_date'],
                        'track': 'track1',
                        'days_since_ipo': ipo['days_since_ipo'],
                        'current_price': track1_info['current_price'],
                        'date': self.today.strftime('%Y-%m-%d')
                    }
                    t1.update(track1_info)
                    track1_results.append(t1)

                if track2_pass:
                    t2 = {
                        'ticker': ticker,
                        'ipo_date': ipo['ipo_date'],
                        'track': 'track2',
                        'days_since_ipo': ipo['days_since_ipo'],
                        'current_price': track2_info['current_price'],
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

        if not track1_df.empty:
            track1_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR,
                                             f"ipo_track1_{self.today.strftime('%Y%m%d')}.csv")
            track1_df.to_csv(track1_output_file, index=False)
            logger.info(f"Track1 결과 저장 완료: {track1_output_file} ({len(track1_df)}개 종목)")
        else:
            logger.info("Track1 조건을 만족하는 종목이 없습니다.")

        if not track2_df.empty:
            track2_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR,
                                             f"ipo_track2_{self.today.strftime('%Y%m%d')}.csv")
            track2_df.to_csv(track2_output_file, index=False)
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


if __name__ == "__main__":
    run_ipo_investment_screening()
