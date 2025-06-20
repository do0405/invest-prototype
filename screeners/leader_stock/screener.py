# -*- coding: utf-8 -*-
"""주도주 투자 전략 (Leader Stock Investment) 스크리너"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from config import (
    DATA_US_DIR,
    RESULTS_DIR,
    US_WITH_RS_PATH,
    STOCK_METADATA_PATH,
)
from utils.calc_utils import (
    get_us_market_today,
    calculate_rsi,
    check_sp500_condition,
)
from utils.io_utils import ensure_dir, extract_ticker_from_filename

# 결과 저장 디렉토리
LEADER_STOCK_RESULTS_DIR = os.path.join(RESULTS_DIR, 'leader_stock')

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LeaderStockScreener:
    """주도주 투자 전략 스크리너 클래스"""
    
    def __init__(self):
        """초기화"""
        self.today = get_us_market_today()
        ensure_dir(LEADER_STOCK_RESULTS_DIR)
        self.market_stage = self._determine_market_stage()
        self._load_metadata()
        
    def _determine_market_stage(self):
        """시장 단계 결정 (Stage 1-4)"""
        # S&P 500 데이터 로드
        try:
            sp500_path = os.path.join(DATA_US_DIR, 'SPY.csv')
            if not os.path.exists(sp500_path):
                logger.error(f"S&P 500 데이터 파일을 찾을 수 없습니다: {sp500_path}")
                return "unknown"
                
            sp500 = pd.read_csv(sp500_path)
            sp500['date'] = pd.to_datetime(sp500['date'])
            sp500 = sp500.sort_values('date')
            
            # 30주 이동평균 계산 (약 150 거래일)
            sp500['sma_30w'] = sp500['close'].rolling(window=150).mean()
            
            # 10주 이동평균 계산 (약 50 거래일)
            sp500['sma_10w'] = sp500['close'].rolling(window=50).mean()
            
            # RSI 계산
            sp500 = calculate_rsi(sp500)
            
            # 최근 데이터
            recent = sp500.iloc[-1]
            
            # VIX 데이터 로드 (변동성 지수)
            vix_path = os.path.join(DATA_US_DIR, 'VIX.csv')
            vix_value = 20  # 기본값
            
            if os.path.exists(vix_path):
                vix = pd.read_csv(vix_path)
                vix['date'] = pd.to_datetime(vix['date'])
                vix = vix.sort_values('date')
                if not vix.empty:
                    vix_value = vix.iloc[-1]['close']
            
            # 시장 단계 결정
            if recent['close'] > recent['sma_30w'] and recent['sma_10w'] > recent['sma_30w']:
                if vix_value < 15 and recent['rsi_14'] > 70:
                    return "stage3"  # 과열 구간
                elif vix_value < 25:
                    return "stage2"  # 본격 상승장
                else:
                    return "stage1"  # 공포 완화 시점
            elif recent['close'] < recent['sma_30w'] and recent['close'] < recent['sma_10w']:
                return "stage4"  # 어깨 구간 (하락장)
            else:
                return "stage1"  # 기본값으로 공포 완화 시점
                
        except Exception as e:
            logger.error(f"시장 단계 결정 중 오류 발생: {e}")
            return "unknown"

    def _load_metadata(self):
        """섹터 및 RS 메타데이터 로드"""
        self.sector_map = {}
        self.pe_map = {}
        self.revenue_growth_map = {}
        self.stock_rs_percentile = {}
        self.market_cap_map = {}
        self.ipo_date_map = {}

        # 섹터, P/E, 매출 성장률, 시가총액, IPO 날짜 메타데이터
        if os.path.exists(STOCK_METADATA_PATH):
            try:
                meta = pd.read_csv(STOCK_METADATA_PATH)
                if 'symbol' in meta.columns and 'sector' in meta.columns:
                    self.sector_map = meta.set_index('symbol')['sector'].to_dict()
                if 'pe_ratio' in meta.columns:
                    self.pe_map = meta.set_index('symbol')['pe_ratio'].to_dict()
                if 'revenue_growth' in meta.columns:
                    self.revenue_growth_map = meta.set_index('symbol')['revenue_growth'].to_dict()
                if 'market_cap' in meta.columns:
                    self.market_cap_map = meta.set_index('symbol')['market_cap'].to_dict()
                if 'ipo_date' in meta.columns:
                    meta['ipo_date'] = pd.to_datetime(meta['ipo_date'], errors='coerce')
                    self.ipo_date_map = meta.set_index('symbol')['ipo_date'].to_dict()
            except Exception as e:
                logger.warning(f"메타데이터 로드 실패: {e}")
        else:
            logger.warning(f"메타데이터 파일이 없습니다: {STOCK_METADATA_PATH}")

        # RS 점수 로드 및 섹터별 백분위 계산
        if os.path.exists(US_WITH_RS_PATH):
            try:
                rs_df = pd.read_csv(US_WITH_RS_PATH)
                if {'symbol', 'rs_score'}.issubset(rs_df.columns) and self.sector_map:
                    rs_df['sector'] = rs_df['symbol'].map(self.sector_map)
                    rs_df.dropna(subset=['sector'], inplace=True)
                    rs_df['sector_rank'] = rs_df.groupby('sector')['rs_score'].rank(pct=True) * 100
                    self.stock_rs_percentile = rs_df.set_index('symbol')['sector_rank'].to_dict()
            except Exception as e:
                logger.warning(f"RS 메타데이터 로드 실패: {e}")

    def _get_vix(self):
        """최근 VIX 값을 반환"""
        vix_path = os.path.join(DATA_US_DIR, 'VIX.csv')
        if os.path.exists(vix_path):
            try:
                vix = pd.read_csv(vix_path)
                vix['date'] = pd.to_datetime(vix['date'])
                vix = vix.sort_values('date')
                if not vix.empty:
                    return float(vix.iloc[-1]['close'])
            except Exception as e:
                logger.warning(f"VIX 데이터 로드 오류: {e}")
        return 20.0

    def _market_trend_ok(self):
        """SPY 200일 이동평균 및 VIX 조건 체크"""
        spy_ok = check_sp500_condition(DATA_US_DIR, ma_days=200)
        vix_value = self._get_vix()
        return spy_ok and vix_value < 30

    def _is_high_pe(self, ticker):
        pe = self.pe_map.get(ticker)
        return pe is not None and pe >= 40

    def _growth_slowdown(self, ticker):
        growth = self.revenue_growth_map.get(ticker)
        return growth is not None and growth < 15

    def _small_cap_or_recent_ipo(self, ticker):
        """소형주이거나 최근 IPO 여부 확인"""
        cap = self.market_cap_map.get(ticker)
        if cap is not None and cap < 2_000_000_000:
            return True
        ipo_date = self.ipo_date_map.get(ticker)
        if ipo_date is not None:
            return (self.today - ipo_date).days <= 5 * 365
        return False

    def _calculate_momentum_indicator(self, df):
        """간단한 모멘텀 지표 (RSI + 5일 수익률)"""
        if len(df) < 6:
            return 0.0
        rsi = df['rsi_14'].iloc[-1]
        roc5 = (df['close'].iloc[-1] / df['close'].iloc[-5] - 1) * 100
        score = rsi + roc5
        return max(min(score, 100), 0)
    
    def calculate_sector_rs(self, sector_etfs):
        """섹터별 상대 강도(RS) 계산"""
        sector_rs = {}
        
        try:
            # S&P 500 데이터 로드
            sp500_path = os.path.join(DATA_US_DIR, 'SPY.csv')
            if not os.path.exists(sp500_path):
                logger.error(f"S&P 500 데이터 파일을 찾을 수 없습니다: {sp500_path}")
                return {}
                
            sp500 = pd.read_csv(sp500_path)
            sp500['date'] = pd.to_datetime(sp500['date'])
            sp500 = sp500.sort_values('date')
            
            # 최근 3개월 S&P 500 성과
            last_date = sp500['date'].max()
            three_months_ago = last_date - timedelta(days=90)
            sp500_3m = sp500[sp500['date'] >= three_months_ago]
            
            if sp500_3m.empty or len(sp500_3m) < 2:
                logger.error("충분한 S&P 500 데이터가 없습니다.")
                return {}
                
            sp500_return = (sp500_3m['close'].iloc[-1] / sp500_3m['close'].iloc[0] - 1) * 100
            
            # 각 섹터 ETF의 상대 강도 계산
            for sector, etf in sector_etfs.items():
                etf_path = os.path.join(DATA_US_DIR, f"{etf}.csv")
                if not os.path.exists(etf_path):
                    logger.warning(f"{sector} 섹터 ETF 데이터를 찾을 수 없습니다: {etf_path}")
                    continue
                    
                etf_data = pd.read_csv(etf_path)
                etf_data['date'] = pd.to_datetime(etf_data['date'])
                etf_data = etf_data.sort_values('date')
                
                etf_3m = etf_data[etf_data['date'] >= three_months_ago]
                if etf_3m.empty or len(etf_3m) < 2:
                    logger.warning(f"{sector} 섹터 ETF의 충분한 데이터가 없습니다.")
                    continue
                    
                etf_return = (etf_3m['close'].iloc[-1] / etf_3m['close'].iloc[0] - 1) * 100
                
                # 상대 강도 = (섹터 수익률 / S&P 500 수익률) * 100
                # S&P 500 수익률이 음수인 경우 조정
                if sp500_return > 0:
                    rs_score = (etf_return / sp500_return) * 100
                else:
                    # S&P 500이 하락 중일 때는 덜 하락한 섹터가 더 강함
                    rs_score = (1 - (etf_return / sp500_return)) * 100 if etf_return < 0 else 100
                
                sector_rs[sector] = {
                    'etf': etf,
                    'rs_score': rs_score,
                    'return': etf_return
                }
            
            # 상대 강도 점수에 따라 백분위 계산
            rs_scores = [data['rs_score'] for data in sector_rs.values()]
            for sector in sector_rs:
                percentile = np.percentile(rs_scores, np.searchsorted(np.sort(rs_scores), sector_rs[sector]['rs_score']) / len(rs_scores) * 100)
                sector_rs[sector]['percentile'] = percentile
            
            return sector_rs
            
        except Exception as e:
            logger.error(f"섹터 상대 강도 계산 중 오류 발생: {e}")
            return {}
    
    def screen_leader_stocks(self):
        """주도주 스크리닝 실행"""
        logger.info("주도주 투자 전략 스크리닝 시작...")
        logger.info(f"현재 시장 단계: {self.market_stage}")
        
        # 섹터 ETF 정의
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
        
        # 섹터 상대 강도 계산
        sector_rs = self.calculate_sector_rs(sector_etfs)
        
        # 강한 섹터 선택 (상대 강도 점수 >= 70)
        strong_sectors = {sector: data for sector, data in sector_rs.items() 
                         if data.get('percentile', 0) >= 70}
        
        logger.info(f"강한 섹터 수: {len(strong_sectors)}")
        for sector, data in strong_sectors.items():
            logger.info(f"  - {sector}: RS 점수 = {data['rs_score']:.2f}, 백분위 = {data['percentile']:.2f}")
        
        # 결과 저장용 데이터프레임
        results = []
        
        # 모든 주식 파일 가져오기
        stock_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        
        # 시장 단계별 조건 설정
        stage_conditions = self._get_stage_conditions()
        
        # 각 주식 분석
        for file in stock_files:
            try:
                file_path = os.path.join(DATA_US_DIR, file)
                ticker = extract_ticker_from_filename(file)
                
                # ETF 제외
                if ticker in sector_etfs.values() or ticker in ['SPY', 'QQQ', 'DIA', 'IWM']:
                    continue
                
                # 데이터 로드
                df = pd.read_csv(file_path)
                if df.empty or len(df) < 200:  # 최소 200일 데이터 필요
                    continue
                    
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                
                # 이동평균 계산
                df['sma_30w'] = df['close'].rolling(window=150).mean()  # 30주 이동평균 (약 150 거래일)
                df['sma_10w'] = df['close'].rolling(window=50).mean()   # 10주 이동평균 (약 50 거래일)
                
                # 볼린저 밴드 계산 (20일, 2 표준편차)
                df['sma_20'] = df['close'].rolling(window=20).mean()
                df['std_20'] = df['close'].rolling(window=20).std()
                df['upper_band'] = df['sma_20'] + (df['std_20'] * 2)
                
                # RSI 계산
                df = calculate_rsi(df)
                
                # 거래량 지표
                df['volume_avg_20'] = df['volume'].rolling(window=20).mean()
                df['volume_ratio'] = df['volume'] / df['volume_avg_20']
                
                # 최근 데이터
                if len(df) < 5:  # 최소 5일 데이터 필요
                    continue
                    
                recent = df.iloc[-1]
                
                # 섹터 정보 조회
                stock_sector = self.sector_map.get(ticker)
                if not stock_sector:
                    continue

                # 섹터 내 RS 상위 10% 필터
                if self.stock_rs_percentile.get(ticker, 0) < 90:
                    continue
                
                # 현재 시장 단계에 맞는 조건 적용
                conditions = stage_conditions.get(self.market_stage, {})
                if not conditions:
                    continue
                    
                # 조건 검사
                condition_results = {}
                for name, condition_func in conditions.items():
                    condition_results[name] = condition_func(
                        df, recent, stock_sector, strong_sectors, ticker
                    )
                
                # 모든 조건을 만족하는지 확인
                all_conditions_met = all(condition_results.values())
                
                if all_conditions_met:
                    # 결과에 추가
                    result = {
                        'ticker': ticker,
                        'sector': stock_sector,
                        'close': recent['close'],
                        'volume': recent['volume'],
                        'volume_ratio': recent['volume_ratio'],
                        'rsi_14': recent['rsi_14'],
                        'above_30w_sma': recent['close'] > recent['sma_30w'],
                        'above_10w_sma': recent['close'] > recent['sma_10w'],
                        'market_stage': self.market_stage,
                        'date': self.today.strftime('%Y-%m-%d')
                    }
                    results.append(result)
            
            except Exception as e:
                logger.error(f"{ticker} 분석 중 오류 발생: {e}")
                continue
        
        # 결과를 데이터프레임으로 변환
        if results:
            results_df = pd.DataFrame(results)
            
            # 시장 단계별 정렬 기준 적용
            if self.market_stage == "stage1":
                # Stage 1: RSI 기준 정렬 (과매도 탈출 순)
                results_df = results_df.sort_values('rsi_14')
            elif self.market_stage == "stage2":
                # Stage 2: 10주 이평선 대비 종가 비율로 정렬
                results_df['price_to_10w'] = results_df['close'] / results_df['sma_10w']
                results_df = results_df.sort_values('price_to_10w', ascending=False)
            elif self.market_stage == "stage3":
                # Stage 3: 거래량 비율로 정렬
                results_df = results_df.sort_values('volume_ratio', ascending=False)
            else:  # stage4
                # Stage 4: RSI 기준 정렬 (약세 순)
                results_df = results_df.sort_values('rsi_14', ascending=False)
            
            # 결과 저장
            output_file = os.path.join(LEADER_STOCK_RESULTS_DIR, f"leader_stocks_{self.today.strftime('%Y%m%d')}.csv")
            results_df.to_csv(output_file, index=False)
            logger.info(f"스크리닝 결과 저장 완료: {output_file} ({len(results_df)}개 종목)")
            
            return results_df
        else:
            logger.info("조건을 만족하는 종목이 없습니다.")
            return pd.DataFrame()
    
    def _get_stage_conditions(self):
        """시장 단계별 조건 정의"""
        conditions = {
            "stage1": {
                "market_trend": lambda df, recent, sector, strong_sectors, ticker=None: self._market_trend_ok(),
                "above_30w_sma": lambda df, recent, sector, strong_sectors, ticker=None: all(df.iloc[-3:]['close'] > df.iloc[-3:]['sma_30w']),
                "rsi_oversold_exit": lambda df, recent, sector, strong_sectors, ticker=None: recent['rsi_14'] > 30,
                "volume_surge": lambda df, recent, sector, strong_sectors, ticker=None: recent['volume_ratio'] >= 2.0,
                "strong_sector": lambda df, recent, sector, strong_sectors, ticker=None: sector in strong_sectors,
            },
            "stage2": {
                "market_trend": lambda df, recent, sector, strong_sectors, ticker=None: self._market_trend_ok(),
                "above_30w_sma_3w": lambda df, recent, sector, strong_sectors, ticker=None: all(df.iloc[-15:]['close'] > df.iloc[-15:]['sma_30w']),
                "10w_above_30w": lambda df, recent, sector, strong_sectors, ticker=None: recent['sma_10w'] > recent['sma_30w'],
                "bollinger_breakout": lambda df, recent, sector, strong_sectors, ticker=None: recent['close'] > recent['upper_band'],
                "volume_above_avg": lambda df, recent, sector, strong_sectors, ticker=None: recent['volume_ratio'] >= 1.5,
            },
            "stage3": {
                "market_trend": lambda df, recent, sector, strong_sectors, ticker=None: self._market_trend_ok(),
                "small_cap_or_ipo": lambda df, recent, sector, strong_sectors, ticker=None: self._small_cap_or_recent_ipo(ticker),
                "volume_explosion": lambda df, recent, sector, strong_sectors, ticker=None: recent['volume_ratio'] >= 5.0,
                "rsi_overbought": lambda df, recent, sector, strong_sectors, ticker=None: recent['rsi_14'] >= 70,
                "momentum": lambda df, recent, sector, strong_sectors, ticker=None: self._calculate_momentum_indicator(df) >= 90,
            },
            "stage4": {
                "high_pe": lambda df, recent, sector, strong_sectors, ticker=None: self._is_high_pe(ticker),
                "growth_slowdown": lambda df, recent, sector, strong_sectors, ticker=None: self._growth_slowdown(ticker),
                "price_decline": lambda df, recent, sector, strong_sectors, ticker=None: df['close'].max() * 0.9 >= recent['close'] >= df['close'].max() * 0.95,
            }
        }
        return conditions


def run_leader_stock_screening():
    """주도주 투자 전략 스크리닝 실행 함수"""
    screener = LeaderStockScreener()
    return screener.screen_leader_stocks()


if __name__ == "__main__":
    run_leader_stock_screening()

