# -*- coding: utf-8 -*-
"""상승 모멘텀 신호 (Uptrend Momentum Signals) 스크리너.

섹터 정보를 포함한 ``data/stock_metadata.csv`` 파일을 사용한다.
파일 위치는 ``config.STOCK_METADATA_PATH`` 설정을 따른다.
"""

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
    MOMENTUM_SIGNALS_CRITERIA,
    MOMENTUM_SIGNALS_RESULTS_DIR,
)
from utils.calc_utils import get_us_market_today, calculate_rsi
from utils.io_utils import ensure_dir, extract_ticker_from_filename
from utils.market_utils import calculate_sector_rs, SECTOR_ETFS
from utils.quantified_trading_rules import momentum_signal
from .indicators import (
    calculate_macd,
    calculate_stochastic,
    calculate_adx,
    calculate_bollinger_bands,
    calculate_moving_averages,
    calculate_volume_indicators,
    calculate_vwap,
    calculate_obv,
    calculate_ad,
    detect_cup_and_handle,
)

# 결과 저장 디렉토리 (config에서 가져옴)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MomentumSignalsScreener:
    """상승 모멘텀 신호 스크리너 클래스"""
    
    def __init__(self, skip_data=False):
        """초기화"""
        self.today = get_us_market_today()
        self.skip_data = skip_data
        ensure_dir(MOMENTUM_SIGNALS_RESULTS_DIR)
        
        if skip_data:
            self.rs_scores = {}
            self.sector_data = {}
            self.pe_data = {}
            self.strong_sectors = []
            logger.info("Skip data mode: 메타데이터 로드 건너뜀")
        else:
            self.rs_scores = self._load_rs_scores()
            self._load_metadata()
            self.strong_sectors = self._load_sector_strength()
    


    def _load_metadata(self):
        """Load sector and RS percentile information."""
        self.sector_map = {}
        self.stock_rs_percentile = {}

        if os.path.exists(STOCK_METADATA_PATH):
            try:
                meta = pd.read_csv(STOCK_METADATA_PATH)
                if {'symbol', 'sector'}.issubset(meta.columns):
                    self.sector_map = meta.set_index('symbol')['sector'].to_dict()
            except Exception as e:
                logger.warning(f"메타데이터 로드 실패: {e}")
        else:
            logger.warning(f"메타데이터 파일이 없습니다: {STOCK_METADATA_PATH}")

        if os.path.exists(US_WITH_RS_PATH) and self.sector_map:
            try:
                rs_df = pd.read_csv(US_WITH_RS_PATH)
                if {'symbol', 'rs_score'}.issubset(rs_df.columns):
                    rs_df['sector'] = rs_df['symbol'].map(self.sector_map)
                    rs_df.dropna(subset=['sector'], inplace=True)
                    rs_df['sector_rank'] = rs_df.groupby('sector')['rs_score'].rank(pct=True) * 100
                    self.stock_rs_percentile = rs_df.set_index('symbol')['sector_rank'].to_dict()
            except Exception as e:
                logger.warning(f"RS 메타데이터 로드 실패: {e}")

    def _load_rs_scores(self):
        """RS 점수 로드"""
        if os.path.exists(US_WITH_RS_PATH):
            rs_df = pd.read_csv(US_WITH_RS_PATH)
            return dict(zip(rs_df['symbol'], rs_df['rs_score']))
        logger.warning(f"RS 점수 파일을 찾을 수 없습니다: {US_WITH_RS_PATH}")
        return {}

    def _load_sector_strength(self):
        """섹터별 상대 강도 계산"""
        sector_rs = calculate_sector_rs(SECTOR_ETFS)
        self.all_sectors = list(SECTOR_ETFS.keys())
        strong = {s: d for s, d in sector_rs.items() if d.get('percentile', 0) >= 60}
        return strong
    
    def screen_momentum_signals(self):
        """상승 모멘텀 신호 스크리닝 실행"""
        logger.info("상승 모멘텀 신호 스크리닝 시작...")
        
        # skip_data 모드에서는 빈 결과 반환
        if self.skip_data:
            logger.info("Skip data mode: 빈 결과 반환")
            return pd.DataFrame()
        
        # 결과 저장용 데이터프레임
        results = []
        
        # 모든 주식 파일 가져오기
        stock_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        
        # 각 주식 분석
        for file in stock_files:
            try:
                file_path = os.path.join(DATA_US_DIR, file)
                ticker = extract_ticker_from_filename(file)

                # 주요 지수 제외
                if ticker in ['SPY', 'QQQ', 'DIA', 'IWM']:
                    continue

                # RS 점수 필터
                rs_score = self.rs_scores.get(ticker)
                if rs_score is None or rs_score < 70:
                    continue

                # 실제 섹터 조회 및 강한 섹터 필터
                sector = self.sector_map.get(ticker)
                if not sector:
                    continue
                if self.strong_sectors and sector not in self.strong_sectors:
                    continue
                if self.stock_rs_percentile.get(ticker, 0) < 60:
                    continue
                
                # 데이터 로드
                df = pd.read_csv(file_path)
                
                # 컬럼명을 소문자로 변환
                df.columns = [c.lower() for c in df.columns]
                
                # 필수 컬럼 존재 여부 확인
                required_columns = ['close', 'volume', 'date', 'high', 'low', 'open']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    logger.warning(f"{ticker}: 필수 컬럼 누락 - {missing_columns}")
                    continue
                
                if df.empty or len(df) < 200:  # 최소 200일 데이터 필요
                    continue
                    
                df['date'] = pd.to_datetime(df['date'], utc=True)
                df = df.sort_values('date')
                
                # 기술적 지표 계산
                df = calculate_moving_averages(df)
                df = calculate_macd(df, include_hist=True)
                df = calculate_rsi(df)
                df = calculate_stochastic(df)
                df = calculate_adx(df)
                df = calculate_bollinger_bands(df)
                df = calculate_volume_indicators(df)
                df = calculate_vwap(df)
                df = calculate_obv(df)
                df = calculate_ad(df)
                
                # 최근 데이터
                if len(df) < 5:  # 최소 5일 데이터 필요
                    continue
                    
                recent = df.iloc[-1]
                prev = df.iloc[-2]
                
                # 모멘텀 신호 조건 검사
                signals = {}
                
                # 1. 골든 크로스 (50일 이평선이 200일 이평선 상향 돌파)
                signals['golden_cross'] = (
                    recent['sma_50'] > recent['sma_200'] and 
                    prev['sma_50'] <= prev['sma_200']
                )
                
                # 2. 단기 이평선 정렬 (5일 > 10일 > 20일)
                signals['short_ma_aligned'] = (
                    recent['sma_5'] > recent['sma_10'] > recent['sma_20']
                )
                
                # 3. 중장기 이평선 정렬 (20일 > 50일 > 100일 > 200일)
                signals['long_ma_aligned'] = (
                    recent['sma_20'] > recent['sma_50'] > 
                    recent['sma_100'] > recent['sma_200']
                )
                
                # 4. MACD 상향 돌파 (MACD가 시그널 라인 상향 돌파)
                signals['macd_crossover'] = (
                    recent['macd'] > recent['macd_signal'] and 
                    prev['macd'] <= prev['macd_signal']
                )
                
                # 5. MACD 히스토그램 증가 (MACD 히스토그램이 증가 추세)
                signals['macd_hist_increasing'] = (
                    recent['macd_hist'] > 0 and 
                    recent['macd_hist'] > prev['macd_hist']
                )
                
                # 6. RSI 상승 추세 (RSI가 50 이상이고 상승 중)
                signals['rsi_uptrend'] = (
                    recent['rsi_14'] > 50 and 
                    recent['rsi_14'] > df.iloc[-3]['rsi_14']
                )
                
                # 7. 스토캐스틱 상향 돌파 (%K가 %D 상향 돌파)
                signals['stoch_crossover'] = (
                    recent['stoch_k'] > recent['stoch_d'] and 
                    prev['stoch_k'] <= prev['stoch_d']
                )
                
                # 8. ADX 강한 추세 (ADX > 25)
                signals['strong_trend'] = recent['adx'] > 25
                
                # 9. +DI > -DI (상승 추세 확인)
                signals['positive_trend'] = recent['+di'] > recent['-di']
                
                # 10. 볼린저 밴드 상단 돌파
                signals['bollinger_breakout'] = recent['close'] > recent['upper_band']
                
                # 11. 거래량 증가 (20일 평균 대비 1.5배 이상)
                signals['volume_surge'] = recent['volume_ratio'] >= 1.5
                
                # 12. 신고가 돌파 (52주 신고가 돌파)
                year_high = df.iloc[-260:]['high'].max() if len(df) >= 260 else df['high'].max()
                signals['new_high'] = recent['close'] > year_high * 0.98  # 52주 신고가의 98% 이상

                # 13. 가격 모멘텀 (5일간 5% 이상 상승)
                five_day_return = (recent['close'] / df.iloc[-6]['close'] - 1) * 100
                signals['price_momentum'] = five_day_return >= 5

                # 13-1. 컵앤핸들 패턴 돌파 여부
                signals['cup_handle_breakout'] = detect_cup_and_handle(df)
                
                # 14. 이격도 양호 (종가가 20일 이동평균선 대비 8% 이내)
                price_to_ma = (recent['close'] / recent['sma_20'] - 1) * 100
                signals['healthy_ma_distance'] = 0 <= price_to_ma <= 8
                
                # 15. 상승 추세 확인 (종가 > 20일 이평선 > 50일 이평선)
                signals['uptrend_confirmed'] = (
                    recent['close'] > recent['sma_20'] > recent['sma_50']
                )

                # 16. 30주 이평선 돌파 3일 연속
                signals['above_30w_3d'] = all(df.iloc[-i]['close'] > df.iloc[-i]['sma_30w'] for i in range(1, 4))

                # 17. Stage 2A 확인 (10주 SMA 상승, 30주 SMA 수평/상승)

                signals['stage_2a'] = (
                    len(df) >= 50
                    and recent['sma_50'] > df.iloc[-50]['sma_50']
                    and recent['sma_30w'] >= df.iloc[-50]['sma_30w']
                )


                # 18. VWAP 돌파
                signals['vwap_breakout'] = (
                    recent['close'] > recent['vwap'] and recent['volume_ratio'] >= 1.5
                )

                # 19. OBV 상승세
                signals['obv_rising'] = (
                    len(df) >= 4 and recent['obv'] > df.iloc[-2]['obv'] > df.iloc[-3]['obv']
                )

                # 20. A/D 라인 신고점 경신
                ad_max = df['ad'].iloc[-50:].max() if len(df) >= 50 else df['ad'].max()
                signals['ad_new_high'] = recent['ad'] >= ad_max
                
                # 추가 규칙 기반 모멘텀 시그널 검사
                rule_result = momentum_signal(
                    df,
                    rs_score,
                    self.strong_sectors.get(sector, {}).get('percentile', 0),
                )
                if not rule_result["signal"]:
                    continue

                # 모멘텀 점수 계산 (각 신호당 1점)
                momentum_score = sum(signals.values())

                # 핵심 신호 확인 (최소 3개 이상 만족해야 함)
                core_signals = [
                    signals['macd_crossover'],
                    signals['rsi_uptrend'],
                    signals['stoch_crossover'],
                    signals['strong_trend'],
                    signals['positive_trend'],
                    signals['volume_surge'],
                    signals['above_30w_3d'],
                    signals['stage_2a']
                ]
                core_signals_count = sum(core_signals)

                min_score = MOMENTUM_SIGNALS_CRITERIA['min_momentum_score']
                min_core = MOMENTUM_SIGNALS_CRITERIA['min_core_signals']

                # 최소 점수 및 핵심 신호 충족 여부 확인
                if momentum_score >= min_score and core_signals_count >= min_core:
                    # 결과에 추가
                    result = {
                        'ticker': ticker,
                        'sector': sector,
                        'close': recent['close'],
                        'volume': recent['volume'],
                        'rs_score': rs_score,
                        'momentum_score': momentum_score,
                        'core_signals': core_signals_count,
                        'rsi_14': recent['rsi_14'],
                        'macd': recent['macd'],
                        'macd_signal': recent['macd_signal'],
                        'adx': recent['adx'],
                        'date': self.today.strftime('%Y-%m-%d')
                    }

                    # 각 신호 결과 추가
                    for signal_name, signal_value in signals.items():
                        result[signal_name] = signal_value

                    # 규칙 기반 결과 병합
                    result.update(rule_result)

                    results.append(result)
            
            except Exception as e:
                logger.error(f"{ticker} 분석 중 오류 발생: {e}")
                continue
        
        # 결과를 데이터프레임으로 변환
        if results:
            results_df = pd.DataFrame(results)
            
            # 모멘텀 점수 기준으로 내림차순 정렬
            results_df = results_df.sort_values(['momentum_score', 'core_signals'], 
                                               ascending=[False, False])
            
            # 결과 저장
            output_file = os.path.join(MOMENTUM_SIGNALS_RESULTS_DIR,
                                      f"momentum_signals_{self.today.strftime('%Y%m%d')}.csv")
            results_df.to_csv(output_file, index=False)
            results_df.to_json(output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"스크리닝 결과 저장 완료: {output_file} ({len(results_df)}개 종목)")
            
            return results_df
        else:
            # 빈 결과일 때도 칼럼명이 있는 빈 파일 생성
            output_file = os.path.join(MOMENTUM_SIGNALS_RESULTS_DIR,
                                      f"momentum_signals_{self.today.strftime('%Y%m%d')}.csv")
            empty_df = pd.DataFrame(columns=['ticker', 'sector', 'close', 'volume', 'rs_score', 'momentum_score', 
                                           'core_signals', 'rsi_14', 'macd', 'macd_signal', 'adx', 'date',
                                           'golden_cross', 'short_ma_aligned', 'long_ma_aligned', 'macd_crossover',
                                           'macd_hist_increasing', 'rsi_uptrend', 'stoch_crossover', 'strong_trend',
                                           'positive_trend', 'bollinger_breakout', 'volume_surge', 'new_high',
                                           'price_momentum', 'cup_handle_breakout', 'healthy_ma_distance',
                                           'uptrend_confirmed', 'above_30w_3d', 'stage_2a', 'vwap_breakout',
                                           'obv_rising', 'ad_new_high', 'rs_rank', 'sector_strong', 'signal'])
            empty_df.to_csv(output_file, index=False)
            empty_df.to_json(output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"조건을 만족하는 종목이 없습니다. 빈 파일 생성: {output_file}")
            return pd.DataFrame()


def run_momentum_signals_screening(skip_data=False):
    """상승 모멘텀 신호 스크리닝 실행 함수"""
    screener = MomentumSignalsScreener(skip_data=skip_data)
    return screener.screen_momentum_signals()


