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

# 로컬 모듈 임포트
from .data_manager import DataManager
from .pattern_analyzer import IPOPatternAnalyzer
from .track_analyzer import IPOTrackAnalyzer
from .result_processor import IPOResultProcessor

from config import IPO_INVESTMENT_RESULTS_DIR
from utils.calc_utils import get_us_market_today
from utils.market_utils import (
    get_vix_value,
    calculate_sector_rs,
    SECTOR_ETFS,
)
from data_collectors.market_breadth_collector import MarketBreadthCollector
from utils.screener_utils import save_screening_results, track_new_tickers, create_screener_summary

logger = logging.getLogger(__name__)


class IPOInvestmentScreener:
    """IPO 투자 전략 스크리너 클래스"""

    def __init__(self, skip_data=False):
        """IPO Investment 스크리너 초기화"""
        self.logger = logging.getLogger(__name__)
        self.today = get_us_market_today()
        self.skip_data = skip_data
        
        # 데이터 매니저 초기화
        self.data_manager = DataManager()
        
        # IPO 데이터 로드 (skip_data가 True면 기존 저장된 데이터 확인)
        if skip_data:
            # DataManager를 통해 실제 수집된 데이터 로드 시도
            try:
                self.ipo_data = self.data_manager.get_ipo_data(days_back=365, use_cache=True)
                if not self.ipo_data.empty:
                    self.logger.info(f"Skip data mode: 실제 IPO 데이터 로드 완료 ({len(self.ipo_data)}개)")
                else:
                    self.ipo_data = self._load_existing_ipo_data()
                    self.logger.info("Skip data mode: 기존 결과 파일에서 데이터 확인")
            except Exception as e:
                self.logger.error(f"DataManager IPO 데이터 로드 실패: {e}")
                self.ipo_data = self._load_existing_ipo_data()
                self.logger.info("Skip data mode: 기존 결과 파일에서 데이터 확인")
        else:
            self.ipo_data = self._load_ipo_data()
        
        # VIX 계산 및 섹터 상대강도 계산
        self.vix = get_vix_value()
        if self.vix is None:
            collector = MarketBreadthCollector()
            collector.collect_vix_data(days=30)
            self.vix = get_vix_value()
        self.sector_rs = calculate_sector_rs(SECTOR_ETFS)
        
        # 분석기 초기화
        self.pattern_analyzer = IPOPatternAnalyzer()
        self.track_analyzer = IPOTrackAnalyzer()
        self.result_processor = IPOResultProcessor(self.today)
    
    def _load_ipo_data(self):
        """IPO 데이터 로드 (실제 외부 데이터 소스 연동)"""
        try:
            # 데이터 매니저를 통해 실제 IPO 데이터 수집
            ipo_data = self.data_manager.get_ipo_data(days_back=365)

            if not ipo_data.empty:
                # 날짜 형식 정규화 및 UTC 변환
                ipo_data['ipo_date'] = pd.to_datetime(ipo_data['ipo_date'], errors='coerce')
                if ipo_data['ipo_date'].dt.tz is None:
                    ipo_data['ipo_date'] = ipo_data['ipo_date'].dt.tz_localize('UTC')
                else:
                    ipo_data['ipo_date'] = ipo_data['ipo_date'].dt.tz_convert('UTC')
                self.logger.info(f"IPO 데이터 로드 완료: {len(ipo_data)}개 종목")
                return ipo_data
            else:
                self.logger.warning("IPO 데이터가 비어있음")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"IPO 데이터 로드 중 오류: {e}")
            return pd.DataFrame()
    
    def _load_existing_ipo_data(self):
        """기존 저장된 IPO 데이터 로드 (skip_data 모드용)"""
        try:
            # 기존 IPO 결과 파일에서 데이터 로드
            from utils.screener_utils import find_latest_file
            latest_file = find_latest_file(IPO_INVESTMENT_RESULTS_DIR, "ipo_investment", "csv")
            
            if latest_file:
                try:
                    existing_data = pd.read_csv(latest_file)
                    if not existing_data.empty and 'symbol' in existing_data.columns:
                        # 기본 IPO 데이터 구조로 변환
                        ipo_data = pd.DataFrame({
                            'symbol': existing_data['symbol'],
                            'ipo_date': pd.to_datetime('2024-01-01'),  # 기본값
                            'ipo_price': 20.0,  # 기본값
                            'sector': 'Technology'  # 기본값
                        })
                        self.logger.info(f"기존 IPO 데이터 로드: {len(ipo_data)}개 종목")
                        return ipo_data
                except (pd.errors.EmptyDataError, pd.errors.ParserError):
                    self.logger.warning("기존 IPO 파일이 비어있거나 손상됨")
                    return pd.DataFrame()
                
                # 빈 데이터프레임인 경우
                if existing_data.empty:
                    self.logger.warning("기존 IPO 파일이 비어있음")
                    return pd.DataFrame()
            
            self.logger.warning("기존 IPO 데이터 파일을 찾을 수 없음")
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"기존 IPO 데이터 로드 중 오류: {e}")
            return pd.DataFrame()
    
    def _get_recent_ipos(self, days: int = 365) -> pd.DataFrame:
        """최근 IPO 종목을 ipo_data에서 필터링 (정확히 1년 기준)"""
        if self.ipo_data.empty:
            return pd.DataFrame()

        # 정확히 1년(365일) 기준으로 필터링
        cutoff_date = pd.Timestamp(self.today - timedelta(days=days), tz='UTC')
        
        # ipo_date가 timezone-aware인지 확인하고 맞춰줌
        if 'ipo_date' in self.ipo_data.columns:
            if self.ipo_data['ipo_date'].dt.tz is None:
                cutoff_date = cutoff_date.tz_localize(None)
            recent = self.ipo_data[self.ipo_data['ipo_date'] >= cutoff_date].copy()
            self.logger.info(f"최근 {days}일 내 IPO 데이터로 필터링 (기준일: {cutoff_date.strftime('%Y-%m-%d')})")
        else:
            # ipo_date 컬럼이 없는 경우 모든 데이터 사용
            recent = self.ipo_data.copy()
            self.logger.info("ipo_date 컬럼이 없어 모든 IPO 데이터를 사용합니다.")
        if recent.empty:
            self.logger.info("날짜 필터링 후 IPO 데이터가 비어있습니다.")
            return pd.DataFrame()
        
        self.logger.info(f"날짜 필터링 후 IPO 데이터: {len(recent)}개")
        
        # symbol이 있는 종목만 필터링
        if 'symbol' in recent.columns:
            recent = recent[recent['symbol'].notna() & (recent['symbol'] != '') & (recent['symbol'] != 'N/A')]
            self.logger.info(f"symbol 필터링 후 IPO 데이터: {len(recent)}개")
        
        if recent.empty:
            self.logger.info("symbol 필터링 후 IPO 데이터가 비어있습니다.")
            return pd.DataFrame()

        # 상장가 정보 처리 (선택적)
        if 'ipo_price' in recent.columns:
            # ipo_price가 숫자가 아닌 경우 기본값 설정
            recent['ipo_price'] = pd.to_numeric(recent['ipo_price'], errors='coerce')
            recent['ipo_price'] = recent['ipo_price'].fillna(20.0)  # 기본값 20달러
        else:
            recent['ipo_price'] = 20.0  # 기본값
        
        # 섹터 정보가 없는 경우 기본값 설정
        if 'sector' not in recent.columns:
            recent['sector'] = 'Unknown'
        else:
            recent['sector'] = recent['sector'].fillna('Unknown')
            
        if recent.empty:
            return pd.DataFrame()
            
        # days_since_ipo 계산
        if 'ipo_date' in recent.columns:
            today_ts = pd.Timestamp(self.today, tz='UTC')
            recent['days_since_ipo'] = (today_ts - recent['ipo_date']).dt.days
        else:
            recent['days_since_ipo'] = 30  # 기본값
            
        self.logger.info(f"최종 IPO 데이터: {len(recent)}개")
        return recent
    
    

    
    def _validate_listing_status(self, ticker: str) -> bool:
        """실제 상장 여부 확인 - OHLCV 데이터 존재 여부로 판단"""
        try:
            # 최근 30일 데이터로 빠른 검증
            data = yf.download(ticker, period="1mo", interval="1d", auto_adjust=False, progress=False)
            
            if data.empty:
                return False
                
            # MultiIndex 컬럼 처리
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
            
            # 필수 OHLCV 컬럼 확인
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing_cols = [col for col in required_cols if col not in data.columns]
            
            if missing_cols:
                logger.debug(f"{ticker}: 필수 컬럼 누락 - {missing_cols}")
                return False
            
            # 실제 거래 데이터 확인 (하루라도 유효한 데이터가 있는지)
            price_data = data[['Open', 'High', 'Low', 'Close']].dropna()
            volume_data = data['Volume'].dropna()
            
            # 하루라도 유효한 가격 데이터가 있는지 확인
            has_price_data = len(price_data) > 0
            # 하루라도 유효한 거래량 데이터가 있는지 확인  
            has_volume_data = len(volume_data) > 0
            # 하루라도 거래량이 0보다 큰 날이 있는지 확인
            has_recent_trading = len(volume_data[volume_data > 0]) > 0
            
            is_valid = has_price_data and has_volume_data and has_recent_trading
            
            if not is_valid:
                logger.debug(f"{ticker}: 상장 검증 실패 - 가격데이터: {has_price_data}, 거래량데이터: {has_volume_data}, 최근거래: {has_recent_trading}")
            
            return is_valid
            
        except Exception as e:
            logger.debug(f"{ticker}: 상장 검증 중 오류 - {e}")
            return False
    
    def screen_ipo_investments(self):
        """IPO 투자 전략 스크리닝 실행"""
        logger.info("IPO 투자 전략 스크리닝 시작...")
        
        # skip_data 모드에서도 정상적으로 스크리닝 수행 (OHLCV 데이터만 건너뛰고 나머지는 진행)
        if self.skip_data:
            logger.info("Skip data mode: OHLCV 업데이트 없이 기존 데이터로 스크리닝 진행")
        
        # 최근 IPO 종목 가져오기
        recent_ipos = self._get_recent_ipos(days=365)
        if recent_ipos.empty:
            logger.info("최근 1년 내 IPO 종목이 없습니다.")
            return pd.DataFrame()
            
        logger.info(f"최근 1년 내 IPO 종목 수: {len(recent_ipos)}")
        
        # 실제 상장된 종목만 필터링
        logger.info("실제 상장 여부 검증 중...")
        valid_ipos = []
        invalid_count = 0
        
        for _, ipo in recent_ipos.iterrows():
            ticker = ipo.get('ticker', ipo.get('symbol', ''))
            if not ticker or ticker == 'N/A':
                invalid_count += 1
                continue
                
            # SPAC 종목 사전 필터링
            company_name = ipo.get('company_name', '')
            if any(keyword in company_name.upper() for keyword in ['ACQUISITION CORP', 'SPAC', 'ACQUISITION CO']):
                logger.debug(f"{ticker}: SPAC 종목으로 제외")
                invalid_count += 1
                continue
            
            # 실제 상장 여부 검증
            if self._validate_listing_status(ticker):
                valid_ipos.append(ipo)
            else:
                invalid_count += 1
        
        if not valid_ipos:
            logger.info("실제 상장된 IPO 종목이 없습니다.")
            return pd.DataFrame()
        
        valid_ipos_df = pd.DataFrame(valid_ipos)
        logger.info(f"상장 검증 완료: {len(valid_ipos_df)}개 유효, {invalid_count}개 제외")
        
        # 결과 저장용 데이터프레임
        base_results = []
        breakout_results = []
        track1_results = []
        track2_results = []
        
        # 각 IPO 종목 분석 (검증된 종목만)
        for ipo in valid_ipos:
            try:
                # 컬럼명 통일 (symbol 또는 ticker)
                ticker = ipo.get('ticker', ipo.get('symbol', ''))
                if not ticker:
                    continue
                    
                # 주가 데이터 로드 (yfinance 사용)
                data = yf.download(ticker, period="6mo", interval="1d", auto_adjust=False)
                if data.empty:
                    continue
                
                # 데이터프레임 형식 변환 (MultiIndex 컬럼 처리)
                df = data.reset_index()
                
                # MultiIndex 컬럼 구조 확인 및 평탄화
                if isinstance(df.columns, pd.MultiIndex):
                    # MultiIndex인 경우 레벨 0만 사용
                    df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
                
                # 컬럼명 정규화
                df.columns = [col.lower().replace(' ', '_') for col in df.columns]
                
                # 필수 컬럼 확인 및 매핑
                column_mapping = {
                    'date': 'date',
                    'open': 'open', 
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'volume': 'volume',
                    'adj_close': 'adj_close'  # 있으면 유지, 없으면 무시
                }
                
                # 필수 컬럼이 있는지 확인
                required_cols = ['open', 'high', 'low', 'close', 'volume']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    logger.warning(f"{ticker}: 필수 컬럼 누락 {missing_cols}")
                    continue
                
                # 데이터 타입 확인 및 변환
                for col in required_cols:
                    if col in df.columns:
                        # 문자열이나 객체 타입인 경우 숫자로 변환 시도
                        if df[col].dtype == 'object' or df[col].dtype == 'string':
                            try:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                            except Exception as e:
                                logger.error(f"{ticker}: {col} 컬럼 타입 변환 실패: {e}")
                                continue
                
                # NaN 값이 있는 행 제거
                df = df.dropna(subset=required_cols)
                
                if df.empty:
                    logger.warning(f"{ticker}: 데이터 정리 후 빈 데이터프레임")
                    continue
                    
                df = df.sort_values('date')
                
                # IPO 분석 데이터 가져오기
                ipo_analysis = self.data_manager.get_ipo_analysis(ticker)
                
                # IPO 발행가 정보 추가
                ipo_price = ipo.get('ipo_price', 0)
                
                # 베이스 패턴 확인
                has_base, base_info = self.pattern_analyzer.check_ipo_base_pattern(df, ipo_price)

                # 브레이크아웃 확인
                has_breakout, breakout_info = self.pattern_analyzer.check_ipo_breakout(df)

                # Track1/Track2 조건 확인
                track1_pass, track1_info = self.track_analyzer.check_track1(df, ipo_price)
                track2_pass, track2_info = self.track_analyzer.check_track2(df, ipo_price)
                
                # 패턴 형성 시점 결정
                pattern_formation_date = self.today.strftime('%Y-%m-%d')  # 기본값
                
                if has_base and base_info.get('pattern_formation_date'):
                    pattern_formation_date = base_info.get('pattern_formation_date')
                
                if has_breakout and breakout_info.get('pattern_formation_date'):
                    # 브레이크아웃이 있으면 브레이크아웃 날짜를 우선 사용
                    pattern_formation_date = breakout_info.get('pattern_formation_date')
                
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
                        'date': base_info.get('pattern_formation_date', self.today.strftime('%Y-%m-%d')),
                        'screening_date': self.today.strftime('%Y-%m-%d')
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
                        'date': breakout_info.get('pattern_formation_date', self.today.strftime('%Y-%m-%d')),
                        'screening_date': self.today.strftime('%Y-%m-%d')
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
                        'date': track1_info.get('pattern_formation_date', self.today.strftime('%Y-%m-%d')),
                        'screening_date': self.today.strftime('%Y-%m-%d')
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
                        'date': track2_info.get('pattern_formation_date', self.today.strftime('%Y-%m-%d')),
                        'screening_date': self.today.strftime('%Y-%m-%d')
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
        logger.info(f"베이스 패턴: {len(base_results)}개, 브레이크아웃: {len(breakout_results)}개")
        logger.info(f"Track1: {len(track1_results)}개, Track2: {len(track2_results)}개")
        
        # CSV 및 JSON 파일로 저장
        self.result_processor.save_results(base_results, breakout_results, track1_results, track2_results)

        # 통합 결과
        dfs = [base_df, breakout_df, track1_df, track2_df]
        dfs = [d for d in dfs if not d.empty]
        combined_results = pd.concat(dfs) if dfs else pd.DataFrame()
        
        return combined_results


def run_ipo_investment_screening(skip_data=False):
    """IPO 투자 전략 스크리닝 실행 함수"""
    print("\n🔍 IPO 투자 전략 스크리닝 시작...")
    
    screener = IPOInvestmentScreener(skip_data=skip_data)
    results_df = screener.screen_ipo_investments()
    
    # DataFrame을 딕셔너리 리스트로 변환
    if not results_df.empty:
        results_list = results_df.to_dict('records')
    else:
        results_list = []
    
    # 결과 저장 (JSON + CSV)
    results_paths = save_screening_results(
        results=results_list,
        output_dir=IPO_INVESTMENT_RESULTS_DIR,
        filename_prefix="ipo_investment_results",
        include_timestamp=True,
        incremental_update=True
    )
    
    # 새로운 티커 추적
    tracker_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR, "new_ipo_tickers.csv")
    new_tickers = track_new_tickers(
        current_results=results_list,
        tracker_file=tracker_file,
        symbol_key='ticker',  # IPO 스크리너는 ticker 키 사용
        retention_days=30  # IPO는 30일간 추적
    )
    
    # 요약 정보 생성
    summary = create_screener_summary(
        screener_name="IPO Investment",
        total_candidates=len(results_list),
        new_tickers=len(new_tickers),
        results_paths=results_paths
    )
    
    print(f"✅ IPO 투자 스크리닝 완료: {len(results_list)}개 종목, 신규 {len(new_tickers)}개")
    
    return results_df


