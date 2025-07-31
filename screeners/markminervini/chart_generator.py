#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
차트 생성 및 데이터 처리 모듈
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ChartGenerator:
    """차트 생성 및 데이터 처리 클래스"""
    
    def __init__(self, project_root: str, image_dir: str):
        self.project_root = project_root
        self.image_dir = image_dir
        self.ensure_directories()
    
    def ensure_directories(self):
        """필요한 디렉토리 생성"""
        os.makedirs(self.image_dir, exist_ok=True)
    
    def fetch_ohlcv_data(self, symbol: str, days: int = 120) -> Optional[pd.DataFrame]:
        """로컬 CSV 파일에서 OHLCV 데이터 가져오기
        
        Args:
            symbol: 주식 심볼
            days: 조회할 일수 (기본 120일)
            
        Returns:
            OHLCV DataFrame 또는 None
        """
        try:
            # 로컬 CSV 파일 경로
            csv_path = os.path.join(self.project_root, 'data', 'us', f'{symbol}.csv')
            
            if not os.path.exists(csv_path):
                logger.warning(f"{symbol}: 로컬 데이터 파일이 없습니다. ({csv_path})")
                return None
            
            # CSV 파일 읽기
            data = pd.read_csv(csv_path)
            
            # 컬럼명 확인 및 정리
            if len(data.columns) >= 7:
                # 일반적인 형태: Date,Open,High,Low,Close,Adj Close,Volume,Symbol
                data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume', 'Symbol']
                data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            else:
                logger.error(f"{symbol}: CSV 파일 형식이 올바르지 않습니다. 컬럼 수: {len(data.columns)}")
                return None
            
            # 날짜 컬럼 처리
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce', utc=True)
            data = data.dropna(subset=['Date'])  # 잘못된 날짜 제거
            
            # timezone 정보 제거 (localize to None)
            data['Date'] = data['Date'].dt.tz_localize(None)
            
            data.set_index('Date', inplace=True)
            data = data.sort_index()
            
            # 인덱스가 DatetimeIndex인지 확인하고 강제 변환
            if not isinstance(data.index, pd.DatetimeIndex):
                data.index = pd.to_datetime(data.index, utc=True).tz_localize(None)
            
            if len(data) < days:
                logger.warning(f"{symbol}: 충분한 데이터가 없습니다. (조회된 데이터: {len(data)}일)")
                return None
                
            # 최근 120일 데이터만 사용
            data = data.tail(days)
            
            # 컬럼명을 mplfinance에 맞게 변경
            data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            
            # 숫자형 데이터로 변환
            for col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
            
            # NaN 값이 있는 행 제거
            data = data.dropna()
            
            return data
            
        except Exception as e:
            logger.error(f"{symbol} 데이터 조회 실패: {e}")
            return None
    
    def generate_chart_image(self, symbol: str, data: pd.DataFrame) -> bool:
        """OHLCV 데이터로 캔들차트 이미지 생성
        
        Args:
            symbol: 주식 심볼
            data: OHLCV DataFrame
            
        Returns:
            성공 여부
        """
        try:
            image_path = os.path.join(self.image_dir, f"{symbol}.png")
            
            # mplfinance 스타일 설정
            mc = mpf.make_marketcolors(
                up='g', down='r',
                edge='inherit',
                wick={'up':'green', 'down':'red'},
                volume='in'
            )
            
            s = mpf.make_mpf_style(
                marketcolors=mc,
                gridstyle='-',
                y_on_right=False
            )
            
            # 차트 생성
            mpf.plot(
                data,
                type='candle',
                volume=True,
                style=s,
                title=f'{symbol} - 120 Days Chart',
                ylabel='Price ($)',
                ylabel_lower='Volume',
                figsize=(12, 8),
                savefig=dict(fname=image_path, dpi=150, bbox_inches='tight')
            )
            
            plt.close('all')  # 메모리 정리
            
            logger.info(f"{symbol} 차트 이미지 생성 완료: {image_path}")
            return True
            
        except Exception as e:
            logger.error(f"{symbol} 차트 이미지 생성 실패: {e}")
            return False