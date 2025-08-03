#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
증분 업데이트 헬퍼 모듈
누락된 데이터부터 증분하는 방식으로 개선
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
import logging

logger = logging.getLogger(__name__)

# 미국 주식 시장 휴일 (간단한 버전)
US_MARKET_HOLIDAYS_2024_2025 = {
    # 2024년 휴일
    '2024-01-01',  # New Year's Day
    '2024-01-15',  # Martin Luther King Jr. Day
    '2024-02-19',  # Presidents' Day
    '2024-03-29',  # Good Friday
    '2024-05-27',  # Memorial Day
    '2024-06-19',  # Juneteenth
    '2024-07-04',  # Independence Day
    '2024-09-02',  # Labor Day
    '2024-11-28',  # Thanksgiving
    '2024-12-25',  # Christmas
    
    # 2025년 휴일
    '2025-01-01',  # New Year's Day
    '2025-01-20',  # Martin Luther King Jr. Day
    '2025-02-17',  # Presidents' Day
    '2025-04-18',  # Good Friday
    '2025-05-26',  # Memorial Day
    '2025-06-19',  # Juneteenth
    '2025-07-04',  # Independence Day
    '2025-09-01',  # Labor Day
    '2025-11-27',  # Thanksgiving
    '2025-12-25',  # Christmas
}

class IncrementalUpdateHelper:
    """증분 업데이트를 위한 헬퍼 클래스"""
    
    def __init__(self):
        self.logger = logger
    
    def is_trading_day(self, date: datetime) -> bool:
        """주어진 날짜가 거래일인지 확인 (주말과 휴일 제외)"""
        # 주말 확인 (토요일=5, 일요일=6)
        if date.weekday() >= 5:
            return False
        
        # 휴일 확인
        date_str = date.strftime('%Y-%m-%d')
        if date_str in US_MARKET_HOLIDAYS_2024_2025:
            return False
        
        return True
    
    def get_next_trading_day(self, date: datetime) -> datetime:
        """다음 거래일 반환"""
        next_day = date + timedelta(days=1)
        while not self.is_trading_day(next_day):
            next_day += timedelta(days=1)
        return next_day
    
    def get_previous_trading_day(self, date: datetime) -> datetime:
        """이전 거래일 반환"""
        prev_day = date - timedelta(days=1)
        while not self.is_trading_day(prev_day):
            prev_day -= timedelta(days=1)
        return prev_day
    
    def find_missing_date_range(self, file_path: str, date_column: str = 'date') -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        파일에서 누락된 날짜 범위를 찾아 반환
        
        Args:
            file_path: 데이터 파일 경로
            date_column: 날짜 컬럼명
            
        Returns:
            (start_date, end_date): 누락된 데이터의 시작일과 종료일
        """
        try:
            if not os.path.exists(file_path):
                # 파일이 없으면 전체 기간 수집
                end_date = datetime.now()
                start_date = end_date - timedelta(days=450)  # 기본 450일
                self.logger.info(f"파일 없음, 전체 수집: {start_date.date()} ~ {end_date.date()}")
                return start_date, end_date
            
            # 기존 파일 읽기
            from utils.screener_utils import read_csv_flexible
            df = read_csv_flexible(file_path, [date_column])
            
            if df is None or df.empty:
                # 파일이 비어있으면 전체 기간 수집
                end_date = datetime.now()
                start_date = end_date - timedelta(days=450)
                self.logger.info(f"빈 파일, 전체 수집: {start_date.date()} ~ {end_date.date()}")
                return start_date, end_date
            
            # 날짜 컬럼 처리
            df[date_column] = pd.to_datetime(df[date_column], utc=True, errors='coerce')
            df = df.dropna(subset=[date_column])
            
            if df.empty:
                # 유효한 날짜가 없으면 전체 기간 수집
                end_date = datetime.now()
                start_date = end_date - timedelta(days=450)
                self.logger.info(f"유효한 날짜 없음, 전체 수집: {start_date.date()} ~ {end_date.date()}")
                return start_date, end_date
            
            # 최신 날짜 찾기
            latest_date = df[date_column].max()
            
            # 타임존 처리
            if latest_date.tz is not None:
                today = pd.Timestamp.now(tz=latest_date.tz)
            else:
                today = pd.Timestamp.now()
                latest_date = pd.Timestamp(latest_date)
            
            # 최신 날짜가 오늘과 같거나 미래면 업데이트 불필요
            if latest_date.date() >= today.date():
                self.logger.info(f"최신 상태: {latest_date.date()}")
                return None, None
            
            # 누락된 기간 계산 (거래일만 고려)
            start_date_dt = latest_date.to_pydatetime().replace(tzinfo=None) if hasattr(latest_date, 'to_pydatetime') else latest_date.replace(tzinfo=None)
            end_date_dt = today.to_pydatetime().replace(tzinfo=None) if hasattr(today, 'to_pydatetime') else today.replace(tzinfo=None)
            
            # 다음 거래일부터 시작
            start_date = self.get_next_trading_day(start_date_dt)
            
            # 오늘이 거래일이 아니면 이전 거래일까지만
            if not self.is_trading_day(end_date_dt):
                end_date = self.get_previous_trading_day(end_date_dt)
            else:
                end_date = end_date_dt
            
            # 시작일이 종료일보다 늦으면 업데이트 불필요
            if start_date.date() > end_date.date():
                self.logger.info(f"거래일 기준 최신 상태: {latest_date.date()} (다음 거래일: {start_date.date()})")
                return None, None
            
            # datetime 객체로 변환 (타임존 제거)
            if hasattr(start_date, 'to_pydatetime'):
                start_date = start_date.to_pydatetime().replace(tzinfo=None)
            if hasattr(end_date, 'to_pydatetime'):
                end_date = end_date.to_pydatetime().replace(tzinfo=None)
            
            self.logger.info(f"누락 기간 발견: {start_date.date()} ~ {end_date.date()}")
            return start_date, end_date
            
        except Exception as e:
            self.logger.error(f"날짜 범위 계산 실패 ({file_path}): {e}")
            # 오류 시 최근 7일만 수집
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            return start_date, end_date
    
    def get_missing_symbols(self, data_dir: str, required_symbols: List[str], 
                          date_threshold_days: int = 7) -> List[str]:
        """
        누락되거나 오래된 데이터를 가진 심볼들을 찾아 반환
        
        Args:
            data_dir: 데이터 디렉토리
            required_symbols: 필요한 심볼 목록
            date_threshold_days: 오래된 데이터 기준 (일)
            
        Returns:
            업데이트가 필요한 심볼 목록
        """
        missing_symbols = []
        cutoff_date = pd.Timestamp.now() - timedelta(days=date_threshold_days)
        
        for symbol in required_symbols:
            file_path = os.path.join(data_dir, f"{symbol}.csv")
            
            try:
                if not os.path.exists(file_path):
                    missing_symbols.append(symbol)
                    continue
                
                # 파일의 최신 날짜 확인
                from utils.screener_utils import read_csv_flexible
                df = read_csv_flexible(file_path, ['date'])
                
                if df is None or df.empty:
                    missing_symbols.append(symbol)
                    continue
                
                df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
                df = df.dropna(subset=['date'])
                
                if df.empty:
                    missing_symbols.append(symbol)
                    continue
                
                latest_date = df['date'].max()
                
                # 타임존 처리
                if hasattr(latest_date, 'tz') and latest_date.tz is not None:
                    cutoff_date_tz = cutoff_date.tz_localize(latest_date.tz) if cutoff_date.tz is None else cutoff_date
                else:
                    cutoff_date_tz = cutoff_date.tz_localize(None) if cutoff_date.tz is not None else cutoff_date
                    latest_date = pd.Timestamp(latest_date)
                
                # 오래된 데이터면 업데이트 필요 (거래일 기준으로 확인)
                latest_date_dt = latest_date.to_pydatetime().replace(tzinfo=None) if hasattr(latest_date, 'to_pydatetime') else latest_date.replace(tzinfo=None)
                cutoff_date_dt = cutoff_date_tz.to_pydatetime().replace(tzinfo=None) if hasattr(cutoff_date_tz, 'to_pydatetime') else cutoff_date_tz.replace(tzinfo=None)
                
                # 최신 데이터가 거래일 기준으로 오래되었는지 확인
                if latest_date_dt.date() < cutoff_date_dt.date():
                    # 추가로 오늘이 거래일인지 확인
                    today = datetime.now()
                    if self.is_trading_day(today):
                        missing_symbols.append(symbol)
                    else:
                        # 오늘이 거래일이 아니면 이전 거래일과 비교
                        last_trading_day = self.get_previous_trading_day(today)
                        if latest_date_dt.date() < last_trading_day.date():
                            missing_symbols.append(symbol)
                    
            except Exception as e:
                self.logger.warning(f"심볼 {symbol} 확인 실패: {e}")
                missing_symbols.append(symbol)
        
        self.logger.info(f"업데이트 필요 심볼: {len(missing_symbols)}개")
        return missing_symbols
    
    def calculate_optimal_batch_size(self, total_items: int, max_batch_size: int = 50) -> int:
        """
        최적의 배치 크기 계산
        
        Args:
            total_items: 전체 아이템 수
            max_batch_size: 최대 배치 크기
            
        Returns:
            최적 배치 크기
        """
        if total_items <= 10:
            return total_items
        elif total_items <= 50:
            return min(10, total_items)
        else:
            return min(max_batch_size, max(10, total_items // 10))
    
    def merge_incremental_data(self, existing_df: pd.DataFrame, new_df: pd.DataFrame, 
                             key_columns: List[str], date_column: str = 'date') -> pd.DataFrame:
        """
        기존 데이터와 새 데이터를 증분 방식으로 병합
        
        Args:
            existing_df: 기존 데이터
            new_df: 새 데이터
            key_columns: 중복 확인용 키 컬럼들
            date_column: 날짜 컬럼명
            
        Returns:
            병합된 데이터프레임
        """
        try:
            if existing_df.empty:
                return new_df
            
            if new_df.empty:
                return existing_df
            
            # 날짜 컬럼 정규화
            if date_column in existing_df.columns:
                existing_df[date_column] = pd.to_datetime(existing_df[date_column], utc=True, errors='coerce')
            if date_column in new_df.columns:
                new_df[date_column] = pd.to_datetime(new_df[date_column], utc=True, errors='coerce')
            
            # 키 컬럼들이 모두 존재하는지 확인
            available_keys = [col for col in key_columns if col in existing_df.columns and col in new_df.columns]
            
            if not available_keys:
                # 키 컬럼이 없으면 단순 결합
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                # 중복 제거 후 병합
                if date_column in available_keys:
                    # 날짜 기준으로 중복 제거 (새 데이터 우선)
                    existing_filtered = existing_df[~existing_df[available_keys].isin(new_df[available_keys].to_dict('list')).all(axis=1)]
                else:
                    # 키 컬럼 기준으로 중복 제거
                    existing_filtered = existing_df[~existing_df[available_keys].isin(new_df[available_keys].to_dict('list')).all(axis=1)]
                
                combined_df = pd.concat([existing_filtered, new_df], ignore_index=True)
            
            # 날짜순 정렬
            if date_column in combined_df.columns:
                combined_df = combined_df.sort_values(date_column, ascending=True)
            
            # 인덱스 재설정
            combined_df = combined_df.reset_index(drop=True)
            
            self.logger.info(f"데이터 병합 완료: 기존 {len(existing_df)}개 + 신규 {len(new_df)}개 = 총 {len(combined_df)}개")
            return combined_df
            
        except Exception as e:
            self.logger.error(f"데이터 병합 실패: {e}")
            # 실패 시 기존 데이터 반환
            return existing_df if not existing_df.empty else new_df

# 전역 헬퍼 인스턴스
incremental_helper = IncrementalUpdateHelper()