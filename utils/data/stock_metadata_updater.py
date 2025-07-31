#!/usr/bin/env python3
"""
주식 메타데이터 업데이트 시스템

이 모듈은 주식 메타데이터를 자동으로 업데이트하는 기능을 제공합니다.
주요 기능:
- Yahoo Finance API를 통한 실시간 데이터 수집
- Alpha Vantage API 연동 (선택적)
- 배치 업데이트 및 증분 업데이트
- 데이터 검증 및 오류 처리
- 백업 및 복원 기능
"""

import logging
import os
import json
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

# 설정 파일 import
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config import DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockMetadataUpdater:
    """주식 메타데이터 업데이트 시스템"""
    
    def __init__(self, 
                 metadata_file: str = None,
                 backup_dir: str = None,
                 alpha_vantage_key: str = None):
        """
        초기화
        
        Args:
            metadata_file: 메타데이터 파일 경로
            backup_dir: 백업 디렉토리 경로
            alpha_vantage_key: Alpha Vantage API 키
        """
        self.metadata_file = metadata_file or os.path.join(DATA_DIR, 'stock_metadata.csv')
        self.backup_dir = Path(backup_dir or os.path.join(DATA_DIR, 'backups'))
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # API 키 설정
        self.alpha_vantage_key = alpha_vantage_key or os.getenv('ALPHA_VANTAGE_API_KEY')
        
        # 요청 제한 설정
        self.request_delay = 0.2  # Yahoo Finance 요청 간격
        self.batch_size = 50  # 배치 크기
        self.max_workers = 5  # 최대 동시 작업자 수
        
        logger.info(f"메타데이터 업데이터 초기화: {self.metadata_file}")
    
    def create_backup(self) -> str:
        """현재 메타데이터 파일 백업"""
        if not os.path.exists(self.metadata_file):
            logger.warning("백업할 메타데이터 파일이 없습니다.")
            return ""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = self.backup_dir / f"stock_metadata_backup_{timestamp}.csv"
        
        try:
            import shutil
            shutil.copy2(self.metadata_file, backup_file)
            logger.info(f"백업 생성 완료: {backup_file}")
            return str(backup_file)
        except Exception as e:
            logger.error(f"백업 생성 실패: {e}")
            return ""
    
    def load_current_metadata(self) -> pd.DataFrame:
        """현재 메타데이터 로드"""
        try:
            if os.path.exists(self.metadata_file):
                df = pd.read_csv(self.metadata_file)
                logger.info(f"기존 메타데이터 로드: {len(df)}개 종목")
                return df
            else:
                logger.info("기존 메타데이터 파일이 없습니다. 새로 생성합니다.")
                return pd.DataFrame(columns=['symbol', 'sector', 'pe_ratio', 'market_cap', 'revenue_growth'])
        except Exception as e:
            logger.error(f"메타데이터 로드 실패: {e}")
            return pd.DataFrame(columns=['symbol', 'sector', 'pe_ratio', 'market_cap', 'revenue_growth'])
    
    def get_stock_symbols_from_data_dir(self) -> List[str]:
        """데이터 디렉토리에서 주식 심볼 목록 추출"""
        data_us_dir = os.path.join(DATA_DIR, 'us')
        if not os.path.exists(data_us_dir):
            logger.warning(f"데이터 디렉토리가 없습니다: {data_us_dir}")
            return []
        
        symbols = []
        for file in os.listdir(data_us_dir):
            if file.endswith('.csv') and not file.startswith('.'):
                symbol = file.replace('.csv', '')
                symbols.append(symbol)
        
        logger.info(f"데이터 디렉토리에서 {len(symbols)}개 심볼 발견")
        return sorted(symbols)
    
    def fetch_stock_info_yahoo(self, symbol: str) -> Dict[str, Any]:
        """Yahoo Finance에서 주식 정보 수집"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # 필요한 정보 추출
            stock_info = {
                'symbol': symbol,
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'pe_ratio': info.get('trailingPE', np.nan),
                'forward_pe': info.get('forwardPE', np.nan),
                'market_cap': info.get('marketCap', np.nan),
                'enterprise_value': info.get('enterpriseValue', np.nan),
                'revenue_growth': info.get('revenueGrowth', np.nan),
                'earnings_growth': info.get('earningsGrowth', np.nan),
                'profit_margin': info.get('profitMargins', np.nan),
                'operating_margin': info.get('operatingMargins', np.nan),
                'return_on_equity': info.get('returnOnEquity', np.nan),
                'return_on_assets': info.get('returnOnAssets', np.nan),
                'debt_to_equity': info.get('debtToEquity', np.nan),
                'current_ratio': info.get('currentRatio', np.nan),
                'quick_ratio': info.get('quickRatio', np.nan),
                'dividend_yield': info.get('dividendYield', np.nan),
                'payout_ratio': info.get('payoutRatio', np.nan),
                'beta': info.get('beta', np.nan),
                'shares_outstanding': info.get('sharesOutstanding', np.nan),
                'float_shares': info.get('floatShares', np.nan),
                'short_ratio': info.get('shortRatio', np.nan),
                'book_value': info.get('bookValue', np.nan),
                'price_to_book': info.get('priceToBook', np.nan),
                'price_to_sales': info.get('priceToSalesTrailing12Months', np.nan),
                'enterprise_to_revenue': info.get('enterpriseToRevenue', np.nan),
                'enterprise_to_ebitda': info.get('enterpriseToEbitda', np.nan),
                'last_updated': datetime.now().isoformat()
            }
            
            time.sleep(self.request_delay)  # API 요청 제한
            return stock_info
            
        except Exception as e:
            logger.warning(f"{symbol} 정보 수집 실패: {e}")
            return {
                'symbol': symbol,
                'sector': '',
                'pe_ratio': np.nan,
                'market_cap': np.nan,
                'revenue_growth': np.nan,
                'last_updated': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def fetch_stock_info_alpha_vantage(self, symbol: str) -> Dict[str, Any]:
        """Alpha Vantage에서 주식 정보 수집 (선택적)"""
        if not self.alpha_vantage_key:
            return {}
        
        try:
            # Company Overview API 호출
            url = f"https://www.alphavantage.co/query"
            params = {
                'function': 'OVERVIEW',
                'symbol': symbol,
                'apikey': self.alpha_vantage_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if 'Symbol' in data:
                return {
                    'symbol': symbol,
                    'sector': data.get('Sector', ''),
                    'industry': data.get('Industry', ''),
                    'pe_ratio': float(data.get('PERatio', 0)) if data.get('PERatio') != 'None' else np.nan,
                    'market_cap': float(data.get('MarketCapitalization', 0)) if data.get('MarketCapitalization') != 'None' else np.nan,
                    'revenue_growth': float(data.get('QuarterlyRevenueGrowthYOY', 0)) if data.get('QuarterlyRevenueGrowthYOY') != 'None' else np.nan,
                    'profit_margin': float(data.get('ProfitMargin', 0)) if data.get('ProfitMargin') != 'None' else np.nan,
                    'operating_margin': float(data.get('OperatingMarginTTM', 0)) if data.get('OperatingMarginTTM') != 'None' else np.nan,
                    'return_on_equity': float(data.get('ReturnOnEquityTTM', 0)) if data.get('ReturnOnEquityTTM') != 'None' else np.nan,
                    'return_on_assets': float(data.get('ReturnOnAssetsTTM', 0)) if data.get('ReturnOnAssetsTTM') != 'None' else np.nan,
                    'debt_to_equity': float(data.get('DebtToEquityRatio', 0)) if data.get('DebtToEquityRatio') != 'None' else np.nan,
                    'dividend_yield': float(data.get('DividendYield', 0)) if data.get('DividendYield') != 'None' else np.nan,
                    'beta': float(data.get('Beta', 0)) if data.get('Beta') != 'None' else np.nan,
                    'shares_outstanding': float(data.get('SharesOutstanding', 0)) if data.get('SharesOutstanding') != 'None' else np.nan,
                    'book_value': float(data.get('BookValue', 0)) if data.get('BookValue') != 'None' else np.nan,
                    'price_to_book': float(data.get('PriceToBookRatio', 0)) if data.get('PriceToBookRatio') != 'None' else np.nan,
                    'price_to_sales': float(data.get('PriceToSalesRatioTTM', 0)) if data.get('PriceToSalesRatioTTM') != 'None' else np.nan,
                    'last_updated': datetime.now().isoformat(),
                    'source': 'alpha_vantage'
                }
            else:
                logger.warning(f"Alpha Vantage에서 {symbol} 데이터를 찾을 수 없습니다.")
                return {}
                
        except Exception as e:
            logger.warning(f"Alpha Vantage {symbol} 정보 수집 실패: {e}")
            return {}
    
    def update_symbols_batch(self, symbols: List[str], use_alpha_vantage: bool = False) -> List[Dict[str, Any]]:
        """심볼 배치 업데이트 (thread-safe)"""
        updated_data = []
        temp_results = []  # 임시 결과 저장
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Yahoo Finance 작업 제출
            future_to_symbol = {}
            for symbol in symbols:
                future = executor.submit(self.fetch_stock_info_yahoo, symbol)
                future_to_symbol[future] = symbol
            
            # 결과 수집 (스레드 안전)
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    stock_info = future.result()
                    
                    # Alpha Vantage 데이터로 보완 (선택적)
                    if use_alpha_vantage and self.alpha_vantage_key:
                        av_info = self.fetch_stock_info_alpha_vantage(symbol)
                        if av_info:
                            # Alpha Vantage 데이터로 누락된 정보 보완
                            for key, value in av_info.items():
                                if key not in stock_info or pd.isna(stock_info[key]):
                                    stock_info[key] = value
                    
                    temp_results.append(stock_info)
                    
                except Exception as e:
                    logger.error(f"{symbol} 업데이트 실패: {e}")
                    temp_results.append({
                        'symbol': symbol,
                        'error': str(e),
                        'last_updated': datetime.now().isoformat()
                    })
        
        # 결과 병합 (메인 스레드에서 안전하게 처리)
        updated_data.extend(temp_results)
        
        return updated_data
    
    def update_metadata(self, 
                       symbols: List[str] = None, 
                       incremental: bool = True,
                       use_alpha_vantage: bool = False,
                       max_age_days: int = 7) -> Dict[str, Any]:
        """메타데이터 업데이트
        
        Args:
            symbols: 업데이트할 심볼 목록 (None이면 모든 심볼)
            incremental: 증분 업데이트 여부
            use_alpha_vantage: Alpha Vantage API 사용 여부
            max_age_days: 최대 데이터 나이 (일)
        
        Returns:
            업데이트 결과 딕셔너리
        """
        logger.info("메타데이터 업데이트 시작")
        
        # 백업 생성
        backup_file = self.create_backup()
        
        # 현재 메타데이터 로드
        current_df = self.load_current_metadata()
        
        # 업데이트할 심볼 결정
        if symbols is None:
            symbols = self.get_stock_symbols_from_data_dir()
        
        # 증분 업데이트인 경우 오래된 데이터만 업데이트
        if incremental and 'last_updated' in current_df.columns:
            cutoff_date = datetime.now() - timedelta(days=max_age_days)
            
            # 오래된 데이터 또는 누락된 심볼 찾기
            symbols_to_update = []
            for symbol in symbols:
                existing_row = current_df[current_df['symbol'] == symbol]
                if existing_row.empty:
                    symbols_to_update.append(symbol)  # 새 심볼
                else:
                    last_updated = existing_row['last_updated'].iloc[0]
                    if pd.isna(last_updated) or datetime.fromisoformat(last_updated) < cutoff_date:
                        symbols_to_update.append(symbol)  # 오래된 데이터
            
            symbols = symbols_to_update
            logger.info(f"증분 업데이트: {len(symbols)}개 심볼 업데이트 필요")
        
        if not symbols:
            logger.info("업데이트할 심볼이 없습니다.")
            return {
                'status': 'success',
                'updated_count': 0,
                'total_count': len(current_df),
                'backup_file': backup_file
            }
        
        # 배치 단위로 업데이트
        all_updated_data = []
        total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(symbols), self.batch_size):
            batch_symbols = symbols[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            
            logger.info(f"배치 {batch_num}/{total_batches} 처리 중: {len(batch_symbols)}개 심볼")
            
            batch_data = self.update_symbols_batch(batch_symbols, use_alpha_vantage)
            all_updated_data.extend(batch_data)
            
            # 배치 간 지연
            if i + self.batch_size < len(symbols):
                time.sleep(1)
        
        # 새 데이터프레임 생성
        new_df = pd.DataFrame(all_updated_data)
        
        # 기존 데이터와 병합
        if not current_df.empty:
            # 업데이트된 심볼 제거 후 새 데이터 추가
            updated_symbols = set(new_df['symbol'].tolist())
            current_df = current_df[~current_df['symbol'].isin(updated_symbols)]
            final_df = pd.concat([current_df, new_df], ignore_index=True)
        else:
            final_df = new_df
        
        # 정렬 및 저장
        final_df = final_df.sort_values('symbol').reset_index(drop=True)
        
        try:
            final_df.to_csv(self.metadata_file, index=False)
            logger.info(f"메타데이터 업데이트 완료: {len(final_df)}개 종목")
            
            return {
                'status': 'success',
                'updated_count': len(all_updated_data),
                'total_count': len(final_df),
                'backup_file': backup_file,
                'metadata_file': self.metadata_file
            }
            
        except Exception as e:
            logger.error(f"메타데이터 저장 실패: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'backup_file': backup_file
            }
    
    def get_update_status(self) -> Dict[str, Any]:
        """업데이트 상태 확인"""
        try:
            df = self.load_current_metadata()
            if df.empty:
                return {
                    'total_symbols': 0,
                    'last_updated': None,
                    'outdated_count': 0,
                    'missing_data_count': 0
                }
            
            # 통계 계산
            total_symbols = len(df)
            
            # 최근 업데이트 시간
            if 'last_updated' in df.columns:
                last_updated_times = pd.to_datetime(df['last_updated'], errors='coerce', utc=True)
                last_updated = last_updated_times.max()
            else:
                last_updated = None
            
            # 오래된 데이터 개수 (7일 이상)
            cutoff_date = datetime.now() - timedelta(days=7)
            if 'last_updated' in df.columns:
                outdated_mask = pd.to_datetime(df['last_updated'], errors='coerce', utc=True) < cutoff_date
                outdated_count = outdated_mask.sum()
            else:
                outdated_count = total_symbols
            
            # 누락된 데이터 개수
            missing_data_count = df[['sector', 'pe_ratio', 'market_cap', 'revenue_growth']].isna().any(axis=1).sum()
            
            return {
                'total_symbols': total_symbols,
                'last_updated': last_updated.isoformat() if last_updated else None,
                'outdated_count': int(outdated_count),
                'missing_data_count': int(missing_data_count),
                'data_completeness': (total_symbols - missing_data_count) / total_symbols if total_symbols > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"업데이트 상태 확인 실패: {e}")
            return {
                'error': str(e)
            }

def main():
    """메인 실행 함수"""
    updater = StockMetadataUpdater()
    
    # 업데이트 상태 확인
    status = updater.get_update_status()
    print(f"현재 상태: {json.dumps(status, indent=2, ensure_ascii=False)}")
    
    # 증분 업데이트 실행
    result = updater.update_metadata(
        incremental=True,
        use_alpha_vantage=False,  # Alpha Vantage 키가 있으면 True로 설정
        max_age_days=7
    )
    
    print(f"업데이트 결과: {json.dumps(result, indent=2, ensure_ascii=False)}")

if __name__ == "__main__":
    main()