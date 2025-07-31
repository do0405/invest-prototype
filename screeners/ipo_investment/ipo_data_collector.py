#!/usr/bin/env python3
"""
고급 IPO 데이터 수집기 - 실제 데이터 수집

이 스크립트는 다양한 데이터 소스를 사용하여 실제 IPO 데이터를 수집합니다.

주요 기능:
- 모듈화된 데이터 소스 지원
- 과거 및 예정된 IPO 데이터 모두 수집
- CSV 및 JSON 형식으로 저장
- 재시도 로직 및 오류 처리
"""

import logging
import os
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
import requests
from pathlib import Path

# SEC Edgar 데이터 소스만 사용
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_sources.sec_edgar_source import SecEdgarSource

# 로깅 설정 (중복 방지)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True  # 기존 설정을 덮어씀
)
logger = logging.getLogger(__name__)

class RealIPODataCollector:
    """실제 IPO 데이터 수집기"""
    
    def __init__(self, data_dir: str = None):
        # 기본 데이터 디렉토리 설정
        if data_dir is None:
            # config에서 DATA_DIR 가져오기
            try:
                sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
                from config import DATA_DIR
                data_dir = os.path.join(DATA_DIR, 'IPO')
            except ImportError:
                data_dir = "../../data/IPO"
        
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # SEC Edgar 데이터 소스만 사용
        self.sec_edgar_source = SecEdgarSource()
        
        logger.info(f"IPO 데이터 수집기 초기화 완료: {self.data_dir}")
    
    # 기존 개별 메서드들은 모듈화된 데이터 소스로 이동됨
    
    def _clean_and_deduplicate(self, ipos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """IPO 데이터 정리 및 중복 제거"""
        if not ipos:
            return []
        
        # 심볼 기준으로 중복 제거 (우선순위: finance_calendars > investpy)
        seen_symbols = set()
        cleaned_ipos = []
        
        # finance_calendars 데이터 우선 처리
        for ipo in ipos:
            symbol = ipo.get('symbol', 'N/A')
            if symbol != 'N/A' and symbol not in seen_symbols:
                seen_symbols.add(symbol)
                cleaned_ipos.append(ipo)
            elif symbol == 'N/A':
                # 심볼이 없는 경우 회사명으로 중복 체크
                company_name = ipo.get('company_name', 'N/A')
                if company_name not in [c.get('company_name') for c in cleaned_ipos]:
                    cleaned_ipos.append(ipo)
        
        logger.info(f"중복 제거 후 {len(cleaned_ipos)}개 IPO 데이터")
        return cleaned_ipos
    
    def _save_to_files(self, data: List[Dict[str, Any]], file_prefix: str) -> Dict[str, str]:
        """데이터를 CSV와 JSON 파일로 저장"""
        if not data:
            logger.warning(f"{file_prefix} 데이터가 비어있어 파일을 저장하지 않습니다.")
            return {}
        
        # 타임스탬프 포함 파일명 사용
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # CSV 파일 저장
        csv_filename = f"{file_prefix}_{timestamp}.csv"
        csv_path = self.data_dir / csv_filename
        
        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"CSV 파일 저장 완료: {csv_path} ({len(data)}개 IPO)")
        
        # JSON 파일 저장
        json_filename = f"{file_prefix}_{timestamp}.json"
        json_path = self.data_dir / json_filename
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"JSON 파일 저장 완료: {json_path} ({len(data)}개 IPO)")
        
        return {
            'csv': str(csv_path),
            'json': str(json_path)
        }
    
    def collect_all_ipo_data(self) -> Dict[str, Any]:
        """SEC Edgar에서 IPO 데이터 수집 및 저장"""
        logger.info("SEC Edgar IPO 데이터 수집 시작")
        
        recent_ipos = []
        upcoming_ipos = []
        
        try:
            # SEC Edgar 사용 가능 여부 확인
            if self.sec_edgar_source.is_available():
                logger.info("SEC Edgar에서 데이터 수집 중...")
                
                # 최근 IPO 데이터 수집
                recent_data = self.sec_edgar_source.get_recent_ipos(months_back=6)
                if recent_data:
                    recent_ipos.extend(recent_data)
                    logger.info(f"SEC Edgar: 최근 IPO {len(recent_data)}개 수집")
                
                # 예정된 IPO 데이터 수집
                upcoming_data = self.sec_edgar_source.get_upcoming_ipos(months_ahead=3)
                if upcoming_data:
                    upcoming_ipos.extend(upcoming_data)
                    logger.info(f"SEC Edgar: 예정 IPO {len(upcoming_data)}개 수집")
                    
            else:
                logger.warning("SEC Edgar API를 사용할 수 없습니다.")
                
        except Exception as e:
            logger.error(f"SEC Edgar 데이터 수집 실패: {e}")
        
        # 데이터 정리 및 중복 제거
        recent_ipos = self._clean_and_deduplicate(recent_ipos)
        upcoming_ipos = self._clean_and_deduplicate(upcoming_ipos)
        
        # 파일 저장
        recent_files = self._save_to_files(recent_ipos, 'recent_ipos')
        upcoming_files = self._save_to_files(upcoming_ipos, 'upcoming_ipos')
        
        logger.info("SEC Edgar IPO 데이터 수집 및 저장 완료")
        
        return {
            'recent_ipos': recent_ipos,
            'upcoming_ipos': upcoming_ipos,
            'files': {
                'recent': recent_files,
                'upcoming': upcoming_files
            },
            'source': 'sec_edgar'
        }

    def get_recent_ipos(self, days_back: int = 365) -> pd.DataFrame:
        """최근 IPO 데이터를 파일에서 로드"""
        # 타임스탬프가 포함된 파일들을 우선 확인
        csv_files = sorted(self.data_dir.glob('recent_ipos_*.csv'))
        if not csv_files:
            # 타임스탬프 없는 파일을 확인
            recent_ipos_file = self.data_dir / 'recent_ipos.csv'
            if recent_ipos_file.exists():
                csv_files = [recent_ipos_file]
        
        if not csv_files:
            return pd.DataFrame()

        df_list = []
        for file in csv_files:
            df = pd.read_csv(file)
            df.columns = [c.lower() for c in df.columns]
            if 'ticker' in df.columns and 'symbol' not in df.columns:
                df.rename(columns={'ticker': 'symbol'}, inplace=True)
            if 'date' in df.columns and 'ipo_date' not in df.columns:
                df.rename(columns={'date': 'ipo_date'}, inplace=True)
            if 'price_range' in df.columns and 'ipo_price' not in df.columns:
                df['ipo_price'] = (
                    df['price_range']
                    .astype(str)
                    .str.replace('$', '')
                    .str.split('-')
                    .str[0]
                    .str.replace(',', '')
                )
            df_list.append(df)

        df_all = pd.concat(df_list, ignore_index=True)
        if 'ipo_date' in df_all.columns:
            df_all['ipo_date'] = pd.to_datetime(df_all['ipo_date'], errors='coerce', utc=True)
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=days_back)
            df_all = df_all[df_all['ipo_date'] >= cutoff]
        return df_all

def main():
    """메인 실행 함수"""
    collector = RealIPODataCollector()
    
    try:
        results = collector.collect_all_ipo_data()
        
        print("\n=== 수집 결과 ===")
        print(f"과거 IPO 데이터: {len(results['recent_ipos'])}개")
        print(f"예정된 IPO 데이터: {len(results['upcoming_ipos'])}개")
        
        # 샘플 데이터 출력
        if results['recent_ipos']:
            print("\n=== 최근 IPO 샘플 ===")
            for ipo in results['recent_ipos'][:3]:
                symbol = ipo.get('symbol', 'N/A')
                company = ipo.get('company_name', 'N/A')
                date = ipo.get('ipo_date', 'N/A')
                print(f"- {symbol}: {company} ({date})")
        
        if results['upcoming_ipos']:
            print("\n=== 예정된 IPO 샘플 ===")
            for ipo in results['upcoming_ipos'][:3]:
                symbol = ipo.get('symbol', 'N/A')
                company = ipo.get('company_name', 'N/A')
                date = ipo.get('expected_ipo_date', 'N/A')
                print(f"- {symbol}: {company} ({date})")
        
            
    except Exception as e:
        logger.error(f"IPO 데이터 수집 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    try:
        collector = RealIPODataCollector()
        results = collector.collect_all_ipo_data()
        
        print(f"\n✅ IPO 데이터 수집 완료!")
        print(f"📊 최근 IPO: {len(results['recent_ipos'])}개")
        print(f"📅 예정된 IPO: {len(results['upcoming_ipos'])}개")
        
        # 저장된 파일 정보
        print("\n=== 저장된 파일 ===")
        files = results['files']
        if files.get('recent'):
            print(f"- recent_csv: {files['recent']['csv']}")
            print(f"- recent_json: {files['recent']['json']}")
        if files.get('upcoming'):
            print(f"- upcoming_csv: {files['upcoming']['csv']}")
            print(f"- upcoming_json: {files['upcoming']['json']}")
            
    except Exception as e:
        logger.error(f"IPO 데이터 수집 중 오류 발생: {e}")
        raise
