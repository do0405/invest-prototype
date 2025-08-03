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
        """데이터를 CSV와 JSON 파일로 저장 (증분 업데이트 지원, 1년 이상 된 데이터 자동 정리)"""
        if not data:
            logger.warning(f"{file_prefix} 데이터가 비어있어 파일을 저장하지 않습니다.")
            return {}
        
        # 날짜만 포함한 파일명 사용 (시간 정보 제거)
        date_str = datetime.now().strftime('%Y%m%d')
        csv_filename = f"{file_prefix}_{date_str}.csv"
        json_filename = f"{file_prefix}_{date_str}.json"
        csv_path = self.data_dir / csv_filename
        json_path = self.data_dir / json_filename
        
        new_df = pd.DataFrame(data)
        
        # 1년 기준 날짜 계산 (pandas Timestamp로 변환)
        one_year_ago = pd.Timestamp(datetime.now() - timedelta(days=365))
        
        # 증분 업데이트 처리
        if csv_path.exists():
            try:
                existing_df = pd.read_csv(csv_path)
                
                # 기존 데이터에서 1년 이상 된 데이터 제거
                if 'ipo_date' in existing_df.columns:
                    existing_df['ipo_date'] = pd.to_datetime(existing_df['ipo_date'], errors='coerce')
                    before_filter = len(existing_df)
                    # NaT 값 제거 후 날짜 비교
                    valid_dates_mask = existing_df['ipo_date'].notna()
                    existing_df = existing_df[valid_dates_mask & (existing_df['ipo_date'] >= one_year_ago)]
                    after_filter = len(existing_df)
                    if before_filter > after_filter:
                        logger.info(f"🗑️ 1년 이상 된 IPO 데이터 {before_filter - after_filter}개 제거")
                
                # 기본 키 컬럼 확인 (symbol 또는 ticker)
                key_col = None
                for col in ['symbol', 'ticker', 'company_name']:
                    if col in new_df.columns:
                        key_col = col
                        break
                
                if key_col and key_col in existing_df.columns:
                    # 기존 데이터에서 새 데이터와 중복되는 항목 제거
                    existing_df = existing_df[~existing_df[key_col].isin(new_df[key_col])]
                    
                    # 기존 데이터와 새 데이터 병합
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    
                    # 날짜 컬럼이 있으면 날짜순 정렬 (최신순)
                    date_cols = [col for col in combined_df.columns if 'date' in col.lower()]
                    if date_cols:
                        # 날짜 컬럼을 datetime으로 변환 후 정렬
                        sort_col = date_cols[0]
                        combined_df[sort_col] = pd.to_datetime(combined_df[sort_col], errors='coerce')
                        # NaT 값을 마지막에 배치하여 정렬
                        combined_df = combined_df.sort_values(sort_col, ascending=False, na_position='last')
                    
                    final_df = combined_df
                    logger.info(f"🔄 증분 업데이트: 기존 {len(existing_df)}개 + 신규 {len(new_df)}개 = 총 {len(final_df)}개 IPO")
                else:
                    final_df = new_df
                    logger.info(f"⚠️ 키 컬럼 불일치, 전체 교체: {len(new_df)}개 IPO")
            except Exception as e:
                logger.warning(f"기존 파일 읽기 실패 ({e}), 전체 교체: {len(new_df)}개 IPO")
                final_df = new_df
        else:
            final_df = new_df
            logger.info(f"✅ 신규 저장: {len(new_df)}개 IPO")
        
        # 최종 데이터에서도 1년 이상 된 데이터 필터링
        if 'ipo_date' in final_df.columns:
            final_df['ipo_date'] = pd.to_datetime(final_df['ipo_date'], errors='coerce')
            before_final_filter = len(final_df)
            # NaT 값 제거 후 날짜 비교
            valid_dates_mask = final_df['ipo_date'].notna()
            final_df = final_df[valid_dates_mask & (final_df['ipo_date'] >= one_year_ago)]
            after_final_filter = len(final_df)
            if before_final_filter > after_final_filter:
                logger.info(f"📅 최종 저장 전 1년 이상 된 데이터 {before_final_filter - after_final_filter}개 추가 제거")
        
        # CSV 파일 저장
        final_df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"CSV 파일 저장 완료: {csv_path} (최근 1년 데이터 {len(final_df)}개)")
        
        # JSON 파일 증분 업데이트
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    existing_json = json.load(f)
                
                # JSON도 동일하게 증분 업데이트
                key_col = None
                for col in ['symbol', 'ticker', 'company_name']:
                    if col in data[0]:
                        key_col = col
                        break
                
                if key_col:
                    existing_keys = {item.get(key_col) for item in existing_json if key_col in item}
                    # 기존 항목에서 업데이트된 항목 제거
                    updated_existing = [item for item in existing_json 
                                      if item.get(key_col) not in {d.get(key_col) for d in data}]
                    combined_json = updated_existing + data
                else:
                    combined_json = data
            except Exception:
                combined_json = data
        else:
            combined_json = data
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(combined_json, f, ensure_ascii=False, indent=2)
        logger.info(f"JSON 파일 저장 완료: {json_path}")
        
        return {
            'csv': str(csv_path),
            'json': str(json_path)
        }
    
    def collect_all_ipo_data(self) -> Dict[str, Any]:
        """SEC Edgar에서 IPO 데이터 수집 및 저장"""
        logger.info("SEC Edgar IPO 데이터 수집 시작")
        
        recent_ipos = []
        
        try:
            # SEC Edgar 사용 가능 여부 확인
            if self.sec_edgar_source.is_available():
                logger.info("SEC Edgar에서 데이터 수집 중...")
                
                # 최근 IPO 데이터 수집
                recent_data = self.sec_edgar_source.get_recent_ipos(months_back=6)
                if recent_data:
                    recent_ipos.extend(recent_data)
                    logger.info(f"SEC Edgar: 최근 IPO {len(recent_data)}개 수집")
                    
            else:
                logger.warning("SEC Edgar API를 사용할 수 없습니다.")
                
        except Exception as e:
            logger.error(f"SEC Edgar 데이터 수집 실패: {e}")
        
        # 데이터 정리 및 중복 제거
        recent_ipos = self._clean_and_deduplicate(recent_ipos)
        
        # 파일 저장
        recent_files = self._save_to_files(recent_ipos, 'recent_ipos')
        
        logger.info("SEC Edgar IPO 데이터 수집 및 저장 완료")
        
        return {
            'recent_ipos': recent_ipos,
            'files': {
                'recent': recent_files
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
            from utils.screener_utils import read_csv_flexible
            df = read_csv_flexible(str(file))
            if df is None:
                continue
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
        print(f"최근 IPO 데이터: {len(results['recent_ipos'])}개")
        
        # 샘플 데이터 출력
        if results['recent_ipos']:
            print("\n=== 최근 IPO 샘플 ===")
            for ipo in results['recent_ipos'][:3]:
                symbol = ipo.get('symbol', 'N/A')
                company = ipo.get('company_name', 'N/A')
                date = ipo.get('ipo_date', 'N/A')
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
        
        # 저장된 파일 정보
        print("\n=== 저장된 파일 ===")
        files = results['files']
        if files.get('recent'):
            print(f"- recent_csv: {files['recent']['csv']}")
            print(f"- recent_json: {files['recent']['json']}")
            
    except Exception as e:
        logger.error(f"IPO 데이터 수집 중 오류 발생: {e}")
        raise
