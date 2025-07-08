"""데이터 정규화 유틸리티"""

import os
import pandas as pd
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class DataNormalizer:
    """데이터 정규화 클래스"""
    
    @staticmethod
    def normalize_advance_decline_data(file_path: str) -> bool:
        """Advance-Decline 데이터 정규화"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"파일이 존재하지 않습니다: {file_path}")
                return False
                
            print(f"📊 Advance-Decline 데이터 정규화 중: {file_path}")
            
            # 데이터 로드
            df = pd.read_csv(file_path)
            
            # 컬럼명 정규화
            df.columns = [col.lower().strip() for col in df.columns]
            
            # 날짜 컬럼 정규화
            if 'date' in df.columns:
                # 다양한 날짜 형식 처리
                try:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
                    # 날짜만 추출 (시간 정보 제거)
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                except Exception as e:
                    logger.warning(f"날짜 변환 오류: {e}")
                    # 대안: 문자열 처리로 날짜 추출
                    df['date'] = df['date'].astype(str).str[:10]
                
            # 필수 컬럼 확인 및 생성
            required_columns = ['advancing', 'declining', 'unchanged']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = 0
                    
            # 데이터 타입 정규화
            for col in required_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                
            # 중복 날짜 제거 (최신 데이터 유지)
            df = df.drop_duplicates(subset=['date'], keep='last')
            
            # 날짜순 정렬
            df = df.sort_values('date')
            
            # 유효하지 않은 데이터 제거
            df = df.dropna(subset=['date'])
            
            # 백업 파일 생성
            backup_path = file_path + '.backup'
            if os.path.exists(file_path):
                os.rename(file_path, backup_path)
                
            # 정규화된 데이터 저장
            df.to_csv(file_path, index=False)
            
            print(f"✅ Advance-Decline 데이터 정규화 완료")
            print(f"  - 총 레코드: {len(df)}개")
            print(f"  - 날짜 범위: {df['date'].min()} ~ {df['date'].max()}")
            print(f"  - 백업 파일: {backup_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Advance-Decline 데이터 정규화 실패: {e}")
            return False
            
    @staticmethod
    def normalize_vix_data(file_path: str) -> bool:
        """VIX 데이터 정규화"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"파일이 존재하지 않습니다: {file_path}")
                return False
                
            print(f"📊 VIX 데이터 정규화 중: {file_path}")
            
            # 데이터 로드
            df = pd.read_csv(file_path)
            
            # 컬럼명 정규화
            df.columns = [col.lower().strip() for col in df.columns]
            
            # 날짜 컬럼 정규화
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                
            # VIX 관련 컬럼 정규화
            vix_columns = ['vix_close', 'vix_high', 'vix_low', 'vix_volume']
            for col in vix_columns:
                if col in df.columns:
                    if 'volume' in col:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    else:
                        df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                        
            # 중복 날짜 제거
            df = df.drop_duplicates(subset=['date'], keep='last')
            
            # 날짜순 정렬
            df = df.sort_values('date')
            
            # 유효하지 않은 데이터 제거
            df = df.dropna(subset=['date', 'vix_close'])
            
            # 백업 파일 생성
            backup_path = file_path + '.backup'
            if os.path.exists(file_path):
                os.rename(file_path, backup_path)
                
            # 정규화된 데이터 저장
            df.to_csv(file_path, index=False)
            
            print(f"✅ VIX 데이터 정규화 완료")
            print(f"  - 총 레코드: {len(df)}개")
            print(f"  - 날짜 범위: {df['date'].min()} ~ {df['date'].max()}")
            print(f"  - 최신 VIX: {df['vix_close'].iloc[-1]}")
            
            return True
            
        except Exception as e:
            logger.error(f"VIX 데이터 정규화 실패: {e}")
            return False
            
    @staticmethod
    def normalize_all_market_data(data_dir: str) -> bool:
        """모든 시장 데이터 정규화"""
        success = True
        
        # Advance-Decline 데이터 정규화
        ad_file = os.path.join(data_dir, 'breadth', 'advance_decline.csv')
        if os.path.exists(ad_file):
            success &= DataNormalizer.normalize_advance_decline_data(ad_file)
            
        # VIX 데이터 정규화
        vix_file = os.path.join(data_dir, 'options', 'vix.csv')
        if os.path.exists(vix_file):
            success &= DataNormalizer.normalize_vix_data(vix_file)
            
        return success

def main():
    """메인 실행 함수"""
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config import DATA_DIR
    
    normalizer = DataNormalizer()
    success = normalizer.normalize_all_market_data(DATA_DIR)
    
    if success:
        print("\n✅ 모든 데이터 정규화 완료")
    else:
        print("\n❌ 일부 데이터 정규화 실패")
        
if __name__ == "__main__":
    main()