#!/usr/bin/env python3
"""
VIX 데이터 변환 유틸리티

options/vix.csv 파일을 us/VIX.csv 형식으로 변환합니다.
"""

import os
import pandas as pd
from datetime import datetime
from config import DATA_DIR


def convert_vix_data() -> bool:
    """VIX 데이터를 options 폴더에서 us 폴더 형식으로 변환"""
    try:
        # 파일 경로 설정
        options_vix_path = os.path.join(DATA_DIR, 'options', 'vix.csv')
        us_vix_path = os.path.join(DATA_DIR, 'us', 'VIX.csv')
        
        # options VIX 데이터 확인
        if not os.path.exists(options_vix_path):
            print(f"⚠️ VIX 데이터 파일이 없습니다: {options_vix_path}")
            return False
            
        # 데이터 로드
        df_options = pd.read_csv(options_vix_path)
        
        if df_options.empty:
            print("⚠️ VIX 데이터가 비어있습니다.")
            return False
            
        print(f"📊 VIX 데이터 변환 중... ({len(df_options)} 행)")
        
        # US 형식으로 변환
        df_us = pd.DataFrame()
        df_us['date'] = pd.to_datetime(df_options['date'], utc=True).dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
        df_us['symbol'] = 'VIX'
        
        # OHLC 데이터 매핑 (VIX는 일반적으로 open 데이터가 없으므로 low 값을 사용)
        df_us['open'] = df_options.get('vix_low', df_options.get('vix_close', 0))
        df_us['high'] = df_options.get('vix_high', df_options.get('vix_close', 0))
        df_us['low'] = df_options.get('vix_low', df_options.get('vix_close', 0))
        df_us['close'] = df_options.get('vix_close', 0)
        df_us['volume'] = df_options.get('vix_volume', 0)
        
        # 데이터 타입 정리
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            df_us[col] = pd.to_numeric(df_us[col], errors='coerce').fillna(0)
            
        # volume은 정수로 변환
        df_us['volume'] = df_us['volume'].astype(int)
        
        # 중복 제거 및 정렬
        df_us = df_us.drop_duplicates(subset=['date'], keep='last')
        df_us = df_us.sort_values('date')
        
        # 파일 저장
        os.makedirs(os.path.dirname(us_vix_path), exist_ok=True)
        df_us.to_csv(us_vix_path, index=False)
        
        print(f"✅ VIX 데이터 변환 완료: {us_vix_path} ({len(df_us)} 행)")
        return True
        
    except Exception as e:
        print(f"❌ VIX 데이터 변환 실패: {e}")
        return False


def main():
    """메인 실행 함수"""
    success = convert_vix_data()
    if success:
        print("\n✅ VIX 데이터 변환 완료")
    else:
        print("\n❌ VIX 데이터 변환 실패")


if __name__ == "__main__":
    main()