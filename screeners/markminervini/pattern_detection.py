

from __future__ import annotations

import os
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime

import pandas as pd
import numpy as np

# 수학적 함수들을 별도 모듈에서 임포트
from .mathematical_functions import (
    kernel_smoothing,
    extract_peaks_troughs,
    calculate_amplitude_contraction,
    quadratic_fit_cup,
    bezier_curve_correlation
)

# 로깅 설정
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# 패턴 감지 함수들을 별도 모듈에서 임포트
from .pattern_detection_core import detect_vcp, detect_cup_and_handle


# -----------------------------------------------------
# Batch analysis
# -----------------------------------------------------

from config import MARKMINERVINI_RESULTS_DIR


def analyze_tickers_from_results(results_dir: str, data_dir: str, output_dir: str = MARKMINERVINI_RESULTS_DIR) -> pd.DataFrame:
    """CSV 파일에서 티커 목록을 읽고 패턴을 감지하여 결과를 반환합니다.
    
    Args:
        results_dir: 재무 결과 파일이 있는 디렉토리 경로
        data_dir: 주가 데이터 CSV 파일이 있는 디렉토리 경로
        output_dir: 결과를 저장할 디렉토리 경로
        
    Returns:
        pd.DataFrame: 패턴 감지 결과
        
    Raises:
        FileNotFoundError: 결과 파일이 존재하지 않을 경우
    """
    os.makedirs(output_dir, exist_ok=True)
    results_file = os.path.join(results_dir, "advanced_financial_results.csv")
    if not os.path.exists(results_file):
        raise FileNotFoundError(f"결과 파일을 찾을 수 없습니다: {results_file}")

    logger.info(f"재무 결과 파일 로드 중: {results_file}")
    results_df = pd.read_csv(results_file)
    logger.info(f"총 {len(results_df)}개 종목에 대한 패턴 분석 시작")
    
    analysis = []

    for _, row in results_df.iterrows():
        symbol = row['symbol']
        fin_met_count = row.get('fin_met_count', 0)
        # fin_met_count 조건 제거 - 모든 종목에 대해 패턴 분석 수행
        file_path = os.path.join(data_dir, f"{symbol}.csv")
        if not os.path.exists(file_path):
            continue
        from utils.screener_utils import read_csv_flexible
        df = read_csv_flexible(file_path, required_columns=['close', 'volume', 'date', 'high', 'low'])
        if df is None:
            continue
        date_col = next((c for c in df.columns if c.lower() in ['date', '날짜', '일자']), None)
        if not date_col:
            continue
        df[date_col] = pd.to_datetime(df[date_col], utc=True)
        df.set_index(date_col, inplace=True)
        col_map = {'high': ['high', 'High', '고가'], 'low': ['low', 'Low', '저가'], 'close': ['close', 'Close', '종가'], 'volume': ['volume', 'Volume', '거래량']}
        found = {}
        for k, names in col_map.items():
            for c in df.columns:
                if c.lower() in [n.lower() for n in names]:
                    found[k] = c
                    break
        if len(found) < 4:
            continue
        df = df.rename(columns={v: k for k, v in found.items()})

        vcp = detect_vcp(df)
        cup = detect_cup_and_handle(df)
        if not vcp and not cup:
            continue

        analysis.append({
            'symbol': symbol,
            'fin_met_count': fin_met_count,
            'vcp': vcp,
            'cup_handle': cup,
        })

    if not analysis:
        return pd.DataFrame()

    out_df = pd.DataFrame(analysis)
    out_df = out_df.sort_values(['vcp', 'cup_handle'], ascending=False)
    out_file = os.path.join(output_dir, 'pattern_detection_results.csv')
    out_df.to_csv(out_file, index=False, encoding='utf-8-sig')
    out_df.to_json(out_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
    return out_df


# 배치 분석 기능을 별도 모듈에서 임포트
from .batch_analysis import run_pattern_detection_on_financial_results


def main():
    """메인 실행 함수"""
    try:
        run_pattern_detection_on_financial_results()
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}")
        print(f"❌ 오류 발생: {e}")
        return 1
    return 0


if __name__ == "__main__":
    main()