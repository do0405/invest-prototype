"""Mark Minervini 스크리너 배치 분석 모듈

이 모듈은 재무 결과 파일에 있는 종목들에 대해 패턴 감지를 수행하고
결과를 CSV 및 JSON 파일로 저장하는 기능을 제공합니다.
"""

import os
import pandas as pd
import numpy as np
import yfinance as yf
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from .pattern_detection_core import detect_vcp, detect_cup_and_handle

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pattern_detection_on_financial_results(input_file: str = "advanced_financial_results.csv",
                                              output_csv: str = "pattern_detection_results.csv",
                                              output_json: str = "pattern_detection_results.json") -> None:
    """재무 결과 파일의 종목들에 대해 패턴 감지를 수행하고 결과를 저장
    
    Args:
        input_file: 입력 재무 결과 파일 경로
        output_csv: 출력 CSV 파일 경로
        output_json: 출력 JSON 파일 경로
    """
    try:
        # 재무 결과 파일 존재 여부 확인
        if not os.path.exists(input_file):
            logger.warning(f"재무 결과 파일이 존재하지 않습니다: {input_file}")
            # 빈 결과 파일 생성
            empty_df = pd.DataFrame()
            empty_df.to_csv(output_csv, index=False)
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            return
        
        # 재무 결과 파일 읽기
        df = pd.read_csv(input_file)
        logger.info(f"재무 결과 파일에서 {len(df)}개 종목을 로드했습니다.")
        
        # 결과 저장용 리스트
        results = []
        
        # 통계 변수
        total_processed = 0
        vcp_detected = 0
        cup_handle_detected = 0
        both_patterns = 0
        
        for index, row in df.iterrows():
            try:
                symbol = row['Symbol']
                logger.info(f"처리 중: {symbol} ({index + 1}/{len(df)})")
                
                # 주가 데이터 가져오기 (1년치)
                end_date = datetime.now()
                start_date = end_date - timedelta(days=365)
                
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date, end=end_date)
                
                if hist.empty or len(hist) < 60:
                    logger.warning(f"{symbol}: 충분한 데이터가 없습니다.")
                    continue
                
                # 컬럼명 매핑 (yfinance 형식에 맞춤)
                hist_df = hist.copy()
                hist_df.columns = [col.lower() for col in hist_df.columns]
                hist_df = hist_df.reset_index()
                
                # 패턴 감지
                is_vcp = detect_vcp(hist_df)
                is_cup_handle = detect_cup_and_handle(hist_df)
                
                # 결과 저장
                result = {
                    'Symbol': symbol,
                    'Company_Name': row.get('Company Name', ''),
                    'VCP_Pattern': is_vcp,
                    'Cup_Handle_Pattern': is_cup_handle,
                    'Both_Patterns': is_vcp and is_cup_handle,
                    'Current_Price': hist_df['close'].iloc[-1] if not hist_df.empty else None,
                    'Analysis_Date': datetime.now().strftime('%Y-%m-%d'),
                    'Data_Points': len(hist_df)
                }
                
                # 재무 데이터 추가
                for col in df.columns:
                    if col not in ['Symbol', 'Company Name']:
                        result[col] = row[col]
                
                results.append(result)
                
                # 통계 업데이트
                total_processed += 1
                if is_vcp:
                    vcp_detected += 1
                if is_cup_handle:
                    cup_handle_detected += 1
                if is_vcp and is_cup_handle:
                    both_patterns += 1
                
            except Exception as e:
                logger.error(f"{symbol} 처리 중 오류: {str(e)}")
                continue
        
        if not results:
            logger.warning("처리된 결과가 없습니다.")
            return
        
        # DataFrame으로 변환
        results_df = pd.DataFrame(results)
        
        # VCP 패턴 우선, Cup&Handle 패턴 차순으로 정렬
        results_df = results_df.sort_values(
            ['VCP_Pattern', 'Cup_Handle_Pattern', 'Both_Patterns'], 
            ascending=[False, False, False]
        )
        
        # CSV 파일로 저장
        results_df.to_csv(output_csv, index=False)
        logger.info(f"결과가 {output_csv}에 저장되었습니다.")
        
        # JSON 파일로 저장
        results_json = {
            'analysis_summary': {
                'total_symbols_processed': total_processed,
                'vcp_patterns_detected': vcp_detected,
                'cup_handle_patterns_detected': cup_handle_detected,
                'both_patterns_detected': both_patterns,
                'vcp_percentage': round(vcp_detected / total_processed * 100, 1) if total_processed > 0 else 0,
                'cup_handle_percentage': round(cup_handle_detected / total_processed * 100, 1) if total_processed > 0 else 0,
                'both_patterns_percentage': round(both_patterns / total_processed * 100, 1) if total_processed > 0 else 0,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'results': results
        }
        
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(results_json, f, indent=2, ensure_ascii=False)
        logger.info(f"결과가 {output_json}에 저장되었습니다.")
        
        # 결과 요약 출력
        logger.info(f"\n=== 패턴 감지 결과 요약 ===")
        logger.info(f"총 처리된 심볼: {total_processed}")
        logger.info(f"VCP 패턴 감지: {vcp_detected}개 ({vcp_detected/total_processed*100:.1f}%)")
        logger.info(f"Cup&Handle 패턴 감지: {cup_handle_detected}개 ({cup_handle_detected/total_processed*100:.1f}%)")
        logger.info(f"두 패턴 모두 감지: {both_patterns}개 ({both_patterns/total_processed*100:.1f}%)")
        
        # 상위 결과 출력
        if vcp_detected > 0:
            vcp_stocks = results_df[results_df['VCP_Pattern'] == True]['Symbol'].head(10).tolist()
            logger.info(f"VCP 패턴 감지된 상위 10개 심볼: {vcp_stocks}")
        
        if cup_handle_detected > 0:
            cup_stocks = results_df[results_df['Cup_Handle_Pattern'] == True]['Symbol'].head(10).tolist()
            logger.info(f"Cup&Handle 패턴 감지된 상위 10개 심볼: {cup_stocks}")
        
    except Exception as e:
        logger.error(f"배치 분석 중 오류 발생: {str(e)}")
        raise

def main():
    """메인 실행 함수"""
    logger.info("Mark Minervini 패턴 감지 배치 분석을 시작합니다...")
    run_pattern_detection_on_financial_results()
    logger.info("패턴 감지 배치 분석이 완료되었습니다.")

if __name__ == "__main__":
    main()