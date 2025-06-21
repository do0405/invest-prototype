# -*- coding: utf-8 -*-
"""
변동성 스큐 역전 전략 포트폴리오 통합 모듈
"""

import os
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# 프로젝트 루트 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
sys.path.insert(0, project_root)

from config import OPTION_VOLATILITY_DIR, OPTION_VOLATILITY_RESULTS_DIR, RESULTS_VER2_DIR
from utils import ensure_dir
from screeners.option_volatility.volatility_skew_screener import VolatilitySkewScreener

class VolatilitySkewPortfolioStrategy:
    """
    변동성 스큐 역전 전략을 포트폴리오 시스템에 통합하는 클래스
    """
    
    def __init__(self):
        self.screener = VolatilitySkewScreener()
        self.strategy_name = "volatility_skew"
        
        # 결과 저장 경로
        ensure_dir(OPTION_VOLATILITY_DIR)
        ensure_dir(OPTION_VOLATILITY_RESULTS_DIR)
        ensure_dir(os.path.join(RESULTS_VER2_DIR, 'buy'))

        self.portfolio_file = os.path.join(OPTION_VOLATILITY_DIR, 'portfolio_signals.csv')
        self.results_file = os.path.join(OPTION_VOLATILITY_RESULTS_DIR, 'volatility_skew_results.csv')

        self.max_positions = 10
        self.max_position_size = 0.1
        self.risk_per_position = 0.02
    
    def run_screening_and_portfolio_creation(self) -> Tuple[List[Dict], str]:
        """
        스크리닝을 실행하고 포트폴리오 신호를 생성합니다.
        """
        try:
            print("\n🔍 변동성 스큐 스크리닝 시작...")
            
            # 스크리닝 실행
            screening_results, screening_file = self.screener.run_screening()
            
            if not screening_results:
                print("⚠️ 스크리닝 결과가 없습니다.")
                return [], ""
            
            # 포트폴리오 신호 생성 (간소화)
            portfolio_signals = self._create_portfolio_signals(screening_results)
            
            # 신호 저장
            signals_file = self._save_portfolio_signals(portfolio_signals)
            
            print(f"✅ 변동성 스큐 포트폴리오 신호 생성 완료: {len(portfolio_signals)}개")
            return portfolio_signals, signals_file
            
        except Exception as e:
            print(f"❌ 변동성 스큐 포트폴리오 생성 오류: {e}")
            return [], ""
    
    def _create_portfolio_signals(self, screening_results: List[Dict]) -> List[Dict]:
        """스크리닝 결과를 표준 포트폴리오 포맷으로 변환합니다."""

        sorted_results = sorted(screening_results, key=lambda x: x['skew_index'])
        selected_stocks = sorted_results[:self.max_positions]

        portfolio_signals = []
        for stock in selected_stocks:
            base_weight = 1.0 / len(selected_stocks)
            confidence_multiplier = stock.get('confidence_score', 1.0)
            final_weight = min(base_weight * confidence_multiplier, self.max_position_size)

            portfolio_signals.append({
                '종목명': stock['symbol'],
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': '시장가',
                '비중(%)': round(final_weight * 100, 2),
                '수익률': 0.0,
                '차익실현': f"{stock['expected_return']*100:.0f}% 수익",
                '손절매': '15% 손절',
                '수익보호': '없음',
                '롱여부': True
            })

        return portfolio_signals
    
    def _save_portfolio_signals(self, signals: List[Dict]) -> str:
        """
        포트폴리오 신호를 CSV 파일로 저장합니다.
        """
        if not signals:
            return ""
        
        # DataFrame 생성
        df = pd.DataFrame(signals)
        
        # 파일명 생성
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"volatility_skew_portfolio_{timestamp}.csv"
        filepath = os.path.join(OPTION_VOLATILITY_DIR, filename)
        buy_result_path = self.results_file
        
        # CSV 저장
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        df.to_csv(buy_result_path, index=False, encoding='utf-8-sig')
        df.to_json(buy_result_path.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
        
        # 최신 파일로도 저장 (포트폴리오 매니저가 읽을 수 있도록)
        latest_filepath = self.portfolio_file
        df.to_csv(latest_filepath, index=False, encoding='utf-8-sig')
        
        print(f"💾 포트폴리오 신호 저장: {filepath}")
        return buy_result_path
    
    def get_latest_signals(self) -> Optional[pd.DataFrame]:
        """
        최신 포트폴리오 신호를 반환합니다.
        """
        try:
            if os.path.exists(self.portfolio_file):
                return pd.read_csv(self.portfolio_file)
            return None
        except Exception as e:
            print(f"❌ 신호 파일 읽기 오류: {e}")
            return None
    


def run_volatility_skew_portfolio_strategy() -> Tuple[List[Dict], str]:
    """
    변동성 스큐 포트폴리오 전략 실행 함수 (main.py에서 호출용)
    """
    strategy = VolatilitySkewPortfolioStrategy()
    return strategy.run_screening_and_portfolio_creation()

