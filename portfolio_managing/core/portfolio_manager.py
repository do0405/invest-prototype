# -*- coding: utf-8 -*-
"""
통합 포트폴리오 관리자
PositionTracker와 RiskManager를 통합하여 포트폴리오 전체를 관리
6개 기존 전략과의 통합 지원
"""

import os
import sys
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, Optional

# 프로젝트 루트 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from .position_tracker import PositionTracker
from .risk_manager import RiskManager
from .portfolio_utils import PortfolioUtils
from .portfolio_reporter import PortfolioReporter
from .price_calculator import PriceCalculator
from .exit_conditions import (
    calculate_profit_target_price,
    calculate_remaining_days,
    update_days_condition,
    check_complex_exit_condition,
    should_check_exit_from_next_day,
)
from config import RESULTS_VER2_DIR
from utils import ensure_dir
from .strategy_config import StrategyConfig

class PortfolioManager:
    """개별 전략 포트폴리오 관리 클래스"""
    
    def __init__(self, portfolio_name: str = "individual_portfolio", initial_capital: float = 100000):
        self.portfolio_name = portfolio_name
        self.initial_capital = initial_capital
        
        # 핵심 모듈 초기화
        self.position_tracker = PositionTracker(portfolio_name)
        self.risk_manager = RiskManager(portfolio_name)
        self.utils = PortfolioUtils(self)
        self.reporter = PortfolioReporter(self)
        
        # 포트폴리오 디렉토리 설정
        self.portfolio_dir = os.path.join(RESULTS_VER2_DIR, 'portfolio_management')
        ensure_dir(self.portfolio_dir)
        
        # 포트폴리오 설정 파일
        self.config_file = os.path.join(self.portfolio_dir, f'{portfolio_name}_config.json')
        
        # 설정 로드
        self.load_portfolio_config()
    
    def load_portfolio_config(self):
        """포트폴리오 설정 로드"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
            else:
                # 기본 설정
                self.config = {
                    'portfolio_name': self.portfolio_name,
                    'initial_capital': self.initial_capital,
                    'strategies': list(StrategyConfig.get_all_strategies()),
                    'created_date': datetime.now().strftime('%Y-%m-%d'),
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                self.save_portfolio_config()
        except Exception as e:
            print(f"⚠️ 포트폴리오 설정 로드 실패: {e}")
            self.config = {}
    
    def save_portfolio_config(self):
        """포트폴리오 설정 저장"""
        try:
            self.config['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ 포트폴리오 설정 저장 실패: {e}")
    
    def load_strategy_results(self, strategy_name: str) -> Optional[pd.DataFrame]:
        """전략 결과 파일 로드"""
        try:
            result_file = StrategyConfig.get_result_file_path(strategy_name, RESULTS_VER2_DIR)
            if result_file and os.path.exists(result_file):
                return pd.read_csv(result_file)
            return None
        except Exception as e:
            print(f"⚠️ {strategy_name} 결과 로드 실패: {e}")
            return None
    
    def process_strategy_signals(self, strategy_name: str, signals_df: pd.DataFrame) -> int:
        """전략 신호 처리"""
        try:
            strategy_config = StrategyConfig.get_strategy_config(strategy_name)
            if not strategy_config:
                print(f"⚠️ {strategy_name} 설정을 찾을 수 없습니다")
                return 0
            
            added_count = 0
            max_positions = strategy_config.get('max_positions', 5)
            
            # 현재 해당 전략의 포지션 수 확인
            current_positions = len(self.position_tracker.get_positions_by_strategy(strategy_name))
            available_slots = max_positions - current_positions
            
            if available_slots <= 0:
                print(f"⚠️ {strategy_name}: 최대 포지션 수 도달 ({current_positions}/{max_positions})")
                return 0
            
            # 상위 신호들만 처리
            top_signals = signals_df.head(available_slots)
            
            for _, signal in top_signals.iterrows():
                if self.utils.add_position_from_signal(strategy_name, signal, strategy_config):
                    added_count += 1
            
            return added_count
            
        except Exception as e:
            print(f"❌ {strategy_name} 신호 처리 실패: {e}")
            return 0
    
    def process_and_update_strategy_files(self):
        """전략 결과 파일들을 처리하고 업데이트합니다."""
        try:
            print("\n🔄 전략 결과 파일 처리 및 업데이트 시작...")
            
            buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
            sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
            
            # buy 디렉토리 처리
            if os.path.exists(buy_dir):
                for file_name in os.listdir(buy_dir):
                    if file_name.endswith('_results.csv'):
                        file_path = os.path.join(buy_dir, file_name)
                        self._process_strategy_file(file_path, 'buy')
            
            # sell 디렉토리 처리
            if os.path.exists(sell_dir):
                for file_name in os.listdir(sell_dir):
                    if file_name.endswith('_results.csv'):
                        file_path = os.path.join(sell_dir, file_name)
                        self._process_strategy_file(file_path, 'sell')
            
            print("✅ 전략 결과 파일 처리 완료")
            
        except Exception as e:
            print(f"❌ 전략 결과 파일 처리 실패: {e}")
    
    def _process_strategy_file(self, file_path: str, position_type: str):
        """개별 전략 파일을 처리합니다."""
        try:
            if not os.path.exists(file_path):
                return
            
            df = pd.read_csv(file_path)
            if df.empty:
                return
            
            print(f"📊 처리 중: {os.path.basename(file_path)}")
            
            updated = False
            rows_to_remove = []
            
            for idx, row in df.iterrows():
                # 2-1. '시장가'를 다음날 시가로 변경
                if row['매수가'] == '시장가':
                    next_day_open = PriceCalculator.get_next_day_open_price(row['종목명'], row['매수일'])
                    if next_day_open:
                        df.loc[idx, '매수가'] = next_day_open
                        updated = True
                        print(f"  📈 {row['종목명']}: 시장가 → ${next_day_open:.2f}")
                
                # n% 수익 목표가 계산
                if 'n% 수익' in str(row['차익실현']):
                    target_price = calculate_profit_target_price(row)
                    if target_price:
                        df.loc[idx, '차익실현'] = str(row['차익실현']).replace('n% 수익', f'{target_price:.2f}')
                        updated = True
                
                # 2-2. n일 후 청산/강제매도 처리
                if 'n일 후' in str(row['차익실현']):
                    remaining_days = calculate_remaining_days(row['매수일'], row['차익실현'])
                    
                    if remaining_days == -1:  # 삭제 조건
                        rows_to_remove.append(idx)
                        print(f"  🗑️ {row['종목명']}: 보유기간 만료로 삭제")
                    elif remaining_days >= 0:  # 일수 업데이트
                        updated_condition = update_days_condition(row['차익실현'], remaining_days)
                        df.loc[idx, '차익실현'] = updated_condition
                        updated = True
                        print(f"  ⏰ {row['종목명']}: {remaining_days}일 남음")
            
            # 만료된 행 제거
            if rows_to_remove:
                df = df.drop(rows_to_remove).reset_index(drop=True)
                updated = True
            
            # 파일 저장
            if updated:
                df.to_csv(file_path, index=False)
                print(f"  ✅ {os.path.basename(file_path)} 업데이트 완료")
            
        except Exception as e:
            print(f"❌ 파일 처리 실패 ({file_path}): {e}")
    
    

    @staticmethod
    def run_individual_strategy_portfolios():
        """개별 전략 포트폴리오 관리"""
        try:
            print("🚀 개별 전략 포트폴리오 관리 시작")
        
            for strategy_name in StrategyConfig.get_all_strategies():
                print(f"\n📊 {strategy_name} 개별 처리 중...")

            # 개별 전략용 포트폴리오 매니저
                portfolio_manager = PortfolioManager(f"{strategy_name}_portfolio")
            
            # 해당 전략만 처리
                strategy_results = portfolio_manager.load_strategy_results(strategy_name)
                success = False
                if strategy_results is not None:
                    added_count = portfolio_manager.process_strategy_signals(strategy_name, strategy_results)
                    success = added_count > 0
            
                if success:
                # 청산 조건 확인
                    # 356번째 줄을 다음과 같이 수정
                    portfolio_manager.utils.check_and_process_exit_conditions()
                
                # 포트폴리오 업데이트
                    portfolio_manager.position_tracker.update_positions()
                
                # 개별 리포트 생성
                    portfolio_manager.reporter.generate_report()
        
            print("✅ 개별 전략 포트폴리오 관리 완료")
        
        except Exception as e:
            print(f"❌ 개별 전략 포트폴리오 관리 실패: {e}")
    
    def monitor_and_process_trading_signals(self):
        """매매 신호를 모니터링하고 조건 충족 시 데이터를 처리합니다."""
        try:
            print("\n🔍 매매 신호 모니터링 시작...")
            
            buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
            sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
            
            # Buy 폴더 처리
            if os.path.exists(buy_dir):
                self._process_signals(buy_dir, "BUY")

            # Sell 폴더 처리
            if os.path.exists(sell_dir):
                self._process_signals(sell_dir, "SELL")
            
            print("✅ 매매 신고 모니터링 완료")
            
        except Exception as e:
            print(f"❌ 매매 신호 모니터링 실패: {e}")
    
    def _process_signals(self, target_dir: str, position_type: str):
        """지정 폴더의 매매 신호를 처리합니다."""
        try:
            for file_name in os.listdir(target_dir):
                if file_name.endswith('_results.csv'):
                    file_path = os.path.join(target_dir, file_name)
                    self._check_exit_conditions(file_path, position_type)

        except Exception as e:
            print(f"❌ {position_type} 신호 처리 실패: {e}")


    



    def _check_exit_conditions(self, file_path: str, position_type: str):
        """지정 포지션의 청산 조건을 확인합니다."""
        try:
            if not os.path.exists(file_path):
                return

            df = pd.read_csv(file_path)
            if df.empty:
                return

            print(f"📊 {position_type} 신호 확인 중: {os.path.basename(file_path)}")

            rows_to_remove = []
            updated = False

            for idx, row in df.iterrows():
                symbol = row['종목명']
                purchase_price = PriceCalculator.parse_price(row['매수가'])
                purchase_date = row.get('매수일', '')

                if purchase_price is None or not purchase_date:
                    continue

                if not should_check_exit_from_next_day(purchase_date):
                    continue

                recent_data = PriceCalculator.get_recent_price_data(symbol)
                if recent_data is None:
                    continue

                recent_close = recent_data.get('close')

                if recent_close and purchase_price:
                    if position_type == 'BUY':
                        return_pct = ((recent_close - purchase_price) / purchase_price) * 100
                    else:
                        return_pct = ((purchase_price - recent_close) / purchase_price) * 100
                    df.loc[idx, '수익률'] = return_pct
                    updated = True
                    print(f"  📊 {symbol}: 수익률 업데이트 {return_pct:.2f}%")

                should_exit, exit_reason = check_complex_exit_condition(row, recent_data, position_type)
                final_return = return_pct if 'return_pct' in locals() else 0

                if should_exit:
                    log_type = 'BUY' if position_type == 'BUY' else 'SELL'
                    self.utils.log_exit_transaction(symbol, log_type, purchase_price, recent_close, final_return, exit_reason)
                    rows_to_remove.append(idx)
                    print(f"  🔄 {symbol}: {exit_reason} - 최종 수익률 {final_return:.2f}% - 데이터 삭제")

            if rows_to_remove:
                df = df.drop(rows_to_remove).reset_index(drop=True)
                updated = True

            if updated:
                df.to_csv(file_path, index=False)
                json_file = file_path.replace('.csv', '.json')
                df.to_json(json_file, orient='records', force_ascii=False, indent=2)
                print(f"  ✅ {os.path.basename(file_path)} 업데이트 완료")

        except Exception as e:

            print(f"❌ {position_type} 청산 조건 확인 실패 ({file_path}): {e}")


