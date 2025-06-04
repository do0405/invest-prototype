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
from config import RESULTS_VER2_DIR
from utils import ensure_dir
from .strategy_config import StrategyConfig

class PortfolioManager:
    """통합 포트폴리오 관리 클래스 - 6개 전략 통합 지원"""
    
    def __init__(self, portfolio_name: str = "main_portfolio", initial_capital: float = 100000):
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
            current_positions = len(self.position_tracker.get_strategy_positions(strategy_name))
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
                    next_day_open = self._get_next_day_open_price(row['종목명'], row['매수일'])
                    if next_day_open:
                        df.loc[idx, '매수가'] = next_day_open
                        updated = True
                        print(f"  📈 {row['종목명']}: 시장가 → ${next_day_open:.2f}")
                
                # n% 수익 목표가 계산
                if 'n% 수익' in str(row['차익실현']):
                    target_price = self._calculate_profit_target_price(row)
                    if target_price:
                        df.loc[idx, '차익실현'] = str(row['차익실현']).replace('n% 수익', f'{target_price:.2f}')
                        updated = True
                
                # 2-2. n일 후 청산/강제매도 처리
                if 'n일 후' in str(row['차익실현']):
                    remaining_days = self._calculate_remaining_days(row['매수일'], row['차익실현'])
                    
                    if remaining_days == -1:  # 삭제 조건
                        rows_to_remove.append(idx)
                        print(f"  🗑️ {row['종목명']}: 보유기간 만료로 삭제")
                    elif remaining_days >= 0:  # 일수 업데이트
                        updated_condition = self._update_days_condition(row['차익실현'], remaining_days)
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
    
    def _get_next_day_open_price(self, symbol: str, purchase_date: str) -> Optional[float]:
        """매수일 다음날의 시가를 가져옵니다."""
        try:
            import yfinance as yf
            from datetime import datetime, timedelta
            
            # 매수일 다음날 계산
            purchase_dt = datetime.strptime(purchase_date, '%Y-%m-%d')
            next_day = purchase_dt + timedelta(days=1)
            
            # 주말/공휴일 고려하여 최대 5일까지 확인
            for i in range(5):
                check_date = next_day + timedelta(days=i)
                end_date = check_date + timedelta(days=1)
                
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=check_date.strftime('%Y-%m-%d'), 
                                    end=end_date.strftime('%Y-%m-%d'))
                
                if not hist.empty:
                    return float(hist['Open'].iloc[0])
            
            return None
            
        except Exception as e:
            print(f"⚠️ {symbol} 다음날 시가 조회 실패: {e}")
            return None
    
    def _calculate_profit_target_price(self, row) -> Optional[float]:
        """n% 수익 목표가를 계산합니다."""
        try:
            import re
            
            # 매수가 확인
            if row['매수가'] == '시장가':
                return None  # 시장가는 먼저 처리되어야 함
            
            purchase_price = float(row['매수가'])
            
            # 차익실현 조건에서 수익률 추출
            condition = str(row['차익실현'])
            
            # "4% 수익" 같은 패턴 찾기
            profit_match = re.search(r'(\d+)% 수익', condition)
            if profit_match:
                profit_pct = float(profit_match.group(1)) / 100
                target_price = purchase_price * (1 + profit_pct)
                return target_price
            
            return None
            
        except Exception as e:
            print(f"⚠️ 수익 목표가 계산 실패: {e}")
            return None
    
    def _calculate_remaining_days(self, purchase_date: str, exit_condition: str) -> int:
        """남은 보유일을 계산합니다."""
        try:
            import re
            from datetime import datetime
            
            # 현재 날짜와 매수일 차이 계산
            purchase_dt = datetime.strptime(purchase_date, '%Y-%m-%d')
            current_dt = datetime.now()
            days_held = (current_dt - purchase_dt).days
            
            # 조건에서 원래 보유일 추출
            condition = str(exit_condition)
            
            # "6일 후 강제매도" 또는 "3일 후 청산" 패턴 찾기
            days_match = re.search(r'(\d+)일 후', condition)
            if days_match:
                original_days = int(days_match.group(1))
                remaining_days = original_days - days_held
                return remaining_days
            
            return 0
            
        except Exception as e:
            print(f"⚠️ 남은 일수 계산 실패: {e}")
            return 0
    
    def _update_days_condition(self, original_condition: str, remaining_days: int) -> str:
        """일수 조건을 업데이트합니다."""
        try:
            import re
            
            # 원래 조건에서 일수 부분만 업데이트
            condition = str(original_condition)
            
            # "6일 후" → "5일 후" 형태로 변경
            updated_condition = re.sub(r'\d+일 후', f'{remaining_days}일 후', condition)
            
            return updated_condition
            
        except Exception as e:
            print(f"⚠️ 일수 조건 업데이트 실패: {e}")
            return original_condition
    

    def run_integrated_portfolio_management():
        """통합 포트폴리오 관리 실행"""
        try:
            print("🚀 통합 포트폴리오 관리 시작")
        
        # 포트폴리오 매니저 초기화
            portfolio_manager = PortfolioManager()
        
        # 모든 전략 처리
            for strategy_name in StrategyConfig.get_all_strategies():
                print(f"\n📊 {strategy_name} 처리 중...")
                strategy_results = portfolio_manager.load_strategy_results(strategy_name)
                if strategy_results is not None:
                    portfolio_manager.process_strategy_signals(strategy_name, strategy_results)
        
        # 청산 조건 확인
            portfolio_manager.utils.check_and_process_exit_conditions()
        
        # 포트폴리오 업데이트
            portfolio_manager.position_tracker.update_positions()
        
        # 요약 출력
            summary = portfolio_manager.utils.get_portfolio_summary()
            print(f"\n📈 포트폴리오 현황:")
            print(f"   총 가치: ${summary.get('current_value', 0):,.2f}")
            print(f"   총 수익: ${summary.get('total_return', 0):,.2f} ({summary.get('total_return_pct', 0):.2f}%)")
            print(f"   활성 포지션: {summary.get('positions', {}).get('total_positions', 0)}개")
        
        # 리포트 생성
            portfolio_manager.reporter.generate_report()
        
            print("✅ 통합 포트폴리오 관리 완료")
        
        except Exception as e:
            print(f"❌ 통합 포트폴리오 관리 실패: {e}")

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
                self._process_buy_signals(buy_dir)
            
            # Sell 폴더 처리
            if os.path.exists(sell_dir):
                self._process_sell_signals(sell_dir)
            
            print("✅ 매매 신고 모니터링 완료")
            
        except Exception as e:
            print(f"❌ 매매 신호 모니터링 실패: {e}")
    
    def _process_buy_signals(self, buy_dir: str):
        """Buy 폴더의 매매 신호를 처리합니다."""
        try:
            for file_name in os.listdir(buy_dir):
                if file_name.endswith('_results.csv'):
                    file_path = os.path.join(buy_dir, file_name)
                    self._check_buy_exit_conditions(file_path)
                    
        except Exception as e:
            print(f"❌ Buy 신호 처리 실패: {e}")
    
    def _process_sell_signals(self, sell_dir: str):
        """Sell 폴더의 매매 신호를 처리합니다."""
        try:
            for file_name in os.listdir(sell_dir):
                if file_name.endswith('_results.csv'):
                    file_path = os.path.join(sell_dir, file_name)
                    self._check_sell_exit_conditions(file_path)
                    
        except Exception as e:
            print(f"❌ Sell 신호 처리 실패: {e}")

    def _check_sell_exit_conditions(self, file_path: str):
        """Sell 포지션(SHORT)의 청산 조건을 확인합니다."""
        try:
            if not os.path.exists(file_path):
                return
            
            df = pd.read_csv(file_path)
            if df.empty:
                return
            
            print(f"📊 Sell 신호 확인 중: {os.path.basename(file_path)}")
            
            rows_to_remove = []
            updated = False
            
            for idx, row in df.iterrows():
                symbol = row['종목명']
                purchase_price = self._parse_price(row['매수가'])
                stop_loss = self._parse_price(row['손절매'])
                profit_protection = self._parse_price(row['수익보호'])
                profit_taking = self._parse_price(row['차익실현'])
                
                if purchase_price is None:
                    continue
                
                # 최근 가격 데이터 가져오기
                recent_data = self._get_recent_price_data(symbol)
                if recent_data is None:
                    continue
                
                recent_high = recent_data.get('high')
                recent_low = recent_data.get('low')
                recent_close = recent_data.get('close')
                
                # 수익률 업데이트 (SHORT 포지션)
                if recent_close and purchase_price:
                    return_pct = ((purchase_price - recent_close) / purchase_price) * 100
                    df.loc[idx, '수익률'] = return_pct
                    updated = True
                
                # SHORT 포지션 청산 조건 확인
                should_exit = False
                exit_reason = ""
                
                # 1. 최근 고가가 손절매가 위로 올라간 경우
                if stop_loss and recent_high and recent_high >= stop_loss:
                    should_exit = True
                    exit_reason = f"손절매 조건 (고가 {recent_high:.2f} >= 손절매 {stop_loss:.2f})"
                
                # 2. 최근 고가가 수익보호가 위로 올라간 경우
                elif profit_protection and recent_high and recent_high >= profit_protection:
                    should_exit = True
                    exit_reason = f"수익보호 조건 (고가 {recent_high:.2f} >= 수익보호 {profit_protection:.2f})"
                
                # 3. 최근 저가가 차익실현가 아래로 떨어진 경우
                elif profit_taking and recent_low and recent_low <= profit_taking:
                    should_exit = True
                    exit_reason = f"차익실현 조건 (저가 {recent_low:.2f} <= 차익실현 {profit_taking:.2f})"
                
                if should_exit:
                    rows_to_remove.append(idx)
                    print(f"  🔄 {symbol}: {exit_reason} - 데이터 삭제")
            
            # 조건 충족 행 제거
            if rows_to_remove:
                df = df.drop(rows_to_remove).reset_index(drop=True)
                updated = True
            
            # 파일 저장
            if updated:
                df.to_csv(file_path, index=False)
                print(f"  ✅ {os.path.basename(file_path)} 업데이트 완료")
                
        except Exception as e:
            print(f"❌ Sell 청산 조건 확인 실패 ({file_path}): {e}")

    def _check_buy_exit_conditions(self, file_path: str):
        """Buy 포지션의 청산 조건을 확인합니다."""
        try:
            if not os.path.exists(file_path):
                return
            
            df = pd.read_csv(file_path)
            if df.empty:
                return
            
            print(f"📊 Buy 신호 확인 중: {os.path.basename(file_path)}")
            
            rows_to_remove = []
            updated = False
            
            for idx, row in df.iterrows():
                symbol = row['종목명']
                purchase_price = self._parse_price(row['매수가'])
                stop_loss = self._parse_price(row['손절매'])
                profit_protection = self._parse_price(row['수익보호'])
                profit_taking = self._parse_price(row['차익실현'])
                
                if purchase_price is None:
                    continue
                
                # 최근 가격 데이터 가져오기
                recent_data = self._get_recent_price_data(symbol)
                if recent_data is None:
                    continue
                
                recent_high = recent_data.get('high')
                recent_low = recent_data.get('low')
                recent_close = recent_data.get('close')
                
                # 수익률 업데이트
                if recent_close and purchase_price:
                    return_pct = ((recent_close - purchase_price) / purchase_price) * 100
                    df.loc[idx, '수익률'] = return_pct
                    updated = True
                
                # Buy 포지션 청산 조건 확인
                should_exit = False
                exit_reason = ""
                
                # 1. 최근 저가가 손절매가 아래로 떨어진 경우
                if stop_loss and recent_low and recent_low <= stop_loss:
                    should_exit = True
                    exit_reason = f"손절매 조건 (저가 {recent_low:.2f} <= 손절매 {stop_loss:.2f})"
                
                # 2. 최근 저가가 수익보호가 아래로 떨어진 경우
                elif profit_protection and recent_low and recent_low <= profit_protection:
                    should_exit = True
                    exit_reason = f"수익보호 조건 (저가 {recent_low:.2f} <= 수익보호 {profit_protection:.2f})"
                
                # 3. 최근 고가가 차익실현가를 넘어간 경우
                elif profit_taking and recent_high and recent_high >= profit_taking:
                    should_exit = True
                    exit_reason = f"차익실현 조건 (고가 {recent_high:.2f} >= 차익실현 {profit_taking:.2f})"
                
                if should_exit:
                    rows_to_remove.append(idx)
                    print(f"  🔄 {symbol}: {exit_reason} - 데이터 삭제")
            
            # 조건 충족 행 제거
            if rows_to_remove:
                df = df.drop(rows_to_remove).reset_index(drop=True)
                updated = True
            
            # 파일 저장
            if updated:
                df.to_csv(file_path, index=False)
                print(f"  ✅ {os.path.basename(file_path)} 업데이트 완료")
                
        except Exception as e:
            print(f"❌ Buy 청산 조건 확인 실패 ({file_path}): {e}")

    def _parse_price(self, price_str) -> Optional[float]:
        """가격 문자열을 파싱합니다."""
        try:
            if pd.isna(price_str) or price_str == '없음' or price_str == '시장가':
                return None
            
            # 숫자가 아닌 문자 제거 후 파싱
            import re
            price_clean = re.sub(r'[^0-9.-]', '', str(price_str))
            if price_clean:
                return float(price_clean)
            return None
            
        except (ValueError, TypeError):
            return None
    
    def _get_recent_price_data(self, symbol: str) -> Optional[Dict]:
        """최근 가격 데이터를 가져옵니다."""
        try:
            import yfinance as yf
            from datetime import datetime, timedelta
            
            # 최근 5일간 데이터 가져오기
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)
            
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date.strftime('%Y-%m-%d'), 
                                end=end_date.strftime('%Y-%m-%d'))
            
            if hist.empty:
                return None
            
            # 가장 최근 데이터 반환
            latest = hist.iloc[-1]
            return {
                'high': float(latest['High']),
                'low': float(latest['Low']),
                'close': float(latest['Close']),
                'open': float(latest['Open']),
                'volume': float(latest['Volume'])
            }
            
        except Exception as e:
            print(f"⚠️ {symbol} 가격 데이터 조회 실패: {e}")
            return None
