# 포트폴리오 관리 시스템 메인 모듈
# 모든 핵심 컴포넌트를 통합하여 사용하기 쉬운 인터페이스 제공

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import traceback

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 핵심 모듈 임포트
from portfolio_management.core.position_tracker import PositionTracker
from portfolio_management.core.risk_manager import RiskManager
from portfolio_management.core.order_manager import OrderManager, OrderSide, OrderType
from portfolio_management.core.performance_analyzer import PerformanceAnalyzer

from config import RESULTS_VER2_DIR
from utils import ensure_directory_exists, get_us_market_today

class PortfolioManager:
    """통합 포트폴리오 관리자"""
    
    def __init__(self, initial_capital: float = 100000.0):
        """
        Args:
            initial_capital: 초기 자본금 (기본 $100,000)
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.order_manager = OrderManager()
        self.position_tracker = PositionTracker()
        
        # 포트폴리오 상태 초기화
        self.update_portfolio_status()
        
        print(f"💼 포트폴리오 관리자 초기화 완료")
        print(f"   - 초기 자본금: ${initial_capital:,.0f}")
        
    def update_portfolio_status(self):
        """포트폴리오 상태 업데이트"""
        self.risk_manager = RiskManager(total_capital=self.initial_capital)
        self.performance_analyzer = PerformanceAnalyzer(initial_capital=self.initial_capital)
        self.is_initialized = False
        
    def initialize(self):
        """포트폴리오 관리자 초기화"""
        try:
            # 기존 데이터 로드
            self.position_tracker.load_positions()
            self.performance_analyzer.load_daily_performance()
            
            # 대기 중인 주문 확인
            self.order_manager.check_pending_orders()
            
            self.is_initialized = True
            print("✅ 포트폴리오 관리자 초기화 완료")
            
        except Exception as e:
            print(f"❌ 포트폴리오 관리자 초기화 오류: {e}")
            traceback.print_exc()
            
    def add_position(self, symbol: str, strategy: str, position_type: str = 'long',
                   entry_price: Optional[float] = None, quantity: Optional[int] = None,
                   stop_loss: Optional[float] = None, notes: str = "") -> bool:
        """새 포지션 추가
        
        Args:
            symbol: 종목 심볼
            strategy: 전략명
            position_type: 포지션 타입 ('long' or 'short')
            entry_price: 진입가 (None이면 현재가 사용)
            quantity: 수량 (None이면 리스크 기반 계산)
            stop_loss: 손절가
            notes: 메모
            
        Returns:
            성공 여부
        """
        try:
            # 현재가 조회
            if entry_price is None:
                entry_price = self.order_manager._get_current_price(symbol)
                if entry_price is None:
                    print(f"❌ {symbol} 현재가 조회 실패")
                    return False
                    
            # 수량 계산 (리스크 기반)
            if quantity is None:
                if stop_loss is None:
                    # 기본 손절가 설정 (5% 손실)
                    stop_loss = entry_price * (0.95 if position_type == 'long' else 1.05)
                    
                quantity, investment_amount = self.risk_manager.calculate_position_size(
                    symbol=symbol,
                    entry_price=entry_price,
                    stop_loss=stop_loss
                )
                
                if quantity <= 0:
                    print(f"❌ {symbol} 포지션 크기 계산 실패")
                    return False
                    
            # 시장가 주문 생성
            order_side = OrderSide.BUY if position_type == 'long' else OrderSide.SELL
            order_id = self.order_manager.create_market_order(
                symbol=symbol,
                side=order_side,
                quantity=quantity,
                strategy=strategy,
                notes=f"포지션 진입: {notes}"
            )
            
            if not order_id:
                print(f"❌ {symbol} 주문 생성 실패")
                return False
                
            # 포지션 추가
            success = self.position_tracker.add_position(
                symbol=symbol,
                strategy=strategy,
                position_type=position_type,
                entry_price=entry_price,
                quantity=quantity,
                stop_loss=stop_loss,
                notes=notes
            )
            
            if success:
                print(f"✅ {symbol} 포지션 추가 완료: {quantity}주 @ ${entry_price:.2f}")
                
                # Trailing Stop 설정 (수익이 나는 경우)
                self._update_trailing_stops()
                
            return success
            
        except Exception as e:
            print(f"❌ 포지션 추가 오류: {e}")
            traceback.print_exc()
            return False
            
    def close_position(self, symbol: str, reason: str = "수동 청산") -> bool:
        """포지션 청산
        
        Args:
            symbol: 종목 심볼
            reason: 청산 사유
            
        Returns:
            성공 여부
        """
        try:
            if symbol not in self.position_tracker.positions:
                print(f"❌ {symbol} 포지션을 찾을 수 없습니다.")
                return False
                
            position = self.position_tracker.positions[symbol]
            
            # 청산 주문 생성
            order_side = OrderSide.SELL if position.position_type == 'long' else OrderSide.BUY
            order_id = self.order_manager.create_market_order(
                symbol=symbol,
                side=order_side,
                quantity=position.quantity,
                strategy=position.strategy,
                notes=f"포지션 청산: {reason}"
            )
            
            if not order_id:
                print(f"❌ {symbol} 청산 주문 생성 실패")
                return False
                
            # 포지션 청산
            success = self.position_tracker.close_position(symbol, reason)
            
            if success:
                print(f"✅ {symbol} 포지션 청산 완료: {reason}")
                
            return success
            
        except Exception as e:
            print(f"❌ 포지션 청산 오류: {e}")
            traceback.print_exc()
            return False
            
    def update_portfolio(self):
        """포트폴리오 업데이트"""
        try:
            # 포지션 가격 업데이트
            self.position_tracker.update_all_prices()
            
            # 포트폴리오 상태 업데이트
            self.update_portfolio_status()
            
            return self.get_portfolio_status()
            
        except Exception as e:
            print(f"❌ 포트폴리오 업데이트 오류: {e}")
            print(traceback.format_exc())
            return None
            
    def _update_trailing_stops(self):
        """Trailing Stop 업데이트"""
        try:
            updated_stops = self.risk_manager.update_trailing_stops(self.position_tracker)
            
            if updated_stops:
                print(f"📈 {len(updated_stops)}개 포지션의 Trailing Stop 업데이트")
                
        except Exception as e:
            print(f"❌ Trailing Stop 업데이트 오류: {e}")
            
    def get_portfolio_status(self) -> Dict:
        """포트폴리오 현재 상태 조회
        
        Returns:
            포트폴리오 상태 정보
        """
        try:
            # 기본 요약
            portfolio_summary = self.position_tracker.get_portfolio_summary()
            
            # 포지션 데이터
            positions_df = self.position_tracker.get_positions_dataframe()
            
            # 성과 분석
            performance_data = self.performance_analyzer.generate_performance_report(
                positions_df, portfolio_summary
            )
            
            # 리스크 지표
            risk_metrics = self.risk_manager.get_risk_metrics(positions_df, portfolio_summary)
            
            # 주문 요약
            order_summary = self.order_manager.get_order_summary()
            
            return {
                'portfolio_summary': portfolio_summary,
                'performance_data': performance_data,
                'risk_metrics': risk_metrics,
                'order_summary': order_summary,
                'positions_count': len(positions_df),
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ 포트폴리오 상태 조회 오류: {e}")
            return {}
            
    def generate_reports(self) -> Dict[str, str]:
        """포트폴리오 리포트 생성
        
        Returns:
            생성된 리포트 파일 경로들
        """
        try:
            # 현재 상태 조회
            status = self.get_portfolio_status()
            
            portfolio_summary = status.get('portfolio_summary', {})
            performance_data = status.get('performance_data', {})
            positions_df = self.position_tracker.get_positions_dataframe()
            
            # 모든 형태의 리포트 생성
            report_files = self.report_generator.generate_all_reports(
                positions_df=positions_df,
                portfolio_summary=portfolio_summary,
                performance_data=performance_data
            )
            
            return report_files
            
        except Exception as e:
            print(f"❌ 리포트 생성 오류: {e}")
            return {}
            
    def run_strategy_signals(self, strategy_signals: List[Dict]) -> List[str]:
        """전략 시그널 실행
        
        Args:
            strategy_signals: 전략 시그널 리스트
            예: [{'action': 'buy', 'symbol': 'AAPL', 'strategy': 'momentum', 'notes': '...'}]
            
        Returns:
            처리된 시그널의 주문 ID 리스트
        """
        order_ids = []
        
        try:
            for signal in strategy_signals:
                action = signal.get('action', '').lower()
                symbol = signal.get('symbol', '')
                strategy = signal.get('strategy', 'unknown')
                notes = signal.get('notes', '')
                
                if not symbol:
                    continue
                    
                if action == 'buy':
                    # 매수 시그널
                    success = self.add_position(
                        symbol=symbol,
                        strategy=strategy,
                        position_type='long',
                        notes=notes
                    )
                    if success:
                        print(f"📈 {symbol} 매수 시그널 실행 완료")
                        
                elif action == 'sell' and symbol in self.position_tracker.positions:
                    # 매도 시그널 (기존 포지션 청산)
                    success = self.close_position(symbol, f"전략 시그널: {notes}")
                    if success:
                        print(f"📉 {symbol} 매도 시그널 실행 완료")
                        
                elif action == 'short':
                    # 공매도 시그널
                    success = self.add_position(
                        symbol=symbol,
                        strategy=strategy,
                        position_type='short',
                        notes=notes
                    )
                    if success:
                        print(f"📉 {symbol} 공매도 시그널 실행 완료")
                        
            return order_ids
            
        except Exception as e:
            print(f"❌ 전략 시그널 실행 오류: {e}")
            return []
            
    def get_strategy_performance(self, strategy: str) -> Dict:
        """특정 전략의 성과 조회
        
        Args:
            strategy: 전략명
            
        Returns:
            전략 성과 정보
        """
        try:
            positions_df = self.position_tracker.get_positions_dataframe()
            
            if positions_df.empty:
                return {}
                
            # 해당 전략의 포지션만 필터링
            strategy_positions = positions_df[positions_df['strategy'] == strategy]
            
            if strategy_positions.empty:
                return {'message': f'전략 "{strategy}"의 포지션이 없습니다.'}
                
            # 전략별 성과 계산
            strategy_performance = self.performance_analyzer.calculate_strategy_performance(
                strategy_positions
            )
            
            return strategy_performance.get(strategy, {})
            
        except Exception as e:
            print(f"❌ 전략 성과 조회 오류: {e}")
            return {}
            
    def cleanup_old_data(self, days_to_keep: int = 90):
        """오래된 데이터 정리
        
        Args:
            days_to_keep: 보관할 일수 (기본 90일)
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # 청산된 포지션 중 오래된 것들 정리
            cleaned_count = self.position_tracker.cleanup_old_positions(cutoff_date)
            
            print(f"🧹 {cleaned_count}개의 오래된 포지션 데이터를 정리했습니다.")
            
        except Exception as e:
            print(f"❌ 데이터 정리 오류: {e}")
            
    def export_data(self, export_dir: Optional[str] = None) -> Dict[str, str]:
        """포트폴리오 데이터 내보내기
        
        Args:
            export_dir: 내보낼 디렉토리 (None이면 기본 디렉토리 사용)
            
        Returns:
            내보낸 파일 경로들
        """
        try:
            if export_dir is None:
                export_dir = os.path.join(RESULTS_VER2_DIR, 'exports')
                
            ensure_directory_exists(export_dir)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            exported_files = {}
            
            # 포지션 데이터
            positions_df = self.position_tracker.get_positions_dataframe()
            if not positions_df.empty:
                positions_file = os.path.join(export_dir, f'positions_{timestamp}.csv')
                positions_df.to_csv(positions_file, index=False, encoding='utf-8-sig')
                exported_files['positions'] = positions_file
                
            # 성과 데이터
            if not self.performance_analyzer.daily_performance.empty:
                performance_file = os.path.join(export_dir, f'daily_performance_{timestamp}.csv')
                self.performance_analyzer.daily_performance.to_csv(
                    performance_file, index=False, encoding='utf-8-sig'
                )
                exported_files['performance'] = performance_file
                
            print(f"📤 {len(exported_files)}개 파일을 내보냈습니다: {export_dir}")
            return exported_files
            
        except Exception as e:
            print(f"❌ 데이터 내보내기 오류: {e}")
            return {}
            
    def print_status_summary(self):
        """포트폴리오 상태 요약 출력"""
        try:
            status = self.get_portfolio_status()
            
            portfolio_summary = status.get('portfolio_summary', {})
            performance_data = status.get('performance_data', {})
            risk_metrics = status.get('risk_metrics', {})
            
            print("\n" + "="*60)
            print("📊 포트폴리오 상태 요약")
            print("="*60)
            
            # 기본 정보
            total_value = portfolio_summary.get('total_value', 0)
            total_positions = portfolio_summary.get('total_positions', 0)
            
            print(f"💼 총 포트폴리오 가치: ${total_value:,.0f}")
            print(f"📈 활성 포지션 수: {total_positions}개")
            
            # 수익률 정보
            returns_metrics = performance_data.get('returns_metrics', {})
            total_return_pct = returns_metrics.get('total_return_pct', 0)
            win_rate = returns_metrics.get('win_rate', 0)
            
            print(f"📊 총 수익률: {total_return_pct:+.2f}%")
            print(f"🎯 승률: {win_rate:.1f}%")
            
            # 리스크 정보
            max_position_weight = risk_metrics.get('max_position_weight', 0)
            var_95 = risk_metrics.get('var_95', 0)
            
            print(f"⚖️ 최대 포지션 비중: {max_position_weight:.1f}%")
            print(f"📉 VaR (95%): ${var_95:,.0f}")
            
            print("="*60)
            
        except Exception as e:
            print(f"❌ 상태 요약 출력 오류: {e}")