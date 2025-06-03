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
from typing import Dict, List, Optional, Tuple, Any

# 프로젝트 루트 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from .position_tracker import PositionTracker
from .risk_manager import RiskManager
from config import RESULTS_VER2_DIR
from utils import ensure_dir

class PortfolioManager:
    """통합 포트폴리오 관리 클래스 - 6개 전략 통합 지원"""
    
    def __init__(self, portfolio_name: str = "main_portfolio", initial_capital: float = 100000):
        self.portfolio_name = portfolio_name
        self.initial_capital = initial_capital
        
        # 핵심 모듈 초기화
        self.position_tracker = PositionTracker(portfolio_name)
        self.risk_manager = RiskManager(portfolio_name)
        
        # 포트폴리오 디렉토리 설정
        self.portfolio_dir = os.path.join(RESULTS_VER2_DIR, 'portfolio_management')
        ensure_dir(self.portfolio_dir)
        
        # 포트폴리오 설정 파일
        self.config_file = os.path.join(self.portfolio_dir, f'{portfolio_name}_config.json')
        
        # 전략별 설정 정의
        self.strategy_configs = {
            'strategy1': {
                'name': '트렌드 하이 모멘텀 롱',
                'type': 'LONG',
                'max_positions': 10,
                'risk_per_position': 0.02,  # 2%
                'max_position_size': 0.10,  # 10%
                'trailing_stop_pct': 0.25,  # 25%
                'result_file': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy1_results.csv')
            },
            'strategy2': {
                'name': '평균회귀 단일 숏',
                'type': 'SHORT',
                'max_positions': 10,
                'risk_per_position': 0.02,  # 2%
                'max_position_size': 0.10,  # 10%
                'profit_target': 0.04,  # 4%
                'max_holding_days': 2,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy2_results.csv')
            },
            'strategy3': {
                'name': '전략3',
                'type': 'LONG',
                'max_positions': 10,
                'risk_per_position': 0.02,
                'max_position_size': 0.10,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy3_results.csv')
            },
            'strategy4': {
                'name': '전략4',
                'type': 'SHORT',
                'max_positions': 10,
                'risk_per_position': 0.02,
                'max_position_size': 0.10,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy4_results.csv')
            },
            'strategy5': {
                'name': '전략5',
                'type': 'LONG',
                'max_positions': 10,
                'risk_per_position': 0.02,
                'max_position_size': 0.10,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy5_results.csv')
            },
            'strategy6': {
                'name': '전략6',
                'type': 'SHORT',
                'max_positions': 10,
                'risk_per_position': 0.02,
                'max_position_size': 0.10,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy6_results.csv')
            }
        }
        
        self.load_portfolio_config()
    
    def load_portfolio_config(self):
        """포트폴리오 설정 로드"""
        default_config = {
            'initial_capital': self.initial_capital,
            'created_date': datetime.now().strftime('%Y-%m-%d'),
            'strategies': [],
            'rebalance_frequency': 'daily',
            'auto_trailing_stop': True,
            'max_positions_per_strategy': 10,
            'strategy_weights': {}
        }
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
            except Exception:
                self.config = default_config
        else:
            self.config = default_config
            self.save_portfolio_config()
    
    def save_portfolio_config(self):
        """포트폴리오 설정 저장"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ 포트폴리오 설정 저장 실패: {e}")
    
    def load_strategy_results(self, strategy_name: str) -> Optional[pd.DataFrame]:
        """전략 결과 파일 로드"""
        try:
            if strategy_name not in self.strategy_configs:
                print(f"❌ 알 수 없는 전략: {strategy_name}")
                return None
            
            result_file = self.strategy_configs[strategy_name]['result_file']
            
            if not os.path.exists(result_file):
                print(f"⚠️ 전략 결과 파일이 없습니다: {result_file}")
                return None
            
            df = pd.read_csv(result_file, encoding='utf-8-sig')
            
            if df.empty:
                print(f"⚠️ {strategy_name} 결과가 비어있습니다")
                return None
            
            return df
            
        except Exception as e:
            print(f"❌ {strategy_name} 결과 로드 실패: {e}")
            return None
    
    def process_strategy_signals(self, strategy_name: str, results_df: pd.DataFrame) -> int:
        """전략 신호 처리 및 포지션 추가"""
        added_count = 0
        current_portfolio_value = self.get_current_portfolio_value()
        
        print(f"📊 {self.strategy_configs[strategy_name]['name']} 신호 처리 중...")
        
        for _, row in results_df.iterrows():
            try:
                symbol = str(row['종목명']).strip()
                if not symbol or symbol == 'nan':
                    continue
                
                # 현재가 가져오기
                current_price = self.get_current_price(symbol)
                if current_price is None:
                    continue
                
                # 포지션 타입 결정
                position_type = 'LONG' if row.get('롱여부', True) else 'SHORT'
                
                # 진입가 설정
                if '매수가' in row and pd.notna(row['매수가']):
                    entry_price = float(row['매수가'])
                else:
                    entry_price = current_price
                
                # 비중 계산
                if '비중(%)' in row and pd.notna(row['비중(%)']):
                    weight_pct = float(row['비중(%)']) / 100.0
                else:
                    weight_pct = self.strategy_configs[strategy_name]['position_limit'] / 100.0
                    
                    # 최대 포지션 크기 제한
                    max_position_size = self.strategy_configs[strategy_name]['max_position_size']
                    if weight_pct > max_position_size:
                        weight_pct = max_position_size
                        
                        if weight_pct <= 0:
                            continue
                
                # ATR 값 계산 (리스크 관리용)
                atr_value = self.calculate_atr(symbol)
                
                # 포지션 추가 (기존 add_position 대신 add_position_with_strategy 사용)
                if self.position_tracker.add_position_with_strategy(
                    symbol=symbol, 
                    strategy_name=strategy_name, 
                    current_price=current_price, 
                    weight=weight_pct,
                    atr_value=atr_value
                ):
                    # 손절매 설정
                    if '손절매' in row and pd.notna(row['손절매']):
                        stop_price = float(row['손절매'])
                        self.risk_manager.set_trailing_stop(
                            symbol, position_type, strategy_name, entry_price, stop_price
                        )
                    
                    # 수익보호 설정 (strategy1의 경우)
                    if strategy_name == 'strategy1' and '수익보호' in row and pd.notna(row['수익보호']):
                        trailing_stop_price = float(row['수익보호'])
                        self.risk_manager.set_trailing_stop(
                            symbol, position_type, strategy_name, entry_price, trailing_stop_price, is_trailing=True
                        )
                    
                    added_count += 1
            
            except Exception as e:
                print(f"⚠️ {symbol} 신호 처리 중 오류: {e}")
                continue
        
        print(f"✅ {strategy_name}: {added_count}개 포지션 추가 완료")
        return added_count

    def calculate_atr(self, symbol: str, period: int = 14) -> float:
        """ATR(Average True Range) 계산"""
        try:
            import yfinance as yf
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=f"{period + 5}d")
            
            if len(hist) < period:
                return None
            
            # True Range 계산
            high_low = hist['High'] - hist['Low']
            high_close = abs(hist['High'] - hist['Close'].shift(1))
            low_close = abs(hist['Low'] - hist['Close'].shift(1))
            
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = true_range.rolling(window=period).mean().iloc[-1]
            
            return float(atr) if pd.notna(atr) else None
            
        except Exception as e:
            print(f"⚠️ {symbol} ATR 계산 실패: {e}")
            return None
    
    def process_all_strategies(self) -> bool:
        """모든 전략 신호를 일괄 처리"""
        try:
            print("\n🔄 모든 전략 신호 일괄 처리 시작...")
            
            success_count = 0
            for strategy_name in self.strategy_configs.keys():
                if self.process_strategy_signals(strategy_name):
                    success_count += 1
            
            print(f"\n✅ 전략 처리 완료: {success_count}/{len(self.strategy_configs)}개 성공")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ 전략 일괄 처리 실패: {e}")
            return False
    
    def update_portfolio(self) -> bool:
        """포트폴리오 전체 업데이트"""
        try:
            print("\n🔄 포트폴리오 업데이트 시작...")
            
            # 1. 포지션 현재가 업데이트
            self.position_tracker.update_positions()
            
            # 2. Trailing Stop 업데이트
            positions = self.position_tracker.positions
            stop_signals = self.risk_manager.update_trailing_stops(positions)
            
            # 3. 스탑 신호 처리
            for signal in stop_signals:
                self.position_tracker.close_position(
                    signal['symbol'], signal['position_type'], signal['strategy']
                )
                self.risk_manager.remove_stop_order(
                    signal['symbol'], signal['position_type'], signal['strategy']
                )
                print(f"🛑 {signal['reason']}: {signal['symbol']} {signal['position_type']}")
            
            # 4. 전략별 특수 규칙 적용
            self.apply_strategy_specific_rules()
            
            # 5. 리스크 체크
            risk_warnings = self.risk_manager.check_risk_limits(positions)
            for warning in risk_warnings:
                print(f"⚠️ {warning['message']}")
            
            print("✅ 포트폴리오 업데이트 완료")
            return True
            
        except Exception as e:
            print(f"❌ 포트폴리오 업데이트 실패: {e}")
            return False
    
    def apply_strategy_specific_rules(self):
        """전략별 특수 규칙 적용"""
        try:
            positions = self.position_tracker.positions
            
            for _, position in positions.iterrows():
                strategy_name = position['strategy']
                symbol = position['symbol']
                
                # Strategy2: 수익 4% 이상 또는 2일 후 청산
                if strategy_name == 'strategy2':
                    pnl_pct = (position['current_price'] - position['entry_price']) / position['entry_price']
                    entry_date = pd.to_datetime(position['entry_date'])
                    days_held = (datetime.now() - entry_date).days
                    
                    # 수익 4% 이상 또는 2일 경과시 청산
                    if pnl_pct >= 0.04 or days_held >= 2:
                        self.position_tracker.close_position(
                            symbol, position['position_type'], strategy_name
                        )
                        reason = "수익목표 달성" if pnl_pct >= 0.04 else "시간 기반 청산"
                        print(f"📈 {reason}: {symbol} 청산 (수익률: {pnl_pct:.2%})")
                
                # 다른 전략별 규칙도 여기에 추가 가능
                
        except Exception as e:
            print(f"⚠️ 전략별 규칙 적용 중 오류: {e}")
    
    def get_portfolio_value(self) -> float:
        """현재 포트폴리오 총 가치 계산"""
        try:
            positions = self.position_tracker.positions
            if positions.empty:
                return self.initial_capital
            
            return positions['market_value'].sum()
            
        except Exception:
            return self.initial_capital
    
    def get_strategy_summary(self) -> Dict:
        """전략별 포트폴리오 요약"""
        try:
            positions = self.position_tracker.positions
            
            if positions.empty:
                return {}
            
            strategy_summary = {}
            
            for strategy_name in positions['strategy'].unique():
                strategy_positions = positions[positions['strategy'] == strategy_name]
                strategy_config = self.strategy_configs.get(strategy_name, {})
                
                total_value = strategy_positions['market_value'].sum()
                total_pnl = strategy_positions['unrealized_pnl'].sum()
                position_count = len(strategy_positions)
                
                strategy_summary[strategy_name] = {
                    'name': strategy_config.get('name', strategy_name),
                    'type': strategy_config.get('type', 'UNKNOWN'),
                    'position_count': position_count,
                    'total_value': total_value,
                    'total_pnl': total_pnl,
                    'weight': total_value / self.get_portfolio_value() if self.get_portfolio_value() > 0 else 0,
                    'avg_pnl_pct': (total_pnl / (total_value - total_pnl)) * 100 if (total_value - total_pnl) > 0 else 0
                }
            
            return strategy_summary
            
        except Exception as e:
            print(f"⚠️ 전략별 요약 생성 실패: {e}")
            return {}
    
    def get_portfolio_summary(self) -> Dict:
        """포트폴리오 종합 요약"""
        try:
            # 포지션 요약
            position_summary = self.position_tracker.get_portfolio_summary()
            
            # 리스크 요약
            positions = self.position_tracker.positions
            risk_summary = self.risk_manager.get_risk_summary(positions)
            
            # 성과 지표
            performance = self.position_tracker.get_performance_metrics()
            
            # 전략별 요약
            strategy_summary = self.get_strategy_summary()
            
            # 통합 요약
            summary = {
                'portfolio_name': self.portfolio_name,
                'initial_capital': self.initial_capital,
                'current_value': self.get_portfolio_value(),
                'total_return': self.get_portfolio_value() - self.initial_capital,
                'total_return_pct': (self.get_portfolio_value() / self.initial_capital - 1) * 100,
                'positions': position_summary,
                'risk': risk_summary,
                'performance': performance,
                'strategies': strategy_summary,
                'active_strategies': self.config['strategies'],
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return summary
            
        except Exception as e:
            print(f"⚠️ 포트폴리오 요약 생성 실패: {e}")
            return {}
    
    def generate_report(self, save_to_file: bool = True) -> str:
        """포트폴리오 리포트 생성"""
        try:
            summary = self.get_portfolio_summary()
            strategy_summary = summary.get('strategies', {})
            
            report = f"""
# 포트폴리오 리포트: {self.portfolio_name}
생성일시: {summary.get('last_updated', 'N/A')}

## 📊 포트폴리오 개요
- 초기 자본: ${summary.get('initial_capital', 0):,.2f}
- 현재 가치: ${summary.get('current_value', 0):,.2f}
- 총 수익: ${summary.get('total_return', 0):,.2f} ({summary.get('total_return_pct', 0):.2f}%)

## 📈 포지션 현황
- 총 포지션: {summary.get('positions', {}).get('total_positions', 0)}개
- 롱 포지션: {summary.get('positions', {}).get('long_positions', 0)}개
- 숏 포지션: {summary.get('positions', {}).get('short_positions', 0)}개
- 미실현 손익: ${summary.get('positions', {}).get('total_unrealized_pnl', 0):,.2f}

## 🎯 전략별 현황
"""
            
            for strategy_name, strategy_data in strategy_summary.items():
                report += f"""
### {strategy_data['name']} ({strategy_name})
- 타입: {strategy_data['type']}
- 포지션 수: {strategy_data['position_count']}개
- 총 가치: ${strategy_data['total_value']:,.2f}
- 포트폴리오 비중: {strategy_data['weight']:.1%}
- 평균 수익률: {strategy_data['avg_pnl_pct']:.2f}%
- 미실현 손익: ${strategy_data['total_pnl']:,.2f}
"""
            
            report += f"""

## ⚠️ 리스크 관리
- 포트폴리오 VaR: {summary.get('risk', {}).get('var_percentage', 0):.2f}%
- 활성 스탑 오더: {summary.get('risk', {}).get('active_stop_orders', 0)}개
- 리스크 경고: {summary.get('risk', {}).get('risk_warnings', 0)}개

## 📊 성과 지표
- 총 거래: {summary.get('performance', {}).get('total_trades', 0)}회
- 승률: {summary.get('performance', {}).get('win_rate', 0):.1f}%
- 평균 보유일: {summary.get('performance', {}).get('avg_holding_days', 0):.1f}일
- 최고 수익: ${summary.get('performance', {}).get('best_trade', 0):,.2f}
- 최대 손실: ${summary.get('performance', {}).get('worst_trade', 0):,.2f}
"""
            
            if save_to_file:
                report_file = os.path.join(self.portfolio_dir, f'{self.portfolio_name}_report.md')
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write(report)
                print(f"📄 리포트 저장: {report_file}")
            
            return report
            
        except Exception as e:
            print(f"❌ 리포트 생성 실패: {e}")
            return ""
    
    # ... existing code ...

# 포트폴리오 관리 함수들
def create_portfolio_manager(portfolio_name: str = "main_portfolio", 
                           initial_capital: float = 100000) -> PortfolioManager:
    """포트폴리오 매니저 생성"""
    return PortfolioManager(portfolio_name, initial_capital)

def run_integrated_portfolio_management(portfolio_name: str = "main_portfolio"):
    """통합 포트폴리오 관리 실행"""
    try:
        print(f"\n🚀 {portfolio_name} 통합 포트폴리오 관리 시작...")
        
        pm = PortfolioManager(portfolio_name)
        
        # 1. 모든 전략 신호 처리
        pm.process_all_strategies()
        
        # 2. 포트폴리오 업데이트
        pm.update_portfolio()
        
        # 3. 요약 출력
        summary = pm.get_portfolio_summary()
        
        print(f"\n📊 {portfolio_name} 포트폴리오 현황:")
        print(f"현재 가치: ${summary.get('current_value', 0):,.2f}")
        print(f"총 수익률: {summary.get('total_return_pct', 0):.2f}%")
        print(f"활성 포지션: {summary.get('positions', {}).get('total_positions', 0)}개")
        print(f"활성 전략: {len(summary.get('strategies', {}))}개")
        
        # 4. 리포트 생성
        pm.generate_report()
        
        return True
        
    except Exception as e:
        print(f"❌ 통합 포트폴리오 관리 실패: {e}")
        return False

if __name__ == "__main__":
    # 테스트 실행
    run_integrated_portfolio_management("test_portfolio")