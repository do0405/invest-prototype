#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
포트폴리오 통합 관리 시스템

long_short_portfolio의 전략들과 portfolio_management를 연결하여
포지션 추적, 손절매, 수익보호, 차익실현 등을 자동으로 처리합니다.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import traceback

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 설정 파일 임포트
from config import DATA_US_DIR, RESULTS_VER2_DIR
from utils import ensure_dir

# 포트폴리오 관리 모듈 임포트
from portfolio_management.portfolio_manager import PortfolioManager
from portfolio_management.core.order_manager import OrderSide, OrderType

# 전략 모듈 임포트
from long_short_portfolio import strategy1
from long_short_portfolio import strategy2
from long_short_portfolio import strategy3
from long_short_portfolio import strategy4
from long_short_portfolio import strategy5
from long_short_portfolio import strategy6


class StrategyPortfolioIntegrator:
    """전략과 포트폴리오 관리를 통합하는 클래스"""
    
    def __init__(self, initial_capital: float = 100000):
        """
        Args:
            initial_capital: 초기 자본금
        """
        self.initial_capital = initial_capital
        self.portfolio_manager = PortfolioManager(initial_capital=initial_capital)
        
        # 전략별 설정
        self.strategies = {
                        'strategy1': {
                'module': strategy1,
                'is_long': True,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy1_results.csv')
            },
            'strategy2': {
                'module': strategy2,
                'is_long': False,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy2_results.csv')
            },
            'strategy3': {
                'module': strategy3,
                'is_long': True,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy3_results.csv')
            },
            'strategy4': {
                'module': strategy4,
                'is_long': True, # Strategy 4 results are now in results/strategy4_results.csv (was buy)
                'result_file': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy4_results.csv')
            },
            'strategy5': {
                'module': strategy5,
                'is_long': True,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'buy', 'strategy5_results.csv')
            },
            'strategy6': {
                'module': strategy6,
                'is_long': False,
                'result_file': os.path.join(RESULTS_VER2_DIR, 'sell', 'strategy6_results.csv')
            }
        }
        
        # 포지션 추적 데이터
        self.active_positions = {}
        self.position_history = []
        
        print(f"🔗 전략 포트폴리오 통합 시스템 초기화 완료")
        print(f"   - 관리 전략 수: {len(self.strategies)}개")
        print(f"   - 초기 자본금: ${initial_capital:,.0f}")
    
    def run_all_strategies(self):
        """모든 전략을 실행하여 스크리닝 결과 생성"""
        print("\n🚀 모든 전략 실행 시작...")
        
        for strategy_name, strategy_config in self.strategies.items():
            try:
                print(f"\n📊 {strategy_name} 실행 중...")
                strategy_module = strategy_config['module']
                
                # 전략 실행
                strategy_module.run_strategy(total_capital=self.initial_capital)
                
                print(f"✅ {strategy_name} 실행 완료")
                
            except Exception as e:
                print(f"❌ {strategy_name} 실행 오류: {e}")
                print(traceback.format_exc())
    
    def load_strategy_results(self) -> Dict[str, pd.DataFrame]:
        """모든 전략의 결과를 로드"""
        strategy_results = {}
        
        for strategy_name, strategy_config in self.strategies.items():
            result_file = strategy_config['result_file']
            
            if os.path.exists(result_file):
                try:
                    df = pd.read_csv(result_file, encoding='utf-8-sig')
                    if not df.empty:
                        df['strategy'] = strategy_name
                        df['is_long'] = strategy_config['is_long']
                        strategy_results[strategy_name] = df
                        print(f"📈 {strategy_name}: {len(df)}개 종목 로드")
                    else:
                        print(f"⚠️ {strategy_name}: 빈 결과 파일")
                except Exception as e:
                    print(f"❌ {strategy_name} 결과 로드 오류: {e}")
            else:
                print(f"⚠️ {strategy_name}: 결과 파일 없음 ({result_file})")
        
        return strategy_results
    
    def create_market_orders(self, strategy_results: Dict[str, pd.DataFrame]):
        """전략 결과를 바탕으로 시장 주문 생성"""
        print("\n📋 시장 주문 생성 중...")
        
        total_orders = 0
        
        for strategy_name, df in strategy_results.items():
            strategy_config = self.strategies[strategy_name]
            is_long = strategy_config['is_long']
            
            for _, row in df.iterrows():
                try:
                    symbol = row['종목명']
                    # '매수가' 컬럼 값 확인
                    entry_price_str = str(row['매수가']).strip()
                    weight_pct = float(row['비중']) # '비중(%)' -> '비중'

                    order_type_to_use = OrderType.LIMIT
                    price_to_use = None
                    current_market_price_for_qty_calc = None # 시장가 주문 시 수량 계산을 위한 가격

                    if entry_price_str.lower() == '시장가':
                        order_type_to_use = OrderType.MARKET
                        # 시장가 주문 시, 수량 계산을 위해 현재가를 가져오는 로직 필요
                        # 여기서는 임시로 해당 종목의 최근 종가를 사용한다고 가정합니다.
                        # 실제 구현에서는 API 등을 통해 실시간 시세를 가져와야 합니다.
                        # 이 예제에서는 utils.get_latest_close와 같은 함수가 있다고 가정합니다.
                        # from utils import get_latest_close 
                        # current_market_price_for_qty_calc = get_latest_close(symbol, DATA_US_DIR)
                        # if current_market_price_for_qty_calc is None:
                        #     print(f"⚠️ {symbol}의 현재가를 가져올 수 없어 주문을 건너<0xEB><0><0x8A><0xAC>니다.")
                        #     continue
                        # 우선은 플레이스홀더로 이전 로직의 entry_price를 사용하나, 실제로는 None이어야 함.
                        # place_order가 MARKET 타입일 때 price=None을 어떻게 처리하는지 확인 필요.
                        # 임시로, 수량 계산을 위해선 가격이 필요하므로, 로드된 데이터의 '매수가'가 숫자인 경우를 대비해 try-except 처리
                        # 또는, 전략 파일에서 시장가일 경우 예상 체결가를 별도 컬럼으로 제공하거나, 여기서 조회해야 함.
                        # 지금은 가장 최근 데이터의 open 가격을 사용하도록 수정 (strategy 파일들 참고)
                        # 실제로는 portfolio_manager.place_order가 price=None을 받고 내부적으로 처리해야 이상적
                        try:
                            # strategy 파일에서 '시장가'로 설정 시, 실제 entry_price는 다음날 시가 등으로 처리됨
                            # 여기서는 주문 객체 생성 시점에는 가격을 명시하지 않음
                            # 수량 계산을 위한 가격은 별도로 처리해야 함.
                            # 예를 들어, 전일 종가 또는 예상 시가.
                            # 지금은 해당 로직이 없으므로, 만약 '시장가'가 아닌 숫자형태의 문자열이면 float으로 변환 시도
                            # 이 부분은 실제 데이터와 utils 함수에 따라 견고하게 수정 필요
                            df_stock = pd.read_csv(os.path.join(DATA_US_DIR, f"{symbol}.csv"))
                            if not df_stock.empty:
                                current_market_price_for_qty_calc = df_stock['close'].iloc[-1] # 예시: 전일 종가
                            else:
                                print(f"⚠️ {symbol} 데이터 파일을 찾을 수 없어 현재가 조회 실패, 주문 건너<0xEB><0><0x8A><0xAC>니다.")
                                continue
                        except ValueError:
                             print(f"⚠️ {symbol}의 '매수가'({entry_price_str})가 '시장가'이지만 수량 계산을 위한 가격 정보를 찾을 수 없습니다. 주문을 건너<0xEB><0><0x8A><0xAC>니다.")
                             continue # 시장가인데 숫자로 변환 안되면 문제
                        price_to_use = None # 시장가 주문 시 가격은 None

                    else:
                        try:
                            price_to_use = float(entry_price_str)
                            current_market_price_for_qty_calc = price_to_use
                            order_type_to_use = OrderType.LIMIT
                        except ValueError:
                            print(f"❌ {symbol}의 '매수가'({entry_price_str})를 숫자로 변환할 수 없습니다. 주문을 건너<0xEB><0><0x8A><0xAC>니다.")
                            continue
                    
                    if current_market_price_for_qty_calc is None or current_market_price_for_qty_calc <= 0:
                        print(f"❌ {symbol}의 수량 계산을 위한 유효한 가격({current_market_price_for_qty_calc})을 얻지 못했습니다. 주문을 건너<0xEB><0><0x8A><0xAC>니다.")
                        continue

                    # 포지션 크기 계산
                    position_value = self.initial_capital * (weight_pct / 100)
                    quantity = int(position_value / current_market_price_for_qty_calc)
                    
                    if quantity > 0:
                        order_side = OrderSide.BUY if is_long else OrderSide.SELL
                        
                        order_id = self.portfolio_manager.place_order(
                            symbol=symbol,
                            side=order_side,
                            order_type=order_type_to_use,
                            quantity=quantity,
                            price=price_to_use # 시장가 주문 시 None, 지정가 주문 시 해당 가격
                        )
                        
                        if order_id:
                            # 포지션 정보 저장 시 entry_price는 실제 체결가로 업데이트 되어야 함.
                            # 초기에는 지정가 또는 예상 시장가로 설정.
                            actual_entry_price_for_position = price_to_use if order_type_to_use == OrderType.LIMIT else current_market_price_for_qty_calc
                            if order_type_to_use == OrderType.MARKET:
                                # 시장가 주문의 경우, 실제 체결가는 주문 실행 후 알 수 있음.
                                # 여기서는 일단 계산에 사용된 가격을 임시로 사용하고, 추후 업데이트 필요.
                                print(f"ℹ️ {symbol} 시장가 주문 생성. 실제 체결가는 주문 처리 후 업데이트 필요.")

                            position_info = {
                                'strategy': strategy_name,
                                'symbol': symbol,
                                'is_long': is_long,
                                'entry_price': actual_entry_price_for_position, # 주문 시점의 가격 (시장가는 예상가)
                                'quantity': quantity,
                                'stop_loss': self._parse_stop_loss(row.get('손절매', '')),
                                'profit_target': self._parse_profit_target(row.get('차익실현', '')),
                                'trailing_stop': row.get('수익보호', '') != '없음',
                                'entry_date': datetime.now(),
                                'order_id': order_id
                            }
                            
                            self.active_positions[f"{strategy_name}_{symbol}"] = position_info
                            total_orders += 1
                            
                            log_price = f"@ ${price_to_use:.2f}" if price_to_use is not None else "(시장가)"
                            print(f"📝 주문 생성: {symbol} ({strategy_name}) - {order_side.value} {quantity}주 {log_price}")
                
                except Exception as e:
                    print(f"❌ 주문 생성 오류 ({strategy_name}, {row.get('종목명', 'Unknown')}): {e}")
                    traceback.print_exc() # 상세 오류 출력
        
        print(f"\n✅ 총 {total_orders}개 주문 생성 완료")
    
    def _parse_stop_loss(self, stop_loss_str: str) -> Optional[float]:
        """손절매 문자열에서 가격 추출"""
        try:
            if pd.isna(stop_loss_str) or stop_loss_str == '없음':
                return None
            
            # 숫자 부분만 추출
            import re
            numbers = re.findall(r'\d+\.?\d*', str(stop_loss_str))
            if numbers:
                return float(numbers[0])
        except (ValueError, TypeError):
            print(f"⚠️ 손절매 가격 파싱 실패: {stop_loss_str}")
        return None
    
    def _parse_profit_target(self, profit_target_str: str) -> Optional[float]:
        """차익실현 문자열에서 가격 추출"""
        try:
            if pd.isna(profit_target_str) or profit_target_str == '없음':
                return None
            
            # 숫자 부분만 추출
            import re
            numbers = re.findall(r'\d+\.?\d*', str(profit_target_str))
            if numbers:
                return float(numbers[0])
        except (ValueError, TypeError):
            print(f"⚠️ 차익실현 가격 파싱 실패: {profit_target_str}")
        return None
    
    def _parse_trailing_stop(self, trailing_stop_str: str) -> bool:
        """수익보호 문자열을 boolean으로 변환"""
        try:
            if pd.isna(trailing_stop_str) or trailing_stop_str == '없음':
                return False
            return True
        except (ValueError, TypeError):
            print(f"⚠️ 수익보호 설정 파싱 실패: {trailing_stop_str}")
            return False
    
    def _format_weight_pct(self, weight: float) -> str:
        """float 비중을 % 기호가 포함된 문자열로 변환"""
        return f"{weight:.2f}%"
    
    def _parse_weight_pct(self, weight_str: str) -> float:
        """비중 문자열을 float로 변환 (출력 시 % 기호 유지)"""
        try:
            if pd.isna(weight_str):
                return 0.0
            # % 기호가 있는 경우 제거하고 float로 변환
            weight_str = str(weight_str).replace('%', '')
            return float(weight_str)
        except (ValueError, TypeError):
            print(f"⚠️ 비중 파싱 실패: {weight_str}")
            return 0.0
    
    def update_positions(self):
        """포지션 업데이트 및 손절매/차익실현 체크"""
        print("\n🔄 포지션 업데이트 중...")
        
        positions_to_close = []
        
        for position_key, position_info in self.active_positions.items():
            try:
                symbol = position_info['symbol']
                strategy = position_info['strategy']
                
                # 현재 가격 조회
                current_price = self._get_current_price(symbol)
                if current_price is None:
                    continue
                
                # 수익률 계산
                entry_price = position_info['entry_price']
                is_long = position_info['is_long']
                
                if is_long:
                    pnl_pct = ((current_price - entry_price) / entry_price) * 100
                else:
                    pnl_pct = ((entry_price - current_price) / entry_price) * 100
                
                # 손절매 체크
                if position_info['stop_loss'] is not None:
                    if is_long and current_price <= position_info['stop_loss']:
                        print(f"🛑 손절매 발동: {symbol} ({strategy}) - 현재가: ${current_price:.2f}, 손절가: ${position_info['stop_loss']:.2f}")
                        positions_to_close.append((position_key, 'stop_loss'))
                        continue
                    elif not is_long and current_price >= position_info['stop_loss']:
                        print(f"🛑 손절매 발동: {symbol} ({strategy}) - 현재가: ${current_price:.2f}, 손절가: ${position_info['stop_loss']:.2f}")
                        positions_to_close.append((position_key, 'stop_loss'))
                        continue
                
                # 차익실현 체크
                if position_info['profit_target'] is not None:
                    if is_long and current_price >= position_info['profit_target']:
                        print(f"💰 차익실현: {symbol} ({strategy}) - 현재가: ${current_price:.2f}, 목표가: ${position_info['profit_target']:.2f}")
                        positions_to_close.append((position_key, 'profit_target'))
                        continue
                    elif not is_long and current_price <= position_info['profit_target']:
                        print(f"💰 차익실현: {symbol} ({strategy}) - 현재가: ${current_price:.2f}, 목표가: ${position_info['profit_target']:.2f}")
                        positions_to_close.append((position_key, 'profit_target'))
                        continue
                
                # 트레일링 스톱 업데이트 (수익보호가 있는 경우)
                if position_info['trailing_stop']:
                    self._update_trailing_stop(position_key, current_price)
                
                # 포지션 정보 업데이트
                position_info['current_price'] = current_price
                position_info['pnl_pct'] = pnl_pct
                position_info['last_update'] = datetime.now()
                
                print(f"📊 {symbol} ({strategy}): ${current_price:.2f} ({pnl_pct:+.2f}%)")
                
            except Exception as e:
                print(f"❌ 포지션 업데이트 오류 ({position_key}): {e}")
        
        # 청산할 포지션 처리
        for position_key, reason in positions_to_close:
            self._close_position(position_key, reason)
    
    def _get_current_price(self, symbol: str) -> Optional[float]:
        """현재 가격 조회"""
        try:
            # strategy2.py의 get_latest_price_data_high 함수 사용
            current_price, _ = strategy2.get_latest_price_data_high(symbol)
            if current_price is None or current_price <= 0:
                print(f"⚠️ {symbol} 유효하지 않은 가격: {current_price}")
                return None
            return current_price
        except Exception as e:
            print(f"⚠️ {symbol} 가격 조회 실패: {e}")
            return None
    
    def _update_trailing_stop(self, position_key: str, current_price: float) -> None:
        """트레일링 스톱 업데이트"""
        try:
            position_info = self.active_positions[position_key]
            symbol = position_info['symbol']
            is_long = position_info['is_long']
            strategy_name = position_info['strategy']

            new_trailing_stop_price = None
            # Strategy1의 경우 ATR 기반 5배 트레일링 스탑 적용
            if strategy_name == 'strategy1':
                atr_multiplier = 5.0 # strategy1의 ATR 배수
                new_trailing_stop_price = self._calculate_atr_trailing_stop(symbol, current_price, is_long, atr_multiplier)
            else:
                # 다른 전략의 경우 기존 로직 또는 '수익보호' 컬럼 값 기반 (예: 고정 비율)
                # 여기서는 예시로 현재 가격 기준으로 업데이트 (필요시 전략별 로직 추가)
                if is_long:
                    # 기존 trailing_stop_price가 없거나, 새로운 가격이 더 높을 때만 업데이트
                    if 'trailing_stop_price' not in position_info or new_trailing_stop_price is None or \
                    (position_info.get('trailing_stop_price') is not None and current_price * 0.95 > position_info['trailing_stop_price']): # 5% trailing
                        new_trailing_stop_price = current_price * 0.95 
                else:
                    if 'trailing_stop_price' not in position_info or new_trailing_stop_price is None or \
                    (position_info.get('trailing_stop_price') is not None and current_price * 1.05 < position_info['trailing_stop_price']): # 5% trailing
                        new_trailing_stop_price = current_price * 1.05

            if new_trailing_stop_price is not None:
                # 기존 트레일링 스탑 가격보다 개선된 경우에만 업데이트
                current_trailing_stop = position_info.get('trailing_stop_price')
                if current_trailing_stop is None or \
                   (is_long and new_trailing_stop_price > current_trailing_stop) or \
                   (not is_long and new_trailing_stop_price < current_trailing_stop):
                    position_info['trailing_stop_price'] = new_trailing_stop_price
                    print(f"📊 {symbol} ({strategy_name}) Trailing Stop 업데이트: ${new_trailing_stop_price:.2f}")
                
        except Exception as e:
            print(f"❌ {symbol} Trailing Stop 업데이트 오류: {e}")
            print(traceback.format_exc())
    
    def _calculate_atr_trailing_stop(self, symbol: str, current_price: float,
                                   is_long: bool, atr_multiplier: float = 2.0) -> Optional[float]:
        """ATR 기반 Trailing Stop 계산"""
        try:
            file_path = os.path.join(DATA_US_DIR, f'{symbol}.csv')
            
            if not os.path.exists(file_path):
                return None
            
            df = pd.read_csv(file_path)
            df.columns = [col.lower() for col in df.columns]
            
            if len(df) < 20:  # ATR 계산을 위한 최소 데이터
                return None
            
            # 최근 20일 데이터
            recent_data = df.tail(20).copy()
            
            # ATR 계산
            tr = pd.DataFrame()
            tr['h-l'] = recent_data['high'] - recent_data['low']
            tr['h-pc'] = abs(recent_data['high'] - recent_data['close'].shift(1))
            tr['l-pc'] = abs(recent_data['low'] - recent_data['close'].shift(1))
            tr['tr'] = tr[['h-l', 'h-pc', 'l-pc']].max(axis=1)
            atr = tr['tr'].rolling(14).mean()
            
            if atr.empty:
                return None
            
            current_atr = atr.iloc[-1]
            
            if is_long:
                return current_price - (current_atr * atr_multiplier)
            else:  # short
                return current_price + (current_atr * atr_multiplier)
            
        except Exception as e:
            print(f"❌ {symbol} ATR Trailing Stop 계산 오류: {e}")
            print(traceback.format_exc())
            return None
    
    def _close_position(self, position_key: str, reason: str) -> None:
        """포지션 청산"""
        try:
            position_info = self.active_positions[position_key]
            symbol = position_info['symbol']
            quantity = position_info['quantity']
            is_long = position_info['is_long']
            
            # 청산 주문 생성
            order_side = OrderSide.SELL if is_long else OrderSide.BUY
            
            order_id = self.portfolio_manager.order_manager.create_market_order(
                symbol=symbol,
                side=order_side,
                quantity=quantity,
                strategy=position_info['strategy'],
                notes=f"포지션 청산 - 사유: {reason}"
            )
            
            if order_id:
                # 포지션 히스토리에 추가
                position_info['close_reason'] = reason
                position_info['close_date'] = datetime.now()
                position_info['close_order_id'] = order_id
                
                self.position_history.append(position_info.copy())
                
                # 활성 포지션에서 제거
                del self.active_positions[position_key]
                
                print(f"🔚 포지션 청산: {symbol} - 사유: {reason}")
            
        except Exception as e:
            print(f"❌ 포지션 청산 오류 ({position_key}): {e}")
            print(traceback.format_exc())
    
    def generate_daily_report(self, portfolio_summary: Dict = None):
        """일일 리포트 생성
        
        Args:
            portfolio_summary: 포트폴리오 요약 정보 (선택적)
        """
        try:
            print("\n📊 일일 리포트 생성 중...")
            
            # 포트폴리오 요약 정보가 없으면 생성
            if portfolio_summary is None:
                portfolio_summary = self._get_active_positions_summary_for_report()
            
            # 리포트 파일 경로 설정
            report_dir = os.path.join(RESULTS_VER2_DIR, 'reports')
            os.makedirs(report_dir, exist_ok=True)  # 디렉토리가 없으면 생성
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = os.path.join(report_dir, f'daily_report_{timestamp}.csv')
            
            # 리포트 데이터 생성
            report_data = []
            
            # 전략별 요약
            for strategy_name, positions in portfolio_summary.items():
                strategy_total_value = sum(p['current_price'] * p.get('quantity', 0) for p in positions)
                strategy_total_pnl = sum(p['pnl_pct'] for p in positions)
                
                report_data.append({
                    '전략': strategy_name,
                    '포지션수': len(positions),
                    '총가치': strategy_total_value,
                    '평균수익률': strategy_total_pnl / len(positions) if positions else 0,
                    '업데이트시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                
                # 개별 포지션 정보
                for position in positions:
                    report_data.append({
                        '전략': strategy_name,
                        '종목': position['symbol'],
                        '현재가': position['current_price'],
                        '수익률': position['pnl_pct'],
                        '비중': position['weight_pct'],
                        '업데이트시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
            
            # 리포트 저장
            if report_data:
                report_df = pd.DataFrame(report_data)
                report_df.to_csv(report_file, index=False, encoding='utf-8-sig')
# JSON 파일 생성 추가
                json_file = report_file.replace('.csv', '.json')
                report_df.to_json(json_file, orient='records', indent=2, force_ascii=False)
                print(f"✅ 일일 리포트 생성 완료: {report_file}")
            else:
                print("ℹ️ 리포트할 데이터가 없습니다.")
            
        except Exception as e:
            print(f"❌ 일일 리포트 생성 오류: {e}")
            print(traceback.format_exc())
    
    def _get_active_positions_summary(self) -> Dict[str, List[Dict]]:
        """활성 포지션 요약 정보 생성"""
        summary = {}
        
        for position_key, position_info in self.active_positions.items():
            strategy = position_info['strategy']
            
            if strategy not in summary:
                summary[strategy] = []
            
            weight_pct = (position_info['quantity'] * position_info['entry_price'] / self.initial_capital) * 100
            summary[strategy].append({
                'symbol': position_info['symbol'],
                'current_price': position_info.get('current_price', 0),
                'pnl_pct': position_info.get('pnl_pct', 0),
                'weight_pct': self._format_weight_pct(weight_pct)
            })
        
        return summary
    
    def manage_strategy_portfolio(self, strategy_name: str, result_file: str, is_initial_run: bool = False):
        """전략 포트폴리오 관리 (초기화 또는 업데이트)

        Args:
            strategy_name (str): 관리할 전략의 이름
            result_file (str): 전략 결과 파일 경로
            is_initial_run (bool): 초기 실행 여부. True이면 포트폴리오를 새로 구성하고,
                                   False이면 기존 포트폴리오를 업데이트합니다.
        """
        try:
            print(f"\n🔄 {strategy_name} 포트폴리오 관리 시작 (초기 실행: {is_initial_run})")
            strategy_config = self.strategies[strategy_name]
            is_long_strategy = strategy_config['is_long']

            # 결과 파일 로드 또는 생성
            if os.path.exists(result_file) and os.path.getsize(result_file) > 0:
                results_df = pd.read_csv(result_file, encoding='utf-8-sig')
                if results_df.empty and not is_initial_run:
                    print(f"⚠️ {strategy_name}: 결과 파일이 비어있어 업데이트할 내용이 없습니다.")
                    return
            elif is_initial_run:
                # 초기 실행 시 빈 DataFrame 생성
                results_df = pd.DataFrame(columns=[
                    '종목명', '매수일', '시장 진입가', '비중(%)', '수익률(%)',
                    '차익실현', '손절매', '수익보호', '롱여부'
                ])
                self._ensure_directory(result_file)
                results_df.to_csv(result_file, index=False, encoding='utf-8-sig')
# JSON 파일 생성 추가
                json_file = result_file.replace('.csv', '.json')
                results_df.to_json(json_file, orient='records', indent=2, force_ascii=False)
                print(f"📝 {strategy_name}: 초기 결과 파일 생성됨")
            else:
                print(f"⚠️ {strategy_name}: 결과 파일 없음 또는 비어있음 ({result_file}). 업데이트를 건너뜁니다.")
                return

            updated_positions = []
            current_symbols_in_file = set(results_df['종목명']) if '종목명' in results_df.columns else set()

            # 전략 결과 로드
            current_strategy_screening_results = self.load_strategy_results().get(strategy_name, pd.DataFrame())

            for _, screening_row in current_strategy_screening_results.iterrows():
                symbol = screening_row['종목명']
                
                # 시장 진입가 처리
                #entry_price_str = screening_row['매수가'] # '시장 진입가' -> '매수가'
                if '매수가' in screening_row:
                    entry_price_str = screening_row['매수가']
                elif '시장 진입가' in screening_row: # '시장 진입가' 컬럼 확인
                    entry_price_str = screening_row['시장 진입가']
                    print(f"INFO: {strategy_name} - {symbol} 종목의 '매수가'가 없어 '시장 진입가' 컬럼을 사용합니다.")
                else:
                    print(f"⚠️ {strategy_name} - {symbol}: '매수가' 또는 '시장 진입가' 컬럼을 찾을 수 없습니다. 해당 종목을 건너<0xEB><0><0x81>니다.")
                    continue
                if str(entry_price_str).lower() == '시장가': # Ensure robust comparison
                    current_price = self._get_current_price(symbol)
                    if current_price is None:
                        print(f"⚠️ {symbol}: 현재가 조회 실패")
                        continue
                    entry_price = current_price
                else:
                    try:
                        entry_price = float(entry_price_str)
                    except ValueError:
                        print(f"⚠️ {symbol}: 유효하지 않은 진입가 ({entry_price_str})")
                        continue

                weight_pct = self._parse_weight_pct(screening_row['비중']) # '비중(%)' -> '비중'
                stop_loss_price = self._parse_stop_loss(screening_row.get('손절매', ''))
                profit_target_price = self._parse_profit_target(screening_row.get('차익실현', ''))
                use_trailing_stop = True if strategy_name == 'strategy1' else self._parse_trailing_stop(screening_row.get('수익보호', '없음'))

                position_key = f"{strategy_name}_{symbol}"
                current_price = self._get_current_price(symbol)
                if current_price is None: continue

                if position_key in self.active_positions:
                    # 기존 포지션 업데이트 로직
                    pos_info = self.active_positions[position_key]
                    pos_info['current_price'] = current_price
                    pos_info['highest_price'] = max(pos_info.get('highest_price', current_price), current_price)
                    pos_info['lowest_price'] = min(pos_info.get('lowest_price', current_price), current_price)
                    
                    if is_long_strategy:
                        pnl_pct = ((current_price - pos_info['entry_price']) / pos_info['entry_price']) * 100
                        if use_trailing_stop:
                            if strategy_name == 'strategy1':
                                # Strategy1: 25% trailing stop (수익 보호)
                                new_ts_price = current_price * 0.75
                                if pos_info.get('trailing_stop_price') is None or new_ts_price > pos_info['trailing_stop_price']:
                                    pos_info['trailing_stop_price'] = new_ts_price
                            elif pos_info.get('trailing_stop_price'): # 기타 전략은 기존 로직 유지 또는 수정
                                # 기존: 최고가의 90% -> 개선: 현재가의 90% 또는 최고가의 90% 중 더 유리한 값
                                new_trailing_stop_current = current_price * 0.90
                                new_trailing_stop_highest = pos_info['highest_price'] * 0.90
                                pos_info['trailing_stop_price'] = max(pos_info.get('trailing_stop_price', 0), new_trailing_stop_current, new_trailing_stop_highest)
                    else:
                        pnl_pct = ((pos_info['entry_price'] - current_price) / pos_info['entry_price']) * 100
                        if use_trailing_stop:
                            if strategy_name == 'strategy1':
                                # Strategy1: 25% trailing stop (수익 보호)
                                new_ts_price = current_price * 1.25
                                if pos_info.get('trailing_stop_price') is None or new_ts_price < pos_info['trailing_stop_price']:
                                    pos_info['trailing_stop_price'] = new_ts_price
                            elif pos_info.get('trailing_stop_price'): # 기타 전략은 기존 로직 유지 또는 수정
                                # 기존: 최저가의 110% -> 개선: 현재가의 110% 또는 최저가의 110% 중 더 유리한 값
                                new_trailing_stop_current = current_price * 1.10
                                new_trailing_stop_lowest = pos_info['lowest_price'] * 1.10
                                pos_info['trailing_stop_price'] = min(pos_info.get('trailing_stop_price', float('inf')), new_trailing_stop_current, new_trailing_stop_lowest)
                    
                    pos_info['pnl_pct'] = pnl_pct
                    self.active_positions[position_key] = pos_info
                    
                    updated_positions.append({
                        '종목명': symbol,
                        '매수일': pos_info['entry_date'].strftime('%Y-%m-%d'),
                        '시장 진입가': pos_info['entry_price'],
                        '비중(%)': self._format_weight_pct((pos_info['quantity'] * pos_info['entry_price'] / self.initial_capital) * 100),
                        '수익률(%)': f"{pnl_pct:.2f}",
                        '차익실현': screening_row.get('차익실현', '없음'),
                        '손절매': screening_row.get('손절매', '없음'),
                        '수익보호': screening_row.get('수익보호', '없음'),
                        '롱여부': str(is_long_strategy),
                        
                    })
                    current_symbols_in_file.discard(symbol)

                elif is_initial_run or symbol not in [p['symbol'] for p in self.active_positions.values() if p['strategy'] == strategy_name]:
                    # 신규 포지션 추가
                    position_value = self.initial_capital * (weight_pct / 100)
                    quantity = int(position_value / entry_price) if entry_price > 0 else 0
                    if quantity == 0: continue

                    # OrderManager를 통해 주문 생성
                    order_side = OrderSide.BUY if is_long_strategy else OrderSide.SELL
                    order_id = self.portfolio_manager.order_manager.create_market_order(
                        symbol=symbol,
                        side=order_side,
                        quantity=quantity,
                        strategy=strategy_name,
                        notes=f"전략 {strategy_name} 신규 포지션 진입"
                    )
                    
                    if not order_id: continue

                    new_pos_info = {
                        'strategy': strategy_name,
                        'symbol': symbol,
                        'is_long': is_long_strategy,
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'stop_loss': stop_loss_price,
                        'profit_target': profit_target_price,
                        'trailing_stop': use_trailing_stop,
                        'entry_date': datetime.now(),
                        'order_id': order_id,
                        'current_price': current_price,
                        'highest_price': current_price,
                        'lowest_price': current_price,
                        'pnl_pct': 0.0
                    }
                    
                    # 모든 신규 포지션에 손절매 및 수익보호(트레일링 스탑) 설정
                    # 손절매는 screening_row에서 가져오거나 기본값 설정 (예: 진입가의 5%)
                    if stop_loss_price is None:
                        new_pos_info['stop_loss'] = entry_price * (0.95 if is_long_strategy else 1.05) 
                        print(f"ℹ️ {symbol} ({strategy_name}): 손절매 정보 없음. 기본값 ({new_pos_info['stop_loss']:.2f}) 설정")
                    else:
                        new_pos_info['stop_loss'] = stop_loss_price

                    if use_trailing_stop: # strategy1은 항상 True
                        if strategy_name == 'strategy1':
                            # Strategy1: 진입가의 25% 트레일링 스탑 (수익 보호)
                            new_pos_info['trailing_stop_price'] = entry_price * (0.75 if is_long_strategy else 1.25)
                        else:
                            # 기타 전략: 진입가의 5% 트레일링 스탑 (기본값)
                            new_pos_info['trailing_stop_price'] = entry_price * (0.95 if is_long_strategy else 1.05)
                    else: # use_trailing_stop이 False인 경우 (strategy1 제외)
                        # 수익보호 사용 안함 명시적 표시 또는 None으로 설정
                        new_pos_info['trailing_stop_price'] = None
                    
                    self.active_positions[position_key] = new_pos_info
                    updated_positions.append({
                        '종목명': symbol,
                        '매수일': new_pos_info['entry_date'].strftime('%Y-%m-%d'),
                        '시장 진입가': entry_price,
                        '비중(%)': self._format_weight_pct(weight_pct),
                        '수익률(%)': '0.00',
                        '차익실현': screening_row.get('차익실현', '없음'),
                        '손절매': screening_row.get('손절매', '없음'),
                        '수익보호': screening_row.get('수익보호', '없음'),
                        '롱여부': str(is_long_strategy),
                        
                    })
                    current_symbols_in_file.discard(symbol)



            # 결과 파일 업데이트
            if updated_positions:
                results_df = pd.DataFrame(updated_positions)
                self._ensure_directory(result_file)
                results_df.to_csv(result_file, index=False, encoding='utf-8-sig')
# JSON 파일 생성 추가
                json_file = result_file.replace('.csv', '.json')
                results_df.to_json(json_file, orient='records', indent=2, force_ascii=False)
                print(f"✅ {strategy_name} 포트폴리오 업데이트 완료")

        except Exception as e:
            print(f"❌ {strategy_name} 포트폴리오 관리 오류: {e}")
            print(traceback.format_exc())

    def run_daily_cycle(self):
        """일일 사이클 실행"""
        try:
            print("\n🔄 일일 사이클 시작")

            # 1. 모든 전략 실행하여 최신 스크리닝 결과 생성
            print("\n📊 모든 전략 실행 중...")
            self.run_all_strategies() # 이 함수는 내부적으로 결과를 파일에 저장하거나 반환해야 함
                                     # 현재는 파일에 저장하는 것으로 가정

            # 2. 각 전략별 포트폴리오 관리 (업데이트)
            print("\n💼 포트폴리오 관리 중...")
            for strategy_name, strategy_config in self.strategies.items():
                self.manage_strategy_portfolio(
                    strategy_name,
                    strategy_config['result_file'],
                    is_initial_run=False # 일일 사이클은 업데이트로 간주
                )

            # 3. 포트폴리오 요약 및 리포트 생성
            # portfolio_manager.update_portfolio()는 직접적인 포지션 관리 로직을 포함하므로,
            # manage_strategy_portfolio에서 이미 처리된 내용을 바탕으로 요약 정보를 생성하도록 수정 필요
            # 여기서는 portfolio_manager의 상태를 직접 업데이트하거나, active_positions를 사용해 요약 생성
            
            # PortfolioManager의 포지션 정보 업데이트 (선택적, 현재는 active_positions 사용)
            # self.portfolio_manager.positions = self.active_positions # 이런 식으로 동기화 가능

            # 3. 활성 포지션 업데이트 (손절매/차익실현/트레일링 스탑 체크 및 청산)
            print("\n🛡️ 활성 포지션 업데이트 및 청산 조건 확인 중...")
            self.update_positions()
            
            # portfolio_summary = self.portfolio_manager.get_portfolio_summary() # PortfolioManager에 요약 함수 추가 필요
                                                                            # 또는 self._get_active_positions_summary() 사용
            
            print("\n📝 일일 리포트 생성 중...")
            # generate_daily_report가 active_positions를 사용한다면 portfolio_summary 전달 불필요
            # self.generate_daily_report(portfolio_summary if portfolio_summary else self._get_active_positions_summary_for_report())
            self.generate_daily_report(self._get_active_positions_summary_for_report()) # 항상 active_positions 기반으로 리포트 생성

            print("\n✅ 일일 사이클 완료")

        except Exception as e:
            print(f"❌ 일일 사이클 실행 오류: {e}")
            traceback.print_exc()

    def _get_active_positions_summary_for_report(self) -> Dict[str, List[Dict]]:
        """리포트 생성을 위한 활성 포지션 요약 정보 생성"""
        summary = {}
        for position_key, position_info in self.active_positions.items():
            strategy = position_info['strategy']
            if strategy not in summary:
                summary[strategy] = []
            
            weight_pct_val = (position_info['quantity'] * position_info['entry_price'] / self.initial_capital) * 100
            summary[strategy].append({
                'symbol': position_info['symbol'],
                'current_price': position_info.get('current_price', position_info['entry_price']),
                'pnl_pct': position_info.get('pnl_pct', 0),
                'weight_pct': self._format_weight_pct(weight_pct_val) # 문자열 포맷팅
            })
        return summary

    def _check_exit_conditions(self, position_key: str, current_price: float) -> bool:
        """포지션 청산 조건 체크"""
        try:
            if current_price <= 0:
                print(f"⚠️ 유효하지 않은 현재가: {current_price}")
                return False
            
            position_info = self.active_positions[position_key]
            symbol = position_info['symbol']
            is_long = position_info['is_long']
            
            # 손절매 체크
            if position_info['stop_loss'] is not None:
                if is_long and current_price <= position_info['stop_loss']:
                    print(f"🛑 손절매 발동: {symbol} - 현재가: ${current_price:.2f}, 손절가: ${position_info['stop_loss']:.2f}")
                    self._close_position(position_key, 'stop_loss')
                    return True
                elif not is_long and current_price >= position_info['stop_loss']:
                    print(f"🛑 손절매 발동: {symbol} - 현재가: ${current_price:.2f}, 손절가: ${position_info['stop_loss']:.2f}")
                    self._close_position(position_key, 'stop_loss')
                    return True
            
            # 차익실현 체크
            if position_info['profit_target'] is not None:
                if is_long and current_price >= position_info['profit_target']:
                    print(f"💰 차익실현: {symbol} - 현재가: ${current_price:.2f}, 목표가: ${position_info['profit_target']:.2f}")
                    self._close_position(position_key, 'profit_target')
                    return True
                elif not is_long and current_price <= position_info['profit_target']:
                    print(f"💰 차익실현: {symbol} - 현재가: ${current_price:.2f}, 목표가: ${position_info['profit_target']:.2f}")
                    self._close_position(position_key, 'profit_target')
                    return True

            # 트레일링 스탑 (수익보호) 체크
            if position_info.get('trailing_stop_price') is not None and position_info['trailing_stop_price'] != 0: # 0은 미설정으로 간주
                if is_long and current_price <= position_info['trailing_stop_price']:
                    print(f"🛡️ 수익보호 발동 (Trailing Stop): {symbol} - 현재가: ${current_price:.2f}, 트레일링 손절가: ${position_info['trailing_stop_price']:.2f}")
                    self._close_position(position_key, 'trailing_stop')
                    return True
                elif not is_long and current_price >= position_info['trailing_stop_price']:
                    print(f"🛡️ 수익보호 발동 (Trailing Stop): {symbol} - 현재가: ${current_price:.2f}, 트레일링 손절가: ${position_info['trailing_stop_price']:.2f}")
                    self._close_position(position_key, 'trailing_stop')
                    return True
            
            return False
            
        except Exception as e:
            print(f"❌ {symbol} 청산 조건 체크 오류: {e}")
            print(traceback.format_exc())
            return False

    def _is_long_strategy(self, strategy_name: str) -> bool:
        """전략이 롱 전략인지 확인"""
        return strategy_name in ['strategy1', 'strategy3', 'strategy5']

    def _ensure_directory(self, file_path: str) -> None:
        """파일 경로의 디렉토리가 존재하는지 확인하고 생성"""
        try:
            directory = os.path.dirname(file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
        except Exception as e:
            print(f"❌ 디렉토리 생성 오류 ({directory}): {e}")
            print(traceback.format_exc())

    def _save_portfolio(self, portfolio_file: str) -> None:
        """포트폴리오 저장"""
        try:
            # 디렉토리 존재 확인
            self._ensure_directory(portfolio_file)
            
            portfolio_entries = []
            
            for position_key, position in self.active_positions.items():
                weight_pct = (position['quantity'] * position['entry_price'] / self.initial_capital) * 100
                portfolio_entries.append({
                    '종목명': position['symbol'],
                    '매수일': datetime.now().strftime('%Y-%m-%d'),
                    '시장 진입가': position['entry_price'],
                    '비중(%)': self._format_weight_pct(weight_pct),
                    '수익률(%)': '0.0',
                    '차익실현': '없음',
                    '손절매': f"시장가+{position.get('stop_loss', 0):.2f}" if position.get('stop_loss') else '없음',
                    '수익보호': f"{position.get('trailing_stop', 0):.2f} (25% trailing stop)" if position.get('trailing_stop') else '없음',
                    '롱여부': str(position['is_long'])
                })
            
            portfolio_df = pd.DataFrame(portfolio_entries)
            portfolio_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')
# JSON 파일 생성 추가
            json_file = portfolio_file.replace('.csv', '.json')
            portfolio_df.to_json(json_file, orient='records', indent=2, force_ascii=False)
            
        except Exception as e:
            print(f"❌ 포트폴리오 저장 오류: {e}")
            print(traceback.format_exc())


def main():
    """메인 실행 함수"""
    print("🚀 전략 포트폴리오 통합 시스템 시작")
    
    # 필요한 디렉토리 생성
    ensure_dir(RESULTS_VER2_DIR)
    ensure_dir(os.path.join(RESULTS_VER2_DIR, 'performance'))
    
    # 통합 시스템 초기화
    integrator = StrategyPortfolioIntegrator(initial_capital=100000)
    
    # 일일 사이클 실행
    integrator.run_daily_cycle()


if __name__ == "__main__":
    main()