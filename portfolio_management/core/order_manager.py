# 주문 관리 모듈 (Order Manager)
# 주문 생성, 실행, 추적 기능

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum
import traceback

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from config import RESULTS_VER2_DIR
from utils import ensure_directory_exists, get_us_market_today

class OrderType(Enum):
    """주문 타입"""
    MARKET = "market"  # 시장가
    LIMIT = "limit"   # 지정가
    STOP = "stop"     # 손절
    STOP_LIMIT = "stop_limit"  # 손절 지정가

class OrderSide(Enum):
    """주문 방향"""
    BUY = "buy"
    SELL = "sell"

class OrderStatus(Enum):
    """주문 상태"""
    PENDING = "pending"      # 대기
    FILLED = "filled"        # 체결
    PARTIALLY_FILLED = "partially_filled"  # 부분체결
    CANCELLED = "cancelled"   # 취소
    REJECTED = "rejected"     # 거부

class Order:
    """개별 주문 클래스"""
    
    def __init__(self, order_id: str, symbol: str, side: OrderSide, order_type: OrderType,
                 quantity: int, price: Optional[float] = None, stop_price: Optional[float] = None,
                 strategy: str = "manual", notes: str = ""):
        self.order_id = order_id
        self.symbol = symbol
        self.side = side
        self.order_type = order_type
        self.quantity = quantity
        self.price = price  # 지정가 주문의 경우
        self.stop_price = stop_price  # 손절 주문의 경우
        self.strategy = strategy
        self.notes = notes
        
        # 주문 상태 정보
        self.status = OrderStatus.PENDING
        self.filled_quantity = 0
        self.filled_price = 0.0
        self.commission = 0.0
        
        # 시간 정보
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.filled_at = None
        
    def fill_order(self, filled_quantity: int, filled_price: float, commission: float = 0.0):
        """주문 체결 처리"""
        self.filled_quantity += filled_quantity
        self.filled_price = filled_price
        self.commission += commission
        self.updated_at = datetime.now()
        
        if self.filled_quantity >= self.quantity:
            self.status = OrderStatus.FILLED
            self.filled_at = datetime.now()
        elif self.filled_quantity > 0:
            self.status = OrderStatus.PARTIALLY_FILLED
            
    def cancel_order(self):
        """주문 취소"""
        if self.status == OrderStatus.PENDING:
            self.status = OrderStatus.CANCELLED
            self.updated_at = datetime.now()
            return True
        return False
        
    def to_dict(self) -> Dict:
        """딕셔너리로 변환"""
        return {
            'order_id': self.order_id,
            'symbol': self.symbol,
            'side': self.side.value,
            'order_type': self.order_type.value,
            'quantity': self.quantity,
            'price': self.price,
            'stop_price': self.stop_price,
            'strategy': self.strategy,
            'notes': self.notes,
            'status': self.status.value,
            'filled_quantity': self.filled_quantity,
            'filled_price': self.filled_price,
            'commission': self.commission,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'filled_at': self.filled_at.isoformat() if self.filled_at else None
        }
        
class OrderManager:
    """주문 관리자"""
    
    def __init__(self, commission_rate: float = 0.001):
        """
        Args:
            commission_rate: 수수료율 (기본 0.1%)
        """
        self.commission_rate = commission_rate
        self.orders: Dict[str, Order] = {}  # order_id -> Order
        self.order_counter = 1
        
    def _generate_order_id(self) -> str:
        """주문 ID 생성"""
        order_id = f"ORD_{datetime.now().strftime('%Y%m%d')}_{self.order_counter:06d}"
        self.order_counter += 1
        return order_id
        
    def create_market_order(self, symbol: str, side: OrderSide, quantity: int,
                          strategy: str = "manual", notes: str = "") -> str:
        """시장가 주문 생성
        
        Returns:
            생성된 주문 ID
        """
        try:
            order_id = self._generate_order_id()
            
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=side,
                order_type=OrderType.MARKET,
                quantity=quantity,
                strategy=strategy,
                notes=notes
            )
            
            self.orders[order_id] = order
            print(f"📝 시장가 주문 생성: {order_id} - {symbol} {side.value} {quantity}주")
            
            # 즉시 체결 시뮬레이션
            self._simulate_market_order_fill(order)
            
            return order_id
            
        except Exception as e:
            print(f"❌ 시장가 주문 생성 오류: {e}")
            traceback.print_exc()
            return ""
            
    def create_limit_order(self, symbol: str, side: OrderSide, quantity: int, price: float,
                         strategy: str = "manual", notes: str = "") -> str:
        """지정가 주문 생성
        
        Returns:
            생성된 주문 ID
        """
        try:
            order_id = self._generate_order_id()
            
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=side,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                price=price,
                strategy=strategy,
                notes=notes
            )
            
            self.orders[order_id] = order
            print(f"📝 지정가 주문 생성: {order_id} - {symbol} {side.value} {quantity}주 @ ${price:.2f}")
            
            return order_id
            
        except Exception as e:
            print(f"❌ 지정가 주문 생성 오류: {e}")
            traceback.print_exc()
            return ""
            
    def create_stop_order(self, symbol: str, side: OrderSide, quantity: int, stop_price: float,
                        strategy: str = "manual", notes: str = "") -> str:
        """손절 주문 생성
        
        Returns:
            생성된 주문 ID
        """
        try:
            order_id = self._generate_order_id()
            
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=side,
                order_type=OrderType.STOP,
                quantity=quantity,
                stop_price=stop_price,
                strategy=strategy,
                notes=notes
            )
            
            self.orders[order_id] = order
            print(f"📝 손절 주문 생성: {order_id} - {symbol} {side.value} {quantity}주 @ ${stop_price:.2f}")
            
            return order_id
            
        except Exception as e:
            print(f"❌ 손절 주문 생성 오류: {e}")
            traceback.print_exc()
            return ""
            
    def _simulate_market_order_fill(self, order: Order):
        """시장가 주문 체결 시뮬레이션"""
        try:
            # 현재 가격 가져오기
            current_price = self._get_current_price(order.symbol)
            
            if current_price is None:
                order.status = OrderStatus.REJECTED
                print(f"❌ {order.symbol} 현재가 조회 실패 - 주문 거부")
                return
                
            # 수수료 계산
            commission = current_price * order.quantity * self.commission_rate
            
            # 주문 체결
            order.fill_order(
                filled_quantity=order.quantity,
                filled_price=current_price,
                commission=commission
            )
            
            print(f"✅ 주문 체결: {order.order_id} - {order.quantity}주 @ ${current_price:.2f}")
            print(f"   수수료: ${commission:.2f}")
            
        except Exception as e:
            print(f"❌ 시장가 주문 체결 시뮬레이션 오류: {e}")
            order.status = OrderStatus.REJECTED
            
    def _get_current_price(self, symbol: str) -> Optional[float]:
        """현재 가격 조회 (최신 종가 사용)"""
        try:
            from config import DATA_US_DIR
            
            file_path = os.path.join(DATA_US_DIR, f'{symbol}.csv')
            
            if not os.path.exists(file_path):
                return None
                
            df = pd.read_csv(file_path)
            df.columns = [col.lower() for col in df.columns]
            
            if 'close' not in df.columns or df.empty:
                return None
                
            return float(df['close'].iloc[-1])
            
        except Exception as e:
            print(f"❌ {symbol} 현재가 조회 오류: {e}")
            return None
            
    def check_pending_orders(self):
        """대기 중인 주문 확인 및 처리"""
        try:
            pending_orders = [
                order for order in self.orders.values() 
                if order.status == OrderStatus.PENDING
            ]
            
            for order in pending_orders:
                if order.order_type == OrderType.LIMIT:
                    self._check_limit_order(order)
                elif order.order_type == OrderType.STOP:
                    self._check_stop_order(order)
                    
        except Exception as e:
            print(f"❌ 대기 주문 확인 오류: {e}")
            
    def _check_limit_order(self, order: Order):
        """지정가 주문 체결 확인"""
        try:
            current_price = self._get_current_price(order.symbol)
            
            if current_price is None:
                return
                
            should_fill = False
            
            if order.side == OrderSide.BUY:
                # 매수: 현재가가 지정가 이하일 때 체결
                should_fill = current_price <= order.price
            else:  # SELL
                # 매도: 현재가가 지정가 이상일 때 체결
                should_fill = current_price >= order.price
                
            if should_fill:
                commission = order.price * order.quantity * self.commission_rate
                order.fill_order(
                    filled_quantity=order.quantity,
                    filled_price=order.price,
                    commission=commission
                )
                
                print(f"✅ 지정가 주문 체결: {order.order_id} - {order.quantity}주 @ ${order.price:.2f}")
                
        except Exception as e:
            print(f"❌ 지정가 주문 확인 오류: {e}")
            
    def _check_stop_order(self, order: Order):
        """손절 주문 체결 확인"""
        try:
            current_price = self._get_current_price(order.symbol)
            
            if current_price is None:
                return
                
            should_fill = False
            
            if order.side == OrderSide.SELL:
                # 손절 매도: 현재가가 손절가 이하일 때 체결
                should_fill = current_price <= order.stop_price
            else:  # BUY (공매도 커버)
                # 손절 매수: 현재가가 손절가 이상일 때 체결
                should_fill = current_price >= order.stop_price
                
            if should_fill:
                # 손절은 시장가로 체결
                commission = current_price * order.quantity * self.commission_rate
                order.fill_order(
                    filled_quantity=order.quantity,
                    filled_price=current_price,
                    commission=commission
                )
                
                print(f"🛑 손절 주문 체결: {order.order_id} - {order.quantity}주 @ ${current_price:.2f}")
                
        except Exception as e:
            print(f"❌ 손절 주문 확인 오류: {e}")
            
    def cancel_order(self, order_id: str) -> bool:
        """주문 취소"""
        try:
            if order_id not in self.orders:
                print(f"❌ 주문 ID {order_id}를 찾을 수 없습니다.")
                return False
                
            order = self.orders[order_id]
            
            if order.cancel_order():
                print(f"🚫 주문 취소: {order_id}")
                return True
            else:
                print(f"❌ 주문 취소 실패: {order_id} (상태: {order.status.value})")
                return False
                
        except Exception as e:
            print(f"❌ 주문 취소 오류: {e}")
            return False
            
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        """주문 상태 조회"""
        try:
            if order_id not in self.orders:
                return None
                
            return self.orders[order_id].to_dict()
            
        except Exception as e:
            print(f"❌ 주문 상태 조회 오류: {e}")
            return None
            
    def get_orders_by_symbol(self, symbol: str) -> List[Dict]:
        """종목별 주문 조회"""
        try:
            symbol_orders = [
                order.to_dict() for order in self.orders.values()
                if order.symbol == symbol
            ]
            
            return sorted(symbol_orders, key=lambda x: x['created_at'], reverse=True)
            
        except Exception as e:
            print(f"❌ 종목별 주문 조회 오류: {e}")
            return []
            
    def get_orders_by_strategy(self, strategy: str) -> List[Dict]:
        """전략별 주문 조회"""
        try:
            strategy_orders = [
                order.to_dict() for order in self.orders.values()
                if order.strategy == strategy
            ]
            
            return sorted(strategy_orders, key=lambda x: x['created_at'], reverse=True)
            
        except Exception as e:
            print(f"❌ 전략별 주문 조회 오류: {e}")
            return []
            
    def get_order_summary(self) -> Dict:
        """주문 요약 정보"""
        try:
            total_orders = len(self.orders)
            
            status_counts = {}
            for status in OrderStatus:
                status_counts[status.value] = sum(
                    1 for order in self.orders.values() 
                    if order.status == status
                )
                
            # 체결된 주문의 총 거래량 및 수수료
            filled_orders = [
                order for order in self.orders.values()
                if order.status == OrderStatus.FILLED
            ]
            
            total_volume = sum(order.filled_price * order.filled_quantity for order in filled_orders)
            total_commission = sum(order.commission for order in filled_orders)
            
            return {
                'total_orders': total_orders,
                'status_counts': status_counts,
                'filled_orders_count': len(filled_orders),
                'total_volume': total_volume,
                'total_commission': total_commission,
                'avg_commission_rate': (total_commission / total_volume * 100) if total_volume > 0 else 0
            }
            
        except Exception as e:
            print(f"❌ 주문 요약 정보 오류: {e}")
            return {}
