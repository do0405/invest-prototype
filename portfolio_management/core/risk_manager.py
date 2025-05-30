# 리스크 관리 모듈 (Risk Manager)
# Trailing Stop 관리, 포지션 사이징, 리스크 지표 모니터링

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import traceback
from scipy import stats

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from config import DATA_US_DIR
from utils import calculate_atr, calculate_historical_volatility

class RiskManager:
    """리스크 관리자"""
    
    def __init__(self, total_capital: float = 100000, max_portfolio_risk: float = 0.02,
                 max_position_weight: float = 0.10, max_sector_weight: float = 0.30):
        """
        Args:
            total_capital: 총 자본금
            max_portfolio_risk: 포트폴리오 최대 리스크 (2%)
            max_position_weight: 개별 포지션 최대 비중 (10%)
            max_sector_weight: 섹터별 최대 비중 (30%)
        """
        self.total_capital = total_capital
        self.max_portfolio_risk = max_portfolio_risk
        self.max_position_weight = max_position_weight
        self.max_sector_weight = max_sector_weight
        
        # 리스크 한도 설정
        self.risk_limits = {
            'max_positions': 20,  # 최대 포지션 수
            'max_daily_loss': 0.05,  # 일일 최대 손실 (5%)
            'max_drawdown': 0.15,  # 최대 낙폭 (15%)
            'var_confidence': 0.95,  # VaR 신뢰도 (95%)
            'correlation_threshold': 0.7  # 상관관계 임계값
        }
        
    def calculate_position_size(self, symbol: str, entry_price: float, stop_loss: float,
                              strategy_risk: float = 0.02) -> Tuple[int, float]:
        """포지션 크기 계산
        
        Args:
            symbol: 종목 심볼
            entry_price: 진입가
            stop_loss: 손절가
            strategy_risk: 전략별 리스크 (기본 2%)
            
        Returns:
            (수량, 투자금액) 튜플
        """
        try:
            # 1주당 리스크 계산
            risk_per_share = abs(entry_price - stop_loss)
            
            if risk_per_share <= 0:
                print(f"⚠️ {symbol}: 리스크가 0 이하입니다. 기본 포지션 크기를 사용합니다.")
                # 기본값: 총자본의 5%
                investment_amount = self.total_capital * 0.05
                quantity = int(investment_amount / entry_price)
                return quantity, investment_amount
                
            # 리스크 기반 포지션 크기 계산
            risk_amount = self.total_capital * strategy_risk
            quantity_by_risk = int(risk_amount / risk_per_share)
            
            # 최대 포지션 비중 제한
            max_investment = self.total_capital * self.max_position_weight
            max_quantity_by_weight = int(max_investment / entry_price)
            
            # 더 작은 값 선택
            final_quantity = min(quantity_by_risk, max_quantity_by_weight)
            final_investment = final_quantity * entry_price
            
            print(f"📊 {symbol} 포지션 크기: {final_quantity}주, 투자금액: ${final_investment:,.2f}")
            print(f"   - 리스크 기반: {quantity_by_risk}주, 비중 제한: {max_quantity_by_weight}주")
            
            return final_quantity, final_investment
            
        except Exception as e:
            print(f"❌ {symbol} 포지션 크기 계산 오류: {e}")
            # 기본값 반환
            investment_amount = self.total_capital * 0.05
            quantity = int(investment_amount / entry_price)
            return quantity, investment_amount
            
    def calculate_trailing_stop(self, symbol: str, entry_price: float, current_price: float,
                               position_type: str = 'long', trailing_pct: float = 0.25,
                               atr_multiplier: float = 2.0) -> Optional[float]:
        """Trailing Stop 계산
        
        Args:
            symbol: 종목 심볼
            entry_price: 진입가
            current_price: 현재가
            position_type: 포지션 타입 ('long' or 'short')
            trailing_pct: 트레일링 비율 (25%)
            atr_multiplier: ATR 배수 (2.0)
            
        Returns:
            새로운 trailing stop 가격
        """
        try:
            # ATR 기반 trailing stop 계산
            atr_stop = self._calculate_atr_trailing_stop(
                symbol, current_price, position_type, atr_multiplier
            )
            
            # 퍼센트 기반 trailing stop 계산
            if position_type == 'long':
                pct_stop = current_price * (1 - trailing_pct)
                # 더 높은 값 선택 (더 보수적)
                trailing_stop = max(atr_stop, pct_stop) if atr_stop else pct_stop
            else:  # short
                pct_stop = current_price * (1 + trailing_pct)
                # 더 낮은 값 선택 (더 보수적)
                trailing_stop = min(atr_stop, pct_stop) if atr_stop else pct_stop
                
            return trailing_stop
            
        except Exception as e:
            print(f"❌ {symbol} Trailing Stop 계산 오류: {e}")
            return None
            
    def _calculate_atr_trailing_stop(self, symbol: str, current_price: float,
                                   position_type: str, atr_multiplier: float) -> Optional[float]:
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
            atr = calculate_atr(recent_data, window=14)
            if atr.empty:
                return None
                
            current_atr = atr.iloc[-1]
            
            if position_type == 'long':
                return current_price - (current_atr * atr_multiplier)
            else:  # short
                return current_price + (current_atr * atr_multiplier)
                
        except Exception as e:
            print(f"❌ {symbol} ATR Trailing Stop 계산 오류: {e}")
            return None
            
    def calculate_portfolio_var(self, positions_df: pd.DataFrame, 
                              confidence_level: float = 0.95) -> float:
        """포트폴리오 VaR (Value at Risk) 계산
        
        Args:
            positions_df: 포지션 DataFrame
            confidence_level: 신뢰도 (기본 95%)
            
        Returns:
            VaR 값 (달러)
        """
        try:
            if positions_df.empty:
                return 0.0
                
            # 각 포지션의 일일 수익률 계산
            returns_data = []
            
            for _, position in positions_df.iterrows():
                symbol = position['symbol']
                current_value = position['current_value']
                
                # 과거 수익률 데이터 가져오기
                daily_returns = self._get_daily_returns(symbol, days=252)  # 1년
                
                if daily_returns is not None and len(daily_returns) > 0:
                    # 포지션 크기에 따른 수익률 조정
                    position_returns = daily_returns * current_value
                    returns_data.append(position_returns)
                    
            if not returns_data:
                return 0.0
                
            # 포트폴리오 수익률 계산 (단순 합산)
            portfolio_returns = np.sum(returns_data, axis=0)
            
            # VaR 계산
            var_percentile = (1 - confidence_level) * 100
            var_value = np.percentile(portfolio_returns, var_percentile)
            
            return abs(var_value)  # 손실 금액이므로 절댓값
            
        except Exception as e:
            print(f"❌ 포트폴리오 VaR 계산 오류: {e}")
            return 0.0
            
    def _get_daily_returns(self, symbol: str, days: int = 252) -> Optional[np.ndarray]:
        """종목의 일일 수익률 가져오기"""
        try:
            file_path = os.path.join(DATA_US_DIR, f'{symbol}.csv')
            
            if not os.path.exists(file_path):
                return None
                
            df = pd.read_csv(file_path)
            df.columns = [col.lower() for col in df.columns]
            
            if 'close' not in df.columns or len(df) < days + 1:
                return None
                
            # 최근 데이터
            recent_data = df.tail(days + 1)
            closes = recent_data['close'].values
            
            # 일일 수익률 계산
            daily_returns = np.diff(closes) / closes[:-1]
            
            return daily_returns
            
        except Exception as e:
            print(f"❌ {symbol} 수익률 계산 오류: {e}")
            return None
            
    def calculate_portfolio_correlation(self, positions_df: pd.DataFrame) -> pd.DataFrame:
        """포트폴리오 내 종목간 상관관계 계산"""
        try:
            if len(positions_df) < 2:
                return pd.DataFrame()
                
            symbols = positions_df['symbol'].tolist()
            returns_matrix = []
            valid_symbols = []
            
            # 각 종목의 수익률 데이터 수집
            for symbol in symbols:
                daily_returns = self._get_daily_returns(symbol, days=60)  # 3개월
                
                if daily_returns is not None and len(daily_returns) >= 30:
                    returns_matrix.append(daily_returns[-30:])  # 최근 30일
                    valid_symbols.append(symbol)
                    
            if len(valid_symbols) < 2:
                return pd.DataFrame()
                
            # 상관관계 매트릭스 계산
            returns_df = pd.DataFrame(returns_matrix, index=valid_symbols).T
            correlation_matrix = returns_df.corr()
            
            return correlation_matrix
            
        except Exception as e:
            print(f"❌ 상관관계 계산 오류: {e}")
            return pd.DataFrame()
            
    def check_risk_limits(self, positions_df: pd.DataFrame, 
                         portfolio_summary: Dict) -> Dict[str, bool]:
        """리스크 한도 확인
        
        Returns:
            각 리스크 지표의 한도 초과 여부
        """
        risk_status = {
            'max_positions_exceeded': False,
            'max_portfolio_risk_exceeded': False,
            'max_position_weight_exceeded': False,
            'high_correlation_detected': False,
            'var_limit_exceeded': False
        }
        
        try:
            # 1. 최대 포지션 수 확인
            if len(positions_df) > self.risk_limits['max_positions']:
                risk_status['max_positions_exceeded'] = True
                print(f"🚨 최대 포지션 수 초과: {len(positions_df)}/{self.risk_limits['max_positions']}")
                
            # 2. 포트폴리오 리스크 확인
            total_unrealized_pnl_pct = portfolio_summary.get('total_unrealized_pnl_pct', 0)
            if total_unrealized_pnl_pct < -(self.max_portfolio_risk * 100):
                risk_status['max_portfolio_risk_exceeded'] = True
                print(f"🚨 포트폴리오 리스크 한도 초과: {total_unrealized_pnl_pct:.2f}%")
                
            # 3. 개별 포지션 비중 확인
            if not positions_df.empty:
                total_value = portfolio_summary.get('total_value', 1)
                max_position_pct = (positions_df['current_value'].max() / total_value) * 100
                
                if max_position_pct > (self.max_position_weight * 100):
                    risk_status['max_position_weight_exceeded'] = True
                    print(f"🚨 개별 포지션 비중 초과: {max_position_pct:.2f}%")
                    
            # 4. 상관관계 확인
            correlation_matrix = self.calculate_portfolio_correlation(positions_df)
            if not correlation_matrix.empty:
                # 대각선 제외하고 최대 상관관계 확인
                np.fill_diagonal(correlation_matrix.values, 0)
                max_correlation = correlation_matrix.abs().max().max()
                
                if max_correlation > self.risk_limits['correlation_threshold']:
                    risk_status['high_correlation_detected'] = True
                    print(f"🚨 높은 상관관계 감지: {max_correlation:.3f}")
                    
            # 5. VaR 한도 확인
            var_value = self.calculate_portfolio_var(positions_df)
            var_limit = self.total_capital * self.risk_limits['max_daily_loss']
            
            if var_value > var_limit:
                risk_status['var_limit_exceeded'] = True
                print(f"🚨 VaR 한도 초과: ${var_value:,.2f} > ${var_limit:,.2f}")
                
        except Exception as e:
            print(f"❌ 리스크 한도 확인 오류: {e}")
            
        return risk_status
        
    def get_risk_metrics(self, positions_df: pd.DataFrame, 
                        portfolio_summary: Dict) -> Dict:
        """리스크 지표 계산"""
        try:
            metrics = {
                'total_positions': len(positions_df),
                'portfolio_value': portfolio_summary.get('total_value', 0),
                'portfolio_risk_pct': portfolio_summary.get('total_unrealized_pnl_pct', 0),
                'var_95': self.calculate_portfolio_var(positions_df, 0.95),
                'var_99': self.calculate_portfolio_var(positions_df, 0.99),
                'max_position_weight': 0,
                'avg_correlation': 0,
                'risk_limits_status': self.check_risk_limits(positions_df, portfolio_summary)
            }
            
            if not positions_df.empty:
                total_value = portfolio_summary.get('total_value', 1)
                position_weights = (positions_df['current_value'] / total_value) * 100
                metrics['max_position_weight'] = position_weights.max()
                
                # 평균 상관관계
                correlation_matrix = self.calculate_portfolio_correlation(positions_df)
                if not correlation_matrix.empty:
                    # 대각선 제외하고 평균 계산
                    np.fill_diagonal(correlation_matrix.values, np.nan)
                    metrics['avg_correlation'] = np.nanmean(correlation_matrix.values)
                    
            return metrics
            
        except Exception as e:
            print(f"❌ 리스크 지표 계산 오류: {e}")
            return {}
            
    def suggest_position_adjustments(self, positions_df: pd.DataFrame,
                                   portfolio_summary: Dict) -> List[Dict]:
        """포지션 조정 제안"""
        suggestions = []
        
        try:
            risk_status = self.check_risk_limits(positions_df, portfolio_summary)
            
            # 1. 포지션 수 초과 시
            if risk_status['max_positions_exceeded']:
                # 손실이 큰 포지션부터 정리 제안
                losing_positions = positions_df[positions_df['unrealized_pnl'] < 0]
                if not losing_positions.empty:
                    worst_position = losing_positions.loc[losing_positions['unrealized_pnl'].idxmin()]
                    suggestions.append({
                        'type': 'close_position',
                        'symbol': worst_position['symbol'],
                        'reason': '포지션 수 한도 초과 - 최대 손실 포지션 정리',
                        'current_pnl': worst_position['unrealized_pnl']
                    })
                    
            # 2. 개별 포지션 비중 초과 시
            if risk_status['max_position_weight_exceeded']:
                total_value = portfolio_summary.get('total_value', 1)
                positions_df['weight_pct'] = (positions_df['current_value'] / total_value) * 100
                
                overweight_positions = positions_df[
                    positions_df['weight_pct'] > (self.max_position_weight * 100)
                ]
                
                for _, position in overweight_positions.iterrows():
                    target_value = self.total_capital * self.max_position_weight
                    reduce_amount = position['current_value'] - target_value
                    
                    suggestions.append({
                        'type': 'reduce_position',
                        'symbol': position['symbol'],
                        'reason': '포지션 비중 초과',
                        'current_weight': position['weight_pct'],
                        'target_weight': self.max_position_weight * 100,
                        'reduce_amount': reduce_amount
                    })
                    
            # 3. 높은 상관관계 감지 시
            if risk_status['high_correlation_detected']:
                correlation_matrix = self.calculate_portfolio_correlation(positions_df)
                if not correlation_matrix.empty:
                    # 가장 높은 상관관계를 가진 종목 쌍 찾기
                    np.fill_diagonal(correlation_matrix.values, 0)
                    max_corr_idx = np.unravel_index(
                        np.argmax(correlation_matrix.abs().values), 
                        correlation_matrix.shape
                    )
                    
                    symbol1 = correlation_matrix.index[max_corr_idx[0]]
                    symbol2 = correlation_matrix.columns[max_corr_idx[1]]
                    corr_value = correlation_matrix.iloc[max_corr_idx[0], max_corr_idx[1]]
                    
                    suggestions.append({
                        'type': 'diversify',
                        'symbols': [symbol1, symbol2],
                        'reason': f'높은 상관관계 감지 ({corr_value:.3f})',
                        'correlation': corr_value
                    })
                    
        except Exception as e:
            print(f"❌ 포지션 조정 제안 오류: {e}")
            
        return suggestions
        
    def update_trailing_stops(self, position_tracker) -> Dict[str, float]:
        """모든 포지션의 Trailing Stop 업데이트
        
        Args:
            position_tracker: PositionTracker 인스턴스
            
        Returns:
            업데이트된 trailing stop 딕셔너리 {symbol: new_stop_price}
        """
        updated_stops = {}
        
        try:
            for symbol, position in position_tracker.positions.items():
                # 수익이 나는 포지션만 trailing stop 업데이트
                if position.unrealized_pnl > 0:
                    new_stop = self.calculate_trailing_stop(
                        symbol=symbol,
                        entry_price=position.entry_price,
                        current_price=position.current_price,
                        position_type=position.position_type
                    )
                    
                    if new_stop is not None:
                        # 기존 손절가보다 유리한 경우만 업데이트
                        if position.position_type == 'long':
                            if position.stop_loss is None or new_stop > position.stop_loss:
                                position.update_stop_loss(new_stop)
                                updated_stops[symbol] = new_stop
                                print(f"📈 {symbol} Trailing Stop 업데이트: ${new_stop:.2f}")
                        else:  # short
                            if position.stop_loss is None or new_stop < position.stop_loss:
                                position.update_stop_loss(new_stop)
                                updated_stops[symbol] = new_stop
                                print(f"📉 {symbol} Trailing Stop 업데이트: ${new_stop:.2f}")
                                
        except Exception as e:
            print(f"❌ Trailing Stop 업데이트 오류: {e}")
            
        return updated_stops