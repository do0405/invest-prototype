# 성과 분석 모듈 (Performance Analyzer)
# 수익률, 샤프 비율, 최대 낙폭 등의 성과 지표 계산

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

from config import RESULTS_VER2_DIR
from utils import ensure_directory_exists, get_us_market_today

class PerformanceAnalyzer:
    """성과 분석기"""
    
    def __init__(self, initial_capital: float = 100000, risk_free_rate: float = 0.02):
        """
        Args:
            initial_capital: 초기 자본금
            risk_free_rate: 무위험 수익률 (연 2%)
        """
        self.initial_capital = initial_capital
        self.risk_free_rate = risk_free_rate
        
        # 성과 데이터 저장 디렉토리
        self.performance_dir = os.path.join(RESULTS_VER2_DIR, 'performance')
        ensure_directory_exists(self.performance_dir)
        
        # 일별 성과 데이터
        self.daily_performance = pd.DataFrame()
        
    def calculate_portfolio_returns(self, positions_df: pd.DataFrame, 
                                  portfolio_summary: Dict) -> Dict:
        """포트폴리오 수익률 계산
        
        Returns:
            수익률 지표 딕셔너리
        """
        try:
            current_value = portfolio_summary.get('total_value', self.initial_capital)
            
            # 기본 수익률 계산
            total_return = (current_value - self.initial_capital) / self.initial_capital
            total_return_pct = total_return * 100
            
            # 실현/미실현 손익
            total_realized_pnl = portfolio_summary.get('total_realized_pnl', 0)
            total_unrealized_pnl = portfolio_summary.get('total_unrealized_pnl', 0)
            
            realized_return_pct = (total_realized_pnl / self.initial_capital) * 100
            unrealized_return_pct = (total_unrealized_pnl / self.initial_capital) * 100
            
            # 승률 계산
            win_rate = self._calculate_win_rate(positions_df)
            
            # 평균 수익/손실
            avg_win, avg_loss = self._calculate_avg_win_loss(positions_df)
            
            # 손익비 (Profit Factor)
            profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else 0
            
            return {
                'total_return_pct': total_return_pct,
                'realized_return_pct': realized_return_pct,
                'unrealized_return_pct': unrealized_return_pct,
                'current_value': current_value,
                'total_pnl': total_realized_pnl + total_unrealized_pnl,
                'win_rate': win_rate,
                'avg_win_pct': avg_win,
                'avg_loss_pct': avg_loss,
                'profit_factor': profit_factor
            }
            
        except Exception as e:
            print(f"❌ 포트폴리오 수익률 계산 오류: {e}")
            return {}
            
    def _calculate_win_rate(self, positions_df: pd.DataFrame) -> float:
        """승률 계산"""
        try:
            if positions_df.empty:
                return 0.0
                
            # 청산된 포지션만 고려
            closed_positions = positions_df[positions_df['status'] == 'closed']
            
            if closed_positions.empty:
                return 0.0
                
            winning_trades = len(closed_positions[closed_positions['realized_pnl'] > 0])
            total_trades = len(closed_positions)
            
            return (winning_trades / total_trades) * 100 if total_trades > 0 else 0.0
            
        except Exception as e:
            print(f"❌ 승률 계산 오류: {e}")
            return 0.0
            
    def _calculate_avg_win_loss(self, positions_df: pd.DataFrame) -> Tuple[float, float]:
        """평균 수익/손실 계산 (퍼센트)"""
        try:
            if positions_df.empty:
                return 0.0, 0.0
                
            # 청산된 포지션만 고려
            closed_positions = positions_df[positions_df['status'] == 'closed']
            
            if closed_positions.empty:
                return 0.0, 0.0
                
            # 수익률 계산 (진입가 대비)
            closed_positions = closed_positions.copy()
            closed_positions['return_pct'] = (
                (closed_positions['exit_price'] - closed_positions['entry_price']) / 
                closed_positions['entry_price'] * 100
            )
            
            # 롱/숏 포지션 구분
            long_positions = closed_positions[closed_positions['position_type'] == 'long']
            short_positions = closed_positions[closed_positions['position_type'] == 'short']
            
            # 숏 포지션은 수익률 부호 반전
            if not short_positions.empty:
                closed_positions.loc[short_positions.index, 'return_pct'] *= -1
                
            winning_trades = closed_positions[closed_positions['return_pct'] > 0]
            losing_trades = closed_positions[closed_positions['return_pct'] < 0]
            
            avg_win = winning_trades['return_pct'].mean() if not winning_trades.empty else 0.0
            avg_loss = losing_trades['return_pct'].mean() if not losing_trades.empty else 0.0
            
            return avg_win, avg_loss
            
        except Exception as e:
            print(f"❌ 평균 수익/손실 계산 오류: {e}")
            return 0.0, 0.0
            
    def calculate_risk_metrics(self, daily_returns: pd.Series) -> Dict:
        """리스크 지표 계산
        
        Args:
            daily_returns: 일별 수익률 시리즈
            
        Returns:
            리스크 지표 딕셔너리
        """
        try:
            if daily_returns.empty or len(daily_returns) < 2:
                return {}
                
            # 기본 통계
            annual_return = daily_returns.mean() * 252  # 연환산
            annual_volatility = daily_returns.std() * np.sqrt(252)  # 연환산
            
            # 샤프 비율
            excess_return = annual_return - self.risk_free_rate
            sharpe_ratio = excess_return / annual_volatility if annual_volatility > 0 else 0
            
            # 최대 낙폭 (Maximum Drawdown)
            cumulative_returns = (1 + daily_returns).cumprod()
            running_max = cumulative_returns.expanding().max()
            drawdown = (cumulative_returns - running_max) / running_max
            max_drawdown = drawdown.min()
            
            # 칼마 비율 (Calmar Ratio)
            calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
            
            # 소르티노 비율 (Sortino Ratio)
            negative_returns = daily_returns[daily_returns < 0]
            downside_deviation = negative_returns.std() * np.sqrt(252) if len(negative_returns) > 0 else 0
            sortino_ratio = excess_return / downside_deviation if downside_deviation > 0 else 0
            
            # VaR (Value at Risk) - 95% 신뢰도
            var_95 = np.percentile(daily_returns, 5)
            var_99 = np.percentile(daily_returns, 1)
            
            # 베타 계산 (S&P 500 대비) - 데이터가 있는 경우
            beta = self._calculate_beta(daily_returns)
            
            return {
                'annual_return': annual_return * 100,
                'annual_volatility': annual_volatility * 100,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown * 100,
                'calmar_ratio': calmar_ratio,
                'sortino_ratio': sortino_ratio,
                'var_95': var_95 * 100,
                'var_99': var_99 * 100,
                'beta': beta,
                'total_trading_days': len(daily_returns)
            }
            
        except Exception as e:
            print(f"❌ 리스크 지표 계산 오류: {e}")
            return {}
            
    def _calculate_beta(self, portfolio_returns: pd.Series) -> float:
        """베타 계산 (S&P 500 대비)"""
        try:
            # S&P 500 데이터 로드 시도
            from config import DATA_US_DIR
            
            spy_file = os.path.join(DATA_US_DIR, 'SPY.csv')
            
            if not os.path.exists(spy_file):
                return 1.0  # 기본값
                
            spy_df = pd.read_csv(spy_file)
            spy_df.columns = [col.lower() for col in spy_df.columns]
            
            if 'close' not in spy_df.columns or len(spy_df) < 2:
                return 1.0
                
            # S&P 500 일별 수익률 계산
            spy_returns = spy_df['close'].pct_change().dropna()
            
            # 날짜 범위 맞추기 (최근 데이터)
            min_length = min(len(portfolio_returns), len(spy_returns))
            
            if min_length < 30:  # 최소 30일 데이터 필요
                return 1.0
                
            portfolio_recent = portfolio_returns.tail(min_length)
            spy_recent = spy_returns.tail(min_length)
            
            # 베타 계산 (공분산 / 시장 분산)
            covariance = np.cov(portfolio_recent, spy_recent)[0, 1]
            market_variance = np.var(spy_recent)
            
            beta = covariance / market_variance if market_variance > 0 else 1.0
            
            return beta
            
        except Exception as e:
            print(f"❌ 베타 계산 오류: {e}")
            return 1.0
            
    def calculate_strategy_performance(self, positions_df: pd.DataFrame) -> Dict[str, Dict]:
        """전략별 성과 분석"""
        try:
            if positions_df.empty:
                return {}
                
            strategy_performance = {}
            
            # 전략별 그룹화
            for strategy in positions_df['strategy'].unique():
                strategy_positions = positions_df[positions_df['strategy'] == strategy]
                
                # 청산된 포지션만 고려
                closed_positions = strategy_positions[strategy_positions['status'] == 'closed']
                
                if closed_positions.empty:
                    continue
                    
                # 전략별 성과 계산
                total_trades = len(closed_positions)
                winning_trades = len(closed_positions[closed_positions['realized_pnl'] > 0])
                win_rate = (winning_trades / total_trades) * 100
                
                total_pnl = closed_positions['realized_pnl'].sum()
                avg_pnl = closed_positions['realized_pnl'].mean()
                
                # 수익률 계산
                closed_positions = closed_positions.copy()
                closed_positions['return_pct'] = (
                    (closed_positions['exit_price'] - closed_positions['entry_price']) / 
                    closed_positions['entry_price'] * 100
                )
                
                # 숏 포지션 수익률 조정
                short_mask = closed_positions['position_type'] == 'short'
                closed_positions.loc[short_mask, 'return_pct'] *= -1
                
                avg_return = closed_positions['return_pct'].mean()
                best_trade = closed_positions['return_pct'].max()
                worst_trade = closed_positions['return_pct'].min()
                
                # 연속 승/패 계산
                consecutive_wins, consecutive_losses = self._calculate_consecutive_trades(
                    closed_positions['return_pct']
                )
                
                strategy_performance[strategy] = {
                    'total_trades': total_trades,
                    'winning_trades': winning_trades,
                    'losing_trades': total_trades - winning_trades,
                    'win_rate': win_rate,
                    'total_pnl': total_pnl,
                    'avg_pnl': avg_pnl,
                    'avg_return_pct': avg_return,
                    'best_trade_pct': best_trade,
                    'worst_trade_pct': worst_trade,
                    'max_consecutive_wins': consecutive_wins,
                    'max_consecutive_losses': consecutive_losses
                }
                
            return strategy_performance
            
        except Exception as e:
            print(f"❌ 전략별 성과 분석 오류: {e}")
            return {}
            
    def _calculate_consecutive_trades(self, returns: pd.Series) -> Tuple[int, int]:
        """연속 승/패 계산"""
        try:
            if returns.empty:
                return 0, 0
                
            # 승/패 시퀀스 생성
            wins_losses = (returns > 0).astype(int)
            
            # 연속 승리 계산
            consecutive_wins = 0
            max_consecutive_wins = 0
            
            # 연속 패배 계산
            consecutive_losses = 0
            max_consecutive_losses = 0
            
            for win in wins_losses:
                if win == 1:  # 승리
                    consecutive_wins += 1
                    consecutive_losses = 0
                    max_consecutive_wins = max(max_consecutive_wins, consecutive_wins)
                else:  # 패배
                    consecutive_losses += 1
                    consecutive_wins = 0
                    max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)
                    
            return max_consecutive_wins, max_consecutive_losses
            
        except Exception as e:
            print(f"❌ 연속 거래 계산 오류: {e}")
            return 0, 0
            
    def update_daily_performance(self, date: datetime, portfolio_value: float,
                               daily_pnl: float = 0, trades_count: int = 0):
        """일별 성과 업데이트"""
        try:
            date_str = date.strftime('%Y-%m-%d')
            
            # 일별 수익률 계산
            if len(self.daily_performance) > 0:
                prev_value = self.daily_performance['portfolio_value'].iloc[-1]
                daily_return = (portfolio_value - prev_value) / prev_value
            else:
                daily_return = (portfolio_value - self.initial_capital) / self.initial_capital
                
            # 새 데이터 추가
            new_data = {
                'date': date_str,
                'portfolio_value': portfolio_value,
                'daily_return': daily_return,
                'daily_pnl': daily_pnl,
                'trades_count': trades_count,
                'cumulative_return': (portfolio_value - self.initial_capital) / self.initial_capital
            }
            
            # DataFrame에 추가
            new_row = pd.DataFrame([new_data])
            self.daily_performance = pd.concat([self.daily_performance, new_row], ignore_index=True)
            
            # 파일 저장
            self._save_daily_performance()
            
        except Exception as e:
            print(f"❌ 일별 성과 업데이트 오류: {e}")
            
    def _save_daily_performance(self):
        """일별 성과 데이터 저장"""
        try:
            file_path = os.path.join(self.performance_dir, 'daily_performance.csv')
            self.daily_performance.to_csv(file_path, index=False)
            
        except Exception as e:
            print(f"❌ 일별 성과 저장 오류: {e}")
            
    def load_daily_performance(self):
        """일별 성과 데이터 로드"""
        try:
            file_path = os.path.join(self.performance_dir, 'daily_performance.csv')
            
            if os.path.exists(file_path):
                self.daily_performance = pd.read_csv(file_path)
                print(f"📂 {len(self.daily_performance)}일의 성과 데이터를 로드했습니다.")
            else:
                self.daily_performance = pd.DataFrame()
                
        except Exception as e:
            print(f"❌ 일별 성과 로드 오류: {e}")
            self.daily_performance = pd.DataFrame()
            
    def generate_performance_report(self, positions_df: pd.DataFrame,
                                  portfolio_summary: Dict) -> Dict:
        """종합 성과 리포트 생성"""
        try:
            # 기본 수익률 지표
            returns_metrics = self.calculate_portfolio_returns(positions_df, portfolio_summary)
            
            # 리스크 지표 (일별 수익률이 있는 경우)
            risk_metrics = {}
            if not self.daily_performance.empty and 'daily_return' in self.daily_performance.columns:
                daily_returns = self.daily_performance['daily_return']
                risk_metrics = self.calculate_risk_metrics(daily_returns)
                
            # 전략별 성과
            strategy_metrics = self.calculate_strategy_performance(positions_df)
            
            # 월별 성과 (일별 데이터가 있는 경우)
            monthly_performance = self._calculate_monthly_performance()
            
            # 종목별 성과
            symbol_performance = self._calculate_symbol_performance(positions_df)
            
            report = {
                'report_date': datetime.now().isoformat(),
                'returns_metrics': returns_metrics,
                'risk_metrics': risk_metrics,
                'strategy_performance': strategy_metrics,
                'monthly_performance': monthly_performance,
                'symbol_performance': symbol_performance,
                'portfolio_summary': portfolio_summary
            }
            
            # 리포트 저장
            self._save_performance_report(report)
            
            return report
            
        except Exception as e:
            print(f"❌ 성과 리포트 생성 오류: {e}")
            return {}
            
    def _calculate_monthly_performance(self) -> Dict:
        """월별 성과 계산"""
        try:
            if self.daily_performance.empty:
                return {}
                
            df = self.daily_performance.copy()
            df['date'] = pd.to_datetime(df['date'])
            df['year_month'] = df['date'].dt.to_period('M')
            
            monthly_stats = df.groupby('year_month').agg({
                'daily_return': ['sum', 'std', 'count'],
                'portfolio_value': ['first', 'last'],
                'trades_count': 'sum'
            }).round(4)
            
            # 컬럼명 정리
            monthly_stats.columns = ['monthly_return', 'volatility', 'trading_days', 
                                   'start_value', 'end_value', 'total_trades']
            
            return monthly_stats.to_dict('index')
            
        except Exception as e:
            print(f"❌ 월별 성과 계산 오류: {e}")
            return {}
            
    def _calculate_symbol_performance(self, positions_df: pd.DataFrame) -> Dict:
        """종목별 성과 계산"""
        try:
            if positions_df.empty:
                return {}
                
            symbol_stats = {}
            
            for symbol in positions_df['symbol'].unique():
                symbol_positions = positions_df[positions_df['symbol'] == symbol]
                closed_positions = symbol_positions[symbol_positions['status'] == 'closed']
                
                if closed_positions.empty:
                    continue
                    
                total_trades = len(closed_positions)
                winning_trades = len(closed_positions[closed_positions['realized_pnl'] > 0])
                total_pnl = closed_positions['realized_pnl'].sum()
                
                symbol_stats[symbol] = {
                    'total_trades': total_trades,
                    'winning_trades': winning_trades,
                    'win_rate': (winning_trades / total_trades) * 100,
                    'total_pnl': total_pnl,
                    'avg_pnl': total_pnl / total_trades
                }
                
            return symbol_stats
            
        except Exception as e:
            print(f"❌ 종목별 성과 계산 오류: {e}")
            return {}
            
    def _save_performance_report(self, report: Dict):
        """성과 리포트 저장"""
        try:
            import json
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path = os.path.join(self.performance_dir, f'performance_report_{timestamp}.json')
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
                
            print(f"📊 성과 리포트 저장: {file_path}")
            
        except Exception as e:
            print(f"❌ 성과 리포트 저장 오류: {e}")