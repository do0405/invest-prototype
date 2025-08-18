"""Stock ranking system using Multi-Criteria Decision Analysis (MCDA).

Integrates with existing codebase to provide comprehensive stock ranking
based on technical, fundamental, and market indicators.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path
import logging
from datetime import datetime, timedelta
from config import DATA_US_DIR

# Import existing modules for integration
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .mcda_calculator import MCDACalculator, MCDAMethod
from .criteria_weights import CriteriaWeights, InvestmentStrategy

# Import existing utility modules
try:
    from utils.calc_utils import *
    from utils.io_utils import *
    from utils.relative_strength import calculate_rs_score
except ImportError as e:
    logging.warning(f"Could not import some utility modules: {e}")
    logging.warning("Some features may not be available")

class StockRankingSystem:
    """Comprehensive stock ranking system using MCDA methods.
    
    This system integrates with the existing codebase to collect data,
    calculate various indicators, and rank stocks using multiple criteria
    decision analysis methods.
    """
    
    def __init__(self,
                 data_directory: Optional[str] = None,
                 cache_directory: Optional[str] = None):
        """Initialize the ranking system.
        
        Args:
            data_directory: Directory containing stock data. If ``None``,
                uses ``config.DATA_US_DIR``.
            cache_directory: Directory for caching calculated indicators
        """
        if data_directory is None:
            data_directory = DATA_US_DIR
        self.data_directory = Path(data_directory)
        self.cache_directory = Path(cache_directory) if cache_directory else None
        
        # Initialize components
        self.mcda_calculator = MCDACalculator()
        self.criteria_weights = CriteriaWeights()
        

            
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Cache for calculated indicators
        self._indicator_cache = {}
        
    def load_stock_data(self, symbol: str, days: int = 330) -> Optional[pd.DataFrame]:
        """Load stock data for a given symbol.
        
        Args:
            symbol: Stock symbol
            days: Number of days of data to load
            
        Returns:
            DataFrame with OHLCV data or None if not found
        """
        try:
            # Try to load from existing data files
            file_path = self.data_directory / f"{symbol}.csv"
            if file_path.exists():
                df = pd.read_csv(file_path)
                df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
                df = df.sort_values('date').tail(days).reset_index(drop=True)
                return df
            else:
                self.logger.warning(f"Data file not found for {symbol}")
                return None
        except Exception as e:
            self.logger.error(f"Error loading data for {symbol}: {e}")
            return None
            
    def get_available_symbols(self) -> List[str]:
        """Get list of available stock symbols.
        
        Returns:
            List of available stock symbols
        """
        symbols = []
        try:
            for file_path in self.data_directory.glob("*.csv"):
                symbol = file_path.stem
                if symbol not in ['SPY', 'QQQ', 'IWM']:  # Exclude index ETFs
                    symbols.append(symbol)
        except Exception as e:
            self.logger.error(f"Error getting available symbols: {e}")
            
        return sorted(symbols)
        
    def calculate_technical_indicators(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate technical indicators for a stock.
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            Dictionary of technical indicator values
        """
        indicators = {}
        
        try:
            # RSI
            indicators['rsi_14'] = self._calculate_rsi(df['close'], 14)
            
            # MACD
            macd_line, macd_signal, _ = self._calculate_macd(df['close'])
            indicators['macd_signal'] = macd_signal
            
            # Volume ratio
            avg_volume = df['volume'].tail(20).mean()
            current_volume = df['volume'].iloc[-1]
            indicators['volume_ratio'] = current_volume / avg_volume if avg_volume > 0 else 1.0
            
            # Price momentum
            current_price = df['close'].iloc[-1]
            price_20d_ago = df['close'].iloc[-21] if len(df) >= 21 else df['close'].iloc[0]
            price_60d_ago = df['close'].iloc[-61] if len(df) >= 61 else df['close'].iloc[0]
            
            indicators['price_momentum_20d'] = ((current_price - price_20d_ago) / price_20d_ago) * 100
            indicators['price_momentum_60d'] = ((current_price - price_60d_ago) / price_60d_ago) * 100
            
            # ATR (normalized)
            atr = self._calculate_atr(df, 14)
            indicators['atr_normalized'] = atr / current_price if current_price > 0 else 0
            
            # Bollinger Bands position
            bb_upper, bb_lower, bb_middle = self._calculate_bollinger_bands(df['close'], 20, 2)
            if bb_upper > bb_lower:
                indicators['bollinger_position'] = (current_price - bb_lower) / (bb_upper - bb_lower)
            else:
                indicators['bollinger_position'] = 0.5
                
            # Volatility
            returns = df['close'].pct_change().dropna()
            indicators['volatility_20d'] = returns.tail(20).std() * np.sqrt(252) if len(returns) >= 20 else 0
            
            # Maximum drawdown
            rolling_max = df['close'].expanding().max()
            drawdown = (df['close'] - rolling_max) / rolling_max
            indicators['max_drawdown'] = abs(drawdown.min()) * 100
            
        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {e}")
            
        return indicators
    
    def _detect_stock_split_distortion(self, df: pd.DataFrame, symbol: str) -> bool:
        """액면병합으로 인한 가격 왜곡을 감지합니다.
        
        Args:
            df: DataFrame with OHLCV data
            symbol: Stock symbol for logging
            
        Returns:
            True if stock split distortion is detected, False otherwise
        """
        try:
            if len(df) < 60:  # 최소 60일 데이터 필요
                return False
                
            # 최근 30일간의 데이터 분석
            recent_data = df.tail(30).copy()
            
            # 1. 급격한 가격 변화 감지 (하루에 50% 이상 하락 또는 2배 이상 상승)
            daily_returns = recent_data['close'].pct_change().dropna()
            extreme_changes = daily_returns[(daily_returns < -0.5) | (daily_returns > 1.0)]
            
            if len(extreme_changes) > 0:
                self.logger.debug(f"{symbol}: Extreme price change detected: {extreme_changes.max():.2%}")
                
                # 극단적 변화가 있는 날의 거래량 확인
                extreme_dates = extreme_changes.index
                for date_idx in extreme_dates:
                    # 해당 날짜의 거래량이 평균의 3배 이상인지 확인
                    avg_volume = recent_data['volume'].mean()
                    day_volume = recent_data.loc[date_idx, 'volume']
                    
                    if day_volume > avg_volume * 3:
                        return True
            
            # 2. 연속적인 가격 패턴 이상 감지
            # 가격이 갑자기 1/2, 1/3, 1/4 등으로 떨어지는 패턴
            price_ratios = recent_data['close'] / recent_data['close'].shift(1)
            suspicious_ratios = price_ratios[
                (price_ratios.between(0.45, 0.55)) |  # 약 1/2
                (price_ratios.between(0.30, 0.37)) |  # 약 1/3
                (price_ratios.between(0.23, 0.27)) |  # 약 1/4
                (price_ratios.between(0.18, 0.22))    # 약 1/5
            ]
            
            if len(suspicious_ratios) > 0:
                self.logger.debug(f"{symbol}: Suspicious price ratio detected: {suspicious_ratios.iloc[0]:.3f}")
                return True
            
            # 3. 가격 레벨의 급격한 변화 감지
            # 최근 5일 평균 가격과 그 이전 25일 평균 가격 비교
            if len(df) >= 30:
                recent_5d_avg = recent_data['close'].tail(5).mean()
                previous_25d_avg = df['close'].iloc[-30:-5].mean()
                
                price_level_ratio = recent_5d_avg / previous_25d_avg
                
                # 가격 레벨이 갑자기 1/2 이하로 떨어지거나 2배 이상 오른 경우
                if price_level_ratio < 0.6 or price_level_ratio > 1.8:
                    # 동시에 거래량도 급증했는지 확인
                    recent_5d_vol = recent_data['volume'].tail(5).mean()
                    previous_25d_vol = df['volume'].iloc[-30:-5].mean()
                    volume_ratio = recent_5d_vol / previous_25d_vol if previous_25d_vol > 0 else 1
                    
                    if volume_ratio > 2.0:  # 거래량이 2배 이상 증가
                        self.logger.debug(f"{symbol}: Price level change with volume surge: {price_level_ratio:.3f}, vol: {volume_ratio:.2f}")
                        return True
            
            # 4. 기술적 지표의 급격한 변화 감지
            # RSI가 갑자기 극값에서 중간값으로 이동하는 패턴
            if len(recent_data) >= 14:
                rsi_values = []
                for i in range(len(recent_data) - 13):
                    rsi = self._calculate_rsi(recent_data['close'].iloc[i:i+14], 14)
                    rsi_values.append(rsi)
                
                if len(rsi_values) >= 10:
                    rsi_series = pd.Series(rsi_values)
                    rsi_change = abs(rsi_series.iloc[-1] - rsi_series.iloc[-10])
                    
                    # RSI가 10일 사이에 40 이상 변화한 경우 (극값에서 중간값으로)
                    if rsi_change > 40:
                        self.logger.debug(f"{symbol}: Extreme RSI change detected: {rsi_change:.1f}")
                        return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error detecting stock split distortion for {symbol}: {e}")
            return False
    
    def _detect_split_with_auto_adjust(self, symbol: str) -> bool:
        """auto_adjust 설정 차이를 이용한 액면병합 감지
        
        최근 60일간의 데이터에서 auto_adjust=False와 auto_adjust=True 설정 간
        차이가 발생하는 경우 해당 티커를 액면병합으로 판단하여 제외합니다.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            True if split detected (should exclude), False otherwise
        """
        try:
            import yfinance as yf
            from datetime import datetime, timedelta
            
            # 최근 60일 데이터 가져오기
            end_date = datetime.now()
            start_date = end_date - timedelta(days=60)
            
            # auto_adjust=False로 데이터 가져오기
            ticker_obj = yf.Ticker(symbol)
            data_no_adjust = ticker_obj.history(
                start=start_date, 
                end=end_date, 
                auto_adjust=False,
                actions=False
            )
            
            # auto_adjust=True로 데이터 가져오기
            data_adjust = ticker_obj.history(
                start=start_date, 
                end=end_date, 
                auto_adjust=True,
                actions=False
            )
            
            if data_no_adjust.empty or data_adjust.empty:
                return False
                
            if len(data_no_adjust) != len(data_adjust):
                return False
            
            # 동일한 날짜 데이터만 비교
            common_dates = data_no_adjust.index.intersection(data_adjust.index)
            if len(common_dates) < 30:  # 최소 30일 데이터 필요
                return False
            
            data_no_adjust_common = data_no_adjust.loc[common_dates]
            data_adjust_common = data_adjust.loc[common_dates]
            
            # 종가 기준으로 차이 계산
            price_diff_ratio = abs(data_no_adjust_common['Close'] - data_adjust_common['Close']) / data_adjust_common['Close']
            
            # 5% 이상 차이가 나는 날이 3일 이상 있으면 액면병합으로 판단
            significant_diff_days = (price_diff_ratio > 0.05).sum()
            
            if significant_diff_days >= 3:
                self.logger.info(f"{symbol}: Auto-adjust 차이 감지 - {significant_diff_days}일간 5% 이상 차이")
                return True
            
            # 거래량 차이도 확인
            volume_diff_ratio = abs(data_no_adjust_common['Volume'] - data_adjust_common['Volume']) / data_adjust_common['Volume']
            significant_vol_diff_days = (volume_diff_ratio > 0.1).sum()  # 10% 이상 차이
            
            if significant_vol_diff_days >= 3:
                self.logger.info(f"{symbol}: Auto-adjust 거래량 차이 감지 - {significant_vol_diff_days}일간 10% 이상 차이")
                return True
                
            return False
            
        except Exception as e:
            self.logger.warning(f"Auto-adjust 기반 액면병합 감지 실패 for {symbol}: {e}")
            return False  # 에러 발생 시 보수적으로 포함
        
    # Fundamental indicators removed as requested
        
    def calculate_market_indicators(self, symbol: str, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate market-related indicators for a stock.
        
        Args:
            symbol: Stock symbol
            df: DataFrame with OHLCV data
            
        Returns:
            Dictionary of market indicator values
        """
        indicators = {}
        
        try:
            # Market cap calculation removed as requested
            current_price = df['close'].iloc[-1]
            
            # RS 점수는 배치로 계산하므로 여기서는 기본값 설정
            # 실제 RS 점수는 calculate_batch_rs_scores에서 계산됨
            indicators['relative_strength'] = 50  # 기본값, 나중에 배치로 업데이트
                
            # Beta calculation
            try:
                if 'spy_returns' in locals() and len(stock_returns) == len(spy_returns) and len(stock_returns) > 20:
                    covariance = np.cov(stock_returns, spy_returns)[0][1]
                    spy_variance = np.var(spy_returns)
                    indicators['beta'] = covariance / spy_variance if spy_variance > 0 else 1.0
                else:
                    indicators['beta'] = 1.0
            except:
                indicators['beta'] = 1.0
                
            # Sharpe ratio (simplified)
            returns = df['close'].pct_change().dropna()
            if len(returns) > 20:
                excess_returns = returns - 0.02/252  # Assuming 2% risk-free rate
                indicators['sharpe_ratio'] = excess_returns.mean() / returns.std() * np.sqrt(252) if returns.std() > 0 else 0
            else:
                indicators['sharpe_ratio'] = 0
                
        except Exception as e:
            self.logger.error(f"Error calculating market indicators for {symbol}: {e}")
            
        return indicators
    
    def calculate_batch_rs_scores(self, symbols: List[str]) -> Dict[str, float]:
        """Mark Minervini 방식으로 배치 RS 점수 계산"""
        try:
            from utils.relative_strength import calculate_rs_score
            
            # 모든 종목 데이터를 결합
            combined_data_list = []
            
            # SPY 데이터 로드
            spy_data = self.load_stock_data('SPY')
            if spy_data is None:
                self.logger.warning("SPY 데이터를 로드할 수 없습니다. RS 점수 계산을 건너뜁니다.")
                return {symbol: 50 for symbol in symbols}
            
            # SPY 데이터 추가
            spy_df = spy_data.copy()
            spy_df['symbol'] = 'SPY'
            if 'date' in spy_df.columns:
                spy_df['date'] = pd.to_datetime(spy_df['date'])
            combined_data_list.append(spy_df[['date', 'symbol', 'close']])
            
            # 각 종목 데이터 추가
            valid_symbols = []
            for symbol in symbols:
                try:
                    df = self.load_stock_data(symbol)
                    if df is not None and len(df) >= 252:
                        stock_df = df.copy()
                        stock_df['symbol'] = symbol
                        if 'date' in stock_df.columns:
                            stock_df['date'] = pd.to_datetime(stock_df['date'])
                        combined_data_list.append(stock_df[['date', 'symbol', 'close']])
                        valid_symbols.append(symbol)
                except Exception as e:
                    self.logger.warning(f"종목 {symbol} 데이터 로드 실패: {e}")
                    continue
            
            if not combined_data_list:
                return {symbol: 50 for symbol in symbols}
            
            # 데이터 결합
            combined_df = pd.concat(combined_data_list, ignore_index=True)
            combined_df = combined_df.set_index(['date', 'symbol'])
            
            # RS 점수 계산 (고도화된 버전 사용)
            print("📊 고도화된 RS 점수 계산 중...")
            rs_scores = calculate_rs_score(combined_df, price_col='close', use_enhanced=True)
            print(f"✅ RS 점수 계산 완료: {len(rs_scores)}개 종목")
            
            # 결과 딕셔너리 생성
            result = {}
            for symbol in symbols:
                if symbol in rs_scores:
                    result[symbol] = rs_scores[symbol]
                else:
                    result[symbol] = 50  # 기본값
            
            return result
            
        except Exception as e:
            self.logger.error(f"배치 RS 점수 계산 오류: {e}")
            return {symbol: 50 for symbol in symbols}
        
    def calculate_all_indicators(self, symbol: str) -> Dict[str, float]:
        """Calculate all indicators for a stock.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary of all indicator values
        """
        # Check cache first
        cache_key = f"{symbol}_{datetime.now().strftime('%Y-%m-%d')}"
        if cache_key in self._indicator_cache:
            return self._indicator_cache[cache_key]
            
        indicators = {}
        
        # Load stock data
        df = self.load_stock_data(symbol)
        if df is None or len(df) < 20:
            self.logger.warning(f"Insufficient data for {symbol}")
            return indicators
            
        # Calculate all types of indicators
        technical = self.calculate_technical_indicators(df)
        market = self.calculate_market_indicators(symbol, df)
        
        indicators.update(technical)
        indicators.update(market)
        
        # Add some sentiment indicators (placeholder)
        # 애널리스트 평점, 내부자 소유권, 기관 소유권 데이터 제거됨
        # 실제 데이터 소스 연결 시 구현 예정
        
        # Cache the results
        self._indicator_cache[cache_key] = indicators
        
        return indicators
        
    def rank_stocks(self, 
                   symbols: Optional[List[str]] = None,
                   strategy: InvestmentStrategy = InvestmentStrategy.BALANCED,
                   method: MCDAMethod = MCDAMethod.TOPSIS,
                   custom_weights: Optional[Dict[str, float]] = None,
                   min_data_points: int = 100) -> pd.DataFrame:
        """Rank stocks using MCDA methods.
        
        Args:
            symbols: List of symbols to rank (if None, use all available)
            strategy: Investment strategy for weight selection
            method: MCDA method to use
            custom_weights: Custom criteria weights (overrides strategy)
            min_data_points: Minimum data points required for ranking
            
        Returns:
            DataFrame with ranked stocks and scores
        """
        if symbols is None:
            symbols = self.get_available_symbols()
            
        self.logger.info(f"Ranking {len(symbols)} stocks using {method.value} method")
        
        # Collect data for all symbols
        stock_data = {}
        valid_symbols = []
        
        # 먼저 유효한 종목들을 식별 (액면병합 필터링 포함)
        for symbol in symbols:
            try:
                df = self.load_stock_data(symbol)
                if df is not None and len(df) >= min_data_points:
                    # 액면병합 감지 및 필터링 (auto_adjust 기반 + 기존 방법 병행)
                    split_detected_traditional = self._detect_stock_split_distortion(df, symbol)
                    split_detected_auto_adjust = self._detect_split_with_auto_adjust(symbol)
                    
                    if not split_detected_traditional and not split_detected_auto_adjust:
                        valid_symbols.append(symbol)
                    else:
                        detection_method = "traditional" if split_detected_traditional else "auto_adjust"
                        self.logger.info(f"Stock split distortion detected for {symbol} using {detection_method} method, excluding from ranking")
                else:
                    self.logger.debug(f"Insufficient data for {symbol}")
            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
        
        if not valid_symbols:
            self.logger.error("No valid symbols found")
            return pd.DataFrame()
        
        # 배치로 RS 점수 계산
        print(f"📊 {len(valid_symbols)}개 종목에 대한 배치 RS 점수 계산 시작...")
        batch_rs_scores = self.calculate_batch_rs_scores(valid_symbols)
        
        # 각 종목의 지표 계산
        for symbol in valid_symbols:
            try:
                indicators = self.calculate_all_indicators(symbol)
                if indicators:  # Only include if we have indicators
                    # 배치로 계산된 RS 점수로 업데이트
                    indicators['relative_strength'] = batch_rs_scores.get(symbol, 50)
                    stock_data[symbol] = indicators
            except Exception as e:
                self.logger.error(f"Error calculating indicators for {symbol}: {e}")
        
        # 실제로 지표가 계산된 종목들만 유지
        valid_symbols = list(stock_data.keys())
                
        if not stock_data:
            self.logger.error("No valid stock data found")
            return pd.DataFrame()
            
        self.logger.info(f"Processing {len(valid_symbols)} valid stocks")
        
        # Create decision matrix
        decision_matrix = pd.DataFrame(stock_data).T
        
        # Handle missing values
        decision_matrix = decision_matrix.fillna(decision_matrix.median())
        
        # Get criteria weights
        if custom_weights:
            weights = custom_weights
        else:
            weights = self.criteria_weights.get_strategy_weights(strategy)
            
        # Filter weights to only include available criteria
        available_criteria = decision_matrix.columns.tolist()
        filtered_weights = {k: v for k, v in weights.items() if k in available_criteria}
        
        if not filtered_weights:
            self.logger.error("No matching criteria found between weights and data")
            return pd.DataFrame()
            
        # Normalize weights
        total_weight = sum(filtered_weights.values())
        if total_weight > 0:
            filtered_weights = {k: v/total_weight for k, v in filtered_weights.items()}
        else:
            # Equal weights if no predefined weights
            equal_weight = 1.0 / len(available_criteria)
            filtered_weights = {k: equal_weight for k in available_criteria}
            
        # Get criteria types
        criteria_types = self.criteria_weights.get_criteria_types(available_criteria)
        
        # Filter decision matrix to only include weighted criteria
        weighted_criteria = list(filtered_weights.keys())
        decision_matrix = decision_matrix[weighted_criteria]
        
        # Prepare weights and types for MCDA calculator
        # Set decision matrix for calculator
        weights_dict = {col: filtered_weights[col] for col in decision_matrix.columns}
        types_dict = {col: criteria_types[col] for col in decision_matrix.columns}

        try:
            self.mcda_calculator.set_decision_matrix(
                decision_matrix,
                weights_dict,
                types_dict
            )

            mcda_results = self.mcda_calculator.calculate_all_methods([method])
            if method == MCDAMethod.TOPSIS:
                scores = mcda_results['topsis_score'].values
            elif method == MCDAMethod.VIKOR:
                scores = -mcda_results['Q'].values  # Lower Q is better
            elif method == MCDAMethod.WEIGHTED_SUM:
                scores = mcda_results['weighted_sum_score'].values
            elif method == MCDAMethod.COPRAS:
                scores = mcda_results['copras_score'].values
            else:
                scores = mcda_results.iloc[:, 0].values
            
            # Create results DataFrame
            results = pd.DataFrame({
                'symbol': valid_symbols,
                'score': scores,
                'rank': range(1, len(scores) + 1)
            })
            
            # Add some key indicators to results
            for symbol in valid_symbols:
                idx = results[results['symbol'] == symbol].index[0]
                indicators = stock_data[symbol]
                
                # Add key metrics for reference
                results.loc[idx, 'price_momentum_20d'] = indicators.get('price_momentum_20d', 0)
                results.loc[idx, 'rsi_14'] = indicators.get('rsi_14', 50)
                results.loc[idx, 'relative_strength'] = indicators.get('relative_strength', 0)
                
            # Sort by score (descending)
            results = results.sort_values('score', ascending=False).reset_index(drop=True)
            results['rank'] = range(1, len(results) + 1)
            
            self.logger.info(f"Successfully ranked {len(results)} stocks")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error calculating MCDA scores: {e}")
            return pd.DataFrame()
            
    def get_top_stocks(self, 
                      n: int = 10,
                      strategy: InvestmentStrategy = InvestmentStrategy.BALANCED,
                      method: MCDAMethod = MCDAMethod.TOPSIS) -> pd.DataFrame:
        """Get top N ranked stocks.
        
        Args:
            n: Number of top stocks to return
            strategy: Investment strategy
            method: MCDA method
            
        Returns:
            DataFrame with top N stocks
        """
        rankings = self.rank_stocks(strategy=strategy, method=method)
        return rankings.head(n)
        
    def compare_strategies(self, 
                          symbols: Optional[List[str]] = None,
                          strategies: Optional[List[InvestmentStrategy]] = None,
                          method: MCDAMethod = MCDAMethod.TOPSIS) -> Dict[str, pd.DataFrame]:
        """Compare rankings across different investment strategies.
        
        Args:
            symbols: List of symbols to compare
            strategies: List of strategies to compare
            method: MCDA method to use
            
        Returns:
            Dictionary mapping strategy names to ranking DataFrames
        """
        if strategies is None:
            strategies = [InvestmentStrategy.GROWTH, InvestmentStrategy.VALUE, 
                         InvestmentStrategy.MOMENTUM, InvestmentStrategy.QUALITY]
            
        results = {}
        
        for strategy in strategies:
            self.logger.info(f"Ranking stocks for {strategy.value} strategy")
            rankings = self.rank_stocks(symbols=symbols, strategy=strategy, method=method)
            results[strategy.value] = rankings
            
        return results
        
    def export_rankings(self, 
                       rankings: pd.DataFrame, 
                       filename: str,
                       include_details: bool = True) -> None:
        """Export rankings to CSV file.
        
        Args:
            rankings: Rankings DataFrame
            filename: Output filename
            include_details: Whether to include detailed indicator values
        """
        try:
            output_path = Path(filename)
            
            if include_details:
                # Add detailed indicators for each stock
                detailed_rankings = rankings.copy()
                
                for idx, row in rankings.iterrows():
                    symbol = row['symbol']
                    indicators = self.calculate_all_indicators(symbol)
                    
                    for indicator, value in indicators.items():
                        if indicator not in detailed_rankings.columns:
                            detailed_rankings[indicator] = np.nan
                        detailed_rankings.loc[idx, indicator] = value
                        
                detailed_rankings.to_csv(output_path, index=False)
            else:
                rankings.to_csv(output_path, index=False)
                
            self.logger.info(f"Rankings exported to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error exporting rankings: {e}")
            
    # Helper methods for technical indicators
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> float:
        """Calculate RSI indicator."""
        try:
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50.0
        except:
            return 50.0
            
    def _calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float, float]:
        """Calculate MACD indicator."""
        try:
            ema_fast = prices.ewm(span=fast).mean()
            ema_slow = prices.ewm(span=slow).mean()
            macd_line = ema_fast - ema_slow
            macd_signal = macd_line.ewm(span=signal).mean()
            macd_histogram = macd_line - macd_signal
            
            return (macd_line.iloc[-1] if not pd.isna(macd_line.iloc[-1]) else 0.0,
                   macd_signal.iloc[-1] if not pd.isna(macd_signal.iloc[-1]) else 0.0,
                   macd_histogram.iloc[-1] if not pd.isna(macd_histogram.iloc[-1]) else 0.0)
        except:
            return 0.0, 0.0, 0.0
            
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range."""
        try:
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = true_range.rolling(window=period).mean()
            
            return atr.iloc[-1] if not pd.isna(atr.iloc[-1]) else 0.0
        except:
            return 0.0
            
    def _calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std_dev: int = 2) -> Tuple[float, float, float]:
        """Calculate Bollinger Bands."""
        try:
            sma = prices.rolling(window=period).mean()
            std = prices.rolling(window=period).std()
            
            upper_band = sma + (std * std_dev)
            lower_band = sma - (std * std_dev)
            
            return (upper_band.iloc[-1] if not pd.isna(upper_band.iloc[-1]) else prices.iloc[-1],
                   lower_band.iloc[-1] if not pd.isna(lower_band.iloc[-1]) else prices.iloc[-1],
                   sma.iloc[-1] if not pd.isna(sma.iloc[-1]) else prices.iloc[-1])
        except:
            return prices.iloc[-1], prices.iloc[-1], prices.iloc[-1]