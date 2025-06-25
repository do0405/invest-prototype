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
    from utils.relative_strength import calculate_relative_strength
    from screeners.base_screener import BaseScreener
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
        
    def calculate_fundamental_indicators(self, symbol: str) -> Dict[str, float]:
        """Calculate fundamental indicators for a stock.
        
        Note: This is a placeholder implementation. In a real system,
        you would integrate with financial data providers.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary of fundamental indicator values
        """
        indicators = {}
        
        # Placeholder values - in real implementation, fetch from financial data
        # These would come from APIs like Alpha Vantage, Yahoo Finance, etc.
        try:
            # Mock fundamental data - replace with actual data fetching
            mock_fundamentals = {
                'pe_ratio': np.random.uniform(10, 30),
                'pb_ratio': np.random.uniform(0.5, 5),
                'roe': np.random.uniform(5, 25),
                'debt_to_equity': np.random.uniform(0, 2),
                'revenue_growth': np.random.uniform(-10, 30),
                'eps_growth': np.random.uniform(-20, 50),
                'profit_margin': np.random.uniform(0, 20),
                'current_ratio': np.random.uniform(0.5, 3),
                'dividend_yield': np.random.uniform(0, 8)
            }
            
            indicators.update(mock_fundamentals)
            
        except Exception as e:
            self.logger.error(f"Error calculating fundamental indicators for {symbol}: {e}")
            
        return indicators
        
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
            # Market cap (placeholder - would need shares outstanding)
            current_price = df['close'].iloc[-1]
            # Mock shares outstanding - replace with actual data
            shares_outstanding = np.random.uniform(50e6, 10e9)
            indicators['market_cap'] = current_price * shares_outstanding
            
            # Relative strength vs SPY
            try:
                spy_data = self.load_stock_data('SPY')
                if spy_data is not None and len(spy_data) >= len(df):
                    stock_returns = df['close'].pct_change().dropna()
                    spy_returns = spy_data['close'].tail(len(stock_returns)).pct_change().dropna()
                    
                    if len(stock_returns) == len(spy_returns) and len(stock_returns) > 0:
                        # Calculate relative strength
                        stock_cum_return = (1 + stock_returns).cumprod().iloc[-1] - 1
                        spy_cum_return = (1 + spy_returns).cumprod().iloc[-1] - 1
                        indicators['relative_strength'] = stock_cum_return - spy_cum_return
                    else:
                        indicators['relative_strength'] = 0
                else:
                    indicators['relative_strength'] = 0
            except:
                indicators['relative_strength'] = 0
                
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
        fundamental = self.calculate_fundamental_indicators(symbol)
        market = self.calculate_market_indicators(symbol, df)
        
        indicators.update(technical)
        indicators.update(fundamental)
        indicators.update(market)
        
        # Add some sentiment indicators (placeholder)
        indicators.update({
            'analyst_rating': np.random.uniform(1, 5),
            'insider_ownership': np.random.uniform(0, 30),
            'institutional_ownership': np.random.uniform(20, 90)
        })
        
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
        
        for symbol in symbols:
            try:
                df = self.load_stock_data(symbol)
                if df is not None and len(df) >= min_data_points:
                    indicators = self.calculate_all_indicators(symbol)
                    if indicators:  # Only include if we have indicators
                        stock_data[symbol] = indicators
                        valid_symbols.append(symbol)
                else:
                    self.logger.debug(f"Insufficient data for {symbol}")
            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                
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
                results.loc[idx, 'pe_ratio'] = indicators.get('pe_ratio', 0)
                results.loc[idx, 'roe'] = indicators.get('roe', 0)
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