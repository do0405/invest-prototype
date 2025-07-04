"""Criteria weights management for stock ranking system.

Provides predefined weight configurations for different investment strategies
and allows custom weight configuration.
"""

import pandas as pd
from typing import Dict, List, Optional
from enum import Enum
from .mcda_calculator import CriteriaType

class InvestmentStrategy(Enum):
    """Investment strategy enumeration."""
    GROWTH = "growth"
    VALUE = "value"
    MOMENTUM = "momentum"
    QUALITY = "quality"
    BALANCED = "balanced"
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    RISK_AVERSE = "risk_averse"
    AGGRESSIVE = "aggressive"

class CriteriaWeights:
    """Manages criteria weights for different investment strategies.
    
    This class provides predefined weight configurations for various investment
    strategies and allows for custom weight configuration.
    """
    
    def __init__(self):
        self._predefined_weights = self._initialize_predefined_weights()
        self._criteria_definitions = self._initialize_criteria_definitions()
        
    def _initialize_criteria_definitions(self) -> Dict[str, Dict]:
        """Initialize criteria definitions with types and descriptions.
        
        Returns:
            Dictionary mapping criteria names to their definitions
        """
        return {
            # Technical Indicators
            'rsi_14': {
                'type': CriteriaType.COST,  # Lower RSI can indicate oversold (buying opportunity)
                'description': '14-day Relative Strength Index',
                'optimal_range': (30, 70),
                'category': 'technical'
            },
            'macd_signal': {
                'type': CriteriaType.BENEFIT,  # Positive MACD signal is bullish
                'description': 'MACD Signal Line',
                'category': 'technical'
            },
            'volume_ratio': {
                'type': CriteriaType.BENEFIT,  # Higher volume indicates stronger moves
                'description': 'Volume ratio vs average',
                'category': 'technical'
            },
            'price_momentum_20d': {
                'type': CriteriaType.BENEFIT,  # Positive momentum is good
                'description': '20-day price momentum (%)',
                'category': 'technical'
            },
            'price_momentum_60d': {
                'type': CriteriaType.BENEFIT,
                'description': '60-day price momentum (%)',
                'category': 'technical'
            },
            'atr_normalized': {
                'type': CriteriaType.COST,  # Lower volatility can be preferred for some strategies
                'description': 'Normalized Average True Range',
                'category': 'technical'
            },
            'bollinger_position': {
                'type': CriteriaType.BENEFIT,  # Higher position in Bollinger bands
                'description': 'Position within Bollinger Bands',
                'category': 'technical'
            },
            
            # Fundamental Indicators
            'pe_ratio': {
                'type': CriteriaType.COST,  # Lower P/E is generally better for value
                'description': 'Price-to-Earnings Ratio',
                'optimal_range': (5, 25),
                'category': 'fundamental'
            },
            'pb_ratio': {
                'type': CriteriaType.COST,  # Lower P/B is generally better
                'description': 'Price-to-Book Ratio',
                'optimal_range': (0.5, 3),
                'category': 'fundamental'
            },
            'roe': {
                'type': CriteriaType.BENEFIT,  # Higher ROE is better
                'description': 'Return on Equity (%)',
                'optimal_range': (15, 50),
                'category': 'fundamental'
            },
            'debt_to_equity': {
                'type': CriteriaType.COST,  # Lower debt is generally better
                'description': 'Debt-to-Equity Ratio',
                'optimal_range': (0, 1),
                'category': 'fundamental'
            },
            'revenue_growth': {
                'type': CriteriaType.BENEFIT,  # Higher growth is better
                'description': 'Revenue Growth Rate (%)',
                'category': 'fundamental'
            },
            'eps_growth': {
                'type': CriteriaType.BENEFIT,  # Higher EPS growth is better
                'description': 'Earnings Per Share Growth (%)',
                'category': 'fundamental'
            },
            'profit_margin': {
                'type': CriteriaType.BENEFIT,  # Higher margins are better
                'description': 'Net Profit Margin (%)',
                'category': 'fundamental'
            },
            'current_ratio': {
                'type': CriteriaType.BENEFIT,  # Higher liquidity is generally better
                'description': 'Current Ratio',
                'optimal_range': (1.5, 3),
                'category': 'fundamental'
            },
            
            # Market Indicators
            'market_cap': {
                'type': CriteriaType.BENEFIT,  # Can be benefit or cost depending on strategy
                'description': 'Market Capitalization',
                'category': 'market'
            },
            'relative_strength': {
                'type': CriteriaType.BENEFIT,  # Higher relative strength vs market
                'description': 'Relative Strength vs Market',
                'category': 'market'
            },
            'beta': {
                'type': CriteriaType.COST,  # Lower beta for risk-averse, higher for aggressive
                'description': 'Beta (Market Sensitivity)',
                'category': 'market'
            },
            'dividend_yield': {
                'type': CriteriaType.BENEFIT,  # Higher dividend yield
                'description': 'Dividend Yield (%)',
                'category': 'market'
            },
            
            # Risk Indicators
            'volatility_20d': {
                'type': CriteriaType.COST,  # Lower volatility for risk-averse
                'description': '20-day Price Volatility',
                'category': 'risk'
            },
            'max_drawdown': {
                'type': CriteriaType.COST,  # Lower drawdown is better
                'description': 'Maximum Drawdown (%)',
                'category': 'risk'
            },
            'sharpe_ratio': {
                'type': CriteriaType.BENEFIT,  # Higher Sharpe ratio is better
                'description': 'Sharpe Ratio',
                'category': 'risk'
            },
            
            # Sentiment/Quality Indicators
            'analyst_rating': {
                'type': CriteriaType.BENEFIT,  # Higher rating is better
                'description': 'Average Analyst Rating',
                'category': 'sentiment'
            },
            'insider_ownership': {
                'type': CriteriaType.BENEFIT,  # Higher insider ownership can be positive
                'description': 'Insider Ownership (%)',
                'category': 'sentiment'
            },
            'institutional_ownership': {
                'type': CriteriaType.BENEFIT,  # Higher institutional ownership
                'description': 'Institutional Ownership (%)',
                'category': 'sentiment'
            }
        }
        
    def _initialize_predefined_weights(self) -> Dict[InvestmentStrategy, Dict[str, float]]:
        """Initialize predefined weight configurations for different strategies.
        
        Returns:
            Dictionary mapping investment strategies to criteria weights
        """
        return {
            InvestmentStrategy.GROWTH: {
                # Focus on growth metrics
                'revenue_growth': 0.20,
                'eps_growth': 0.20,
                'price_momentum_60d': 0.15,
                'roe': 0.10,
                'relative_strength': 0.10,
                'pe_ratio': 0.05,  # Less important for growth
                'profit_margin': 0.08,
                'price_momentum_20d': 0.07,
                'volume_ratio': 0.05
            },
            
            InvestmentStrategy.VALUE: {
                # Focus on valuation metrics
                'pe_ratio': 0.25,
                'pb_ratio': 0.20,
                'dividend_yield': 0.15,
                'debt_to_equity': 0.10,
                'current_ratio': 0.10,
                'roe': 0.08,
                'profit_margin': 0.07,
                'revenue_growth': 0.05
            },
            
            InvestmentStrategy.MOMENTUM: {
                # Focus on price and volume momentum
                'price_momentum_20d': 0.25,
                'price_momentum_60d': 0.20,
                'relative_strength': 0.20,
                'volume_ratio': 0.15,
                'rsi_14': 0.08,
                'macd_signal': 0.07,
                'bollinger_position': 0.05
            },
            
            InvestmentStrategy.QUALITY: {
                # Focus on quality metrics
                'roe': 0.20,
                'profit_margin': 0.18,
                'debt_to_equity': 0.15,
                'current_ratio': 0.12,
                'revenue_growth': 0.10,
                'eps_growth': 0.10,
                'sharpe_ratio': 0.08,
                'institutional_ownership': 0.07
            },
            
            InvestmentStrategy.BALANCED: {
                # Balanced approach across all categories
                'roe': 0.12,
                'price_momentum_20d': 0.10,
                'pe_ratio': 0.10,
                'revenue_growth': 0.10,
                'relative_strength': 0.08,
                'debt_to_equity': 0.08,
                'profit_margin': 0.08,
                'volume_ratio': 0.07,
                'eps_growth': 0.07,
                'dividend_yield': 0.06,
                'current_ratio': 0.06,
                'volatility_20d': 0.04,
                'pb_ratio': 0.04
            },
            
            InvestmentStrategy.TECHNICAL: {
                # Focus on technical indicators
                'price_momentum_20d': 0.20,
                'price_momentum_60d': 0.18,
                'rsi_14': 0.15,
                'macd_signal': 0.12,
                'volume_ratio': 0.12,
                'bollinger_position': 0.10,
                'relative_strength': 0.08,
                'atr_normalized': 0.05
            },
            
            InvestmentStrategy.FUNDAMENTAL: {
                # Focus on fundamental analysis
                'roe': 0.18,
                'revenue_growth': 0.16,
                'eps_growth': 0.14,
                'pe_ratio': 0.12,
                'profit_margin': 0.10,
                'debt_to_equity': 0.10,
                'pb_ratio': 0.08,
                'current_ratio': 0.07,
                'dividend_yield': 0.05
            },
            
            InvestmentStrategy.RISK_AVERSE: {
                # Focus on low-risk, stable investments
                'volatility_20d': 0.20,  # Lower volatility preferred
                'beta': 0.15,  # Lower beta preferred
                'debt_to_equity': 0.15,
                'current_ratio': 0.12,
                'dividend_yield': 0.10,
                'sharpe_ratio': 0.10,
                'max_drawdown': 0.08,
                'profit_margin': 0.05,
                'institutional_ownership': 0.05
            },
            
            InvestmentStrategy.AGGRESSIVE: {
                # Focus on high-growth, high-risk investments
                'price_momentum_60d': 0.22,
                'revenue_growth': 0.20,
                'eps_growth': 0.18,
                'relative_strength': 0.15,
                'volume_ratio': 0.10,
                'beta': 0.08,  # Higher beta acceptable
                'price_momentum_20d': 0.07
            }
        }
        
    def get_strategy_weights(self, strategy: InvestmentStrategy) -> Dict[str, float]:
        """Get predefined weights for a specific investment strategy.
        
        Args:
            strategy: Investment strategy
            
        Returns:
            Dictionary of criteria weights
        """
        if strategy not in self._predefined_weights:
            raise ValueError(f"Strategy {strategy} not found in predefined weights")
            
        return self._predefined_weights[strategy].copy()
        
    def get_criteria_types(self, criteria_list: List[str]) -> Dict[str, CriteriaType]:
        """Get criteria types for a list of criteria.
        
        Args:
            criteria_list: List of criteria names
            
        Returns:
            Dictionary mapping criteria names to their types
        """
        criteria_types = {}
        for criterion in criteria_list:
            if criterion in self._criteria_definitions:
                criteria_types[criterion] = self._criteria_definitions[criterion]['type']
            else:
                # Default to benefit type for unknown criteria
                criteria_types[criterion] = CriteriaType.BENEFIT
                
        return criteria_types
        
    def create_custom_weights(self, 
                            criteria_weights: Dict[str, float],
                            normalize: bool = True) -> Dict[str, float]:
        """Create custom weights configuration.
        
        Args:
            criteria_weights: Dictionary of criteria weights
            normalize: Whether to normalize weights to sum to 1
            
        Returns:
            Dictionary of normalized criteria weights
        """
        weights = criteria_weights.copy()
        
        if normalize:
            total_weight = sum(weights.values())
            if total_weight > 0:
                weights = {k: v / total_weight for k, v in weights.items()}
            else:
                # Equal weights if all weights are zero
                equal_weight = 1.0 / len(weights)
                weights = {k: equal_weight for k in weights.keys()}
                
        return weights
        
    def combine_strategies(self, 
                          strategies: Dict[InvestmentStrategy, float],
                          normalize: bool = True) -> Dict[str, float]:
        """Combine multiple investment strategies with weights.
        
        Args:
            strategies: Dictionary mapping strategies to their weights
            normalize: Whether to normalize final weights
            
        Returns:
            Combined criteria weights
        """
        combined_weights = {}
        
        # Normalize strategy weights
        total_strategy_weight = sum(strategies.values())
        if total_strategy_weight == 0:
            raise ValueError("Total strategy weights cannot be zero")
            
        normalized_strategy_weights = {
            strategy: weight / total_strategy_weight 
            for strategy, weight in strategies.items()
        }
        
        # Combine weights from all strategies
        for strategy, strategy_weight in normalized_strategy_weights.items():
            strategy_criteria_weights = self.get_strategy_weights(strategy)
            
            for criterion, criterion_weight in strategy_criteria_weights.items():
                if criterion not in combined_weights:
                    combined_weights[criterion] = 0
                combined_weights[criterion] += criterion_weight * strategy_weight
                
        if normalize:
            combined_weights = self.create_custom_weights(combined_weights, normalize=True)
            
        return combined_weights
        
    def get_available_strategies(self) -> List[InvestmentStrategy]:
        """Get list of available investment strategies.
        
        Returns:
            List of available investment strategies
        """
        return list(self._predefined_weights.keys())
        
    def get_criteria_info(self, criterion: str) -> Optional[Dict]:
        """Get information about a specific criterion.
        
        Args:
            criterion: Criterion name
            
        Returns:
            Dictionary with criterion information or None if not found
        """
        return self._criteria_definitions.get(criterion)
        
    def get_criteria_by_category(self, category: str) -> List[str]:
        """Get list of criteria by category.
        
        Args:
            category: Category name (technical, fundamental, market, risk, sentiment)
            
        Returns:
            List of criteria names in the specified category
        """
        criteria = []
        for criterion, info in self._criteria_definitions.items():
            if info.get('category') == category:
                criteria.append(criterion)
        return criteria
        
    def validate_weights(self, weights: Dict[str, float], tolerance: float = 1e-6) -> bool:
        """Validate that weights sum to approximately 1.
        
        Args:
            weights: Dictionary of weights to validate
            tolerance: Tolerance for weight sum validation
            
        Returns:
            True if weights are valid, False otherwise
        """
        total_weight = sum(weights.values())
        return abs(total_weight - 1.0) <= tolerance
        
    def print_strategy_summary(self, strategy: InvestmentStrategy) -> None:
        """Print a summary of a strategy's weight configuration.
        
        Args:
            strategy: Investment strategy to summarize
        """
        weights = self.get_strategy_weights(strategy)
        
        print(f"\n=== {strategy.value.upper()} STRATEGY WEIGHTS ===")
        print(f"Total criteria: {len(weights)}")
        print(f"Weight sum: {sum(weights.values()):.6f}")
        print("\nTop 5 criteria:")
        
        sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
        for i, (criterion, weight) in enumerate(sorted_weights[:5]):
            info = self.get_criteria_info(criterion)
            category = info['category'] if info else 'unknown'
            print(f"  {i+1}. {criterion}: {weight:.3f} ({category})")
            
        print("\nBy category:")
        categories = {}
        for criterion, weight in weights.items():
            info = self.get_criteria_info(criterion)
            category = info['category'] if info else 'unknown'
            if category not in categories:
                categories[category] = 0
            categories[category] += weight
            
        for category, total_weight in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"  {category}: {total_weight:.3f} ({total_weight*100:.1f}%)")