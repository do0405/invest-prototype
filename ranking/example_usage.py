"""Example usage of the stock ranking system.

Demonstrates how to use the MCDA-based stock ranking system
with different investment strategies and methods.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ranking.ranking_system import StockRankingSystem
from ranking.criteria_weights import InvestmentStrategy
from ranking.mcda_calculator import MCDAMethod

def main():
    """Main example function demonstrating the ranking system."""
    
    print("=== Stock Ranking System Example ===")
    print()
    
    # Initialize the ranking system
    print("Initializing ranking system...")
    ranking_system = StockRankingSystem()
    
    # Get available symbols
    print("Getting available symbols...")
    symbols = ranking_system.get_available_symbols()
    print(f"Found {len(symbols)} available symbols")
    
    if len(symbols) == 0:
        print("No stock data found. Please ensure data files are in the correct directory.")
        return
    
    # Limit to first 20 symbols for demonstration
    test_symbols = symbols[:20] if len(symbols) > 20 else symbols
    print(f"Using {len(test_symbols)} symbols for demonstration: {test_symbols[:10]}...")
    print()
    
    # Example 1: Basic ranking with balanced strategy
    print("=== Example 1: Balanced Strategy Ranking ===")
    try:
        balanced_rankings = ranking_system.rank_stocks(
            symbols=test_symbols,
            strategy=InvestmentStrategy.BALANCED,
            method=MCDAMethod.TOPSIS
        )
        
        if not balanced_rankings.empty:
            print("Top 10 stocks (Balanced Strategy):")
            print(balanced_rankings[['rank', 'symbol', 'score', 'price_momentum_20d', 'rsi_14']].head(10))
        else:
            print("No rankings generated")
    except Exception as e:
        print(f"Error in balanced ranking: {e}")
    
    print()
    
    # Example 2: Growth strategy ranking
    print("=== Example 2: Growth Strategy Ranking ===")
    try:
        growth_rankings = ranking_system.rank_stocks(
            symbols=test_symbols,
            strategy=InvestmentStrategy.GROWTH,
            method=MCDAMethod.TOPSIS
        )
        
        if not growth_rankings.empty:
            print("Top 10 stocks (Growth Strategy):")
            print(growth_rankings[['rank', 'symbol', 'score', 'price_momentum_20d', 'roe']].head(10))
        else:
            print("No rankings generated")
    except Exception as e:
        print(f"Error in growth ranking: {e}")
    
    print()
    
    # Example 3: Value strategy ranking
    print("=== Example 3: Value Strategy Ranking ===")
    try:
        value_rankings = ranking_system.rank_stocks(
            symbols=test_symbols,
            strategy=InvestmentStrategy.VALUE,
            method=MCDAMethod.TOPSIS
        )
        
        if not value_rankings.empty:
            print("Top 10 stocks (Value Strategy):")
            print(value_rankings[['rank', 'symbol', 'score', 'pe_ratio', 'roe']].head(10))
        else:
            print("No rankings generated")
    except Exception as e:
        print(f"Error in value ranking: {e}")
    
    print()
    
    # Example 4: Compare different MCDA methods
    print("=== Example 4: Comparing MCDA Methods ===")
    try:
        methods_comparison = {}
        methods = [MCDAMethod.TOPSIS, MCDAMethod.VIKOR, MCDAMethod.WSM]
        
        for method in methods:
            try:
                rankings = ranking_system.rank_stocks(
                    symbols=test_symbols[:10],  # Use fewer symbols for comparison
                    strategy=InvestmentStrategy.BALANCED,
                    method=method
                )
                if not rankings.empty:
                    methods_comparison[method.value] = rankings[['symbol', 'score']].head(5)
            except Exception as e:
                print(f"Error with {method.value}: {e}")
        
        if methods_comparison:
            print("Top 5 stocks by different MCDA methods:")
            for method_name, rankings in methods_comparison.items():
                print(f"\n{method_name.upper()}:")
                for idx, row in rankings.iterrows():
                    print(f"  {idx+1}. {row['symbol']}: {row['score']:.4f}")
        else:
            print("No method comparisons available")
    except Exception as e:
        print(f"Error in methods comparison: {e}")
    
    print()
    
    # Example 5: Custom weights
    print("=== Example 5: Custom Weights Example ===")
    try:
        # Define custom weights focusing on momentum and technical indicators
        custom_weights = {
            'price_momentum_20d': 0.25,
            'price_momentum_60d': 0.20,
            'rsi_14': 0.15,
            'volume_ratio': 0.15,
            'relative_strength': 0.10,
            'roe': 0.10,
            'volatility_20d': 0.05
        }
        
        custom_rankings = ranking_system.rank_stocks(
            symbols=test_symbols,
            custom_weights=custom_weights,
            method=MCDAMethod.TOPSIS
        )
        
        if not custom_rankings.empty:
            print("Top 10 stocks (Custom Momentum-focused Weights):")
            print(custom_rankings[['rank', 'symbol', 'score', 'price_momentum_20d', 'rsi_14']].head(10))
        else:
            print("No rankings generated with custom weights")
    except Exception as e:
        print(f"Error with custom weights: {e}")
    
    print()
    
    # Example 6: Strategy comparison
    print("=== Example 6: Strategy Comparison ===")
    try:
        strategies_to_compare = [
            InvestmentStrategy.GROWTH,
            InvestmentStrategy.VALUE,
            InvestmentStrategy.MOMENTUM
        ]
        
        strategy_comparison = ranking_system.compare_strategies(
            symbols=test_symbols[:15],
            strategies=strategies_to_compare,
            method=MCDAMethod.TOPSIS
        )
        
        if strategy_comparison:
            print("Top 5 stocks by different strategies:")
            for strategy_name, rankings in strategy_comparison.items():
                if not rankings.empty:
                    print(f"\n{strategy_name.upper()}:")
                    top_5 = rankings.head(5)
                    for idx, row in top_5.iterrows():
                        print(f"  {row['rank']}. {row['symbol']}: {row['score']:.4f}")
        else:
            print("No strategy comparisons available")
    except Exception as e:
        print(f"Error in strategy comparison: {e}")
    
    print()
    
    # Example 7: Export rankings
    print("=== Example 7: Export Rankings ===")
    try:
        if 'balanced_rankings' in locals() and not balanced_rankings.empty:
            output_file = "ranking_results.csv"
            ranking_system.export_rankings(
                balanced_rankings,
                output_file,
                include_details=True
            )
            print(f"Rankings exported to {output_file}")
        else:
            print("No rankings available to export")
    except Exception as e:
        print(f"Error exporting rankings: {e}")
    
    print()
    
    # Example 8: Show strategy weights
    print("=== Example 8: Strategy Weights Information ===")
    try:
        criteria_weights = ranking_system.criteria_weights
        
        # Show weights for different strategies
        strategies_to_show = [InvestmentStrategy.GROWTH, InvestmentStrategy.VALUE]
        
        for strategy in strategies_to_show:
            print(f"\n{strategy.value.upper()} Strategy Weights:")
            weights = criteria_weights.get_strategy_weights(strategy)
            
            # Sort by weight (descending) and show top 8
            sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
            for criterion, weight in sorted_weights[:8]:
                print(f"  {criterion}: {weight:.3f} ({weight*100:.1f}%)")
    except Exception as e:
        print(f"Error showing strategy weights: {e}")
    
    print()
    print("=== Example Complete ===")

def test_individual_stock():
    """Test indicator calculation for individual stock."""
    print("\n=== Testing Individual Stock Analysis ===")
    
    ranking_system = StockRankingSystem()
    symbols = ranking_system.get_available_symbols()
    
    if not symbols:
        print("No symbols available for testing")
        return
    
    # Test with first available symbol
    test_symbol = symbols[0]
    print(f"Testing with symbol: {test_symbol}")
    
    try:
        # Load data
        df = ranking_system.load_stock_data(test_symbol)
        if df is not None:
            print(f"Loaded {len(df)} days of data")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"Price range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")
            
            # Calculate indicators
            indicators = ranking_system.calculate_all_indicators(test_symbol)
            
            print(f"\nCalculated {len(indicators)} indicators:")
            for category in ['technical', 'fundamental', 'market', 'risk']:
                category_indicators = {
                    k: v for k, v in indicators.items() 
                    if ranking_system.criteria_weights.get_criteria_info(k) and 
                    ranking_system.criteria_weights.get_criteria_info(k).get('category') == category
                }
                
                if category_indicators:
                    print(f"\n{category.upper()} Indicators:")
                    for name, value in list(category_indicators.items())[:5]:  # Show first 5
                        print(f"  {name}: {value:.4f}")
        else:
            print(f"Could not load data for {test_symbol}")
            
    except Exception as e:
        print(f"Error testing individual stock: {e}")

if __name__ == "__main__":
    try:
        main()
        test_individual_stock()
    except Exception as e:
        print(f"Error running examples: {e}")
        import traceback
        traceback.print_exc()