from config import DATA_US_DIR
from ranking.ranking_system import StockRankingSystem
from ranking.criteria_weights import InvestmentStrategy
from ranking.mcda_calculator import MCDAMethod
from ranking.utils import load_all_screener_symbols, get_market_regime_strategy


def main():
    # 모든 스크리너 결과를 활용해 종목 코드를 로드
    symbols = load_all_screener_symbols()

    strategy = get_market_regime_strategy(InvestmentStrategy.BALANCED)

    ranking_system = StockRankingSystem(data_directory=DATA_US_DIR)
    rankings = ranking_system.rank_stocks(
        symbols=symbols,
        strategy=strategy,
        method=MCDAMethod.TOPSIS,
    )

    top10 = rankings[['rank', 'symbol', 'score']].head(10)
    print(top10.to_string(index=False))


if __name__ == '__main__':
    main()
