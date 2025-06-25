import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATA_US_DIR
from ranking.ranking_system import StockRankingSystem
from ranking.criteria_weights import InvestmentStrategy
from ranking.mcda_calculator import MCDAMethod
from ranking.utils import load_all_screener_symbols, get_market_regime_strategy


def main():
    # 스크리너 결과 전체를 대상으로 종목 코드 로드
    symbols = load_all_screener_symbols()

    strategy = get_market_regime_strategy(InvestmentStrategy.BALANCED)

    ranking_system = StockRankingSystem(data_directory=DATA_US_DIR)
    rankings = ranking_system.rank_stocks(
        symbols=symbols,
        strategy=strategy,
        method=MCDAMethod.TOPSIS,
    )

    if rankings.empty:
        print('No rankings generated')
        return

    top10 = rankings[['rank', 'symbol', 'score']].head(10)
    print(top10.to_string(index=False))


if __name__ == '__main__':
    main()
