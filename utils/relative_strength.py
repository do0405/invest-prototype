import pandas as pd
from scipy.stats import percentileofscore

__all__ = ["calculate_rs_score_enhanced", "calculate_rs_score"]


def calculate_rs_score_enhanced(df: pd.DataFrame, price_col: str = "close", benchmark_symbol: str = "SPY") -> pd.Series:
    """Fred6724의 TradingView 기반 RS Rating 알고리즘을 구현한 고도화된 RS 점수 계산"""
    import numpy as np

    try:
        if not isinstance(df.index, pd.MultiIndex):
            date_col = next((col for col in ["date", "time"] if col in df.columns), None)
            symbol_col = next((col for col in ["symbol", "pair"] if col in df.columns), None)
            if date_col and symbol_col:
                if not pd.api.types.is_datetime64_dtype(df[date_col]):
                    df[date_col] = pd.to_datetime(df[date_col], utc=True)
                df = df.set_index([date_col, symbol_col])
            else:
                print("❌ 날짜/심볼 컬럼을 찾을 수 없습니다.")
                return pd.Series(dtype=float)

        if df.index.nlevels != 2:
            print(f"⚠️ 인덱스 레벨이 2가 아닙니다: {df.index.nlevels}")
            return pd.Series(dtype=float)

        try:
            benchmark_data = df.xs(benchmark_symbol, level=1)[price_col]
            if len(benchmark_data) < 252:
                print(f"⚠️ {benchmark_symbol} 벤치마크 데이터가 부족합니다 (필요: 252일, 현재: {len(benchmark_data)}일)")
                return pd.Series(dtype=float)
        except KeyError:
            print(f"⚠️ {benchmark_symbol} 벤치마크 데이터를 찾을 수 없습니다.")
            return pd.Series(dtype=float)

        rs_scores = {}
        for symbol in df.index.get_level_values(1).unique():
            if symbol == benchmark_symbol:
                continue
            try:
                symbol_data = df.xs(symbol, level=1)[price_col]
                if len(symbol_data) < 252:
                    continue
                close = symbol_data.tail(252).values
                bench = benchmark_data.tail(252).values
                p3 = (close[-1] - close[-63]) / close[-63] * 100
                p6 = (close[-1] - close[-126]) / close[-126] * 100
                p9 = (close[-1] - close[-189]) / close[-189] * 100
                p12 = (close[-1] - close[-252]) / close[-252] * 100
                b3 = (bench[-1] - bench[-63]) / bench[-63] * 100
                b6 = (bench[-1] - bench[-126]) / bench[-126] * 100
                b9 = (bench[-1] - bench[-189]) / bench[-189] * 100
                b12 = (bench[-1] - bench[-252]) / bench[-252] * 100
                stock_score = 0.4 * p3 + 0.2 * p6 + 0.2 * p9 + 0.2 * p12
                bench_score = 0.4 * b3 + 0.2 * b6 + 0.2 * b9 + 0.2 * b12
                if bench_score != 0:
                    rs_score = stock_score / bench_score * 100
                    rs_scores[str(symbol)] = rs_score
            except Exception:
                continue

        if not rs_scores:
            print("⚠️ RS Score를 계산할 수 있는 종목이 없습니다.")
            return pd.Series(dtype=float)

        rs_score_values = list(rs_scores.values())
        rs_ratings = {symbol: round(percentileofscore(rs_score_values, score, kind="rank"), 2)
                      for symbol, score in rs_scores.items()}
        return pd.Series(rs_ratings)
    except Exception as e:
        print(f"❌ 고도화된 RS Score 계산 오류: {e}")
        return pd.Series(dtype=float)


def calculate_rs_score(df: pd.DataFrame, price_col: str = "close", window: int = 126, use_enhanced: bool = True) -> pd.Series:
    """RS 점수 계산 함수. 기본적으로 고도화 버전을 사용한다."""
    if use_enhanced:
        return calculate_rs_score_enhanced(df, price_col)

    try:
        if not isinstance(df.index, pd.MultiIndex):
            date_col = next((col for col in ["date", "time"] if col in df.columns), None)
            symbol_col = next((col for col in ["symbol", "pair"] if col in df.columns), None)
            if date_col and symbol_col:
                if not pd.api.types.is_datetime64_dtype(df[date_col]):
                    df[date_col] = pd.to_datetime(df[date_col], utc=True)
                df = df.set_index([date_col, symbol_col])
            else:
                print("❌ 날짜/심볼 컬럼을 찾을 수 없습니다.")
                return pd.Series(dtype=float)

        if df.index.nlevels != 2:
            print(f"⚠️ 인덱스 레벨이 2가 아닙니다: {df.index.nlevels}")
            return pd.Series(dtype=float)

        grouped = df.groupby(level=1)[price_col]
        symbol_counts = grouped.count()
        valid_symbols = symbol_counts[symbol_counts >= window].index
        if len(valid_symbols) == 0:
            print("⚠️ 충분한 데이터가 있는 심볼이 없습니다.")
            return pd.Series(dtype=float)

        returns = {}
        for symbol in valid_symbols:
            try:
                symbol_data = df.xs(symbol, level=1)[price_col]
                if len(symbol_data) >= window:
                    recent_data = symbol_data.iloc[-window:]
                    first_price = recent_data.iloc[0]
                    last_price = recent_data.iloc[-1]
                    if first_price > 0:
                        returns[str(symbol)] = (last_price / first_price) - 1
            except Exception:
                continue

        returns_series = pd.Series(returns)
        if len(returns_series) > 0:
            ranks = percentileofscore(returns_series.values, returns_series.values)
            from scipy.stats import rankdata
            ranks = rankdata(returns_series.values)
            rs_scores = (ranks / len(ranks)) * 100
            return pd.Series(rs_scores, index=returns_series.index)
        return pd.Series(dtype=float)
    except Exception as e:
        print(f"❌ RS Score 계산 오류: {e}")
        return pd.Series(dtype=float)
