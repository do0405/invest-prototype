from __future__ import annotations

import os
from datetime import datetime

import pandas as pd

from utils.io_utils import ensure_dir
from utils.market_data_contract import load_benchmark_data, load_local_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_benchmark_candidates,
    get_markminervini_with_rs_path,
    get_momentum_signals_results_dir,
    get_primary_benchmark_symbol,
    get_stock_metadata_path,
    is_index_symbol,
    market_key,
)
from utils.screener_utils import create_screener_summary, read_csv_flexible, save_screening_results, track_new_tickers


def _market_today(market: str) -> datetime.date:
    if market_key(market) == "us":
        from utils.calc_utils import get_us_market_today

        return get_us_market_today()
    return datetime.now().date()


class Stage2MomentumScreener:
    def __init__(self, *, market: str = "us", skip_data: bool = False):
        self.market = market_key(market)
        self.skip_data = skip_data
        self.today = _market_today(self.market)
        self.results_dir = get_momentum_signals_results_dir(self.market)
        ensure_market_dirs(self.market)
        ensure_dir(self.results_dir)

        self.rs_scores = self._load_rs_scores()
        self.sector_map = self._load_metadata()
        self.benchmark_symbol, self.benchmark_df = self._load_market_benchmark()

    def _load_rs_scores(self) -> dict[str, float]:
        rs_path = get_markminervini_with_rs_path(self.market)
        if not os.path.exists(rs_path):
            return {}

        rs_df = read_csv_flexible(rs_path, required_columns=["symbol", "rs_score"])
        if rs_df is None or rs_df.empty:
            return {}

        rs_df["symbol"] = rs_df["symbol"].astype(str).str.upper()
        rs_df["rs_score"] = pd.to_numeric(rs_df["rs_score"], errors="coerce").fillna(0.0)
        return dict(zip(rs_df["symbol"], rs_df["rs_score"]))

    def _load_metadata(self) -> dict[str, str]:
        metadata_path = get_stock_metadata_path(self.market)
        if not os.path.exists(metadata_path):
            return {}

        metadata_df = read_csv_flexible(metadata_path, required_columns=["symbol"])
        if metadata_df is None or metadata_df.empty:
            return {}

        if "sector" not in metadata_df.columns:
            return {}

        metadata_df["symbol"] = metadata_df["symbol"].astype(str).str.upper()
        return metadata_df.set_index("symbol")["sector"].fillna("Unknown").astype(str).to_dict()

    def _load_market_benchmark(self) -> tuple[str, pd.DataFrame]:
        benchmark_symbol, benchmark_df = load_benchmark_data(
            self.market,
            get_benchmark_candidates(self.market),
            allow_yfinance_fallback=True,
        )
        resolved_symbol = benchmark_symbol or get_primary_benchmark_symbol(self.market)
        return resolved_symbol, benchmark_df

    def _check_market_environment(self) -> bool:
        if self.benchmark_df is None or self.benchmark_df.empty or len(self.benchmark_df) < 150:
            return True

        benchmark_df = self.benchmark_df.copy().sort_values("date").reset_index(drop=True)
        benchmark_df["sma_150"] = benchmark_df["close"].rolling(window=150).mean()
        latest = benchmark_df.iloc[-1]
        if pd.isna(latest["sma_150"]):
            return True
        prev_ma = benchmark_df["sma_150"].iloc[-10] if len(benchmark_df) >= 160 else benchmark_df["sma_150"].iloc[-2]
        if pd.isna(prev_ma):
            return True
        return bool(latest["close"] > latest["sma_150"] and latest["sma_150"] > prev_ma)

    @staticmethod
    def _calculate_weekly_data(frame: pd.DataFrame) -> pd.DataFrame:
        weekly = frame.copy()
        weekly["date"] = pd.to_datetime(weekly["date"], errors="coerce")
        weekly = weekly.dropna(subset=["date"]).sort_values("date").set_index("date")
        aggregated = (
            weekly.resample("W")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna()
            .reset_index()
        )
        return aggregated

    @staticmethod
    def _calculate_moving_averages(frame: pd.DataFrame) -> pd.DataFrame:
        df = frame.copy()
        df["sma_10w"] = df["close"].rolling(window=10).mean()
        df["sma_20w"] = df["close"].rolling(window=20).mean()
        df["sma_30w"] = df["close"].rolling(window=30).mean()
        df["sma_40w"] = df["close"].rolling(window=40).mean()
        return df

    @staticmethod
    def _calculate_volume_indicators(frame: pd.DataFrame) -> pd.DataFrame:
        df = frame.copy()
        df["avg_volume_20w"] = df["volume"].rolling(window=20).mean()
        df["volume_ratio"] = df["volume"] / df["avg_volume_20w"].replace({0: pd.NA})
        df["obv"] = 0.0
        for index in range(1, len(df)):
            previous = df.iloc[index - 1]["obv"]
            if df.iloc[index]["close"] > df.iloc[index - 1]["close"]:
                df.iloc[index, df.columns.get_loc("obv")] = previous + df.iloc[index]["volume"]
            elif df.iloc[index]["close"] < df.iloc[index - 1]["close"]:
                df.iloc[index, df.columns.get_loc("obv")] = previous - df.iloc[index]["volume"]
            else:
                df.iloc[index, df.columns.get_loc("obv")] = previous
        df["obv_rising"] = df["obv"] > df["obv"].shift(1)
        return df

    def _calculate_relative_strength(self, frame: pd.DataFrame) -> pd.DataFrame:
        if self.benchmark_df is None or self.benchmark_df.empty:
            result = frame.copy()
            result["relative_strength"] = 1.0
            return result

        benchmark_weekly = self._calculate_weekly_data(self.benchmark_df)
        if benchmark_weekly.empty:
            result = frame.copy()
            result["relative_strength"] = 1.0
            return result

        merged = pd.merge_asof(
            frame.sort_values("date"),
            benchmark_weekly[["date", "close"]].rename(columns={"close": "benchmark_close"}).sort_values("date"),
            on="date",
            direction="backward",
        )
        stock_return = merged["close"].pct_change()
        benchmark_return = merged["benchmark_close"].pct_change()
        merged["relative_strength"] = ((1 + stock_return) / (1 + benchmark_return)).fillna(1.0)
        return merged

    @staticmethod
    def _check_minimal_resistance(frame: pd.DataFrame) -> bool:
        if len(frame) < 52:
            return True
        recent_price = frame.iloc[-1]["close"]
        year_high = frame.iloc[-52:]["high"].max()
        return bool(recent_price >= year_high * 0.95)

    def _detect_breakout(self, frame: pd.DataFrame) -> dict[str, object]:
        if len(frame) < 40:
            return {"detected": False}

        recent = frame.iloc[-6:].copy()
        for _, week in recent.iterrows():
            if pd.isna(week["sma_30w"]) or pd.isna(week["volume_ratio"]):
                continue
            conditions = [
                week["close"] > week["sma_30w"],
                week["volume_ratio"] >= 2.0,
                week["relative_strength"] >= 1.0,
                bool(week.get("obv_rising", True)),
            ]
            if all(conditions):
                resistance_frame = frame.iloc[-10:-1] if len(frame) >= 10 else frame.iloc[:-1]
                resistance_level = resistance_frame["high"].max() if not resistance_frame.empty else week["high"]
                return {
                    "detected": True,
                    "date": week["date"].strftime("%Y-%m-%d") if hasattr(week["date"], "strftime") else str(week["date"]),
                    "close_price": float(week["close"]),
                    "sma_30w": float(week["sma_30w"]),
                    "volume_ratio": float(week["volume_ratio"]),
                    "relative_strength": float(week["relative_strength"]),
                    "resistance_level": float(resistance_level),
                    "entry_type": "A",
                    "obv_rising": bool(week.get("obv_rising", True)),
                }
        return {"detected": False}

    def screen(self) -> pd.DataFrame:
        market_favorable = self._check_market_environment()
        rows: list[dict[str, object]] = []
        symbols = set(self.rs_scores) if self.rs_scores else set()
        if not symbols:
            from utils.market_runtime import get_market_data_dir

            data_dir = get_market_data_dir(self.market)
            if os.path.isdir(data_dir):
                symbols = {os.path.splitext(name)[0].upper() for name in os.listdir(data_dir) if name.endswith(".csv")}

        for symbol in sorted(symbols):
            if is_index_symbol(self.market, symbol):
                continue

            rs_score = float(self.rs_scores.get(symbol, 0))
            if rs_score < 70:
                continue

            frame = load_local_ohlcv_frame(self.market, symbol)
            if frame.empty or len(frame) < 150:
                continue

            weekly = self._calculate_weekly_data(frame)
            if len(weekly) < 30:
                continue
            weekly = self._calculate_moving_averages(weekly)
            weekly = self._calculate_volume_indicators(weekly)
            weekly = self._calculate_relative_strength(weekly)

            if not self._check_minimal_resistance(weekly):
                continue

            breakout = self._detect_breakout(weekly)
            if not breakout.get("detected"):
                continue

            rows.append(
                {
                    "symbol": symbol,
                    "market": self.market,
                    "sector": self.sector_map.get(symbol, "Unknown"),
                    "rs_score": rs_score,
                    "close_price": breakout["close_price"],
                    "sma_30w": breakout["sma_30w"],
                    "volume_ratio": breakout["volume_ratio"],
                    "relative_strength": breakout["relative_strength"],
                    "entry_type": breakout["entry_type"],
                    "obv_rising": breakout["obv_rising"],
                    "resistance_level": breakout["resistance_level"],
                    "market_favorable": market_favorable,
                    "benchmark_symbol": self.benchmark_symbol,
                    "date": breakout["date"],
                    "screening_date": self.today.strftime("%Y-%m-%d"),
                }
            )

        results_df = pd.DataFrame(rows)
        if not results_df.empty:
            results_df = results_df.sort_values(["rs_score", "volume_ratio"], ascending=[False, False]).reset_index(drop=True)
        return results_df


def run_momentum_signals_screening(*, skip_data: bool = False, market: str = "us") -> pd.DataFrame:
    screener = Stage2MomentumScreener(market=market, skip_data=skip_data)
    results_df = screener.screen()

    results_dir = get_momentum_signals_results_dir(market)
    if not results_df.empty:
        results_list = results_df.to_dict("records")
        results_paths = save_screening_results(
            results=results_list,
            output_dir=results_dir,
            filename_prefix="momentum_signals",
            include_timestamp=True,
            incremental_update=True,
        )
        tracker_file = os.path.join(results_dir, "new_momentum_tickers.csv")
        new_tickers = track_new_tickers(
            current_results=results_list,
            tracker_file=tracker_file,
            symbol_key="symbol",
            retention_days=14,
        )
        create_screener_summary(
            screener_name=f"Momentum Signals ({market_key(market).upper()})",
            total_candidates=len(results_list),
            new_tickers=len(new_tickers),
            results_paths=results_paths,
        )

    return results_df


if __name__ == "__main__":
    run_momentum_signals_screening(market="us")
