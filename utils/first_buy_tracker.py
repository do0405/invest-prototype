import os
import pandas as pd
from datetime import datetime

from .io_utils import ensure_dir

__all__ = ["update_first_buy_signals"]


def update_first_buy_signals(df: pd.DataFrame, results_dir: str,
                             symbol_col: str = "symbol",
                             date_col: str | None = "date",
                             run_date: str | None = None) -> None:
    """Update first buy signal record for a screener.

    Parameters
    ----------
    df : pd.DataFrame
        Latest screener results.
    results_dir : str
        Directory where ``first_buy_signals.json`` resides.
    symbol_col : str
        Column name containing the stock symbol. Defaults to ``symbol``.
    date_col : str | None
        Column name containing the signal date. If ``None`` or missing, ``run_date`` is used.
    run_date : str | None
        Date string ``YYYY-MM-DD`` to use when ``date_col`` is absent.
    """
    ensure_dir(results_dir)
    first_file = os.path.join(results_dir, "first_buy_signals.json")
    if os.path.exists(first_file):
        try:
            first_df = pd.read_json(first_file)
        except Exception:
            first_df = pd.DataFrame(columns=["symbol", "first_signal_date"])
    else:
        first_df = pd.DataFrame(columns=["symbol", "first_signal_date"])

    if symbol_col not in df.columns:
        if "ticker" in df.columns:
            symbol_col = "ticker"
        else:
            return

    if date_col and date_col in df.columns:
        # 날짜 형식을 명시적으로 지정하여 파싱 경고 방지
        try:
            date_series = pd.to_datetime(df[date_col], format="%Y-%m-%d", errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
        except ValueError:
            # 형식이 맞지 않는 경우 자동 파싱 사용
            date_series = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    else:
        if run_date is None:
            run_date = datetime.now().strftime("%Y-%m-%d")
        date_series = pd.Series([run_date] * len(df))

    new_df = pd.DataFrame({"symbol": df[symbol_col], "first_signal_date": date_series})
    new_df = new_df.drop_duplicates(subset="symbol")

    # Filter only symbols not already recorded
    to_add = new_df[~new_df["symbol"].isin(first_df["symbol"])]
    if not to_add.empty:
        combined = pd.concat([first_df, to_add], ignore_index=True)
        combined.to_json(first_file, orient="records", indent=2, force_ascii=False)
