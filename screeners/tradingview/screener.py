from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from utils.market_data_contract import load_local_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_market_data_dir,
    get_stock_metadata_path,
    get_tradingview_results_dir,
    is_index_symbol,
    market_key,
)
from utils.progress_runtime import is_progress_tick, progress_interval


@dataclass(frozen=True)
class PresetDefinition:
    key: str
    label: str
    market: str
    sorter: str
    predicate: Callable[[dict[str, float | str | bool | None]], bool]


def _load_market_metadata_frame(market: str) -> pd.DataFrame:
    metadata_path = get_stock_metadata_path(market)
    if not os.path.exists(metadata_path):
        return pd.DataFrame()
    frame = pd.read_csv(metadata_path)
    if frame.empty or "symbol" not in frame.columns:
        return pd.DataFrame()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    for column in ("market_cap", "shares_outstanding"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("sector", "industry"):
        if column in frame.columns:
            frame[column] = frame[column].fillna("").astype(str)
    return frame


def _load_market_cap_map(metadata_frame: pd.DataFrame) -> dict[str, float]:
    if metadata_frame.empty or "market_cap" not in metadata_frame.columns:
        return {}
    return metadata_frame.set_index("symbol")["market_cap"].dropna().to_dict()


def _load_text_metadata_map(metadata_frame: pd.DataFrame, column: str) -> dict[str, str]:
    if metadata_frame.empty or column not in metadata_frame.columns:
        return {}
    return metadata_frame.set_index("symbol")[column].fillna("").astype(str).to_dict()


def _load_numeric_metadata_map(metadata_frame: pd.DataFrame, column: str) -> dict[str, float]:
    if metadata_frame.empty or column not in metadata_frame.columns:
        return {}
    return metadata_frame.set_index("symbol")[column].dropna().to_dict()


def _ema(series: pd.Series, length: int) -> float | None:
    if len(series) < length:
        return None
    return float(series.ewm(span=length, adjust=False).mean().iloc[-1])


def _atr_percent(frame: pd.DataFrame, length: int = 14) -> float | None:
    if len(frame) < length + 1:
        return None
    df = frame.copy()
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(window=length).mean().iloc[-1]
    close = df["close"].iloc[-1]
    if pd.isna(atr) or close == 0:
        return None
    return float((atr / close) * 100)


def _adr_percent(frame: pd.DataFrame, length: int = 20) -> float | None:
    if len(frame) < length:
        return None
    adr = ((frame["high"] - frame["low"]) / frame["close"].replace({0: pd.NA}) * 100).rolling(window=length).mean().iloc[-1]
    return None if pd.isna(adr) else float(adr)


def _pct_change(frame: pd.DataFrame, periods: int) -> float | None:
    if len(frame) <= periods:
        return None
    start = frame["close"].iloc[-periods - 1]
    end = frame["close"].iloc[-1]
    if start == 0:
        return None
    return float((end / start - 1.0) * 100.0)


def _average_traded_value(frame: pd.DataFrame, window: int) -> float | None:
    if len(frame) < window:
        return None
    typical_price = (frame["high"] + frame["low"] + frame["close"]) / 3.0
    traded_value = typical_price * frame["volume"]
    value = traded_value.rolling(window=window).mean().iloc[-1]
    return None if pd.isna(value) else float(value)


def _relative_volume(frame: pd.DataFrame, window: int = 20) -> float | None:
    if len(frame) < window:
        return None
    average_volume = frame["volume"].rolling(window=window).mean().iloc[-1]
    if pd.isna(average_volume) or average_volume == 0:
        return None
    return float(frame["volume"].iloc[-1] / average_volume)


def _breakout_strength(frame: pd.DataFrame) -> float | None:
    if len(frame) < 20:
        return None
    recent_high = frame["high"].iloc[-20:].max()
    current_close = frame["close"].iloc[-1]
    if recent_high == 0:
        return None
    distance = (current_close / recent_high - 1.0) * 100.0
    rel_vol = _relative_volume(frame) or 0.0
    atr_pct = _atr_percent(frame) or 0.0
    return float(distance + (rel_vol * 2.0) + atr_pct)


def _distance_from_52w_low(frame: pd.DataFrame) -> float | None:
    if len(frame) < 100:
        return None
    low_52w = frame["low"].iloc[-252:].min() if len(frame) >= 252 else frame["low"].min()
    if low_52w == 0:
        return None
    return float((frame["close"].iloc[-1] / low_52w - 1.0) * 100.0)


def _build_metrics(
    symbol: str,
    market: str,
    frame: pd.DataFrame,
    market_cap_map: dict[str, float],
    shares_outstanding_map: dict[str, float],
    sector_map: dict[str, str],
    industry_map: dict[str, str],
) -> dict[str, float | str | bool | None]:
    df = frame.copy().sort_values("date").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "volume"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["close", "high", "low", "volume"])

    latest_close = float(df["close"].iloc[-1])
    metrics: dict[str, float | str | bool | None] = {
        "symbol": symbol,
        "market": market,
        "close": latest_close,
        "market_cap": market_cap_map.get(symbol),
        "shares_outstanding": shares_outstanding_map.get(symbol),
        "sector": sector_map.get(symbol, ""),
        "industry": industry_map.get(symbol, ""),
        "ema5": _ema(df["close"], 5),
        "ema10": _ema(df["close"], 10),
        "ema20": _ema(df["close"], 20),
        "ema50": _ema(df["close"], 50),
        "ema100": _ema(df["close"], 100),
        "avg_volume_60d": float(df["volume"].rolling(window=60).mean().iloc[-1]) if len(df) >= 60 else None,
        "avg_traded_value_60d": _average_traded_value(df, 60),
        "avg_traded_value_30d": _average_traded_value(df, 30),
        "avg_traded_value_10d": _average_traded_value(df, 10),
        "change_1d_pct": _pct_change(df, 1),
        "perf_1m_pct": _pct_change(df, 20),
        "perf_3m_pct": _pct_change(df, 63),
        "perf_6m_pct": _pct_change(df, 126),
        "atr_pct": _atr_percent(df),
        "adr_pct": _adr_percent(df),
        "relative_volume": _relative_volume(df),
        "distance_from_52w_low_pct": _distance_from_52w_low(df),
        "breakout_strength": _breakout_strength(df),
        "date": str(df["date"].iloc[-1]),
    }
    return metrics


def _gte(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


def _lte(left: float | None, right: float | None) -> bool:
    return left is not None and right is not None and left <= right


def _between(value: float | None, low: float | None = None, high: float | None = None) -> bool:
    if value is None:
        return False
    if low is not None and value < low:
        return False
    if high is not None and value > high:
        return False
    return True


def _preset_definitions() -> list[PresetDefinition]:
    return [
        PresetDefinition(
            key="us_breakout_rvol",
            label="US Breakout RVOL",
            market="us",
            sorter="relative_volume",
            predicate=lambda m: all(
                [
                    _gte(m["close"], 1),
                    _gte(m["avg_volume_60d"], 100000),
                    _gte(m["avg_traded_value_30d"], 10_000_000),
                    _gte(m["atr_pct"], 4),
                    _gte(m["adr_pct"], 4),
                    _gte(m["relative_volume"], 1.5),
                    _gte(m["change_1d_pct"], 3.9),
                    _gte(m["perf_6m_pct"], 0),
                    _gte(m["distance_from_52w_low_pct"], 30),
                    _gte(m["ema10"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                    _gte(m["ema20"], m["ema50"] or float("inf")) if m["ema50"] is not None else False,
                    _gte(m["ema50"], m["ema100"] or float("inf")) if m["ema100"] is not None else False,
                    _gte(m["close"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                ]
            ),
        ),
        PresetDefinition(
            key="us_breakout_10m",
            label="US Breakout 10M",
            market="us",
            sorter="relative_volume",
            predicate=lambda m: all(
                [
                    _gte(m["close"], 1),
                    _between(m["market_cap"], 25_000_000, 150_000_000_000) if m["market_cap"] is not None else True,
                    _gte(m["avg_volume_60d"], 100000),
                    _gte(m["avg_traded_value_60d"], 10_000_000),
                    _gte(m["avg_traded_value_30d"], 10_000_000),
                    _gte(m["avg_traded_value_10d"], 10_000_000),
                    _gte(m["atr_pct"], 3),
                    _gte(m["adr_pct"], 3),
                    _gte(m["relative_volume"], 2),
                    _gte(m["change_1d_pct"], 3.9),
                    _gte(m["perf_3m_pct"], 0),
                    _gte(m["close"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                    _gte(m["ema20"], m["ema50"] or float("inf")) if m["ema50"] is not None else False,
                    _gte(m["ema50"], m["ema100"] or float("inf")) if m["ema100"] is not None else False,
                ]
            ),
        ),
        PresetDefinition(
            key="us_breakout_strength",
            label="US Breakout Strength",
            market="us",
            sorter="breakout_strength",
            predicate=lambda m: all(
                [
                    _gte(m["close"], 1),
                    _gte(m["avg_volume_60d"], 100000),
                    _gte(m["atr_pct"], 4),
                    _gte(m["adr_pct"], 4),
                    _gte(m["relative_volume"], 1.5),
                    _gte(m["change_1d_pct"], 3.9),
                    _gte(m["distance_from_52w_low_pct"], 30),
                    _gte(m["close"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                ]
            ),
        ),
        PresetDefinition(
            key="us_market_leader",
            label="US Market Leader",
            market="us",
            sorter="avg_traded_value_30d",
            predicate=lambda m: all(
                [
                    _gte(m["close"], 1),
                    _gte(m["market_cap"], 100_000_000) if m["market_cap"] is not None else True,
                    _gte(m["avg_volume_60d"], 100000),
                    _gte(m["avg_traded_value_60d"], 10_000_000),
                    _gte(m["avg_traded_value_30d"], 10_000_000),
                    _gte(m["avg_traded_value_10d"], 10_000_000),
                    _gte(m["atr_pct"], 4),
                    _gte(m["adr_pct"], 4),
                    _gte(m["perf_3m_pct"], 30),
                    _gte(m["perf_6m_pct"], 0),
                    _gte(m["change_1d_pct"], 0),
                    _gte(m["distance_from_52w_low_pct"], 50),
                    _gte(m["ema5"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                    _gte(m["ema20"], m["ema50"] or float("inf")) if m["ema50"] is not None else False,
                    _gte(m["ema50"], m["ema100"] or float("inf")) if m["ema100"] is not None else False,
                    _gte(m["close"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                ]
            ),
        ),
        PresetDefinition(
            key="us_trend_breakout",
            label="US Trend Breakout",
            market="us",
            sorter="relative_volume",
            predicate=lambda m: all(
                [
                    _gte(m["close"], 1),
                    _gte(m["avg_traded_value_60d"], 10_000_000),
                    _gte(m["avg_traded_value_30d"], 10_000_000),
                    _gte(m["avg_traded_value_10d"], 10_000_000),
                    _gte(m["atr_pct"], 3),
                    _gte(m["adr_pct"], 3),
                    _gte(m["relative_volume"], 2),
                    _gte(m["change_1d_pct"], 3.9),
                    _gte(m["perf_3m_pct"], 0),
                    _gte(m["close"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                    _gte(m["ema20"], m["ema50"] or float("inf")) if m["ema50"] is not None else False,
                    _gte(m["ema50"], m["ema100"] or float("inf")) if m["ema100"] is not None else False,
                ]
            ),
        ),
        PresetDefinition(
            key="us_high_volatility",
            label="US High Volatility",
            market="us",
            sorter="adr_pct",
            predicate=lambda m: all(
                [
                    _gte(m["close"], 1),
                    _gte(m["avg_volume_60d"], 100000),
                    _gte(m["avg_traded_value_30d"], 10_000_000),
                    _gte(m["avg_traded_value_10d"], 10_000_000),
                    _gte(m["atr_pct"], 4.5),
                    _gte(m["adr_pct"], 4.5),
                    _gte(m["perf_3m_pct"], 30),
                    _gte(m["distance_from_52w_low_pct"], 50),
                    _gte(m["close"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                    _gte(m["ema20"], m["ema50"] or float("inf")) if m["ema50"] is not None else False,
                    _gte(m["ema50"], m["ema100"] or float("inf")) if m["ema100"] is not None else False,
                ]
            ),
        ),
        PresetDefinition(
            key="kr_breakout_rvol",
            label="KR Breakout RVOL",
            market="kr",
            sorter="relative_volume",
            predicate=lambda m: all(
                [
                    _gte(m["avg_volume_60d"], 100000),
                    _gte(m["avg_traded_value_60d"], 3_000_000_000),
                    _gte(m["avg_traded_value_30d"], 10_000_000_000),
                    _gte(m["avg_traded_value_10d"], 10_000_000_000),
                    _gte(m["atr_pct"], 4),
                    _gte(m["adr_pct"], 4),
                    _gte(m["relative_volume"], 1.5),
                    _gte(m["change_1d_pct"], 4),
                    _gte(m["perf_6m_pct"], 0),
                    _gte(m["distance_from_52w_low_pct"], 50),
                    _gte(m["close"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                    _gte(m["ema10"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                    _gte(m["ema20"], m["ema50"] or float("inf")) if m["ema50"] is not None else False,
                    _gte(m["ema50"], m["ema100"] or float("inf")) if m["ema100"] is not None else False,
                ]
            ),
        ),
        PresetDefinition(
            key="kr_market_leader",
            label="KR Market Leader",
            market="kr",
            sorter="avg_traded_value_30d",
            predicate=lambda m: all(
                [
                    _gte(m["avg_volume_60d"], 100000),
                    _gte(m["avg_traded_value_60d"], 3_000_000_000),
                    _gte(m["avg_traded_value_30d"], 5_000_000_000),
                    _gte(m["avg_traded_value_10d"], 5_000_000_000),
                    _gte(m["atr_pct"], 4),
                    _gte(m["adr_pct"], 4),
                    _gte(m["change_1d_pct"], 0),
                    _gte(m["perf_1m_pct"], 0),
                    _gte(m["perf_3m_pct"], 0),
                    _gte(m["distance_from_52w_low_pct"], 50),
                    _gte(m["close"], m["ema10"] or float("inf")) if m["ema10"] is not None else False,
                    _gte(m["ema10"], m["ema20"] or float("inf")) if m["ema20"] is not None else False,
                    _gte(m["ema20"], m["ema50"] or float("inf")) if m["ema50"] is not None else False,
                    _gte(m["ema50"], m["ema100"] or float("inf")) if m["ema100"] is not None else False,
                ]
            ),
        ),
    ]


def _preset_definitions_for_market(market: str) -> list[PresetDefinition]:
    normalized_market = market_key(market)
    return [item for item in _preset_definitions() if item.market == normalized_market]


def run_tradingview_preset_screeners(*, market: str = "us") -> dict[str, pd.DataFrame]:
    normalized_market = market_key(market)
    ensure_market_dirs(normalized_market)
    results_dir = get_tradingview_results_dir(normalized_market)
    os.makedirs(results_dir, exist_ok=True)

    metadata_frame = _load_market_metadata_frame(normalized_market)
    market_cap_map = _load_market_cap_map(metadata_frame)
    shares_outstanding_map = _load_numeric_metadata_map(metadata_frame, "shares_outstanding")
    sector_map = _load_text_metadata_map(metadata_frame, "sector")
    industry_map = _load_text_metadata_map(metadata_frame, "industry")
    data_dir = get_market_data_dir(normalized_market)
    symbols = sorted(
        {
            os.path.splitext(name)[0].upper()
            for name in os.listdir(data_dir)
            if name.endswith(".csv") and not is_index_symbol(normalized_market, os.path.splitext(name)[0].upper())
        }
    ) if os.path.isdir(data_dir) else []
    presets = _preset_definitions_for_market(normalized_market)
    print(
        f"[TradingView] Metric build started ({normalized_market}) - "
        f"symbols={len(symbols)}, presets={len(presets)}"
    )

    metrics_rows: list[dict[str, float | str | bool | None]] = []
    interval = progress_interval(len(symbols), target_updates=8, min_interval=50)
    for index, symbol in enumerate(symbols, start=1):
        frame = load_local_ohlcv_frame(normalized_market, symbol)
        if frame.empty or len(frame) < 120:
            if is_progress_tick(index, len(symbols), interval):
                print(
                    f"[TradingView] Metric build progress ({normalized_market}) - "
                    f"processed={index}/{len(symbols)}, eligible={len(metrics_rows)}"
                )
            continue
        metrics_rows.append(
            _build_metrics(
                symbol,
                normalized_market,
                frame,
                market_cap_map,
                shares_outstanding_map,
                sector_map,
                industry_map,
            )
        )
        if is_progress_tick(index, len(symbols), interval):
            print(
                f"[TradingView] Metric build progress ({normalized_market}) - "
                f"processed={index}/{len(symbols)}, eligible={len(metrics_rows)}"
            )

    metrics_df = pd.DataFrame(metrics_rows)
    output: dict[str, pd.DataFrame] = {}
    print(f"[TradingView] Metric build completed ({normalized_market}) - metrics={len(metrics_df)}")
    for preset_index, preset in enumerate(presets, start=1):
        if metrics_df.empty:
            preset_df = pd.DataFrame()
        else:
            preset_df = metrics_df[metrics_df.apply(lambda row: preset.predicate(row.to_dict()), axis=1)].copy()
            if not preset_df.empty:
                preset_df["preset"] = preset.key
                preset_df["preset_label"] = preset.label
                preset_df = preset_df.sort_values(preset.sorter, ascending=False).reset_index(drop=True)

        csv_path = os.path.join(results_dir, f"{preset.key}.csv")
        json_path = csv_path.replace(".csv", ".json")
        preset_df.to_csv(csv_path, index=False)
        preset_df.to_json(json_path, orient="records", indent=2, force_ascii=False)
        output[preset.key] = preset_df
        print(
            f"[TradingView] Preset {preset_index}/{len(presets)} ({normalized_market}) - "
            f"{preset.key}, candidates={len(preset_df)}"
        )

    return output
