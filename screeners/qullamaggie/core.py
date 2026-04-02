from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

import numpy as np
import pandas as pd

from utils.actual_data_calibration import bounded_quantile_value
from utils.indicator_helpers import (
    normalize_indicator_frame,
    rolling_atr as _helper_rolling_atr,
    rolling_average_volume,
    rolling_max,
    rolling_min,
    rolling_sma,
    rolling_traded_value,
)
from utils.market_data_contract import PricePolicy
from utils.typing_utils import row_to_record
from screeners.leader_core_bridge import MarketTruthSnapshot, shared_market_alias_to_qullamaggie_state

from .earnings_data_collector import EarningsDataCollector


class SupportsEarningsCollector(Protocol):
    def get_earnings_surprise(self, symbol: str) -> Mapping[str, Any] | None: ...


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(casted) or np.isinf(casted):
        return None
    return casted


def _safe_bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if value in {1, 1.0, "1", "true", "True", "TRUE", "yes", "Y"}:
        return True
    return False


def _clamp(value: float | None, low: float = 0.0, high: float = 1.0) -> float:
    if value is None:
        return low
    if np.isnan(value) or np.isinf(value):
        return low
    return float(min(max(value, low), high))


def _score_ratio(value: float | None, good_min: float, bad_min: float) -> float:
    if value is None:
        return 0.0
    if value >= good_min:
        return 1.0
    if value <= bad_min:
        return 0.0
    return _clamp((value - bad_min) / max(good_min - bad_min, 1e-9))


def _score_inverse(value: float | None, good_max: float, bad_max: float) -> float:
    if value is None:
        return 0.0
    if value <= good_max:
        return 1.0
    if value >= bad_max:
        return 0.0
    return _clamp((bad_max - value) / max(bad_max - good_max, 1e-9))


def _score_range(value: float | None, ideal_low: float, ideal_high: float, min_low: float, max_high: float) -> float:
    if value is None:
        return 0.0
    if ideal_low <= value <= ideal_high:
        return 1.0
    if value < min_low or value > max_high:
        return 0.0
    if value < ideal_low:
        return _clamp((value - min_low) / max(ideal_low - min_low, 1e-9))
    return _clamp((max_high - value) / max(max_high - ideal_high, 1e-9))


def _weighted_mean(items: list[tuple[float | None, float]]) -> float:
    total_weight = 0.0
    total_value = 0.0
    for value, weight in items:
        if value is None:
            continue
        total_value += float(value) * float(weight)
        total_weight += float(weight)
    if total_weight <= 0:
        return 0.0
    return total_value / total_weight


def _round_or_none(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _as_date_str(value: Any) -> str | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _unique(items: list[str]) -> list[str]:
    return list(dict.fromkeys([item for item in items if item]))


def _percentile_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(50.0, index=series.index, dtype=float)
    if numeric.nunique(dropna=True) <= 1:
        return pd.Series(100.0, index=series.index, dtype=float)
    return numeric.rank(pct=True, method="average").fillna(0.5) * 100.0


def _rolling_atr(daily: pd.DataFrame, window: int) -> pd.Series:
    return _helper_rolling_atr(daily, window, min_periods=max(2, window // 3))


@dataclass(frozen=True)
class MarketProfile:
    market_code: str
    timezone: str
    currency: str
    session_open: str
    session_close: str
    supports_premarket: bool
    supports_afterhours: bool
    orh_windows: tuple[str, ...]
    ep_orh_windows: tuple[str, ...]
    price_floor: float
    min_adv20: float
    min_turnover20: float


@dataclass(frozen=True)
class MarketRegime:
    market_code: str
    benchmark_symbol: str
    regime_state: str
    regime_score: float
    market_alignment_score: float
    breadth_support_score: float
    rotation_support_score: float
    benchmark_trend_score: float | None
    reason_codes: tuple[str, ...]
    data_flags: tuple[str, ...] = ()
    leader_health_score: float | None = None
    market_alias: str = ""


class QullamaggieAnalyzer:
    MARKET_PROFILES: dict[str, MarketProfile] = {
        "us": MarketProfile(
            market_code="US",
            timezone="America/New_York",
            currency="USD",
            session_open="09:30",
            session_close="16:00",
            supports_premarket=True,
            supports_afterhours=True,
            orh_windows=("5m_ORH", "60m_ORH"),
            ep_orh_windows=("1m_ORH", "5m_ORH"),
            price_floor=5.0,
            min_adv20=250_000.0,
            min_turnover20=2_000_000.0,
        ),
        "kr": MarketProfile(
            market_code="KR",
            timezone="Asia/Seoul",
            currency="KRW",
            session_open="09:00",
            session_close="15:30",
            supports_premarket=False,
            supports_afterhours=False,
            orh_windows=("15m_ORH", "30m_ORH"),
            ep_orh_windows=("5m_ORH", "15m_ORH"),
            price_floor=1_000.0,
            min_adv20=150_000.0,
            min_turnover20=800_000_000.0,
        ),
    }

    def market_profile(self, market: str) -> MarketProfile:
        market_key = str(market or "us").strip().lower()
        return self.MARKET_PROFILES.get(market_key, self.MARKET_PROFILES["us"])

    def normalize_daily_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        return normalize_indicator_frame(frame, price_policy=PricePolicy.SPLIT_ADJUSTED)

    def _higher_low_score(self, low_series: pd.Series) -> float:
        recent = pd.to_numeric(low_series, errors="coerce").dropna()
        if len(recent) < 15:
            return 30.0
        window = recent.iloc[-30:]
        chunks = np.array_split(window.to_numpy(dtype=float), 3)
        lows = [float(np.min(chunk)) for chunk in chunks if len(chunk) > 0]
        if len(lows) < 3:
            return 30.0
        if lows[2] > lows[1] > lows[0]:
            return 100.0
        if lows[2] > lows[1] and lows[1] >= lows[0] * 0.99:
            return 75.0
        if lows[2] > lows[0]:
            return 55.0
        return 20.0

    def _ma_surf_score(self, close: float | None, sma10: float | None, sma20: float | None, sma50: float | None) -> float:
        if close is None or sma10 is None or sma20 is None or sma50 is None:
            return 20.0
        stack_score = float(np.mean([close > sma10, sma10 > sma20, sma20 > sma50])) * 100.0
        surf_distance = abs(close - sma10) / max(close, 1e-9)
        surf_score = _score_inverse(surf_distance, 0.03, 0.12) * 100.0
        return _weighted_mean([(stack_score, 0.7), (surf_score, 0.3)])

    def _find_base_window(self, daily: pd.DataFrame) -> dict[str, float | int | None]:
        latest_close = _safe_float(daily["close"].iloc[-1]) if not daily.empty else None
        if latest_close is None or len(daily) < 20:
            return {
                "base_length": None,
                "base_high": None,
                "base_low": None,
                "base_depth_pct": None,
                "base_close_position": None,
                "base_quality_score": 0.0,
                "base_start_idx": None,
            }

        best_window: dict[str, float | int | None] = {
            "base_length": None,
            "base_high": None,
            "base_low": None,
            "base_depth_pct": None,
            "base_close_position": None,
            "base_quality_score": 0.0,
            "base_start_idx": None,
        }

        for length in (10, 15, 20, 25, 30, 35, 40, 50, 60):
            if len(daily) < length + 5:
                continue
            window = daily.iloc[-length:].copy()
            base_high = _safe_float(window["high"].max())
            base_low = _safe_float(window["low"].min())
            latest_close = _safe_float(window["close"].iloc[-1])
            if latest_close is None or base_high is None or base_low is None or base_high <= 0 or base_high <= base_low:
                continue
            base_depth = (base_high - base_low) / base_high
            close_pos = (latest_close - base_low) / max(base_high - base_low, 1e-9)
            daily_change = window["close"].pct_change().abs().rolling(5, min_periods=2).mean().iloc[-1]
            quiet_score = _score_inverse(_safe_float(daily_change), 0.01, 0.04) * 100.0
            depth_score = _score_inverse(base_depth, 0.16, 0.38) * 100.0
            close_score = _score_ratio(close_pos, 0.72, 0.35) * 100.0
            length_score = _score_range(float(length), 12.0, 40.0, 5.0, 60.0) * 100.0
            quality = _weighted_mean(
                [
                    (depth_score, 0.35),
                    (close_score, 0.30),
                    (quiet_score, 0.20),
                    (length_score, 0.15),
                ]
            )
            if quality > float(best_window["base_quality_score"] or 0.0):
                best_window = {
                    "base_length": length,
                    "base_high": base_high,
                    "base_low": base_low,
                    "base_depth_pct": base_depth * 100.0,
                    "base_close_position": close_pos * 100.0,
                    "base_quality_score": quality,
                    "base_start_idx": len(daily) - length,
                }

        return best_window

    def compute_feature_row(
        self,
        symbol: str,
        market: str,
        daily_frame: pd.DataFrame,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        profile = self.market_profile(market)
        daily = self.normalize_daily_frame(daily_frame)
        base_metadata = dict(metadata or {})
        if daily.empty:
            return {
                "symbol": str(symbol or "").strip().upper(),
                "market": profile.market_code,
                "bars": 0,
                "has_daily_bars": False,
            }

        daily["sma10"] = rolling_sma(daily["close"], 10, min_periods=5)
        daily["sma20"] = rolling_sma(daily["close"], 20, min_periods=10)
        daily["sma50"] = rolling_sma(daily["close"], 50, min_periods=20)
        daily["sma200"] = rolling_sma(daily["close"], 200, min_periods=60)
        daily["adr_pct"] = ((daily["high"] - daily["low"]) / daily["close"].replace(0, np.nan)) * 100.0
        daily["adr20_pct"] = daily["adr_pct"].rolling(20, min_periods=5).mean()
        daily["atr14"] = _rolling_atr(daily, 14)
        daily["atr20"] = _rolling_atr(daily, 20)
        daily["atr60"] = _rolling_atr(daily, 60)
        daily["atr10"] = _rolling_atr(daily, 10)
        daily["adv20"] = rolling_average_volume(daily, 20, min_periods=5)
        daily["avg_turnover20"] = rolling_traded_value(daily, 20, min_periods=5)
        daily["rolling_high_252"] = rolling_max(daily["high"], 252, min_periods=60)
        daily["rolling_low_252"] = rolling_min(daily["low"], 252, min_periods=60)
        daily["dcr"] = (daily["close"] - daily["low"]) / (daily["high"] - daily["low"]).replace(0, np.nan)

        latest = daily.iloc[-1]
        close = _safe_float(latest["close"])
        open_price = _safe_float(latest["open"])
        high = _safe_float(latest["high"])
        low = _safe_float(latest["low"])
        volume = _safe_float(latest["volume"])
        adv20 = _safe_float(latest["adv20"])
        avg_turnover20 = _safe_float(latest["avg_turnover20"])
        sma10 = _safe_float(latest["sma10"])
        sma20 = _safe_float(latest["sma20"])
        sma50 = _safe_float(latest["sma50"])
        sma200 = _safe_float(latest["sma200"])
        adr20_pct = _safe_float(latest["adr20_pct"])
        atr20 = _safe_float(latest["atr20"])
        atr60 = _safe_float(latest["atr60"])
        atr10 = _safe_float(latest["atr10"])
        dcr = _safe_float(latest["dcr"]) or 0.5

        base = self._find_base_window(daily)
        base_start_idx = int(base["base_start_idx"]) if base["base_start_idx"] is not None else None
        base_high = _safe_float(base["base_high"])
        base_low = _safe_float(base["base_low"])
        base_length = int(base["base_length"]) if base["base_length"] is not None else None
        base_depth_pct = _safe_float(base["base_depth_pct"])
        base_close_position = _safe_float(base["base_close_position"])
        base_quality_score = _safe_float(base["base_quality_score"]) or 0.0

        prior_window_start = max(0, (base_start_idx or len(daily)) - 60)
        prior_window_end = max(1, base_start_idx or len(daily))
        prior_window = daily.iloc[prior_window_start:prior_window_end]
        prior_low = _safe_float(prior_window["close"].min()) if not prior_window.empty else None
        prior_run_pct = None
        if base_high is not None and prior_low is not None and prior_low > 0:
            prior_run_pct = (base_high / prior_low) - 1.0

        high_52w = _safe_float(latest["rolling_high_252"]) or _safe_float(daily["high"].max())
        low_52w = _safe_float(latest["rolling_low_252"]) or _safe_float(daily["low"].min())
        high_52w_proximity = None
        close_to_high_52w = _safe_divide(close, high_52w)
        if close_to_high_52w is not None:
            high_52w_proximity = close_to_high_52w

        atr10_over_atr60 = _safe_divide(atr10, atr60)

        higher_low_score = self._higher_low_score(daily["low"])
        ma_surf_score = self._ma_surf_score(close, sma10, sma20, sma50)
        atr_contraction_score = _score_inverse(atr10_over_atr60, 0.70, 1.10) * 100.0
        compression_score = _weighted_mean(
            [
                (atr_contraction_score, 0.40),
                (higher_low_score, 0.30),
                (ma_surf_score, 0.30),
            ]
        )

        close_gt_50dma = bool(close is not None and sma50 is not None and close > sma50)
        close_gt_200dma = bool(close is not None and sma200 is not None and close > sma200)
        ma_stack_ok = bool(
            close is not None
            and sma10 is not None
            and sma20 is not None
            and sma50 is not None
            and close > sma10 > sma20 > sma50
        )

        last_5_avg_vol = _safe_float(daily["volume"].tail(5).mean())
        volume_dry_up_score = 50.0
        last_5_to_adv20 = _safe_divide(last_5_avg_vol, adv20)
        if last_5_to_adv20 is not None:
            volume_dry_up_score = _score_inverse(last_5_to_adv20, 0.65, 1.15) * 100.0

        pivot_price = base_high * 1.001 if base_high is not None else None
        stop_price = None
        if base_low is not None and pivot_price is not None:
            volatility_buffer = min(adr20_pct or 4.0, ((atr20 or 0.0) / max(pivot_price, 1e-9)) * 100.0 if atr20 else 4.0)
            stop_price = max(base_low, pivot_price * (1.0 - max(volatility_buffer, 1.0) / 100.0))

        entry_price = max(close or 0.0, pivot_price or 0.0) if close is not None else pivot_price
        risk_unit_pct = None
        if entry_price is not None and stop_price is not None and entry_price > 0:
            risk_unit_pct = ((entry_price - stop_price) / entry_price) * 100.0

        pivot_distance_pct = None
        if close is not None and pivot_price is not None and pivot_price > 0:
            pivot_distance_pct = ((close / pivot_price) - 1.0) * 100.0
        readiness_score = _weighted_mean(
            [
                ((_score_inverse(abs(pivot_distance_pct or 0.0), 0.8, 6.0) * 100.0), 0.35),
                (volume_dry_up_score, 0.20),
                (base_close_position or 50.0, 0.20),
                (dcr * 100.0, 0.15),
                (100.0 if ma_stack_ok else 40.0, 0.10),
            ]
        )

        close_series = pd.to_numeric(daily["close"], errors="coerce")
        ret_1d = _safe_float(close_series.pct_change(1).iloc[-1])
        ret_1m = _safe_float(close_series.pct_change(21).iloc[-1])
        ret_3m = _safe_float(close_series.pct_change(63).iloc[-1])
        ret_6m = _safe_float(close_series.pct_change(126).iloc[-1])
        ret_12m = _safe_float(close_series.pct_change(252).iloc[-1])

        gap_pct = None
        if len(daily) >= 2 and open_price is not None:
            prev_close = _safe_float(daily["close"].iloc[-2])
            gap_ratio = _safe_divide(open_price, prev_close)
            if gap_ratio is not None:
                gap_pct = gap_ratio - 1.0

        rvol = _safe_divide(volume, adv20)

        no_excessive_run = ret_3m is None or ret_3m < 1.0
        neglected_base_score = _weighted_mean(
            [
                ((_score_inverse(ret_3m, 0.65, 1.50) * 100.0), 0.45),
                ((_score_range(float(base_length or 0.0), 20.0, 90.0, 10.0, 140.0) * 100.0), 0.30),
                (compression_score, 0.25),
            ]
        )

        atr20_ratio = _safe_divide(atr20, close)

        sector = str(base_metadata.get("sector") or "").strip()
        industry = str(base_metadata.get("industry") or base_metadata.get("industry_group") or "").strip()
        exchange = str(base_metadata.get("exchange") or "").strip()
        market_cap = _safe_float(base_metadata.get("market_cap"))
        revenue_growth = _safe_float(base_metadata.get("revenue_growth"))
        earnings_growth = _safe_float(base_metadata.get("earnings_growth"))
        roe = _safe_float(base_metadata.get("return_on_equity"))

        return {
            "symbol": str(symbol or "").strip().upper(),
            "market": profile.market_code,
            "as_of_ts": _as_date_str(latest["date"]),
            "bars": int(len(daily)),
            "has_daily_bars": True,
            "close": close,
            "open": open_price,
            "high": high,
            "low": low,
            "volume": volume,
            "adv20": adv20,
            "avg_turnover20": avg_turnover20,
            "sma10": sma10,
            "sma20": sma20,
            "sma50": sma50,
            "sma200": sma200,
            "adr20_pct": adr20_pct,
            "atr20": atr20,
            "atr20_pct": (atr20_ratio * 100.0) if atr20_ratio is not None else None,
            "natr20": (atr20_ratio * 100.0) if atr20_ratio is not None else None,
            "dcr": dcr,
            "dcr_high_freq": float((daily["dcr"].tail(20) >= 0.60).mean()) if len(daily) >= 5 else 0.0,
            "ret_1d": ret_1d,
            "ret_1m": ret_1m,
            "ret_3m": ret_3m,
            "ret_6m": ret_6m,
            "ret_12m": ret_12m,
            "gap_pct": gap_pct,
            "rvol": rvol,
            "rolling_high_252": high_52w,
            "rolling_low_252": low_52w,
            "high_52w_proximity": high_52w_proximity,
            "base_length": base_length,
            "base_high": base_high,
            "base_low": base_low,
            "base_depth_pct": base_depth_pct,
            "base_close_position": base_close_position,
            "base_quality_score": base_quality_score,
            "prior_run_pct": prior_run_pct,
            "atr10_over_atr60": atr10_over_atr60,
            "higher_low_score": higher_low_score,
            "ma_surf_score": ma_surf_score,
            "compression_score": compression_score,
            "readiness_score": readiness_score,
            "pivot_price": pivot_price,
            "stop_price": stop_price,
            "risk_unit_pct": risk_unit_pct,
            "pivot_distance_pct": pivot_distance_pct,
            "close_gt_50dma": close_gt_50dma,
            "close_gt_200dma": close_gt_200dma,
            "ma_stack_ok": ma_stack_ok,
            "volume_dry_up_score": volume_dry_up_score,
            "no_excessive_run": no_excessive_run,
            "neglected_base_score": neglected_base_score,
            "sector": sector,
            "industry": industry,
            "exchange": exchange,
            "market_cap": market_cap,
            "revenue_growth": revenue_growth,
            "earnings_growth": earnings_growth,
            "return_on_equity": roe,
            "has_sector_mapping": bool(sector),
            "has_fundamentals": any(value is not None for value in (market_cap, revenue_growth, earnings_growth, roe)),
            "has_corp_actions": False,
            "has_intraday_bars": False,
        }

    def finalize_feature_table(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        if feature_table.empty:
            return feature_table

        table = feature_table.copy()
        for column in (
            "ret_1m",
            "ret_3m",
            "ret_6m",
            "ret_12m",
            "adv20",
            "avg_turnover20",
            "compression_score",
            "readiness_score",
            "rvol",
        ):
            if column in table.columns:
                table[column] = pd.to_numeric(table[column], errors="coerce")
            else:
                table[column] = pd.Series(np.nan, index=table.index, dtype=float)

        table["ret_1m_pctile"] = _percentile_series(table["ret_1m"])
        table["ret_3m_pctile"] = _percentile_series(table["ret_3m"])
        table["ret_6m_pctile"] = _percentile_series(table["ret_6m"])
        table["turnover20_pctile"] = _percentile_series(table["avg_turnover20"])
        table["adv20_pctile"] = _percentile_series(table["adv20"])
        table["high_proximity_score"] = table["high_52w_proximity"].apply(lambda value: _score_ratio(value, 0.95, 0.75) * 100.0)

        def _fundamental_score_row(row: pd.Series) -> float:
            earnings_growth = _safe_float(row.get("earnings_growth"))
            revenue_growth = _safe_float(row.get("revenue_growth"))
            return_on_equity = _safe_float(row.get("return_on_equity"))
            market_cap = _safe_float(row.get("market_cap"))
            market_cap_log = np.log10(max(market_cap, 1.0)) if market_cap is not None else None
            return _weighted_mean(
                [
                    ((_score_ratio(earnings_growth, 25.0, 0.0) * 100.0) if earnings_growth is not None else 70.0, 0.35),
                    ((_score_ratio(revenue_growth, 15.0, 0.0) * 100.0) if revenue_growth is not None else 70.0, 0.35),
                    ((_score_ratio(return_on_equity, 0.15, 0.02) * 100.0) if return_on_equity is not None else 70.0, 0.15),
                    ((_score_ratio(market_cap_log, 9.0, 7.0) * 100.0) if market_cap_log is not None else 70.0, 0.15),
                ]
            )

        def _breakout_universe_pass_row(row: pd.Series) -> bool:
            close_value = _safe_float(row.get("close")) or 0.0
            adv20_value = _safe_float(row.get("adv20")) or 0.0
            turnover20_value = _safe_float(row.get("avg_turnover20")) or 0.0
            sma50_value = _safe_float(row.get("sma50"))
            ret_1m_pctile = _safe_float(row.get("ret_1m_pctile")) or 0.0
            ret_3m_pctile = _safe_float(row.get("ret_3m_pctile")) or 0.0
            ret_6m_pctile = _safe_float(row.get("ret_6m_pctile")) or 0.0
            high_52w_proximity = _safe_float(row.get("high_52w_proximity")) or 0.0

            close_vs_sma50_ok = _safe_bool(row.get("close_gt_50dma"))
            if not close_vs_sma50_ok and sma50_value is not None:
                close_vs_sma50_ok = close_value >= (sma50_value * 0.98)

            return bool(
                close_value >= 5.0
                and adv20_value >= 150_000.0
                and turnover20_value > 0.0
                and close_vs_sma50_ok
                and ret_1m_pctile >= 65.0
                and ret_3m_pctile >= 70.0
                and ret_6m_pctile >= 65.0
                and high_52w_proximity >= 0.85
            )

        def _ep_universe_pass_row(row: pd.Series) -> bool:
            close_value = _safe_float(row.get("close")) or 0.0
            adv20_value = _safe_float(row.get("adv20")) or 0.0
            gap_pct = _safe_float(row.get("gap_pct")) or 0.0
            rvol_value = _safe_float(row.get("rvol")) or 0.0
            high_52w_proximity = _safe_float(row.get("high_52w_proximity")) or 0.0
            return bool(
                close_value >= 5.0
                and adv20_value >= 150_000.0
                and (gap_pct >= 0.05 or rvol_value >= 1.5 or high_52w_proximity >= 0.90)
            )

        table["leadership_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    (_safe_float(row.get("ret_1m_pctile")), 0.25),
                    (_safe_float(row.get("ret_3m_pctile")), 0.35),
                    (_safe_float(row.get("ret_6m_pctile")), 0.30),
                    (_safe_float(row.get("high_proximity_score")), 0.10),
                ]
            ),
            axis=1,
        )
        table["trend_quality_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    (100.0 if _safe_bool(row.get("ma_stack_ok")) else 35.0, 0.35),
                    (_safe_float(row.get("higher_low_score")) or 30.0, 0.25),
                    (_safe_float(row.get("compression_score")) or 0.0, 0.25),
                    (((_safe_float(row.get("dcr_high_freq")) or 0.0) * 100.0), 0.15),
                ]
            ),
            axis=1,
        )
        table["price_score"] = table["close"].apply(lambda value: _score_ratio(value, 15.0, 3.0) * 100.0)
        table["liquidity_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    (_safe_float(row.get("turnover20_pctile")), 0.55),
                    (_safe_float(row.get("adv20_pctile")), 0.25),
                    (_safe_float(row.get("price_score")), 0.20),
                ]
            ),
            axis=1,
        )

        sector_summary = (
            table[table["sector"].fillna("").astype(str) != ""]
            .groupby("sector", dropna=False)
            .agg(
                sector_leadership=("leadership_score", "mean"),
                sector_compression=("compression_score", "mean"),
                sector_high_proximity=("high_52w_proximity", "mean"),
                sector_member_count=("symbol", "count"),
            )
            .reset_index()
        )
        if not sector_summary.empty:
            sector_summary["group_strength_score"] = sector_summary.apply(
                lambda row: _weighted_mean(
                    [
                        (_safe_float(row.get("sector_leadership")), 0.45),
                        (_safe_float(row.get("sector_compression")), 0.25),
                        ((_score_ratio(_safe_float(row.get("sector_high_proximity")), 0.92, 0.75) * 100.0), 0.20),
                        ((_score_ratio(_safe_float(row.get("sector_member_count")), 10.0, 2.0) * 100.0), 0.10),
                    ]
                ),
                axis=1,
            )
            table = table.merge(
                sector_summary[["sector", "group_strength_score", "sector_member_count"]],
                on="sector",
                how="left",
            )
        else:
            table["group_strength_score"] = np.nan
            table["sector_member_count"] = np.nan

        table["group_strength_score"] = table["group_strength_score"].fillna(70.0)
        table["fundamental_score"] = table.apply(_fundamental_score_row, axis=1)
        table["a_pp_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    (_safe_float(row.get("leadership_score")), 0.30),
                    (_safe_float(row.get("trend_quality_score")), 0.25),
                    (_safe_float(row.get("liquidity_score")), 0.15),
                    (_safe_float(row.get("group_strength_score")), 0.15),
                    (_safe_float(row.get("fundamental_score")), 0.15),
                ]
            ),
            axis=1,
        )
        table["stock_grade"] = table["a_pp_score"].apply(self.stock_grade)
        table["data_confidence_score"] = table.apply(
            lambda row: self.data_confidence_score(
                has_daily_bars=_safe_bool(row.get("has_daily_bars")),
                has_intraday_bars=_safe_bool(row.get("has_intraday_bars")),
                has_corp_actions=_safe_bool(row.get("has_corp_actions")),
                has_sector_mapping=_safe_bool(row.get("has_sector_mapping")),
                has_event_data=False,
                has_fundamentals=_safe_bool(row.get("has_fundamentals")),
                has_estimate_data=False,
            ),
            axis=1,
        )
        table["breakout_universe_pass"] = table.apply(_breakout_universe_pass_row, axis=1)
        table["ep_universe_pass"] = table.apply(_ep_universe_pass_row, axis=1)
        table["focus_seed_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    (_safe_float(row.get("a_pp_score")), 0.60),
                    (_safe_float(row.get("compression_score")), 0.25),
                    (_safe_float(row.get("readiness_score")), 0.15),
                ]
            ),
            axis=1,
        )
        return table

    def build_actual_data_calibration(self, feature_table: pd.DataFrame, *, market: str) -> dict[str, float]:
        profile = self.market_profile(market)
        if feature_table.empty:
            return {
                "price_floor": profile.price_floor,
                "adv20_floor": profile.min_adv20,
                "turnover20_floor": profile.min_turnover20,
                "breakout_min_ret_1m_pctile": 65.0,
                "breakout_min_ret_3m_pctile": 70.0,
                "breakout_min_ret_6m_pctile": 65.0,
                "breakout_min_high_52w_proximity": 0.85,
                "breakout_min_compression_score": 65.0,
                "breakout_min_prior_run_pct": 0.30,
                "breakout_min_base_length": 5.0,
                "breakout_max_base_length": 60.0,
                "breakout_setup_pass_score": 70.0,
                "breakout_setup_five_star_score": 90.0,
                "breakout_volume_confirmation_min": 1.5,
                "breakout_volume_strong_min": 1.6,
                "ep_min_gap_pct": 0.10,
                "ep_watch_volume_min": 2.0,
                "ep_core_volume_min": 3.0 if market == "us" else 2.5,
                "ep_min_neglected_base_score": 55.0,
                "ep_setup_pass_score": 70.0,
                "ep_setup_five_star_score": 90.0,
                "focus_seed_min": 80.0,
                "top_candidate_score_min": 85.0,
                "group_strength_min": 70.0,
                "breakout_success_min_pivot_distance": -1.5,
            }

        table = feature_table.copy()
        liquid = table[
            table["close"].fillna(0.0) >= profile.price_floor
        ].copy()
        if liquid.empty:
            liquid = table

        price_floor = bounded_quantile_value(
            liquid["close"],
            0.20,
            profile.price_floor,
            lower=profile.price_floor,
        )
        adv20_floor = bounded_quantile_value(
            liquid["adv20"],
            0.25,
            profile.min_adv20,
            lower=profile.min_adv20,
            positive_only=True,
        )
        turnover20_floor = bounded_quantile_value(
            liquid["avg_turnover20"],
            0.20,
            profile.min_turnover20,
            lower=profile.min_turnover20,
            positive_only=True,
        )
        breakout_min_high_52w_proximity = bounded_quantile_value(
            liquid["high_52w_proximity"],
            0.55,
            0.85,
            lower=0.80,
            upper=0.96,
            positive_only=True,
        )
        breakout_min_ret_1m_pctile = bounded_quantile_value(
            liquid["ret_1m_pctile"],
            0.60,
            65.0,
            lower=55.0,
            upper=88.0,
        )
        breakout_min_ret_3m_pctile = bounded_quantile_value(
            liquid["ret_3m_pctile"],
            0.60,
            70.0,
            lower=60.0,
            upper=88.0,
        )
        breakout_min_ret_6m_pctile = bounded_quantile_value(
            liquid["ret_6m_pctile"],
            0.60,
            65.0,
            lower=55.0,
            upper=88.0,
        )
        breakout_min_compression_score = bounded_quantile_value(
            liquid["compression_score"],
            0.55,
            65.0,
            lower=58.0,
            upper=82.0,
        )
        breakout_min_prior_run_pct = bounded_quantile_value(
            liquid["prior_run_pct"],
            0.55,
            0.30,
            lower=0.18,
            upper=0.65,
            positive_only=True,
        )
        breakout_min_base_length = bounded_quantile_value(
            liquid["base_length"],
            0.25,
            5.0,
            lower=5.0,
            upper=25.0,
            positive_only=True,
        )
        breakout_max_base_length = bounded_quantile_value(
            liquid["base_length"],
            0.85,
            60.0,
            lower=25.0,
            upper=90.0,
            positive_only=True,
        )
        breakout_setup_pass_score = bounded_quantile_value(
            liquid["focus_seed_score"],
            0.60,
            70.0,
            lower=68.0,
            upper=88.0,
        )
        breakout_setup_five_star_score = bounded_quantile_value(
            liquid["focus_seed_score"],
            0.88,
            90.0,
            lower=84.0,
            upper=96.0,
        )
        breakout_volume_confirmation_min = bounded_quantile_value(
            liquid["rvol"],
            0.65,
            1.5,
            lower=1.1,
            upper=2.5,
            positive_only=True,
        )
        breakout_volume_strong_min = bounded_quantile_value(
            liquid["rvol"],
            0.85,
            1.6,
            lower=max(1.3, breakout_volume_confirmation_min),
            upper=3.0,
            positive_only=True,
        )
        ep_min_gap_pct = bounded_quantile_value(
            liquid["gap_pct"],
            0.75,
            0.10,
            lower=0.04,
            upper=0.18,
            positive_only=True,
        )
        ep_watch_volume_min = bounded_quantile_value(
            liquid["rvol"],
            0.70,
            2.0,
            lower=1.5,
            upper=3.0,
            positive_only=True,
        )
        ep_core_volume_min = bounded_quantile_value(
            liquid["rvol"],
            0.85,
            3.0 if market == "us" else 2.5,
            lower=max(1.8, ep_watch_volume_min),
            upper=4.5,
            positive_only=True,
        )
        ep_min_neglected_base_score = bounded_quantile_value(
            liquid["neglected_base_score"],
            0.55,
            55.0,
            lower=45.0,
            upper=80.0,
        )
        ep_setup_pass_score = bounded_quantile_value(
            liquid["a_pp_score"],
            0.60,
            70.0,
            lower=65.0,
            upper=88.0,
        )
        ep_setup_five_star_score = bounded_quantile_value(
            liquid["a_pp_score"],
            0.88,
            90.0,
            lower=84.0,
            upper=96.0,
        )
        focus_seed_min = bounded_quantile_value(
            liquid["focus_seed_score"],
            0.75,
            80.0,
            lower=70.0,
            upper=92.0,
        )
        top_candidate_score_min = bounded_quantile_value(
            liquid["a_pp_score"],
            0.85,
            85.0,
            lower=78.0,
            upper=94.0,
        )
        group_strength_min = bounded_quantile_value(
            liquid["group_strength_score"],
            0.60,
            70.0,
            lower=60.0,
            upper=85.0,
        )
        breakout_success_min_pivot_distance = bounded_quantile_value(
            liquid["pivot_distance_pct"],
            0.45,
            -1.5,
            lower=-4.0,
            upper=1.0,
        )

        return {
            "price_floor": price_floor,
            "adv20_floor": adv20_floor,
            "turnover20_floor": turnover20_floor,
            "breakout_min_ret_1m_pctile": breakout_min_ret_1m_pctile,
            "breakout_min_ret_3m_pctile": breakout_min_ret_3m_pctile,
            "breakout_min_ret_6m_pctile": breakout_min_ret_6m_pctile,
            "breakout_min_high_52w_proximity": breakout_min_high_52w_proximity,
            "breakout_min_compression_score": breakout_min_compression_score,
            "breakout_min_prior_run_pct": breakout_min_prior_run_pct,
            "breakout_min_base_length": breakout_min_base_length,
            "breakout_max_base_length": max(breakout_max_base_length, breakout_min_base_length + 5.0),
            "breakout_setup_pass_score": breakout_setup_pass_score,
            "breakout_setup_five_star_score": max(breakout_setup_five_star_score, breakout_setup_pass_score + 8.0),
            "breakout_volume_confirmation_min": breakout_volume_confirmation_min,
            "breakout_volume_strong_min": max(breakout_volume_strong_min, breakout_volume_confirmation_min),
            "ep_min_gap_pct": ep_min_gap_pct,
            "ep_watch_volume_min": ep_watch_volume_min,
            "ep_core_volume_min": max(ep_core_volume_min, ep_watch_volume_min),
            "ep_min_neglected_base_score": ep_min_neglected_base_score,
            "ep_setup_pass_score": ep_setup_pass_score,
            "ep_setup_five_star_score": max(ep_setup_five_star_score, ep_setup_pass_score + 8.0),
            "focus_seed_min": focus_seed_min,
            "top_candidate_score_min": top_candidate_score_min,
            "group_strength_min": group_strength_min,
            "breakout_success_min_pivot_distance": breakout_success_min_pivot_distance,
        }

    def apply_actual_data_calibration(
        self,
        feature_table: pd.DataFrame,
        *,
        market: str,
        calibration: Mapping[str, Any],
    ) -> pd.DataFrame:
        if feature_table.empty:
            return feature_table

        table = feature_table.copy()
        price_floor = _safe_float(calibration.get("price_floor")) or self.market_profile(market).price_floor
        adv20_floor = _safe_float(calibration.get("adv20_floor")) or self.market_profile(market).min_adv20
        turnover20_floor = _safe_float(calibration.get("turnover20_floor")) or self.market_profile(market).min_turnover20
        breakout_min_ret_1m_pctile = _safe_float(calibration.get("breakout_min_ret_1m_pctile")) or 65.0
        breakout_min_ret_3m_pctile = _safe_float(calibration.get("breakout_min_ret_3m_pctile")) or 70.0
        breakout_min_ret_6m_pctile = _safe_float(calibration.get("breakout_min_ret_6m_pctile")) or 65.0
        breakout_min_high_52w_proximity = _safe_float(calibration.get("breakout_min_high_52w_proximity")) or 0.85
        ep_min_gap_pct = _safe_float(calibration.get("ep_min_gap_pct")) or 0.10
        ep_watch_volume_min = _safe_float(calibration.get("ep_watch_volume_min")) or 2.0

        def _breakout_universe_pass_row(row: pd.Series) -> bool:
            close_value = _safe_float(row.get("close")) or 0.0
            adv20_value = _safe_float(row.get("adv20")) or 0.0
            turnover20_value = _safe_float(row.get("avg_turnover20")) or 0.0
            sma50_value = _safe_float(row.get("sma50"))
            ret_1m_pctile = _safe_float(row.get("ret_1m_pctile")) or 0.0
            ret_3m_pctile = _safe_float(row.get("ret_3m_pctile")) or 0.0
            ret_6m_pctile = _safe_float(row.get("ret_6m_pctile")) or 0.0
            high_52w_proximity = _safe_float(row.get("high_52w_proximity")) or 0.0

            close_vs_sma50_ok = _safe_bool(row.get("close_gt_50dma"))
            if not close_vs_sma50_ok and sma50_value is not None:
                close_vs_sma50_ok = close_value >= (sma50_value * 0.98)

            return bool(
                close_value >= price_floor
                and adv20_value >= adv20_floor
                and turnover20_value >= turnover20_floor
                and close_vs_sma50_ok
                and ret_1m_pctile >= breakout_min_ret_1m_pctile
                and ret_3m_pctile >= breakout_min_ret_3m_pctile
                and ret_6m_pctile >= breakout_min_ret_6m_pctile
                and high_52w_proximity >= breakout_min_high_52w_proximity
            )

        def _ep_universe_pass_row(row: pd.Series) -> bool:
            close_value = _safe_float(row.get("close")) or 0.0
            adv20_value = _safe_float(row.get("adv20")) or 0.0
            turnover20_value = _safe_float(row.get("avg_turnover20")) or 0.0
            gap_pct = _safe_float(row.get("gap_pct")) or 0.0
            rvol_value = _safe_float(row.get("rvol")) or 0.0
            high_52w_proximity = _safe_float(row.get("high_52w_proximity")) or 0.0
            return bool(
                close_value >= price_floor
                and adv20_value >= adv20_floor
                and turnover20_value >= turnover20_floor
                and (
                    gap_pct >= ep_min_gap_pct * 0.6
                    or rvol_value >= ep_watch_volume_min * 0.8
                    or high_52w_proximity >= min(breakout_min_high_52w_proximity + 0.03, 0.97)
                )
            )

        table["breakout_universe_pass"] = table.apply(_breakout_universe_pass_row, axis=1)
        table["ep_universe_pass"] = table.apply(_ep_universe_pass_row, axis=1)
        table.attrs["actual_data_calibration"] = dict(calibration)
        return table

    def stock_grade(self, a_pp_score: float | None) -> str:
        score = _safe_float(a_pp_score) or 0.0
        if score >= 85.0:
            return "A++"
        if score >= 75.0:
            return "A+"
        if score >= 60.0:
            return "B"
        return "Discard"

    def setup_grade(self, setup_score: float | None, *, five_star: bool = False, watch: bool = False) -> str:
        if five_star:
            return "5-star"
        score = _safe_float(setup_score) or 0.0
        if watch and score < 80.0:
            return "Watch"
        if score >= 90.0:
            return "4-star"
        if score >= 80.0:
            return "3-star"
        if score >= 70.0:
            return "2-star"
        return "Watch"

    def data_confidence_score(
        self,
        *,
        has_daily_bars: bool,
        has_intraday_bars: bool,
        has_corp_actions: bool,
        has_sector_mapping: bool,
        has_event_data: bool,
        has_fundamentals: bool,
        has_estimate_data: bool,
    ) -> float:
        return _weighted_mean(
            [
                (100.0 if has_daily_bars else 0.0, 0.30),
                (100.0 if has_intraday_bars else 0.0, 0.12),
                (100.0 if has_corp_actions else 0.0, 0.08),
                (100.0 if has_sector_mapping else 0.0, 0.10),
                (100.0 if has_event_data else 0.0, 0.15),
                (100.0 if has_fundamentals else 0.0, 0.15),
                (100.0 if has_estimate_data else 0.0, 0.10),
            ]
        )

    def apply_basic_filters(
        self,
        frame: pd.DataFrame,
        *,
        market: str = "us",
        calibration: Mapping[str, Any] | None = None,
    ) -> tuple[bool, pd.DataFrame]:
        daily = self.normalize_daily_frame(frame)
        if daily.empty:
            return False, daily
        features = self.compute_feature_row("__FILTER__", market, daily)
        profile = self.market_profile(market)
        calibration_map = dict(calibration or {})
        price_floor = _safe_float(calibration_map.get("price_floor")) or profile.price_floor
        adv20_floor = _safe_float(calibration_map.get("adv20_floor")) or profile.min_adv20
        turnover20_floor = _safe_float(calibration_map.get("turnover20_floor")) or profile.min_turnover20
        passed = bool(
            (features.get("close") or 0.0) >= price_floor
            and (features.get("adv20") or 0.0) >= adv20_floor
            and (features.get("avg_turnover20") or 0.0) >= turnover20_floor
            and (features.get("adr20_pct") or 0.0) >= 2.0
            and (
                _safe_bool(features.get("close_gt_50dma"))
                or ((features.get("close") or 0.0) >= (features.get("sma50") or 0.0) * 0.97)
            )
        )
        return passed, daily

    def check_vcp_pattern(
        self,
        frame: pd.DataFrame,
        *,
        market: str = "us",
        calibration: Mapping[str, Any] | None = None,
        feature_row: Mapping[str, Any] | None = None,
    ) -> bool:
        calibration_map = dict(calibration or {})
        features = dict(feature_row or self.compute_feature_row("__VCP__", market, frame))
        base_length = _safe_float(features.get("base_length"))
        return bool(
            (features.get("prior_run_pct") or 0.0) >= (_safe_float(calibration_map.get("breakout_min_prior_run_pct")) or 0.30)
            and (features.get("compression_score") or 0.0) >= (_safe_float(calibration_map.get("breakout_min_compression_score")) or 68.0)
            and (features.get("higher_low_score") or 0.0) >= 65.0
            and base_length is not None
            and base_length >= (_safe_float(calibration_map.get("breakout_min_base_length")) or 10.0)
            and base_length <= (_safe_float(calibration_map.get("breakout_max_base_length")) or 45.0)
        )

    def _benchmark_trend_score(
        self,
        benchmark_daily: pd.DataFrame,
    ) -> float | None:
        benchmark = self.normalize_daily_frame(benchmark_daily)
        if benchmark.empty or len(benchmark) < 20:
            return None
        benchmark["sma20"] = rolling_sma(benchmark["close"], 20, min_periods=10)
        benchmark["sma50"] = rolling_sma(benchmark["close"], 50, min_periods=20)
        benchmark["sma10"] = rolling_sma(benchmark["close"], 10, min_periods=5)
        benchmark_close = _safe_float(benchmark["close"].iloc[-1])
        sma20 = _safe_float(benchmark["sma20"].iloc[-1])
        sma50 = _safe_float(benchmark["sma50"].iloc[-1])
        sma10 = _safe_float(benchmark["sma10"].iloc[-1])
        slope = None
        if len(benchmark) >= 30:
            prev = _safe_float(benchmark["sma20"].iloc[-10])
            slope_ratio = _safe_divide(sma20, prev)
            if slope_ratio is not None:
                slope = slope_ratio - 1.0
        return _weighted_mean(
            [
                (100.0 if benchmark_close is not None and sma50 is not None and benchmark_close > sma50 else 0.0, 0.30),
                (100.0 if sma10 is not None and sma20 is not None and sma10 > sma20 else 0.0, 0.25),
                ((_score_ratio(slope, 0.01, -0.01) * 100.0), 0.25),
                (100.0 if benchmark_close is not None and sma20 is not None and benchmark_close > sma20 else 0.0, 0.20),
            ]
        )

    def _default_regime(self, market: str) -> MarketRegime:
        return MarketRegime(
            market_code=self.market_profile(market).market_code,
            benchmark_symbol="",
            regime_state="RISK_NEUTRAL",
            regime_score=55.0,
            market_alignment_score=55.0,
            breadth_support_score=55.0,
            rotation_support_score=55.0,
            benchmark_trend_score=None,
            reason_codes=("REGIME_DEFAULTED",),
            data_flags=("missing_regime_context",),
        )

    def build_market_regime_from_truth(
        self,
        *,
        market: str,
        benchmark_symbol: str,
        market_truth: MarketTruthSnapshot,
        benchmark_daily: pd.DataFrame | None = None,
    ) -> MarketRegime:
        reasons = [f"CORE_{market_truth.market_alias}"]
        data_flags = ["core_market_truth"]
        if market_truth.market_state:
            reasons.append(f"MARKET_{market_truth.market_state.upper()}")
        if market_truth.breadth_state:
            reasons.append(f"BREADTH_{market_truth.breadth_state.upper()}")
        if market_truth.rotation_support_score is not None and market_truth.rotation_support_score >= 70.0:
            reasons.append("ROTATION_SUPPORTIVE")
        if market_truth.leader_health_score is not None and market_truth.leader_health_score >= 70.0:
            reasons.append("LEADER_HEALTHY")
        trend_adjustment = 0.0
        benchmark_trend_score = self._benchmark_trend_score(benchmark_daily if benchmark_daily is not None else pd.DataFrame())
        if benchmark_trend_score is None:
            data_flags.append("missing_benchmark_history")
        else:
            if benchmark_trend_score >= 70.0:
                reasons.append("BENCHMARK_UPTREND")
            trend_adjustment = (benchmark_trend_score - 50.0) * 0.10
        regime_score = _clamp((market_truth.market_alignment_score or 55.0) + trend_adjustment, 0.0, 100.0)
        return MarketRegime(
            market_code=self.market_profile(market).market_code,
            benchmark_symbol=str(benchmark_symbol or "").upper(),
            regime_state=shared_market_alias_to_qullamaggie_state(market_truth.market_alias),
            regime_score=round(regime_score, 2),
            market_alignment_score=round(market_truth.market_alignment_score or 55.0, 2),
            breadth_support_score=round(market_truth.breadth_support_score or 55.0, 2),
            rotation_support_score=round(market_truth.rotation_support_score or 55.0, 2),
            benchmark_trend_score=_round_or_none(benchmark_trend_score),
            reason_codes=tuple(_unique(reasons)),
            data_flags=tuple(_unique(data_flags)),
            leader_health_score=_round_or_none(market_truth.leader_health_score),
            market_alias=market_truth.market_alias,
        )

    def _regime_alignment_score(self, regime: MarketRegime) -> float:
        if regime.regime_state == "RISK_ON_AGGRESSIVE":
            return 100.0
        if regime.regime_state == "RISK_ON":
            return 85.0
        if regime.regime_state == "RISK_NEUTRAL":
            return 55.0
        return 20.0

    def _candidate_stage(self, *, tier: str, setup_family: str, passed: bool, universe_ready: bool) -> str:
        if tier == "Tier 1":
            return "DAILY_FOCUS"
        if tier == "Tier 2":
            return "WEEKLY_FOCUS"
        if passed:
            return "WIDE_LIST"
        if universe_ready:
            return "UNIVERSE"
        if setup_family == "EP":
            return "EP_WATCH"
        return "UNIVERSE"

    def _execution_quality(
        self,
        *,
        risk_unit_pct: float | None,
        volatility_cap_pct: float | None,
        volume_ratio: float | None,
        dcr: float | None,
    ) -> float:
        risk_score = 50.0
        if volatility_cap_pct is not None and volatility_cap_pct > 0:
            risk_score = _score_inverse(risk_unit_pct, volatility_cap_pct, max(volatility_cap_pct * 1.6, volatility_cap_pct + 1.0)) * 100.0
        volume_score = _score_ratio(volume_ratio, 1.8, 0.8) * 100.0
        close_position_score = (dcr or 0.5) * 100.0
        return _weighted_mean(
            [
                (risk_score, 0.45),
                (volume_score, 0.25),
                (close_position_score, 0.30),
            ]
        )

    def _candidate_data_flags(
        self,
        feature_row: Mapping[str, Any],
        *,
        has_event_data: bool,
        has_estimate_data: bool,
        needs_review: bool,
    ) -> list[str]:
        flags = ["HAS_DAILY"]
        flags.append("HAS_INTRADAY" if _safe_bool(feature_row.get("has_intraday_bars")) else "NO_INTRADAY")
        flags.append("HAS_SECTOR_MAPPING" if _safe_bool(feature_row.get("has_sector_mapping")) else "NO_SECTOR_MAPPING")
        flags.append("HAS_FUNDAMENTALS" if _safe_bool(feature_row.get("has_fundamentals")) else "NO_FUNDAMENTALS")
        flags.append("HAS_EVENT_DATA" if has_event_data else "NO_EVENT_DATA")
        flags.append("HAS_ESTIMATE_DATA" if has_estimate_data else "NO_ESTIMATE_DATA")
        if needs_review:
            flags.append("NEEDS_REVIEW")
        return _unique(flags)

    def analyze_breakout(
        self,
        symbol: str,
        daily_frame: pd.DataFrame,
        *,
        market: str = "us",
        feature_row: Mapping[str, Any] | None = None,
        regime: MarketRegime | None = None,
        calibration: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if feature_row is None:
            single = pd.DataFrame([self.compute_feature_row(symbol, market, daily_frame)])
            feature_row = row_to_record(self.finalize_feature_table(single).iloc[0])
        else:
            feature_row = dict(feature_row)

        regime_ctx = regime or self._default_regime(market)
        profile = self.market_profile(market)
        calibration_map = dict(calibration or {})
        a_pp_score = _safe_float(feature_row.get("a_pp_score")) or 0.0
        stock_grade = str(feature_row.get("stock_grade") or self.stock_grade(a_pp_score))
        adr20_pct = _safe_float(feature_row.get("adr20_pct")) or _safe_float(feature_row.get("atr20_pct")) or 4.0
        risk_unit_pct = _safe_float(feature_row.get("risk_unit_pct"))
        volume_ratio = _safe_float(feature_row.get("rvol"))
        dcr = _safe_float(feature_row.get("dcr")) or 0.5
        breakout_volume_confirmation_min = _safe_float(calibration_map.get("breakout_volume_confirmation_min")) or 1.5
        breakout_volume_strong_min = _safe_float(calibration_map.get("breakout_volume_strong_min")) or breakout_volume_confirmation_min
        breakout_setup_pass_score = _safe_float(calibration_map.get("breakout_setup_pass_score")) or 70.0
        breakout_setup_five_star_score = _safe_float(calibration_map.get("breakout_setup_five_star_score")) or 90.0
        universe_ready = _safe_bool(feature_row.get("breakout_universe_pass"))

        prior_run_pass = (_safe_float(feature_row.get("prior_run_pct")) or 0.0) >= (_safe_float(calibration_map.get("breakout_min_prior_run_pct")) or 0.30)
        base_length = _safe_float(feature_row.get("base_length"))
        base_length_pass = (
            base_length is not None
            and (_safe_float(calibration_map.get("breakout_min_base_length")) or 5.0) <= base_length
            and base_length <= (_safe_float(calibration_map.get("breakout_max_base_length")) or 60.0)
        )
        compression_pass = (_safe_float(feature_row.get("compression_score")) or 0.0) >= (_safe_float(calibration_map.get("breakout_min_compression_score")) or 65.0)
        high_proximity_pass = (_safe_float(feature_row.get("high_52w_proximity")) or 0.0) >= (_safe_float(calibration_map.get("breakout_min_high_52w_proximity")) or 0.85)
        risk_pass = risk_unit_pct is not None and risk_unit_pct <= adr20_pct
        too_extended = ((_safe_float(feature_row.get("pivot_distance_pct")) or -10.0) > 5.0) or (
            (_safe_float(feature_row.get("close")) or 0.0) > ((_safe_float(feature_row.get("sma10")) or 0.0) * 1.12)
            if _safe_float(feature_row.get("sma10")) is not None
            else False
        )

        intraday_confirmation_score = _weighted_mean(
            [
                ((_score_ratio(volume_ratio, breakout_volume_strong_min, max(0.8, breakout_volume_confirmation_min * 0.6)) * 100.0), 0.50),
                ((dcr * 100.0), 0.30),
                ((100.0 if (_safe_float(feature_row.get("pivot_distance_pct")) or -10.0) >= 0.0 else 60.0), 0.20),
            ]
        )
        regime_alignment_score = self._regime_alignment_score(regime_ctx)
        breakout_setup_score = _weighted_mean(
            [
                (a_pp_score, 0.30),
                (_safe_float(feature_row.get("compression_score")) or 0.0, 0.25),
                (_safe_float(feature_row.get("readiness_score")) or 0.0, 0.20),
                (intraday_confirmation_score, 0.15),
                (regime_alignment_score, 0.10),
            ]
        )
        execution_quality_score = self._execution_quality(
            risk_unit_pct=risk_unit_pct,
            volatility_cap_pct=adr20_pct,
            volume_ratio=volume_ratio,
            dcr=dcr,
        )
        data_confidence_score = self.data_confidence_score(
            has_daily_bars=_safe_bool(feature_row.get("has_daily_bars")),
            has_intraday_bars=_safe_bool(feature_row.get("has_intraday_bars")),
            has_corp_actions=_safe_bool(feature_row.get("has_corp_actions")),
            has_sector_mapping=_safe_bool(feature_row.get("has_sector_mapping")),
            has_event_data=False,
            has_fundamentals=_safe_bool(feature_row.get("has_fundamentals")),
            has_estimate_data=False,
        )
        final_priority_score = (
            0.35 * breakout_setup_score
            + 0.25 * a_pp_score
            + 0.20 * regime_ctx.regime_score
            + 0.10 * execution_quality_score
            + 0.10 * data_confidence_score
        )

        regime_supportive = regime_ctx.regime_state in {"RISK_ON", "RISK_ON_AGGRESSIVE"}
        five_star = bool(
            universe_ready
            and stock_grade == "A++"
            and breakout_setup_score >= breakout_setup_five_star_score
            and risk_pass
            and regime_supportive
            and not too_extended
        )
        passed = bool(
            universe_ready
            and prior_run_pass
            and base_length_pass
            and compression_pass
            and high_proximity_pass
            and risk_pass
            and breakout_setup_score >= breakout_setup_pass_score
        )
        tier = "Tier 1" if five_star else "Tier 2" if passed and stock_grade == "A++" else "Tier 3"
        stage = self._candidate_stage(
            tier=tier,
            setup_family="BREAKOUT",
            passed=passed,
            universe_ready=universe_ready,
        )

        reasons: list[str] = []
        if (_safe_float(feature_row.get("ret_3m_pctile")) or 0.0) >= 80.0:
            reasons.append("TOP_RS_3M")
        if (_safe_float(feature_row.get("ret_6m_pctile")) or 0.0) >= 80.0:
            reasons.append("TOP_RS_6M")
        if (_safe_float(feature_row.get("high_52w_proximity")) or 0.0) >= 0.95:
            reasons.append("NEAR_52W_HIGH")
        if (_safe_float(feature_row.get("compression_score")) or 0.0) >= max((_safe_float(calibration_map.get("breakout_min_compression_score")) or 65.0) + 8.0, 75.0):
            reasons.append("TIGHT_BASE")
        if (_safe_float(feature_row.get("higher_low_score")) or 0.0) >= 70.0:
            reasons.append("HIGHER_LOWS")
        if risk_pass:
            reasons.append("RISK_WITHIN_ADR")
        if regime_supportive:
            reasons.append("REGIME_SUPPORTIVE")
        if (_safe_float(feature_row.get("pivot_distance_pct")) or -10.0) >= 0.0:
            reasons.append("PIVOT_CLEARED")
        if (volume_ratio or 0.0) >= breakout_volume_confirmation_min:
            reasons.append("VOLUME_CONFIRMATION")
        if passed and len(reasons) < 3:
            reasons.extend(["A_PP_LEADER", "OHLCV_VALIDATED"])

        fail_codes: list[str] = []
        if not universe_ready:
            fail_codes.append("OUTSIDE_BREAKOUT_UNIVERSE")
        if not prior_run_pass:
            fail_codes.append("PRIOR_RUN_TOO_WEAK")
        if not base_length_pass:
            fail_codes.append("BASE_NOT_READY")
        if not compression_pass:
            fail_codes.append("LOOSE_BASE")
        if not high_proximity_pass:
            fail_codes.append("FAR_FROM_52W_HIGH")
        if not risk_pass:
            fail_codes.append("STOP_TOO_WIDE")
        if too_extended:
            fail_codes.append("TOO_EXTENDED")
        if not regime_supportive:
            fail_codes.append("REGIME_HEADWIND")

        needs_review = data_confidence_score < 55.0
        data_flags = self._candidate_data_flags(
            feature_row,
            has_event_data=False,
            has_estimate_data=False,
            needs_review=needs_review,
        )

        entry_timeframe = profile.orh_windows[0]
        target_price = (_safe_float(feature_row.get("pivot_price")) or 0.0) * 1.10
        entry_price = max(_safe_float(feature_row.get("close")) or 0.0, _safe_float(feature_row.get("pivot_price")) or 0.0)
        stop_price = _safe_float(feature_row.get("stop_price"))
        risk_reward_ratio = _round_or_none((target_price - entry_price) / max(entry_price - (stop_price or 0.0), 1e-9))
        return {
            "as_of_ts": feature_row.get("as_of_ts"),
            "symbol": str(symbol).upper(),
            "market": profile.market_code,
            "market_code": profile.market_code,
            "setup_family": "BREAKOUT",
            "candidate_stage": stage,
            "priority_tier": tier,
            "stock_grade": stock_grade,
            "setup_grade": self.setup_grade(breakout_setup_score, five_star=five_star),
            "a_pp_score": _round_or_none(a_pp_score),
            "setup_score": _round_or_none(breakout_setup_score),
            "score": _round_or_none(breakout_setup_score),
            "final_priority_score": _round_or_none(final_priority_score),
            "regime_state": regime_ctx.regime_state,
            "market_alias": regime_ctx.market_alias,
            "market_alignment_score": _round_or_none(regime_ctx.market_alignment_score),
            "breadth_support_score": _round_or_none(regime_ctx.breadth_support_score),
            "rotation_support_score": _round_or_none(regime_ctx.rotation_support_score),
            "leader_health_score": _round_or_none(regime_ctx.leader_health_score),
            "reason_codes": _unique(reasons),
            "fail_codes": _unique(fail_codes),
            "data_flags": data_flags,
            "data_confidence_score": _round_or_none(data_confidence_score),
            "pivot_price": _round_or_none(_safe_float(feature_row.get("pivot_price"))),
            "stop_price": _round_or_none(stop_price),
            "risk_unit_pct": _round_or_none(risk_unit_pct),
            "entry_timeframe": entry_timeframe,
            "suggested_entry": _round_or_none(entry_price),
            "suggested_stop": _round_or_none(stop_price),
            "current_price": _round_or_none(_safe_float(feature_row.get("close"))),
            "volume_ratio": _round_or_none(volume_ratio),
            "adr": _round_or_none(adr20_pct),
            "vcp_pattern": self.check_vcp_pattern(daily_frame, market=market, calibration=calibration_map, feature_row=feature_row),
            "breakout_level": _round_or_none(_safe_float(feature_row.get("pivot_price"))),
            "stop_loss": _round_or_none(stop_price),
            "risk_reward_ratio": risk_reward_ratio,
            "passed": passed,
            "scores": {
                "a_pp_score": _round_or_none(a_pp_score),
                "setup_score": _round_or_none(breakout_setup_score),
                "final_priority_score": _round_or_none(final_priority_score),
                "data_confidence_score": _round_or_none(data_confidence_score),
            },
            "execution": {
                "entry_timeframe": entry_timeframe,
                "pivot_price": _round_or_none(_safe_float(feature_row.get("pivot_price"))),
                "stop_price": _round_or_none(stop_price),
                "risk_unit_pct": _round_or_none(risk_unit_pct),
            },
            "metrics": {
                "prior_run_pct": _round_or_none(_safe_float(feature_row.get("prior_run_pct"))),
                "base_length": feature_row.get("base_length"),
                "compression_score": _round_or_none(_safe_float(feature_row.get("compression_score"))),
                "readiness_score": _round_or_none(_safe_float(feature_row.get("readiness_score"))),
                "intraday_confirmation_score": _round_or_none(intraday_confirmation_score),
                "execution_quality_score": _round_or_none(execution_quality_score),
                "pivot_distance_pct": _round_or_none(_safe_float(feature_row.get("pivot_distance_pct"))),
                "high_52w_proximity": _round_or_none(_safe_float(feature_row.get("high_52w_proximity"))),
            },
        }

    def analyze_episode_pivot(
        self,
        symbol: str,
        daily_frame: pd.DataFrame,
        enable_earnings_filter: bool = True,
        *,
        market: str = "us",
        feature_row: Mapping[str, Any] | None = None,
        regime: MarketRegime | None = None,
        earnings_collector: SupportsEarningsCollector | None = None,
        earnings_payload: Mapping[str, Any] | None = None,
        calibration: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if feature_row is None:
            single = pd.DataFrame([self.compute_feature_row(symbol, market, daily_frame)])
            feature_row = row_to_record(self.finalize_feature_table(single).iloc[0])
        else:
            feature_row = dict(feature_row)

        daily = self.normalize_daily_frame(daily_frame)
        regime_ctx = regime or self._default_regime(market)
        profile = self.market_profile(market)
        calibration_map = dict(calibration or {})

        gap_pct = _safe_float(feature_row.get("gap_pct")) or 0.0
        volume_ratio = _safe_float(feature_row.get("rvol")) or 0.0
        adr20_pct = _safe_float(feature_row.get("adr20_pct")) or _safe_float(feature_row.get("atr20_pct")) or 4.0
        dcr = _safe_float(feature_row.get("dcr")) or 0.5
        close = _safe_float(feature_row.get("close")) or 0.0
        open_price = _safe_float(feature_row.get("open")) or close
        prev_high = _safe_float(daily["high"].iloc[-2]) if len(daily) >= 2 else None
        stop_price = prev_high
        entry_price = max(close, open_price, prev_high or 0.0)
        risk_unit_pct = None
        if stop_price is not None and entry_price > 0:
            risk_unit_pct = ((entry_price - stop_price) / entry_price) * 100.0

        ep_core_volume_min = _safe_float(calibration_map.get("ep_core_volume_min")) or (3.0 if market == "us" else 2.5)
        ep_watch_volume_min = _safe_float(calibration_map.get("ep_watch_volume_min")) or 2.0
        ep_min_gap_pct = _safe_float(calibration_map.get("ep_min_gap_pct")) or 0.10
        ep_min_neglected_base_score = _safe_float(calibration_map.get("ep_min_neglected_base_score")) or 55.0
        ep_setup_pass_score = _safe_float(calibration_map.get("ep_setup_pass_score")) or 70.0
        ep_setup_five_star_score = _safe_float(calibration_map.get("ep_setup_five_star_score")) or 90.0
        universe_ready = _safe_bool(feature_row.get("ep_universe_pass"))

        neglected_base_score = _safe_float(feature_row.get("neglected_base_score")) or 0.0
        technical_event_prefilter = bool(
            gap_pct >= ep_min_gap_pct
            and volume_ratio >= ep_watch_volume_min
            and dcr >= 0.65
            and close >= open_price
            and (_safe_bool(feature_row.get("no_excessive_run")) and neglected_base_score >= ep_min_neglected_base_score)
        )
        earnings_fetch_skipped = False
        if earnings_payload is None and enable_earnings_filter and technical_event_prefilter:
            collector = earnings_collector or EarningsDataCollector(market=market)
            earnings_payload = collector.get_earnings_surprise(symbol)
        elif earnings_payload is None and enable_earnings_filter:
            earnings_fetch_skipped = True

        earnings_payload_map = dict(earnings_payload or {})
        earnings_fetch_status = str(earnings_payload_map.get("fetch_status") or "").strip().lower()
        earnings_data_usable = bool(earnings_payload_map) and (
            earnings_fetch_status in {"", "complete"}
            or str(earnings_payload_map.get("data_quality") or "").strip().lower() == "stale_cache"
        )
        has_event_data = bool(earnings_data_usable)
        has_estimate_data = bool(earnings_data_usable and earnings_payload_map.get("eps_estimate") is not None)
        event_meets = bool(earnings_data_usable and earnings_payload_map.get("meets_criteria"))
        eps_surprise_pct = _safe_float(earnings_payload_map.get("eps_surprise_pct")) if earnings_data_usable else None
        revenue_surprise_pct = _safe_float(earnings_payload_map.get("revenue_surprise_pct")) if earnings_data_usable else None
        yoy_eps_growth = _safe_float(earnings_payload_map.get("yoy_eps_growth")) if earnings_data_usable else None
        yoy_revenue_growth = _safe_float(earnings_payload_map.get("yoy_revenue_growth")) if earnings_data_usable else None

        volume_shock_score = max(
            _score_ratio(volume_ratio, ep_core_volume_min, 1.0) * 100.0,
            _score_ratio(volume_ratio, 4.5, 1.5) * 90.0,
        )
        event_intensity_score = 35.0
        earnings_quality_score = 35.0
        if earnings_data_usable:
            event_intensity_score = _weighted_mean(
                [
                    (100.0 if event_meets else 55.0, 0.35),
                    ((_score_ratio(eps_surprise_pct, 20.0, 0.0) * 100.0), 0.25),
                    ((_score_ratio(yoy_eps_growth, 100.0, 10.0) * 100.0), 0.25),
                    ((_score_ratio(revenue_surprise_pct, 20.0, -5.0) * 100.0), 0.15),
                ]
            )
            earnings_quality_score = _weighted_mean(
                [
                    ((_score_ratio(eps_surprise_pct, 20.0, 0.0) * 100.0), 0.35),
                    ((_score_ratio(yoy_eps_growth, 100.0, 10.0) * 100.0), 0.35),
                    ((_score_ratio(yoy_revenue_growth, 20.0, 0.0) * 100.0), 0.15),
                    ((_score_ratio(revenue_surprise_pct, 15.0, -5.0) * 100.0), 0.15),
                ]
            )
        elif not enable_earnings_filter:
            event_intensity_score = 40.0
            earnings_quality_score = 40.0

        regime_alignment_score = self._regime_alignment_score(regime_ctx)
        ep_setup_score = _weighted_mean(
            [
                (volume_shock_score, 0.30),
                (event_intensity_score, 0.25),
                (earnings_quality_score, 0.20),
                (neglected_base_score, 0.15),
                (regime_alignment_score, 0.10),
            ]
        )
        stop_width_pass = risk_unit_pct is not None and risk_unit_pct <= adr20_pct * 1.5
        gap_pass = gap_pct >= ep_min_gap_pct
        volume_pass_core = volume_ratio >= ep_core_volume_min
        volume_pass_watch = volume_ratio >= ep_watch_volume_min
        neglected_base_pass = (_safe_bool(feature_row.get("no_excessive_run")) and neglected_base_score >= ep_min_neglected_base_score)
        ep_core = gap_pass and volume_pass_core and event_meets and neglected_base_pass
        ep_price_volume = gap_pass and volume_pass_watch and dcr >= 0.65 and close >= open_price and neglected_base_pass
        regime_supportive = regime_ctx.regime_state in {"RISK_ON", "RISK_ON_AGGRESSIVE"}
        five_star = bool(
            universe_ready
            and ep_core
            and ep_setup_score >= ep_setup_five_star_score
            and stop_width_pass
            and regime_supportive
        )
        passed = bool(
            universe_ready
            and (ep_core or (ep_price_volume and ep_setup_score >= ep_setup_pass_score))
        )
        execution_quality_score = self._execution_quality(
            risk_unit_pct=risk_unit_pct,
            volatility_cap_pct=adr20_pct * 1.5,
            volume_ratio=volume_ratio,
            dcr=dcr,
        )
        data_confidence_score = self.data_confidence_score(
            has_daily_bars=_safe_bool(feature_row.get("has_daily_bars")),
            has_intraday_bars=_safe_bool(feature_row.get("has_intraday_bars")),
            has_corp_actions=_safe_bool(feature_row.get("has_corp_actions")),
            has_sector_mapping=_safe_bool(feature_row.get("has_sector_mapping")),
            has_event_data=has_event_data,
            has_fundamentals=_safe_bool(feature_row.get("has_fundamentals")),
            has_estimate_data=has_estimate_data,
        )
        final_priority_score = (
            0.35 * ep_setup_score
            + 0.25 * (_safe_float(feature_row.get("a_pp_score")) or 0.0)
            + 0.20 * regime_ctx.regime_score
            + 0.10 * execution_quality_score
            + 0.10 * data_confidence_score
        )

        tier = "Tier 1" if five_star else "Tier 2" if (ep_core or ep_price_volume) else "Tier 3"
        stage = self._candidate_stage(
            tier=tier,
            setup_family="EP",
            passed=passed,
            universe_ready=universe_ready,
        )

        reasons: list[str] = []
        if gap_pass:
            reasons.append("GAP_CONFIRMED")
        if volume_pass_watch:
            reasons.append("VOLUME_SHOCK")
        if dcr >= 0.70:
            reasons.append("HIGH_DCR")
        if neglected_base_pass:
            reasons.append("NEGLECTED_BASE")
        if event_meets:
            reasons.append("EARNINGS_CATALYST")
        elif has_event_data:
            reasons.append("EVENT_PROXY_PRESENT")
        if stop_width_pass:
            reasons.append("STOP_WITHIN_1_5X_ADR")
        if regime_supportive:
            reasons.append("REGIME_SUPPORTIVE")
        if passed and len(reasons) < 3:
            reasons.extend(["PRICE_VOLUME_REVALUATION", "OHLCV_VALIDATED"])

        fail_codes: list[str] = []
        if not universe_ready:
            fail_codes.append("OUTSIDE_EP_UNIVERSE")
        if not gap_pass:
            fail_codes.append("NO_GAP_CONFIRMATION")
        if not volume_pass_watch:
            fail_codes.append("LOW_VOLUME_SHOCK")
        if enable_earnings_filter and not event_meets and not ep_price_volume:
            fail_codes.append("NO_EVENT_CONFIRMATION")
        if not neglected_base_pass:
            fail_codes.append("TOO_EXTENDED")
        if not stop_width_pass:
            fail_codes.append("STOP_TOO_WIDE")
        if not regime_supportive:
            fail_codes.append("REGIME_HEADWIND")

        needs_review = data_confidence_score < 60.0 or (ep_price_volume and not has_event_data)
        data_flags = self._candidate_data_flags(
            feature_row,
            has_event_data=has_event_data,
            has_estimate_data=has_estimate_data,
            needs_review=needs_review,
        )
        if earnings_fetch_skipped:
            data_flags.append("EVENT_FETCH_SKIPPED_TECHNICAL_PREFILTER")
        elif earnings_fetch_status == "rate_limited":
            data_flags.append("EARNINGS_RATE_LIMITED")
        elif earnings_fetch_status == "soft_unavailable":
            data_flags.append("EARNINGS_SOFT_UNAVAILABLE")
        elif earnings_fetch_status == "delisted":
            data_flags.append("EARNINGS_DELISTED")
        elif earnings_fetch_status == "failed":
            data_flags.append("EARNINGS_FETCH_FAILED")
        data_flags.append("EARNINGS_FILTER_ENABLED" if enable_earnings_filter else "EVENT_FILTER_DISABLED")
        data_flags = _unique(data_flags)

        a_pp_score = _safe_float(feature_row.get("a_pp_score")) or 0.0
        stock_grade = str(feature_row.get("stock_grade") or self.stock_grade(a_pp_score))
        setup_grade = self.setup_grade(ep_setup_score, five_star=five_star, watch=not ep_core)
        entry_timeframe = profile.ep_orh_windows[0]
        target_price = entry_price * 1.10
        return {
            "as_of_ts": feature_row.get("as_of_ts"),
            "symbol": str(symbol).upper(),
            "market": profile.market_code,
            "market_code": profile.market_code,
            "setup_family": "EP",
            "ep_type": "EP_CORE" if ep_core else "EP_PRICE_VOLUME" if ep_price_volume else "NONE",
            "candidate_stage": stage,
            "priority_tier": tier,
            "stock_grade": stock_grade,
            "setup_grade": setup_grade,
            "a_pp_score": _round_or_none(a_pp_score),
            "setup_score": _round_or_none(ep_setup_score),
            "score": _round_or_none(ep_setup_score),
            "final_priority_score": _round_or_none(final_priority_score),
            "regime_state": regime_ctx.regime_state,
            "market_alias": regime_ctx.market_alias,
            "market_alignment_score": _round_or_none(regime_ctx.market_alignment_score),
            "breadth_support_score": _round_or_none(regime_ctx.breadth_support_score),
            "rotation_support_score": _round_or_none(regime_ctx.rotation_support_score),
            "leader_health_score": _round_or_none(regime_ctx.leader_health_score),
            "reason_codes": _unique(reasons),
            "fail_codes": _unique(fail_codes),
            "data_flags": data_flags,
            "data_confidence_score": _round_or_none(data_confidence_score),
            "pivot_price": _round_or_none(entry_price),
            "stop_price": _round_or_none(stop_price),
            "risk_unit_pct": _round_or_none(risk_unit_pct),
            "entry_timeframe": entry_timeframe,
            "suggested_entry": _round_or_none(entry_price),
            "suggested_stop": _round_or_none(stop_price),
            "current_price": _round_or_none(close),
            "gap_percent": _round_or_none(gap_pct * 100.0),
            "volume_ratio": _round_or_none(volume_ratio),
            "ma50_relation": "Above" if _safe_bool(feature_row.get("close_gt_50dma")) else "Below",
            "earnings_surprise": bool(has_event_data) if enable_earnings_filter else None,
            "earnings_fetch_status": earnings_fetch_status or ("complete" if earnings_data_usable else ""),
            "earnings_unavailable_reason": earnings_payload_map.get("unavailable_reason"),
            "eps_surprise_pct": _round_or_none(eps_surprise_pct),
            "revenue_surprise_pct": _round_or_none(revenue_surprise_pct),
            "yoy_eps_growth": _round_or_none(yoy_eps_growth),
            "yoy_revenue_growth": _round_or_none(yoy_revenue_growth),
            "stop_loss": _round_or_none(stop_price),
            "risk_reward_ratio": _round_or_none((target_price - entry_price) / max(entry_price - (stop_price or 0.0), 1e-9)),
            "passed": passed,
            "scores": {
                "a_pp_score": _round_or_none(a_pp_score),
                "setup_score": _round_or_none(ep_setup_score),
                "final_priority_score": _round_or_none(final_priority_score),
                "data_confidence_score": _round_or_none(data_confidence_score),
            },
            "execution": {
                "entry_timeframe": entry_timeframe,
                "pivot_price": _round_or_none(entry_price),
                "stop_price": _round_or_none(stop_price),
                "risk_unit_pct": _round_or_none(risk_unit_pct),
            },
            "metrics": {
                "volume_shock_score": _round_or_none(volume_shock_score),
                "event_intensity_score": _round_or_none(event_intensity_score),
                "earnings_quality_score": _round_or_none(earnings_quality_score),
                "neglected_base_score": _round_or_none(neglected_base_score),
                "execution_quality_score": _round_or_none(execution_quality_score),
                "gap_pct": _round_or_none(gap_pct),
                "volume_ratio": _round_or_none(volume_ratio),
            },
        }

    def analyze_parabolic_short(
        self,
        symbol: str,
        daily_frame: pd.DataFrame,
        *,
        market: str = "us",
        feature_row: Mapping[str, Any] | None = None,
        regime: MarketRegime | None = None,
    ) -> dict[str, Any]:
        if feature_row is None:
            single = pd.DataFrame([self.compute_feature_row(symbol, market, daily_frame)])
            feature_row = row_to_record(self.finalize_feature_table(single).iloc[0])
        else:
            feature_row = dict(feature_row)

        daily = self.normalize_daily_frame(daily_frame)
        latest = daily.iloc[-1] if not daily.empty else None
        regime_ctx = regime or self._default_regime(market)

        consecutive_up_days = 0
        if len(daily) >= 2:
            for change in daily["close"].pct_change().iloc[::-1]:
                if pd.isna(change) or change <= 0:
                    break
                consecutive_up_days += 1

        first_down_candle = bool(
            latest is not None
            and _safe_float(latest["close"]) is not None
            and _safe_float(latest["open"]) is not None
            and latest["close"] < latest["open"]
        )
        deviation_from_sma20 = None
        close_value = _safe_float(feature_row.get("close"))
        sma20_value = _safe_float(feature_row.get("sma20"))
        close_to_sma20 = _safe_divide(close_value, sma20_value)
        if close_to_sma20 is not None:
            deviation_from_sma20 = (close_to_sma20 - 1.0) * 100.0

        short_score = _weighted_mean(
            [
                ((_score_ratio(_safe_float(feature_row.get("ret_1m")), 0.25, 0.05) * 100.0), 0.25),
                ((_score_ratio(_safe_float(feature_row.get("ret_3m")), 0.80, 0.20) * 100.0), 0.25),
                ((_score_ratio(deviation_from_sma20, 18.0, 5.0) * 100.0), 0.25),
                ((100.0 if first_down_candle else 20.0), 0.15),
                ((_score_ratio(float(consecutive_up_days), 4.0, 1.0) * 100.0), 0.10),
            ]
        )
        passed = bool(short_score >= 75.0 and first_down_candle and consecutive_up_days >= 4)
        return {
            "symbol": str(symbol).upper(),
            "market": self.market_profile(market).market_code,
            "setup_family": "PARABOLIC_SHORT",
            "candidate_stage": "LEGACY_WATCH",
            "stock_grade": str(feature_row.get("stock_grade") or self.stock_grade(feature_row.get("a_pp_score"))),
            "setup_grade": self.setup_grade(short_score, watch=not passed),
            "regime_state": regime_ctx.regime_state,
            "market_alias": regime_ctx.market_alias,
            "market_alignment_score": _round_or_none(regime_ctx.market_alignment_score),
            "breadth_support_score": _round_or_none(regime_ctx.breadth_support_score),
            "rotation_support_score": _round_or_none(regime_ctx.rotation_support_score),
            "leader_health_score": _round_or_none(regime_ctx.leader_health_score),
            "current_price": _round_or_none(_safe_float(feature_row.get("close"))),
            "short_term_rise": _round_or_none((_safe_float(feature_row.get("ret_1m")) or 0.0) * 100.0),
            "consecutive_up_days": consecutive_up_days,
            "volume_ratio": _round_or_none(_safe_float(feature_row.get("rvol"))),
            "rsi14": None,
            "ma20_deviation": _round_or_none(deviation_from_sma20),
            "first_down_candle": first_down_candle,
            "stop_loss": _round_or_none(_safe_float(feature_row.get("high"))),
            "risk_reward_ratio": None,
            "reason_codes": _unique(["OVEREXTENDED_RUN", "FIRST_DOWN_DAY"] if passed else []),
            "fail_codes": _unique([] if passed else ["NO_PARABOLIC_REVERSAL"]),
            "data_flags": _unique(["HAS_DAILY", "LEGACY_MODULE"]),
            "data_confidence_score": _round_or_none(
                self.data_confidence_score(
                    has_daily_bars=True,
                    has_intraday_bars=False,
                    has_corp_actions=False,
                    has_sector_mapping=_safe_bool(feature_row.get("has_sector_mapping")),
                    has_event_data=False,
                    has_fundamentals=_safe_bool(feature_row.get("has_fundamentals")),
                    has_estimate_data=False,
                )
            ),
            "score": _round_or_none(short_score),
            "passed": passed,
        }
