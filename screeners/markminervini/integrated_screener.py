from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yfinance as yf

from utils.actual_data_calibration import bounded_quantile_value
from utils.indicator_helpers import normalize_indicator_frame
from utils.io_utils import safe_filename
from utils.market_data_contract import PricePolicy, normalize_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_markminervini_advanced_financial_results_path,
    get_markminervini_integrated_pattern_results_path,
    get_markminervini_integrated_results_path,
    get_markminervini_results_dir,
    get_markminervini_with_rs_path,
    iter_provider_symbols,
    market_key,
)
from utils.progress_runtime import is_progress_tick, progress_interval
from .enhanced_pattern_analyzer import EnhancedPatternAnalyzer

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(casted):
        return None
    return casted


def _numeric_frame_series(
    frame: pd.DataFrame,
    primary: str,
    fallback: str | None = None,
    *,
    fill_value: float | None = None,
) -> pd.Series:
    if primary in frame.columns:
        source = frame[primary]
    elif fallback is not None and fallback in frame.columns:
        source = frame[fallback]
    else:
        default_value = fill_value if fill_value is not None else float("nan")
        return pd.Series(default_value, index=frame.index, dtype=float)

    numeric = pd.to_numeric(source, errors="coerce")
    if fill_value is not None:
        numeric = numeric.fillna(fill_value)
    return numeric


class IntegratedScreener:
    def __init__(self, market: str = "us"):
        self.market = market_key(market)
        ensure_market_dirs(self.market)
        self.results_dir = get_markminervini_results_dir(self.market)
        self.with_rs_path = get_markminervini_with_rs_path(self.market)
        self.advanced_path = get_markminervini_advanced_financial_results_path(self.market)
        self.integrated_path = get_markminervini_integrated_results_path(self.market)
        self.pattern_results_csv = get_markminervini_integrated_pattern_results_path(self.market)
        self.pattern_results_json = self.pattern_results_csv.replace(".csv", ".json")
        self.patternless_results_csv = os.path.join(self.results_dir, "integrated_without_patterns.csv")
        self.patternless_results_json = self.patternless_results_csv.replace(".csv", ".json")
        self.pre_pattern_quant_financial_csv = os.path.join(self.results_dir, "pre_pattern_quant_financial_candidates.csv")
        self.pre_pattern_quant_financial_json = self.pre_pattern_quant_financial_csv.replace(".csv", ".json")
        self.pattern_enriched_csv = os.path.join(self.results_dir, "integrated_with_patterns.csv")
        self.pattern_enriched_json = self.pattern_enriched_csv.replace(".csv", ".json")
        self.pattern_actionable_csv = os.path.join(self.results_dir, "integrated_actionable_patterns.csv")
        self.pattern_actionable_json = self.pattern_actionable_csv.replace(".csv", ".json")
        self.actual_data_calibration_json = os.path.join(self.results_dir, "actual_data_pattern_calibration.json")
        self.pattern_analyzer = EnhancedPatternAnalyzer()

    @staticmethod
    def _write_frame(frame: pd.DataFrame, csv_path: str, json_path: str, *, include_snapshot: bool = True) -> None:
        frame.to_csv(csv_path, index=False)
        frame.to_json(json_path, orient="records", indent=2, force_ascii=False)

        if include_snapshot:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            csv_target = Path(csv_path)
            json_target = Path(json_path)
            snapshot_csv_path = csv_target.with_name(f"{csv_target.stem}_{timestamp}{csv_target.suffix}")
            snapshot_json_path = json_target.with_name(f"{json_target.stem}_{timestamp}{json_target.suffix}")
            frame.to_csv(snapshot_csv_path, index=False)
            frame.to_json(snapshot_json_path, orient="records", indent=2, force_ascii=False)

    @staticmethod
    def _pattern_stage_summary(row: pd.Series) -> str:
        buckets = {
            str(row.get("vcp_state_bucket") or ""),
            str(row.get("cup_handle_state_bucket") or ""),
        }
        if "BROKEOUT_RECENT" in buckets:
            return "RECENT_BREAKOUT"
        if "COMPLETED" in buckets:
            return "COMPLETED"
        if "FORMING" in buckets:
            return "FORMING"
        if "STALE" in buckets:
            return "STALE"
        if "FAILED" in buckets:
            return "FAILED"
        return "NONE"

    def _annotate_pattern_coverage(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        annotated = frame.copy()
        vcp_bucket = annotated["vcp_state_bucket"].fillna("").astype(str)
        cup_handle_bucket = annotated["cup_handle_state_bucket"].fillna("").astype(str)
        annotated["has_forming_pattern"] = (vcp_bucket == "FORMING") | (cup_handle_bucket == "FORMING")
        annotated["has_completed_pattern"] = (vcp_bucket == "COMPLETED") | (cup_handle_bucket == "COMPLETED")
        annotated["has_recent_breakout_pattern"] = (vcp_bucket == "BROKEOUT_RECENT") | (cup_handle_bucket == "BROKEOUT_RECENT")
        annotated["has_failed_pattern"] = (vcp_bucket == "FAILED") | (cup_handle_bucket == "FAILED")
        annotated["pattern_included"] = (
            annotated["has_forming_pattern"]
            | annotated["has_completed_pattern"]
            | annotated["has_recent_breakout_pattern"]
        )
        annotated["pattern_stage_summary"] = annotated.apply(self._pattern_stage_summary, axis=1)
        return annotated

    @staticmethod
    def _percentile_score(series: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.dropna().empty:
            return pd.Series(50.0, index=series.index, dtype=float)
        if numeric.nunique(dropna=True) <= 1:
            return pd.Series(100.0, index=series.index, dtype=float)
        return numeric.rank(pct=True, method="average").fillna(0.5) * 100.0

    @staticmethod
    def _score_ratio(value: float | None, good_min: float, bad_min: float) -> float:
        if value is None:
            return 0.0
        if value >= good_min:
            return 1.0
        if value <= bad_min:
            return 0.0
        return float((value - bad_min) / max(good_min - bad_min, 1e-9))

    @staticmethod
    def _score_inverse(value: float | None, good_max: float, bad_max: float) -> float:
        if value is None:
            return 0.0
        if value <= good_max:
            return 1.0
        if value >= bad_max:
            return 0.0
        return float((bad_max - value) / max(bad_max - good_max, 1e-9))

    @staticmethod
    def _weighted_score(pairs: list[tuple[float | None, float]]) -> float:
        total = 0.0
        weight_sum = 0.0
        for value, weight in pairs:
            if value is None:
                continue
            total += float(value) * float(weight)
            weight_sum += float(weight)
        if weight_sum <= 0:
            return 0.0
        return total / weight_sum

    @staticmethod
    def _pattern_payload(row: pd.Series, prefix: str) -> tuple[dict[str, Any], dict[str, Any]]:
        dims = row.get(f"{prefix}_dimensional_scores")
        metrics = row.get(f"{prefix}_metrics")
        return (
            dims if isinstance(dims, dict) else {},
            metrics if isinstance(metrics, dict) else {},
        )

    def _best_pattern_prefix(self, row: pd.Series) -> str:
        vcp_conf = _safe_float(row.get("vcp_confidence")) or 0.0
        cup_conf = _safe_float(row.get("cup_handle_confidence")) or 0.0
        return "cup_handle" if cup_conf > vcp_conf else "vcp"

    def _compute_pattern_priority_components(self, row: pd.Series) -> dict[str, float]:
        prefix = self._best_pattern_prefix(row)
        dimensional_scores, metrics = self._pattern_payload(row, prefix)
        max_confidence = _safe_float(row.get("max_pattern_confidence")) or 0.0
        technical_quality = _safe_float(dimensional_scores.get("technical_quality"))
        volume_confirmation = _safe_float(dimensional_scores.get("volume_confirmation"))
        temporal_validity = _safe_float(dimensional_scores.get("temporal_validity"))
        distance_to_pivot_pct = _safe_float(row.get(f"{prefix}_distance_to_pivot_pct"))
        stage_score_map = {
            "RECENT_BREAKOUT": 100.0,
            "COMPLETED": 88.0,
            "FORMING": 76.0,
            "STALE": 25.0,
            "FAILED": 0.0,
            "NONE": 0.0,
        }
        stage_score = stage_score_map.get(str(row.get("pattern_stage_summary") or "NONE"), 0.0)

        trend_template_score = min(max((_safe_float(row.get("met_count")) or 0.0) / 7.0, 0.0), 1.0) * 100.0
        rs_score = min(max(_safe_float(row.get("rs_score")) or 0.0, 0.0), 100.0)
        distance_to_52w_high_score = self._score_inverse(_safe_float(row.get("distance_to_52w_high")), 0.15, 0.35) * 100.0
        liquidity_score = _safe_float(row.get("tv_median20_pctile")) or 0.0
        leader_score_raw = self._weighted_score(
            [
                (trend_template_score, 0.35),
                (rs_score, 0.35),
                (distance_to_52w_high_score, 0.15),
                (liquidity_score, 0.15),
            ]
        )

        pattern_quality_raw = 0.0
        tightness_raw = 0.0
        breakout_readiness_raw = 0.0
        if bool(row.get("pattern_included", False)):
            fallback_quality = max_confidence * 100.0
            pattern_quality_raw = self._weighted_score(
                [
                    (((technical_quality if technical_quality is not None else max_confidence) * 100.0), 0.70),
                    (((temporal_validity if temporal_validity is not None else max_confidence) * 100.0), 0.30),
                ]
            )

            tightness_components: list[float] = []
            if volume_confirmation is not None:
                tightness_components.append(volume_confirmation * 100.0)
            tightness_pct = _safe_float(metrics.get("tightness_pct"))
            if tightness_pct is not None:
                tightness_components.append(self._score_inverse(tightness_pct, 0.05, 0.12) * 100.0)
            range_ratio = _safe_float(metrics.get("range_ratio_last10_vs_first10"))
            if range_ratio is not None:
                tightness_components.append(self._score_inverse(range_ratio, 0.75, 1.10) * 100.0)
            natr_ratio = _safe_float(metrics.get("natr_ratio_last10_vs_first10"))
            if natr_ratio is not None:
                tightness_components.append(self._score_inverse(natr_ratio, 0.75, 1.10) * 100.0)
            handle_depth_pct = _safe_float(metrics.get("handle_depth_pct"))
            if handle_depth_pct is not None:
                tightness_components.append(self._score_inverse(handle_depth_pct, 0.10, 0.16) * 100.0)
            handle_volume_ratio = _safe_float(metrics.get("handle_volume_ratio"))
            if handle_volume_ratio is not None:
                tightness_components.append(self._score_inverse(handle_volume_ratio, 0.90, 1.15) * 100.0)
            if not tightness_components:
                tightness_components.append(fallback_quality)
            tightness_raw = float(sum(tightness_components) / len(tightness_components))

            pivot_proximity_score = 0.0
            if distance_to_pivot_pct is not None:
                if distance_to_pivot_pct <= 0:
                    pivot_proximity_score = self._score_inverse(abs(distance_to_pivot_pct), 0.02, 0.18) * 100.0
                else:
                    pivot_proximity_score = self._score_inverse(distance_to_pivot_pct, 0.05, 0.20) * 100.0
            breakout_readiness_raw = self._weighted_score(
                [
                    (stage_score, 0.45),
                    (pivot_proximity_score, 0.35),
                    (max_confidence * 100.0, 0.20),
                ]
            )

        leader_score = round(leader_score_raw * 0.25, 2)
        pattern_quality_score = round(pattern_quality_raw * 0.35, 2)
        tightness_dryup_score = round(tightness_raw * 0.20, 2)
        breakout_readiness_score = round(breakout_readiness_raw * 0.20, 2)
        final_score = round(
            leader_score + pattern_quality_score + tightness_dryup_score + breakout_readiness_score,
            2,
        )
        return {
            "leader_score_component": leader_score,
            "pattern_quality_score_component": pattern_quality_score,
            "tightness_dryup_score_component": tightness_dryup_score,
            "breakout_readiness_score_component": breakout_readiness_score,
            "actual_data_pattern_priority_score": final_score,
        }

    @staticmethod
    def _pattern_pre_gate(prerequisites: dict[str, Any], market_threshold: float | None) -> tuple[bool, str]:
        bars = int(prerequisites.get("bars") or 0)
        tv_median20 = _safe_float(prerequisites.get("tv_median20"))
        if bars < 220:
            return False, "insufficient_bars"
        if not bool(prerequisites.get("recent_data_quality_pass")):
            return False, "recent_data_quality_failed"
        if not bool(prerequisites.get("trend_template_lite_pass")):
            return False, "trend_template_lite_failed"
        if tv_median20 is None or market_threshold is None or tv_median20 < market_threshold:
            return False, "liquidity_below_market_threshold"
        return True, "passed"

    def _apply_actual_data_pattern_ranking(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
        defaults = {
            "actionable_min_total_met_count": 6.0,
            "actionable_min_rs_score": 70.0,
            "actionable_min_confidence": 0.55,
            "actionable_min_priority_score": 72.0,
        }
        if frame.empty:
            return frame, defaults

        ranked = frame.copy()
        ranked["max_pattern_confidence"] = ranked[["vcp_confidence", "cup_handle_confidence"]].max(axis=1).fillna(0.0)
        ranked["pattern_count"] = ranked[["vcp_detected", "cup_handle_detected"]].fillna(False).sum(axis=1)
        ranked["tv_median20_pctile"] = self._percentile_score(ranked["tv_median20"])
        component_frame = ranked.apply(
            lambda row: pd.Series(self._compute_pattern_priority_components(row)),
            axis=1,
        )
        ranked = pd.concat([ranked, component_frame], axis=1)

        pattern_included = ranked[
            ranked["pattern_included"].fillna(False) & ranked["pattern_pre_gate_pass"].fillna(False)
        ].copy()
        defaults["actionable_min_total_met_count"] = bounded_quantile_value(
            ranked["total_met_count"],
            0.70,
            defaults["actionable_min_total_met_count"],
            lower=4.0,
            upper=12.0,
        )
        defaults["actionable_min_rs_score"] = bounded_quantile_value(
            ranked["rs_score"],
            0.70,
            defaults["actionable_min_rs_score"],
            lower=70.0,
            upper=95.0,
        )
        defaults["actionable_min_confidence"] = bounded_quantile_value(
            pattern_included["max_pattern_confidence"] if not pattern_included.empty else ranked["max_pattern_confidence"],
            0.55,
            defaults["actionable_min_confidence"],
            lower=0.35,
            upper=0.90,
            positive_only=True,
        )
        defaults["actionable_min_priority_score"] = bounded_quantile_value(
            pattern_included["actual_data_pattern_priority_score"] if not pattern_included.empty else ranked["actual_data_pattern_priority_score"],
            0.55,
            defaults["actionable_min_priority_score"],
            lower=60.0,
            upper=92.0,
        )

        ranked["actionable_pattern_pass"] = (
            ranked["pattern_pre_gate_pass"].fillna(False)
            & ranked["pattern_included"].fillna(False)
            & (pd.to_numeric(ranked["total_met_count"], errors="coerce").fillna(0.0) >= defaults["actionable_min_total_met_count"])
            & (pd.to_numeric(ranked["rs_score"], errors="coerce").fillna(0.0) >= defaults["actionable_min_rs_score"])
            & (pd.to_numeric(ranked["max_pattern_confidence"], errors="coerce").fillna(0.0) >= defaults["actionable_min_confidence"])
            & (pd.to_numeric(ranked["actual_data_pattern_priority_score"], errors="coerce").fillna(0.0) >= defaults["actionable_min_priority_score"])
        )
        return ranked, defaults

    def _local_market_data_dir(self) -> str:
        return os.path.join(project_root, "data", self.market)

    def _local_ohlcv_candidates(self, symbol: str) -> list[str]:
        symbol_key = str(symbol or "").strip().upper()
        safe_symbol = safe_filename(symbol_key)
        data_dir = self._local_market_data_dir()
        return [
            os.path.join(data_dir, f"{symbol_key}.csv"),
            os.path.join(data_dir, f"{safe_symbol}.csv"),
        ]

    def _load_local_ohlcv(self, symbol: str) -> pd.DataFrame:
        symbol_key = str(symbol or "").strip().upper()
        for path in self._local_ohlcv_candidates(symbol_key):
            if not os.path.exists(path):
                continue
            try:
                frame = pd.read_csv(path)
            except Exception:
                continue
            normalized = normalize_ohlcv_frame(frame, symbol_key, price_policy=PricePolicy.SPLIT_ADJUSTED)
            if not normalized.empty:
                return normalized
        return pd.DataFrame()

    def _write_local_ohlcv(self, symbol: str, frame: pd.DataFrame) -> None:
        symbol_key = str(symbol or "").strip().upper()
        if frame.empty:
            return
        os.makedirs(self._local_market_data_dir(), exist_ok=True)
        output_path = self._local_ohlcv_candidates(symbol_key)[1]
        dividends = _numeric_frame_series(frame, "dividends", fill_value=0.0)
        stock_splits = _numeric_frame_series(frame, "stock_splits", fill_value=0.0)
        split_factor = _numeric_frame_series(frame, "split_factor", fill_value=1.0)
        cache_frame = pd.DataFrame(
            {
                "date": frame["date"],
                "Open": _numeric_frame_series(frame, "raw_open", "open"),
                "High": _numeric_frame_series(frame, "raw_high", "high"),
                "Low": _numeric_frame_series(frame, "raw_low", "low"),
                "Close": _numeric_frame_series(frame, "raw_close", "close"),
                "Adj Close": _numeric_frame_series(frame, "adj_close", "close"),
                "Volume": _numeric_frame_series(frame, "volume"),
                "Dividends": dividends,
                "Stock Splits": stock_splits,
                "Split Factor": split_factor,
                "symbol": symbol_key,
            }
        )
        cache_frame.to_csv(output_path, index=False)

    def _download_ohlcv(self, symbol: str, days: int) -> pd.DataFrame:
        symbol_key = str(symbol or "").strip().upper()
        period_days = max(int(days or 0), 30)
        for provider_symbol in iter_provider_symbols(symbol_key, self.market):
            try:
                frame = yf.download(
                    provider_symbol,
                    period=f"{period_days}d",
                    interval="1d",
                    auto_adjust=False,
                    actions=True,
                    progress=False,
                )
            except Exception:
                continue
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                continue
            if isinstance(frame.columns, pd.MultiIndex):
                frame.columns = frame.columns.get_level_values(0)
            normalized = normalize_ohlcv_frame(frame, symbol_key, price_policy=PricePolicy.SPLIT_ADJUSTED)
            if normalized.empty:
                continue
            self._write_local_ohlcv(symbol_key, normalized)
            return normalized
        return pd.DataFrame()

    def merge_technical_and_financial(self) -> pd.DataFrame:
        if not os.path.exists(self.with_rs_path):
            return pd.DataFrame()

        technical_df = pd.read_csv(self.with_rs_path)
        if technical_df.empty:
            return technical_df

        if os.path.exists(self.advanced_path):
            financial_df = pd.read_csv(self.advanced_path)
        else:
            financial_df = pd.DataFrame(columns=["symbol", "fin_met_count", "has_error"])

        merged = pd.merge(
            technical_df,
            financial_df,
            on="symbol",
            how="left",
            suffixes=("", "_fin"),
        )
        if "fin_met_count" not in merged.columns:
            merged["fin_met_count"] = 0
        merged["fin_met_count"] = pd.to_numeric(merged["fin_met_count"], errors="coerce").fillna(0).astype(int)
        merged["total_met_count"] = merged["met_count"].fillna(0).astype(int) + merged["fin_met_count"]
        merged = merged.sort_values(["total_met_count", "rs_score"], ascending=[False, False]).reset_index(drop=True)
        self._write_frame(merged, self.integrated_path, self.integrated_path.replace(".csv", ".json"))
        self._write_frame(merged, self.patternless_results_csv, self.patternless_results_json)
        self._write_frame(merged, self.pre_pattern_quant_financial_csv, self.pre_pattern_quant_financial_json)
        return merged

    def fetch_ohlcv_data(self, symbol: str, days: int = 365) -> pd.DataFrame:
        frame = self._load_local_ohlcv(symbol)
        if frame.empty:
            frame = self._download_ohlcv(symbol, days=days)
        if frame.empty:
            return pd.DataFrame()

        normalized = normalize_indicator_frame(
            frame,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
            utc_dates=True,
        ).rename(
            columns={
                "date": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "adj_close": "Adj Close",
                "volume": "Volume",
                "raw_open": "Raw Open",
                "raw_high": "Raw High",
                "raw_low": "Raw Low",
                "raw_close": "Raw Close",
                "stock_splits": "Stock Splits",
                "dividends": "Dividends",
                "split_factor": "Split Factor",
            }
        ).copy()
        normalized["Date"] = pd.to_datetime(normalized["Date"], errors="coerce", utc=True)
        normalized = normalized.dropna(subset=["Date"]).sort_values("Date")
        if normalized.empty:
            return pd.DataFrame()

        return normalized.set_index("Date")

    def run_integrated_screening(self, max_symbols: Optional[int] = None) -> pd.DataFrame:
        merged = self.merge_technical_and_financial()
        print(f"[Integrated] Seed merge completed ({self.market}) - rows={len(merged)}")
        if merged.empty:
            empty = pd.DataFrame()
            self._write_frame(empty, self.pattern_results_csv, self.pattern_results_json)
            self._write_frame(empty, self.patternless_results_csv, self.patternless_results_json)
            self._write_frame(empty, self.pre_pattern_quant_financial_csv, self.pre_pattern_quant_financial_json)
            self._write_frame(empty, self.pattern_enriched_csv, self.pattern_enriched_json)
            self._write_frame(empty, self.pattern_actionable_csv, self.pattern_actionable_json)
            return empty

        target_df = merged.head(max_symbols) if max_symbols else merged
        pattern_rows: list[dict[str, object]] = []
        prepared_targets: list[dict[str, Any]] = []
        total_targets = len(target_df)
        print(f"[Integrated] Pattern analysis started ({self.market}) - targets={total_targets}")
        interval = progress_interval(total_targets, target_updates=8, min_interval=25)
        for index, row in enumerate(target_df.to_dict("records"), start=1):
            symbol = str(row.get("symbol", "")).strip().upper()
            if not symbol:
                if is_progress_tick(index, total_targets, interval):
                    print(
                        f"[Integrated] Pattern analysis progress ({self.market}) - "
                        f"processed={index}/{total_targets}, detected={len(pattern_rows)}"
                )
                continue
            stock_df = self.fetch_ohlcv_data(symbol)
            if stock_df.empty:
                if is_progress_tick(index, total_targets, interval):
                    print(
                        f"[Integrated] Pattern analysis progress ({self.market}) - "
                        f"processed={index}/{total_targets}, detected={len(pattern_rows)}"
                )
                continue

            prepared_targets.append(
                {
                    "row": row,
                    "symbol": symbol,
                    "stock_df": stock_df,
                    "prerequisites": self.pattern_analyzer.evaluate_prerequisites(stock_df),
                }
            )
            if is_progress_tick(index, total_targets, interval):
                print(
                    f"[Integrated] Pattern analysis progress ({self.market}) - "
                    f"processed={index}/{total_targets}, prepared={len(prepared_targets)}"
                )

        eligible_liquidity = pd.Series(
            [
                item["prerequisites"]["tv_median20"]
                for item in prepared_targets
                if int(item["prerequisites"].get("bars") or 0) >= 220
                and bool(item["prerequisites"].get("recent_data_quality_pass"))
                and bool(item["prerequisites"].get("trend_template_lite_pass"))
                and item["prerequisites"].get("tv_median20") is not None
            ],
            dtype=float,
        )
        pattern_market_threshold = (
            float(eligible_liquidity.quantile(0.40))
            if not eligible_liquidity.dropna().empty
            else None
        )

        for prepared in prepared_targets:
            row = prepared["row"]
            symbol = prepared["symbol"]
            prerequisites = prepared["prerequisites"]
            pattern_pre_gate_pass, pattern_pre_gate_reason = self._pattern_pre_gate(prerequisites, pattern_market_threshold)
            if pattern_pre_gate_pass:
                patterns = self.pattern_analyzer.analyze_patterns_enhanced(symbol, prepared["stock_df"])
            else:
                patterns = {
                    "vcp": self.pattern_analyzer._empty_pattern_output("VCP"),
                    "cup_handle": self.pattern_analyzer._empty_pattern_output("CUP_HANDLE"),
                }
            vcp = patterns.get("vcp", {})
            cup_handle = patterns.get("cup_handle", {})
            pattern_rows.append(
                {
                    **row,
                    "market": self.market,
                    "bars": int(prerequisites.get("bars") or 0),
                    "tv_median20": _safe_float(prerequisites.get("tv_median20")),
                    "recent_data_quality_pass": bool(prerequisites.get("recent_data_quality_pass")),
                    "trend_template_lite_pass": bool(prerequisites.get("trend_template_lite_pass")),
                    "pattern_pre_gate_pass": pattern_pre_gate_pass,
                    "pattern_pre_gate_reason": pattern_pre_gate_reason,
                    "pattern_market_threshold": pattern_market_threshold,
                    "vcp_detected": bool(vcp.get("detected", False)),
                    "vcp_confidence": float(vcp.get("confidence", 0.0) or 0.0),
                    "vcp_confidence_level": vcp.get("confidence_level"),
                    "vcp_state_detail": vcp.get("state_detail"),
                    "vcp_state_bucket": vcp.get("state_bucket"),
                    "vcp_pattern_start": vcp.get("pattern_start"),
                    "vcp_pattern_end": vcp.get("pattern_end"),
                    "vcp_pivot_price": vcp.get("pivot_price"),
                    "vcp_invalidation_price": vcp.get("invalidation_price"),
                    "vcp_breakout_date": vcp.get("breakout_date"),
                    "vcp_breakout_price": vcp.get("breakout_price"),
                    "vcp_breakout_volume": vcp.get("breakout_volume"),
                    "vcp_volume_multiple": vcp.get("volume_multiple"),
                    "vcp_distance_to_pivot_pct": vcp.get("distance_to_pivot_pct"),
                    "vcp_extended": bool(vcp.get("extended", False)),
                    "vcp_dimensional_scores": vcp.get("dimensional_scores", {}),
                    "vcp_metrics": vcp.get("metrics", {}),
                    "vcp_pivots": vcp.get("pivots", []),
                    "cup_handle_detected": bool(cup_handle.get("detected", False)),
                    "cup_handle_confidence": float(cup_handle.get("confidence", 0.0) or 0.0),
                    "cup_handle_confidence_level": cup_handle.get("confidence_level"),
                    "cup_handle_state_detail": cup_handle.get("state_detail"),
                    "cup_handle_state_bucket": cup_handle.get("state_bucket"),
                    "cup_handle_pattern_start": cup_handle.get("pattern_start"),
                    "cup_handle_pattern_end": cup_handle.get("pattern_end"),
                    "cup_handle_pivot_price": cup_handle.get("pivot_price"),
                    "cup_handle_invalidation_price": cup_handle.get("invalidation_price"),
                    "cup_handle_breakout_date": cup_handle.get("breakout_date"),
                    "cup_handle_breakout_price": cup_handle.get("breakout_price"),
                    "cup_handle_breakout_volume": cup_handle.get("breakout_volume"),
                    "cup_handle_volume_multiple": cup_handle.get("volume_multiple"),
                    "cup_handle_distance_to_pivot_pct": cup_handle.get("distance_to_pivot_pct"),
                    "cup_handle_extended": bool(cup_handle.get("extended", False)),
                    "cup_handle_dimensional_scores": cup_handle.get("dimensional_scores", {}),
                    "cup_handle_metrics": cup_handle.get("metrics", {}),
                    "cup_handle_pivots": cup_handle.get("pivots", []),
                }
            )

        pattern_df = pd.DataFrame(pattern_rows)
        actual_data_calibration: dict[str, float] = {}
        if not pattern_df.empty:
            pattern_df = self._annotate_pattern_coverage(pattern_df)
            pattern_df, actual_data_calibration = self._apply_actual_data_pattern_ranking(pattern_df)
            pattern_df = pattern_df.sort_values(
                ["actionable_pattern_pass", "actual_data_pattern_priority_score", "total_met_count", "rs_score"],
                ascending=[False, False, False, False],
            ).reset_index(drop=True)

        actionable_df = pattern_df[pattern_df["actionable_pattern_pass"]].copy() if not pattern_df.empty else pd.DataFrame()
        self._write_frame(pattern_df, self.pattern_results_csv, self.pattern_results_json)
        self._write_frame(pattern_df, self.pattern_enriched_csv, self.pattern_enriched_json)
        self._write_frame(actionable_df, self.pattern_actionable_csv, self.pattern_actionable_json)
        with open(self.actual_data_calibration_json, "w", encoding="utf-8") as handle:
            json.dump(actual_data_calibration, handle, ensure_ascii=False, indent=2)
        print(
            f"[Integrated] Pattern outputs saved ({self.market}) - "
            f"enriched={len(pattern_df)}, actionable={len(actionable_df)}"
        )
        return pattern_df


def run_integrated_screening(max_symbols: Optional[int] = None, *, market: str = "us") -> pd.DataFrame:
    screener = IntegratedScreener(market=market)
    return screener.run_integrated_screening(max_symbols=max_symbols)


if __name__ == "__main__":
    run_integrated_screening(market="us")
