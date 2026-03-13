from __future__ import annotations

import json
import os
from typing import Optional

import pandas as pd
import yfinance as yf

from utils.actual_data_calibration import bounded_quantile_value
from utils.io_utils import safe_filename
from utils.market_data_contract import normalize_ohlcv_frame
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
        self.pattern_enriched_csv = os.path.join(self.results_dir, "integrated_with_patterns.csv")
        self.pattern_enriched_json = self.pattern_enriched_csv.replace(".csv", ".json")
        self.pattern_actionable_csv = os.path.join(self.results_dir, "integrated_actionable_patterns.csv")
        self.pattern_actionable_json = self.pattern_actionable_csv.replace(".csv", ".json")
        self.actual_data_calibration_json = os.path.join(self.results_dir, "actual_data_pattern_calibration.json")
        self.pattern_analyzer = EnhancedPatternAnalyzer()

    @staticmethod
    def _write_frame(frame: pd.DataFrame, csv_path: str, json_path: str) -> None:
        frame.to_csv(csv_path, index=False)
        frame.to_json(json_path, orient="records", indent=2, force_ascii=False)

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
        annotated["has_forming_pattern"] = annotated[["vcp_state_bucket", "cup_handle_state_bucket"]].eq("FORMING").any(axis=1)
        annotated["has_completed_pattern"] = annotated[["vcp_state_bucket", "cup_handle_state_bucket"]].eq("COMPLETED").any(axis=1)
        annotated["has_recent_breakout_pattern"] = annotated[["vcp_state_bucket", "cup_handle_state_bucket"]].eq("BROKEOUT_RECENT").any(axis=1)
        annotated["has_failed_pattern"] = annotated[["vcp_state_bucket", "cup_handle_state_bucket"]].eq("FAILED").any(axis=1)
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

    def _apply_actual_data_pattern_ranking(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
        defaults = {
            "actionable_min_total_met_count": 6.0,
            "actionable_min_rs_score": 85.0,
            "actionable_min_confidence": 0.55,
            "actionable_min_priority_score": 72.0,
        }
        if frame.empty:
            return frame, defaults

        ranked = frame.copy()
        ranked["max_pattern_confidence"] = ranked[["vcp_confidence", "cup_handle_confidence"]].max(axis=1).fillna(0.0)
        ranked["pattern_count"] = ranked[["vcp_detected", "cup_handle_detected"]].fillna(False).sum(axis=1)
        ranked["total_met_count_pctile"] = self._percentile_score(ranked["total_met_count"])
        ranked["rs_score_pctile"] = self._percentile_score(ranked["rs_score"])
        ranked["pattern_confidence_pctile"] = self._percentile_score(ranked["max_pattern_confidence"])
        stage_score_map = {
            "RECENT_BREAKOUT": 100.0,
            "FORMING": 82.0,
            "COMPLETED": 68.0,
            "STALE": 45.0,
            "FAILED": 0.0,
            "NONE": 25.0,
        }
        ranked["pattern_stage_score"] = ranked["pattern_stage_summary"].map(stage_score_map).fillna(25.0)
        ranked["actual_data_pattern_priority_score"] = (
            (0.25 * ranked["total_met_count_pctile"])
            + (0.25 * ranked["rs_score_pctile"])
            + (0.20 * ranked["pattern_confidence_pctile"])
            + (0.20 * ranked["pattern_stage_score"])
            + (0.10 * (ranked["pattern_count"].clip(lower=0, upper=2) / 2.0 * 100.0))
        )

        pattern_included = ranked[ranked["pattern_included"].fillna(False)].copy()
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
            upper=98.0,
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
            ranked["pattern_included"].fillna(False)
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
            normalized = normalize_ohlcv_frame(frame, symbol_key)
            if not normalized.empty:
                return normalized
        return pd.DataFrame()

    def _write_local_ohlcv(self, symbol: str, frame: pd.DataFrame) -> None:
        symbol_key = str(symbol or "").strip().upper()
        if frame.empty:
            return
        os.makedirs(self._local_market_data_dir(), exist_ok=True)
        output_path = self._local_ohlcv_candidates(symbol_key)[1]
        cache_frame = frame.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
        )[["date", "Open", "High", "Low", "Close", "Volume", "symbol"]]
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
                    progress=False,
                )
            except Exception:
                continue
            if isinstance(frame.columns, pd.MultiIndex):
                frame.columns = frame.columns.get_level_values(0)
            normalized = normalize_ohlcv_frame(frame, symbol_key)
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
        return merged

    def fetch_ohlcv_data(self, symbol: str, days: int = 365) -> pd.DataFrame:
        frame = self._load_local_ohlcv(symbol)
        if frame.empty:
            frame = self._download_ohlcv(symbol, days=days)
        if frame.empty:
            return pd.DataFrame()

        normalized = frame.rename(
            columns={
                "date": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
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
            self._write_frame(empty, self.pattern_enriched_csv, self.pattern_enriched_json)
            self._write_frame(empty, self.pattern_actionable_csv, self.pattern_actionable_json)
            return empty

        target_df = merged.head(max_symbols) if max_symbols else merged
        pattern_rows: list[dict[str, object]] = []
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

            patterns = self.pattern_analyzer.analyze_patterns_enhanced(symbol, stock_df)
            vcp = patterns.get("vcp", {})
            cup_handle = patterns.get("cup_handle", {})
            pattern_rows.append(
                {
                    **row,
                    "market": self.market,
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
            if is_progress_tick(index, total_targets, interval):
                print(
                    f"[Integrated] Pattern analysis progress ({self.market}) - "
                    f"processed={index}/{total_targets}, detected={len(pattern_rows)}"
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
