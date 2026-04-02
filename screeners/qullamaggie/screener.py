from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from utils.market_data_contract import PricePolicy, load_benchmark_data, load_local_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_benchmark_candidates,
    get_market_data_dir,
    get_primary_benchmark_symbol,
    get_qullamaggie_results_dir,
    get_stock_metadata_path,
    is_index_symbol,
    market_key,
)
from utils.progress_runtime import is_progress_tick, progress_interval
from utils.screener_utils import create_screener_summary, save_screening_results, track_new_tickers
from utils.typing_utils import frame_keyed_records, row_to_record
from screeners.leader_core_bridge import load_market_truth_snapshot

from .core import MarketRegime, QullamaggieAnalyzer, _safe_bool, _safe_float
from .earnings_data_collector import EarningsDataCollector


_ANALYZER = QullamaggieAnalyzer()
_SCREENING_CONTEXT: dict[str, Any] = {}


def _load_market_symbols(market: str) -> list[str]:
    data_dir = get_market_data_dir(market)
    if not os.path.isdir(data_dir):
        return []
    symbols = {
        os.path.splitext(name)[0].upper()
        for name in os.listdir(data_dir)
        if name.endswith(".csv")
    }
    return sorted(symbol for symbol in symbols if not is_index_symbol(market, symbol))


def _load_metadata_map(market: str) -> dict[str, dict[str, Any]]:
    metadata_path = get_stock_metadata_path(market)
    if not os.path.exists(metadata_path):
        return {}
    frame = pd.read_csv(metadata_path)
    if frame.empty or "symbol" not in frame.columns:
        return {}
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    return frame_keyed_records(frame, key_column="symbol", uppercase_keys=True, drop_na=True)


def _feature_to_snapshot(feature_row: dict[str, Any], regime: MarketRegime, market: str, *, stage: str) -> dict[str, Any]:
    profile = _ANALYZER.market_profile(market)
    a_pp_score = _safe_float(feature_row.get("a_pp_score")) or 0.0
    setup_score = _safe_float(feature_row.get("focus_seed_score")) or 0.0
    final_priority_score = (0.60 * a_pp_score) + (0.25 * setup_score) + (0.15 * regime.regime_score)
    data_confidence_score = _safe_float(feature_row.get("data_confidence_score")) or 0.0
    reasons: list[str] = []
    if (_safe_float(feature_row.get("ret_3m_pctile")) or 0.0) >= 80.0:
        reasons.append("TOP_RS_3M")
    if (_safe_float(feature_row.get("ret_6m_pctile")) or 0.0) >= 80.0:
        reasons.append("TOP_RS_6M")
    if (_safe_float(feature_row.get("high_52w_proximity")) or 0.0) >= 0.92:
        reasons.append("NEAR_52W_HIGH")
    if (_safe_float(feature_row.get("compression_score")) or 0.0) >= 70.0:
        reasons.append("TIGHT_BASE")
    if regime.regime_state in {"RISK_ON", "RISK_ON_AGGRESSIVE"}:
        reasons.append("REGIME_SUPPORTIVE")
    if not reasons:
        reasons.append("LEADERSHIP_POOL")

    data_flags = [
        "HAS_DAILY",
        "HAS_SECTOR_MAPPING" if _safe_bool(feature_row.get("has_sector_mapping")) else "NO_SECTOR_MAPPING",
        "HAS_FUNDAMENTALS" if _safe_bool(feature_row.get("has_fundamentals")) else "NO_FUNDAMENTALS",
        "NO_INTRADAY",
    ]
    return {
        "as_of_ts": feature_row.get("as_of_ts"),
        "symbol": feature_row.get("symbol"),
        "market": profile.market_code,
        "market_code": profile.market_code,
        "setup_family": "LEADERSHIP",
        "candidate_stage": stage,
        "stock_grade": feature_row.get("stock_grade"),
        "setup_grade": "Watch",
        "a_pp_score": round(a_pp_score, 2),
        "setup_score": round(setup_score, 2),
        "final_priority_score": round(final_priority_score, 2),
        "regime_state": regime.regime_state,
        "market_alias": regime.market_alias,
        "market_alignment_score": round(regime.market_alignment_score, 2),
        "breadth_support_score": round(regime.breadth_support_score, 2),
        "rotation_support_score": round(regime.rotation_support_score, 2),
        "leader_health_score": _safe_float(regime.leader_health_score),
        "reason_codes": reasons,
        "fail_codes": [],
        "data_flags": data_flags,
        "data_confidence_score": round(data_confidence_score, 2),
        "pivot_price": feature_row.get("pivot_price"),
        "stop_price": feature_row.get("stop_price"),
        "risk_unit_pct": feature_row.get("risk_unit_pct"),
        "entry_timeframe": profile.orh_windows[0],
        "scores": {
            "a_pp_score": round(a_pp_score, 2),
            "setup_score": round(setup_score, 2),
            "final_priority_score": round(final_priority_score, 2),
            "data_confidence_score": round(data_confidence_score, 2),
        },
    }


def _feature_to_patternless_pool_row(feature_row: dict[str, Any], regime: MarketRegime, market: str) -> dict[str, Any]:
    profile = _ANALYZER.market_profile(market)
    return {
        "as_of_ts": feature_row.get("as_of_ts"),
        "symbol": feature_row.get("symbol"),
        "market": profile.market_code,
        "sector": feature_row.get("sector"),
        "setup_scope": "PATTERN_EXCLUDED_POOL",
        "stock_grade": feature_row.get("stock_grade"),
        "a_pp_score": round(_safe_float(feature_row.get("a_pp_score")) or 0.0, 2),
        "focus_seed_score": round(_safe_float(feature_row.get("focus_seed_score")) or 0.0, 2),
        "compression_score": round(_safe_float(feature_row.get("compression_score")) or 0.0, 2),
        "high_52w_proximity": round(_safe_float(feature_row.get("high_52w_proximity")) or 0.0, 4),
        "breakout_universe_pass": bool(feature_row.get("breakout_universe_pass")),
        "ep_universe_pass": bool(feature_row.get("ep_universe_pass")),
        "pivot_price": feature_row.get("pivot_price"),
        "stop_price": feature_row.get("stop_price"),
        "regime_state": regime.regime_state,
        "market_alias": regime.market_alias,
        "market_alignment_score": round(regime.market_alignment_score, 2),
        "breadth_support_score": round(regime.breadth_support_score, 2),
        "rotation_support_score": round(regime.rotation_support_score, 2),
        "leader_health_score": _safe_float(regime.leader_health_score),
    }


def _to_pre_pattern_quant_financial_row(candidate: dict[str, Any]) -> dict[str, Any]:
    patternless = {
        key: value
        for key, value in candidate.items()
        if not str(key).startswith("vcp_") and not str(key).startswith("cup_handle_")
    }
    patternless["screening_stage"] = "PRE_PATTERN_QUANT_FINANCIAL"
    patternless.setdefault("pattern_filter_applied", False)
    return patternless


def _write_records(results_dir: str, stem: str, records: list[dict[str, Any]]) -> None:
    save_screening_results(
        results=records,
        output_dir=results_dir,
        filename_prefix=stem,
        include_timestamp=True,
        incremental_update=True,
    )


def _build_context(
    market: str,
    frames: dict[str, pd.DataFrame],
    metadata_map: dict[str, dict[str, Any]],
    *,
    enable_earnings_filter: bool,
) -> dict[str, Any]:
    feature_rows = [
        _ANALYZER.compute_feature_row(symbol, market, frame, metadata_map.get(symbol))
        for symbol, frame in frames.items()
    ]
    feature_table = _ANALYZER.finalize_feature_table(pd.DataFrame(feature_rows)) if feature_rows else pd.DataFrame()
    calibration = _ANALYZER.build_actual_data_calibration(feature_table, market=market)
    if not feature_table.empty:
        feature_table = _ANALYZER.apply_actual_data_calibration(
            feature_table,
            market=market,
            calibration=calibration,
        )
    feature_map = frame_keyed_records(feature_table, key_column="symbol", uppercase_keys=True) if not feature_table.empty else {}

    benchmark_symbol, benchmark_daily = load_benchmark_data(
        market,
        get_benchmark_candidates(market),
        allow_yfinance_fallback=True,
        price_policy=PricePolicy.SPLIT_ADJUSTED,
    )
    benchmark_symbol = benchmark_symbol or get_primary_benchmark_symbol(market)
    as_of_date = str(pd.Timestamp(benchmark_daily["date"].iloc[-1]).date()) if not benchmark_daily.empty else ""
    market_truth = load_market_truth_snapshot(market, as_of_date=as_of_date)
    regime = _ANALYZER.build_market_regime_from_truth(
        market=market,
        benchmark_symbol=benchmark_symbol,
        market_truth=market_truth,
        benchmark_daily=benchmark_daily,
    )
    return {
        "market": market,
        "feature_table": feature_table,
        "feature_map": feature_map,
        "calibration": calibration,
        "frames": frames,
        "metadata_map": metadata_map,
        "benchmark_symbol": benchmark_symbol,
        "benchmark_daily": benchmark_daily,
        "regime": regime,
        "earnings_collector": EarningsDataCollector(market=market) if enable_earnings_filter else None,
    }


def apply_basic_filters(df: pd.DataFrame):
    return _ANALYZER.apply_basic_filters(
        df,
        market=_SCREENING_CONTEXT.get("market", "us"),
        calibration=_SCREENING_CONTEXT.get("calibration"),
    )


def check_vcp_pattern(df: pd.DataFrame):
    return _ANALYZER.check_vcp_pattern(
        df,
        market=_SCREENING_CONTEXT.get("market", "us"),
        calibration=_SCREENING_CONTEXT.get("calibration"),
    )


def screen_breakout_setup(symbol: str, frame: pd.DataFrame):
    context = _SCREENING_CONTEXT
    return _ANALYZER.analyze_breakout(
        symbol,
        frame,
        market=context.get("market", "us"),
        feature_row=context.get("feature_map", {}).get(str(symbol).upper()),
        regime=context.get("regime"),
        calibration=context.get("calibration"),
    )


def screen_episode_pivot_setup(symbol: str, frame: pd.DataFrame, enable_earnings_filter: bool = True, market: str = "us"):
    context = _SCREENING_CONTEXT
    return _ANALYZER.analyze_episode_pivot(
        symbol,
        frame,
        enable_earnings_filter,
        market=market,
        feature_row=context.get("feature_map", {}).get(str(symbol).upper()),
        regime=context.get("regime"),
        earnings_collector=context.get("earnings_collector"),
        calibration=context.get("calibration"),
    )


def screen_parabolic_short_setup(symbol: str, frame: pd.DataFrame):
    context = _SCREENING_CONTEXT
    return _ANALYZER.analyze_parabolic_short(
        symbol,
        frame,
        market=context.get("market", "us"),
        feature_row=context.get("feature_map", {}).get(str(symbol).upper()),
        regime=context.get("regime"),
    )


class QullamaggieScreener:
    def __init__(self, *, market: str = "us", enable_earnings_filter: bool = True) -> None:
        self.market = market_key(market)
        self.enable_earnings_filter = bool(enable_earnings_filter)
        ensure_market_dirs(self.market)
        self.results_dir = get_qullamaggie_results_dir(self.market)

    def _load_frames(self) -> dict[str, pd.DataFrame]:
        symbols = _load_market_symbols(self.market)
        frames: dict[str, pd.DataFrame] = {}
        if not symbols:
            return frames

        def _load(symbol: str) -> tuple[str, pd.DataFrame]:
            return symbol, load_local_ohlcv_frame(self.market, symbol, price_policy=PricePolicy.SPLIT_ADJUSTED)

        max_workers = min(8, max(1, len(symbols)))
        print(
            f"[Qullamaggie] Frame load started ({self.market}) - "
            f"symbols={len(symbols)}, workers={max_workers}"
        )
        interval = progress_interval(len(symbols), target_updates=8, min_interval=50)
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_load, symbol): symbol for symbol in symbols}
            for future in as_completed(future_map):
                completed += 1
                symbol, frame = future.result()
                if not frame.empty:
                    frames[symbol] = frame
                if is_progress_tick(completed, len(symbols), interval):
                    print(
                        f"[Qullamaggie] Frame load progress ({self.market}) - "
                        f"processed={completed}/{len(symbols)}, loaded={len(frames)}"
                    )
        print(f"[Qullamaggie] Frame load completed ({self.market}) - loaded={len(frames)}")
        return frames

    def run(self, setup_type: str | None = None) -> dict[str, Any]:
        global _SCREENING_CONTEXT

        metadata_map = _load_metadata_map(self.market)
        frames = self._load_frames()
        print(
            f"[Qullamaggie] Context build started ({self.market}) - "
            f"frames={len(frames)}, metadata={len(metadata_map)}"
        )
        _SCREENING_CONTEXT = _build_context(
            self.market,
            frames,
            metadata_map,
            enable_earnings_filter=self.enable_earnings_filter,
        )
        regime: MarketRegime = _SCREENING_CONTEXT["regime"]
        feature_table: pd.DataFrame = _SCREENING_CONTEXT["feature_table"]
        print(
            f"[Qullamaggie] Context ready ({self.market}) - "
            f"features={len(feature_table)}, regime={regime.regime_state}"
        )

        results: dict[str, Any] = {
            "breakout": [],
            "episode_pivot": [],
            "parabolic_short": [],
            "all_candidates": [],
            "pre_pattern_quant_financial": [],
            "pattern_excluded_pool": [],
            "pattern_included_candidates": [],
            "universe_list": [],
            "wide_list": [],
            "weekly_focus": [],
            "daily_focus": [],
            "market_regime": {
                "benchmark_symbol": regime.benchmark_symbol,
                "regime_state": regime.regime_state,
                "market_alias": regime.market_alias,
                "market_alignment_score": regime.market_alignment_score,
                "breadth_support_score": regime.breadth_support_score,
                "rotation_support_score": regime.rotation_support_score,
                "leader_health_score": regime.leader_health_score,
                "reason_codes": list(regime.reason_codes),
                "data_flags": list(regime.data_flags),
            },
            "actual_data_calibration": dict(_SCREENING_CONTEXT.get("calibration", {})),
        }

        def process_symbol(symbol: str) -> dict[str, list[dict[str, Any]]]:
            frame = frames.get(symbol, pd.DataFrame())
            if frame.empty:
                return {"breakout": [], "episode_pivot": [], "parabolic_short": []}
            symbol_results = {"breakout": [], "episode_pivot": [], "parabolic_short": []}
            if setup_type in {None, "breakout"}:
                breakout = screen_breakout_setup(symbol, frame.copy())
                if breakout.get("passed"):
                    symbol_results["breakout"].append(breakout)
            if setup_type in {None, "episode_pivot"}:
                episode = screen_episode_pivot_setup(symbol, frame.copy(), self.enable_earnings_filter, market=self.market)
                if episode.get("passed"):
                    symbol_results["episode_pivot"].append(episode)
            if setup_type in {None, "parabolic_short"}:
                parabolic = screen_parabolic_short_setup(symbol, frame.copy())
                if parabolic.get("passed"):
                    symbol_results["parabolic_short"].append(parabolic)
            return symbol_results

        max_workers = min(8, max(1, len(frames)))
        total_symbols = len(frames)
        print(
            f"[Qullamaggie] Setup scan started ({self.market}) - "
            f"symbols={total_symbols}, workers={max_workers}, setup={setup_type or 'all'}"
        )
        interval = progress_interval(total_symbols, target_updates=8, min_interval=50)
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(process_symbol, symbol): symbol for symbol in sorted(frames)}
            for future in as_completed(future_map):
                completed += 1
                symbol_results = future.result()
                for key in ("breakout", "episode_pivot", "parabolic_short"):
                    results[key].extend(symbol_results[key])
                if is_progress_tick(completed, total_symbols, interval):
                    print(
                        f"[Qullamaggie] Setup scan progress ({self.market}) - "
                        f"processed={completed}/{total_symbols}, breakout={len(results['breakout'])}, "
                        f"episode={len(results['episode_pivot'])}, short={len(results['parabolic_short'])}"
                    )

        results["breakout"] = sorted(results["breakout"], key=lambda item: item.get("final_priority_score", 0), reverse=True)
        results["episode_pivot"] = sorted(results["episode_pivot"], key=lambda item: item.get("final_priority_score", 0), reverse=True)
        results["parabolic_short"] = sorted(results["parabolic_short"], key=lambda item: item.get("score", 0), reverse=True)
        results["all_candidates"] = sorted(
            results["breakout"] + results["episode_pivot"],
            key=lambda item: item.get("final_priority_score", 0),
            reverse=True,
        )

        results["pre_pattern_quant_financial"] = [
            _to_pre_pattern_quant_financial_row(row)
            for row in results["all_candidates"]
        ]

        if not feature_table.empty:
            universe_rows = [
                _feature_to_snapshot(row_to_record(row), regime, self.market, stage="UNIVERSE")
                for _, row in feature_table.sort_values(["a_pp_score", "focus_seed_score"], ascending=[False, False]).iterrows()
                if bool(row.get("breakout_universe_pass") or row.get("ep_universe_pass"))
            ]
            patternless_rows = [
                _feature_to_patternless_pool_row(row_to_record(row), regime, self.market)
                for _, row in feature_table.sort_values(["a_pp_score", "focus_seed_score"], ascending=[False, False]).iterrows()
                if bool(row.get("breakout_universe_pass") or row.get("ep_universe_pass"))
            ]
            results["universe_list"] = universe_rows[:600]
            results["pattern_excluded_pool"] = patternless_rows[:600]
            results["wide_list"] = [
                {**row, "candidate_stage": "WIDE_LIST"}
                for row in results["universe_list"][:100]
            ]
            weekly_seed = [row for row in results["all_candidates"] if row.get("priority_tier") in {"Tier 1", "Tier 2"}]
            if not weekly_seed:
                weekly_seed = [
                    {**row, "candidate_stage": "WEEKLY_FOCUS"}
                    for row in results["wide_list"][:20]
                ]
            results["weekly_focus"] = weekly_seed[:20]
            results["daily_focus"] = [row for row in results["all_candidates"] if row.get("priority_tier") == "Tier 1"][:5]
            if not results["daily_focus"]:
                results["daily_focus"] = results["all_candidates"][:5]
            results["pattern_included_candidates"] = list(results["all_candidates"])

        print(
            f"[Qullamaggie] Persisting results ({self.market}) - "
            f"all={len(results['all_candidates'])}, universe={len(results['universe_list'])}"
        )
        self._persist_results(results)
        _SCREENING_CONTEXT = {}
        return results

    def _persist_results(self, results: dict[str, Any]) -> None:
        def persist_result(key: str, filename_prefix: str, tracker_name: str) -> None:
            payload = sorted(results[key], key=lambda item: item.get("score", item.get("final_priority_score", 0)), reverse=True)
            results_paths = save_screening_results(
                results=payload,
                output_dir=self.results_dir,
                filename_prefix=filename_prefix,
                include_timestamp=True,
                incremental_update=True,
            )
            tracker_file = os.path.join(self.results_dir, tracker_name)
            new_tickers = track_new_tickers(
                current_results=payload,
                tracker_file=tracker_file,
                symbol_key="symbol",
                retention_days=14,
            )
            create_screener_summary(
                screener_name=f"Qullamaggie {key} ({self.market.upper()})",
                total_candidates=len(payload),
                new_tickers=len(new_tickers),
                results_paths=results_paths,
            )

        persist_result("breakout", "breakout_results", "new_breakout_tickers.csv")
        persist_result("episode_pivot", "episode_pivot_results", "new_episode_pivot_tickers.csv")
        persist_result("parabolic_short", "parabolic_short_results", "new_parabolic_short_tickers.csv")

        _write_records(self.results_dir, "candidate_snapshots", results.get("all_candidates", []))
        _write_records(self.results_dir, "pre_pattern_quant_financial_candidates", results.get("pre_pattern_quant_financial", []))
        _write_records(self.results_dir, "pattern_excluded_pool", results.get("pattern_excluded_pool", []))
        _write_records(self.results_dir, "pattern_included_candidates", results.get("pattern_included_candidates", []))
        _write_records(self.results_dir, "universe_list", results.get("universe_list", []))
        _write_records(self.results_dir, "wide_list", results.get("wide_list", []))
        _write_records(self.results_dir, "weekly_focus_list", results.get("weekly_focus", []))
        _write_records(self.results_dir, "daily_focus_list", results.get("daily_focus", []))

        summary = {
            "market": self.market.upper(),
            "regime": results.get("market_regime", {}),
            "actual_data_calibration": results.get("actual_data_calibration", {}),
            "counts": {
                "breakout": len(results.get("breakout", [])),
                "episode_pivot": len(results.get("episode_pivot", [])),
                "parabolic_short": len(results.get("parabolic_short", [])),
                "universe": len(results.get("universe_list", [])),
                "pre_pattern_quant_financial": len(results.get("pre_pattern_quant_financial", [])),
                "pattern_excluded_pool": len(results.get("pattern_excluded_pool", [])),
                "pattern_included_candidates": len(results.get("pattern_included_candidates", [])),
                "daily_focus": len(results.get("daily_focus", [])),
            },
        }
        with open(os.path.join(self.results_dir, "market_summary.json"), "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
        with open(os.path.join(self.results_dir, "actual_data_calibration.json"), "w", encoding="utf-8") as handle:
            json.dump(results.get("actual_data_calibration", {}), handle, ensure_ascii=False, indent=2)


def run_qullamaggie_screening(
    setup_type: str | None = None,
    enable_earnings_filter: bool | None = None,
    *,
    market: str = "us",
):
    normalized_market = market_key(market)
    if enable_earnings_filter is None:
        enable_earnings_filter = True
    screener = QullamaggieScreener(
        market=normalized_market,
        enable_earnings_filter=enable_earnings_filter,
    )
    return screener.run(setup_type=setup_type)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Qullamaggie screener")
    parser.add_argument("--setup", choices=["breakout", "episode_pivot", "parabolic_short"])
    parser.add_argument("--market", default="us", help="Target market (us|kr)")
    parser.add_argument("--disable-earnings-filter", action="store_true")
    args = parser.parse_args()

    run_qullamaggie_screening(
        setup_type=args.setup,
        enable_earnings_filter=not args.disable_earnings_filter,
        market=args.market,
    )


if __name__ == "__main__":
    main()
