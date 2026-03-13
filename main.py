#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main entrypoint for data collection and screener tasks."""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from datetime import datetime


_SUPPORTED_MARKETS = {"us", "kr"}


def _add_project_root() -> str:
    """Ensure project root is in sys.path without importing heavy utility packages."""
    project_root = os.path.dirname(os.path.abspath(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    return project_root


def _resolve_markets_arg(market: str) -> list[str]:
    market_value = str(market or "us").lower().strip()
    if market_value in {"", "none", "null", "undefined"}:
        return ["us"]
    if market_value == "both":
        return ["us", "kr"]
    raw_parts = market_value.split(",") if "," in market_value else [market_value]

    markets: list[str] = []
    for raw_part in raw_parts:
        part = raw_part.strip()
        if not part or part in {"none", "null", "undefined"}:
            continue
        if part not in _SUPPORTED_MARKETS:
            continue
        if part not in markets:
            markets.append(part)

    return markets or ["us"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Invest prototype task runner")
    parser.add_argument("--skip-data", action="store_true", help="Skip OHLCV data collection")
    parser.add_argument("--skip-us-ohlcv", action="store_true", help="Skip US OHLCV collection only")
    parser.add_argument("--force-screening", action="store_true", help="Reserved flag for compatibility")
    parser.add_argument("--no-symbol-update", action="store_true", help="Skip symbol list refresh for US")
    parser.add_argument("--include-kr-data", action="store_true", help="Collect KR CSVs during data phase")
    parser.add_argument(
        "--task",
        default="all",
        choices=[
            "all",
            "screening",
            "momentum",
            "leader",
            "qullamaggie",
            "kr-collect",
            "tradingview",
            "weinstein",
        ],
        help="Task selector",
    )
    parser.add_argument(
        "--market",
        default="us",
        help="Target market for screening and KR collection tasks (us|kr|both|csv list)",
    )
    parser.add_argument("--schedule", action="store_true", help="Run scheduler mode")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        _add_project_root()
        from utils.console_runtime import bootstrap_windows_utf8
        from utils.yfinance_runtime import bootstrap_yfinance_cache

        # Import runtime dependencies lazily to keep `import main` fast and predictable.
        bootstrap_windows_utf8()
        bootstrap_yfinance_cache()

        from config import RESULTS_DIR
        from orchestrator.tasks import (
            collect_data_main,
            ensure_directories,
            run_all_screening_processes,
            run_kr_ohlcv_collection,
            run_leader_lagging_screening,
            run_tradingview_preset_screeners,
            run_qullamaggie_strategy_task,
            run_weinstein_stage2_screening,
            run_scheduler,
            setup_scheduler,
        )
        from utils.file_cleanup import cleanup_old_timestamped_files

        print("[Main] Task runner started")
        print(f"[Main] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print("[Main] Ensuring directories")
        ensure_directories()

        print("[Main] Cleaning old timestamped files")
        cleanup_result = cleanup_old_timestamped_files(
            directory=RESULTS_DIR,
            days_threshold=30,
            extensions=[".csv", ".json"],
            dry_run=False,
        )
        print(f"[Main] Cleanup deleted={cleanup_result.get('deleted_count', 0)}")

        if args.schedule:
            print("[Main] Scheduler mode")
            setup_scheduler()
            run_scheduler()
            return

        task = args.task
        markets = _resolve_markets_arg(args.market)

        if task == "screening":
            run_all_screening_processes(skip_data=args.skip_data, markets=markets)
            return

        if task == "momentum":
            print("[Main] Momentum task is deprecated and no longer runs from orchestrator")
            return

        if task == "leader":
            for market in markets:
                run_leader_lagging_screening(market=market)
            return

        if task == "qullamaggie":
            for market in markets:
                run_qullamaggie_strategy_task(skip_data=args.skip_data, market=market)
            return

        if task == "tradingview":
            for market in markets:
                run_tradingview_preset_screeners(market=market)
            return

        if task == "weinstein":
            for market in markets:
                run_weinstein_stage2_screening(market=market)
            return

        if task == "kr-collect":
            if "kr" not in markets:
                markets = ["kr"]
            if "kr" in markets:
                summary = run_kr_ohlcv_collection()
                print(f"[Main] KR collection summary: {summary}")
            return

        # task == "all"
        update_symbols = not args.no_symbol_update
        include_kr_runtime = args.include_kr_data or ("kr" in markets)
        print(
            f"[Main] Data phase started - markets={markets}, "
            f"skip_data={args.skip_data}, skip_us_ohlcv={args.skip_us_ohlcv}, "
            f"include_kr_data={include_kr_runtime}"
        )
        collect_data_main(
            update_symbols=update_symbols,
            skip_ohlcv=args.skip_data,
            include_kr=include_kr_runtime,
            skip_us_ohlcv=args.skip_us_ohlcv,
        )
        print("[Main] Data phase completed")
        print(f"[Main] Screening phase started - markets={markets}")
        run_all_screening_processes(skip_data=args.skip_data, markets=markets)
        print("[Main] Screening phase completed")

        print("[Main] All tasks completed")
        print(f"[Main] End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except KeyboardInterrupt:
        print("[Main] Interrupted by user")
    except Exception as e:
        print(f"[Main] Fatal error: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
