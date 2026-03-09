#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main entrypoint for data collection and screener tasks."""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from datetime import datetime


def _add_project_root() -> str:
    """Ensure project root is in sys.path without importing heavy utility packages."""
    project_root = os.path.dirname(os.path.abspath(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    return project_root


def _resolve_markets_arg(market: str) -> list[str]:
    market_key = str(market or "us").lower().strip()
    if market_key in {"", "none", "null", "undefined"}:
        return ["us"]
    if market_key == "both":
        return ["us", "kr"]
    if "," in market_key:
        parts = [
            part.strip()
            for part in market_key.split(",")
            if part.strip() and part.strip() not in {"none", "null", "undefined"}
        ]
        return parts or ["us"]
    return [market_key or "us"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Invest prototype task runner")
    parser.add_argument("--skip-data", action="store_true", help="Skip OHLCV data collection")
    parser.add_argument("--force-screening", action="store_true", help="Reserved flag for compatibility")
    parser.add_argument("--no-symbol-update", action="store_true", help="Skip symbol list refresh for US")
    parser.add_argument("--include-kr-data", action="store_true", help="Collect KR CSVs during data phase")
    parser.add_argument(
        "--task",
        default="all",
        choices=[
            "all",
            "screening",
            "leader-stock",
            "momentum",
            "qullamaggie",
            "kr-collect",
            "breadth-external-collect",
        ],
        help="Task selector",
    )
    parser.add_argument(
        "--market",
        default="us",
        help="Target market for KR collection and breadth cache tasks (us|kr|both|csv list)",
    )
    parser.add_argument("--schedule", action="store_true", help="Run scheduler mode")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        _add_project_root()

        # Import runtime dependencies lazily to keep `import main` fast and predictable.
        from config import RESULTS_DIR
        from orchestrator.tasks import (
            collect_data_main,
            ensure_directories,
            run_all_screening_processes,
            run_kr_ohlcv_collection,
            run_market_breadth_external_collection,
            run_leader_stock_screener,
            run_momentum_signals_screener,
            run_qullamaggie_strategy_task,
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

        if task == "leader-stock":
            run_leader_stock_screener(skip_data=args.skip_data)
            return

        if task == "screening":
            run_all_screening_processes(skip_data=args.skip_data)
            return

        if task == "momentum":
            run_momentum_signals_screener(skip_data=args.skip_data)
            return

        if task == "qullamaggie":
            run_qullamaggie_strategy_task(skip_data=args.skip_data)
            return

        if task == "kr-collect":
            if "kr" not in markets:
                markets = ["kr"]
            if "kr" in markets:
                summary = run_kr_ohlcv_collection()
                print(f"[Main] KR collection summary: {summary}")
            return

        if task == "breadth-external-collect":
            summary = run_market_breadth_external_collection(markets=markets)
            print(f"[Main] breadth external cache summary: {summary}")
            return

        # task == "all"
        update_symbols = not args.no_symbol_update
        collect_data_main(
            update_symbols=update_symbols,
            skip_ohlcv=args.skip_data,
            include_kr=args.include_kr_data,
        )
        run_all_screening_processes(skip_data=args.skip_data)

        print("[Main] All tasks completed")
        print(f"[Main] End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except KeyboardInterrupt:
        print("[Main] Interrupted by user")
    except Exception as e:
        print(f"[Main] Fatal error: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
