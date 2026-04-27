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


def _unstable_runtime_reason(
    *,
    os_name: str | None = None,
    version_info: object | None = None,
    executable: str | None = None,
    prefix: str | None = None,
    base_prefix: str | None = None,
    override: str | None = None,
) -> str | None:
    resolved_os_name = os.name if os_name is None else str(os_name)
    if resolved_os_name != "nt":
        return None

    resolved_version = sys.version_info if version_info is None else version_info
    major_minor = tuple(resolved_version[:2])
    if major_minor != (3, 13):
        return None

    resolved_override = (
        os.environ.get("INVEST_PROTO_ALLOW_UNSTABLE_VENV_PY313", "")
        if override is None
        else str(override)
    )
    if resolved_override.strip() == "1":
        return None

    resolved_executable = os.path.abspath(sys.executable if executable is None else executable)
    resolved_prefix = sys.prefix if prefix is None else prefix
    resolved_base_prefix = getattr(sys, "base_prefix", resolved_prefix) if base_prefix is None else base_prefix

    normalized_prefix = os.path.normcase(os.path.abspath(str(resolved_prefix)))
    normalized_base_prefix = os.path.normcase(os.path.abspath(str(resolved_base_prefix)))
    normalized_executable = os.path.normcase(resolved_executable)
    in_venv = normalized_prefix != normalized_base_prefix or f"{os.sep}.venv{os.sep}" in normalized_executable
    if not in_venv:
        return None

    return (
        "[Main] Unstable runtime blocked - Windows + Python 3.13 venv detected. "
        f"Executable={resolved_executable}. "
        "This path has crashed with access violations on this machine. "
        "Run the pipeline with a non-venv interpreter instead. "
        "On Windows, prefer system Python 3.12 "
        "(for example, `C:\\Users\\<user>\\AppData\\Local\\Programs\\Python\\Python312\\python.exe main.py ...`). "
        "If you need to bypass this guard temporarily, set "
        "`INVEST_PROTO_ALLOW_UNSTABLE_VENV_PY313=1`."
    )



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
    invalid_parts: list[str] = []
    for raw_part in raw_parts:
        part = raw_part.strip()
        if not part or part in {"none", "null", "undefined"}:
            continue
        if part not in _SUPPORTED_MARKETS:
            invalid_parts.append(part)
            continue
        if part not in markets:
            markets.append(part)

    if invalid_parts:
        invalid_csv = ", ".join(sorted(set(invalid_parts)))
        raise ValueError(f"Unsupported market: {invalid_csv}")

    return markets or ["us"]



def _summary_ok(summary: object) -> bool:
    if isinstance(summary, dict):
        if summary.get("ok") is False:
            return False
        if str(summary.get("status") or "").strip().lower() in {"failed", "degraded"}:
            return False
        if int(summary.get("failed_steps") or 0) > 0:
            return False
        if str(summary.get("error") or "").strip():
            return False
        if int(summary.get("failed") or 0) > 0:
            return False
    return True


def _full_run_label(*, standalone: bool, analysis_summary: object) -> str:
    if standalone:
        return "Full live standalone e2e"
    if isinstance(analysis_summary, dict):
        modes = analysis_summary.get("market_truth_modes")
        if isinstance(modes, dict) and any(
            str(mode).strip() == "standalone_auto"
            for mode in modes.values()
        ):
            return "Full live auto-fallback e2e"
    return "Full live e2e"


def _print_task_summary(task_label: str, summary: object) -> None:
    if _summary_ok(summary):
        print(f"[Main] {task_label} completed")
    else:
        print(f"[Main] {task_label} completed with failures")


def _exit_if_failed(summary: object) -> None:
    if not _summary_ok(summary):
        raise SystemExit(1)


def _resolve_cleanup_results_dir(results_dir: str) -> str:
    override = str(os.environ.get("INVEST_PROTO_RESULTS_DIR") or "").strip()
    if not override:
        return results_dir
    if os.path.isabs(override):
        return os.path.abspath(override)
    project_root = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(project_root, override))



def _combine_task_results(results: list[object]) -> dict[str, object]:
    failed_results = [result for result in results if not _summary_ok(result)]
    summary: dict[str, object] = {
        "ok": not failed_results,
        "status": "ok" if not failed_results else "failed",
        "failed_steps": len(failed_results),
        "total_steps": len(results),
    }
    if failed_results:
        summary["error"] = "; ".join(
            str(
                result.get("error")
                if isinstance(result, dict)
                else getattr(result, "error", "") or "failed"
            ).strip()
            or "failed"
            for result in failed_results
        )
    return summary



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Invest prototype task runner")
    parser.add_argument("--skip-data", action="store_true", help="Skip OHLCV data collection")
    parser.add_argument("--skip-us-ohlcv", action="store_true", help="Skip US OHLCV collection only")
    parser.add_argument("--no-symbol-update", action="store_true", help="Skip symbol list refresh for US")
    parser.add_argument("--include-kr-data", action="store_true", help="Collect KR CSVs during data phase")
    parser.add_argument(
        "--task",
        default="all",
        choices=[
            "all",
            "screening",
            "signals",
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
    parser.add_argument(
        "--enable-augment",
        action="store_true",
        help="Run diagnostics-only screening augment after screening and before signals",
    )
    parser.add_argument(
        "--standalone",
        action="store_true",
        help="Run screening and signals without reading market-intel-core compat artifacts",
    )
    parser.add_argument(
        "--as-of",
        dest="as_of_date",
        help="Replay screening/signals as of YYYY-MM-DD instead of resolving latest completed market day",
    )
    parser.add_argument(
        "--cleanup-old-results",
        action="store_true",
        help="Opt in to deleting old timestamped CSV/JSON result files before running",
    )
    parser.add_argument("--schedule", action="store_true", help="Run scheduler mode")
    return parser



def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        _add_project_root()
        runtime_warning = _unstable_runtime_reason()
        if runtime_warning is not None:
            print(runtime_warning)
            raise SystemExit(2)
        from utils.console_runtime import bootstrap_windows_utf8
        from utils.yfinance_runtime import bootstrap_yfinance_cache

        # Import runtime dependencies lazily to keep `import main` fast and predictable.
        bootstrap_windows_utf8()
        bootstrap_yfinance_cache()

        from config import RESULTS_DIR
        from orchestrator.tasks import (
            collect_data_main,
            ensure_directories,
            run_kr_ohlcv_collection,
            run_leader_lagging_screening,
            run_market_analysis_pipeline,
            run_qullamaggie_strategy_task,
            run_scheduler,
            run_signal_engine_processes,
            run_tradingview_preset_screeners,
            run_weinstein_stage2_screening,
            setup_scheduler,
            write_full_run_summaries,
        )
        from utils.file_cleanup import cleanup_old_timestamped_files

        task = args.task
        if args.enable_augment and (args.schedule or task not in {"all", "screening"}):
            parser.error("--enable-augment is only supported with --task screening or --task all")
        markets = ["us"] if args.schedule else _resolve_markets_arg(args.market)
        include_signal_dirs = bool(args.schedule or task in {"all", "signals"})
        include_augment_dirs = bool(args.enable_augment and task in {"all", "screening"})

        print("[Main] Task runner started")
        print(f"[Main] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print("[Main] Ensuring directories")
        ensure_directories(
            markets=markets,
            include_signal_dirs=include_signal_dirs,
            include_augment_dirs=include_augment_dirs,
        )

        if args.cleanup_old_results:
            print("[Main] Cleaning old timestamped files")
            cleanup_result = cleanup_old_timestamped_files(
                directory=_resolve_cleanup_results_dir(RESULTS_DIR),
                days_threshold=30,
                extensions=[".csv", ".json"],
                dry_run=False,
            )
            print(f"[Main] Cleanup deleted={cleanup_result.get('deleted_count', 0)}")
        else:
            print("[Main] Old result cleanup skipped (--cleanup-old-results not set)")

        if args.schedule:
            print("[Main] Scheduler mode")
            setup_scheduler()
            run_scheduler()
            return

        if task == "screening":
            pipeline_kwargs = {
                "skip_data": args.skip_data,
                "markets": markets,
                "include_signals": False,
                "enable_augment": args.enable_augment,
                "standalone": args.standalone,
            }
            if args.as_of_date:
                pipeline_kwargs["as_of_date"] = args.as_of_date
            summary = run_market_analysis_pipeline(**pipeline_kwargs)
            _print_task_summary("Screening task", summary)
            _exit_if_failed(summary)
            return

        if task == "signals":
            signal_kwargs = {"markets": markets, "standalone": args.standalone}
            if args.skip_data:
                signal_kwargs["local_only"] = True
            if args.as_of_date:
                signal_kwargs["as_of_date"] = args.as_of_date
            summary = run_signal_engine_processes(**signal_kwargs)
            _print_task_summary("Signals task", summary)
            _exit_if_failed(summary)
            return

        if task == "leader":
            results: list[object] = []
            for market in markets:
                task_kwargs = {"market": market, "standalone": args.standalone}
                if args.as_of_date:
                    task_kwargs["as_of_date"] = args.as_of_date
                results.append(run_leader_lagging_screening(**task_kwargs))
            summary = _combine_task_results(results)
            _print_task_summary("Leader task", summary)
            _exit_if_failed(summary)
            return

        if task == "qullamaggie":
            results: list[object] = []
            for market in markets:
                task_kwargs = {
                    "skip_data": args.skip_data,
                    "market": market,
                    "standalone": args.standalone,
                }
                if args.as_of_date:
                    task_kwargs["as_of_date"] = args.as_of_date
                results.append(run_qullamaggie_strategy_task(**task_kwargs))
            summary = _combine_task_results(results)
            _print_task_summary("Qullamaggie task", summary)
            _exit_if_failed(summary)
            return

        if task == "tradingview":
            results: list[object] = []
            for market in markets:
                task_kwargs = {"market": market, "standalone": args.standalone}
                if args.as_of_date:
                    task_kwargs["as_of_date"] = args.as_of_date
                results.append(run_tradingview_preset_screeners(**task_kwargs))
            summary = _combine_task_results(results)
            _print_task_summary("TradingView task", summary)
            _exit_if_failed(summary)
            return

        if task == "weinstein":
            results: list[object] = []
            for market in markets:
                task_kwargs = {"market": market, "standalone": args.standalone}
                if args.as_of_date:
                    task_kwargs["as_of_date"] = args.as_of_date
                results.append(run_weinstein_stage2_screening(**task_kwargs))
            summary = _combine_task_results(results)
            _print_task_summary("Weinstein task", summary)
            _exit_if_failed(summary)
            return

        if task == "kr-collect":
            if "kr" not in markets:
                markets = ["kr"]
            if "kr" in markets:
                summary = run_kr_ohlcv_collection()
                print(f"[Main] KR collection summary: {summary}")
                _exit_if_failed(summary)
            return

        # task == "all"
        update_symbols = not args.no_symbol_update
        include_us_runtime = "us" in markets
        include_kr_runtime = args.include_kr_data or ("kr" in markets)
        if args.skip_data:
            print("[Main] Data phase skipped (--skip-data)")
            data_summary = {"ok": True, "failed_steps": 0, "skipped": True}
        else:
            print(
                f"[Main] Data phase started - markets={markets}, "
                f"skip_data={args.skip_data}, skip_us_ohlcv={args.skip_us_ohlcv}, "
                f"include_kr_data={include_kr_runtime}"
            )
            data_summary = collect_data_main(
                update_symbols=update_symbols,
                skip_ohlcv=False,
                include_kr=include_kr_runtime,
                include_us=include_us_runtime,
                skip_us_ohlcv=args.skip_us_ohlcv,
            )
            print("[Main] Data phase completed")
        print(f"[Main] Analysis phase started - markets={markets}")
        pipeline_kwargs = {
            "skip_data": args.skip_data,
            "markets": markets,
            "include_signals": True,
            "enable_augment": args.enable_augment,
            "standalone": args.standalone,
        }
        if args.as_of_date:
            pipeline_kwargs["as_of_date"] = args.as_of_date
        analysis_summary = run_market_analysis_pipeline(**pipeline_kwargs)
        print("[Main] Analysis phase completed")

        pipeline_ok = _summary_ok(data_summary) and _summary_ok(analysis_summary)
        full_run_label = _full_run_label(
            standalone=args.standalone,
            analysis_summary=analysis_summary,
        )
        full_run_summary = {
            "label": full_run_label,
            "ok": pipeline_ok,
            "status": "ok" if pipeline_ok else "failed",
            "failed_steps": (
                int(data_summary.get("failed_steps", 0) or 0)
                if isinstance(data_summary, dict)
                else 0
            )
            + (
                int(analysis_summary.get("failed_steps", 0) or 0)
                if isinstance(analysis_summary, dict)
                else 0
            ),
            "summaries": [data_summary, analysis_summary],
        }
        write_full_run_summaries(
            full_run_label,
            full_run_summary,
            markets=markets,
        )
        if pipeline_ok:
            print("[Main] All tasks completed")
        else:
            print("[Main] Pipeline completed with failures")
        print(f"[Main] End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if not pipeline_ok:
            raise SystemExit(1)
    except KeyboardInterrupt:
        print("[Main] Interrupted by user")
    except ValueError as exc:
        parser.error(str(exc))
    except Exception as e:
        print(f"[Main] Fatal error: {e}")
        print(traceback.format_exc())
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
