"""Utility tasks for running collectors and screeners."""

from __future__ import annotations

import os
import subprocess
import sys
import time
import traceback
from datetime import datetime
from typing import Optional

from config import (
    ADVANCED_FINANCIAL_RESULTS_PATH,
    LEADER_STOCK_RESULTS_DIR,
    MARKMINERVINI_RESULTS_DIR,
    MOMENTUM_SIGNALS_RESULTS_DIR,
    OPTION_RESULTS_DIR,
    RESULTS_DIR,
    SCREENER_RESULTS_DIR,
)

__all__ = [
    "ensure_directories",
    "collect_data_main",
    "run_kr_ohlcv_collection",
    "run_market_breadth_external_collection",
    "run_all_screening_processes",
    "run_leader_stock_screener",
    "run_momentum_signals_screener",
    "run_stock_metadata_collection",
    "run_qullamaggie_strategy_task",
    "run_keep_alive",
    "setup_scheduler",
    "run_scheduler",
]


def run_stock_metadata_collection() -> None:
    """Run stock metadata collection task."""
    print("\n[Task] Stock metadata collection started")
    try:
        from data_collectors.stock_metadata_collector import main as collect_stock_metadata_main

        collect_stock_metadata_main()
        print("[Task] Stock metadata collection completed")
    except Exception as e:
        print(f"[Task] Stock metadata collection failed: {e}")
        print(traceback.format_exc())


def ensure_directories() -> None:
    """Create required directories for the application."""
    from utils import create_required_dirs, ensure_dir

    create_required_dirs()
    additional = [
        SCREENER_RESULTS_DIR,
        OPTION_RESULTS_DIR,
        MARKMINERVINI_RESULTS_DIR,
        LEADER_STOCK_RESULTS_DIR,
        MOMENTUM_SIGNALS_RESULTS_DIR,
    ]
    for directory in additional:
        ensure_dir(directory)


def collect_data_main(
    update_symbols: bool = True,
    skip_ohlcv: bool = False,
    include_kr: bool = False,
) -> None:
    """Collect base datasets for downstream screeners."""
    print("\n[Task] Data collection started")
    try:
        from data_collector import collect_data

        print("[Task] Step 1/4 - Stock metadata")
        run_stock_metadata_collection()

        if not skip_ohlcv:
            print("[Task] Step 2/4 - US OHLCV")
            collect_data(update_symbols=update_symbols)
        else:
            print("[Task] Step 2/4 - US OHLCV skipped")

        if include_kr and not skip_ohlcv:
            print("[Task] Step 3/4 - KR OHLCV")
            run_kr_ohlcv_collection()
        elif include_kr:
            print("[Task] Step 3/4 - KR OHLCV skipped (skip_ohlcv=True)")

        external_markets = ["us"]
        if include_kr:
            external_markets.append("kr")
        print("[Task] Step 4/4 - Breadth external CSV caches")
        run_market_breadth_external_collection(markets=external_markets)

        print("[Task] Data collection completed")
    except Exception as e:
        print(f"[Task] Data collection failed: {e}")
        print(traceback.format_exc())


def run_kr_ohlcv_collection(
    days: int = 450,
    include_kosdaq: bool = True,
    include_etf: bool = True,
    include_etn: bool = False,
) -> dict:
    """Collect KR OHLCV data through pykrx and persist CSV artifacts."""
    try:
        from data_collectors.kr_ohlcv_collector import collect_kr_ohlcv_csv

        summary = collect_kr_ohlcv_csv(
            days=days,
            include_kosdaq=include_kosdaq,
            include_etf=include_etf,
            include_etn=include_etn,
        )
        print(
            "[Task] KR OHLCV completed - "
            f"total={summary.get('total')}, saved={summary.get('saved')}, failed={summary.get('failed')}"
        )
        return summary
    except Exception as e:
        print(f"[Task] KR OHLCV failed: {e}")
        print(traceback.format_exc())
        return {"total": 0, "saved": 0, "failed": 0, "error": str(e)}


def run_market_breadth_external_collection(markets: Optional[list[str]] = None, as_of: Optional[str] = None) -> dict:
    """Collect ETF flow/RS caches used by breadth calculations."""
    try:
        from data_collectors.market_breadth_external_collector import collect_market_breadth_external_csv
    except Exception as e:
        print(f"[Task] breadth external collector import failed: {e}")
        return {"success": False, "results": {}, "errors": {"import": str(e)}}

    target_markets = markets or ["us"]
    results: dict[str, dict] = {}
    errors: dict[str, str] = {}
    success = True

    for market in target_markets:
        market_key = (market or "us").lower().strip()
        try:
            summary = collect_market_breadth_external_csv(market=market_key, as_of=as_of)
            results[market_key] = summary
        except Exception as e:
            success = False
            errors[market_key] = str(e)

    if errors:
        success = False
    print(f"[Task] Breadth external cache completed - success={success}, errors={errors}")
    return {"success": success, "results": results, "errors": errors}


def run_all_screening_processes(skip_data: bool = False) -> None:
    """Execute all screening processes in sequence."""
    print("\n[Task] Full screening process started")
    try:
        from screeners.markminervini.screener import run_us_screening
        from screeners.markminervini.advanced_financial import run_advanced_financial_screening
        from screeners.markminervini.integrated_screener import IntegratedScreener
        from screeners.markminervini.ticker_tracker import track_new_tickers

        if skip_data:
            print("[Task] skip_data=True (screening-only run)")

        print("[Task] Step 1/8 - US technical screening")
        run_us_screening()

        print("[Task] Step 2/8 - Advanced financial screening")
        run_advanced_financial_screening(skip_data=skip_data)

        print("[Task] Step 3/8 - Integrated screening")
        screener = IntegratedScreener()
        screener.run_integrated_screening()

        print("[Task] Step 4/8 - New ticker tracking")
        track_new_tickers(ADVANCED_FINANCIAL_RESULTS_PATH)

        print("[Task] Step 5/8 - Leader stock screening")
        run_leader_stock_screener(skip_data=skip_data)

        print("[Task] Step 6/8 - Momentum signals screening")
        run_momentum_signals_screener(skip_data=skip_data)

        print("[Task] Step 7/8 - Qullamaggie strategy")
        run_qullamaggie_strategy_task(skip_data=skip_data)

        print("[Task] Step 8/8 - Breadth external CSV caches")
        run_market_breadth_external_collection(markets=["us"])
        print("[Task] Full screening process completed")
    except Exception as e:
        print(f"[Task] Full screening process failed: {e}")
        print(traceback.format_exc())


def run_leader_stock_screener(skip_data: bool = False) -> None:
    """Run the leader stock screener."""
    try:
        from screeners.leader_stock.screener import run_leader_stock_screening
        from utils.first_buy_tracker import update_first_buy_signals

        print("\n[Task] Leader stock screener started")
        df = run_leader_stock_screening(skip_data=skip_data)
        if not df.empty:
            print(f"[Task] Leader stock screener completed: {len(df)} symbols")
            update_first_buy_signals(df, LEADER_STOCK_RESULTS_DIR)
        else:
            print("[Task] Leader stock screener returned no symbols")
    except Exception as e:
        print(f"[Task] Leader stock screener failed: {e}")
        print(traceback.format_exc())


def run_momentum_signals_screener(skip_data: bool = False) -> None:
    """Run the momentum signals screener."""
    try:
        from screeners.momentum_signals.screener import run_momentum_signals_screening
        from utils.first_buy_tracker import update_first_buy_signals

        print("\n[Task] Momentum signals screener started")
        df = run_momentum_signals_screening(skip_data=skip_data)
        if not df.empty:
            print(f"[Task] Momentum signals screener completed: {len(df)} symbols")
            update_first_buy_signals(df, MOMENTUM_SIGNALS_RESULTS_DIR)
        else:
            print("[Task] Momentum signals screener returned no symbols")
    except Exception as e:
        print(f"[Task] Momentum signals screener failed: {e}")
        print(traceback.format_exc())


def run_qullamaggie_strategy_task(
    setups: Optional[list[str]] | None = None, skip_data: bool = False
) -> None:
    """Run the Qullamaggie strategy."""
    try:
        from screeners.qullamaggie import run_qullamaggie_strategy
    except Exception as e:
        print(f"[Task] Qullamaggie import failed: {e}")
        return

    try:
        print("\n[Task] Qullamaggie strategy started")
        run_qullamaggie_strategy(setups, skip_data=skip_data)
        print("[Task] Qullamaggie strategy completed")
    except Exception as e:
        print(f"[Task] Qullamaggie strategy failed: {e}")
        print(traceback.format_exc())


def run_keep_alive() -> None:
    """Run a lightweight cycle for keep-alive schedules."""
    run_all_screening_processes(skip_data=True)


def _convert_kst_to_local(time_str: str) -> str:
    """Convert HH:MM KST to local timezone HH:MM."""
    try:
        import pytz
    except Exception:
        return time_str

    kst = pytz.timezone("Asia/Seoul")
    local_tz = datetime.now().astimezone().tzinfo
    dt = datetime.strptime(time_str, "%H:%M")
    kst_dt = kst.localize(dt)
    return kst_dt.astimezone(local_tz).strftime("%H:%M")


_SCHED_CONF = {"full_time": "14:30", "interval": 1}


def setup_scheduler(full_run_time: str = "14:30", keep_alive_interval: int = 1) -> None:
    """Configure scheduler settings."""
    _SCHED_CONF["full_time"] = full_run_time
    _SCHED_CONF["interval"] = keep_alive_interval
    print(
        "[Task] Scheduler configured - "
        f"daily full run after {full_run_time} KST, keep-alive interval {keep_alive_interval} min"
    )


def run_scheduler() -> None:
    """Run keep-alive loop with one daily full run after configured time."""
    try:
        import pytz
    except Exception:
        pytz = None

    full_time = datetime.strptime(_SCHED_CONF["full_time"], "%H:%M").time()
    interval = _SCHED_CONF["interval"]
    kst_tz = pytz.timezone("Asia/Seoul") if pytz else None
    last_full_date = None

    print("[Task] Scheduler started (Ctrl+C to stop)")
    try:
        while True:
            run_keep_alive()
            now = datetime.now(kst_tz)
            if now.time() >= full_time and (last_full_date != now.date()):
                time.sleep(interval * 60)
                subprocess.run([sys.executable, "main.py"], check=False)
                last_full_date = datetime.now(kst_tz).date() if kst_tz else datetime.now().date()
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        print("\n[Task] Scheduler stopped")
