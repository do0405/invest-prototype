"""Utility tasks for running collectors and screeners."""

from __future__ import annotations

import subprocess
import sys
import time
import traceback
from datetime import datetime
from typing import Any, Callable, Optional

from utils.market_runtime import ensure_market_dirs, market_key
from utils.yahoo_throttle import wait_for_yahoo_phase_handoff

__all__ = [
    "ensure_directories",
    "collect_data_main",
    "run_kr_ohlcv_collection",
    "run_all_screening_processes",
    "run_leader_lagging_screening",
    "run_stock_metadata_collection",
    "run_qullamaggie_strategy_task",
    "run_weinstein_stage2_screening",
    "run_scheduler",
    "setup_scheduler",
]


def _summarize_step_result(result: Any) -> str:
    if result is None:
        return ""
    if hasattr(result, "shape"):
        try:
            rows = int(result.shape[0])  # type: ignore[index]
            return f"rows={rows}"
        except Exception:
            return ""
    if isinstance(result, dict):
        for key in ("saved", "latest", "failed", "count", "total"):
            if key in result:
                return ", ".join(
                    f"{name}={result[name]}"
                    for name in ("total", "saved", "latest", "failed")
                    if name in result
                )
        return f"keys={len(result)}"
    if isinstance(result, (list, tuple, set)):
        return f"items={len(result)}"
    return ""


def _run_timed_step(
    step_number: int,
    total_steps: int,
    label: str,
    market: str,
    action: Callable[[], Any],
) -> Any:
    print(f"[Task] Step {step_number}/{total_steps} - {label} ({market})")
    started_at = time.perf_counter()
    result = action()
    elapsed = time.perf_counter() - started_at
    summary = _summarize_step_result(result)
    summary_suffix = f" - {summary}" if summary else ""
    print(f"[Task] Step {step_number}/{total_steps} completed ({market}) - {label} in {elapsed:.1f}s{summary_suffix}")
    return result


def run_stock_metadata_collection(*, market: str = "us") -> Any:
    print(f"\n[Task] Stock metadata collection started ({market})")
    try:
        from data_collectors.stock_metadata_collector import main as collect_stock_metadata_main

        result = collect_stock_metadata_main(market=market)
        row_count = len(result) if hasattr(result, "__len__") else 0
        print(f"[Task] Stock metadata collection completed ({market}) - rows={row_count}")
        return result
    except Exception as exc:
        print(f"[Task] Stock metadata collection failed ({market}): {exc}")
        print(traceback.format_exc())
        return None


def ensure_directories() -> None:
    from utils import create_required_dirs

    create_required_dirs()
    ensure_market_dirs("us")
    ensure_market_dirs("kr")


def collect_data_main(
    update_symbols: bool = True,
    skip_ohlcv: bool = False,
    include_kr: bool = False,
    skip_us_ohlcv: bool = False,
) -> None:
    print("\n[Task] Data collection started")
    try:
        from data_collector import collect_data

        steps: list[tuple[str, Callable[[], Any], bool]] = []
        if not skip_ohlcv:
            if not skip_us_ohlcv:
                steps.append(("US OHLCV", lambda: collect_data(update_symbols=update_symbols), True))
            if include_kr:
                steps.append(("KR OHLCV", lambda: run_kr_ohlcv_collection(include_etn=True), True))
        steps.append(("US stock metadata", lambda: run_stock_metadata_collection(market="us"), True))
        if include_kr:
            steps.append(("KR stock metadata", lambda: run_stock_metadata_collection(market="kr"), True))

        total_steps = len(steps)
        for index, (label, action, yahoo_backed) in enumerate(steps, start=1):
            if yahoo_backed:
                wait_for_yahoo_phase_handoff(label)
            _run_timed_step(index, total_steps, label, "pipeline", action)

        print("[Task] Data collection completed")
    except Exception as exc:
        print(f"[Task] Data collection failed: {exc}")
        print(traceback.format_exc())


def run_kr_ohlcv_collection(
    days: Optional[int] = None,
    include_kosdaq: bool = True,
    include_etf: bool = True,
    include_etn: bool = True,
    provider_mode: str = "yfinance_only",
) -> dict:
    try:
        import data_collectors.kr_ohlcv_collector as kr_ohlcv_collector

        effective_days = int(
            kr_ohlcv_collector.KR_OHLCV_DEFAULT_LOOKBACK_DAYS if days is None else days
        )

        summary = kr_ohlcv_collector.collect_kr_ohlcv_csv(
            days=effective_days,
            include_kosdaq=include_kosdaq,
            include_etf=include_etf,
            include_etn=include_etn,
            provider_mode=provider_mode,
        )
        soft_unavailable = summary.get("soft_unavailable", summary.get("skipped_empty", 0))
        delisted = summary.get("delisted", 0)
        print(
            "[Task] KR OHLCV completed - "
            f"source={summary.get('source')}, total={summary.get('total')}, "
            f"saved={summary.get('saved')}, latest={summary.get('latest')}, "
            f"kept_existing={summary.get('kept_existing')}, soft_unavailable={soft_unavailable}, "
            f"delisted={delisted}, rate_limited={summary.get('rate_limited')}, failed={summary.get('failed')}"
        )
        return summary
    except Exception as exc:
        print(f"[Task] KR OHLCV failed: {exc}")
        print(traceback.format_exc())
        return {"total": 0, "saved": 0, "failed": 0, "error": str(exc)}


def run_markminervini_screening(*, market: str) -> Any:
    from screeners.markminervini.screener import run_market_screening

    print(f"[Task] Mark Minervini technical screening ({market})")
    return run_market_screening(market=market)


def run_advanced_financial_screening(*, market: str, skip_data: bool) -> Any:
    from screeners.markminervini.advanced_financial import run_advanced_financial_screening

    print(f"[Task] Advanced financial screening ({market})")
    return run_advanced_financial_screening(skip_data=skip_data, market=market)


def run_integrated_screening(*, market: str) -> Any:
    from screeners.markminervini.integrated_screener import IntegratedScreener

    print(f"[Task] Integrated screening ({market})")
    return IntegratedScreener(market=market).run_integrated_screening()


def run_new_ticker_tracking(*, market: str) -> Any:
    from screeners.markminervini.ticker_tracker import track_new_tickers
    from utils.market_runtime import get_markminervini_advanced_financial_results_path

    print(f"[Task] New ticker tracking ({market})")
    return track_new_tickers(get_markminervini_advanced_financial_results_path(market), market=market)


def run_qullamaggie_strategy_task(
    setups: Optional[list[str]] | None = None,
    skip_data: bool = False,
    *,
    market: str = "us",
) -> Any:
    _ = skip_data
    try:
        from screeners.qullamaggie.screener import run_qullamaggie_screening
    except Exception as exc:
        print(f"[Task] Qullamaggie import failed ({market}): {exc}")
        return

    setup_type = None
    if setups:
        setup_type = setups[0] if len(setups) == 1 else None

    try:
        print(f"\n[Task] Qullamaggie screening started ({market})")
        result = run_qullamaggie_screening(setup_type=setup_type, market=market)
        print(f"[Task] Qullamaggie screening completed ({market})")
        return result
    except Exception as exc:
        print(f"[Task] Qullamaggie screening failed ({market}): {exc}")
        print(traceback.format_exc())
        return None


def run_tradingview_preset_screeners(*, market: str) -> Any:
    try:
        from screeners.tradingview.screener import run_tradingview_preset_screeners

        print(f"\n[Task] TradingView preset screeners started ({market})")
        results = run_tradingview_preset_screeners(market=market)
        preset_count = len(results)
        candidate_count = sum(len(frame) for frame in results.values())
        print(
            f"[Task] TradingView preset screeners completed ({market}) - "
            f"presets={preset_count}, candidates={candidate_count}"
        )
        return results
    except Exception as exc:
        print(f"[Task] TradingView preset screeners failed ({market}): {exc}")
        print(traceback.format_exc())
        return None


def run_weinstein_stage2_screening(*, market: str) -> Any:
    try:
        from screeners.weinstein_stage2.screener import run_weinstein_stage2_screening

        print(f"\n[Task] Weinstein Stage 2 screening started ({market})")
        result = run_weinstein_stage2_screening(market=market)
        print(f"[Task] Weinstein Stage 2 screening completed ({market})")
        return result
    except Exception as exc:
        print(f"[Task] Weinstein Stage 2 screening failed ({market}): {exc}")
        print(traceback.format_exc())
        return None


def run_leader_lagging_screening(*, market: str) -> Any:
    try:
        from screeners.leader_lagging.screener import run_leader_lagging_screening

        print(f"\n[Task] Leader / lagging screening started ({market})")
        result = run_leader_lagging_screening(market=market)
        print(f"[Task] Leader / lagging screening completed ({market})")
        return result
    except Exception as exc:
        print(f"[Task] Leader / lagging screening failed ({market}): {exc}")
        print(traceback.format_exc())
        return None


def run_all_screening_processes(skip_data: bool = False, markets: Optional[list[str]] = None) -> None:
    target_markets = [market_key(item) for item in (markets or ["us"])]
    print("\n[Task] Full screening process started")
    try:
        for market in target_markets:
            print(f"\n[Task] Market pipeline started ({market})")
            _run_timed_step(1, 8, "Mark Minervini technical", market, lambda: run_markminervini_screening(market=market))
            wait_for_yahoo_phase_handoff("Advanced financial")
            _run_timed_step(
                2,
                8,
                "Advanced financial",
                market,
                lambda: run_advanced_financial_screening(market=market, skip_data=skip_data),
            )
            _run_timed_step(3, 8, "Integrated screening", market, lambda: run_integrated_screening(market=market))
            _run_timed_step(4, 8, "New ticker tracking", market, lambda: run_new_ticker_tracking(market=market))
            _run_timed_step(5, 8, "Weinstein Stage 2", market, lambda: run_weinstein_stage2_screening(market=market))
            _run_timed_step(6, 8, "Leader / lagging", market, lambda: run_leader_lagging_screening(market=market))
            wait_for_yahoo_phase_handoff("Qullamaggie")
            _run_timed_step(
                7,
                8,
                "Qullamaggie",
                market,
                lambda: run_qullamaggie_strategy_task(skip_data=skip_data, market=market),
            )
            _run_timed_step(8, 8, "TradingView presets", market, lambda: run_tradingview_preset_screeners(market=market))

            print(f"[Task] Market pipeline completed ({market})")

        print("[Task] Full screening process completed")
    except Exception as exc:
        print(f"[Task] Full screening process failed: {exc}")
        print(traceback.format_exc())


def _convert_kst_to_local(time_str: str) -> str:
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
    _SCHED_CONF["full_time"] = full_run_time
    _SCHED_CONF["interval"] = keep_alive_interval
    print(
        "[Task] Scheduler configured - "
        f"daily full run after {full_run_time} KST, keep-alive interval {keep_alive_interval} min"
    )


def run_scheduler() -> None:
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
            run_all_screening_processes(skip_data=True, markets=["us"])
            now = datetime.now(kst_tz)
            if now.time() >= full_time and (last_full_date != now.date()):
                time.sleep(interval * 60)
                subprocess.run([sys.executable, "main.py"], check=False)
                last_full_date = datetime.now(kst_tz).date() if kst_tz else datetime.now().date()
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        print("\n[Task] Scheduler stopped")
