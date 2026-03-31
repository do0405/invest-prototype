"""Utility tasks for running collectors and screeners."""

from __future__ import annotations

import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

from utils.market_runtime import ensure_market_dirs, market_key, require_market_key
from utils.yahoo_throttle import wait_for_yahoo_phase_handoff

__all__ = [
    "ensure_directories",
    "collect_data_main",
    "run_kr_ohlcv_collection",
    "run_all_screening_processes",
    "run_market_analysis_pipeline",
    "run_signal_engine_processes",
    "run_leader_lagging_screening",
    "run_stock_metadata_collection",
    "run_qullamaggie_strategy_task",
    "run_weinstein_stage2_screening",
    "run_scheduler",
    "setup_scheduler",
]


@dataclass
class TaskStepOutcome:
    ok: bool
    label: str
    market: str
    elapsed_seconds: float
    summary: str = ""
    error: str = ""
    result: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "label": self.label,
            "market": self.market,
            "elapsed_seconds": self.elapsed_seconds,
            "summary": self.summary,
            "error": self.error,
        }



def _normalize_markets(markets: Optional[list[str]] = None) -> list[str]:
    normalized_markets: list[str] = []
    for item in markets or ["us"]:
        normalized = require_market_key(item)
        if normalized not in normalized_markets:
            normalized_markets.append(normalized)
    return normalized_markets



def _is_failure_result(result: Any) -> bool:
    if isinstance(result, TaskStepOutcome):
        return not result.ok
    if isinstance(result, dict):
        if result.get("ok") is False:
            return True
        error = str(result.get("error") or "").strip()
        if error:
            return True
    return False



def _extract_error(result: Any) -> str:
    if isinstance(result, TaskStepOutcome):
        return result.error
    if isinstance(result, dict):
        error = str(result.get("error") or "").strip()
        if error:
            return error
        if result.get("ok") is False and "failed_steps" in result:
            return f"failed_steps={result.get('failed_steps')}"
    return ""



def _summarize_step_result(result: Any) -> str:
    if result is None:
        return ""
    if isinstance(result, TaskStepOutcome):
        return result.summary
    if hasattr(result, "shape"):
        try:
            rows = int(result.shape[0])  # type: ignore[index]
            return f"rows={rows}"
        except Exception:
            return ""
    if isinstance(result, dict):
        if result.get("ok") is False and "failed_steps" in result:
            return f"failed_steps={result.get('failed_steps')}"
        error = str(result.get("error") or "").strip()
        if error:
            return f"error={error}"
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
) -> TaskStepOutcome:
    print(f"[Task] Step {step_number}/{total_steps} - {label} ({market})")
    started_at = time.perf_counter()
    try:
        result = action()
        ok = not _is_failure_result(result)
        error = _extract_error(result)
    except Exception as exc:
        result = None
        ok = False
        error = str(exc)
        print(f"[Task] Step {step_number}/{total_steps} raised ({market}) - {label}: {exc}")
        print(traceback.format_exc())
    elapsed = time.perf_counter() - started_at
    summary = _summarize_step_result(result)
    summary_suffix = f" - {summary}" if summary else ""
    if ok:
        print(f"[Task] Step {step_number}/{total_steps} completed ({market}) - {label} in {elapsed:.1f}s{summary_suffix}")
    else:
        error_suffix = f" - {error}" if error and not summary else ""
        print(f"[Task] Step {step_number}/{total_steps} failed ({market}) - {label} in {elapsed:.1f}s{summary_suffix}{error_suffix}")
    return TaskStepOutcome(
        ok=ok,
        label=label,
        market=market,
        elapsed_seconds=elapsed,
        summary=summary,
        error=error,
        result=result,
    )



def _build_process_summary(label: str, outcomes: list[TaskStepOutcome]) -> dict[str, Any]:
    failed = [outcome for outcome in outcomes if not outcome.ok]
    elapsed = sum(outcome.elapsed_seconds for outcome in outcomes)
    summary = {
        "label": label,
        "ok": not failed,
        "failed_steps": len(failed),
        "total_steps": len(outcomes),
        "elapsed_seconds": elapsed,
        "markets": [outcome.market for outcome in outcomes],
        "steps": [outcome.to_dict() for outcome in outcomes],
    }
    if failed:
        summary["error"] = "; ".join(
            f"{outcome.market}:{outcome.label}:{outcome.error or outcome.summary or 'failed'}"
            for outcome in failed
        )
        print(
            f"[Task] {label} completed with degraded status - "
            f"failed_steps={len(failed)}, total_steps={len(outcomes)}, elapsed={elapsed:.1f}s"
        )
    else:
        print(f"[Task] {label} completed - total_steps={len(outcomes)}, elapsed={elapsed:.1f}s")
    return summary



def _unexpected_process_summary(label: str, exc: Exception) -> dict[str, Any]:
    print(f"[Task] {label} failed: {exc}")
    print(traceback.format_exc())
    return {
        "label": label,
        "ok": False,
        "failed_steps": 1,
        "total_steps": 0,
        "elapsed_seconds": 0.0,
        "markets": [],
        "steps": [],
        "error": str(exc),
    }



def _combine_process_summaries(label: str, summaries: list[dict[str, Any]]) -> dict[str, Any]:
    valid_summaries = [summary for summary in summaries if summary]
    failed_steps = sum(int(summary.get("failed_steps", 0)) for summary in valid_summaries)
    total_steps = sum(int(summary.get("total_steps", 0)) for summary in valid_summaries)
    elapsed = sum(float(summary.get("elapsed_seconds", 0.0)) for summary in valid_summaries)
    combined = {
        "label": label,
        "ok": failed_steps == 0,
        "failed_steps": failed_steps,
        "total_steps": total_steps,
        "elapsed_seconds": elapsed,
        "summaries": valid_summaries,
    }
    if failed_steps:
        combined["error"] = "; ".join(
            str(summary.get("error") or summary.get("label") or "failed")
            for summary in valid_summaries
            if summary.get("ok") is False
        )
        print(
            f"[Task] {label} completed with degraded status - "
            f"failed_steps={failed_steps}, total_steps={total_steps}, elapsed={elapsed:.1f}s"
        )
    else:
        print(f"[Task] {label} completed - total_steps={total_steps}, elapsed={elapsed:.1f}s")
    return combined



def run_stock_metadata_collection(*, market: str = "us") -> Any:
    normalized_market = require_market_key(market)
    print(f"\n[Task] Stock metadata collection started ({normalized_market})")
    try:
        from data_collectors.stock_metadata_collector import main as collect_stock_metadata_main

        result = collect_stock_metadata_main(market=normalized_market)
        row_count = len(result) if hasattr(result, "__len__") else 0
        print(f"[Task] Stock metadata collection completed ({normalized_market}) - rows={row_count}")
        return result
    except Exception as exc:
        print(f"[Task] Stock metadata collection failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



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
) -> dict[str, Any]:
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

        outcomes: list[TaskStepOutcome] = []
        total_steps = len(steps)
        for index, (label, action, yahoo_backed) in enumerate(steps, start=1):
            if yahoo_backed:
                wait_for_yahoo_phase_handoff(label)
            outcomes.append(_run_timed_step(index, total_steps, label, "pipeline", action))

        return _build_process_summary("Data collection", outcomes)
    except Exception as exc:
        return _unexpected_process_summary("Data collection", exc)



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
    normalized_market = require_market_key(market)
    from screeners.markminervini.screener import run_market_screening

    print(f"[Task] Mark Minervini technical screening ({normalized_market})")
    return run_market_screening(market=normalized_market)



def run_advanced_financial_screening(*, market: str, skip_data: bool) -> Any:
    normalized_market = require_market_key(market)
    from screeners.markminervini.advanced_financial import run_advanced_financial_screening

    print(f"[Task] Advanced financial screening ({normalized_market})")
    return run_advanced_financial_screening(skip_data=skip_data, market=normalized_market)



def run_integrated_screening(*, market: str) -> Any:
    normalized_market = require_market_key(market)
    from screeners.markminervini.integrated_screener import IntegratedScreener

    print(f"[Task] Integrated screening ({normalized_market})")
    return IntegratedScreener(market=normalized_market).run_integrated_screening()



def run_new_ticker_tracking(*, market: str) -> Any:
    normalized_market = require_market_key(market)
    from screeners.markminervini.ticker_tracker import track_new_tickers
    from utils.market_runtime import get_markminervini_advanced_financial_results_path

    print(f"[Task] New ticker tracking ({normalized_market})")
    return track_new_tickers(get_markminervini_advanced_financial_results_path(normalized_market), market=normalized_market)



def run_qullamaggie_strategy_task(
    setups: Optional[list[str]] | None = None,
    skip_data: bool = False,
    *,
    market: str = "us",
) -> Any:
    normalized_market = require_market_key(market)
    _ = skip_data
    try:
        from screeners.qullamaggie.screener import run_qullamaggie_screening
    except Exception as exc:
        print(f"[Task] Qullamaggie import failed ({normalized_market}): {exc}")
        return {"error": str(exc), "market": normalized_market}

    setup_type = None
    if setups:
        setup_type = setups[0] if len(setups) == 1 else None

    try:
        print(f"\n[Task] Qullamaggie screening started ({normalized_market})")
        result = run_qullamaggie_screening(setup_type=setup_type, market=normalized_market)
        print(f"[Task] Qullamaggie screening completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Qullamaggie screening failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def run_tradingview_preset_screeners(*, market: str) -> Any:
    normalized_market = require_market_key(market)
    try:
        from screeners.tradingview.screener import run_tradingview_preset_screeners

        print(f"\n[Task] TradingView preset screeners started ({normalized_market})")
        results = run_tradingview_preset_screeners(market=normalized_market)
        preset_count = len(results)
        candidate_count = sum(len(frame) for frame in results.values())
        print(
            f"[Task] TradingView preset screeners completed ({normalized_market}) - "
            f"presets={preset_count}, candidates={candidate_count}"
        )
        return results
    except Exception as exc:
        print(f"[Task] TradingView preset screeners failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def run_signal_engine_task(*, market: str) -> Any:
    normalized_market = require_market_key(market)
    try:
        from screeners.signals import run_signal_scan

        print(f"\n[Task] Multi-screener signal engine started ({normalized_market})")
        result = run_signal_scan(market=normalized_market)
        print(f"[Task] Multi-screener signal engine completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Multi-screener signal engine failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def run_signal_engine_processes(markets: Optional[list[str]] = None) -> dict[str, Any]:
    target_markets = _normalize_markets(markets)
    print("\n[Task] Signal engine process started")
    try:
        outcomes: list[TaskStepOutcome] = []
        total_steps = len(target_markets)
        for index, market in enumerate(target_markets, start=1):
            outcomes.append(
                _run_timed_step(
                    index,
                    total_steps,
                    "Multi-screener signal engine",
                    market,
                    lambda market=market: run_signal_engine_task(market=market),
                )
            )

        return _build_process_summary("Signal engine process", outcomes)
    except Exception as exc:
        return _unexpected_process_summary("Signal engine process", exc)



def run_weinstein_stage2_screening(*, market: str) -> Any:
    normalized_market = require_market_key(market)
    try:
        from screeners.weinstein_stage2.screener import run_weinstein_stage2_screening

        print(f"\n[Task] Weinstein Stage 2 screening started ({normalized_market})")
        result = run_weinstein_stage2_screening(market=normalized_market)
        print(f"[Task] Weinstein Stage 2 screening completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Weinstein Stage 2 screening failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def run_leader_lagging_screening(*, market: str) -> Any:
    normalized_market = require_market_key(market)
    try:
        from screeners.leader_lagging.screener import run_leader_lagging_screening

        print(f"\n[Task] Leader / lagging screening started ({normalized_market})")
        result = run_leader_lagging_screening(market=normalized_market)
        print(f"[Task] Leader / lagging screening completed ({normalized_market})")
        return result
    except Exception as exc:
        print(f"[Task] Leader / lagging screening failed ({normalized_market}): {exc}")
        print(traceback.format_exc())
        return {"error": str(exc), "market": normalized_market}



def run_all_screening_processes(skip_data: bool = False, markets: Optional[list[str]] = None) -> dict[str, Any]:
    target_markets = _normalize_markets(markets)
    print("\n[Task] Full screening process started")
    try:
        outcomes: list[TaskStepOutcome] = []
        for market in target_markets:
            print(f"\n[Task] Market pipeline started ({market})")
            outcomes.append(_run_timed_step(1, 8, "Mark Minervini technical", market, lambda market=market: run_markminervini_screening(market=market)))
            wait_for_yahoo_phase_handoff("Advanced financial")
            outcomes.append(
                _run_timed_step(
                    2,
                    8,
                    "Advanced financial",
                    market,
                    lambda market=market: run_advanced_financial_screening(market=market, skip_data=skip_data),
                )
            )
            outcomes.append(_run_timed_step(3, 8, "Integrated screening", market, lambda market=market: run_integrated_screening(market=market)))
            outcomes.append(_run_timed_step(4, 8, "New ticker tracking", market, lambda market=market: run_new_ticker_tracking(market=market)))
            outcomes.append(_run_timed_step(5, 8, "Weinstein Stage 2", market, lambda market=market: run_weinstein_stage2_screening(market=market)))
            outcomes.append(_run_timed_step(6, 8, "Leader / lagging", market, lambda market=market: run_leader_lagging_screening(market=market)))
            wait_for_yahoo_phase_handoff("Qullamaggie")
            outcomes.append(
                _run_timed_step(
                    7,
                    8,
                    "Qullamaggie",
                    market,
                    lambda market=market: run_qullamaggie_strategy_task(skip_data=skip_data, market=market),
                )
            )
            outcomes.append(_run_timed_step(8, 8, "TradingView presets", market, lambda market=market: run_tradingview_preset_screeners(market=market)))

            print(f"[Task] Market pipeline completed ({market})")

        return _build_process_summary("Full screening process", outcomes)
    except Exception as exc:
        return _unexpected_process_summary("Full screening process", exc)



def run_market_analysis_pipeline(
    *,
    skip_data: bool = False,
    markets: Optional[list[str]] = None,
    include_signals: bool = False,
) -> dict[str, Any]:
    target_markets = _normalize_markets(markets)
    print(
        "\n[Task] Market analysis pipeline started - "
        f"markets={target_markets}, skip_data={skip_data}, include_signals={include_signals}"
    )
    screening_summary = run_all_screening_processes(skip_data=skip_data, markets=target_markets)
    summaries = [screening_summary]
    if include_signals:
        summaries.append(run_signal_engine_processes(markets=target_markets))
    return _combine_process_summaries("Market analysis pipeline", summaries)



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
            run_market_analysis_pipeline(skip_data=True, markets=["us"], include_signals=True)
            now = datetime.now(kst_tz)
            if now.time() >= full_time and (last_full_date != now.date()):
                time.sleep(interval * 60)
                subprocess.run([sys.executable, "main.py"], check=False)
                last_full_date = datetime.now(kst_tz).date() if kst_tz else datetime.now().date()
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        print("\n[Task] Scheduler stopped")