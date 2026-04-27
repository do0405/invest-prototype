from __future__ import annotations

import json
import sys
import threading
import time
import types

import data_collectors
import pandas as pd
import pytest

from orchestrator import tasks
from tests._paths import runtime_root
from utils.market_data_contract import OhlcvFreshnessSummary, describe_ohlcv_freshness
from utils.runtime_context import RuntimeContext


@pytest.fixture(autouse=True)
def _disable_shared_ohlcv_cache_for_orchestrator_unit_tests(monkeypatch):
    monkeypatch.setenv("INVEST_PROTO_SCREENING_SHARED_OHLCV_CACHE", "0")


def test_collect_data_main_runs_ohlcv_before_metadata_and_applies_handoffs(monkeypatch):
    calls: list[tuple[str, object]] = []
    handoffs: list[str] = []

    fake_data_collector = types.ModuleType("data_collector")

    def _fake_collect_data(*, update_symbols: bool = True):  # noqa: ANN202
        calls.append(("us_ohlcv", update_symbols))

    setattr(fake_data_collector, "collect_data", _fake_collect_data)

    monkeypatch.setitem(sys.modules, "data_collector", fake_data_collector)
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))
    monkeypatch.setattr(
        tasks,
        "run_stock_metadata_collection",
        lambda *, market="us": calls.append((f"{market}_metadata", None)) or {"ok": True},
    )
    monkeypatch.setattr(
        tasks,
        "run_kr_ohlcv_collection",
        lambda **kwargs: calls.append(("kr_ohlcv", kwargs)) or {"total": 1, "saved": 1, "failed": 0},
    )

    summary = tasks.collect_data_main(update_symbols=True, skip_ohlcv=False, include_kr=True)

    assert calls == [
        ("us_ohlcv", True),
        ("kr_ohlcv", {"include_etn": True}),
        ("us_metadata", None),
        ("kr_metadata", None),
    ]
    assert handoffs == ["US OHLCV", "US stock metadata"]
    assert summary["ok"] is True



def test_run_kr_ohlcv_collection_defaults_to_include_etn(monkeypatch):
    observed: dict[str, object] = {}
    fake_module = types.ModuleType("data_collectors.kr_ohlcv_collector")
    setattr(fake_module, "KR_OHLCV_DEFAULT_LOOKBACK_DAYS", 520)

    def _fake_collect_kr_ohlcv_csv(**kwargs):  # noqa: ANN202
        observed.update(kwargs)
        return {"total": 1, "saved": 1, "failed": 0}

    setattr(fake_module, "collect_kr_ohlcv_csv", _fake_collect_kr_ohlcv_csv)
    monkeypatch.setitem(sys.modules, "data_collectors.kr_ohlcv_collector", fake_module)
    monkeypatch.setattr(data_collectors, "kr_ohlcv_collector", fake_module, raising=False)

    summary = tasks.run_kr_ohlcv_collection()

    assert summary["saved"] == 1
    assert observed["days"] == 520
    assert observed["include_etn"] is True



def test_ensure_directories_scopes_selected_markets_and_phases(monkeypatch):
    calls: list[tuple[str, object]] = []

    fake_io_utils = types.ModuleType("utils.io_utils")
    setattr(
        fake_io_utils,
        "create_required_dirs",
        lambda: calls.append(("create_required_dirs", None)),
    )
    monkeypatch.setitem(sys.modules, "utils.io_utils", fake_io_utils)
    monkeypatch.setattr(
        tasks,
        "ensure_market_dirs",
        lambda market, include_signal_dirs=False: calls.append(
            (
                "ensure_market_dirs",
                {
                    "market": market,
                    "include_signal_dirs": include_signal_dirs,
                },
            )
        ),
    )
    monkeypatch.setattr(
        tasks,
        "preflight_market_output_dirs",
        lambda market, *, include_signal_dirs=False, include_augment_dirs=False: calls.append(
            (
                "preflight_market_output_dirs",
                {
                    "market": market,
                    "include_signal_dirs": include_signal_dirs,
                    "include_augment_dirs": include_augment_dirs,
                },
            )
        ),
    )

    tasks.ensure_directories(
        markets=["kr"],
        include_signal_dirs=False,
        include_augment_dirs=False,
    )

    assert calls == [
        ("create_required_dirs", None),
        (
            "ensure_market_dirs",
            {
                "market": "kr",
                "include_signal_dirs": False,
            },
        ),
        (
            "preflight_market_output_dirs",
            {
                "market": "kr",
                "include_signal_dirs": False,
                "include_augment_dirs": False,
            },
        ),
    ]


def test_run_all_screening_processes_applies_yahoo_handoffs_before_financial_steps(monkeypatch):
    calls: list[tuple[str, str]] = []
    handoffs: list[str] = []

    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_PARALLEL", "0")
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))
    monkeypatch.setattr(tasks, "run_markminervini_screening", lambda *, market="us", standalone=False: calls.append((market, "technical")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", lambda *, market="us", skip_data=False, standalone=False: calls.append((market, "financial")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_integrated_screening", lambda *, market="us", standalone=False: calls.append((market, "integrated")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda *, market="us", standalone=False: calls.append((market, "tracking")) or {"rows": 1})
    monkeypatch.setattr(
        tasks,
        "run_weinstein_stage2_screening",
        lambda *, market="us", standalone=False, runtime_context=None: calls.append((market, "weinstein")) or {"rows": 1},
    )
    monkeypatch.setattr(
        tasks,
        "run_leader_lagging_screening",
        lambda *, market="us", standalone=False, runtime_context=None: calls.append((market, "leader")) or {"rows": 1},
    )
    monkeypatch.setattr(
        tasks,
        "run_qullamaggie_strategy_task",
        lambda *, skip_data=False, market="us", standalone=False, runtime_context=None: calls.append((market, "qullamaggie")) or {"rows": 1},
    )
    monkeypatch.setattr(
        tasks,
        "run_tradingview_preset_screeners",
        lambda *, market="us", standalone=False, runtime_context=None: calls.append((market, "tradingview")) or {"growth": []},
    )

    summary = tasks.run_all_screening_processes(markets=["us"])

    assert calls == [
        ("us", "technical"),
        ("us", "financial"),
        ("us", "integrated"),
        ("us", "tracking"),
        ("us", "weinstein"),
        ("us", "leader"),
        ("us", "qullamaggie"),
        ("us", "tradingview"),
    ]
    assert handoffs == ["Advanced financial", "Qullamaggie"]
    assert summary["ok"] is True


def test_run_all_screening_processes_parallelizes_safe_local_stages(monkeypatch):
    calls: list[tuple[str, str, float]] = []
    handoffs: list[str] = []
    runtime_state_writes: list[tuple[str, str]] = []
    lock = threading.Lock()
    parent_context = RuntimeContext(market="us", as_of_date="2026-04-18")

    def _record(stage: str, phase: str) -> None:
        with lock:
            calls.append((stage, phase, time.perf_counter()))

    def _fast_stage(stage: str):
        def _run(**kwargs):  # noqa: ANN003, ANN202
            runtime_context = kwargs.get("runtime_context")
            if isinstance(runtime_context, RuntimeContext):
                runtime_context.add_timing(f"{stage}.fake_seconds", 0.25)
            _record(stage, "start")
            _record(stage, "end")
            return {"rows": 1}

        return _run

    def _slow_stage(stage: str):
        def _run(**kwargs):  # noqa: ANN003, ANN202
            runtime_context = kwargs.get("runtime_context")
            assert isinstance(runtime_context, RuntimeContext)
            assert runtime_context is not parent_context
            runtime_context.add_timing(f"{stage}.fake_seconds", 0.5)
            runtime_context.update_runtime_state(
                data_freshness={
                    "counts": {"closed": 1, "stale": 0, "future_or_partial": 0, "empty": 0},
                    "stages": {
                        stage: {
                            "counts": {"closed": 1, "stale": 0, "future_or_partial": 0, "empty": 0},
                            "target_date": "2026-04-18",
                            "latest_completed_session": "2026-04-18",
                            "mode": "default_completed_session",
                            "examples": [],
                        }
                    },
                },
                status="running",
            )
            _record(stage, "start")
            time.sleep(0.15)
            _record(stage, "end")
            return {"rows": 1}

        return _run

    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_PARALLEL", "1")
    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_WORKERS", "4")
    monkeypatch.setattr(tasks, "_write_runtime_profiles", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "_build_and_store_source_registry_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))
    monkeypatch.setattr(
        tasks,
        "_write_runtime_state",
        lambda path, payload: runtime_state_writes.append(
            (path, str(payload.get("status") or ""))
        )
        or time.sleep(0.2),
    )
    monkeypatch.setattr(tasks, "run_markminervini_screening", _fast_stage("technical"))
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", _fast_stage("financial"))
    monkeypatch.setattr(tasks, "run_integrated_screening", _fast_stage("integrated"))
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", _fast_stage("tracking"))
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", _slow_stage("weinstein"))
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", _slow_stage("leader_lagging"))
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", _slow_stage("qullamaggie"))
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", _slow_stage("tradingview"))

    started = time.perf_counter()
    summary = tasks.run_all_screening_processes(
        markets=["us"],
        skip_data=True,
        runtime_contexts={"us": parent_context},
    )
    elapsed = time.perf_counter() - started

    assert [step["label"] for step in summary["steps"]] == [
        "Mark Minervini technical",
        "Advanced financial",
        "Integrated screening",
        "New ticker tracking",
        "Weinstein Stage 2",
        "Leader / lagging",
        "Qullamaggie",
        "TradingView presets",
    ]
    assert summary["ok"] is True
    assert handoffs == ["Advanced financial"]
    assert elapsed < 1.0
    assert runtime_state_writes
    assert summary["elapsed_seconds"] == pytest.approx(elapsed, rel=0.3, abs=0.15)
    assert summary["timings"]["process_total_seconds"] == pytest.approx(
        summary["elapsed_seconds"],
        rel=0.01,
        abs=0.01,
    )
    tracking_end = max(ts for stage, phase, ts in calls if stage == "tracking" and phase == "end")
    parallel_starts = {
        stage: ts
        for stage, phase, ts in calls
        if phase == "start" and stage in {"weinstein", "leader_lagging", "qullamaggie", "tradingview"}
    }
    parallel_ends = {
        stage: ts
        for stage, phase, ts in calls
        if phase == "end" and stage in {"weinstein", "leader_lagging", "qullamaggie", "tradingview"}
    }
    assert all(ts >= tracking_end for ts in parallel_starts.values())
    assert max(parallel_starts.values()) - min(parallel_starts.values()) < 0.08
    assert max(parallel_ends.values()) - min(parallel_starts.values()) < 0.35
    assert parent_context.timings["weinstein.fake_seconds"] == 0.5
    assert parent_context.runtime_state["data_freshness"]["stages"]["tradingview"]["counts"]["closed"] == 1


def test_parallel_stage_child_progress_updates_parent_runtime_state(monkeypatch):
    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_PARALLEL", "1")
    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_WORKERS", "2")
    parent_context = RuntimeContext(market="us", as_of_date="2026-04-18", run_id="test-run")
    progress_events: list[dict[str, object]] = []
    parent_context.bind_progress_callback(lambda payload: progress_events.append(dict(payload)), run_id="test-run")

    def _leader_action(runtime_context: RuntimeContext | None):  # noqa: ANN202
        assert runtime_context is not None
        assert runtime_context is not parent_context
        runtime_context.update_runtime_state(
            current_stage="Leader / lagging",
            current_symbol="AAA",
            current_chunk="follower_analysis:1/2",
            status="running",
        )
        return {"rows": 1}

    def _tradingview_action(runtime_context: RuntimeContext | None):  # noqa: ANN202
        assert runtime_context is not None
        runtime_context.update_runtime_state(
            current_stage="TradingView presets",
            current_symbol="BBB",
            current_chunk="metrics:1/2",
            status="running",
        )
        return {"rows": 1}

    outcomes = tasks._run_screening_stage_specs(
        [
            tasks._ScreeningStageSpec(6, 8, "Leader / lagging", "us", _leader_action, parent_context),
            tasks._ScreeningStageSpec(8, 8, "TradingView presets", "us", _tradingview_action, parent_context),
        ],
        parallel=True,
    )

    assert [outcome.label for outcome in outcomes] == ["Leader / lagging", "TradingView presets"]
    parallel_stages = parent_context.runtime_metrics["parallel_stages"]
    assert parallel_stages["Leader / lagging"]["current_symbol"] == "AAA"
    assert parallel_stages["Leader / lagging"]["current_chunk"] == "follower_analysis:1/2"
    assert parallel_stages["TradingView presets"]["current_symbol"] == "BBB"
    assert parent_context.runtime_state["parallel_stages"]["Leader / lagging"]["current_chunk"] == "follower_analysis:1/2"
    assert any("parallel_stages" in event for event in progress_events)


def test_write_runtime_profile_exposes_parallel_and_shared_cache_sections(monkeypatch):
    output_root = runtime_root("_test_runtime_parallel_profile")
    output_root.mkdir(parents=True, exist_ok=True)
    profile_path = output_root / "runtime_profile.json"
    monkeypatch.setattr(tasks, "get_runtime_profile_path", lambda market: str(profile_path))

    tasks._write_runtime_profiles(
        "Full screening process",
        [
            tasks.TaskStepOutcome(
                ok=True,
                label="Leader / lagging",
                market="us",
                elapsed_seconds=1.25,
                runtime_metrics={
                    "parallel_stages": {
                        "Leader / lagging": {
                            "status": "ok",
                            "current_chunk": "follower_analysis:10/10",
                        }
                    },
                    "shared_ohlcv_cache": {
                        "symbols": 3,
                        "loaded": 2,
                        "seconds": 0.5,
                        "cache_hits": 1,
                    },
                },
            )
        ],
    )

    payload = json.loads(profile_path.read_text(encoding="utf-8"))
    assert payload["parallel_stages"]["Leader / lagging"]["status"] == "ok"
    assert payload["shared_ohlcv_cache"]["loaded"] == 2


def test_preload_shared_screening_ohlcv_cache_loads_market_symbols_once(monkeypatch):
    data_dir = runtime_root("_test_shared_ohlcv_cache_symbols") / "data" / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    for symbol in ("AAA", "BBB", "SPY"):
        (data_dir / f"{symbol}.csv").write_text("date,close\n2026-04-17,1\n", encoding="utf-8")
    observed: dict[str, object] = {}

    def _fake_loader(market, symbols, **kwargs):  # noqa: ANN001, ANN003, ANN202
        observed["market"] = market
        observed["symbols"] = list(symbols)
        observed["required_columns"] = tuple(kwargs.get("required_columns") or ())
        return {
            symbol: pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-04-17"]),
                    "close": [1.0],
                    "volume": [100.0],
                }
            )
            for symbol in symbols
        }

    monkeypatch.setenv("INVEST_PROTO_SCREENING_SHARED_OHLCV_CACHE", "1")
    monkeypatch.setattr(tasks, "get_market_data_dir", lambda market: str(data_dir))
    monkeypatch.setattr(tasks, "load_local_ohlcv_frames_ordered", _fake_loader)

    runtime_context = RuntimeContext(market="us", as_of_date="2026-04-17")
    tasks._preload_shared_screening_ohlcv_cache("us", runtime_context)

    assert observed["market"] == "us"
    assert observed["symbols"] == ["AAA", "BBB"]
    assert observed["required_columns"] == tasks.SCREENING_OHLCV_READ_COLUMNS
    cache_metrics = runtime_context.runtime_metrics["shared_ohlcv_cache"]
    assert cache_metrics["symbols"] == 2
    assert cache_metrics["loaded"] == 2
    assert cache_metrics["status"] == "ok"


def test_run_all_screening_processes_keeps_qullamaggie_provider_lane_sequential(monkeypatch):
    calls: list[str] = []
    handoffs: list[str] = []

    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_PARALLEL", "1")
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))
    monkeypatch.setattr(tasks, "run_markminervini_screening", lambda **kwargs: calls.append("technical") or {"rows": 1})
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", lambda **kwargs: calls.append("financial") or {"rows": 1})
    monkeypatch.setattr(tasks, "run_integrated_screening", lambda **kwargs: calls.append("integrated") or {"rows": 1})
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda **kwargs: calls.append("tracking") or {"rows": 1})
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", lambda **kwargs: calls.append("weinstein") or {"rows": 1})
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", lambda **kwargs: calls.append("leader") or {"rows": 1})
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", lambda **kwargs: calls.append("qullamaggie") or {"rows": 1})
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", lambda **kwargs: calls.append("tradingview") or {"rows": 1})

    summary = tasks.run_all_screening_processes(markets=["us"], skip_data=False)

    assert summary["ok"] is True
    assert handoffs == ["Advanced financial", "Qullamaggie"]
    assert calls.index("qullamaggie") > calls.index("tracking")



def test_run_signal_engine_task_calls_signal_scan(monkeypatch):
    captured: dict[str, object] = {}
    fake_module = types.ModuleType("screeners.signals")

    def _fake_run_multi_screener_signal_scan(*, market="us", as_of_date=None, upcoming_earnings_fetcher=None, earnings_collector=None, standalone=False):  # noqa: ANN202
        captured["market"] = market
        captured["standalone"] = standalone
        return {"all_signals_v2": []}

    setattr(fake_module, "run_multi_screener_signal_scan", _fake_run_multi_screener_signal_scan)
    monkeypatch.setitem(sys.modules, "screeners.signals", fake_module)
    monkeypatch.setattr(
        tasks,
        "probe_market_intel_compat_availability",
        lambda *args, **kwargs: types.SimpleNamespace(status="compat"),
    )

    result = tasks.run_signal_engine_task(market="kr")

    assert captured["market"] == "kr"
    assert captured["standalone"] is False
    assert result == {"all_signals_v2": []}


def test_run_signal_engine_task_local_only_injects_noop_earnings_providers(monkeypatch):
    captured: dict[str, object] = {}
    fake_module = types.ModuleType("screeners.signals")

    def _fake_run_multi_screener_signal_scan(  # noqa: ANN202
        *,
        market="us",
        as_of_date=None,
        upcoming_earnings_fetcher=None,
        earnings_collector=None,
        standalone=False,
    ):
        captured["market"] = market
        captured["standalone"] = standalone
        upcoming = upcoming_earnings_fetcher(market, "2026-04-17", 10)
        captured["upcoming_empty"] = bool(upcoming.empty)
        captured["earnings_surprise"] = earnings_collector.get_earnings_surprise("AAA")
        captured["provider_diagnostics_rows"] = earnings_collector.provider_diagnostics_rows()
        captured["log_provider_summary"] = earnings_collector.log_provider_summary()
        return {"all_signals_v2": []}

    setattr(fake_module, "run_multi_screener_signal_scan", _fake_run_multi_screener_signal_scan)
    monkeypatch.setitem(sys.modules, "screeners.signals", fake_module)
    monkeypatch.setattr(
        tasks,
        "probe_market_intel_compat_availability",
        lambda *args, **kwargs: types.SimpleNamespace(status="compat"),
    )

    result = tasks.run_signal_engine_task(market="us", local_only=True)

    assert captured == {
        "market": "us",
        "standalone": False,
        "upcoming_empty": True,
        "earnings_surprise": None,
        "provider_diagnostics_rows": [],
        "log_provider_summary": None,
    }
    assert result == {"all_signals_v2": []}



def test_run_signal_engine_processes_runs_each_market(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        tasks,
        "run_signal_engine_task",
        lambda *, market="us", standalone=False: calls.append(market) or {"all_signals_v2": []},
    )

    summary = tasks.run_signal_engine_processes(markets=["us", "kr"])

    assert calls == ["us", "kr"]
    assert summary["ok"] is True
    assert summary["steps"][0]["status"] == "ok"
    assert "status_counts" in summary["steps"][0]
    assert "cache_stats" in summary["steps"][0]
    assert "timings" in summary["steps"][0]


def test_run_signal_engine_processes_passes_standalone_to_each_market(monkeypatch):
    calls: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        tasks,
        "run_signal_engine_task",
        lambda *, market="us", standalone=False: calls.append((market, standalone)) or {"all_signals_v2": []},
    )

    summary = tasks.run_signal_engine_processes(markets=["us", "kr"], standalone=True)

    assert calls == [("us", True), ("kr", True)]
    assert summary["ok"] is True


def test_run_signal_engine_processes_passes_local_only_to_each_market(monkeypatch):
    calls: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        tasks,
        "run_signal_engine_task",
        lambda *, market="us", standalone=False, local_only=False: calls.append((market, local_only)) or {"all_signals_v2": []},
    )

    summary = tasks.run_signal_engine_processes(markets=["us", "kr"], local_only=True)

    assert calls == [("us", True), ("kr", True)]
    assert summary["ok"] is True


def test_run_signal_engine_processes_auto_falls_back_to_standalone_when_compat_missing(monkeypatch):
    observed: list[tuple[str, bool, str, str]] = []

    monkeypatch.setattr(
        tasks,
        "probe_market_intel_compat_availability",
        lambda *args, **kwargs: types.SimpleNamespace(status="missing"),
    )
    monkeypatch.setattr(
        tasks,
        "run_signal_engine_task",
        lambda *, market="us", standalone=False, runtime_context=None, as_of_date=None, local_only=False: observed.append(  # noqa: E501
            (
                market,
                standalone,
                str(getattr(runtime_context, "runtime_state", {}).get("market_truth_mode") or ""),
                str(getattr(runtime_context, "runtime_state", {}).get("fallback_reason") or ""),
            )
        )
        or {"all_signals_v2": []},
    )

    summary = tasks.run_signal_engine_processes(
        markets=["us"],
        runtime_contexts={"us": RuntimeContext(market="us", as_of_date="2026-04-18")},
    )

    assert summary["ok"] is True
    assert observed == [("us", True, "standalone_auto", "missing")]
    assert summary["market_truth_modes"] == {"us": "standalone_auto"}
    assert summary["fallback_reasons"] == {"us": "missing"}


def test_run_signal_engine_processes_preserves_existing_runtime_context_as_of(monkeypatch):
    runtime_contexts = {
        "us": RuntimeContext(market="us", as_of_date="2026-04-17"),
    }
    initialize_calls: list[tuple[list[str], str | None]] = []
    observed: list[tuple[str, str | None, str | None]] = []

    def _fake_initialize(contexts, markets, *, explicit_as_of=None):  # noqa: ANN001, ANN202
        initialize_calls.append((list(markets), explicit_as_of))
        contexts["us"].set_as_of_date("2099-01-01")

    monkeypatch.setattr(tasks, "_initialize_runtime_context_as_of", _fake_initialize)
    monkeypatch.setattr(
        tasks,
        "run_signal_engine_task",
        lambda *, market="us", standalone=False, runtime_context=None, as_of_date=None, local_only=False: observed.append(  # noqa: E501
            (
                market,
                runtime_context.as_of_date if runtime_context is not None else None,
                as_of_date,
            )
        )
        or {"all_signals_v2": []},
    )

    summary = tasks.run_signal_engine_processes(
        markets=["us"],
        runtime_contexts=runtime_contexts,
    )

    assert summary["ok"] is True
    assert initialize_calls == []
    assert observed == [("us", "2026-04-17", "2026-04-17")]


def test_run_qullamaggie_strategy_task_skip_data_disables_earnings_filter(monkeypatch):
    captured: dict[str, object] = {}
    fake_module = types.ModuleType("screeners.qullamaggie.screener")

    def _fake_run_qullamaggie_screening(  # noqa: ANN202
        *,
        setup_type=None,
        market="us",
        standalone=False,
        runtime_context=None,
        enable_earnings_filter=None,
    ):
        captured["setup_type"] = setup_type
        captured["market"] = market
        captured["standalone"] = standalone
        captured["runtime_context"] = runtime_context
        captured["enable_earnings_filter"] = enable_earnings_filter
        return {"rows": 1}

    setattr(fake_module, "run_qullamaggie_screening", _fake_run_qullamaggie_screening)
    monkeypatch.setitem(sys.modules, "screeners.qullamaggie.screener", fake_module)

    result = tasks.run_qullamaggie_strategy_task(skip_data=True, market="us", standalone=True)

    assert result == {"rows": 1}
    assert captured["enable_earnings_filter"] is False
    assert captured["standalone"] is True


def test_run_screening_augment_processes_preserves_partial_status_and_telemetry(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        tasks,
        "run_screening_augment_task",
        lambda *, market="us", runtime_context=None: {
            "ok": True,
            "status": "partial",
            "status_counts": {"partial": 1},
            "timings": {"stumpy_seconds": 0.12, "chronos2_seconds": 0.34, "timesfm2p5_seconds": 0.56},
            "cache_stats": {"hits": 1, "misses": 0},
            "rows_read": 3,
            "rows_written": 10,
        },
    )

    summary = tasks.run_screening_augment_processes(markets=["us"])

    assert summary["ok"] is True
    assert summary["steps"][0]["status"] == "partial"
    assert summary["status_counts"] == {"partial": 1}
    assert summary["cache_stats"] == {"hits": 1, "misses": 0}
    assert summary["rows_read"] == 3
    assert summary["rows_written"] == 10
    assert summary["steps"][0]["timings"]["timesfm2p5_seconds"] == 0.56


def test_run_market_analysis_pipeline_runs_augment_between_screening_and_signals(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        tasks,
        "run_all_screening_processes",
        lambda **kwargs: calls.append("screening") or {"ok": True, "failed_steps": 0},
    )
    monkeypatch.setattr(
        tasks,
        "run_screening_augment_processes",
        lambda **kwargs: calls.append("augment") or {"ok": True, "failed_steps": 0},
    )
    monkeypatch.setattr(
        tasks,
        "run_signal_engine_processes",
        lambda **kwargs: calls.append("signals") or {"ok": True, "failed_steps": 0},
    )

    summary = tasks.run_market_analysis_pipeline(
        skip_data=True,
        markets=["us"],
        include_signals=True,
        enable_augment=True,
    )

    assert calls == ["screening", "augment", "signals"]
    assert summary["ok"] is True


def test_run_market_analysis_pipeline_writes_full_run_summary_per_market(monkeypatch):
    output_root = runtime_root("_test_runtime_full_run_summary")
    output_root.mkdir(parents=True, exist_ok=True)
    summary_paths = {
        "us": output_root / "us_full_run_summary.json",
        "kr": output_root / "kr_full_run_summary.json",
    }

    monkeypatch.setattr(
        tasks,
        "get_full_run_summary_path",
        lambda market: str(summary_paths[market]),
    )
    monkeypatch.setattr(
        tasks,
        "run_all_screening_processes",
        lambda **kwargs: {
            "label": "Full screening process",
            "ok": True,
            "failed_steps": 0,
            "total_steps": 2,
            "elapsed_seconds": 1.0,
            "markets": kwargs["markets"],
            "status_counts": {"ok": 2},
            "cache_stats": {},
            "timings": {"process_total_seconds": 1.0},
            "rows_read": 0,
            "rows_written": 2,
        },
    )
    monkeypatch.setattr(
        tasks,
        "run_signal_engine_processes",
        lambda **kwargs: {
            "label": "Signal engine process",
            "ok": True,
            "failed_steps": 0,
            "total_steps": 2,
            "elapsed_seconds": 2.0,
            "markets": kwargs["markets"],
            "status_counts": {"ok": 2},
            "cache_stats": {},
            "timings": {"process_total_seconds": 2.0},
            "rows_read": 0,
            "rows_written": 4,
        },
    )

    summary = tasks.run_market_analysis_pipeline(
        skip_data=True,
        markets=["us", "kr"],
        include_signals=True,
        standalone=True,
    )

    assert summary["ok"] is True
    for market in ("us", "kr"):
        payload = json.loads(summary_paths[market].read_text(encoding="utf-8"))
        assert payload["label"] == "Market analysis pipeline"
        assert payload["market"] == market
        assert payload["summary"]["total_steps"] == 4
        assert payload["summary"]["rows_written"] == 6


def test_run_market_analysis_pipeline_continues_after_partial_augment(
    monkeypatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        tasks,
        "run_all_screening_processes",
        lambda **kwargs: calls.append("screening") or {"ok": True, "failed_steps": 0},
    )
    monkeypatch.setattr(
        tasks,
        "run_screening_augment_processes",
        lambda **kwargs: calls.append("augment") or {
            "ok": True,
            "failed_steps": 0,
            "status": "partial",
            "status_counts": {"partial": 1},
            "cache_stats": {"hits": 1, "misses": 0},
            "timings": {"stumpy_seconds": 0.1, "chronos2_seconds": 0.2, "timesfm2p5_seconds": 0.3},
            "rows_written": 6,
        },
    )
    monkeypatch.setattr(
        tasks,
        "run_signal_engine_processes",
        lambda **kwargs: calls.append("signals") or {"ok": True, "failed_steps": 0},
    )

    summary = tasks.run_market_analysis_pipeline(
        skip_data=True,
        markets=["us"],
        include_signals=True,
        enable_augment=True,
    )

    assert calls == ["screening", "augment", "signals"]
    assert summary["ok"] is True


def test_run_market_analysis_pipeline_reuses_runtime_contexts_between_steps(
    monkeypatch,
):
    seen_context_ids: list[tuple[str, int]] = []

    def _fake_screening(**kwargs):  # noqa: ANN003
        runtime_contexts = kwargs["runtime_contexts"]
        runtime_contexts["us"].source_registry_snapshot = {
            "schema_version": 1,
            "market": "US",
            "as_of_date": "2026-04-18",
            "registry": {},
            "source_rows": [],
        }
        runtime_contexts["us"].screening_frames["markminervini_with_rs"] = pd.DataFrame(
            [{"symbol": "AAA", "rs_score": 90.0, "met_count": 7}]
        )
        seen_context_ids.append(("screening", id(runtime_contexts["us"])))
        return {"ok": True, "failed_steps": 0}

    def _fake_augment(**kwargs):  # noqa: ANN003
        runtime_contexts = kwargs["runtime_contexts"]
        seen_context_ids.append(("augment", id(runtime_contexts["us"])))
        assert runtime_contexts["us"].source_registry_snapshot is not None
        return {"ok": True, "failed_steps": 0}

    def _fake_signals(**kwargs):  # noqa: ANN003
        runtime_contexts = kwargs["runtime_contexts"]
        seen_context_ids.append(("signals", id(runtime_contexts["us"])))
        assert "markminervini_with_rs" in runtime_contexts["us"].screening_frames
        return {"ok": True, "failed_steps": 0}

    monkeypatch.setattr(tasks, "run_all_screening_processes", _fake_screening)
    monkeypatch.setattr(tasks, "run_screening_augment_processes", _fake_augment)
    monkeypatch.setattr(tasks, "run_signal_engine_processes", _fake_signals)

    summary = tasks.run_market_analysis_pipeline(
        skip_data=True,
        markets=["us"],
        include_signals=True,
        enable_augment=True,
    )

    assert summary["ok"] is True
    assert seen_context_ids[0][1] == seen_context_ids[1][1] == seen_context_ids[2][1]


def test_run_market_analysis_pipeline_passes_standalone_to_screening_and_signals(monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        tasks,
        "run_all_screening_processes",
        lambda **kwargs: calls.append(("screening", kwargs)) or {"ok": True, "failed_steps": 0},
    )
    monkeypatch.setattr(
        tasks,
        "run_signal_engine_processes",
        lambda **kwargs: calls.append(("signals", kwargs)) or {"ok": True, "failed_steps": 0},
    )

    summary = tasks.run_market_analysis_pipeline(
        skip_data=True,
        markets=["us"],
        include_signals=True,
        enable_augment=False,
        standalone=True,
    )

    assert calls[0][0] == "screening"
    assert calls[0][1]["skip_data"] is True
    assert calls[0][1]["markets"] == ["us"]
    assert calls[0][1]["standalone"] is True
    assert "runtime_contexts" in calls[0][1]
    assert calls[1][0] == "signals"
    assert calls[1][1]["markets"] == ["us"]
    assert calls[1][1]["standalone"] is True
    assert "runtime_contexts" in calls[1][1]
    assert summary["ok"] is True


def test_run_market_analysis_pipeline_skip_data_passes_local_only_to_signals(monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        tasks,
        "run_all_screening_processes",
        lambda **kwargs: calls.append(("screening", kwargs)) or {"ok": True, "failed_steps": 0},
    )
    monkeypatch.setattr(
        tasks,
        "run_signal_engine_processes",
        lambda **kwargs: calls.append(("signals", kwargs)) or {"ok": True, "failed_steps": 0},
    )

    summary = tasks.run_market_analysis_pipeline(
        skip_data=True,
        markets=["us"],
        include_signals=True,
    )

    assert summary["ok"] is True
    assert calls[1][0] == "signals"
    assert calls[1][1]["local_only"] is True


def test_run_market_analysis_pipeline_initializes_runtime_context_as_of(monkeypatch):
    observed: list[tuple[str, str]] = []

    def _fake_screening(**kwargs):  # noqa: ANN003
        runtime_contexts = kwargs["runtime_contexts"]
        observed.extend(
            ("screening", runtime_contexts[market].as_of_date)
            for market in ("us", "kr")
        )
        return {"ok": True, "failed_steps": 0}

    def _fake_signals(**kwargs):  # noqa: ANN003
        runtime_contexts = kwargs["runtime_contexts"]
        observed.extend(
            ("signals", runtime_contexts[market].as_of_date)
            for market in ("us", "kr")
        )
        return {"ok": True, "failed_steps": 0}

    monkeypatch.setattr(tasks, "run_all_screening_processes", _fake_screening)
    monkeypatch.setattr(tasks, "run_signal_engine_processes", _fake_signals)

    summary = tasks.run_market_analysis_pipeline(
        skip_data=True,
        markets=["us", "kr"],
        include_signals=True,
        standalone=True,
        as_of_date="2026-04-17",
    )

    assert summary["ok"] is True
    assert observed == [
        ("screening", "2026-04-17"),
        ("screening", "2026-04-17"),
        ("signals", "2026-04-17"),
        ("signals", "2026-04-17"),
    ]


def test_run_all_screening_processes_passes_standalone_to_affected_screeners(monkeypatch):
    calls: list[tuple[str, dict[str, object]]] = []
    handoffs: list[str] = []

    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))
    monkeypatch.setattr(tasks, "run_markminervini_screening", lambda **kwargs: calls.append(("technical", kwargs)) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", lambda **kwargs: calls.append(("financial", kwargs)) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_integrated_screening", lambda **kwargs: calls.append(("integrated", kwargs)) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda **kwargs: calls.append(("tracking", kwargs)) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", lambda **kwargs: calls.append(("weinstein", kwargs)) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", lambda **kwargs: calls.append(("leader", kwargs)) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", lambda **kwargs: calls.append(("qullamaggie", kwargs)) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", lambda **kwargs: calls.append(("tradingview", kwargs)) or {"growth": []})

    summary = tasks.run_all_screening_processes(
        markets=["us"],
        standalone=True,
        runtime_contexts={"us": RuntimeContext(market="us", as_of_date="2026-04-18")},
    )

    assert summary["ok"] is True


def test_run_all_screening_processes_auto_falls_back_to_standalone_when_compat_missing(monkeypatch):
    calls: list[tuple[str, bool, str, str]] = []

    def _record(stage: str):
        def _run(**kwargs):  # noqa: ANN003, ANN202
            runtime_context = kwargs.get("runtime_context")
            calls.append(
                (
                    stage,
                    bool(kwargs.get("standalone")),
                    str(getattr(runtime_context, "runtime_state", {}).get("market_truth_mode") or ""),
                    str(getattr(runtime_context, "runtime_state", {}).get("fallback_reason") or ""),
                )
            )
            return {"growth": []} if stage == "tradingview" else {"rows": 1}

        return _run

    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_PARALLEL", "0")
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: None)
    monkeypatch.setattr(
        tasks,
        "probe_market_intel_compat_availability",
        lambda *args, **kwargs: types.SimpleNamespace(status="missing"),
    )
    monkeypatch.setattr(tasks, "_write_runtime_profiles", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "_build_and_store_source_registry_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "run_markminervini_screening", _record("technical"))
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", _record("financial"))
    monkeypatch.setattr(tasks, "run_integrated_screening", _record("integrated"))
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", _record("tracking"))
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", _record("weinstein"))
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", _record("leader"))
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", _record("qullamaggie"))
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", _record("tradingview"))

    summary = tasks.run_all_screening_processes(
        markets=["us"],
        runtime_contexts={"us": RuntimeContext(market="us", as_of_date="2026-04-18")},
    )

    assert summary["ok"] is True
    assert all(standalone is True for _, standalone, _, _ in calls)
    assert summary["market_truth_modes"] == {"us": "standalone_auto"}
    assert summary["fallback_reasons"] == {"us": "missing"}
    assert ("weinstein", True, "standalone_auto", "missing") in calls


def test_run_all_screening_processes_manual_standalone_skips_compat_probe(monkeypatch):
    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_PARALLEL", "0")
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: None)
    monkeypatch.setattr(
        tasks,
        "probe_market_intel_compat_availability",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("compat probe should not run in manual standalone")),
    )
    monkeypatch.setattr(tasks, "_write_runtime_profiles", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "_build_and_store_source_registry_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "run_markminervini_screening", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_integrated_screening", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", lambda **kwargs: {"growth": []})

    summary = tasks.run_all_screening_processes(
        markets=["us"],
        standalone=True,
        runtime_contexts={"us": RuntimeContext(market="us", as_of_date="2026-04-18")},
    )

    assert summary["ok"] is True
    assert summary["market_truth_modes"] == {"us": "standalone_manual"}


def test_run_all_screening_processes_passes_runtime_context_between_markminervini_steps(
    monkeypatch,
) -> None:
    handoffs: list[str] = []
    seen: dict[str, object] = {}

    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))

    def _technical(**kwargs):  # noqa: ANN003
        runtime_context = kwargs["runtime_context"]
        assert isinstance(runtime_context, RuntimeContext)
        frame = pd.DataFrame([{"symbol": "AAA", "rs_score": 91.0, "met_count": 7}])
        runtime_context.screening_frames["markminervini_with_rs"] = frame
        seen["technical_context_id"] = id(runtime_context)
        return frame

    def _financial(**kwargs):  # noqa: ANN003
        runtime_context = kwargs["runtime_context"]
        assert id(runtime_context) == seen["technical_context_id"]
        assert "markminervini_with_rs" in runtime_context.screening_frames
        frame = pd.DataFrame([{"symbol": "AAA", "fin_met_count": 5}])
        runtime_context.screening_frames["advanced_financial_df"] = frame
        return frame

    def _integrated(**kwargs):  # noqa: ANN003
        runtime_context = kwargs["runtime_context"]
        assert id(runtime_context) == seen["technical_context_id"]
        assert "advanced_financial_df" in runtime_context.screening_frames
        return {"rows": 1}

    monkeypatch.setattr(tasks, "run_markminervini_screening", _technical)
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", _financial)
    monkeypatch.setattr(tasks, "run_integrated_screening", _integrated)
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", lambda **kwargs: {"rows": 1})
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", lambda **kwargs: {"rows": 1})

    summary = tasks.run_all_screening_processes(
        markets=["us"],
        runtime_contexts={"us": RuntimeContext(market="us")},
    )

    assert summary["ok"] is True
    assert handoffs == ["Advanced financial", "Qullamaggie"]


def test_collect_data_main_skips_us_steps_for_kr_only_runtime(monkeypatch):
    calls: list[tuple[str, object]] = []
    handoffs: list[str] = []

    fake_data_collector = types.ModuleType("data_collector")
    setattr(fake_data_collector, "collect_data", lambda **kwargs: calls.append(("us_ohlcv", kwargs)))

    monkeypatch.setitem(sys.modules, "data_collector", fake_data_collector)
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))
    monkeypatch.setattr(
        tasks,
        "run_stock_metadata_collection",
        lambda *, market="us": calls.append((f"{market}_metadata", None)) or {"ok": True},
    )
    monkeypatch.setattr(
        tasks,
        "run_kr_ohlcv_collection",
        lambda **kwargs: calls.append(("kr_ohlcv", kwargs)) or {"total": 1, "saved": 1, "failed": 0},
    )

    summary = tasks.collect_data_main(
        update_symbols=True,
        skip_ohlcv=False,
        include_kr=True,
        include_us=False,
    )

    assert calls == [
        ("kr_ohlcv", {"include_etn": True}),
        ("kr_metadata", None),
    ]
    assert handoffs == []
    assert summary["ok"] is True


def test_run_market_analysis_pipeline_skips_augment_when_disabled(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        tasks,
        "run_all_screening_processes",
        lambda **kwargs: calls.append("screening") or {"ok": True, "failed_steps": 0},
    )
    monkeypatch.setattr(
        tasks,
        "run_screening_augment_processes",
        lambda **kwargs: calls.append("augment") or {"ok": True, "failed_steps": 0},
    )

    tasks.run_market_analysis_pipeline(
        skip_data=True,
        markets=["us"],
        include_signals=False,
        enable_augment=False,
    )

    assert calls == ["screening"]


def test_run_weinstein_stage2_screening_auto_falls_back_when_compat_missing(monkeypatch):
    from screeners.weinstein_stage2 import screener as weinstein_module

    captured: dict[str, object] = {}
    runtime_context = RuntimeContext(market="us", as_of_date="2026-04-18")

    monkeypatch.setattr(
        tasks,
        "probe_market_intel_compat_availability",
        lambda *args, **kwargs: types.SimpleNamespace(status="missing"),
    )
    monkeypatch.setattr(
        weinstein_module,
        "run_weinstein_stage2_screening",
        lambda **kwargs: captured.update(kwargs) or {"rows": 1},
    )

    result = tasks.run_weinstein_stage2_screening(
        market="us",
        runtime_context=runtime_context,
    )

    assert result == {"rows": 1}
    assert captured["standalone"] is True
    assert runtime_context.runtime_state["market_truth_mode"] == "standalone_auto"
    assert runtime_context.runtime_state["fallback_reason"] == "missing"


def test_run_weinstein_stage2_screening_preserves_auto_fallback_provenance_for_child_context(monkeypatch):
    from screeners.weinstein_stage2 import screener as weinstein_module

    captured: dict[str, object] = {}
    runtime_context = RuntimeContext(market="us", as_of_date="2026-04-18")
    runtime_context.update_runtime_state(
        market_truth_mode="standalone_auto",
        fallback_reason="missing",
        compat_availability="missing",
        market_truth_probe_as_of="2026-04-18",
    )

    def _fake_run(**kwargs):  # noqa: ANN202
        child_context = kwargs.get("runtime_context")
        captured["standalone"] = kwargs.get("standalone")
        captured["market_truth_mode"] = str(getattr(child_context, "runtime_state", {}).get("market_truth_mode") or "")
        captured["fallback_reason"] = str(getattr(child_context, "runtime_state", {}).get("fallback_reason") or "")
        return {"rows": 1}

    monkeypatch.setattr(weinstein_module, "run_weinstein_stage2_screening", _fake_run)

    result = tasks.run_weinstein_stage2_screening(
        market="us",
        standalone=True,
        runtime_context=runtime_context,
    )

    assert result == {"rows": 1}
    assert captured["standalone"] is True
    assert captured["market_truth_mode"] == "standalone_auto"
    assert captured["fallback_reason"] == "missing"


def test_run_all_screening_processes_reports_failed_steps_honestly(monkeypatch, capsys):
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: None)
    monkeypatch.setattr(tasks, "run_markminervini_screening", lambda *, market="us", standalone=False: {"rows": 1})
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", lambda *, market="us", skip_data=False, standalone=False: {"rows": 1})
    monkeypatch.setattr(tasks, "run_integrated_screening", lambda *, market="us", standalone=False: {"rows": 1})
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda *, market="us", standalone=False: {"rows": 1})
    monkeypatch.setattr(
        tasks,
        "run_weinstein_stage2_screening",
        lambda *, market="us", standalone=False, runtime_context=None: {"rows": 1},
    )
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", lambda *, market="us", standalone=False: {"error": "boom"})
    monkeypatch.setattr(
        tasks,
        "run_qullamaggie_strategy_task",
        lambda *, skip_data=False, market="us", standalone=False, runtime_context=None: {"rows": 1},
    )
    monkeypatch.setattr(
        tasks,
        "run_tradingview_preset_screeners",
        lambda *, market="us", standalone=False, runtime_context=None: {"growth": []},
    )

    summary = tasks.run_all_screening_processes(markets=["us"])

    captured = capsys.readouterr()
    assert summary["ok"] is False
    assert summary["failed_steps"] == 1
    assert "failed (us) - Leader / lagging" in captured.out
    assert "degraded" in captured.out.lower()



def test_run_signal_engine_processes_rejects_invalid_markets():
    with pytest.raises(ValueError, match="Unsupported market"):
        tasks.run_signal_engine_processes(markets=["jp"])


def test_write_runtime_profiles_include_debug_fields(monkeypatch) -> None:
    output_root = runtime_root("_test_runtime_orchestrator_profile_debug")
    output_root.mkdir(parents=True, exist_ok=True)
    profile_path = output_root / "us_runtime_profile.json"

    monkeypatch.setattr(tasks, "get_runtime_profile_path", lambda market: str(profile_path))

    outcomes = [
        tasks.TaskStepOutcome(
            ok=True,
            label="Mark Minervini technical",
            market="us",
            elapsed_seconds=1.25,
            status="ok",
            status_counts={"ok": 1},
        ),
        tasks.TaskStepOutcome(
            ok=False,
            label="Leader / lagging",
            market="us",
            elapsed_seconds=0.75,
            error="429 Too Many Requests",
            status="failed",
            status_counts={"failed": 1},
            retryable=True,
            error_code="rate_limited",
            error_detail="429 Too Many Requests",
            timings={"provider_wait_seconds": 3.5},
            collector_diagnostics={"counts": {"single_fetches": 2}, "examples": []},
            current_symbol="AAA",
            current_chunk="feature_analysis:3/10",
            as_of_date="2026-04-18",
            data_freshness={
                "counts": {"closed": 1, "stale": 1, "future_or_partial": 0, "empty": 0},
                "stages": {
                    "leader_lagging": {
                        "counts": {"closed": 1, "stale": 1, "future_or_partial": 0, "empty": 0},
                        "target_date": "2026-04-18",
                        "latest_completed_session": "2026-04-21",
                        "mode": "default_completed_session",
                        "examples": [{"symbol": "AAA", "status": "stale"}],
                    }
                },
            },
            cooldown_snapshot={"cooldown_in": 12.5},
            last_retryable_error="429 Too Many Requests",
            runtime_metrics={
                "frame_load": {
                    "files": 2,
                    "rows": 200,
                    "parquet_hits": 1,
                    "parquet_misses": 1,
                    "seconds": 0.25,
                },
                "feature_analysis": {
                    "symbols": 10,
                    "cache_hits": 4,
                    "cache_misses": 6,
                    "seconds": 0.5,
                },
                "output_persist": {
                    "files": 3,
                    "rows": 20,
                    "bytes": 1024,
                    "seconds": 0.1,
                },
                "augment": {
                    "chronos2": {"input_count": 5, "seconds": 0.2},
                },
                "worker_budget": {
                    "leader_lagging.feature_analysis": {
                        "workers": 2,
                        "total_items": 10,
                    }
                },
            },
        ),
    ]

    tasks._write_runtime_profiles("Full screening process", outcomes)

    payload = json.loads(profile_path.read_text(encoding="utf-8"))

    assert payload["label"] == "Full screening process"
    assert payload["last_successful_stage"] == "Mark Minervini technical"
    assert payload["last_stage"] == "Leader / lagging"
    assert payload["last_error_code"] == "rate_limited"
    assert payload["last_error_detail"] == "429 Too Many Requests"
    assert payload["run_id"]
    assert payload["started_at"]
    assert payload["last_progress_at"]
    assert payload["current_symbol"] == "AAA"
    assert payload["current_chunk"] == "feature_analysis:3/10"
    assert payload["as_of_date"] == "2026-04-18"
    assert payload["data_freshness"]["counts"]["stale"] == 1
    assert payload["data_freshness"]["stages"]["leader_lagging"]["examples"][0]["symbol"] == "AAA"
    assert payload["steps"][1]["data_freshness"]["stages"]["leader_lagging"]["mode"] == "default_completed_session"
    assert payload["timings"]["provider_wait_seconds"] == 3.5
    assert payload["collector_diagnostics"]["counts"]["single_fetches"] == 2
    assert payload["steps"][1]["collector_diagnostics"]["counts"]["single_fetches"] == 2
    assert payload["cooldown_snapshot"] == {"cooldown_in": 12.5}
    assert payload["last_retryable_error"] == "429 Too Many Requests"
    assert payload["frame_load"]["files"] == 2
    assert payload["feature_analysis"]["cache_hits"] == 4
    assert payload["output_persist"]["bytes"] == 1024
    assert payload["augment"]["chronos2"]["input_count"] == 5
    assert payload["worker_budget"]["leader_lagging.feature_analysis"]["workers"] == 2
    assert payload["steps"][1]["runtime_metrics"]["output_persist"]["rows"] == 20



def test_runtime_state_json_tracks_stage_progress_and_failure(monkeypatch) -> None:
    runtime_dir = runtime_root("_test_runtime_state_json")
    runtime_dir.mkdir(parents=True, exist_ok=True)
    state_path = runtime_dir / "runtime_state.json"

    monkeypatch.setattr(tasks, "get_runtime_state_path", lambda market: str(state_path))

    runtime_context = RuntimeContext(market="us")
    tasks._ensure_runtime_context_state(runtime_context, market="us")
    runtime_context.set_as_of_date("2026-04-18")
    runtime_context.update_runtime_state(
        current_stage="Qullamaggie",
        current_symbol="AAA",
        current_chunk="setup_scan:1/3",
        status="running",
    )
    tasks._run_timed_step(
        1,
        1,
        "Qullamaggie",
        "us",
        lambda: {
            "ok": False,
            "status": "failed",
            "error_code": "rate_limited",
            "error_detail": "429 Too Many Requests",
            "retryable": True,
        },
        runtime_context=runtime_context,
    )
    tasks._flush_runtime_state_writes([str(state_path)])

    payload = json.loads(state_path.read_text(encoding="utf-8"))

    assert payload["run_id"]
    assert payload["market"] == "us"
    assert payload["as_of_date"] == "2026-04-18"
    assert payload["current_stage"] == "Qullamaggie"
    assert payload["current_symbol"] == ""
    assert payload["current_chunk"] == ""
    assert payload["last_error_code"] == "rate_limited"
    assert payload["last_error_detail"] == "429 Too Many Requests"
    assert payload["last_retryable_error"] == "429 Too Many Requests"
    assert payload["status"] == "failed"
    assert payload["cooldown_snapshot"] == {}
    assert payload["last_progress_at"]


def test_runtime_context_merges_data_freshness_by_stage() -> None:
    runtime_context = RuntimeContext(market="us")
    first_summary = OhlcvFreshnessSummary.from_reports(
        [
            describe_ohlcv_freshness(
                pd.DataFrame({"date": ["2026-04-17"], "close": [10.0]}),
                market="us",
                symbol="AAA",
                as_of="2026-04-21",
                latest_completed_session="2026-04-21",
            )
        ]
    )
    second_summary = OhlcvFreshnessSummary.from_reports(
        [
            describe_ohlcv_freshness(
                pd.DataFrame({"date": ["2026-04-21"], "close": [10.0]}),
                market="us",
                symbol="BBB",
                as_of="2026-04-21",
                latest_completed_session="2026-04-21",
            )
        ]
    )

    runtime_context.update_data_freshness("markminervini_technical", first_summary)
    runtime_context.update_data_freshness("tradingview_presets", second_summary)

    freshness = runtime_context.runtime_state["data_freshness"]
    assert freshness["counts"] == {
        "closed": 1,
        "stale": 1,
        "future_or_partial": 0,
        "empty": 0,
    }
    assert freshness["stages"]["markminervini_technical"]["examples"][0]["symbol"] == "AAA"
    assert freshness["stages"]["tradingview_presets"]["counts"]["closed"] == 1


def test_run_timed_step_preserves_progress_snapshot_in_outcome() -> None:
    runtime_context = RuntimeContext(market="us")
    runtime_context.set_as_of_date("2026-04-18")

    def _action() -> dict[str, object]:
        runtime_context.update_runtime_state(
            current_stage="Leader / lagging",
            current_symbol="AAA",
            current_chunk="feature_analysis:3/10",
            data_freshness={
                "counts": {"closed": 0, "stale": 1, "future_or_partial": 0, "empty": 0},
                "stages": {
                    "leader_lagging": {
                        "counts": {"closed": 0, "stale": 1, "future_or_partial": 0, "empty": 0},
                        "target_date": "2026-04-18",
                        "latest_completed_session": "2026-04-21",
                        "mode": "default_completed_session",
                        "examples": [{"symbol": "AAA", "status": "stale"}],
                    }
                },
            },
            cooldown_snapshot={"cooldown_in": 12.5},
            last_retryable_error="429 Too Many Requests",
            status="running",
        )
        return {
            "ok": False,
            "status": "rate_limited",
            "error_code": "rate_limited",
            "error_detail": "429 Too Many Requests",
            "retryable": True,
        }

    outcome = tasks._run_timed_step(
        1,
        1,
        "Leader / lagging",
        "us",
        _action,
        runtime_context=runtime_context,
    )

    assert outcome.current_symbol == "AAA"
    assert outcome.current_chunk == "feature_analysis:3/10"
    assert outcome.as_of_date == "2026-04-18"
    assert outcome.data_freshness["counts"]["stale"] == 1
    assert outcome.to_dict()["data_freshness"]["stages"]["leader_lagging"]["target_date"] == "2026-04-18"
    assert outcome.cooldown_snapshot == {"cooldown_in": 12.5}
    assert outcome.last_retryable_error == "429 Too Many Requests"
    assert runtime_context.runtime_state["current_symbol"] == ""
    assert runtime_context.runtime_state["current_chunk"] == ""


def test_run_scheduler_runs_screening_with_signals(monkeypatch):
    calls: list[dict[str, object]] = []

    def _fake_market_pipeline(*, skip_data=False, markets=None, include_signals=False):  # noqa: ANN202
        calls.append(
            {
                "skip_data": skip_data,
                "markets": markets,
                "include_signals": include_signals,
            }
        )
        raise KeyboardInterrupt()

    monkeypatch.setattr(tasks, "run_market_analysis_pipeline", _fake_market_pipeline)

    tasks.run_scheduler()

    assert calls == [{"skip_data": True, "markets": ["us"], "include_signals": True}]
