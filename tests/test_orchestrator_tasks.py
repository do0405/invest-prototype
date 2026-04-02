from __future__ import annotations

import sys
import types

import data_collectors
import pytest

from orchestrator import tasks


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
    assert handoffs == ["US OHLCV", "KR OHLCV", "US stock metadata", "KR stock metadata"]
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



def test_run_all_screening_processes_applies_yahoo_handoffs_before_financial_steps(monkeypatch):
    calls: list[tuple[str, str]] = []
    handoffs: list[str] = []

    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))
    monkeypatch.setattr(tasks, "run_markminervini_screening", lambda *, market="us": calls.append((market, "technical")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", lambda *, market="us", skip_data=False: calls.append((market, "financial")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_integrated_screening", lambda *, market="us": calls.append((market, "integrated")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda *, market="us": calls.append((market, "tracking")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", lambda *, market="us": calls.append((market, "weinstein")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", lambda *, market="us": calls.append((market, "leader")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", lambda *, skip_data=False, market="us": calls.append((market, "qullamaggie")) or {"rows": 1})
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", lambda *, market="us": calls.append((market, "tradingview")) or {"growth": []})

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



def test_run_signal_engine_task_calls_signal_scan(monkeypatch):
    captured: dict[str, object] = {}
    fake_module = types.ModuleType("screeners.signals")

    def _fake_run_multi_screener_signal_scan(*, market="us", as_of_date=None, upcoming_earnings_fetcher=None, earnings_collector=None):  # noqa: ANN202
        captured["market"] = market
        return {"all_signals_v2": []}

    setattr(fake_module, "run_multi_screener_signal_scan", _fake_run_multi_screener_signal_scan)
    monkeypatch.setitem(sys.modules, "screeners.signals", fake_module)

    result = tasks.run_signal_engine_task(market="kr")

    assert captured["market"] == "kr"
    assert result == {"all_signals_v2": []}



def test_run_signal_engine_processes_runs_each_market(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        tasks,
        "run_signal_engine_task",
        lambda *, market="us": calls.append(market) or {"all_signals_v2": []},
    )

    summary = tasks.run_signal_engine_processes(markets=["us", "kr"])

    assert calls == ["us", "kr"]
    assert summary["ok"] is True


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



def test_run_all_screening_processes_reports_failed_steps_honestly(monkeypatch, capsys):
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: None)
    monkeypatch.setattr(tasks, "run_markminervini_screening", lambda *, market="us": {"rows": 1})
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", lambda *, market="us", skip_data=False: {"rows": 1})
    monkeypatch.setattr(tasks, "run_integrated_screening", lambda *, market="us": {"rows": 1})
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda *, market="us": {"rows": 1})
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", lambda *, market="us": {"rows": 1})
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", lambda *, market="us": {"error": "boom"})
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", lambda *, skip_data=False, market="us": {"rows": 1})
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", lambda *, market="us": {"growth": []})

    summary = tasks.run_all_screening_processes(markets=["us"])

    captured = capsys.readouterr()
    assert summary["ok"] is False
    assert summary["failed_steps"] == 1
    assert "failed (us) - Leader / lagging" in captured.out
    assert "degraded" in captured.out.lower()



def test_run_signal_engine_processes_rejects_invalid_markets():
    with pytest.raises(ValueError, match="Unsupported market"):
        tasks.run_signal_engine_processes(markets=["jp"])



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
