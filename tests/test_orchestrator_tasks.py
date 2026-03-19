from __future__ import annotations

import sys
import types

import data_collectors

from orchestrator import tasks


def test_collect_data_main_runs_ohlcv_before_metadata_and_applies_handoffs(monkeypatch):
    calls: list[tuple[str, object]] = []
    handoffs: list[str] = []

    fake_data_collector = types.ModuleType("data_collector")

    def _fake_collect_data(*, update_symbols: bool = True):  # noqa: ANN202
        calls.append(("us_ohlcv", update_symbols))

    fake_data_collector.collect_data = _fake_collect_data

    monkeypatch.setitem(sys.modules, "data_collector", fake_data_collector)
    monkeypatch.setattr(tasks, "wait_for_yahoo_phase_handoff", lambda label: handoffs.append(label))
    monkeypatch.setattr(
        tasks,
        "run_stock_metadata_collection",
        lambda *, market="us": calls.append((f"{market}_metadata", None)),
    )
    monkeypatch.setattr(
        tasks,
        "run_kr_ohlcv_collection",
        lambda **kwargs: calls.append(("kr_ohlcv", kwargs)),
    )

    tasks.collect_data_main(update_symbols=True, skip_ohlcv=False, include_kr=True)

    assert calls == [
        ("us_ohlcv", True),
        ("kr_ohlcv", {"include_etn": True}),
        ("us_metadata", None),
        ("kr_metadata", None),
    ]
    assert handoffs == ["US OHLCV", "KR OHLCV", "US stock metadata", "KR stock metadata"]


def test_run_kr_ohlcv_collection_defaults_to_include_etn(monkeypatch):
    observed: dict[str, object] = {}
    fake_module = types.ModuleType("data_collectors.kr_ohlcv_collector")
    fake_module.KR_OHLCV_DEFAULT_LOOKBACK_DAYS = 520

    def _fake_collect_kr_ohlcv_csv(**kwargs):  # noqa: ANN202
        observed.update(kwargs)
        return {"total": 1, "saved": 1, "failed": 0}

    fake_module.collect_kr_ohlcv_csv = _fake_collect_kr_ohlcv_csv
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
    monkeypatch.setattr(tasks, "run_markminervini_screening", lambda *, market="us": calls.append((market, "technical")))
    monkeypatch.setattr(tasks, "run_advanced_financial_screening", lambda *, market="us", skip_data=False: calls.append((market, "financial")))
    monkeypatch.setattr(tasks, "run_integrated_screening", lambda *, market="us": calls.append((market, "integrated")))
    monkeypatch.setattr(tasks, "run_new_ticker_tracking", lambda *, market="us": calls.append((market, "tracking")))
    monkeypatch.setattr(tasks, "run_weinstein_stage2_screening", lambda *, market="us": calls.append((market, "weinstein")))
    monkeypatch.setattr(tasks, "run_leader_lagging_screening", lambda *, market="us": calls.append((market, "leader")))
    monkeypatch.setattr(tasks, "run_qullamaggie_strategy_task", lambda *, skip_data=False, market="us": calls.append((market, "qullamaggie")))
    monkeypatch.setattr(tasks, "run_tradingview_preset_screeners", lambda *, market="us": calls.append((market, "tradingview")))

    tasks.run_all_screening_processes(markets=["us"])

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


def test_run_signal_engine_task_calls_signal_scan(monkeypatch):
    captured: dict[str, object] = {}
    fake_module = types.ModuleType("screeners.signals")

    def _fake_run_signal_scan(*, market="us", as_of_date=None, upcoming_earnings_fetcher=None, earnings_collector=None):  # noqa: ANN202
        captured["market"] = market
        return {"all_signals_v2": []}

    setattr(fake_module, "run_signal_scan", _fake_run_signal_scan)
    monkeypatch.setitem(sys.modules, "screeners.signals", fake_module)

    result = tasks.run_signal_engine_task(market="kr")

    assert captured["market"] == "kr"
    assert result == {"all_signals_v2": []}


def test_run_signal_engine_processes_runs_each_market(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        tasks,
        "run_signal_engine_task",
        lambda *, market="us": calls.append(market),
    )

    tasks.run_signal_engine_processes(markets=["us", "kr"])

    assert calls == ["us", "kr"]
