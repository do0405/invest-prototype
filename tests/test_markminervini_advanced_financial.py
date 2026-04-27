from __future__ import annotations

from pathlib import Path

import pandas as pd

from screeners.markminervini import advanced_financial
from tests._paths import runtime_root
from utils.runtime_context import RuntimeContext


def test_run_advanced_financial_screening_normalizes_symbol_types_before_merge(
    monkeypatch,
):
    root = runtime_root("_test_runtime_markminervini_advanced_financial")
    root.mkdir(parents=True, exist_ok=True)
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    with_rs_path = results_dir / "with_rs.csv"
    advanced_path = results_dir / "advanced_financial_results.csv"
    integrated_path = results_dir / "integrated_results.csv"

    pd.DataFrame(
        [
            {
                "symbol": "001510",
                "rs_score": 88.0,
                "met_count": 7,
            }
        ]
    ).to_csv(with_rs_path, index=False)

    monkeypatch.setattr(advanced_financial, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_with_rs_path",
        lambda market: str(with_rs_path),
    )
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_advanced_financial_results_path",
        lambda market: str(advanced_path),
    )
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_integrated_results_path",
        lambda market: str(integrated_path),
    )
    monkeypatch.setattr(
        advanced_financial,
        "collect_financial_data_hybrid",
        lambda symbols, max_retries=2, delay=1.0, market="kr": pd.DataFrame(
            [
                    {
                        "symbol": "001510",
                        "provider_symbol": "001510.KS",
                    "annual_eps_growth": 30.0,
                    "eps_growth_acceleration": True,
                    "annual_revenue_growth": 20.0,
                    "revenue_growth_acceleration": True,
                    "net_margin_improved": True,
                    "eps_3q_accel": True,
                    "sales_3q_accel": True,
                    "margin_3q_accel": True,
                    "debt_to_equity": 10.0,
                    "fetch_status": "complete",
                    "has_error": False,
                    "unavailable_reason": None,
                }
            ]
        ),
    )

    result = advanced_financial.run_advanced_financial_screening(market="kr")

    assert len(result) == 1
    assert str(result.loc[0, "symbol"]) == "001510"
    assert int(result.loc[0, "fin_met_count"]) >= 4
    assert Path(advanced_path).exists()


def test_run_advanced_financial_screening_reuses_runtime_context_technical_frame(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_markminervini_advanced_financial_context")
    root.mkdir(parents=True, exist_ok=True)
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    advanced_path = results_dir / "advanced_financial_results.csv"
    integrated_path = results_dir / "integrated_results.csv"

    runtime_context = RuntimeContext(market="us")
    runtime_context.screening_frames["markminervini_with_rs"] = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "rs_score": 88.0,
                "met_count": 7,
            }
        ]
    )

    monkeypatch.setattr(advanced_financial, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_with_rs_path",
        lambda market: str(root / "missing.csv"),
    )
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_advanced_financial_results_path",
        lambda market: str(advanced_path),
    )
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_integrated_results_path",
        lambda market: str(integrated_path),
    )
    monkeypatch.setattr(
        advanced_financial.pd,
        "read_csv",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("technical csv should not be reread when runtime_context already has it")
        ),
    )
    monkeypatch.setattr(
        advanced_financial,
        "collect_financial_data_hybrid",
        lambda symbols, max_retries=2, delay=1.0, market="us": pd.DataFrame(
            [
                {
                    "symbol": "AAA",
                    "provider_symbol": "AAA",
                    "annual_eps_growth": 30.0,
                    "eps_growth_acceleration": True,
                    "annual_revenue_growth": 20.0,
                    "revenue_growth_acceleration": True,
                    "net_margin_improved": True,
                    "eps_3q_accel": True,
                    "sales_3q_accel": True,
                    "margin_3q_accel": True,
                    "debt_to_equity": 10.0,
                    "fetch_status": "complete",
                    "has_error": False,
                    "unavailable_reason": None,
                }
            ]
        ),
    )

    result = advanced_financial.run_advanced_financial_screening(
        market="us",
        runtime_context=runtime_context,
    )

    assert len(result) == 1
    assert str(result.loc[0, "symbol"]) == "AAA"
    assert runtime_context.screening_frames["advanced_financial_df"].equals(result)


def test_run_advanced_financial_screening_skip_data_reuses_cached_financial_rows(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_markminervini_advanced_financial_skip_cache")
    root.mkdir(parents=True, exist_ok=True)
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    advanced_path = results_dir / "advanced_financial_results.csv"
    integrated_path = results_dir / "integrated_results.csv"
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "provider_symbol": "AAA",
                "fin_met_count": 5,
                "fetch_status": "complete",
                "unavailable_reason": "",
                "has_error": False,
            },
            {
                "symbol": "CACHED_ONLY",
                "provider_symbol": "CACHED_ONLY",
                "fin_met_count": 9,
                "fetch_status": "complete",
                "unavailable_reason": "",
                "has_error": False,
            },
        ]
    ).to_csv(advanced_path, index=False)

    monkeypatch.setattr(advanced_financial, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_with_rs_path",
        lambda market: str(root / "missing_with_rs.csv"),
    )
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_advanced_financial_results_path",
        lambda market: str(advanced_path),
    )
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_integrated_results_path",
        lambda market: str(integrated_path),
    )
    monkeypatch.setattr(
        advanced_financial,
        "collect_financial_data_hybrid",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("skip_data=True must not call financial providers")
        ),
    )

    runtime_context = RuntimeContext(market="us")
    technical_df = pd.DataFrame(
        [
            {"symbol": "AAA", "rs_score": 95.0, "met_count": 8},
            {"symbol": "ZZZ", "rs_score": 70.0, "met_count": 6},
        ]
    )

    result = advanced_financial.run_advanced_financial_screening(
        skip_data=True,
        market="us",
        technical_df=technical_df,
        runtime_context=runtime_context,
    )

    by_symbol = result.set_index("symbol")
    assert set(by_symbol.index) == {"AAA", "ZZZ"}
    assert int(by_symbol.loc["AAA", "fin_met_count"]) == 5
    assert str(by_symbol.loc["AAA", "fetch_status"]) == "complete"
    assert int(by_symbol.loc["ZZZ", "fin_met_count"]) == 0
    assert str(by_symbol.loc["ZZZ", "fetch_status"]) == "skipped_local_only"
    assert str(by_symbol.loc["ZZZ", "unavailable_reason"]) == "local_only_no_cached_financials"
    assert bool(by_symbol.loc["ZZZ", "has_error"]) is False
    assert Path(integrated_path).exists()
    assert runtime_context.screening_frames["advanced_financial_df"].equals(result)


def test_run_advanced_financial_screening_skip_data_builds_placeholders_without_cache(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_markminervini_advanced_financial_skip_placeholder")
    root.mkdir(parents=True, exist_ok=True)
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    advanced_path = results_dir / "advanced_financial_results.csv"
    integrated_path = results_dir / "integrated_results.csv"

    monkeypatch.setattr(advanced_financial, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_with_rs_path",
        lambda market: str(root / "missing_with_rs.csv"),
    )
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_advanced_financial_results_path",
        lambda market: str(advanced_path),
    )
    monkeypatch.setattr(
        advanced_financial,
        "get_markminervini_integrated_results_path",
        lambda market: str(integrated_path),
    )
    monkeypatch.setattr(
        advanced_financial,
        "collect_financial_data_hybrid",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("skip_data=True must not call financial providers")
        ),
    )

    result = advanced_financial.run_advanced_financial_screening(
        skip_data=True,
        market="us",
        technical_df=pd.DataFrame([{"symbol": "AAA", "rs_score": 88.0, "met_count": 7}]),
    )

    assert len(result) == 1
    assert str(result.loc[0, "symbol"]) == "AAA"
    assert int(result.loc[0, "fin_met_count"]) == 0
    assert str(result.loc[0, "fetch_status"]) == "skipped_local_only"
    assert str(result.loc[0, "unavailable_reason"]) == "local_only_no_cached_financials"
    assert bool(result.loc[0, "has_error"]) is False
    assert Path(advanced_path).exists()
    assert Path(integrated_path).exists()
