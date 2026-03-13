from __future__ import annotations

from .screener import run_qullamaggie_screening


def run_qullamaggie_strategy(
    setups: list[str] | None = None,
    skip_data: bool = False,
    enable_earnings_filter: bool = True,
    *,
    market: str = "us",
) -> bool:
    _ = skip_data
    if not setups:
        run_qullamaggie_screening(
            setup_type=None,
            enable_earnings_filter=enable_earnings_filter,
            market=market,
        )
        return True

    for setup in setups:
        run_qullamaggie_screening(
            setup_type=setup,
            enable_earnings_filter=enable_earnings_filter,
            market=market,
        )
    return True
