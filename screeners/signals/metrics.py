from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Mapping

import pandas as pd


def _resolved_max_workers(total_items: int, max_workers: int | None) -> int:
    if total_items <= 1:
        return 1
    if max_workers is None:
        return min(8, total_items)
    return max(1, min(int(max_workers), total_items))


def load_feature_map(
    frames: Mapping[str, pd.DataFrame],
    *,
    analyzer: Any,
    market: str,
    metadata_map: Mapping[str, Mapping[str, Any]],
    frame_keyed_records_fn: Callable[..., dict[str, dict[str, Any]]],
    max_workers: int | None = None,
) -> dict[str, dict[str, Any]]:
    symbols = [symbol for symbol in sorted(frames) if not frames[symbol].empty]
    rows: list[dict[str, Any]] = []
    resolved_workers = _resolved_max_workers(len(symbols), max_workers)

    def _compute(symbol: str) -> dict[str, Any]:
        return analyzer.compute_feature_row(
            symbol,
            market,
            frames[symbol],
            metadata_map.get(symbol),
        )

    if resolved_workers == 1:
        for symbol in symbols:
            rows.append(_compute(symbol))
    else:
        row_map: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=resolved_workers) as executor:
            future_map = {
                symbol: executor.submit(_compute, symbol)
                for symbol in symbols
            }
            for symbol in symbols:
                row_map[symbol] = future_map[symbol].result()
        rows = [row_map[symbol] for symbol in symbols]

    if not rows:
        return {}

    table = analyzer.finalize_feature_table(pd.DataFrame(rows))
    return frame_keyed_records_fn(
        table, key_column="symbol", uppercase_keys=True, drop_na=True
    )


def build_metrics_map(
    *,
    frames: Mapping[str, pd.DataFrame],
    market: str,
    metadata_map: Mapping[str, Mapping[str, Any]],
    financial_map: Mapping[str, Mapping[str, Any]],
    feature_map: Mapping[str, Mapping[str, Any]],
    source_registry: Mapping[str, Mapping[str, Any]],
    peg_ready_map: Mapping[str, Mapping[str, Any]],
    peg_event_history_map: Mapping[str, Mapping[str, Any]],
    build_metrics_fn: Callable[..., dict[str, Any]],
    max_workers: int | None = None,
) -> dict[str, dict[str, Any]]:
    metrics_map: dict[str, dict[str, Any]] = {}
    symbols = sorted(frames)
    resolved_workers = _resolved_max_workers(len(symbols), max_workers)

    def _build(symbol: str) -> dict[str, Any]:
        frame = frames[symbol]
        source_entry = source_registry.get(
            symbol,
            {
                "symbol": symbol,
                "source_buy_eligible": (
                    symbol in peg_ready_map or symbol in peg_event_history_map
                ),
                "buy_eligible": symbol in peg_ready_map or symbol in peg_event_history_map,
                "screen_stage": (
                    "PEG_READY"
                    if (symbol in peg_ready_map or symbol in peg_event_history_map)
                    else ""
                ),
                "source_tags": (
                    ["PEG_READY"]
                    if (symbol in peg_ready_map or symbol in peg_event_history_map)
                    else []
                ),
                "source_overlap_bonus": 0.0,
            },
        )
        return build_metrics_fn(
            symbol=symbol,
            market=market,
            frame=frame,
            metadata=metadata_map.get(symbol, {}),
            financial_row=financial_map.get(symbol, {}),
            feature_row=feature_map.get(symbol, {}),
            source_entry=source_entry,
        )

    if resolved_workers == 1:
        for symbol in symbols:
            metrics_map[symbol] = _build(symbol)
    else:
        with ThreadPoolExecutor(max_workers=resolved_workers) as executor:
            future_map = {
                symbol: executor.submit(_build, symbol)
                for symbol in symbols
            }
            for symbol in symbols:
                metrics_map[symbol] = future_map[symbol].result()
    return metrics_map
