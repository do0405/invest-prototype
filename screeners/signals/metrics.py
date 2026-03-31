from __future__ import annotations

from typing import Any, Callable, Mapping

import pandas as pd



def load_feature_map(
    frames: Mapping[str, pd.DataFrame],
    *,
    analyzer: Any,
    market: str,
    metadata_map: Mapping[str, Mapping[str, Any]],
    frame_keyed_records_fn: Callable[..., dict[str, dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol, frame in frames.items():
        if frame.empty:
            continue
        rows.append(analyzer.compute_feature_row(symbol, market, frame, metadata_map.get(symbol)))

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
) -> dict[str, dict[str, Any]]:
    metrics_map: dict[str, dict[str, Any]] = {}
    for symbol, frame in frames.items():
        source_entry = source_registry.get(
            symbol,
            {
                "symbol": symbol,
                "buy_eligible": symbol in peg_ready_map or symbol in peg_event_history_map,
                "watch_only": False,
                "screen_stage": (
                    "PEG_ONLY" if (symbol in peg_ready_map or symbol in peg_event_history_map) else "UNIVERSE"
                ),
                "source_tags": ["PEG_ONLY"] if (symbol in peg_ready_map or symbol in peg_event_history_map) else [],
                "source_overlap_bonus": 0.0,
            },
        )
        metrics_map[symbol] = build_metrics_fn(
            symbol=symbol,
            market=market,
            frame=frame,
            metadata=metadata_map.get(symbol, {}),
            financial_row=financial_map.get(symbol, {}),
            feature_row=feature_map.get(symbol, {}),
            source_entry=source_entry,
        )
    return metrics_map