from __future__ import annotations

from typing import Any, Callable

import numpy as np
import pandas as pd

from utils.market_data_contract import PricePolicy, load_local_ohlcv_frame


def _round_or_none(value: float | None, digits: int = 4) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return round(float(value), digits)


def _shape_trailing_close_path(
    frame: pd.DataFrame,
    window_size: int,
) -> np.ndarray | None:
    if frame.empty or len(frame) < window_size:
        return None
    closes = pd.to_numeric(frame["close"], errors="coerce").dropna().tail(window_size)
    if len(closes) != window_size:
        return None
    values = closes.to_numpy(dtype=float)
    base = max(float(values[-1]), 1e-9)
    return ((values / base) - 1.0).astype(float)


def _standardized_path(values: np.ndarray) -> np.ndarray:
    std = float(np.std(values))
    if not np.isfinite(std) or std <= 1e-9:
        return np.zeros(len(values), dtype=float)
    return ((values - float(np.mean(values))) / std).astype(float)


def _pearson(left: np.ndarray, right: np.ndarray) -> float:
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    left_std = float(np.std(left))
    right_std = float(np.std(right))
    if (
        not np.isfinite(left_std)
        or not np.isfinite(right_std)
        or left_std <= 1e-12
        or right_std <= 1e-12
    ):
        return 0.0
    matrix = np.corrcoef(left, right)
    value = float(matrix[0, 1])
    return value if np.isfinite(value) else 0.0


def _shape_similarity_score(left: np.ndarray, right: np.ndarray) -> float:
    corr = _pearson(left, right)
    return max(0.0, min(100.0, 50.0 + (corr * 50.0)))


def _best_lag(anchor: np.ndarray, peer: np.ndarray, max_lag_days: int) -> tuple[int, float]:
    best = (0, _pearson(anchor, peer))
    for lag in range(-max_lag_days, max_lag_days + 1):
        if lag == 0:
            continue
        offset = abs(lag)
        if len(anchor) <= offset or len(peer) <= offset:
            continue
        if lag > 0:
            left = anchor[:-offset]
            right = peer[offset:]
        else:
            left = anchor[offset:]
            right = peer[:-offset]
        corr = _pearson(left, right)
        if corr > best[1] or (np.isclose(corr, best[1]) and abs(lag) < abs(best[0])):
            best = (lag, corr)
    return best


def generate_global_lag_diagnostic_rows(
    *,
    merged_candidate_rows: list[dict[str, Any]],
    market: str,
    as_of_date: str | None = None,
    load_ohlcv_frame_fn: Callable[..., pd.DataFrame] = load_local_ohlcv_frame,
    top_k_per_anchor: int = 5,
    window_sizes: tuple[int, ...] = (40, 80, 120),
    max_lag_days: int = 5,
) -> list[dict[str, Any]]:
    ordered_rows = [dict(row) for row in merged_candidate_rows if str(row.get("symbol") or "").strip()]
    if len(ordered_rows) < 2:
        return []

    frames = {
        str(row.get("symbol") or "").strip().upper(): load_ohlcv_frame_fn(
            symbol=str(row.get("symbol") or "").strip().upper(),
            market=market,
            as_of=as_of_date,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
        )
        for row in ordered_rows
    }
    row_by_symbol = {
        str(row.get("symbol") or "").strip().upper(): row for row in ordered_rows
    }

    output_rows: list[dict[str, Any]] = []
    for window_size in window_sizes:
        path_by_symbol: dict[str, np.ndarray] = {}
        standardized_by_symbol: dict[str, np.ndarray] = {}
        for symbol, frame in frames.items():
            path = _shape_trailing_close_path(frame, window_size)
            if path is not None:
                path_by_symbol[symbol] = path
                standardized_by_symbol[symbol] = _standardized_path(path)
        symbols = sorted(path_by_symbol)
        if len(symbols) < 2:
            continue
        for anchor_symbol in symbols:
            anchor_path = path_by_symbol[anchor_symbol]
            peers: list[tuple[float, str]] = []
            for peer_symbol in symbols:
                if peer_symbol == anchor_symbol:
                    continue
                peers.append(
                    (
                        _shape_similarity_score(anchor_path, path_by_symbol[peer_symbol]),
                        peer_symbol,
                    )
                )
            ranked_peers = sorted(peers, key=lambda item: (-item[0], item[1]))[:top_k_per_anchor]
            for rank, (shape_score, peer_symbol) in enumerate(ranked_peers, start=1):
                best_lag_days, lag_corr = _best_lag(
                    standardized_by_symbol[anchor_symbol],
                    standardized_by_symbol[peer_symbol],
                    max_lag_days=max_lag_days,
                )
                anchor_row = row_by_symbol[anchor_symbol]
                peer_row = row_by_symbol[peer_symbol]
                output_rows.append(
                    {
                        "anchor_symbol": anchor_symbol,
                        "peer_symbol": peer_symbol,
                        "window_size": int(window_size),
                        "shape_similarity_score": _round_or_none(shape_score),
                        "best_lag_days": int(best_lag_days),
                        "lag_correlation_score": _round_or_none(lag_corr),
                        "lead_confidence": _round_or_none(max(0.0, lag_corr * 100.0)),
                        "anchor_source_tags": list(anchor_row.get("source_tags", [])),
                        "peer_source_tags": list(peer_row.get("source_tags", [])),
                        "anchor_primary_source_style": str(anchor_row.get("primary_source_style") or ""),
                        "peer_primary_source_style": str(peer_row.get("primary_source_style") or ""),
                        "same_primary_style": str(anchor_row.get("primary_source_style") or "")
                        == str(peer_row.get("primary_source_style") or ""),
                        "pair_rank_for_anchor": int(rank),
                    }
                )
    return sorted(
        output_rows,
        key=lambda row: (
            str(row.get("anchor_symbol") or ""),
            int(row.get("window_size") or 0),
            int(row.get("pair_rank_for_anchor") or 0),
            str(row.get("peer_symbol") or ""),
        ),
    )
