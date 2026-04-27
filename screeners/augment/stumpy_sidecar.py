from __future__ import annotations

import importlib
import math
from collections import defaultdict, deque
from typing import Any, Callable

import numpy as np
import pandas as pd

from utils.market_data_contract import PricePolicy, load_local_ohlcv_frame


def _round_or_none(value: float | None, digits: int = 4) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return round(float(value), digits)


def _safe_close_volume_paths(
    frame: pd.DataFrame, window_size: int
) -> tuple[np.ndarray, np.ndarray] | None:
    if frame.empty or len(frame) < window_size:
        return None
    closes = pd.to_numeric(frame["close"], errors="coerce").tail(window_size).to_numpy(dtype=float)
    volume_source = (
        frame["volume"]
        if "volume" in frame.columns
        else pd.Series(0.0, index=frame.index, dtype=float)
    )
    volumes = pd.to_numeric(volume_source, errors="coerce").tail(window_size).to_numpy(dtype=float)
    if np.isnan(closes).any():
        return None
    if np.isnan(volumes).any():
        volumes = np.nan_to_num(volumes, nan=0.0)
    close_base = max(float(closes[-1]), 1e-9)
    volume_base = max(float(np.log1p(max(closes[-1], 1e-9) * max(volumes[-1], 1.0))), 1e-9)
    price_path = (closes / close_base) - 1.0
    volume_path = (np.log1p(np.maximum(closes, 0.0) * np.maximum(volumes, 0.0)) / volume_base) - 1.0
    return price_path.astype(float), volume_path.astype(float)


def _default_distance_fn(left: np.ndarray, right: np.ndarray) -> float:
    stumpy = importlib.import_module("stumpy")

    profile = stumpy.mass(np.asarray(left, dtype=float), np.asarray(right, dtype=float))
    return float(np.asarray(profile, dtype=float)[0])


def _default_self_profile_fn(series: np.ndarray, window_size: int) -> float:
    stumpy = importlib.import_module("stumpy")

    mp = stumpy.stump(np.asarray(series, dtype=float), m=window_size)
    profile = getattr(mp, "P_", None)
    if profile is None:
        profile = np.asarray(mp, dtype=object)[:, 0]
    profile_array = np.asarray(profile, dtype=float)
    return float(profile_array[-1])


def _score_from_distance(distance: float | None) -> float | None:
    if distance is None or not np.isfinite(distance):
        return None
    return float(max(0.0, min(100.0, 100.0 / (1.0 + max(distance, 0.0)))))


def _shape_label(frame: pd.DataFrame, window_size: int) -> str:
    closes = pd.to_numeric(frame["close"], errors="coerce").tail(window_size).to_numpy(dtype=float)
    if len(closes) < window_size or closes[0] <= 0:
        return "SIDEWAYS"
    net_return = (float(closes[-1]) / float(closes[0])) - 1.0
    if net_return >= 0.08:
        return "UP"
    if net_return <= -0.08:
        return "DOWN"
    return "SIDEWAYS"


def _connected_components(nodes: list[str], edges: dict[str, set[str]]) -> list[list[str]]:
    components: list[list[str]] = []
    seen: set[str] = set()
    for node in nodes:
        if node in seen:
            continue
        queue: deque[str] = deque([node])
        component: list[str] = []
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            component.append(current)
            queue.extend(sorted(edges.get(current, set()) - seen))
        components.append(sorted(component))
    return components


def _pair_key(left: str, right: str) -> tuple[str, str]:
    ordered = tuple(sorted((left, right)))
    return ordered[0], ordered[1]


def build_runtime_skip_rows(
    *,
    source_rows: list[dict[str, Any]],
    source_tag: str,
    market: str,
    window_sizes: tuple[int, ...] = (40, 80, 120),
    status: str = "RUNTIME_SKIP",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in source_rows:
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        for window_size in window_sizes:
            rows.append(
                {
                    "symbol": symbol,
                    "market": str(item.get("market") or market).upper(),
                    "source_tag": source_tag,
                    "window_size": int(window_size),
                    "stumpy_cluster_id": "",
                    "stumpy_cluster_size": None,
                    "stumpy_exemplar_symbol": "",
                    "stumpy_price_motif_score": None,
                    "stumpy_self_discord_score": None,
                    "stumpy_volume_overlay_score": None,
                    "stumpy_shape_label": "SIDEWAYS",
                    "stumpy_status": str(status or "RUNTIME_SKIP").strip().upper(),
                }
            )
    return sorted(
        rows,
        key=lambda row: (str(row.get("symbol") or ""), int(row.get("window_size") or 0)),
    )


def generate_stumpy_summary_rows(
    *,
    source_rows: list[dict[str, Any]],
    source_tag: str,
    market: str,
    as_of_date: str | None = None,
    window_sizes: tuple[int, ...] = (40, 80, 120),
    load_ohlcv_frame_fn: Callable[..., pd.DataFrame] = load_local_ohlcv_frame,
    distance_fn: Callable[[np.ndarray, np.ndarray], float] | None = None,
    self_profile_fn: Callable[[np.ndarray, int], float] | None = None,
) -> list[dict[str, Any]]:
    resolved_distance = distance_fn or _default_distance_fn
    resolved_self_profile = self_profile_fn or _default_self_profile_fn
    rows: list[dict[str, Any]] = []
    if not source_rows:
        return rows

    loaded_frames: dict[str, pd.DataFrame] = {}
    for item in source_rows:
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        loaded_frames[symbol] = load_ohlcv_frame_fn(
            symbol=symbol,
            market=market,
            as_of=as_of_date,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
        )

    for window_size in window_sizes:
        valid_entries: list[dict[str, Any]] = []
        pending_rows: list[dict[str, Any]] = []
        for item in source_rows:
            symbol = str(item.get("symbol") or "").strip().upper()
            frame = loaded_frames.get(symbol, pd.DataFrame())
            base_row = {
                "symbol": symbol,
                "market": str(item.get("market") or market).upper(),
                "source_tag": source_tag,
                "window_size": int(window_size),
                "stumpy_cluster_id": "",
                "stumpy_cluster_size": None,
                "stumpy_exemplar_symbol": "",
                "stumpy_price_motif_score": None,
                "stumpy_self_discord_score": None,
                "stumpy_volume_overlay_score": None,
                "stumpy_shape_label": "SIDEWAYS",
                "stumpy_status": "OK",
            }
            if frame.empty:
                base_row["stumpy_status"] = "MISSING_OHLCV"
                pending_rows.append(base_row)
                continue
            if len(frame) < window_size:
                base_row["stumpy_status"] = "INSUFFICIENT_HISTORY"
                pending_rows.append(base_row)
                continue
            paths = _safe_close_volume_paths(frame, window_size)
            if paths is None:
                base_row["stumpy_status"] = "MISSING_OHLCV"
                pending_rows.append(base_row)
                continue
            price_path, volume_path = paths
            closes = pd.to_numeric(frame["close"], errors="coerce").to_numpy(dtype=float)
            self_distance = resolved_self_profile(closes, window_size)
            valid_entries.append(
                {
                    "base_row": base_row,
                    "frame": frame,
                    "symbol": symbol,
                    "price_path": price_path,
                    "volume_path": volume_path,
                    "self_discord_score": _score_from_distance(self_distance),
                }
            )

        if not valid_entries:
            rows.extend(pending_rows)
            continue

        nearest_neighbor: dict[str, str | None] = {}
        nearest_distance: dict[str, float | None] = {}
        nearest_volume_score: dict[str, float | None] = {}
        pair_distance: dict[tuple[str, str], float] = {}
        pair_volume_score: dict[tuple[str, str], float | None] = {}

        for left in valid_entries:
            best_symbol: str | None = None
            best_distance: float | None = None
            best_volume_score: float | None = None
            for right in valid_entries:
                if left["symbol"] == right["symbol"]:
                    continue
                key = _pair_key(left["symbol"], right["symbol"])
                if key not in pair_distance:
                    pair_distance[key] = resolved_distance(left["price_path"], right["price_path"])
                    pair_volume_score[key] = _score_from_distance(
                        resolved_distance(left["volume_path"], right["volume_path"])
                    )
                candidate_distance = pair_distance[key]
                if best_distance is None or candidate_distance < best_distance:
                    best_symbol = right["symbol"]
                    best_distance = candidate_distance
                    best_volume_score = pair_volume_score[key]
            nearest_neighbor[left["symbol"]] = best_symbol
            nearest_distance[left["symbol"]] = best_distance
            nearest_volume_score[left["symbol"]] = best_volume_score

        finite_top1 = [value for value in nearest_distance.values() if value is not None and math.isfinite(value)]
        threshold = float(np.quantile(finite_top1, 0.35)) if finite_top1 else float("inf")
        edges: dict[str, set[str]] = defaultdict(set)
        for symbol, neighbor in nearest_neighbor.items():
            if not neighbor:
                continue
            if nearest_neighbor.get(neighbor) != symbol:
                continue
            distance = nearest_distance.get(symbol)
            if distance is None or distance > threshold:
                continue
            edges[symbol].add(neighbor)
            edges[neighbor].add(symbol)

        nodes = [entry["symbol"] for entry in valid_entries]
        components = _connected_components(nodes, edges)
        cluster_lookup: dict[str, dict[str, Any]] = {}
        for cluster_index, component in enumerate(sorted(components, key=lambda items: items[0]), start=1):
            cluster_id = f"{source_tag}_W{window_size}_C{cluster_index}"
            if len(component) == 1:
                exemplar_symbol = component[0]
            else:
                mean_distances: dict[str, float] = {}
                for symbol in component:
                    distances = [
                        pair_distance[_pair_key(symbol, peer)]
                        for peer in component
                        if peer != symbol
                    ]
                    mean_distances[symbol] = float(np.mean(distances)) if distances else float("inf")
                exemplar_symbol = min(mean_distances, key=lambda item: (mean_distances[item], item))
            exemplar_frame = next(entry["frame"] for entry in valid_entries if entry["symbol"] == exemplar_symbol)
            shape_label = _shape_label(exemplar_frame, window_size)
            for symbol in component:
                cluster_lookup[symbol] = {
                    "stumpy_cluster_id": cluster_id,
                    "stumpy_cluster_size": len(component),
                    "stumpy_exemplar_symbol": exemplar_symbol,
                    "stumpy_shape_label": shape_label,
                    "stumpy_status": "OK" if len(component) > 1 else "SINGLETON",
                }

        for entry in valid_entries:
            symbol = entry["symbol"]
            cluster_payload = cluster_lookup[symbol]
            row = dict(entry["base_row"])
            row.update(cluster_payload)
            row["stumpy_price_motif_score"] = _round_or_none(
                _score_from_distance(nearest_distance.get(symbol))
            )
            row["stumpy_self_discord_score"] = _round_or_none(entry["self_discord_score"])
            row["stumpy_volume_overlay_score"] = _round_or_none(nearest_volume_score.get(symbol))
            rows.append(row)

        rows.extend(pending_rows)

    return sorted(
        rows,
        key=lambda row: (str(row.get("symbol") or ""), int(row.get("window_size") or 0)),
    )
