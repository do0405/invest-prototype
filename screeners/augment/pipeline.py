from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from screeners.signals import source_registry as signal_source_registry
from screeners.source_contracts import (
    CANONICAL_SOURCE_SPECS,
    primary_source_style,
    source_engine_bonus,
    source_priority_score,
    source_style_tags,
    source_tag_priority,
    normalize_source_disposition,
    sorted_source_tags,
    stage_priority,
)
from utils.market_data_contract import load_local_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_augment_results_dir,
    get_market_source_registry_snapshot_path,
    get_market_screeners_root,
    market_key,
    results_root_override_active,
)
from utils.screener_utils import save_screening_results
from utils.symbol_normalization import (
    normalize_provider_symbol_value,
    normalize_symbol_columns,
    normalize_symbol_value,
)
from utils.runtime_context import RuntimeContext

from .chronos_rerank import CHRONOS_MODEL_ID, generate_chronos_rerank_rows
from .lag_diagnostics import generate_global_lag_diagnostic_rows
from .run_summary import build_augment_run_summary, write_augment_run_summary
from .stumpy_sidecar import build_runtime_skip_rows, generate_stumpy_summary_rows
from .timesfm_rerank import TIMESFM_MODEL_ID, generate_timesfm_rerank_rows
from .tsfm_metrics import TSFM_MODEL_FAMILY, build_soft_skip_rows


def build_buy_eligible_source_specs():
    return [
        spec
        for spec in CANONICAL_SOURCE_SPECS
        if str(getattr(spec, "source_disposition", "")).strip().lower()
        == "buy_eligible"
        or spec.buy_eligible
    ]


def build_merged_candidate_pool_rows(
    registry: dict[str, dict[str, Any]],
    *,
    market: str,
) -> list[dict[str, Any]]:
    normalized_market = market_key(market).upper()
    rows: list[dict[str, Any]] = []
    for symbol in sorted(registry):
        entry = registry[symbol]
        source_disposition = normalize_source_disposition(
            entry.get("source_disposition"),
            default=(
                "buy_eligible"
                if bool(entry.get("source_buy_eligible") or entry.get("buy_eligible"))
                else ("watch_only" if bool(entry.get("watch_only")) else "")
            ),
        )
        if source_disposition not in {"buy_eligible", "watch_only"}:
            continue
        rows.append(
            {
                "symbol": str(entry.get("symbol") or symbol),
                "market": str(entry.get("market") or normalized_market).upper(),
                "source_disposition": source_disposition,
                "source_buy_eligible": bool(
                    entry.get("source_buy_eligible") or entry.get("buy_eligible")
                ),
                "buy_eligible": bool(
                    entry.get("source_buy_eligible") or entry.get("buy_eligible")
                ),
                "watch_only": source_disposition == "watch_only",
                "screen_stage": str(entry.get("screen_stage") or ""),
                "source_tags": list(entry.get("source_tags", [])),
                "primary_source_tag": str(entry.get("primary_source_tag") or ""),
                "primary_source_stage": str(entry.get("primary_source_stage") or ""),
                "primary_source_style": str(entry.get("primary_source_style") or ""),
                "source_style_tags": list(entry.get("source_style_tags", [])),
                "source_priority_score": float(entry.get("source_priority_score") or 0.0),
                "trend_source_bonus": float(entry.get("trend_source_bonus") or 0.0),
                "ug_source_bonus": float(entry.get("ug_source_bonus") or 0.0),
                "source_overlap_bonus": float(entry.get("source_overlap_bonus") or 0.0),
                "sector": str(entry.get("sector") or ""),
                "industry": str(entry.get("industry") or ""),
                "group_name": str(entry.get("group_name") or ""),
                "as_of_ts": entry.get("as_of_ts"),
            }
        )
    return rows


def _write_records(output_dir: str, filename_prefix: str, rows: list[dict[str, Any]]) -> dict[str, str]:
    return save_screening_results(
        results=rows,
        output_dir=output_dir,
        filename_prefix=filename_prefix,
        include_timestamp=False,
    )


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _raise_isolated_results_prerequisite_error(
    *,
    snapshot_path: str,
    expected_paths: Sequence[str],
) -> None:
    examples = ", ".join(str(path) for path in list(expected_paths)[:3])
    if len(expected_paths) > 3:
        examples = f"{examples}, ..."
    raise ValueError(
        "augment prerequisite screening artifacts are missing under isolated results root. "
        f"Expected source registry snapshot at {snapshot_path} or at least one screening artifact such as {examples}"
    )


def _load_raw_source_rows(
    *,
    screeners_root: str,
    market: str,
    source_specs: Sequence[Any],
) -> list[dict[str, Any]]:
    normalized_market = market_key(market)
    payloads: list[dict[str, Any]] = []
    for spec in source_specs:
        path = os.path.join(screeners_root, spec.relative_path)
        if not os.path.exists(path):
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if frame.empty:
            continue
        frame.columns = [str(column).strip() for column in frame.columns]
        symbol_column = signal_source_registry.resolve_symbol_column(frame)
        if symbol_column is None:
            continue
        if symbol_column == "provider_symbol":
            frame = normalize_symbol_columns(
                frame,
                normalized_market,
                provider_columns=(symbol_column,),
            )
        else:
            frame = normalize_symbol_columns(
                frame,
                normalized_market,
                columns=(symbol_column,),
            )
        rows: list[dict[str, Any]] = []
        for _, row in frame.iterrows():
            raw_symbol = row.get(symbol_column)
            if symbol_column == "provider_symbol":
                provider_symbol = normalize_provider_symbol_value(raw_symbol)
                if normalized_market == "kr" and provider_symbol and "." in provider_symbol:
                    base, _suffix = provider_symbol.rsplit(".", 1)
                    symbol = normalize_symbol_value(base, normalized_market)
                else:
                    symbol = provider_symbol
            else:
                symbol = normalize_symbol_value(raw_symbol, normalized_market)
            if not symbol:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "market": normalized_market.upper(),
                    "source_tag": spec.source_tag,
                    "screen_stage": spec.screen_stage,
                    "relative_path": spec.relative_path,
                    "as_of_ts": _safe_text(row.get("as_of_ts") or row.get("date")) or None,
                }
            )
        if rows:
            payloads.append({"spec": spec, "path": path, "rows": rows})
    return payloads


def _flatten_output_paths(paths: Mapping[str, str] | None) -> list[str]:
    if not isinstance(paths, Mapping):
        return []
    return [str(value) for value in paths.values() if str(value).strip()]


def _row_status_counts(rows: Sequence[Mapping[str, Any]], status_key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get(status_key) or "").strip().upper()
        if not status:
            continue
        counts[status] = counts.get(status, 0) + 1
    return counts


def _merge_status(current: str, incoming: str) -> str:
    priority = {
        "OK": 0,
        "SKIPPED_MISSING_DEP": 1,
        "SKIPPED_MISSING_MODEL": 2,
        "FAILED_RUNTIME": 3,
    }
    left = str(current or "OK").strip().upper()
    right = str(incoming or "OK").strip().upper()
    return right if priority.get(right, 0) >= priority.get(left, 0) else left


def _module_summary(
    *,
    status: str,
    input_count: int = 0,
    rows_written: int,
    elapsed_seconds: float,
    detail: str = "",
    status_counts: Mapping[str, int] | None = None,
    output_files: Sequence[str] | None = None,
) -> dict[str, Any]:
    return {
        "status": str(status or "OK").strip().upper(),
        "input_count": int(max(input_count, 0)),
        "rows_written": int(rows_written),
        "elapsed_seconds": round(float(elapsed_seconds), 6),
        "detail": str(detail or "").strip(),
        "status_counts": {
            str(key): int(value) for key, value in (status_counts or {}).items()
        },
        "output_files": [str(path) for path in (output_files or []) if str(path).strip()],
    }


def _tsfm_failure_payload(
    *,
    merged_rows: list[dict[str, Any]],
    market: str,
    exc: Exception,
    model_id: str,
) -> tuple[list[dict[str, Any]], str]:
    message = str(exc or "").strip()
    if isinstance(exc, ImportError):
        return build_soft_skip_rows(
            merged_candidate_rows=merged_rows,
            market=market,
            fm_status="RUNTIME_SKIP",
            model_id=model_id,
            model_family=TSFM_MODEL_FAMILY,
        ), "SKIPPED_MISSING_DEP"
    if "MISSING_MODEL" in message.upper():
        return build_soft_skip_rows(
            merged_candidate_rows=merged_rows,
            market=market,
            fm_status="MISSING_MODEL",
            model_id=model_id,
            model_family=TSFM_MODEL_FAMILY,
        ), "SKIPPED_MISSING_MODEL"
    return build_soft_skip_rows(
        merged_candidate_rows=merged_rows,
        market=market,
        fm_status="RUNTIME_SKIP",
        model_id=model_id,
        model_family=TSFM_MODEL_FAMILY,
    ), "FAILED_RUNTIME"


def run_screening_augment(
    *,
    market: str,
    load_ohlcv_frame_fn: Callable[..., pd.DataFrame] = load_local_ohlcv_frame,
    runtime_context: RuntimeContext | None = None,
    source_registry_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_market = market_key(market)
    ensure_market_dirs(normalized_market)

    screeners_root = get_market_screeners_root(normalized_market)
    augment_dir = get_augment_results_dir(normalized_market)
    os.makedirs(augment_dir, exist_ok=True)
    output_files: list[str] = []
    cache_stats = {"hits": 0, "misses": 0}

    buy_specs = build_buy_eligible_source_specs()
    active_snapshot = source_registry_snapshot
    provenance_kind = "explicit_snapshot" if active_snapshot is not None else ""
    snapshot_path = get_market_source_registry_snapshot_path(normalized_market)
    if active_snapshot is None and runtime_context is not None:
        active_snapshot = runtime_context.source_registry_snapshot
        provenance_kind = "runtime_context" if active_snapshot is not None else provenance_kind
    as_of_date = str(
        (runtime_context.as_of_date if runtime_context is not None else "")
        or (active_snapshot or {}).get("as_of_date")
        or ""
    ).strip()
    if active_snapshot is None:
        active_snapshot = signal_source_registry.read_source_registry_snapshot(
            snapshot_path,
            market=normalized_market,
            as_of_date=as_of_date or None,
        )
        if active_snapshot is not None and runtime_context is not None:
            runtime_context.source_registry_snapshot = active_snapshot
            runtime_context.record_cache_hit("source_registry_snapshot")
        provenance_kind = "snapshot_file" if active_snapshot is not None else provenance_kind
    if active_snapshot is not None:
        as_of_date = str(active_snapshot.get("as_of_date") or as_of_date).strip()
    if not as_of_date:
        as_of_date = datetime.now().strftime("%Y-%m-%d")
    if runtime_context is not None:
        runtime_context.set_as_of_date(as_of_date)

    if active_snapshot is not None:
        cache_stats["hits"] += 1
        if runtime_context is not None and runtime_context.source_registry_snapshot is None:
            runtime_context.source_registry_snapshot = dict(active_snapshot)
        raw_payloads = signal_source_registry.source_payloads_from_snapshot(
            active_snapshot,
            screeners_root=screeners_root,
            source_specs=buy_specs,
            buy_eligible_only=True,
        )
        registry = signal_source_registry.registry_from_snapshot(
            active_snapshot,
            buy_eligible_only=True,
        )
    else:
        if results_root_override_active():
            existing_paths = signal_source_registry.existing_source_artifact_paths(
                screeners_root=screeners_root,
                source_specs=buy_specs,
            )
            if not existing_paths:
                _raise_isolated_results_prerequisite_error(
                    snapshot_path=snapshot_path,
                    expected_paths=signal_source_registry.expected_source_artifact_paths(
                        screeners_root=screeners_root,
                        source_specs=buy_specs,
                    ),
                )
        provenance_kind = "csv_scan"
        cache_stats["misses"] += 1
        if runtime_context is not None:
            runtime_context.record_cache_miss("source_registry_snapshot")
        raw_payloads = _load_raw_source_rows(
            screeners_root=screeners_root,
            market=normalized_market,
            source_specs=buy_specs,
        )
        registry = signal_source_registry.load_source_registry(
            screeners_root=screeners_root,
            market=normalized_market,
            source_specs=buy_specs,
            stage_priority=stage_priority,
            source_tag_priority=source_tag_priority,
            sorted_source_tags=sorted_source_tags,
            source_style_tags=source_style_tags,
            primary_source_style=primary_source_style,
            source_priority_score=source_priority_score,
            source_engine_bonus=source_engine_bonus,
            safe_text=_safe_text,
        )
    rows_read = sum(len(payload.get("rows", [])) for payload in raw_payloads)
    merged_rows = build_merged_candidate_pool_rows(registry, market=normalized_market)
    merged_paths = _write_records(augment_dir, "merged_candidate_pool", merged_rows)
    output_files.extend(_flatten_output_paths(merged_paths))

    timings: dict[str, float] = {}
    module_summaries: dict[str, dict[str, Any]] = {}
    total_stumpy_rows = 0
    total_rows_written = len(merged_rows)

    stumpy_started_at = time.perf_counter()
    total_stumpy_rows = 0
    stumpy_output_files: list[str] = []
    stumpy_status = "OK"
    stumpy_detail = ""
    stumpy_status_counts: dict[str, int] = {}
    for payload in raw_payloads:
        spec = payload["spec"]
        source_rows = payload["rows"]
        try:
            stumpy_rows = generate_stumpy_summary_rows(
                source_rows=source_rows,
                source_tag=spec.source_tag,
                market=normalized_market,
                as_of_date=as_of_date,
                load_ohlcv_frame_fn=load_ohlcv_frame_fn,
            )
        except Exception as exc:
            stumpy_status = _merge_status(
                stumpy_status,
                "SKIPPED_MISSING_DEP" if isinstance(exc, ImportError) else "FAILED_RUNTIME",
            )
            if not stumpy_detail:
                stumpy_detail = str(exc)
            stumpy_rows = build_runtime_skip_rows(
                source_rows=source_rows,
                source_tag=spec.source_tag,
                market=normalized_market,
            )
        total_stumpy_rows += len(stumpy_rows)
        source_dir = os.path.dirname(payload["path"])
        stem = os.path.splitext(os.path.basename(payload["path"]))[0]
        written_paths = _write_records(source_dir, f"{stem}_stumpy_summary", stumpy_rows)
        stumpy_output_files.extend(_flatten_output_paths(written_paths))
        counts = _row_status_counts(stumpy_rows, "stumpy_status")
        for key, value in counts.items():
            stumpy_status_counts[key] = stumpy_status_counts.get(key, 0) + value
    stumpy_elapsed = time.perf_counter() - stumpy_started_at
    timings["stumpy_seconds"] = round(stumpy_elapsed, 6)
    total_rows_written += total_stumpy_rows
    output_files.extend(stumpy_output_files)
    module_summaries["stumpy"] = _module_summary(
        status=stumpy_status,
        input_count=sum(len(payload.get("rows", [])) for payload in raw_payloads),
        rows_written=total_stumpy_rows,
        elapsed_seconds=stumpy_elapsed,
        detail=stumpy_detail,
        status_counts=stumpy_status_counts,
        output_files=stumpy_output_files,
    )

    lag_started_at = time.perf_counter()
    lag_status = "OK"
    lag_detail = ""
    try:
        lag_rows = generate_global_lag_diagnostic_rows(
            merged_candidate_rows=merged_rows,
            market=normalized_market,
            as_of_date=as_of_date,
            load_ohlcv_frame_fn=load_ohlcv_frame_fn,
        )
    except Exception as exc:
        lag_status = "FAILED_RUNTIME"
        lag_detail = str(exc)
        lag_rows = []
    lag_paths = _write_records(augment_dir, "stumpy_global_pairs", lag_rows)
    lag_output_files = _flatten_output_paths(lag_paths)
    lag_elapsed = time.perf_counter() - lag_started_at
    timings["lag_diagnostics_seconds"] = round(lag_elapsed, 6)
    total_rows_written += len(lag_rows)
    output_files.extend(lag_output_files)
    module_summaries["lag_diagnostics"] = _module_summary(
        status=lag_status,
        input_count=len(merged_rows),
        rows_written=len(lag_rows),
        elapsed_seconds=lag_elapsed,
        detail=lag_detail,
        output_files=lag_output_files,
    )

    chronos_started_at = time.perf_counter()
    chronos_detail = ""
    chronos_status = "OK"
    try:
        rerank_rows = generate_chronos_rerank_rows(
            merged_candidate_rows=merged_rows,
            market=normalized_market,
            as_of_date=as_of_date,
            load_ohlcv_frame_fn=load_ohlcv_frame_fn,
        )
    except Exception as exc:
        rerank_rows, chronos_status = _tsfm_failure_payload(
            merged_rows=merged_rows,
            market=normalized_market,
            exc=exc,
            model_id=CHRONOS_MODEL_ID,
        )
        chronos_detail = str(exc)
    rerank_paths = _write_records(augment_dir, "chronos2_rerank", rerank_rows)
    rerank_output_files = _flatten_output_paths(rerank_paths)
    chronos_elapsed = time.perf_counter() - chronos_started_at
    timings["chronos2_seconds"] = round(chronos_elapsed, 6)
    total_rows_written += len(rerank_rows)
    output_files.extend(rerank_output_files)
    module_summaries["chronos2"] = _module_summary(
        status=chronos_status,
        input_count=len(merged_rows),
        rows_written=len(rerank_rows),
        elapsed_seconds=chronos_elapsed,
        detail=chronos_detail,
        status_counts=_row_status_counts(rerank_rows, "fm_status"),
        output_files=rerank_output_files,
    )

    timesfm_started_at = time.perf_counter()
    timesfm_detail = ""
    timesfm_status = "OK"
    try:
        timesfm_rows = generate_timesfm_rerank_rows(
            merged_candidate_rows=merged_rows,
            market=normalized_market,
            as_of_date=as_of_date,
            load_ohlcv_frame_fn=load_ohlcv_frame_fn,
        )
    except Exception as exc:
        timesfm_rows, timesfm_status = _tsfm_failure_payload(
            merged_rows=merged_rows,
            market=normalized_market,
            exc=exc,
            model_id=TIMESFM_MODEL_ID,
        )
        timesfm_detail = str(exc)
    timesfm_paths = _write_records(augment_dir, "timesfm2p5_rerank", timesfm_rows)
    timesfm_output_files = _flatten_output_paths(timesfm_paths)
    timesfm_elapsed = time.perf_counter() - timesfm_started_at
    timings["timesfm2p5_seconds"] = round(timesfm_elapsed, 6)
    total_rows_written += len(timesfm_rows)
    output_files.extend(timesfm_output_files)
    module_summaries["timesfm2p5"] = _module_summary(
        status=timesfm_status,
        input_count=len(merged_rows),
        rows_written=len(timesfm_rows),
        elapsed_seconds=timesfm_elapsed,
        detail=timesfm_detail,
        status_counts=_row_status_counts(timesfm_rows, "fm_status"),
        output_files=timesfm_output_files,
    )

    overall_status = (
        "ok"
        if all(summary["status"] == "OK" for summary in module_summaries.values())
        else "partial"
    )
    timings["total_seconds"] = round(time.perf_counter() - started_at, 6)
    source_registry_provenance = {
        "kind": provenance_kind or "csv_scan",
        "snapshot_used": active_snapshot is not None,
    }
    summary_payload = build_augment_run_summary(
        market=normalized_market,
        as_of_date=as_of_date,
        status=overall_status,
        diagnostic_only=True,
        input_universe_counts={
            "buy_eligible_symbols": len(merged_rows),
            "raw_source_files": len(raw_payloads),
        },
        source_registry_provenance=source_registry_provenance,
        module_summaries=module_summaries,
        timings=timings,
        cache_stats=cache_stats,
        rows_read=rows_read,
        rows_written=total_rows_written,
        output_files=output_files,
    )
    summary_path = write_augment_run_summary(
        os.path.join(augment_dir, "augment_run_summary.json"),
        summary_payload,
    )
    output_files.append(summary_path)
    runtime_metrics = {
        "augment": {
            key: {
                "input_count": int(value.get("input_count", 0)),
                "rows_written": int(value.get("rows_written", 0)),
                "seconds": float(value.get("elapsed_seconds", 0.0)),
                "status": str(value.get("status") or ""),
            }
            for key, value in module_summaries.items()
        }
    }
    if runtime_context is not None:
        for key, value in runtime_metrics["augment"].items():
            runtime_context.set_runtime_metric("augment", key, value)

    return {
        "market": normalized_market,
        "ok": overall_status != "failed",
        "status": overall_status,
        "status_counts": {overall_status: 1},
        "timings": timings,
        "cache_stats": cache_stats,
        "rows_read": rows_read,
        "rows_written": total_rows_written,
        "module_summaries": module_summaries,
        "runtime_metrics": runtime_metrics,
        "summary_path": summary_path,
        "raw_source_files": len(raw_payloads),
        "merged_candidates": len(merged_rows),
        "stumpy_rows": total_stumpy_rows,
        "chronos_rows": len(rerank_rows),
        "timesfm_rows": len(timesfm_rows),
        "lag_rows": len(lag_rows),
    }
