# -*- coding: utf-8 -*-
"""File and data loading utilities."""

import json
import os
import pandas as pd
import concurrent.futures
import re
import time
from datetime import datetime, timedelta

from config import (
    DATA_DIR,
    DATA_KR_DIR,
    DATA_US_DIR,
    EXTERNAL_DATA_DIR,
    RESULTS_DIR,
)

__all__ = [
    "ensure_dir",
    "create_required_dirs",
    "load_csvs_parallel",
    "extract_ticker_from_filename",
    "process_stock_data",
    "safe_filename",
    "write_dataframe_csv_with_fallback",
    "write_dataframe_json_with_fallback",
    "write_json_with_fallback",
]


def ensure_dir(directory: str) -> None:
    """Create directory if it does not exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"📁 디렉토리 생성됨: {directory}")


def create_required_dirs(directories=None) -> None:
    """Create a set of directories used in the project."""
    if directories is None:
        directories = [
            DATA_DIR,
            DATA_US_DIR,
            DATA_KR_DIR,
            RESULTS_DIR,
            EXTERNAL_DATA_DIR,
        ]

    for directory in directories:
        ensure_dir(directory)


def load_csvs_parallel(file_paths, max_workers=4):
    """Load multiple CSV files in parallel (thread-safe)."""
    results = {}
    temp_results = []  # 임시 결과 저장

    def load_csv(file_path):
        try:
            df = pd.read_csv(file_path)
            
            # 컬럼명을 소문자로 변환
            df.columns = [c.lower() for c in df.columns]
            
            # 필수 컬럼 존재 여부 확인
            required_columns = ['close', 'volume', 'date']
            if not all(col in df.columns for col in required_columns):
                print(f"⚠️ {os.path.basename(file_path)}: 필수 컬럼 누락 - {[col for col in required_columns if col not in df.columns]}")
                return os.path.basename(file_path), None
            
            return os.path.basename(file_path), df
        except Exception as e:
            print(f"❌ {file_path} 로드 오류: {e}")
            return os.path.basename(file_path), None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(load_csv, file_path): file_path for file_path in file_paths}
        for future in concurrent.futures.as_completed(future_to_file):
            file_name, df = future.result()
            if df is not None:
                temp_results.append((file_name, df))
    
    # 결과 병합 (메인 스레드에서 안전하게 처리)
    for file_name, df in temp_results:
        results[file_name] = df

    return results


def extract_ticker_from_filename(filename: str) -> str:
    """Extract ticker symbol from file name."""
    base_name = os.path.splitext(filename)[0]
    ticker = base_name.split('_')[0]
    return ticker


def process_stock_data(file, data_dir, min_days=200, recent_days=200):
    """Load and preprocess stock data."""
    try:
        file_path = os.path.join(data_dir, file)
        symbol = extract_ticker_from_filename(file)
        from utils.screener_utils import read_csv_flexible
        df = read_csv_flexible(file_path, required_columns=['date', 'close'])
        if df is None:
            return None, None, None
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')
        else:
            return None, None, None
        if len(df) < min_days:
            return None, None, None
        recent_data = df.iloc[-recent_days:].copy()
        return symbol, df, recent_data
    except Exception as e:
        print(f"❌ {file} 처리 오류: {e}")
        return None, None, None


def safe_filename(filename: str) -> str:
    """Return a filesystem safe filename."""
    invalid_chars = r'[<>:\\/?*"|]'
    safe_name = re.sub(invalid_chars, '_', filename)
    reserved_names = [
        'CON', 'PRN', 'AUX', 'NUL',
        'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
        'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
    ]
    name_without_ext = os.path.splitext(safe_name)[0]
    extension = os.path.splitext(safe_name)[1]
    if name_without_ext.upper() in reserved_names:
        safe_name = name_without_ext + '_file' + extension
    return safe_name


def _timestamped_output_path(path: str) -> str:
    directory = os.path.dirname(path)
    stem, suffix = os.path.splitext(os.path.basename(path))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return os.path.join(directory, f"{stem}_{timestamp}{suffix}")


def _compact_json_enabled() -> bool:
    raw = str(os.getenv("INVEST_PROTO_COMPACT_JSON", "")).strip().lower()
    if not raw:
        return False
    return raw not in {"0", "false", "no", "off"}


def _resolved_json_indent(indent: int | None) -> int | None:
    return None if _compact_json_enabled() else indent


def _record_output_metric(
    runtime_context,
    *,
    path: str,
    rows: int,
    seconds: float,
    kind: str,
    label: str = "",
) -> None:
    if runtime_context is None or not hasattr(runtime_context, "record_output_write"):
        return
    try:
        bytes_written = os.path.getsize(path) if os.path.exists(path) else 0
    except OSError:
        bytes_written = 0
    runtime_context.record_output_write(
        path=path,
        rows=rows,
        bytes_written=bytes_written,
        seconds=seconds,
        kind=kind,
        label=label,
    )


def _payload_row_count(payload) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            return len(rows)
    return 1


def write_dataframe_csv_with_fallback(
    frame: pd.DataFrame,
    path: str,
    *,
    index: bool = False,
    runtime_context=None,
    metric_label: str = "",
) -> str:
    ensure_dir(os.path.dirname(path))
    started = time.perf_counter()
    try:
        frame.to_csv(path, index=index)
        _record_output_metric(
            runtime_context,
            path=path,
            rows=int(len(frame)),
            seconds=time.perf_counter() - started,
            kind="csv",
            label=metric_label,
        )
        return path
    except PermissionError:
        fallback_path = _timestamped_output_path(path)
        try:
            frame.to_csv(fallback_path, index=index)
        except PermissionError as fallback_exc:
            raise PermissionError(
                "Unable to write CSV output. "
                f"Primary path permission denied: {path}; "
                f"fallback path permission denied: {fallback_path}. "
                "Set INVEST_PROTO_RESULTS_DIR to a writable output root or release the locked results directory."
            ) from fallback_exc
        print(f"[IO] Locked output fallback used - primary={path}, fallback={fallback_path}")
        _record_output_metric(
            runtime_context,
            path=fallback_path,
            rows=int(len(frame)),
            seconds=time.perf_counter() - started,
            kind="csv",
            label=metric_label,
        )
        return fallback_path


def write_dataframe_json_with_fallback(
    frame: pd.DataFrame,
    path: str,
    *,
    orient: str = "records",
    indent: int = 2,
    force_ascii: bool = False,
    runtime_context=None,
    metric_label: str = "",
) -> str:
    ensure_dir(os.path.dirname(path))
    started = time.perf_counter()
    json_indent = _resolved_json_indent(indent)
    try:
        frame.to_json(path, orient=orient, indent=json_indent, force_ascii=force_ascii)
        _record_output_metric(
            runtime_context,
            path=path,
            rows=int(len(frame)),
            seconds=time.perf_counter() - started,
            kind="json",
            label=metric_label,
        )
        return path
    except PermissionError:
        fallback_path = _timestamped_output_path(path)
        try:
            frame.to_json(fallback_path, orient=orient, indent=json_indent, force_ascii=force_ascii)
        except PermissionError as fallback_exc:
            raise PermissionError(
                "Unable to write JSON output. "
                f"Primary path permission denied: {path}; "
                f"fallback path permission denied: {fallback_path}. "
                "Set INVEST_PROTO_RESULTS_DIR to a writable output root or release the locked results directory."
            ) from fallback_exc
        print(f"[IO] Locked output fallback used - primary={path}, fallback={fallback_path}")
        _record_output_metric(
            runtime_context,
            path=fallback_path,
            rows=int(len(frame)),
            seconds=time.perf_counter() - started,
            kind="json",
            label=metric_label,
        )
        return fallback_path


def write_json_with_fallback(
    payload,
    path: str,
    *,
    ensure_ascii: bool = False,
    indent: int = 2,
    runtime_context=None,
    metric_label: str = "",
) -> str:
    ensure_dir(os.path.dirname(path))
    started = time.perf_counter()
    json_indent = _resolved_json_indent(indent)
    separators = (",", ":") if _compact_json_enabled() else None
    rows = _payload_row_count(payload)
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=ensure_ascii, indent=json_indent, separators=separators)
        _record_output_metric(
            runtime_context,
            path=path,
            rows=rows,
            seconds=time.perf_counter() - started,
            kind="json",
            label=metric_label,
        )
        return path
    except PermissionError:
        fallback_path = _timestamped_output_path(path)
        try:
            with open(fallback_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=ensure_ascii, indent=json_indent, separators=separators)
        except PermissionError as fallback_exc:
            raise PermissionError(
                "Unable to write JSON output. "
                f"Primary path permission denied: {path}; "
                f"fallback path permission denied: {fallback_path}. "
                "Set INVEST_PROTO_RESULTS_DIR to a writable output root or release the locked results directory."
            ) from fallback_exc
        print(f"[IO] Locked output fallback used - primary={path}, fallback={fallback_path}")
        _record_output_metric(
            runtime_context,
            path=fallback_path,
            rows=rows,
            seconds=time.perf_counter() - started,
            kind="json",
            label=metric_label,
        )
        return fallback_path
