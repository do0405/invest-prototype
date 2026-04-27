from __future__ import annotations

import os
import re
import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable, Optional

import pandas as pd

from config import DATA_KR_DIR, DATA_US_DIR
from .io_utils import safe_filename
from .market_runtime import require_market_key
from .screening_cache import env_flag_enabled, source_file_signature, stable_payload_hash
from .typing_utils import is_na_like


class PricePolicy(str, Enum):
    RAW = "raw"
    SPLIT_ADJUSTED = "split_adjusted"
    TOTAL_RETURN_ADJUSTED = "total_return_adjusted"


CANONICAL_OHLCV_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

AUGMENTED_OHLCV_COLUMNS: tuple[str, ...] = (
    *CANONICAL_OHLCV_COLUMNS,
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "adj_close",
    "dividends",
    "stock_splits",
    "split_factor",
    "price_adjustment_factor",
    "price_adjustment_source",
    "price_policy",
)

SCREENING_OHLCV_READ_COLUMNS: tuple[str, ...] = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "dividends",
    "stock_splits",
    "split_factor",
)

OHLCV_FRESHNESS_STATUSES: tuple[str, ...] = (
    "closed",
    "stale",
    "future_or_partial",
    "empty",
)

OHLCV_FRESHNESS_EXAMPLE_LIMIT = 10
DEFAULT_LOCAL_FRAME_WORKER_CAP = 8
OHLCV_PARQUET_CACHE_VERSION = "1"
_OHLCV_PARQUET_DISABLED_REASON = ""


@dataclass(frozen=True)
class OhlcvFreshnessReport:
    market: str
    symbol: str = ""
    status: str = "empty"
    latest_date: str = ""
    target_date: str = ""
    latest_completed_session: str = ""
    mode: str = "default_completed_session"
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "market": self.market,
            "symbol": self.symbol,
            "status": self.status,
            "latest_date": self.latest_date,
            "target_date": self.target_date,
            "latest_completed_session": self.latest_completed_session,
            "mode": self.mode,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class OhlcvFreshnessSummary:
    counts: dict[str, int] = field(default_factory=dict)
    target_date: str = ""
    latest_completed_session: str = ""
    mode: str = "default_completed_session"
    examples: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def empty(
        cls,
        *,
        target_date: str = "",
        latest_completed_session: str = "",
        mode: str = "default_completed_session",
    ) -> "OhlcvFreshnessSummary":
        return cls(
            counts=_empty_freshness_counts(),
            target_date=str(target_date or "").strip(),
            latest_completed_session=str(latest_completed_session or "").strip(),
            mode=str(mode or "default_completed_session").strip() or "default_completed_session",
            examples=[],
        )

    @classmethod
    def from_reports(
        cls,
        reports: Iterable[OhlcvFreshnessReport],
    ) -> "OhlcvFreshnessSummary":
        counts = _empty_freshness_counts()
        examples: list[dict[str, Any]] = []
        target_date = ""
        latest_completed_session = ""
        mode = ""
        for report in reports:
            status = report.status if report.status in counts else "empty"
            counts[status] += 1
            if not target_date and report.target_date:
                target_date = report.target_date
            if not latest_completed_session and report.latest_completed_session:
                latest_completed_session = report.latest_completed_session
            if not mode and report.mode:
                mode = report.mode
            if status != "closed" and len(examples) < OHLCV_FRESHNESS_EXAMPLE_LIMIT:
                examples.append(report.to_dict())
        return cls(
            counts=counts,
            target_date=target_date,
            latest_completed_session=latest_completed_session,
            mode=mode or "default_completed_session",
            examples=examples,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "counts": {status: int(self.counts.get(status, 0)) for status in OHLCV_FRESHNESS_STATUSES},
            "target_date": self.target_date,
            "latest_completed_session": self.latest_completed_session,
            "mode": self.mode,
            "examples": [dict(item) for item in self.examples[:OHLCV_FRESHNESS_EXAMPLE_LIMIT]],
        }

OHLCV_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "date": ("date", "Date", "timestamp", "Timestamp", "datetime", "Datetime"),
    "open": ("open", "Open"),
    "high": ("high", "High"),
    "low": ("low", "Low"),
    "close": ("close", "Close"),
    "adj_close": ("adj_close", "Adj Close", "adj close", "adjusted_close", "adjusted close"),
    "volume": ("volume", "Volume"),
    "dividends": ("dividends", "Dividends", "dividend", "Dividend"),
    "stock_splits": ("stock_splits", "Stock Splits", "stock splits", "split", "splits"),
    "split_factor": ("split_factor", "Split Factor", "split factor"),
}

LEGACY_MOJIBAKE_ALIASES: dict[str, tuple[str, ...]] = {}


def normalize_market(market: str) -> str:
    market_key = str(market or "us").strip().lower()
    if not market_key:
        return "us"
    if not re.match(r"^[a-z0-9_-]+$", market_key):
        return "us"
    return market_key


def resolve_price_policy(price_policy: PricePolicy | str | None) -> PricePolicy:
    if isinstance(price_policy, PricePolicy):
        return price_policy
    normalized = str(price_policy or PricePolicy.SPLIT_ADJUSTED.value).strip().lower()
    for policy in PricePolicy:
        if normalized == policy.value:
            return policy
    return PricePolicy.SPLIT_ADJUSTED


def _normalize_trading_date(value: object) -> str | None:
    if is_na_like(value):
        return None

    text_value = str(value)
    parsed: pd.Timestamp | None = None
    try:
        parsed = pd.Timestamp(pd.to_datetime(text_value, errors="raise", format="mixed"))
    except Exception:
        try:
            parsed = pd.Timestamp(pd.to_datetime(text_value, errors="raise"))
        except Exception:
            return None

    if parsed is None or is_na_like(parsed):
        return None
    return parsed.date().isoformat()


def _empty_freshness_counts() -> dict[str, int]:
    return {status: 0 for status in OHLCV_FRESHNESS_STATUSES}


def _latest_ohlcv_date(frame: pd.DataFrame) -> str:
    if frame is None or frame.empty or "date" not in frame.columns:
        return ""
    dates = pd.to_datetime(frame["date"], errors="coerce")
    dates = dates.dropna()
    if dates.empty:
        return ""
    return pd.Timestamp(dates.max()).date().isoformat()


def _resolve_freshness_target(
    market: str,
    *,
    as_of: str | None = None,
    latest_completed_session: str | None = None,
) -> tuple[str, str]:
    completed = str(latest_completed_session or "").strip()
    if not completed:
        from .exchange_calendar import resolve_latest_completed_session

        completed = resolve_latest_completed_session(market)
    target = str(as_of or "").strip() or completed
    normalized_target = _normalize_trading_date(target) or target
    normalized_completed = _normalize_trading_date(completed) or completed
    return normalized_target, normalized_completed


def describe_ohlcv_freshness(
    frame: pd.DataFrame,
    *,
    market: str,
    symbol: str = "",
    as_of: str | None = None,
    latest_completed_session: str | None = None,
    explicit_as_of: bool = False,
) -> OhlcvFreshnessReport:
    normalized_market = require_market_key(market)
    target_date, completed_session = _resolve_freshness_target(
        normalized_market,
        as_of=as_of,
        latest_completed_session=latest_completed_session,
    )
    mode = "explicit_replay" if explicit_as_of else "default_completed_session"
    latest_date = _latest_ohlcv_date(frame)
    symbol_key = str(symbol or "").strip().upper()
    if not latest_date or not target_date:
        return OhlcvFreshnessReport(
            market=normalized_market,
            symbol=symbol_key,
            status="empty",
            latest_date=latest_date,
            target_date=target_date,
            latest_completed_session=completed_session,
            mode=mode,
            reason="missing_latest_or_target_date",
        )
    if latest_date < target_date:
        status = "stale"
        reason = "latest_row_older_than_target_date"
    elif latest_date > target_date:
        status = "future_or_partial"
        reason = "latest_row_newer_than_target_date"
    else:
        status = "closed"
        reason = "latest_row_matches_target_date"
    return OhlcvFreshnessReport(
        market=normalized_market,
        symbol=symbol_key,
        status=status,
        latest_date=latest_date,
        target_date=target_date,
        latest_completed_session=completed_session,
        mode=mode,
        reason=reason,
    )


def normalize_ohlcv_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame

    renamed = frame.copy()
    col_lookup = {str(col).strip().lower(): col for col in renamed.columns}

    for canonical, aliases in OHLCV_COLUMN_ALIASES.items():
        if canonical in renamed.columns:
            continue
        for alias in aliases:
            alias_key = str(alias).strip().lower()
            original = col_lookup.get(alias_key)
            if original is not None:
                renamed = renamed.rename(columns={original: canonical})
                col_lookup = {str(col).strip().lower(): col for col in renamed.columns}
                break

    return renamed


def _derive_split_factor(normalized: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    raw_split_factor = (
        normalized["split_factor"]
        if "split_factor" in normalized.columns
        else pd.Series(pd.NA, index=normalized.index)
    )
    explicit_split_factor = pd.to_numeric(raw_split_factor, errors="coerce")
    stock_splits = pd.to_numeric(
        normalized["stock_splits"]
        if "stock_splits" in normalized.columns
        else pd.Series(0.0, index=normalized.index),
        errors="coerce",
    ).fillna(0.0)

    factor = pd.Series(1.0, index=normalized.index, dtype=float)
    source = pd.Series("raw", index=normalized.index, dtype=object)

    running_factor = 1.0
    for index in range(len(normalized) - 1, -1, -1):
        factor.iat[index] = running_factor
        if running_factor != 1.0:
            source.iat[index] = "stock_splits_cumulative"
        event_value = stock_splits.iat[index]
        if pd.notna(event_value) and float(event_value) > 0:
            running_factor /= float(event_value)

    valid_explicit = explicit_split_factor.notna() & (explicit_split_factor > 0)
    if valid_explicit.any():
        factor.loc[valid_explicit] = explicit_split_factor.loc[valid_explicit].astype(float)
        source.loc[valid_explicit] = "split_factor"

    return factor, source


def normalize_ohlcv_frame(
    frame: pd.DataFrame,
    symbol: str,
    *,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    normalized = normalize_ohlcv_columns(frame.copy())
    has_input_adj_close = "adj_close" in normalized.columns
    if "date" not in normalized.columns:
        normalized = normalized.reset_index()
        if len(normalized.columns) > 0:
            normalized = normalized.rename(columns={normalized.columns[0]: "date"})

    if "date" not in normalized.columns:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    normalized["date"] = normalized["date"].map(_normalize_trading_date)
    normalized["symbol"] = symbol

    raw_close_source = normalized["close"] if "close" in normalized.columns else pd.Series(0.0, index=normalized.index)
    for column in ("open", "high", "low", "close"):
        if column not in normalized.columns:
            normalized[column] = raw_close_source
    if "adj_close" not in normalized.columns:
        normalized["adj_close"] = raw_close_source
    if "volume" not in normalized.columns:
        normalized["volume"] = 0.0
    if "dividends" not in normalized.columns:
        normalized["dividends"] = 0.0
    if "stock_splits" not in normalized.columns:
        normalized["stock_splits"] = 0.0
    if "split_factor" not in normalized.columns:
        normalized["split_factor"] = pd.NA

    for column in ("open", "high", "low", "close", "adj_close", "volume", "dividends", "stock_splits", "split_factor"):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["raw_open"] = pd.to_numeric(normalized["open"], errors="coerce")
    normalized["raw_high"] = pd.to_numeric(normalized["high"], errors="coerce")
    normalized["raw_low"] = pd.to_numeric(normalized["low"], errors="coerce")
    normalized["raw_close"] = pd.to_numeric(normalized["close"], errors="coerce")
    normalized["adj_close"] = pd.to_numeric(normalized["adj_close"], errors="coerce")
    normalized["dividends"] = pd.to_numeric(normalized["dividends"], errors="coerce").fillna(0.0)
    normalized["stock_splits"] = pd.to_numeric(normalized["stock_splits"], errors="coerce").fillna(0.0)

    policy = resolve_price_policy(price_policy)
    split_factor, split_source = _derive_split_factor(normalized)
    adjustment_factor = pd.Series(1.0, index=normalized.index, dtype=float)
    adjustment_source = pd.Series("raw", index=normalized.index, dtype=object)
    valid_adj = normalized["adj_close"].notna() & normalized["raw_close"].notna() & (normalized["raw_close"] != 0)
    if policy == PricePolicy.SPLIT_ADJUSTED:
        valid_split = split_factor.notna() & (split_factor > 0)
        if valid_split.any():
            adjustment_factor.loc[valid_split] = split_factor.loc[valid_split].astype(float)
            adjustment_source.loc[valid_split] = split_source.loc[valid_split]
    elif policy == PricePolicy.TOTAL_RETURN_ADJUSTED:
        if has_input_adj_close and valid_adj.any():
            candidate = (normalized["adj_close"] / normalized["raw_close"]).replace([float("inf"), float("-inf")], pd.NA)
            candidate = pd.to_numeric(candidate, errors="coerce")
            valid_factor = valid_adj & candidate.notna() & (candidate > 0)
            adjustment_factor.loc[valid_factor] = candidate.loc[valid_factor].astype(float)
            adjustment_source.loc[valid_factor] = "adj_close_proxy"
        valid_split_fallback = (adjustment_source == "raw") & split_factor.notna() & (split_factor > 0)
        if valid_split_fallback.any():
            adjustment_factor.loc[valid_split_fallback] = split_factor.loc[valid_split_fallback].astype(float)
            adjustment_source.loc[valid_split_fallback] = split_source.loc[valid_split_fallback]

    normalized["split_factor"] = split_factor
    normalized["price_adjustment_factor"] = adjustment_factor
    normalized["price_adjustment_source"] = adjustment_source
    normalized["price_policy"] = policy.value

    if policy == PricePolicy.RAW:
        normalized["open"] = normalized["raw_open"]
        normalized["high"] = normalized["raw_high"]
        normalized["low"] = normalized["raw_low"]
        normalized["close"] = normalized["raw_close"]
    else:
        normalized["open"] = normalized["raw_open"] * normalized["price_adjustment_factor"]
        normalized["high"] = normalized["raw_high"] * normalized["price_adjustment_factor"]
        normalized["low"] = normalized["raw_low"] * normalized["price_adjustment_factor"]
        normalized["close"] = normalized["raw_close"] * normalized["price_adjustment_factor"]

    normalized = normalized[list(AUGMENTED_OHLCV_COLUMNS)]
    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    return normalized


def _market_dir(market: str) -> str:
    return DATA_KR_DIR if require_market_key(market) == "kr" else DATA_US_DIR


def _filter_as_of(frame: pd.DataFrame, as_of: str | None) -> pd.DataFrame:
    if frame.empty or not as_of:
        return frame
    as_of_ts = pd.to_datetime(as_of, errors="coerce")
    if is_na_like(as_of_ts):
        return frame
    scoped = frame.copy()
    scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
    scoped = scoped.dropna(subset=["date"])
    scoped = scoped[scoped["date"] <= as_of_ts]
    if scoped.empty:
        return pd.DataFrame(columns=frame.columns)
    scoped["date"] = scoped["date"].dt.strftime("%Y-%m-%d")
    return scoped.reset_index(drop=True)


def _candidate_paths(market: str, symbol: str) -> list[str]:
    symbol_key = str(symbol or "").strip()
    safe_symbol = safe_filename(symbol_key)
    data_dir = _market_dir(market)
    candidates = [
        os.path.join(data_dir, f"{symbol_key}.csv"),
        os.path.join(data_dir, f"{safe_symbol}.csv"),
    ]
    return list(dict.fromkeys(candidates))


def _required_columns_key(required_columns: Iterable[str] | None) -> tuple[str, ...]:
    if required_columns is None:
        return ()
    return tuple(
        sorted(
            {
                str(column or "").strip().lower()
                for column in required_columns
                if str(column or "").strip()
            }
        )
    )


def _read_csv_usecols(required_columns: Iterable[str] | None):
    required = set(_required_columns_key(required_columns))
    if not required:
        return None
    allowed = {"symbol"}
    for canonical, aliases in OHLCV_COLUMN_ALIASES.items():
        if canonical in required:
            allowed.add(canonical.lower())
            allowed.update(str(alias).strip().lower() for alias in aliases)
    return lambda column: str(column or "").strip().lower() in allowed


def _ohlcv_parquet_cache_paths(market: str, symbol: str) -> tuple[str, str]:
    cache_dir = os.path.join(_market_dir(market), "cache", "ohlcv_parquet")
    safe_symbol = safe_filename(str(symbol or "").strip().upper())
    return (
        os.path.join(cache_dir, f"{safe_symbol}.parquet"),
        os.path.join(cache_dir, f"{safe_symbol}.json"),
    )


def _ohlcv_parquet_identity(
    *,
    market: str,
    symbol: str,
    source_path: str,
    price_policy: PricePolicy | str,
    required_columns: Iterable[str] | None,
) -> dict[str, Any]:
    return {
        "schema_version": OHLCV_PARQUET_CACHE_VERSION,
        "market": require_market_key(market),
        "symbol": str(symbol or "").strip().upper(),
        "price_policy": _price_policy_key(price_policy),
        "required_columns_hash": stable_payload_hash(_required_columns_key(required_columns)),
        "source": source_file_signature(source_path),
    }


def _read_ohlcv_parquet_cache(
    *,
    market: str,
    symbol: str,
    source_path: str,
    price_policy: PricePolicy | str,
    required_columns: Iterable[str] | None,
    runtime_context: Any | None,
) -> pd.DataFrame | None:
    if (
        not env_flag_enabled("INVEST_PROTO_OHLCV_PARQUET_CACHE", default=True)
        or _OHLCV_PARQUET_DISABLED_REASON
    ):
        return None
    parquet_path, metadata_path = _ohlcv_parquet_cache_paths(market, symbol)
    identity = _ohlcv_parquet_identity(
        market=market,
        symbol=symbol,
        source_path=source_path,
        price_policy=price_policy,
        required_columns=required_columns,
    )
    try:
        if not os.path.exists(parquet_path) or not os.path.exists(metadata_path):
            return None
        with open(metadata_path, "r", encoding="utf-8") as handle:
            metadata = json.load(handle)
        if metadata.get("identity") != identity:
            return None
        started = time.perf_counter()
        frame = pd.read_parquet(parquet_path)
        if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
            runtime_context.add_runtime_metric("frame_load", "parquet_hits", 1)
            runtime_context.add_runtime_metric("frame_load", "parquet_seconds", time.perf_counter() - started)
        return frame
    except Exception as exc:
        _handle_ohlcv_parquet_failure("read", symbol, exc)
        if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
            runtime_context.add_runtime_metric("frame_load", "parquet_read_failures", 1)
        return None


def _write_ohlcv_parquet_cache(
    frame: pd.DataFrame,
    *,
    market: str,
    symbol: str,
    source_path: str,
    price_policy: PricePolicy | str,
    required_columns: Iterable[str] | None,
    runtime_context: Any | None,
) -> None:
    if (
        frame.empty
        or not env_flag_enabled("INVEST_PROTO_OHLCV_PARQUET_CACHE", default=True)
        or _OHLCV_PARQUET_DISABLED_REASON
    ):
        return
    parquet_path, metadata_path = _ohlcv_parquet_cache_paths(market, symbol)
    identity = _ohlcv_parquet_identity(
        market=market,
        symbol=symbol,
        source_path=source_path,
        price_policy=price_policy,
        required_columns=required_columns,
    )
    try:
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        started = time.perf_counter()
        unique_suffix = f".{os.getpid()}.{time.time_ns()}.tmp"
        tmp_parquet_path = f"{parquet_path}{unique_suffix}"
        tmp_metadata_path = f"{metadata_path}{unique_suffix}"
        frame.to_parquet(tmp_parquet_path, index=False)
        with open(tmp_metadata_path, "w", encoding="utf-8") as handle:
            json.dump({"identity": identity}, handle, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp_parquet_path, parquet_path)
        os.replace(tmp_metadata_path, metadata_path)
        if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
            runtime_context.add_runtime_metric("frame_load", "parquet_writes", 1)
            runtime_context.add_runtime_metric("frame_load", "parquet_seconds", time.perf_counter() - started)
    except Exception as exc:
        _handle_ohlcv_parquet_failure("write", symbol, exc)
        if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
            runtime_context.add_runtime_metric("frame_load", "parquet_write_failures", 1)


def _handle_ohlcv_parquet_failure(operation: str, symbol: str, exc: Exception) -> None:
    global _OHLCV_PARQUET_DISABLED_REASON
    message = str(exc)
    missing_engine = (
        isinstance(exc, ImportError)
        or "Unable to find a usable engine" in message
        or "Missing optional dependency" in message
        or "pyarrow" in message and "fastparquet" in message
    )
    if missing_engine:
        if not _OHLCV_PARQUET_DISABLED_REASON:
            _OHLCV_PARQUET_DISABLED_REASON = message
            print(f"[Cache] OHLCV parquet cache disabled - {message}")
        return
    print(f"[Cache] OHLCV parquet cache {operation} skipped ({symbol}) - {exc}")


def load_local_ohlcv_frame(
    market: str,
    symbol: str,
    as_of: str | None = None,
    *,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
    required_columns: Iterable[str] | None = None,
    runtime_context: Any | None = None,
) -> pd.DataFrame:
    symbol_key = str(symbol or "").strip().upper()
    for path in _candidate_paths(market, symbol_key):
        if not os.path.exists(path):
            continue
        cached = _read_ohlcv_parquet_cache(
            market=market,
            symbol=symbol_key,
            source_path=path,
            price_policy=price_policy,
            required_columns=required_columns,
            runtime_context=runtime_context,
        )
        if cached is not None and not cached.empty:
            if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
                runtime_context.add_runtime_metric("frame_load", "files", 1)
                runtime_context.add_runtime_metric("frame_load", "rows", len(cached))
            return _filter_as_of(cached, as_of)
        try:
            read_kwargs = {}
            usecols = _read_csv_usecols(required_columns)
            if usecols is not None:
                read_kwargs["usecols"] = usecols
            read_started = time.perf_counter()
            frame = pd.read_csv(path, **read_kwargs)
            if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
                runtime_context.add_runtime_metric("frame_load", "csv_reads", 1)
                runtime_context.add_runtime_metric("frame_load", "csv_seconds", time.perf_counter() - read_started)
        except Exception:
            continue
        normalized = normalize_ohlcv_frame(frame, symbol=symbol_key, price_policy=price_policy)
        if normalized.empty:
            continue
        _write_ohlcv_parquet_cache(
            normalized,
            market=market,
            symbol=symbol_key,
            source_path=path,
            price_policy=price_policy,
            required_columns=required_columns,
            runtime_context=runtime_context,
        )
        if runtime_context is not None and hasattr(runtime_context, "add_runtime_metric"):
            runtime_context.add_runtime_metric("frame_load", "parquet_misses", 1)
            runtime_context.add_runtime_metric("frame_load", "files", 1)
            runtime_context.add_runtime_metric("frame_load", "rows", len(normalized))
        return _filter_as_of(normalized, as_of)
    return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)


def _price_policy_key(price_policy: PricePolicy | str) -> str:
    try:
        return PricePolicy(price_policy).value
    except ValueError:
        return str(price_policy or PricePolicy.SPLIT_ADJUSTED.value)


def _ohlcv_cache_key(
    market: str,
    symbol: str,
    as_of: str | None,
    price_policy: PricePolicy | str,
    required_columns: Iterable[str] | None = None,
) -> tuple[str, str, str, str, str]:
    return (
        str(market or "").strip().lower(),
        str(as_of or "").strip(),
        _price_policy_key(price_policy),
        stable_payload_hash(_required_columns_key(required_columns)),
        str(symbol or "").strip().upper(),
    )


def _runtime_worker_count(
    total_items: int,
    *,
    env_var: str,
    cap: int = DEFAULT_LOCAL_FRAME_WORKER_CAP,
    explicit: int | None = None,
    runtime_context: Any | None = None,
    scope: str = "",
) -> int:
    total = max(int(total_items), 0)
    if total <= 1:
        workers = 1
        if runtime_context is not None and hasattr(runtime_context, "record_worker_budget"):
            runtime_context.record_worker_budget(
                scope,
                total_items=total,
                workers=workers,
                env_var=env_var,
                cap=cap,
                stage_parallel=_stage_parallel_enabled(),
            )
        return workers
    if explicit is not None:
        workers = max(1, min(int(explicit), total))
        if runtime_context is not None and hasattr(runtime_context, "record_worker_budget"):
            runtime_context.record_worker_budget(
                scope,
                total_items=total,
                workers=workers,
                env_var=env_var,
                configured=str(explicit),
                cap=cap,
                stage_parallel=_stage_parallel_enabled(),
            )
        return workers
    raw_value = str(os.environ.get(env_var) or "").strip()
    if raw_value:
        try:
            workers = max(1, min(int(raw_value), total))
            if runtime_context is not None and hasattr(runtime_context, "record_worker_budget"):
                runtime_context.record_worker_budget(
                    scope,
                    total_items=total,
                    workers=workers,
                    env_var=env_var,
                    configured=raw_value,
                    cap=cap,
                    stage_parallel=_stage_parallel_enabled(),
                )
            return workers
        except ValueError:
            pass
    cpu_count = os.cpu_count() or 1
    effective_cap = _stage_parallel_internal_cap(cap, cpu_count=cpu_count, scope=scope)
    workers = max(1, min(int(effective_cap), int(cpu_count), total))
    if runtime_context is not None and hasattr(runtime_context, "record_worker_budget"):
        runtime_context.record_worker_budget(
            scope,
            total_items=total,
            workers=workers,
            env_var=env_var,
            cap=effective_cap,
            stage_parallel=_stage_parallel_enabled(),
        )
    return workers


def _stage_parallel_enabled() -> bool:
    return env_flag_enabled("INVEST_PROTO_SCREENING_STAGE_PARALLEL", default=True)


def _stage_parallel_internal_cap(cap: int, *, cpu_count: int, scope: str = "") -> int:
    if not _stage_parallel_enabled():
        return int(cap)
    raw_stage_workers = str(os.getenv("INVEST_PROTO_SCREENING_STAGE_WORKERS") or "").strip()
    try:
        stage_workers = int(raw_stage_workers) if raw_stage_workers else min(4, int(cpu_count), 4)
    except ValueError:
        stage_workers = min(4, int(cpu_count), 4)
    stage_workers = max(1, stage_workers)
    per_stage_budget = max(1, int(cpu_count) // stage_workers)
    if (
        env_flag_enabled("INVEST_PROTO_SCREENING_SHARED_OHLCV_CACHE", default=True)
        and str(scope or "").strip()
        in {"leader_lagging.feature_analysis", "leader_lagging.follower_analysis"}
    ):
        return max(1, min(int(cap), int(cpu_count)))
    return max(1, min(int(cap), per_stage_budget))


def load_local_ohlcv_frame_cached(
    market: str,
    symbol: str,
    as_of: str | None = None,
    *,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
    runtime_context: Any | None = None,
    required_columns: Iterable[str] | None = None,
    load_frame_fn: Callable[..., pd.DataFrame] = load_local_ohlcv_frame,
) -> pd.DataFrame:
    cache_key = _ohlcv_cache_key(market, symbol, as_of, price_policy, required_columns)
    if runtime_context is not None and hasattr(runtime_context, "get_ohlcv_frame_cache"):
        cached = runtime_context.get_ohlcv_frame_cache(cache_key)
        if cached is not None:
            return cached
    try:
        frame = load_frame_fn(
            market,
            symbol,
            as_of=as_of,
            price_policy=price_policy,
            required_columns=required_columns,
            runtime_context=runtime_context,
        )
    except TypeError as exc:
        if "required_columns" not in str(exc) and "runtime_context" not in str(exc):
            raise
        frame = load_frame_fn(
            market,
            symbol,
            as_of=as_of,
            price_policy=price_policy,
        )
    if runtime_context is not None and hasattr(runtime_context, "set_ohlcv_frame_cache"):
        runtime_context.set_ohlcv_frame_cache(cache_key, frame)
    return frame.copy()


def load_local_ohlcv_frames_ordered(
    market: str,
    symbols: Iterable[str],
    as_of: str | None = None,
    *,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
    runtime_context: Any | None = None,
    max_workers: int | None = None,
    required_columns: Iterable[str] | None = None,
    worker_scope: str = "ohlcv_frame_load",
    load_frame_fn: Callable[..., pd.DataFrame] = load_local_ohlcv_frame,
) -> dict[str, pd.DataFrame]:
    ordered_symbols = list(
        dict.fromkeys(
            str(symbol or "").strip().upper()
            for symbol in symbols
            if str(symbol or "").strip()
        )
    )
    worker_count = _runtime_worker_count(
        len(ordered_symbols),
        env_var="INVEST_PROTO_LOCAL_FRAME_WORKERS",
        explicit=max_workers,
        runtime_context=runtime_context,
        scope=worker_scope,
    )

    def _load(symbol: str) -> pd.DataFrame:
        return load_local_ohlcv_frame_cached(
            market,
            symbol,
            as_of=as_of,
            price_policy=price_policy,
            runtime_context=runtime_context,
            required_columns=required_columns,
            load_frame_fn=load_frame_fn,
        )

    if worker_count == 1:
        return {symbol: _load(symbol) for symbol in ordered_symbols}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {symbol: executor.submit(_load, symbol) for symbol in ordered_symbols}
        return {symbol: future_map[symbol].result() for symbol in ordered_symbols}


def _download_yfinance_ohlcv(
    symbol: str,
    as_of: str | None = None,
    *,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> pd.DataFrame:
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    symbol_key = str(symbol or "").strip().upper()
    if not symbol_key:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    try:
        ticker = yf.Ticker(symbol_key)
        history = ticker.history(period="3y", interval="1d", auto_adjust=False, actions=True)
    except Exception:
        return pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)

    normalized = normalize_ohlcv_frame(history, symbol=symbol_key, price_policy=price_policy)
    if normalized.empty:
        return normalized
    return _filter_as_of(normalized, as_of)


def load_market_ohlcv_frames(
    market: str,
    symbols: Iterable[str],
    as_of: str | None = None,
    *,
    allow_yfinance_fallback: bool = True,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> dict[str, pd.DataFrame]:
    # Compatibility shim retained for external callers. Runtime code loads
    # per-symbol frames explicitly to keep fallback behavior local.
    frames: dict[str, pd.DataFrame] = {}
    for raw_symbol in symbols:
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol:
            continue
        frame = load_local_ohlcv_frame(market, symbol, as_of=as_of, price_policy=price_policy)
        if frame.empty and allow_yfinance_fallback:
            frame = _download_yfinance_ohlcv(symbol, as_of=as_of, price_policy=price_policy)
        if not frame.empty:
            frames[symbol] = frame
    return frames


def load_benchmark_data(
    market: str,
    candidates: list[str],
    as_of: str | None = None,
    *,
    allow_yfinance_fallback: bool = True,
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
) -> tuple[Optional[str], pd.DataFrame]:
    for candidate in candidates:
        symbol = str(candidate or "").strip().upper()
        if not symbol:
            continue
        frame = load_local_ohlcv_frame(market, symbol, as_of=as_of, price_policy=price_policy)
        if frame.empty and allow_yfinance_fallback:
            frame = _download_yfinance_ohlcv(symbol, as_of=as_of, price_policy=price_policy)
        if not frame.empty:
            return symbol, frame
    return None, pd.DataFrame(columns=AUGMENTED_OHLCV_COLUMNS)
