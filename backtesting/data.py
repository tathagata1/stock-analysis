"""Market-data loading, caching, and benchmark helpers for backtests."""

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import warnings
import uuid

import numpy as np
import pandas as pd
import yfinance as yf

import dao.dao as dao


INDEX_BENCHMARKS = {
    "dow30": "^DJI",
    "nasdaq100": "^NDX",
    "sp500": "^GSPC",
    "ftse100": "^FTSE",
    "ftse250": "^FTMC",
}
PRICE_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]


def clean_symbol_for_filename(symbol):
    """Return a filesystem-safe representation of a market symbol."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", symbol).strip("_") or "symbol"


def _atomic_write_text(path, content):
    path = Path(path)
    temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary_path.write_text(content, encoding="utf-8")
    temporary_path.replace(path)


def _atomic_write_csv(frame, path):
    path = Path(path)
    temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    frame.to_csv(temporary_path, index=False)
    temporary_path.replace(path)


def normalize_history(history):
    """Normalize a price-history frame to the columns used by the backtester."""
    if history is None or history.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    frame = history.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    if "Date" not in frame.columns:
        frame = frame.reset_index()
        frame = frame.rename(columns={frame.columns[0]: "Date"})
    if "Adj Close" not in frame.columns and "Close" in frame.columns:
        frame["Adj Close"] = frame["Close"]

    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    if frame["Date"].dt.tz is not None:
        frame["Date"] = frame["Date"].dt.tz_localize(None)
    for column in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    return (
        frame[PRICE_COLUMNS]
        .dropna(subset=["Date", "Open", "High", "Low", "Close"])
        .drop_duplicates(subset="Date", keep="last")
        .sort_values("Date")
        .reset_index(drop=True)
    )


def load_price_history_cached(
    symbol,
    start,
    end=None,
    cache_root="cache",
    max_age_hours=24,
    force_refresh=False,
):
    """Load daily prices from a fresh cache or Yahoo Finance."""
    if float(max_age_hours) < 0:
        raise ValueError("max_age_hours cannot be negative")
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize() if end is not None else pd.Timestamp.today().normalize()
    if end_ts < start_ts:
        raise ValueError("end_date must be on or after start_date")

    price_cache_dir = Path(cache_root) / "price_history"
    price_cache_dir.mkdir(parents=True, exist_ok=True)
    end_label = "latest" if end is None else f"{end_ts:%Y%m%d}"
    cache_stem = f"{clean_symbol_for_filename(symbol)}_{start_ts:%Y%m%d}_{end_label}_1d"
    cache_file = price_cache_dir / f"{cache_stem}.csv"
    metadata_file = price_cache_dir / f"{cache_stem}.json"

    cache_exists = cache_file.exists()
    cache_age_hours = (
        max(0.0, (datetime.now().timestamp() - cache_file.stat().st_mtime) / 3600)
        if cache_exists
        else None
    )
    cache_is_fresh = cache_exists and cache_age_hours < max_age_hours
    cache_used = cache_is_fresh and not force_refresh
    stale_fallback = False

    history = None
    if cache_used:
        try:
            history = normalize_history(pd.read_csv(cache_file, parse_dates=["Date"]))
            if history.empty:
                raise ValueError("Cached price history is empty")
        except Exception as exc:
            warnings.warn(f"Ignoring unreadable cache for {symbol}. Error: {exc}")
            cache_used = False

    if not cache_used:
        try:
            # yfinance treats end as exclusive, so add one day to make the input inclusive.
            history = yf.Ticker(symbol).history(
                start=start_ts.strftime("%Y-%m-%d"),
                end=(end_ts + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                interval="1d",
                auto_adjust=False,
                actions=False,
            )
            history = normalize_history(history)
            if history.empty:
                raise ValueError(f"Yahoo Finance returned no daily data for {symbol}")
            _atomic_write_csv(history, cache_file)
            _atomic_write_text(
                metadata_file,
                json.dumps(
                    {
                        "schema_version": 1,
                        "symbol": symbol,
                        "start_date": start_ts.strftime("%Y-%m-%d"),
                        "end_date": end_ts.strftime("%Y-%m-%d"),
                        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "rows": len(history),
                    },
                    indent=2,
                ),
            )
            cache_age_hours = 0.0
        except Exception as exc:
            if not cache_exists:
                raise
            warnings.warn(f"Refresh failed for {symbol}; using stale cache. Error: {exc}")
            history = normalize_history(pd.read_csv(cache_file, parse_dates=["Date"]))
            if history.empty:
                raise RuntimeError(f"Stale price cache for {symbol} is empty") from exc
            cache_used = True
            stale_fallback = True

    metadata = {
        "symbol": symbol,
        "rows": len(history),
        "cache_used": cache_used,
        "stale_fallback": stale_fallback,
        "cache_age_hours": None if cache_age_hours is None else round(cache_age_hours, 3),
        "cache_file": str(cache_file),
    }
    return history, metadata


def resolve_benchmark(index_value):
    """Map a supported index name to its Yahoo Finance benchmark symbol."""
    normalized = str(index_value).strip().lower()
    return INDEX_BENCHMARKS.get(normalized, str(index_value).strip())


def current_index_membership(symbol, index_value, cache_root, max_age_hours):
    """Check current membership for a supported named index."""
    normalized = str(index_value).strip().lower()
    if normalized not in INDEX_BENCHMARKS:
        return {
            "index_name": index_value,
            "ticker": symbol,
            "is_current_constituent": None,
            "note": "Membership check skipped for a custom benchmark symbol.",
        }

    constituents, cache_meta = dao.get_index_tickers_cached(
        index_name=normalized,
        limit=None,
        cache_dir=cache_root,
        max_age_hours=max_age_hours,
    )
    normalized_ticker = symbol.strip().upper().replace(".", "-")
    normalized_constituents = {item.strip().upper().replace(".", "-") for item in constituents}
    return {
        "index_name": normalized,
        "ticker": symbol,
        "is_current_constituent": normalized_ticker in normalized_constituents,
        "constituents_loaded": len(constituents),
        "cache_used": cache_meta.get("cache_used"),
        "cache_file": cache_meta.get("cache_file"),
        "note": "Current membership only; this is not a historical constituent test.",
    }
