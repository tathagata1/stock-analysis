"""High-level data preparation for price-intent backtests."""

from .data import (
    current_index_membership,
    load_price_history_cached,
    resolve_benchmark,
)


def load_backtest_market_data(
    ticker,
    index_name,
    start_date,
    end_date=None,
    cache_dir="cache",
    cache_max_age_hours=24,
    refresh_price_cache=False,
    check_index_membership=True,
):
    """Load ticker, benchmark, cache metadata, and optional membership details."""
    benchmark_ticker = resolve_benchmark(index_name)
    price_history, ticker_cache_meta = load_price_history_cached(
        ticker,
        start_date,
        end_date,
        cache_dir,
        cache_max_age_hours,
        refresh_price_cache,
    )
    benchmark_history, benchmark_cache_meta = load_price_history_cached(
        benchmark_ticker,
        start_date,
        end_date,
        cache_dir,
        cache_max_age_hours,
        refresh_price_cache,
    )
    membership = (
        current_index_membership(
            ticker,
            index_name,
            cache_dir,
            cache_max_age_hours,
        )
        if check_index_membership
        else {"note": "Membership check disabled."}
    )

    load_message = (
        f"Loaded {len(price_history):,} {ticker} sessions and "
        f"{len(benchmark_history):,} {benchmark_ticker} sessions from "
        f"{price_history['Date'].min():%Y-%m-%d} to "
        f"{price_history['Date'].max():%Y-%m-%d}."
    )
    return {
        "benchmark_ticker": benchmark_ticker,
        "price_history": price_history,
        "benchmark_history": benchmark_history,
        "ticker_cache_meta": ticker_cache_meta,
        "benchmark_cache_meta": benchmark_cache_meta,
        "membership": membership,
        "load_message": load_message,
    }


def load_multiple_backtest_market_data(
    tickers,
    index_name,
    start_date,
    end_date=None,
    cache_dir="cache",
    cache_max_age_hours=24,
    refresh_price_cache=False,
    check_index_membership=True,
):
    """Load multiple ticker histories while fetching the shared benchmark once."""
    ticker_values = [tickers] if isinstance(tickers, str) else tickers
    normalized_tickers = list(
        dict.fromkeys(
            str(ticker).strip().upper()
            for ticker in ticker_values
            if ticker is not None and str(ticker).strip()
        )
    )
    if not normalized_tickers:
        raise ValueError("tickers must contain at least one valid symbol")

    benchmark_ticker = resolve_benchmark(index_name)
    benchmark_history, benchmark_cache_meta = load_price_history_cached(
        benchmark_ticker,
        start_date,
        end_date,
        cache_dir,
        cache_max_age_hours,
        refresh_price_cache,
    )

    market_data_by_ticker = {}
    for ticker in normalized_tickers:
        if ticker == benchmark_ticker.upper():
            price_history = benchmark_history.copy()
            ticker_cache_meta = dict(benchmark_cache_meta)
        else:
            price_history, ticker_cache_meta = load_price_history_cached(
                ticker,
                start_date,
                end_date,
                cache_dir,
                cache_max_age_hours,
                refresh_price_cache,
            )
        membership = (
            current_index_membership(
                ticker,
                index_name,
                cache_dir,
                cache_max_age_hours,
            )
            if check_index_membership
            else {"ticker": ticker, "note": "Membership check disabled."}
        )
        load_message = (
            f"Loaded {len(price_history):,} {ticker} sessions and "
            f"{len(benchmark_history):,} {benchmark_ticker} sessions from "
            f"{price_history['Date'].min():%Y-%m-%d} to "
            f"{price_history['Date'].max():%Y-%m-%d}."
        )
        market_data_by_ticker[ticker] = {
            "benchmark_ticker": benchmark_ticker,
            "price_history": price_history,
            "benchmark_history": benchmark_history,
            "ticker_cache_meta": ticker_cache_meta,
            "benchmark_cache_meta": benchmark_cache_meta,
            "membership": membership,
            "load_message": load_message,
        }

    return market_data_by_ticker
