import json
from datetime import datetime
from pathlib import Path

import pandas as pd

import backtesting
import dao
import prediction
import simulation


DEFAULT_SIGNAL_LOOKBACK_START = 1
DEFAULT_SIGNAL_LOOKBACK_END = 30
DEFAULT_CACHE_DIR = "cache"
DEFAULT_INDEX_CACHE_MAX_AGE_HOURS = 24


def _safe_float(value):
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _build_metrics_dict(cumulative_return, sharpe_ratio, sortino_ratio, max_drawdown, calmar_ratio):
    return {
        "cumulative_return": cumulative_return,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "max_drawdown": max_drawdown,
        "calmar_ratio": calmar_ratio,
    }


def current_run_date():
    return datetime.now().strftime("%Y%m%d")


def load_portfolio(portfolio_path):
    return dao.read_json(portfolio_path)


def _index_ticker_cache_path(index_name, cache_dir=DEFAULT_CACHE_DIR):
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    return cache_path / f"{index_name.lower()}_tickers.json"


def _read_cached_tickers(cache_file):
    payload = json.loads(cache_file.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        tickers = payload.get("tickers", [])
    elif isinstance(payload, list):
        tickers = payload
    else:
        tickers = []
    return [str(t).strip() for t in tickers if str(t).strip()]


def get_index_tickers_cached(index_name="sp500", limit=None, cache_dir=DEFAULT_CACHE_DIR, max_age_hours=DEFAULT_INDEX_CACHE_MAX_AGE_HOURS):
    cache_file = _index_ticker_cache_path(index_name, cache_dir=cache_dir)
    now = datetime.now().timestamp()
    max_age_seconds = max_age_hours * 3600
    cache_used = False
    cache_deleted = False
    tickers = []

    if cache_file.exists():
        cache_age_seconds = now - cache_file.stat().st_mtime
        if cache_age_seconds < max_age_seconds:
            try:
                tickers = _read_cached_tickers(cache_file)
                cache_used = True
            except Exception:
                cache_file.unlink(missing_ok=True)
                cache_deleted = True
        else:
            cache_file.unlink(missing_ok=True)
            cache_deleted = True

    if not cache_used:
        tickers = dao.get_index_constituents(index_name)
        payload = {
            "index_name": index_name,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "tickers": tickers,
        }
        cache_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if limit is not None:
        tickers = tickers[:limit]

    cache_age_seconds = None
    if cache_file.exists():
        cache_age_seconds = max(0, datetime.now().timestamp() - cache_file.stat().st_mtime)

    cache_meta = {
        "cache_file": str(cache_file),
        "cache_used": cache_used,
        "cache_deleted": cache_deleted,
        "max_age_hours": max_age_hours,
        "cache_age_hours": None if cache_age_seconds is None else round(cache_age_seconds / 3600, 3),
    }
    return tickers, cache_meta


def get_index_tickers(index_name="sp500", limit=None):
    tickers = dao.get_index_constituents(index_name)
    if limit is None:
        return tickers
    return tickers[:limit]


def build_prediction_frame(ticker, include_sentiment=False, include_fundamentals=True):
    df_5y = pd.DataFrame(dao.get_yahoo_finance_5y(ticker))
    if df_5y.empty:
        raise ValueError(f"No history returned for {ticker}")

    stats_row = None
    if include_fundamentals:
        df_stats = pd.DataFrame(dao.get_yahoo_finance_key_stats(ticker))
        stats_row = df_stats.iloc[0]
        df_pred = prediction.get_prediction(
            df_5y,
            stats_row,
            include_sentiment=include_sentiment,
            include_fundamental=True,
        )
    else:
        df_pred = prediction.get_statsless_prediction(df_5y, include_sentiment=include_sentiment)

    df_pred = prediction.add_total_signal(df_pred)
    df_pred = prediction.convert_signal_to_text(df_pred)
    return df_pred, stats_row


def backtest_prediction_frame(df_pred):
    df_tested, cumulative_return, sharpe_ratio, sortino_ratio, max_drawdown, calmar_ratio = backtesting.backtest(df_pred.copy())
    metrics = _build_metrics_dict(cumulative_return, sharpe_ratio, sortino_ratio, max_drawdown, calmar_ratio)
    return df_tested, metrics


def get_recent_weighted_signal(df_tested, start_iloc=DEFAULT_SIGNAL_LOOKBACK_START, end_iloc=DEFAULT_SIGNAL_LOOKBACK_END):
    signals = df_tested["Signal_Text"].iloc[start_iloc:end_iloc + 1].reset_index(drop=True)
    signal_text, signal_number = prediction.get_weighted_signal(signals)
    return {
        "signal_text": signal_text,
        "signal_number": signal_number,
        "signals_considered": len(signals),
    }


def build_stock_analysis(ticker, include_sentiment=False, include_fundamentals=True):
    df_pred, stats_row = build_prediction_frame(
        ticker,
        include_sentiment=include_sentiment,
        include_fundamentals=include_fundamentals,
    )
    df_tested, metrics = backtest_prediction_frame(df_pred)
    recent_signal = get_recent_weighted_signal(df_tested)

    return {
        "ticker": ticker,
        "df_pred": df_pred,
        "df_tested": df_tested,
        "stats": stats_row,
        "metrics": metrics,
        "recent_signal": recent_signal,
    }


def build_prediction_summary_row(stock_analysis, run_date=None):
    run_date = run_date or current_run_date()
    return {
        "current_date": run_date,
        "TICKER": stock_analysis["ticker"],
        "Signal": stock_analysis["recent_signal"]["signal_number"],
        "Signal_Text": stock_analysis["recent_signal"]["signal_text"],
        "cumulative_return": stock_analysis["metrics"]["cumulative_return"],
        "sharpe_ratio": stock_analysis["metrics"]["sharpe_ratio"],
        "sortino_ratio": stock_analysis["metrics"]["sortino_ratio"],
        "max_drawdown": stock_analysis["metrics"]["max_drawdown"],
        "calmar_ratio": stock_analysis["metrics"]["calmar_ratio"],
    }


def get_price_history(df_pred):
    history = df_pred[['Date', 'Open', 'Close']].copy()
    history['Date'] = pd.to_datetime(history['Date']).dt.tz_localize(None)
    history['Open'] = pd.to_numeric(history['Open'], errors='coerce')
    history['Close'] = pd.to_numeric(history['Close'], errors='coerce')
    history = history.dropna(subset=['Date', 'Open', 'Close']).sort_values('Date').reset_index(drop=True)
    return history


def resolve_buy_date(price_history, buy_date):
    if price_history.empty:
        raise ValueError("Price history is empty")

    requested_date = pd.to_datetime(buy_date).normalize()
    normalized_dates = price_history['Date'].dt.normalize()

    exact_match = price_history.loc[normalized_dates == requested_date]
    if not exact_match.empty:
        matched_row = exact_match.iloc[0]
        resolution = "exact"
    else:
        next_trading_day = price_history.loc[normalized_dates > requested_date]
        if not next_trading_day.empty:
            matched_row = next_trading_day.iloc[0]
            resolution = "next_trading_day"
        else:
            previous_trading_day = price_history.loc[normalized_dates < requested_date]
            if previous_trading_day.empty:
                raise ValueError(f"Buy date {buy_date} is earlier than the available price history")
            matched_row = previous_trading_day.iloc[-1]
            resolution = "previous_trading_day"

    return {
        "requested_buy_date": requested_date,
        "resolved_buy_date": matched_row['Date'],
        "reference_open_on_resolved_date": _safe_float(matched_row['Open']),
        "reference_close_on_resolved_date": _safe_float(matched_row['Close']),
        "buy_date_resolution": resolution,
    }


def build_manual_position_analysis(df_pred, buy_date, units=1.0):
    history = get_price_history(df_pred)
    buy_lookup = resolve_buy_date(history, buy_date)

    open_price = buy_lookup["reference_open_on_resolved_date"]
    close_price = buy_lookup["reference_close_on_resolved_date"]
    if open_price is None or close_price is None:
        raise ValueError("Open and Close prices must be available on the resolved buy date")

    buy_price_value = (open_price + close_price) / 2

    units_value = _safe_float(units)
    if units_value is None or units_value < 0:
        raise ValueError("units must be zero or a positive number")

    latest_row = history.iloc[-1]
    latest_close = _safe_float(latest_row['Close'])
    cost_basis = buy_price_value * units_value
    market_value = None if latest_close is None else latest_close * units_value
    pnl = None if market_value is None else market_value - cost_basis
    pnl_pct = None
    if cost_basis:
        pnl_pct = (pnl / cost_basis) * 100 if pnl is not None else None

    summary = pd.DataFrame([{
        "requested_buy_date": buy_lookup["requested_buy_date"],
        "resolved_buy_date": buy_lookup["resolved_buy_date"],
        "buy_date_resolution": buy_lookup["buy_date_resolution"],
        "reference_open_on_resolved_date": open_price,
        "buy_price": buy_price_value,
        "reference_close_on_resolved_date": buy_lookup["reference_close_on_resolved_date"],
        "units": units_value,
        "latest_close": latest_close,
        "latest_date": latest_row['Date'],
        "cost_basis": cost_basis,
        "market_value": market_value,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
    }])

    buy_marker = pd.DataFrame([{
        "Date": buy_lookup["resolved_buy_date"],
        "Close": buy_price_value,
        "requested_buy_date": buy_lookup["requested_buy_date"],
        "buy_date_resolution": buy_lookup["buy_date_resolution"],
        "buy_price_method": "average_of_open_and_close",
    }])

    return {
        "price_history": history,
        "buy_lookup": buy_lookup,
        "buy_marker": buy_marker,
        "summary": summary,
    }


def exploratory_simulation_from_prediction(df_pred, initial_funds=1000, start_iloc=1, end_iloc=31):
    sim_input = df_pred[['High', 'Low', 'Close', 'Date', 'TICKER', 'Signal_Text']].copy()
    return simulation.simulate_exploratory_trading(sim_input, start_iloc, end_iloc, initial_funds)


def portfolio_simulation_from_prediction(df_pred, holding, initial_funds=1000, start_iloc=1):
    sim_input = df_pred[['High', 'Low', 'Close', 'Date', 'TICKER', 'Signal_Text']].copy()
    date_series = sim_input['Date'].astype(str).str[:10]
    matches = sim_input.index[date_series == str(holding['date'])].tolist()
    if not matches:
        raise ValueError(f"Holding date {holding['date']} not found for {holding['ticker']}")
    end_iloc = matches[0]
    return simulation.simulate_portfolio_trades(
        sim_input,
        start_iloc,
        end_iloc,
        initial_funds,
        holding['units'],
        holding['average_buying_price'],
    )


def simulation_summary_row(ticker, simulation_output, run_date=None):
    run_date = run_date or current_run_date()
    positions = [txn.get('position') for txn in simulation_output.get('transactions', []) if txn.get('position')]
    signal_text, signal_number = prediction.get_weighted_signal(pd.Series(positions)) if positions else ("HOLD", 0)
    return {
        "current_date": run_date,
        "TICKER": ticker,
        "closing_stock_price": simulation_output.get('closing_stock_price'),
        "final_cash_balance": simulation_output.get('final_cash_balance'),
        "unrealized_gains_losses": simulation_output.get('unrealized_gains_losses'),
        "unrealized_gain_loss_%": simulation_output.get('unrealized_gain_loss_%'),
        "units_held": simulation_output.get('units_held'),
        "average_price_per_unit": simulation_output.get('average_price_per_unit'),
        "signal_text": signal_text,
        "signal_number": signal_number,
    }


def run_holdings_workflow(portfolio_path, initial_funds=1000, include_sentiment=False):
    holdings = load_portfolio(portfolio_path)
    analyses = {}
    prediction_rows = []
    simulation_rows = []

    for holding in holdings:
        ticker = holding['ticker']
        analysis = build_stock_analysis(ticker, include_sentiment=include_sentiment, include_fundamentals=True)
        analyses[ticker] = analysis
        prediction_rows.append(build_prediction_summary_row(analysis))

        sim_output = portfolio_simulation_from_prediction(analysis['df_pred'], holding, initial_funds=initial_funds)
        analyses[ticker]['portfolio_simulation'] = sim_output
        simulation_rows.append(simulation_summary_row(ticker, sim_output))

    return {
        "holdings": holdings,
        "analyses": analyses,
        "prediction_summary": pd.DataFrame(prediction_rows),
        "portfolio_simulation_summary": pd.DataFrame(simulation_rows),
    }


def run_specific_stock_simulation_workflow(tickers, initial_funds=1000, include_sentiment=False, include_fundamentals=True):
    analyses = {}
    prediction_rows = []
    simulation_rows = []

    for ticker in tickers:
        analysis = build_stock_analysis(
            ticker,
            include_sentiment=include_sentiment,
            include_fundamentals=include_fundamentals,
        )
        analyses[ticker] = analysis
        prediction_rows.append(build_prediction_summary_row(analysis))

        sim_output = exploratory_simulation_from_prediction(analysis['df_pred'], initial_funds=initial_funds)
        analyses[ticker]['exploratory_simulation'] = sim_output
        simulation_rows.append(simulation_summary_row(ticker, sim_output))

    return {
        "tickers": list(tickers),
        "analyses": analyses,
        "prediction_summary": pd.DataFrame(prediction_rows),
        "exploratory_simulation_summary": pd.DataFrame(simulation_rows),
    }


def run_index_search_workflow(
    index_name="sp500",
    limit=50,
    include_sentiment=False,
    include_fundamentals=True,
    use_ticker_cache=True,
    ticker_cache_dir=DEFAULT_CACHE_DIR,
    ticker_cache_max_age_hours=DEFAULT_INDEX_CACHE_MAX_AGE_HOURS,
):
    if use_ticker_cache:
        tickers, ticker_cache = get_index_tickers_cached(
            index_name=index_name,
            limit=limit,
            cache_dir=ticker_cache_dir,
            max_age_hours=ticker_cache_max_age_hours,
        )
    else:
        tickers = get_index_tickers(index_name=index_name, limit=limit)
        ticker_cache = None
    analyses = {}
    prediction_rows = []
    for ticker in tickers:
        analysis = build_stock_analysis(
            ticker,
            include_sentiment=include_sentiment,
            include_fundamentals=include_fundamentals,
        )
        analyses[ticker] = analysis
        prediction_rows.append(build_prediction_summary_row(analysis))

    result = {
        "index_name": index_name,
        "tickers": tickers,
        "analyses": analyses,
        "prediction_summary": pd.DataFrame(prediction_rows),
        "ticker_cache": ticker_cache,
    }
    if not result["prediction_summary"].empty:
        result["prediction_summary"] = result["prediction_summary"].sort_values(
            by=['Signal', 'cumulative_return'],
            ascending=[False, False],
        ).reset_index(drop=True)
    return result


def extract_holdings_pnl_view(workflow_result):
    rows = []
    for holding in workflow_result['holdings']:
        ticker = holding['ticker']
        analysis = workflow_result['analyses'][ticker]
        latest_close = _safe_float(analysis['df_pred']['Close'].iloc[0])
        units = _safe_float(holding['units']) or 0
        avg_price = _safe_float(holding['average_buying_price']) or 0
        market_value = (latest_close or 0) * units if latest_close is not None else None
        cost_basis = avg_price * units
        pnl = (market_value - cost_basis) if market_value is not None else None
        rows.append({
            'TICKER': ticker,
            'units': units,
            'average_buying_price': avg_price,
            'latest_close': latest_close,
            'market_value': market_value,
            'cost_basis': cost_basis,
            'unrealized_pnl': pnl,
            'signal_text': analysis['recent_signal']['signal_text'],
            'signal_number': analysis['recent_signal']['signal_number'],
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(by=['signal_number', 'unrealized_pnl'], ascending=[False, False])
