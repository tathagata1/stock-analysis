import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import dao
import prediction
import simulation


DEFAULT_SIGNAL_LOOKBACK_START = 1
DEFAULT_SIGNAL_LOOKBACK_END = 30
DEFAULT_CACHE_DIR = "cache"
DEFAULT_INDEX_CACHE_MAX_AGE_HOURS = 24
SIGNAL_TEXT_TO_NUMBER = {
    "STRONG SELL": -2,
    "WEAK SELL": -1,
    "HOLD": 0,
    "WEAK BUY": 1,
    "STRONG BUY": 2,
}
SIGNAL_NUMBER_TO_TEXT = {value: key for key, value in SIGNAL_TEXT_TO_NUMBER.items()}
TRADE_ALLOCATION_BY_SIGNAL = {
    "STRONG BUY": 0.10,
    "WEAK BUY": 0.05,
    "WEAK SELL": 0.05,
    "STRONG SELL": 0.10,
}


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


def _normalize_signal_text(signal_text):
    return str(signal_text).strip().upper()


def _signal_number_to_text(signal_number):
    try:
        normalized_number = int(round(float(signal_number)))
    except Exception:
        normalized_number = 0
    normalized_number = max(-2, min(2, normalized_number))
    return SIGNAL_NUMBER_TO_TEXT[normalized_number]


def _average_open_close(open_price, close_price):
    open_value = _safe_float(open_price)
    close_value = _safe_float(close_price)
    if open_value is None or close_value is None:
        return None
    return (open_value + close_value) / 2


def _calculate_sharpe_ratio(daily_returns, risk_free_rate=0):
    if len(daily_returns) < 2 or np.std(daily_returns) == 0:
        return np.nan
    return (np.mean(daily_returns - risk_free_rate) / np.std(daily_returns)) * np.sqrt(252)


def _calculate_sortino_ratio(daily_returns, risk_free_rate=0):
    negative_returns = daily_returns[daily_returns < 0]
    downside_deviation = np.std(negative_returns)
    if len(daily_returns) < 2 or downside_deviation == 0:
        return np.nan
    return (np.mean(daily_returns - risk_free_rate) / downside_deviation) * np.sqrt(252)


def _calculate_max_drawdown(cumulative_returns):
    if cumulative_returns.empty:
        return np.nan
    drawdown = cumulative_returns - cumulative_returns.cummax()
    return drawdown.min()


def _calculate_calmar_ratio(cumulative_returns):
    if cumulative_returns.empty:
        return np.nan
    max_drawdown = _calculate_max_drawdown(cumulative_returns)
    annual_return = cumulative_returns.iloc[-1]
    return annual_return / abs(max_drawdown) if max_drawdown != 0 else np.nan


def _interpret_cumulative_return(value):
    if value > 0.5:
        return "Positive: Excellent"
    if value > 0:
        return "Positive: Good"
    return "Negative: Poor"


def _interpret_sharpe_ratio(value):
    if value > 2:
        return "Positive: Excellent"
    if value > 1:
        return "Positive: Good"
    if value > 0:
        return "Positive: Bad"
    return "Negative: Poor"


def _interpret_sortino_ratio(value):
    if value > 2:
        return "Positive: Excellent"
    if value > 1:
        return "Positive: Good"
    if value > 0:
        return "Negative: Poor"
    return "Negative: Unacceptable"


def _interpret_max_drawdown(value):
    if value > -0.1:
        return "Positive: Excellent"
    if value > -0.2:
        return "Positive: Good"
    return "Negative: Poor"


def _interpret_calmar_ratio(value):
    if value > 3:
        return "Positive: Excellent "
    if value > 1:
        return "Positive: Good"
    if value > 0:
        return "Positive: Acceptable"
    return "Negative: Poor"


def _backtest_prediction_frame(df):
    df = df.copy()
    df['Backtest_Strategy_Return'] = df['Daily_Return'] * df['Signal'].shift(1)
    df['Backtest_Cumulative_Strategy_Return'] = (1 + df['Backtest_Strategy_Return']).cumprod() - 1

    cumulative_return = df['Backtest_Cumulative_Strategy_Return'].iloc[-1] if not df.empty else np.nan
    sharpe_ratio = _calculate_sharpe_ratio(df['Backtest_Strategy_Return'])
    sortino_ratio = _calculate_sortino_ratio(df['Backtest_Strategy_Return'])
    max_drawdown = _calculate_max_drawdown(df['Backtest_Cumulative_Strategy_Return'])
    calmar_ratio = _calculate_calmar_ratio(df['Backtest_Cumulative_Strategy_Return'])

    metrics = _build_metrics_dict(
        _interpret_cumulative_return(cumulative_return),
        _interpret_sharpe_ratio(sharpe_ratio),
        _interpret_sortino_ratio(sortino_ratio),
        _interpret_max_drawdown(max_drawdown),
        _interpret_calmar_ratio(calmar_ratio),
    )
    return df, metrics


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


def build_prediction_frame(ticker, include_sentiment=False):
    df_5y = pd.DataFrame(dao.get_yahoo_finance_5y(ticker))
    if df_5y.empty:
        raise ValueError(f"No history returned for {ticker}")

    stats_row = None
    df_stats = pd.DataFrame(dao.get_yahoo_finance_key_stats(ticker))
    stats_row = df_stats.iloc[0]
    df_pred = prediction.get_prediction(
        df_5y,
        stats_row,
        include_sentiment=include_sentiment
    )

    df_pred = prediction.add_total_signal(df_pred)
    df_pred = prediction.convert_signal_to_text(df_pred)
    return df_pred, stats_row


def backtest_prediction_frame(df_pred):
    return _backtest_prediction_frame(df_pred)


def get_recent_weighted_signal(df_tested, start_iloc=DEFAULT_SIGNAL_LOOKBACK_START, end_iloc=DEFAULT_SIGNAL_LOOKBACK_END):
    signals = df_tested["Signal_Text"].iloc[start_iloc:end_iloc + 1].reset_index(drop=True)
    signal_text, signal_number = prediction.get_weighted_signal(signals)
    return {
        "signal_text": signal_text,
        "signal_number": signal_number,
        "signals_considered": len(signals),
    }


def build_stock_analysis(ticker, include_sentiment=False):
    df_pred, stats_row = build_prediction_frame(
        ticker,
        include_sentiment=include_sentiment
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
    history['Trade_Price'] = (history['Open'] + history['Close']) / 2
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

    

    units_value = _safe_float(units)
    if units_value is None or units_value < 0:
        raise ValueError("units must be zero or a positive number")

    latest_row = history.iloc[-1]
    latest_close = _safe_float(latest_row['Close'])
    buy_price_value = _average_open_close(open_price, close_price)
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


def build_index_daily_signal_frame(index_workflow_result, start_date=None, end_date=None):
    signal_rows = []
    analyses = index_workflow_result.get("analyses", {})

    for ticker, analysis in analyses.items():
        df_pred = analysis.get("df_pred")
        if df_pred is None or df_pred.empty:
            continue

        frame = df_pred[['Date', 'Signal_Text']].copy()
        frame['Date'] = pd.to_datetime(frame['Date']).dt.tz_localize(None).dt.normalize()
        frame['Signal_Text'] = frame['Signal_Text'].map(_normalize_signal_text)
        frame['signal_number'] = frame['Signal_Text'].map(SIGNAL_TEXT_TO_NUMBER)
        frame['TICKER'] = ticker
        frame = frame.dropna(subset=['Date', 'signal_number'])
        signal_rows.append(frame[['Date', 'TICKER', 'Signal_Text', 'signal_number']])

    if not signal_rows:
        return pd.DataFrame(
            columns=[
                'Date',
                'constituents',
                'avg_signal_number',
                'signal_number',
                'signal_text',
                'strong_sell_count',
                'weak_sell_count',
                'hold_count',
                'weak_buy_count',
                'strong_buy_count',
            ]
        )

    combined = pd.concat(signal_rows, ignore_index=True)

    if start_date is not None:
        combined = combined[combined['Date'] >= pd.to_datetime(start_date).normalize()]
    if end_date is not None:
        combined = combined[combined['Date'] <= pd.to_datetime(end_date).normalize()]

    if combined.empty:
        return pd.DataFrame(
            columns=[
                'Date',
                'constituents',
                'avg_signal_number',
                'signal_number',
                'signal_text',
                'strong_sell_count',
                'weak_sell_count',
                'hold_count',
                'weak_buy_count',
                'strong_buy_count',
            ]
        )

    grouped = combined.groupby('Date')
    summary = grouped.agg(
        constituents=('TICKER', 'nunique'),
        avg_signal_number=('signal_number', 'mean'),
    ).reset_index()

    for signal_text in SIGNAL_TEXT_TO_NUMBER:
        column_name = signal_text.lower().replace(' ', '_') + '_count'
        counts = (
            combined.assign(is_match=combined['Signal_Text'] == signal_text)
            .groupby('Date')['is_match']
            .sum()
            .reset_index(name=column_name)
        )
        summary = summary.merge(counts, on='Date', how='left')

    count_columns = [column for column in summary.columns if column.endswith('_count')]
    summary[count_columns] = summary[count_columns].fillna(0).astype(int)
    summary['signal_number'] = summary['avg_signal_number'].round().clip(-2, 2).astype(int)
    summary['signal_text'] = summary['signal_number'].map(_signal_number_to_text)
    return summary.sort_values('Date').reset_index(drop=True)


def simulate_manual_position_with_index_signals(df_pred, buy_date, units, initial_funds, index_daily_signal_frame):
    price_history = get_price_history(df_pred)
    if price_history.empty:
        raise ValueError("Price history is empty")

    buy_lookup = resolve_buy_date(price_history, buy_date)
    resolved_buy_date = pd.to_datetime(buy_lookup["resolved_buy_date"]).normalize()
    simulation_frame = price_history.copy()
    simulation_frame['Date'] = pd.to_datetime(simulation_frame['Date']).dt.normalize()
    simulation_frame = simulation_frame[simulation_frame['Date'] >= resolved_buy_date].copy()

    if simulation_frame.empty:
        raise ValueError("No price history is available on or after the resolved buy date")

    if index_daily_signal_frame is None or index_daily_signal_frame.empty:
        raise ValueError("index_daily_signal_frame is empty")

    index_signals = index_daily_signal_frame.copy()
    index_signals['Date'] = pd.to_datetime(index_signals['Date']).dt.normalize()
    simulation_frame = simulation_frame.merge(
        index_signals[['Date', 'signal_text', 'signal_number', 'avg_signal_number', 'constituents']],
        on='Date',
        how='left',
    )
    simulation_frame['signal_text'] = simulation_frame['signal_text'].fillna('HOLD')
    simulation_frame['signal_number'] = simulation_frame['signal_number'].fillna(0).astype(int)
    simulation_frame['avg_signal_number'] = simulation_frame['avg_signal_number'].fillna(0.0)
    simulation_frame['constituents'] = simulation_frame['constituents'].fillna(0).astype(int)

    initial_units = _safe_float(units)
    if initial_units is None or initial_units < 0:
        raise ValueError("units must be zero or a positive number")

    starting_cash = _safe_float(initial_funds)
    if starting_cash is None or starting_cash < 0:
        raise ValueError("initial_funds must be zero or a positive number")

    first_row = simulation_frame.iloc[0]
    initial_buy_price = _average_open_close(first_row['Open'], first_row['Close'])
    initial_cost = initial_units * initial_buy_price
    if initial_cost > starting_cash:
        raise ValueError("initial_funds is lower than the cost of the initial holdings")

    cash_balance = starting_cash - initial_cost
    stock_units = initial_units
    total_cost = initial_cost
    transactions = []
    daily_rows = []

    if initial_units > 0:
        transactions.append({
            "Date": first_row['Date'],
            "action": "BUY",
            "reason": "INITIAL POSITION",
            "signal_text": "INITIAL BUY",
            "trade_price": initial_buy_price,
            "units": initial_units,
            "cash_balance": cash_balance,
            "units_held": stock_units,
        })

    initial_holdings_value = stock_units * first_row['Close']
    initial_portfolio_value = cash_balance + initial_holdings_value
    daily_rows.append({
        "Date": first_row['Date'],
        "Open": first_row['Open'],
        "Close": first_row['Close'],
        "Trade_Price": initial_buy_price,
        "signal_text": first_row['signal_text'],
        "signal_number": first_row['signal_number'],
        "avg_signal_number": first_row['avg_signal_number'],
        "constituents": first_row['constituents'],
        "action": "BUY" if initial_units > 0 else "HOLD",
        "trade_units": initial_units,
        "cash_balance": cash_balance,
        "units_held": stock_units,
        "average_cost_per_unit": (total_cost / stock_units) if stock_units > 0 else 0,
        "holdings_value": initial_holdings_value,
        "portfolio_value": initial_portfolio_value,
    })

    for _, row in simulation_frame.iloc[1:].iterrows():
        trade_price = _average_open_close(row['Open'], row['Close'])
        signal_text = _normalize_signal_text(row['signal_text'])
        action = "HOLD"
        trade_units = 0.0

        if signal_text in ("WEAK BUY", "STRONG BUY") and trade_price:
            allocation = cash_balance * TRADE_ALLOCATION_BY_SIGNAL[signal_text]
            if allocation > 0:
                trade_units = allocation / trade_price
                cash_balance -= allocation
                stock_units += trade_units
                total_cost += allocation
                action = "BUY"
        elif signal_text in ("WEAK SELL", "STRONG SELL") and trade_price:
            trade_units = stock_units * TRADE_ALLOCATION_BY_SIGNAL[signal_text]
            if trade_units > 0:
                average_cost_before_sale = (total_cost / stock_units) if stock_units > 0 else 0
                proceeds = trade_units * trade_price
                cash_balance += proceeds
                stock_units -= trade_units
                total_cost -= average_cost_before_sale * trade_units
                total_cost = max(total_cost, 0)
                action = "SELL"

        holdings_value = stock_units * row['Close']
        portfolio_value = cash_balance + holdings_value
        average_cost_per_unit = (total_cost / stock_units) if stock_units > 0 else 0

        daily_rows.append({
            "Date": row['Date'],
            "Open": row['Open'],
            "Close": row['Close'],
            "Trade_Price": trade_price,
            "signal_text": signal_text,
            "signal_number": row['signal_number'],
            "avg_signal_number": row['avg_signal_number'],
            "constituents": row['constituents'],
            "action": action,
            "trade_units": trade_units,
            "cash_balance": cash_balance,
            "units_held": stock_units,
            "average_cost_per_unit": average_cost_per_unit,
            "holdings_value": holdings_value,
            "portfolio_value": portfolio_value,
        })

        if action in ("BUY", "SELL"):
            transactions.append({
                "Date": row['Date'],
                "action": action,
                "reason": "INDEX SIGNAL",
                "signal_text": signal_text,
                "trade_price": trade_price,
                "units": trade_units,
                "cash_balance": cash_balance,
                "units_held": stock_units,
            })

    daily_history = pd.DataFrame(daily_rows)
    transactions_frame = pd.DataFrame(transactions)

    latest_row = daily_history.iloc[-1]
    total_portfolio_value = latest_row['portfolio_value']
    profit_loss = total_portfolio_value - starting_cash
    profit_loss_pct = (profit_loss / starting_cash * 100) if starting_cash else None

    summary = pd.DataFrame([{
        "requested_buy_date": buy_lookup["requested_buy_date"],
        "resolved_buy_date": buy_lookup["resolved_buy_date"],
        "buy_date_resolution": buy_lookup["buy_date_resolution"],
        "initial_funds": starting_cash,
        "initial_buy_price": initial_buy_price,
        "initial_units": initial_units,
        "ending_cash_balance": latest_row['cash_balance'],
        "units_held": latest_row['units_held'],
        "average_cost_per_unit": latest_row['average_cost_per_unit'],
        "latest_close": latest_row['Close'],
        "holdings_value": latest_row['holdings_value'],
        "total_portfolio_value": total_portfolio_value,
        "profit_loss": profit_loss,
        "profit_loss_pct": profit_loss_pct,
        "buy_transactions": int((transactions_frame['action'] == 'BUY').sum()) if not transactions_frame.empty else 0,
        "sell_transactions": int((transactions_frame['action'] == 'SELL').sum()) if not transactions_frame.empty else 0,
    }])

    return {
        "buy_lookup": buy_lookup,
        "price_history": price_history,
        "index_daily_signal": index_signals.sort_values('Date').reset_index(drop=True),
        "daily_history": daily_history,
        "transactions": transactions_frame,
        "summary": summary,
    }


def simulate_index_signal_strategy(df_pred, initial_funds, index_daily_signal_frame):
    price_history = get_price_history(df_pred)
    if price_history.empty:
        raise ValueError("Price history is empty")

    starting_cash = _safe_float(initial_funds)
    if starting_cash is None or starting_cash < 0:
        raise ValueError("initial_funds must be zero or a positive number")

    if index_daily_signal_frame is None or index_daily_signal_frame.empty:
        raise ValueError("index_daily_signal_frame is empty")

    simulation_frame = price_history.copy()
    simulation_frame['Date'] = pd.to_datetime(simulation_frame['Date']).dt.normalize()

    index_signals = index_daily_signal_frame.copy()
    index_signals['Date'] = pd.to_datetime(index_signals['Date']).dt.normalize()

    simulation_frame = simulation_frame.merge(
        index_signals[['Date', 'signal_text', 'signal_number', 'avg_signal_number', 'constituents']],
        on='Date',
        how='left',
    )
    simulation_frame['signal_text'] = simulation_frame['signal_text'].fillna('HOLD')
    simulation_frame['signal_number'] = simulation_frame['signal_number'].fillna(0).astype(int)
    simulation_frame['avg_signal_number'] = simulation_frame['avg_signal_number'].fillna(0.0)
    simulation_frame['constituents'] = simulation_frame['constituents'].fillna(0).astype(int)

    cash_balance = starting_cash
    stock_units = 0.0
    total_cost = 0.0
    transactions = []
    daily_rows = []

    for _, row in simulation_frame.iterrows():
        trade_price = _average_open_close(row['Open'], row['Close'])
        signal_text = _normalize_signal_text(row['signal_text'])
        action = "HOLD"
        trade_units = 0.0
        trade_value = 0.0

        if signal_text in ("WEAK BUY", "STRONG BUY") and trade_price:
            target_trade_value = starting_cash * TRADE_ALLOCATION_BY_SIGNAL[signal_text]
            trade_value = min(target_trade_value, cash_balance)
            if trade_value > 0:
                trade_units = trade_value / trade_price
                cash_balance -= trade_value
                stock_units += trade_units
                total_cost += trade_value
                action = "BUY"
        elif signal_text in ("WEAK SELL", "STRONG SELL") and trade_price and stock_units > 0:
            trade_units = stock_units * TRADE_ALLOCATION_BY_SIGNAL[signal_text]
            trade_units = min(trade_units, stock_units)
            trade_value = trade_units * trade_price
            if trade_units > 0 and trade_value > 0:
                average_cost_before_sale = (total_cost / stock_units) if stock_units > 0 else 0
                cash_balance += trade_value
                stock_units -= trade_units
                total_cost -= average_cost_before_sale * trade_units
                total_cost = max(total_cost, 0.0)
                stock_units = max(stock_units, 0.0)
                action = "SELL"

        holdings_value = stock_units * row['Close']
        portfolio_value = cash_balance + holdings_value
        average_cost_per_unit = (total_cost / stock_units) if stock_units > 0 else 0.0
        profit_loss = portfolio_value - starting_cash
        profit_loss_pct = (profit_loss / starting_cash * 100) if starting_cash else None

        daily_rows.append({
            "Date": row['Date'],
            "Open": row['Open'],
            "Close": row['Close'],
            "Trade_Price": trade_price,
            "signal_text": signal_text,
            "signal_number": row['signal_number'],
            "avg_signal_number": row['avg_signal_number'],
            "constituents": row['constituents'],
            "action": action,
            "trade_units": trade_units,
            "trade_value": trade_value,
            "cash_balance": cash_balance,
            "units_held": stock_units,
            "average_cost_per_unit": average_cost_per_unit,
            "holdings_value": holdings_value,
            "portfolio_value": portfolio_value,
            "profit_loss": profit_loss,
            "profit_loss_pct": profit_loss_pct,
        })

        if action in ("BUY", "SELL"):
            transactions.append({
                "Date": row['Date'],
                "action": action,
                "signal_text": signal_text,
                "trade_price": trade_price,
                "units": trade_units,
                "trade_value": trade_value,
                "cash_balance": cash_balance,
                "units_held": stock_units,
                "portfolio_value": portfolio_value,
            })

    daily_history = pd.DataFrame(daily_rows)
    transactions_frame = pd.DataFrame(transactions)

    latest_row = daily_history.iloc[-1]
    summary = pd.DataFrame([{
        "start_date": daily_history.iloc[0]['Date'],
        "end_date": latest_row['Date'],
        "initial_funds": starting_cash,
        "ending_cash_balance": latest_row['cash_balance'],
        "units_held": latest_row['units_held'],
        "average_cost_per_unit": latest_row['average_cost_per_unit'],
        "latest_close": latest_row['Close'],
        "holdings_value": latest_row['holdings_value'],
        "total_portfolio_value": latest_row['portfolio_value'],
        "profit_loss": latest_row['profit_loss'],
        "profit_loss_pct": latest_row['profit_loss_pct'],
        "buy_transactions": int((transactions_frame['action'] == 'BUY').sum()) if not transactions_frame.empty else 0,
        "sell_transactions": int((transactions_frame['action'] == 'SELL').sum()) if not transactions_frame.empty else 0,
    }])

    return {
        "price_history": price_history,
        "index_daily_signal": index_signals.sort_values('Date').reset_index(drop=True),
        "daily_history": daily_history,
        "transactions": transactions_frame,
        "summary": summary,
    }


def simulate_prediction_signal_strategy(df_pred, initial_funds):
    price_history = get_price_history(df_pred)
    if price_history.empty:
        raise ValueError("Price history is empty")

    starting_cash = _safe_float(initial_funds)
    if starting_cash is None or starting_cash < 0:
        raise ValueError("initial_funds must be zero or a positive number")

    signal_history = df_pred[['Date', 'Signal_Text']].copy()
    signal_history['Date'] = pd.to_datetime(signal_history['Date']).dt.tz_localize(None).dt.normalize()
    signal_history['signal_text'] = signal_history['Signal_Text'].map(_normalize_signal_text)
    signal_history['signal_number'] = signal_history['signal_text'].map(SIGNAL_TEXT_TO_NUMBER).fillna(0).astype(int)
    signal_history = signal_history[['Date', 'signal_text', 'signal_number']]

    simulation_frame = price_history.copy()
    simulation_frame['Date'] = pd.to_datetime(simulation_frame['Date']).dt.normalize()
    simulation_frame = simulation_frame.merge(signal_history, on='Date', how='left')
    simulation_frame['signal_text'] = simulation_frame['signal_text'].fillna('HOLD')
    simulation_frame['signal_number'] = simulation_frame['signal_number'].fillna(0).astype(int)

    cash_balance = starting_cash
    stock_units = 0.0
    total_cost = 0.0
    transactions = []
    daily_rows = []

    for _, row in simulation_frame.iterrows():
        trade_price = _average_open_close(row['Open'], row['Close'])
        signal_text = _normalize_signal_text(row['signal_text'])
        action = "HOLD"
        trade_units = 0.0
        trade_value = 0.0

        if signal_text in ("WEAK BUY", "STRONG BUY") and trade_price:
            target_trade_value = starting_cash * TRADE_ALLOCATION_BY_SIGNAL[signal_text]
            trade_value = min(target_trade_value, cash_balance)
            if trade_value > 0:
                trade_units = trade_value / trade_price
                cash_balance -= trade_value
                stock_units += trade_units
                total_cost += trade_value
                action = "BUY"
        elif signal_text in ("WEAK SELL", "STRONG SELL") and trade_price and stock_units > 0:
            trade_units = stock_units * TRADE_ALLOCATION_BY_SIGNAL[signal_text]
            trade_units = min(trade_units, stock_units)
            trade_value = trade_units * trade_price
            if trade_units > 0 and trade_value > 0:
                average_cost_before_sale = (total_cost / stock_units) if stock_units > 0 else 0
                cash_balance += trade_value
                stock_units -= trade_units
                total_cost -= average_cost_before_sale * trade_units
                total_cost = max(total_cost, 0.0)
                stock_units = max(stock_units, 0.0)
                action = "SELL"

        holdings_value = stock_units * row['Close']
        portfolio_value = cash_balance + holdings_value
        average_cost_per_unit = (total_cost / stock_units) if stock_units > 0 else 0.0
        profit_loss = portfolio_value - starting_cash
        profit_loss_pct = (profit_loss / starting_cash * 100) if starting_cash else None

        daily_rows.append({
            "Date": row['Date'],
            "Open": row['Open'],
            "Close": row['Close'],
            "Trade_Price": trade_price,
            "signal_text": signal_text,
            "signal_number": row['signal_number'],
            "action": action,
            "trade_units": trade_units,
            "trade_value": trade_value,
            "cash_balance": cash_balance,
            "units_held": stock_units,
            "average_cost_per_unit": average_cost_per_unit,
            "holdings_value": holdings_value,
            "portfolio_value": portfolio_value,
            "profit_loss": profit_loss,
            "profit_loss_pct": profit_loss_pct,
        })

        if action in ("BUY", "SELL"):
            transactions.append({
                "Date": row['Date'],
                "action": action,
                "signal_text": signal_text,
                "trade_price": trade_price,
                "units": trade_units,
                "trade_value": trade_value,
                "cash_balance": cash_balance,
                "units_held": stock_units,
                "portfolio_value": portfolio_value,
            })

    daily_history = pd.DataFrame(daily_rows)
    transactions_frame = pd.DataFrame(transactions)

    latest_row = daily_history.iloc[-1]
    summary = pd.DataFrame([{
        "start_date": daily_history.iloc[0]['Date'],
        "end_date": latest_row['Date'],
        "initial_funds": starting_cash,
        "ending_cash_balance": latest_row['cash_balance'],
        "units_held": latest_row['units_held'],
        "average_cost_per_unit": latest_row['average_cost_per_unit'],
        "latest_close": latest_row['Close'],
        "holdings_value": latest_row['holdings_value'],
        "total_portfolio_value": latest_row['portfolio_value'],
        "profit_loss": latest_row['profit_loss'],
        "profit_loss_pct": latest_row['profit_loss_pct'],
        "buy_transactions": int((transactions_frame['action'] == 'BUY').sum()) if not transactions_frame.empty else 0,
        "sell_transactions": int((transactions_frame['action'] == 'SELL').sum()) if not transactions_frame.empty else 0,
    }])

    return {
        "price_history": price_history,
        "daily_history": daily_history,
        "transactions": transactions_frame,
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
        analysis = build_stock_analysis(ticker, include_sentiment=include_sentiment)
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


def run_specific_stock_simulation_workflow(tickers, initial_funds=1000, include_sentiment=False):
    analyses = {}
    prediction_rows = []
    simulation_rows = []

    for ticker in tickers:
        analysis = build_stock_analysis(
            ticker,
            include_sentiment=include_sentiment
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
            include_sentiment=include_sentiment
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
