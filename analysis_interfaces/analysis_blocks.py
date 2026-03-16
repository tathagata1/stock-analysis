
from datetime import datetime


import pandas as pd

import dao.dao as dao
import analysis_types.prediction  as prediction
import analysis_types.simulation as simulation
import config






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


def current_run_date():
    return datetime.now().strftime("%Y%m%d")


def load_portfolio(portfolio_path):
    return dao.read_json(portfolio_path)


def build_prediction_and_stats(ticker, include_sentiment=False):
    df_5y = pd.DataFrame(dao.get_yahoo_finance_5y(ticker))
    if df_5y.empty:
        raise ValueError(f"No history returned for {ticker}")
    df_pred = prediction.get_prediction(
        df_5y,
        stats=pd.DataFrame(dao.get_yahoo_finance_key_stats(ticker)).iloc[0],
        include_sentiment=include_sentiment,
    )
    df_pred = prediction.add_total_signal(df_pred)
    df_pred = prediction.convert_signal_to_text(df_pred)
    return df_pred


def get_recent_weighted_signal(df_pred, start_iloc=config.DEFAULT_SIGNAL_LOOKBACK_START, end_iloc=config.DEFAULT_SIGNAL_LOOKBACK_END):
    signals = df_pred["Signal_Text"].iloc[start_iloc:end_iloc + 1].reset_index(drop=True)
    signal_text, signal_number = prediction.get_weighted_signal(signals)
    return {
        "signal_text": signal_text,
        "signal_number": signal_number,
        "signals_considered": len(signals),
    }


def build_stock_analysis(ticker, include_sentiment=False):
    df_pred, stats_row = build_prediction_and_stats(
        ticker,
        include_sentiment=include_sentiment,
    )
    recent_signal = get_recent_weighted_signal(df_pred)

    return {
        "ticker": ticker,
        "df_pred": df_pred,
        "stats": stats_row,
        "recent_signal": recent_signal,
    }


def build_prediction_summary_row(stock_analysis, run_date=None):
    run_date = run_date or current_run_date()
    return {
        "current_date": run_date,
        "TICKER": stock_analysis["ticker"],
        "Signal": stock_analysis["recent_signal"]["signal_number"],
        "Signal_Text": stock_analysis["recent_signal"]["signal_text"],
    }


def get_price_history(df_pred):
    return simulation.get_price_history(df_pred)


def simulate_prediction_signal_strategy(df_pred, initial_funds):
    return simulation.simulate_prediction_signal_strategy(df_pred, initial_funds)


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
            include_sentiment=include_sentiment,
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
    ticker_cache_dir=config.DEFAULT_CACHE_DIR,
    ticker_cache_max_age_hours=config.DEFAULT_INDEX_CACHE_MAX_AGE_HOURS,
):
    if use_ticker_cache:
        tickers, ticker_cache = dao.get_index_tickers_cached(
            index_name=index_name,
            limit=limit,
            cache_dir=ticker_cache_dir,
            max_age_hours=ticker_cache_max_age_hours,
        )
    else:
        tickers = dao.get_index_tickers(index_name=index_name, limit=limit)
        ticker_cache = None
    analyses = {}
    prediction_rows = []
    for ticker in tickers:
        analysis = build_stock_analysis(
            ticker,
            include_sentiment=include_sentiment,
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
            by=['Signal', 'TICKER'],
            ascending=[False, True],
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
