
from datetime import datetime
import pandas as pd
from analysis_interfaces.interface_specific_stock import build_prediction_and_stats
import dao.dao as dao
import analysis_types.prediction  as prediction
import config


def current_run_date():
    return datetime.now().strftime("%Y%m%d")

def build_prediction_summary_row(stock_analysis, run_date=None):
    run_date = run_date or current_run_date()
    return {
        "current_date": run_date,
        "TICKER": stock_analysis["ticker"],
        "Signal": stock_analysis["recent_signal"]["signal_number"],
        "Signal_Text": stock_analysis["recent_signal"]["signal_text"],
    }

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
