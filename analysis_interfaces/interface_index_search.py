
from datetime import datetime
import pandas as pd
from analysis_interfaces.interface_specific_stock import build_prediction_and_stats
import dao.dao as dao
import config.config as config
from config.logging_config import get_logger

logger = get_logger(__name__)


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


def build_stock_analysis(ticker, include_sentiment=False, period="max"):
    logger.info("Building stock analysis. ticker=%s include_sentiment=%s period=%s", ticker, include_sentiment, period)
    df_pred, stats_row = build_prediction_and_stats(
        ticker,
        include_sentiment=include_sentiment,
        return_stats=True,
        period=period,
    )
    
    latest_row = df_pred.iloc[0]
    recent_signal = {
        "signal_text": latest_row["Signal_Text"],
        "signal_number": latest_row["Signal"],
    }

    return {
        "ticker": ticker,
        "df_pred": df_pred,
        "stats": stats_row,
        "recent_signal": recent_signal,
    }

def run_index_search_workflow(
    index_name,
    limit,
    include_sentiment=False,
    use_ticker_cache=True,
    ticker_cache_dir=config.DEFAULT_CACHE_DIR,
    ticker_cache_max_age_hours=config.DEFAULT_INDEX_CACHE_MAX_AGE_HOURS,
    period="max",
):
    logger.info(
        "Running index search workflow. index_name=%s limit=%s include_sentiment=%s use_ticker_cache=%s",
        index_name,
        limit,
        include_sentiment,
        use_ticker_cache,
    )
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
        logger.info("Processing index constituent. index_name=%s ticker=%s", index_name, ticker)
        analysis = build_stock_analysis(
            ticker,
            include_sentiment=include_sentiment,
            period=period,
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
    logger.info(
        "Completed index search workflow. index_name=%s ticker_count=%s summary_rows=%s",
        index_name,
        len(tickers),
        len(result["prediction_summary"]),
    )
    return result
