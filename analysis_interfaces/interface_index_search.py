
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
        "Analysis_Status": stock_analysis["recent_signal"]["analysis_status"],
        "Signal_Coverage": stock_analysis["recent_signal"]["signal_coverage"],
    }


def build_stock_analysis(
    ticker,
    include_sentiment=False,
    period="1y",
    historical_analysis=False,
):
    logger.info("Building stock analysis. ticker=%s include_sentiment=%s period=%s", ticker, include_sentiment, period)
    df_pred, stats_row = build_prediction_and_stats(
        ticker,
        include_sentiment=include_sentiment,
        return_stats=True,
        period=period,
        historical_analysis=historical_analysis,
    )

    if df_pred.empty:
        raise ValueError(f"Prediction pipeline returned no rows for {ticker}")
    latest_row = df_pred.iloc[0]
    recent_signal = {
        "signal_text": latest_row["Signal_Text"],
        "signal_number": latest_row["Signal"],
        "analysis_status": latest_row.get("analysis_status", "UNKNOWN"),
        "signal_coverage": latest_row.get("signal_coverage"),
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
    period="1y",
    historical_analysis=False,
    analysis_cache=None,
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
    failure_rows = []
    analysis_cache = analysis_cache if analysis_cache is not None else {}
    
    for ticker in tickers:
        logger.info("Processing index constituent. index_name=%s ticker=%s", index_name, ticker)
        try:
            analysis = analysis_cache.get(ticker)
            if analysis is None:
                analysis = build_stock_analysis(
                    ticker,
                    include_sentiment=include_sentiment,
                    period=period,
                    historical_analysis=historical_analysis,
                )
                analysis_cache[ticker] = analysis
            analyses[ticker] = analysis
            prediction_rows.append(build_prediction_summary_row(analysis))
        except Exception as exc:
            logger.exception(
                "Index constituent analysis failed. index_name=%s ticker=%s",
                index_name,
                ticker,
            )
            failure_rows.append({
                "index_name": index_name,
                "TICKER": ticker,
                "error_type": type(exc).__name__,
                "error": str(exc),
            })

    result = {
        "index_name": index_name,
        "tickers": tickers,
        "analyses": analyses,
        "prediction_summary": pd.DataFrame(prediction_rows),
        "ticker_cache": ticker_cache,
        "failures": pd.DataFrame(failure_rows),
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


def run_multi_index_search_workflow(
    index_names,
    limits_by_index,
    include_sentiment=False,
    use_ticker_cache=True,
    ticker_cache_dir=config.DEFAULT_CACHE_DIR,
    ticker_cache_max_age_hours=config.DEFAULT_INDEX_CACHE_MAX_AGE_HOURS,
    period="1y",
    historical_analysis=False,
):
    logger.info(
        "Running multi-index search workflow. index_count=%s include_sentiment=%s use_ticker_cache=%s period=%s",
        len(index_names),
        include_sentiment,
        use_ticker_cache,
        period,
    )
    results_by_index = {}
    prediction_frames = []
    ticker_cache_rows = []
    failure_frames = []
    analysis_cache = {}

    for index_name in index_names:
        result = run_index_search_workflow(
            index_name=index_name,
            limit=limits_by_index.get(index_name),
            include_sentiment=include_sentiment,
            use_ticker_cache=use_ticker_cache,
            ticker_cache_dir=ticker_cache_dir,
            ticker_cache_max_age_hours=ticker_cache_max_age_hours,
            period=period,
            historical_analysis=historical_analysis,
            analysis_cache=analysis_cache,
        )

        results_by_index[index_name] = result

        prediction_frame = result["prediction_summary"].copy()
        if not prediction_frame.empty:
            prediction_frame.insert(0, "index_name", index_name)
            prediction_frames.append(prediction_frame)

        ticker_cache = result.get("ticker_cache") or {}
        ticker_cache_rows.append({
            "index_name": index_name,
            "tickers_scanned": len(result.get("tickers", [])),
            "cache_used": ticker_cache.get("cache_used"),
            "cache_file": ticker_cache.get("cache_file"),
            "cache_age_hours": ticker_cache.get("cache_age_hours"),
            "stale_fallback": ticker_cache.get("stale_fallback"),
            "refresh_failed": ticker_cache.get("refresh_failed"),
        })
        if not result["failures"].empty:
            failure_frames.append(result["failures"])

    combined_prediction_summary = (
        pd.concat(prediction_frames, ignore_index=True)
        if prediction_frames
        else pd.DataFrame()
    )
    ranked_prediction_summary = combined_prediction_summary
    if not combined_prediction_summary.empty:
        ranked_prediction_summary = combined_prediction_summary.sort_values(
            by=["Signal", "index_name", "TICKER"],
            ascending=[False, True, True],
        ).reset_index(drop=True)

    signal_only = ranked_prediction_summary
    if not ranked_prediction_summary.empty:
        signal_only = ranked_prediction_summary[
            ranked_prediction_summary["Signal_Text"].str.contains("BUY|SELL", na=False)
        ].reset_index(drop=True)

    result = {
        "results_by_index": results_by_index,
        "combined_prediction_summary": combined_prediction_summary,
        "ranked_prediction_summary": ranked_prediction_summary,
        "signal_only": signal_only,
        "ticker_cache_summary": pd.DataFrame(ticker_cache_rows),
        "failures": (
            pd.concat(failure_frames, ignore_index=True)
            if failure_frames
            else pd.DataFrame(columns=["index_name", "TICKER", "error_type", "error"])
        ),
        "unique_tickers_analyzed": len(analysis_cache),
    }
    logger.info(
        "Completed multi-index search workflow. index_count=%s combined_rows=%s signal_rows=%s",
        len(index_names),
        len(combined_prediction_summary),
        len(signal_only),
    )
    return result
