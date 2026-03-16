import pandas as pd

import dao.dao as dao
import analysis_types.prediction  as prediction
import analysis_types.simulation as simulation
from config.logging_config import get_logger

logger = get_logger(__name__)


def build_prediction_and_stats(ticker, include_sentiment=False, return_stats=False, period="1d"):
    logger.info(
        "Building prediction and stats. ticker=%s include_sentiment=%s return_stats=%s period=%s",
        ticker,
        include_sentiment,
        return_stats,
        period,
    )
    df_5y = pd.DataFrame(dao.get_yahoo_finance(ticker, period))
    stats_row = pd.DataFrame(dao.get_yahoo_finance_key_stats(ticker)).iloc[0]
    df_pred = prediction.get_prediction(
        df_5y,
        stats=stats_row,
        include_sentiment=include_sentiment,
    )
    df_pred = prediction.add_total_signal(df_pred)
    df_pred = prediction.convert_signal_to_text(df_pred)
    logger.info("Built prediction and stats successfully. ticker=%s rows=%s", ticker, len(df_pred))
    if return_stats:
        return df_pred, stats_row
    return df_pred


def simulate_prediction_signal_strategy(df_pred, initial_funds):
    logger.info("Running prediction signal simulation. initial_funds=%s", initial_funds)
    return simulation.simulate_prediction_signal_strategy(df_pred, initial_funds)
