import pandas as pd

import dao.dao as dao
import analysis_types.prediction  as prediction
import analysis_types.simulation as simulation


def build_prediction_and_stats(ticker, include_sentiment=False):
    df_5y = pd.DataFrame(dao.get_yahoo_finance_5y(ticker))
    df_pred = prediction.get_prediction(
        df_5y,
        stats=pd.DataFrame(dao.get_yahoo_finance_key_stats(ticker)).iloc[0],
        include_sentiment=include_sentiment,
    )
    df_pred = prediction.add_total_signal(df_pred)
    df_pred = prediction.convert_signal_to_text(df_pred)
    return df_pred


def simulate_prediction_signal_strategy(df_pred, initial_funds):
    return simulation.simulate_prediction_signal_strategy(df_pred, initial_funds)

