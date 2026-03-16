import numpy as np

import analysis_functions.fundamental_analysis as fundamental_analysis
import analysis_functions.technical_analysis as technical_analysis
import analysis_functions.sentiment_analysis as sentiment_analysis

import config

def get_prediction(df, stats=None, include_sentiment=True):
    df = technical_analysis.get_technical_analysis_calculations(df)
    df['technical_analysis_buy_score'] = df.apply(technical_analysis.calculate_buy_score, axis=1)
    df['technical_analysis_sell_score'] = df.apply(technical_analysis.calculate_sell_score, axis=1)
    df['fundamental_analysis_score'] = fundamental_analysis.get_fundamental_analysis(stats)
    ticker = df["TICKER"].iloc[0]
    df['sentiment_analysis_score'] = sentiment_analysis.apply_sentiment_analysis(ticker) if include_sentiment else 0
    return df


def add_total_signal(df):
    technical_signal = df['technical_analysis_buy_score'] + df['technical_analysis_sell_score']
    sentiment_signal = df['sentiment_analysis_score']
    fundamental_signal = df['fundamental_analysis_score']

    df['Signal'] = (
        (technical_signal * config.TECHNICAL_SIGNAL_WEIGHT)
        + (sentiment_signal * config.SENTIMENT_SIGNAL_WEIGHT)
        + (fundamental_signal * config.FUNDAMENTAL_SIGNAL_WEIGHT)
    )
    return df


def convert_signal_to_text(df):
    conditions = [
        (df['Signal'] <= -1.5),
        (df['Signal'] > -1.5) & (df['Signal'] < -0.5),
        (df['Signal'] >= -0.5) & (df['Signal'] < 0.05),
        (df['Signal'] >= 0.5) & (df['Signal'] < 1.5),
        (df['Signal'] >= 1.5),
    ]
    choices = ['STRONG SELL', 'WEAK SELL', 'HOLD', 'WEAK BUY', 'STRONG BUY']
    df['Signal_Text'] = np.select(conditions, choices, default='HOLD')
    return df
