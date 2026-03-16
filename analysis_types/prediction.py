import numpy as np

import analysis_functions.fundamental_analysis as fundamental_analysis
import analysis_functions.technical_analysis as technical_analysis
import analysis_functions.sentiment_analysis as sentiment_analysis

import config

SIGNAL_MAPPING = {"STRONG SELL": -2,"WEAK SELL": -1,"HOLD": 0,"WEAK BUY": 1,"STRONG BUY": 2,}

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

def get_weighted_signal(signals):
    if len(signals) == 0:
        return "HOLD", 0
    reverse_mapping = {value: key for key, value in SIGNAL_MAPPING.items()}
    numeric_signals = signals.map(SIGNAL_MAPPING)
    weights = np.array([0.5 ** i for i in range(len(numeric_signals))])
    normalized_weights = weights / weights.sum()
    weighted_avg = np.dot(numeric_signals, normalized_weights)
    closest_signal = round(weighted_avg)
    return reverse_mapping[closest_signal], closest_signal
