import numpy as np

import analysis_functions.fundamental_analysis as fundamental_analysis
import analysis_functions.technical_analysis as technical_analysis
import analysis_functions.sentiment_analysis as sentiment_analysis

import config.config as config
from config.logging_config import get_logger

logger = get_logger(__name__)

def get_prediction(df, stats=None, include_sentiment=True):
    ticker = df["TICKER"].iloc[0] if not df.empty and "TICKER" in df.columns else "UNKNOWN"
    logger.info("Building prediction frame. ticker=%s include_sentiment=%s rows=%s", ticker, include_sentiment, len(df))
    df = technical_analysis.get_technical_analysis_calculations(df)
    df['technical_analysis_buy_score'] = df.apply(technical_analysis.calculate_buy_score, axis=1)
    df['technical_analysis_sell_score'] = df.apply(technical_analysis.calculate_sell_score, axis=1)
    df['fundamental_analysis_score'] = fundamental_analysis.get_fundamental_analysis(stats)
    df['sentiment_analysis_score'] = sentiment_analysis.apply_sentiment_analysis(ticker) if include_sentiment else 0
    logger.info("Built prediction frame successfully. ticker=%s", ticker)
    return df


def add_total_signal(df):
    ticker = df["TICKER"].iloc[0] if not df.empty and "TICKER" in df.columns else "UNKNOWN"
    logger.info("Calculating total signal. ticker=%s", ticker)
    technical_signal = (df['technical_analysis_buy_score'] + df['technical_analysis_sell_score']) * config.TECHNICAL_SIGNAL_WEIGHT
    sentiment_signal = df['sentiment_analysis_score'] * config.SENTIMENT_SIGNAL_WEIGHT
    fundamental_signal = df['fundamental_analysis_score'] * config.FUNDAMENTAL_SIGNAL_WEIGHT

    df['Signal'] = (
        (technical_signal * config.TECHNICAL_SIGNAL_WEIGHT)
        + (sentiment_signal * config.SENTIMENT_SIGNAL_WEIGHT)
        + (fundamental_signal * config.FUNDAMENTAL_SIGNAL_WEIGHT)
    )
    logger.info("Calculated total signal successfully. ticker=%s", ticker)
    return df


def convert_signal_to_text(df):
    ticker = df["TICKER"].iloc[0] if not df.empty and "TICKER" in df.columns else "UNKNOWN"
    conditions = [
        (df['Signal'] <= config.STRONG_SELL_THRESHOLD),
        (df['Signal'] > config.STRONG_SELL_THRESHOLD) & (df['Signal'] < config.WEAK_SELL_THRESHOLD),
        (df['Signal'] >= config.WEAK_SELL_THRESHOLD) & (df['Signal'] < config.WEAK_BUY_THRESHOLD),
        (df['Signal'] >= config.WEAK_BUY_THRESHOLD) & (df['Signal'] < config.STRONG_BUY_THRESHOLD),
        (df['Signal'] >= config.STRONG_BUY_THRESHOLD),
    ]
    choices = ['STRONG SELL', 'WEAK SELL', 'HOLD', 'WEAK BUY', 'STRONG BUY']
    df['Signal_Text'] = np.select(conditions, choices, default='HOLD')
    logger.info("Converted numeric signals to text labels. ticker=%s", ticker)
    return df
