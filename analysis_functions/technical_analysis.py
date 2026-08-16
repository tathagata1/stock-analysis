import config.config as config
from config.logging_config import get_logger
import numpy as np
import pandas as pd

logger = get_logger(__name__)

#Momentum Indicators
def rsi(data, window=config.rsi_period): #Relative Strength Index
    delta = data['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def stochastic_oscillator(df, k_window=config.stochastic_k, d_window=config.stochastic_d):
    min_low = df['Low'].rolling(window=k_window).min()
    max_high = df['High'].rolling(window=k_window).max()
    k_value = 100 * (df['Close'] - min_low) / (max_high - min_low)
    d_value = k_value.rolling(window=d_window).mean()
    return k_value, d_value

#Trend Indicators
def sma(df, days): #Simple Moving Average
    sma = df['Close'].rolling(window=days).mean()
    return sma

def ema(df, days): #Exponential Moving Average
    ema = df['Close'].ewm(span=days, adjust=False).mean()
    return ema

def macd(df): #Moving Average Convergence Divergence
    ema_12 = ema(df, config.fast_period)
    ema_26 = ema(df, config.slow_period)
    macd_line = ema_12 - ema_26
    signal_line = macd_line.ewm(span=config.signal_period, adjust=False).mean()
    return macd_line, signal_line

#Volatility Indicators
def bollinger_bands(df, window=config.bolinger_period):
    middle_band = sma(df, window)
    upper_band = middle_band + (df['Close'].rolling(window).std() * 2)
    lower_band = middle_band - (df['Close'].rolling(window).std() * 2)
    return middle_band, upper_band, lower_band

def atr(data, period=config.atr_period): #Average True Range
    prior_close = data['Close'].shift(1)
    true_range = pd.concat(
        [
            data['High'] - data['Low'],
            (data['High'] - prior_close).abs(),
            (data['Low'] - prior_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(window=period, min_periods=period).mean()

#Volume Indicators
def high_volume(
    df,
    window=config.VOLUME_LOOKBACK_PERIOD,
    multiplier=config.HIGH_VOLUME_MULTIPLIER,
):
    """Flag volume relative to the *prior* trailing average.

    Shifting the average by one session makes historical flags prefix-invariant:
    adding future rows cannot change an earlier classification.
    """
    prior_average = (
        pd.to_numeric(df['Volume'], errors='coerce')
        .shift(1)
        .rolling(window=window, min_periods=window)
        .mean()
    )
    result = (df['Volume'] > multiplier * prior_average).astype('boolean')
    return result.where(prior_average.notna(), pd.NA)

def vwap(df, window=config.VWAP_LOOKBACK_PERIOD):
    """Return a rolling daily-bar volume-weighted average price.

    This is intentionally a rolling daily approximation, not intraday session
    VWAP.  A fixed window avoids dependence on the requested history start date.
    """
    volume = pd.to_numeric(df['Volume'], errors='coerce')
    close = pd.to_numeric(df['Close'], errors='coerce')
    weighted = (volume * close).rolling(window, min_periods=window).sum()
    volume_sum = volume.rolling(window, min_periods=window).sum()
    return weighted / volume_sum.replace(0, np.nan)


def _has_values(row, *columns):
    return all(pd.notna(row.get(column)) for column in columns)

# Technical Analysis functions
def calculate_buy_score(row):
    availability = row.get('technical_data_available', True)
    if pd.notna(availability) and not bool(availability):
        return np.nan
    score = 0
    if pd.notna(row.get('RSI')) and row['RSI'] < config.rsi_buy:
        score += 0.2
    elif pd.notna(row.get('RSI')) and row['RSI'] < (config.rsi_buy + 10):
        score += 0.1
    if _has_values(row, 'SMA' + str(config.sma1), 'SMA' + str(config.sma2)) and row['SMA' + str(config.sma1)] > row['SMA' + str(config.sma2)]:
        score += 0.2
    if _has_values(row, 'EMA' + str(config.ema1), 'EMA' + str(config.ema2)) and row['EMA' + str(config.ema1)] > row['EMA' + str(config.ema2)]:
        score += 0.2
    if _has_values(row, 'Close', 'VWAP') and row['Close'] < row['VWAP']:
        score += 0.1
    if _has_values(row, '%K', '%D') and row['%K'] < config.stoc_buy and row['%D'] < config.stoc_buy:
        score += 0.1
    if _has_values(row, 'Close', 'Lower_Band') and row['Close'] < row['Lower_Band']:
        score += 0.1
    if _has_values(row, 'MACD', 'Signal_Line') and row['MACD'] > row['Signal_Line']:
        score += 0.15
    if pd.notna(row.get('High_Volume')) and bool(row['High_Volume']):
        score += 0.05
    if pd.notna(row.get('ATR_Pct')) and row['ATR_Pct'] > config.ATR_PERCENT_THRESHOLD:
        score += 0.05
    return score

def calculate_sell_score(row):
    availability = row.get('technical_data_available', True)
    if pd.notna(availability) and not bool(availability):
        return np.nan
    score = 0
    if pd.notna(row.get('RSI')) and row['RSI'] > config.rsi_sell:
        score -= 0.2
    elif pd.notna(row.get('RSI')) and row['RSI'] > (config.rsi_sell - 10):
        score -= 0.1
    if _has_values(row, 'SMA' + str(config.sma1), 'SMA' + str(config.sma2)) and row['SMA' + str(config.sma1)] < row['SMA' + str(config.sma2)]:
        score -= 0.2
    if _has_values(row, 'EMA' + str(config.ema1), 'EMA' + str(config.ema2)) and row['EMA' + str(config.ema1)] < row['EMA' + str(config.ema2)]:
        score -= 0.2
    if _has_values(row, 'Close', 'VWAP') and row['Close'] > row['VWAP']:
        score -= 0.1
    if _has_values(row, '%K', '%D') and row['%K'] > config.stoc_sell and row['%D'] > config.stoc_sell:
        score -= 0.1
    if _has_values(row, 'Close', 'Upper_Band') and row['Close'] > row['Upper_Band']:
        score -= 0.1
    if _has_values(row, 'MACD', 'Signal_Line') and row['MACD'] < row['Signal_Line']:
        score -= 0.15
    if pd.notna(row.get('High_Volume')) and not bool(row['High_Volume']):
        score -= 0.05
    if pd.notna(row.get('ATR_Pct')) and row['ATR_Pct'] <= config.ATR_PERCENT_THRESHOLD:
        score -= 0.05
    return score

# Calculation and Data Preparation Function
def get_technical_analysis_calculations(df):
    ticker = df['TICKER'].iloc[0] if not df.empty and 'TICKER' in df.columns else "UNKNOWN"
    logger.info("Calculating technical indicators. ticker=%s rows=%s", ticker, len(df))
    if df.empty:
        return df.copy()

    # Ensure numeric conversion with error handling
    cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    
    # Calculate technical indicators
    df['SMA' + str(config.sma1)] = sma(df, config.sma1)
    df['SMA' + str(config.sma2)] = sma(df, config.sma2)
    df['EMA' + str(config.ema1)] = ema(df, config.ema1)
    df['EMA' + str(config.ema2)] = ema(df, config.ema2)
    df['RSI'] = rsi(df)
    df['High_Volume'] = high_volume(df)
    df['Middle_Band'], df['Upper_Band'], df['Lower_Band'] = bollinger_bands(df)
    df['MACD'], df['Signal_Line'] = macd(df)
    df['%K'], df['%D'] = stochastic_oscillator(df)
    df['ATR'] = atr(df)
    df['ATR_Pct'] = df['ATR'] / df['Close'].replace(0, np.nan) * 100
    df['VWAP'] = vwap(df)
    warmup_complete = pd.Series(
        np.arange(len(df)) >= config.TECHNICAL_MIN_HISTORY - 1,
        index=df.index,
    )
    required = [
        'RSI',
        'SMA' + str(config.sma2),
        'EMA' + str(config.ema2),
        'Middle_Band',
        '%D',
        'ATR_Pct',
        'VWAP',
        'High_Volume',
    ]
    df['technical_data_available'] = warmup_complete & df[required].notna().all(axis=1)
    
    # Calculate other stuff
    df['Daily_Return'] = df['Close'].pct_change()
    df['Cumulative_Return'] = (1 + df['Daily_Return']).cumprod() - 1
    df['Daily Gain/Loss']=df["Close"] - df["Open"]
    logger.info("Calculated technical indicators successfully. ticker=%s", ticker)
    return df
