import config
import pandas as pd

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
    data['HL'] = data['High'] - data['Low']
    data['HC'] = abs(data['High'] - data['Close'].shift(1))
    data['LC'] = abs(data['Low'] - data['Close'].shift(1))
    data['TR'] = data[['HL', 'HC', 'LC']].max(axis=1)
    data1 = data['TR'].rolling(window=period).mean()
    return data1

#Volume Indicators
def high_volume(df):
    avg_volume = df['Volume'].mean()
    high_volume = df['Volume'] > (1.5 * avg_volume)
    return high_volume

def vwap(df): #Volume-Weighted Average Price
    q = df['Volume']
    p = df['Close']
    vwap_value = (q * p).cumsum() / q.cumsum()
    return vwap_value

# Calculation and Data Preparation Function
def get_technical_analysis_calculations(df):
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
    df['VWAP'] = vwap(df)
    
    # Calculate other stuff
    df['Daily_Return'] = df['Close'].pct_change()
    df['Cumulative_Return'] = (1 + df['Daily_Return']).cumprod() - 1
    df['Daily Gain/Loss']=df["Close"] - df["Open"]
    return df

# Technical Analysis functions
def calculate_buy_score(row):
    score = 0
    if row['RSI'] < config.rsi_buy and row['SMA' + str(config.sma1)] > row['SMA' + str(config.sma2)]:
        score += 0.3
    if row['RSI'] < config.rsi_buy and row['EMA' + str(config.ema1)] > row['EMA' + str(config.ema2)]:
        score += 0.3
    if row['Close'] < row['VWAP']:
        score += 0.15
    if row['%K'] < config.stoc_buy and row['%D'] < config.stoc_buy:
        score += 0.15
    if row['Close'] < row['Lower_Band']:
        score += 0.03
    if row['MACD'] > row['Signal_Line']:
        score += 0.03
    if row['High_Volume']:
        score += 0.02
    if row['ATR'] > config.atr:
        score += 0.02
    return score


def calculate_sell_score(row):
    score = 0
    if row['RSI'] > config.rsi_sell and row['SMA' + str(config.sma1)] < row['SMA' + str(config.sma2)]:
        score -= 0.3
    if row['RSI'] > config.rsi_sell and row['EMA' + str(config.ema1)] < row['EMA' + str(config.ema2)]:
        score -= 0.3
    if row['Close'] > row['VWAP']:
        score -= 0.15
    if row['%K'] > config.stoc_sell and row['%D'] > config.stoc_sell:
        score -= 0.15
    if row['Close'] > row['Upper_Band']:
        score -= 0.03
    if row['MACD'] < row['Signal_Line']:
        score -= 0.03
    if not row['High_Volume']:
        score -= 0.02
    if not row['ATR'] > config.atr:
        score -= 0.02
    return score
