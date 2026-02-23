import numpy as np

def calculate_sharpe_ratio(daily_returns, risk_free_rate=0):
    """
    Calculate Sharpe Ratio.
    """
    if len(daily_returns) < 2 or np.std(daily_returns) == 0:
        return np.nan
    return (np.mean(daily_returns - risk_free_rate) / np.std(daily_returns)) * np.sqrt(252)

def calculate_sortino_ratio(daily_returns, risk_free_rate=0):
    """
    Calculate Sortino Ratio.
    """
    negative_returns = daily_returns[daily_returns < 0]
    downside_deviation = np.std(negative_returns)
    if len(daily_returns) < 2 or downside_deviation == 0:
        return np.nan
    return (np.mean(daily_returns - risk_free_rate) / downside_deviation) * np.sqrt(252)

def calculate_drawdown(cumulative_returns):
    """
    Calculate Drawdown and Max Drawdown.
    """
    if cumulative_returns.empty:
        return np.nan, np.nan
    drawdown = cumulative_returns - cumulative_returns.cummax()
    max_drawdown = drawdown.min()
    return drawdown, max_drawdown

def calculate_max_drawdown(cumulative_returns):
    """
    Calculate Max Drawdown only.
    """
    if cumulative_returns.empty:
        return np.nan
    _, max_drawdown = calculate_drawdown(cumulative_returns)
    return max_drawdown

def calculate_calmar_ratio(cumulative_returns):
    """
    Calculate Calmar Ratio.
    """
    if cumulative_returns.empty:
        return np.nan
    max_drawdown = calculate_max_drawdown(cumulative_returns)
    annual_return = cumulative_returns.iloc[-1]
    return annual_return / abs(max_drawdown) if max_drawdown != 0 else np.nan

def calculate_trade_metrics(df):
    """
    Calculate trade metrics: Total Trades, Avg Trade Return, Profit Factor.
    """
    trades = df[df['Backtest_Position'] != df['Backtest_Position'].shift(1)]
    if trades.empty:
        return 0, np.nan, np.nan
    avg_trade_return = trades['Backtest_Strategy_Return'].mean()
    positive_return_sum = trades[trades['Backtest_Strategy_Return'] > 0]['Backtest_Strategy_Return'].sum()
    negative_return_sum = abs(trades[trades['Backtest_Strategy_Return'] < 0]['Backtest_Strategy_Return'].sum())
    profit_factor = positive_return_sum / negative_return_sum if negative_return_sum != 0 else np.nan
    return len(trades), avg_trade_return, profit_factor

# performance
#Excellent
#Good
#Acceptable
#Poor
#Unacceptable
def interpret_cumulative_return(value):
    if value > 0.5:
        return "Positive: Excellent"
    elif value > 0:
        return "Positive: Good"
    else:
        return "Negative: Poor"

# risk-adjusted return
def interpret_sharpe_ratio(value):
    if value > 2:
        return "Positive: Excellent"
    elif value > 1:
        return "Positive: Good"
    elif value > 0:
        return "Positive: Bad"
    else:
        return "Negative: Poor"

#return with limited downside risk
def interpret_sortino_ratio(value):
    if value > 2:
        return "Positive: Excellent"
    elif value > 1:
        return "Positive: Good"
    elif value > 0:
        return "Negative: Poor"
    else:
        return "Negative: Unacceptable"

#drawdown
def interpret_max_drawdown(value):
    if value > -0.1:
        return "Positive: Excellent"
    elif value > -0.2:
        return "Positive: Good"
    else:
        return "Negative: Poor"

#performance relative to risk
def interpret_calmar_ratio(value):
    if value > 3:
        return "Positive: Excellent "
    elif value > 1:
        return "Positive: Good"
    elif value > 0:
        return "Positive: Acceptable"
    else:
        return "Negative: Poor"

# Main backtesting function
def backtest(df):
    # Calculate strategy returns directly from Signal
    df['Backtest_Strategy_Return'] = df['Daily_Return'] * df['Signal'].shift(1)
    df['Backtest_Cumulative_Strategy_Return'] = (1 + df['Backtest_Strategy_Return']).cumprod() - 1

    # Calculate key metrics
    cumulative_return = df['Backtest_Cumulative_Strategy_Return'].iloc[-1] if not df.empty else np.nan
    sharpe_ratio = calculate_sharpe_ratio(df['Backtest_Strategy_Return'])
    sortino_ratio = calculate_sortino_ratio(df['Backtest_Strategy_Return'])
    max_drawdown = calculate_max_drawdown(df['Backtest_Cumulative_Strategy_Return'])
    calmar_ratio = calculate_calmar_ratio(df['Backtest_Cumulative_Strategy_Return'])

    return df, interpret_cumulative_return(cumulative_return), interpret_sharpe_ratio(sharpe_ratio), interpret_sortino_ratio(sortino_ratio), interpret_max_drawdown(max_drawdown), interpret_calmar_ratio(calmar_ratio)