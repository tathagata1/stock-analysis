import math
import random

import pandas as pd

SIGNAL_TEXT_TO_NUMBER = {
    "STRONG SELL": -2,
    "WEAK SELL": -1,
    "HOLD": 0,
    "WEAK BUY": 1,
    "STRONG BUY": 2,
}
PREDICTION_TRADE_ALLOCATION_BY_SIGNAL = {
    "STRONG BUY": 0.10,
    "WEAK BUY": 0.05,
    "WEAK SELL": 0.05,
    "STRONG SELL": 0.10,
}
EXPLORATORY_BUY_ALLOCATION_BY_SIGNAL = {
    "STRONG BUY": 0.10,
    "WEAK BUY": 0.05,
}
EXPLORATORY_SELL_ALLOCATION_BY_SIGNAL = {
    "STRONG SELL": 0.30,
    "WEAK SELL": 0.15,
}


def _safe_float(value):
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _normalize_signal_text(signal_text):
    return str(signal_text).strip().upper()


def _average_open_close(open_price, close_price):
    open_value = _safe_float(open_price)
    close_value = _safe_float(close_price)
    if open_value is None or close_value is None:
        return None
    return (open_value + close_value) / 2


def get_price_history(df_pred):
    if isinstance(df_pred, tuple):
        df_pred = df_pred[0]
    history = df_pred[['Date', 'Open', 'Close']].copy()
    history['Date'] = pd.to_datetime(history['Date']).dt.tz_localize(None)
    history['Open'] = pd.to_numeric(history['Open'], errors='coerce')
    history['Close'] = pd.to_numeric(history['Close'], errors='coerce')
    history = history.dropna(subset=['Date', 'Open', 'Close']).sort_values('Date').reset_index(drop=True)
    history['Trade_Price'] = (history['Open'] + history['Close']) / 2
    return history


def simulate_prediction_signal_strategy(df_pred, initial_funds):
    if isinstance(df_pred, tuple):
        df_pred = df_pred[0]

    price_history = get_price_history(df_pred)
    if price_history.empty:
        raise ValueError("Price history is empty")

    starting_cash = _safe_float(initial_funds)
    if starting_cash is None or starting_cash < 0:
        raise ValueError("initial_funds must be zero or a positive number")

    signal_history = df_pred[['Date', 'Signal_Text']].copy()
    signal_history['Date'] = pd.to_datetime(signal_history['Date']).dt.tz_localize(None).dt.normalize()
    signal_history['signal_text'] = signal_history['Signal_Text'].map(_normalize_signal_text)
    signal_history['signal_number'] = signal_history['signal_text'].map(SIGNAL_TEXT_TO_NUMBER).fillna(0).astype(int)
    signal_history = signal_history[['Date', 'signal_text', 'signal_number']]

    simulation_frame = price_history.copy()
    simulation_frame['Date'] = pd.to_datetime(simulation_frame['Date']).dt.normalize()
    simulation_frame = simulation_frame.merge(signal_history, on='Date', how='left')
    simulation_frame['signal_text'] = simulation_frame['signal_text'].fillna('HOLD')
    simulation_frame['signal_number'] = simulation_frame['signal_number'].fillna(0).astype(int)

    cash_balance = starting_cash
    stock_units = 0.0
    total_cost = 0.0
    transactions = []
    daily_rows = []

    for _, row in simulation_frame.iterrows():
        trade_price = _average_open_close(row['Open'], row['Close'])
        signal_text = _normalize_signal_text(row['signal_text'])
        action = "HOLD"
        trade_units = 0.0
        trade_value = 0.0

        if signal_text in ("WEAK BUY", "STRONG BUY") and trade_price:
            target_trade_value = starting_cash * PREDICTION_TRADE_ALLOCATION_BY_SIGNAL[signal_text]
            trade_value = min(target_trade_value, cash_balance)
            if trade_value > 0:
                trade_units = trade_value / trade_price
                cash_balance -= trade_value
                stock_units += trade_units
                total_cost += trade_value
                action = "BUY"
        elif signal_text in ("WEAK SELL", "STRONG SELL") and trade_price and stock_units > 0:
            trade_units = min(stock_units * PREDICTION_TRADE_ALLOCATION_BY_SIGNAL[signal_text], stock_units)
            trade_value = trade_units * trade_price
            if trade_units > 0 and trade_value > 0:
                average_cost_before_sale = (total_cost / stock_units) if stock_units > 0 else 0
                cash_balance += trade_value
                stock_units -= trade_units
                total_cost -= average_cost_before_sale * trade_units
                total_cost = max(total_cost, 0.0)
                stock_units = max(stock_units, 0.0)
                action = "SELL"

        holdings_value = stock_units * row['Close']
        portfolio_value = cash_balance + holdings_value
        average_cost_per_unit = (total_cost / stock_units) if stock_units > 0 else 0.0
        profit_loss = portfolio_value - starting_cash
        profit_loss_pct = (profit_loss / starting_cash * 100) if starting_cash else None

        daily_rows.append({
            "Date": row['Date'],
            "Open": row['Open'],
            "Close": row['Close'],
            "Trade_Price": trade_price,
            "signal_text": signal_text,
            "signal_number": row['signal_number'],
            "action": action,
            "trade_units": trade_units,
            "trade_value": trade_value,
            "cash_balance": cash_balance,
            "units_held": stock_units,
            "average_cost_per_unit": average_cost_per_unit,
            "holdings_value": holdings_value,
            "portfolio_value": portfolio_value,
            "profit_loss": profit_loss,
            "profit_loss_pct": profit_loss_pct,
        })

        if action in ("BUY", "SELL"):
            transactions.append({
                "Date": row['Date'],
                "action": action,
                "signal_text": signal_text,
                "trade_price": trade_price,
                "units": trade_units,
                "trade_value": trade_value,
                "cash_balance": cash_balance,
                "units_held": stock_units,
                "portfolio_value": portfolio_value,
            })

    daily_history = pd.DataFrame(daily_rows)
    transactions_frame = pd.DataFrame(transactions)
    latest_row = daily_history.iloc[-1]

    return {
        "price_history": price_history,
        "daily_history": daily_history,
        "transactions": transactions_frame,
        "summary": pd.DataFrame([{
            "start_date": daily_history.iloc[0]['Date'],
            "end_date": latest_row['Date'],
            "initial_funds": starting_cash,
            "ending_cash_balance": latest_row['cash_balance'],
            "units_held": latest_row['units_held'],
            "average_cost_per_unit": latest_row['average_cost_per_unit'],
            "latest_close": latest_row['Close'],
            "holdings_value": latest_row['holdings_value'],
            "total_portfolio_value": latest_row['portfolio_value'],
            "profit_loss": latest_row['profit_loss'],
            "profit_loss_pct": latest_row['profit_loss_pct'],
            "buy_transactions": int((transactions_frame['action'] == 'BUY').sum()) if not transactions_frame.empty else 0,
            "sell_transactions": int((transactions_frame['action'] == 'SELL').sum()) if not transactions_frame.empty else 0,
        }]),
    }


def _simulate_trading(sim_df, initial_funds, stock_units=0, total_cost=0):
    cash_balance = initial_funds
    transaction_log = []

    for _, row in sim_df.iterrows():
        position = row['Signal_Text']
        high, low = row['High'], row['Low']
        random_price = random.uniform(low, high)

        if position in EXPLORATORY_BUY_ALLOCATION_BY_SIGNAL:
            allocation = EXPLORATORY_BUY_ALLOCATION_BY_SIGNAL[position] * cash_balance
            units_to_buy = allocation / random_price
            if cash_balance >= allocation:
                cash_balance -= allocation
                stock_units += units_to_buy
                total_cost += allocation
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_buy, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position in EXPLORATORY_SELL_ALLOCATION_BY_SIGNAL:
            units_to_sell = stock_units * EXPLORATORY_SELL_ALLOCATION_BY_SIGNAL[position]
            if units_to_sell > 0:
                average_cost = (total_cost / stock_units) if stock_units > 0 else 0
                proceeds = units_to_sell * random_price
                cash_balance += proceeds
                stock_units -= units_to_sell
                total_cost -= average_cost * units_to_sell
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_sell, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        else:
            transaction_log.append({"date": row['Date'], "position": position, "units": "N/A", "random_price": "N/A", "cash_balance": cash_balance, "units_held": stock_units})

    closing_price = sim_df['Close'].iloc[-1]
    unrealized_gains_losses = stock_units * closing_price
    avg_price_per_unit = (total_cost / stock_units) if stock_units > 0 else 0
    invested_value = stock_units * avg_price_per_unit
    unrealised_gains_losses_percentage = 0
    if invested_value != 0:
        unrealised_gains_losses_percentage = (unrealized_gains_losses - invested_value) / invested_value * 100
        if math.isnan(unrealised_gains_losses_percentage):
            unrealised_gains_losses_percentage = 0

    return {
        "closing_stock_price": closing_price,
        "final_cash_balance": round(cash_balance, 2),
        "unrealized_gains_losses": round(unrealized_gains_losses, 2),
        "unrealized_gain_loss_%": round(unrealised_gains_losses_percentage,2),
        "units_held": round(stock_units, 4),
        "average_price_per_unit": round(avg_price_per_unit, 2),
        "transactions": transaction_log
    }


def simulate_exploratory_trading(file_to_sim_df, start_iloc, end_iloc, initial_funds):
    sim_df = file_to_sim_df.iloc[start_iloc:start_iloc + end_iloc][::-1]
    return _simulate_trading(sim_df, initial_funds)


def simulate_portfolio_trades(file_to_sim_df, start_iloc, end_iloc, initial_funds, units_held, avg_price):
    sim_df = file_to_sim_df.iloc[start_iloc:start_iloc + end_iloc][::-1]
    return _simulate_trading(sim_df, initial_funds, stock_units=units_held, total_cost=avg_price * units_held)
