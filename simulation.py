import random
import math

strong_buy_allocation = 0.10
weak_buy_allocation = 0.05
strong_sell_allocation = 0.30
weak_sell_allocation = 0.15


def simulate_exploratory_trading(file_to_sim_df, start_iloc, end_iloc, initial_funds):
    sim_df = file_to_sim_df.iloc[start_iloc:start_iloc+end_iloc][::-1]
    
    # Initialize variables
    cash_balance = initial_funds  # Cash balance
    stock_units = 0  # Total units held
    total_cost = 0  # Total cost of acquired shares (used to calculate average price)
    transaction_log = []  # To keep track of transactions

    # Iterate through the rows of the reversed DataFrame
    for _, row in sim_df.iterrows():
        position = row['Signal_Text']
        high, low = row['High'], row['Low']

        # Random price between High and Low
        random_price = random.uniform(low, high)

        # Execute actions based on Signal_Text
        if position == 'STRONG BUY':
            allocation = strong_buy_allocation * cash_balance  # Allocate 10% of cash balance
            units_to_buy = allocation / random_price  # Fractional units
            if cash_balance >= allocation:
                cash_balance -= allocation
                stock_units += units_to_buy
                total_cost += allocation  # Add the cost to total cost
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_buy, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position == 'WEAK BUY':
            allocation = weak_buy_allocation * cash_balance  # Allocate 5% of cash balance
            units_to_buy = allocation / random_price  # Fractional units
            if cash_balance >= allocation:
                cash_balance -= allocation
                stock_units += units_to_buy
                total_cost += allocation  # Add the cost to total cost
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_buy, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position == 'WEAK SELL':
            units_to_sell = stock_units * weak_sell_allocation  # Sell 5% of current holdings
            if stock_units >= units_to_sell:
                proceeds = units_to_sell * random_price
                cash_balance += proceeds
                stock_units -= units_to_sell
                total_cost -= (units_to_sell / stock_units) * total_cost if stock_units > 0 else total_cost
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_sell, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position == 'STRONG SELL':
            units_to_sell = stock_units * strong_sell_allocation  # Sell 10% of current holdings
            if stock_units >= units_to_sell:
                proceeds = units_to_sell * random_price
                cash_balance += proceeds
                stock_units -= units_to_sell
                total_cost -= (units_to_sell / stock_units) * total_cost if stock_units > 0 else total_cost
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_sell, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position == 'HOLD':
            transaction_log.append({"date": row['Date'], "position": position, "units": "N/A", "random_price": "N/A", "cash_balance": cash_balance, "units_held": stock_units})


    # Calculate final cash balance and unrealized gains/losses
    closing_price = sim_df['Close'].iloc[-1]  # Closing price of the last day
    unrealized_gains_losses = stock_units * closing_price  # Value of stocks held
    avg_price_per_unit = (total_cost / stock_units) if stock_units > 0 else 0
    unrealised_gains_losses_percentage = (unrealized_gains_losses-(stock_units*avg_price_per_unit))/((stock_units*avg_price_per_unit))*100
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

def simulate_portfolio_trades(file_to_sim_df, start_iloc, end_iloc, initial_funds, units_held, avg_price):
    sim_df = file_to_sim_df.iloc[start_iloc:start_iloc+end_iloc][::-1]
    
    # Initialize variables
    cash_balance = initial_funds  # Cash balance
    stock_units = units_held  # Total units held
    total_cost = avg_price * units_held # Total cost of acquired shares (used to calculate average price)
    transaction_log = []  # To keep track of transactions

    # Iterate through the rows of the reversed DataFrame
    for _, row in sim_df.iterrows():
        position = row['Signal_Text']
        high, low = row['High'], row['Low']

        # Random price between High and Low
        random_price = random.uniform(low, high)

        # Execute actions based on Signal_Text
        if position == 'STRONG BUY':
            allocation = strong_buy_allocation * cash_balance  # Allocate 10% of cash balance
            units_to_buy = allocation / random_price  # Fractional units
            if cash_balance >= allocation:
                cash_balance -= allocation
                stock_units += units_to_buy
                total_cost += allocation  # Add the cost to total cost
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_buy, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position == 'WEAK BUY':
            allocation = weak_buy_allocation * cash_balance  # Allocate 5% of cash balance
            units_to_buy = allocation / random_price  # Fractional units
            if cash_balance >= allocation:
                cash_balance -= allocation
                stock_units += units_to_buy
                total_cost += allocation  # Add the cost to total cost
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_buy, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position == 'WEAK SELL':
            units_to_sell = stock_units * weak_sell_allocation  # Sell 5% of current holdings
            if stock_units >= units_to_sell:
                proceeds = units_to_sell * random_price
                cash_balance += proceeds
                stock_units -= units_to_sell
                total_cost -= (units_to_sell / stock_units) * total_cost if stock_units > 0 else total_cost
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_sell, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position == 'STRONG SELL':
            units_to_sell = stock_units * strong_sell_allocation  # Sell 10% of current holdings
            if stock_units >= units_to_sell:
                proceeds = units_to_sell * random_price
                cash_balance += proceeds
                stock_units -= units_to_sell
                total_cost -= (units_to_sell / stock_units) * total_cost if stock_units > 0 else total_cost
                transaction_log.append({"date": row['Date'], "position": position, "units": units_to_sell, "random_price": random_price, "cash_balance": cash_balance, "units_held": stock_units})
        elif position == 'HOLD':
            transaction_log.append({"date": row['Date'], "position": position, "units": "N/A", "random_price": "N/A", "cash_balance": cash_balance, "units_held": stock_units})

    # Calculate final cash balance and unrealized gains/losses
    closing_price = sim_df['Close'].iloc[-1]  # Closing price of the last day
    unrealized_gains_losses = stock_units * closing_price  # Value of stocks held
    avg_price_per_unit = (total_cost / stock_units) if stock_units > 0 else 0
    unrealised_gains_losses_percentage = (unrealized_gains_losses-(stock_units*avg_price_per_unit))/((stock_units*avg_price_per_unit))*100
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