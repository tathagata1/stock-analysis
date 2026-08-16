"""Long-only price-intent backtesting engine."""

import numpy as np
import pandas as pd

from .data import normalize_history


TRANSACTION_COLUMNS = [
    "Date",
    "action",
    "reason",
    "raw_price",
    "fill_price",
    "shares",
    "gross_value",
    "commission",
    "cash_after",
    "realized_pnl",
    "entry_number",
]
ROUND_TRIP_COLUMNS = [
    "entry_number",
    "entry_date",
    "exit_date",
    "entry_price",
    "exit_price",
    "shares",
    "exit_reason",
    "holding_sessions",
    "pnl",
    "return_pct",
]


def _validate_backtest_inputs(
    history,
    entry_limit,
    target,
    fixed_stop,
    capital,
    size_pct,
    share_limit,
    entries_limit,
    wait_days,
    minimum_holding_days,
    holding_limit,
    trailing_pct,
    fee,
    slip_bps,
    priority,
):
    if history.empty:
        raise ValueError("Price history is empty")
    if entry_limit is None or float(entry_limit) <= 0:
        raise ValueError("buy_price must be greater than zero")
    if target is not None and float(target) <= float(entry_limit):
        raise ValueError("sell_price must be above buy_price for this long-only backtest")
    if fixed_stop is not None and float(fixed_stop) >= float(entry_limit):
        raise ValueError("stop_loss must be below buy_price for this long-only backtest")
    if float(capital) <= 0:
        raise ValueError("initial_capital must be greater than zero")
    if not 0 < float(size_pct) <= 1:
        raise ValueError("position_size_pct must be greater than zero and at most 1")
    if share_limit is not None and float(share_limit) <= 0:
        raise ValueError("max_shares must be greater than zero or None")
    if int(entries_limit) < 1 or int(wait_days) < 0:
        raise ValueError("max_entries must be at least 1 and cooldown_days cannot be negative")
    if minimum_holding_days is not None and int(minimum_holding_days) < 0:
        raise ValueError("min_holding_days must be non-negative or None")
    if holding_limit is not None and int(holding_limit) < 1:
        raise ValueError("max_holding_days must be at least 1 or None")
    if (
        minimum_holding_days is not None
        and holding_limit is not None
        and int(minimum_holding_days) > int(holding_limit)
    ):
        raise ValueError("min_holding_days cannot exceed max_holding_days")
    if trailing_pct is not None and not 0 < float(trailing_pct) < 1:
        raise ValueError("trailing_stop_pct must be between 0 and 1 or None")
    if float(fee) < 0 or float(slip_bps) < 0:
        raise ValueError("commission_per_order and slippage_bps cannot be negative")
    if priority not in {"stop", "target"}:
        raise ValueError('same_day_exit_priority must be either "stop" or "target"')


def _buy_fill(open_price, limit_price, slip_bps):
    raw_price = min(float(open_price), float(limit_price))
    fill_price = min(float(limit_price), raw_price * (1 + float(slip_bps) / 10_000))
    return raw_price, fill_price


def _target_fill(open_price, target_price, slip_bps):
    raw_price = max(float(open_price), float(target_price))
    fill_price = max(float(target_price), raw_price * (1 - float(slip_bps) / 10_000))
    return raw_price, fill_price


def _stop_fill(open_price, stop_price, slip_bps):
    raw_price = min(float(open_price), float(stop_price))
    fill_price = raw_price * (1 - float(slip_bps) / 10_000)
    return raw_price, fill_price


def _market_fill(close_price, side, slip_bps):
    multiplier = (
        1 + float(slip_bps) / 10_000
        if side == "buy"
        else 1 - float(slip_bps) / 10_000
    )
    return float(close_price), float(close_price) * multiplier


def _aligned_benchmark_equity(dates, benchmark, capital):
    benchmark_close = benchmark.set_index("Date")["Close"].sort_index()
    aligned = benchmark_close.reindex(pd.DatetimeIndex(dates), method="ffill")
    if aligned.notna().any():
        first_value = aligned.dropna().iloc[0]
        return aligned / first_value * float(capital)
    return pd.Series(np.nan, index=pd.DatetimeIndex(dates))


def run_price_intent_backtest(
    history,
    benchmark,
    entry_limit,
    target=None,
    fixed_stop=None,
    capital=10_000,
    size_pct=1.0,
    share_limit=None,
    use_whole_shares=True,
    entries_limit=1,
    reentry=True,
    wait_days=0,
    holding_limit=None,
    trailing_pct=None,
    fee=0.0,
    slip_bps=0.0,
    priority="stop",
    liquidate_at_end=True,
    minimum_holding_days=None,
):
    """Run the long-only backtest and return results as pandas data frames."""
    prices = normalize_history(history)
    _validate_backtest_inputs(
        prices,
        entry_limit,
        target,
        fixed_stop,
        capital,
        size_pct,
        share_limit,
        entries_limit,
        wait_days,
        minimum_holding_days,
        holding_limit,
        trailing_pct,
        fee,
        slip_bps,
        priority,
    )

    cash = float(capital)
    shares = 0.0
    entries = 0
    next_entry_index = 0
    position = None
    transactions = []
    round_trips = []
    equity_rows = []

    for row_index, row in prices.iterrows():
        date = row["Date"]
        action_today = "HOLD"
        active_stop = np.nan

        # Exits are evaluated before new entries, and never on the entry candle.
        if position is not None and row_index > position["entry_index"]:
            trailing_level = (
                position["highest_completed_high"] * (1 - float(trailing_pct))
                if trailing_pct is not None
                else None
            )
            stop_candidates = [
                value for value in [fixed_stop, trailing_level] if value is not None
            ]
            active_stop = max(stop_candidates) if stop_candidates else np.nan
            target_hit = target is not None and row["High"] >= float(target)
            stop_hit = pd.notna(active_stop) and row["Low"] <= active_stop
            holding_sessions = row_index - position["entry_index"]
            minimum_hold_met = (
                minimum_holding_days is None
                or holding_sessions >= int(minimum_holding_days)
            )

            target_hit = minimum_hold_met and target_hit
            stop_hit = minimum_hold_met and stop_hit

            exit_reason = None
            raw_exit = None
            fill_exit = None
            if target_hit and stop_hit:
                if priority == "stop":
                    exit_reason = (
                        "TRAILING_STOP"
                        if trailing_level is not None and active_stop == trailing_level
                        else "STOP_LOSS"
                    )
                    raw_exit, fill_exit = _stop_fill(row["Open"], active_stop, slip_bps)
                else:
                    exit_reason = "TARGET"
                    raw_exit, fill_exit = _target_fill(row["Open"], target, slip_bps)
            elif stop_hit:
                exit_reason = (
                    "TRAILING_STOP"
                    if trailing_level is not None and active_stop == trailing_level
                    else "STOP_LOSS"
                )
                raw_exit, fill_exit = _stop_fill(row["Open"], active_stop, slip_bps)
            elif target_hit:
                exit_reason = "TARGET"
                raw_exit, fill_exit = _target_fill(row["Open"], target, slip_bps)
            elif (
                minimum_hold_met
                and holding_limit is not None
                and holding_sessions >= int(holding_limit)
            ):
                exit_reason = "TIME_LIMIT"
                raw_exit, fill_exit = _market_fill(row["Close"], "sell", slip_bps)
            elif liquidate_at_end and row_index == len(prices) - 1:
                exit_reason = "END_OF_PERIOD"
                raw_exit, fill_exit = _market_fill(row["Close"], "sell", slip_bps)

            if exit_reason is not None:
                gross_value = shares * fill_exit
                cash += gross_value - float(fee)
                pnl = gross_value - float(fee) - position["entry_cost"]
                return_pct = pnl / position["entry_cost"] * 100
                transactions.append(
                    {
                        "Date": date,
                        "action": "SELL",
                        "reason": exit_reason,
                        "raw_price": raw_exit,
                        "fill_price": fill_exit,
                        "shares": shares,
                        "gross_value": gross_value,
                        "commission": float(fee),
                        "cash_after": cash,
                        "realized_pnl": pnl,
                        "entry_number": entries,
                    }
                )
                round_trips.append(
                    {
                        "entry_number": entries,
                        "entry_date": position["entry_date"],
                        "exit_date": date,
                        "entry_price": position["entry_price"],
                        "exit_price": fill_exit,
                        "shares": shares,
                        "exit_reason": exit_reason,
                        "holding_sessions": holding_sessions,
                        "pnl": pnl,
                        "return_pct": return_pct,
                    }
                )
                shares = 0.0
                position = None
                next_entry_index = row_index + int(wait_days) + 1
                action_today = f"SELL: {exit_reason}"

        can_enter = (
            position is None
            and action_today == "HOLD"
            and row_index >= next_entry_index
            and entries < int(entries_limit)
            and (reentry or entries == 0)
            and row["Low"] <= float(entry_limit)
        )
        if can_enter:
            raw_entry, fill_entry = _buy_fill(row["Open"], entry_limit, slip_bps)
            available_for_shares = max(0.0, cash * float(size_pct) - float(fee))
            quantity = available_for_shares / fill_entry
            if share_limit is not None:
                quantity = min(quantity, float(share_limit))
            if use_whole_shares:
                quantity = float(np.floor(quantity))

            entry_cost = quantity * fill_entry + float(fee)
            if quantity > 0 and entry_cost <= cash + 1e-9:
                cash -= entry_cost
                shares = quantity
                entries += 1
                position = {
                    "entry_index": row_index,
                    "entry_date": date,
                    "entry_price": fill_entry,
                    "entry_cost": entry_cost,
                    "highest_completed_high": float(row["High"]),
                }
                transactions.append(
                    {
                        "Date": date,
                        "action": "BUY",
                        "reason": "BUY_LIMIT",
                        "raw_price": raw_entry,
                        "fill_price": fill_entry,
                        "shares": quantity,
                        "gross_value": quantity * fill_entry,
                        "commission": float(fee),
                        "cash_after": cash,
                        "realized_pnl": np.nan,
                        "entry_number": entries,
                    }
                )
                action_today = "BUY: BUY_LIMIT"

        # If the last-session entry could not be evaluated for an exit, liquidate at its close.
        if position is not None and liquidate_at_end and row_index == len(prices) - 1:
            raw_exit, fill_exit = _market_fill(row["Close"], "sell", slip_bps)
            gross_value = shares * fill_exit
            cash += gross_value - float(fee)
            pnl = gross_value - float(fee) - position["entry_cost"]
            return_pct = pnl / position["entry_cost"] * 100
            transactions.append(
                {
                    "Date": date,
                    "action": "SELL",
                    "reason": "END_OF_PERIOD",
                    "raw_price": raw_exit,
                    "fill_price": fill_exit,
                    "shares": shares,
                    "gross_value": gross_value,
                    "commission": float(fee),
                    "cash_after": cash,
                    "realized_pnl": pnl,
                    "entry_number": entries,
                }
            )
            round_trips.append(
                {
                    "entry_number": entries,
                    "entry_date": position["entry_date"],
                    "exit_date": date,
                    "entry_price": position["entry_price"],
                    "exit_price": fill_exit,
                    "shares": shares,
                    "exit_reason": "END_OF_PERIOD",
                    "holding_sessions": row_index - position["entry_index"],
                    "pnl": pnl,
                    "return_pct": return_pct,
                }
            )
            shares = 0.0
            position = None
            action_today = "SELL: END_OF_PERIOD"

        market_value = shares * row["Close"]
        strategy_equity = cash + market_value
        equity_rows.append(
            {
                "Date": date,
                "Close": row["Close"],
                "cash": cash,
                "shares": shares,
                "market_value": market_value,
                "strategy_equity": strategy_equity,
                "action": action_today,
                "active_stop": active_stop,
            }
        )

        if position is not None:
            position["highest_completed_high"] = max(
                position["highest_completed_high"], float(row["High"])
            )

    equity = pd.DataFrame(equity_rows)
    equity["buy_hold_equity"] = float(capital) * equity["Close"] / equity["Close"].iloc[0]
    equity["benchmark_equity"] = _aligned_benchmark_equity(
        equity["Date"], benchmark, capital
    ).to_numpy()
    equity["drawdown_pct"] = (
        equity["strategy_equity"] / equity["strategy_equity"].cummax() - 1
    ) * 100

    transactions_frame = pd.DataFrame(transactions, columns=TRANSACTION_COLUMNS)
    round_trips_frame = pd.DataFrame(round_trips, columns=ROUND_TRIP_COLUMNS)
    elapsed_days = max(1, (equity["Date"].iloc[-1] - equity["Date"].iloc[0]).days)
    ending_equity = equity["strategy_equity"].iloc[-1]
    total_return_pct = (ending_equity / float(capital) - 1) * 100
    annualized_return_pct = (
        (ending_equity / float(capital)) ** (365.25 / elapsed_days) - 1
    ) * 100
    buy_hold_return_pct = (
        equity["buy_hold_equity"].iloc[-1] / float(capital) - 1
    ) * 100
    benchmark_return_pct = (
        (equity["benchmark_equity"].dropna().iloc[-1] / float(capital) - 1) * 100
        if equity["benchmark_equity"].notna().any()
        else np.nan
    )
    completed_trades = len(round_trips_frame)
    winning_trades = int((round_trips_frame["pnl"] > 0).sum()) if completed_trades else 0

    summary = pd.DataFrame(
        [
            {
                "start_date": equity["Date"].iloc[0],
                "end_date": equity["Date"].iloc[-1],
                "initial_capital": float(capital),
                "ending_equity": ending_equity,
                "strategy_return_pct": total_return_pct,
                "annualized_return_pct": annualized_return_pct,
                "buy_hold_return_pct": buy_hold_return_pct,
                "benchmark_return_pct": benchmark_return_pct,
                "excess_vs_buy_hold_pct_points": total_return_pct - buy_hold_return_pct,
                "excess_vs_benchmark_pct_points": total_return_pct
                - benchmark_return_pct,
                "max_drawdown_pct": equity["drawdown_pct"].min(),
                "completed_trades": completed_trades,
                "winning_trades": winning_trades,
                "win_rate_pct": (
                    winning_trades / completed_trades * 100
                    if completed_trades
                    else np.nan
                ),
                "total_realized_pnl": (
                    round_trips_frame["pnl"].sum() if completed_trades else 0.0
                ),
                "exposure_pct": (equity["shares"] > 0).mean() * 100,
            }
        ]
    )

    return {
        "price_history": prices,
        "equity_curve": equity,
        "transactions": transactions_frame,
        "round_trips": round_trips_frame,
        "summary": summary,
    }
