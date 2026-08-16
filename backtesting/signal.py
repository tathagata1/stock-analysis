"""Walk-forward backtest for an already-computed composite signal frame."""

import numpy as np
import pandas as pd

import config.config as config
from analysis_functions.technical_analysis import (
    calculate_buy_score,
    calculate_sell_score,
    get_technical_analysis_calculations,
)
from .data import normalize_history
from .engine import _aligned_benchmark_equity


def build_technical_entry_signal(history):
    """Return a causal signed entry signal derived from technical indicators.

    Positive values are bullish and negative values are bearish. Warm-up rows
    remain missing so a market-entry strategy waits for complete indicator data.
    The backtest engine applies the required one-session execution lag.
    """
    prices = normalize_history(history)
    indicators = get_technical_analysis_calculations(prices.copy())
    buy_score = indicators.apply(calculate_buy_score, axis=1)
    sell_score = indicators.apply(calculate_sell_score, axis=1)
    signal = buy_score + sell_score
    return signal.where(indicators["technical_data_available"])


def run_composite_signal_backtest(
    predictions,
    benchmark=None,
    capital=10_000,
    position_size_pct=1.0,
    allow_short=False,
    long_entry_threshold=None,
    long_exit_threshold=0.0,
    short_entry_threshold=None,
    short_exit_threshold=0.0,
    commission_per_order=0.0,
    slippage_bps=0.0,
    use_whole_shares=False,
    liquidate_at_end=True,
):
    """Backtest composite signals with all decisions lagged by one session.

    The caller supplies a historical prediction frame.  A signal observed after
    session *t* can only trade at session *t+1*'s open, preventing direct signal
    look-ahead.  This function evaluates the composite strategy; it is separate
    from the rolling price-intent engine.
    """
    if predictions is None or predictions.empty:
        raise ValueError("Prediction history is empty")
    required = {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Signal"}
    missing = required.difference(predictions.columns)
    if missing:
        raise ValueError("Prediction history is missing: " + ", ".join(sorted(missing)))
    if float(capital) <= 0:
        raise ValueError("capital must be greater than zero")
    if not 0 < float(position_size_pct) <= 1:
        raise ValueError("position_size_pct must be greater than zero and at most one")
    if float(commission_per_order) < 0 or float(slippage_bps) < 0:
        raise ValueError("cost settings cannot be negative")

    long_entry_threshold = (
        config.WEAK_BUY_THRESHOLD
        if long_entry_threshold is None
        else float(long_entry_threshold)
    )
    short_entry_threshold = (
        config.WEAK_SELL_THRESHOLD
        if short_entry_threshold is None
        else float(short_entry_threshold)
    )
    if float(long_exit_threshold) > float(long_entry_threshold):
        raise ValueError("long_exit_threshold cannot exceed long_entry_threshold")
    if float(short_exit_threshold) < float(short_entry_threshold):
        raise ValueError("short_exit_threshold cannot be below short_entry_threshold")

    prices = normalize_history(predictions)
    signal_frame = predictions[["Date", "Signal"]].copy()
    signal_frame["Date"] = pd.to_datetime(signal_frame["Date"], errors="coerce")
    if signal_frame["Date"].dt.tz is not None:
        signal_frame["Date"] = signal_frame["Date"].dt.tz_localize(None)
    signal_frame["Signal"] = pd.to_numeric(signal_frame["Signal"], errors="coerce")
    signal_frame = signal_frame.dropna(subset=["Date"]).drop_duplicates("Date", keep="last")
    frame = prices.merge(signal_frame, on="Date", how="left", validate="one_to_one")
    frame["decision_signal"] = frame["Signal"].shift(1)

    cash = float(capital)
    signed_shares = 0.0
    position = None
    entries = 0
    transactions = []
    round_trips = []
    equity_rows = []

    def fill(price, action):
        direction = 1 if action in {"BUY", "BUY_TO_COVER"} else -1
        return float(price) * (1 + direction * float(slippage_bps) / 10_000)

    def close_position(row, reason, at_close=False):
        nonlocal cash, signed_shares, position
        raw_price = row["Close"] if at_close else row["Open"]
        action = "SELL" if position["side"] == "LONG" else "BUY_TO_COVER"
        exit_price = fill(raw_price, action)
        quantity = abs(signed_shares)
        gross_value = quantity * exit_price
        if position["side"] == "LONG":
            cash += gross_value - float(commission_per_order)
            pnl = gross_value - float(commission_per_order) - position["entry_cash_flow"]
        else:
            cash -= gross_value + float(commission_per_order)
            pnl = position["entry_cash_flow"] - gross_value - float(commission_per_order)
        transactions.append({
            "Date": row["Date"], "action": action, "side": position["side"],
            "reason": reason, "fill_price": exit_price, "shares": quantity,
            "commission": float(commission_per_order), "cash_after": cash,
            "decision_signal": row["decision_signal"], "entry_number": position["entry_number"],
        })
        round_trips.append({
            "entry_number": position["entry_number"], "side": position["side"],
            "entry_date": position["entry_date"], "exit_date": row["Date"],
            "entry_price": position["entry_price"], "exit_price": exit_price,
            "shares": quantity, "exit_reason": reason,
            "holding_sessions": int(row.name - position["entry_index"]),
            "pnl": pnl, "return_pct": pnl / position["return_basis"] * 100,
        })
        signed_shares = 0.0
        position = None

    for row_index, row in frame.iterrows():
        action_today = "HOLD"
        decision = row["decision_signal"]

        if position is not None and pd.notna(decision):
            should_exit = (
                position["side"] == "LONG" and decision <= float(long_exit_threshold)
            ) or (
                position["side"] == "SHORT" and decision >= float(short_exit_threshold)
            )
            if should_exit:
                exiting_side = position["side"]
                close_position(row, "SIGNAL_EXIT")
                action_today = f"EXIT {exiting_side}: SIGNAL_EXIT"

        if position is None and action_today == "HOLD" and pd.notna(decision):
            side = (
                "LONG" if decision >= float(long_entry_threshold)
                else "SHORT" if allow_short and decision <= float(short_entry_threshold)
                else None
            )
            if side is not None:
                action = "BUY" if side == "LONG" else "SELL_SHORT"
                entry_price = fill(row["Open"], action)
                available = max(0.0, cash * float(position_size_pct) - float(commission_per_order))
                quantity = available / entry_price
                if use_whole_shares:
                    quantity = float(np.floor(quantity))
                if quantity > 0:
                    gross_value = quantity * entry_price
                    entry_cash_flow = (
                        gross_value + float(commission_per_order)
                        if side == "LONG"
                        else gross_value - float(commission_per_order)
                    )
                    cash += -entry_cash_flow if side == "LONG" else entry_cash_flow
                    signed_shares = quantity if side == "LONG" else -quantity
                    entries += 1
                    position = {
                        "side": side, "entry_index": row_index, "entry_date": row["Date"],
                        "entry_price": entry_price, "entry_cash_flow": entry_cash_flow,
                        "return_basis": gross_value + float(commission_per_order),
                        "entry_number": entries,
                    }
                    transactions.append({
                        "Date": row["Date"], "action": action, "side": side,
                        "reason": "LAGGED_SIGNAL_ENTRY", "fill_price": entry_price,
                        "shares": quantity, "commission": float(commission_per_order),
                        "cash_after": cash, "decision_signal": decision,
                        "entry_number": entries,
                    })
                    action_today = f"ENTER {side}"

        if position is not None and liquidate_at_end and row_index == len(frame) - 1:
            exiting_side = position["side"]
            close_position(row, "END_OF_PERIOD", at_close=True)
            action_today = f"EXIT {exiting_side}: END_OF_PERIOD"

        equity_rows.append({
            "Date": row["Date"], "Close": row["Close"], "Signal": row["Signal"],
            "decision_signal": decision, "cash": cash, "shares": signed_shares,
            "strategy_equity": cash + signed_shares * row["Close"], "action": action_today,
        })

    equity = pd.DataFrame(equity_rows)
    total_return_price = frame["Adj Close"].where(frame["Adj Close"].notna(), frame["Close"])
    equity["buy_hold_equity"] = float(capital) * total_return_price / total_return_price.iloc[0]
    benchmark_prices = normalize_history(benchmark) if benchmark is not None else pd.DataFrame()
    equity["benchmark_equity"] = _aligned_benchmark_equity(
        equity["Date"], benchmark_prices, capital
    ).to_numpy()
    equity["drawdown_pct"] = (
        equity["strategy_equity"] / equity["strategy_equity"].cummax() - 1
    ) * 100
    transactions_frame = pd.DataFrame(
        transactions,
        columns=[
            "Date", "action", "side", "reason", "fill_price", "shares",
            "commission", "cash_after", "decision_signal", "entry_number",
        ],
    )
    round_trips_frame = pd.DataFrame(
        round_trips,
        columns=[
            "entry_number", "side", "entry_date", "exit_date", "entry_price",
            "exit_price", "shares", "exit_reason", "holding_sessions", "pnl",
            "return_pct",
        ],
    )
    ending_equity = float(equity["strategy_equity"].iloc[-1])
    summary = pd.DataFrame([{
        "start_date": equity["Date"].iloc[0],
        "end_date": equity["Date"].iloc[-1],
        "initial_capital": float(capital),
        "ending_equity": ending_equity,
        "strategy_return_pct": (ending_equity / float(capital) - 1) * 100,
        "buy_hold_return_pct": (equity["buy_hold_equity"].iloc[-1] / float(capital) - 1) * 100,
        "benchmark_return_pct": (
            (equity["benchmark_equity"].dropna().iloc[-1] / float(capital) - 1) * 100
            if equity["benchmark_equity"].notna().any() else np.nan
        ),
        "max_drawdown_pct": equity["drawdown_pct"].min(),
        "completed_trades": len(round_trips_frame),
        "total_realized_pnl": (
            round_trips_frame["pnl"].sum() if not round_trips_frame.empty else 0.0
        ),
        "signal_lag_sessions": 1,
    }])
    return {
        "price_history": frame,
        "equity_curve": equity,
        "transactions": transactions_frame,
        "round_trips": round_trips_frame,
        "summary": summary,
        "allow_short": bool(allow_short),
        "strategy": "composite_signal",
    }
