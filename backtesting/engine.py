"""Long/short price-intent backtesting engine."""

import numpy as np
import pandas as pd

from .data import normalize_history


TRANSACTION_COLUMNS = [
    "Date",
    "action",
    "side",
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
    "side",
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


def build_dynamic_price_levels(
    history,
    lookback_days=20,
    atr_window_days=14,
    stop_atr_multiplier=2.0,
    trailing_atr_multiplier=3.0,
):
    """Build prior-session price channels and ATR-based risk levels."""
    prices = normalize_history(history)
    if prices.empty:
        raise ValueError("Price history is empty")
    if int(lookback_days) < 2 or int(atr_window_days) < 2:
        raise ValueError("Price lookback and ATR window must each be at least 2")
    if float(stop_atr_multiplier) <= 0 or float(trailing_atr_multiplier) <= 0:
        raise ValueError("ATR multipliers must be greater than zero")

    prior_close = prices["Close"].shift(1)
    true_range = pd.concat(
        [
            prices["High"] - prices["Low"],
            (prices["High"] - prior_close).abs(),
            (prices["Low"] - prior_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.rolling(
        int(atr_window_days), min_periods=int(atr_window_days)
    ).mean().shift(1)
    buy_price = (
        prices["Low"]
        .rolling(int(lookback_days), min_periods=int(lookback_days))
        .min()
        .shift(1)
    )
    sell_price = (
        prices["High"]
        .rolling(int(lookback_days), min_periods=int(lookback_days))
        .max()
        .shift(1)
    )
    stop_distance = atr * float(stop_atr_multiplier)
    trailing_stop_pct = (
        atr * float(trailing_atr_multiplier) / prior_close
    ).clip(upper=0.95)
    trailing_stop_pct = trailing_stop_pct.where(trailing_stop_pct > 0)
    long_stop = buy_price - stop_distance

    return pd.DataFrame(
        {
            "Date": prices["Date"],
            "buy_price": buy_price,
            "sell_price": sell_price,
            "stop_loss": long_stop.where(long_stop > 0),
            "short_stop_loss": sell_price + stop_distance,
            "trailing_stop_pct": trailing_stop_pct,
            "atr": atr,
        }
    )


def _coerce_level(value, length, name):
    if value is None:
        return pd.Series(np.nan, index=range(length), dtype=float)
    if np.isscalar(value):
        return pd.Series(float(value), index=range(length), dtype=float)

    series = pd.Series(value).reset_index(drop=True)
    if len(series) != length:
        raise ValueError(f"{name} must have one value per price-history row")
    return pd.to_numeric(series, errors="coerce")


def _validate_relationship(left, right, comparison, message):
    comparable = left.notna() & right.notna()
    if comparable.any() and not comparison(left[comparable], right[comparable]).all():
        raise ValueError(message)


def _validate_backtest_inputs(
    history,
    levels,
    capital,
    size_pct,
    share_limit,
    entries_limit,
    wait_days,
    minimum_holding_days,
    holding_limit,
    fee,
    slip_bps,
    priority,
    allow_short,
    entry_bar_exit_policy,
    level_update_mode,
    entry_at_market,
    market_entry_side,
    target_pct,
    fixed_stop_pct,
):
    if history.empty:
        raise ValueError("Price history is empty")
    if not entry_at_market:
        entry_values = levels["entry_limit"].dropna()
        if entry_values.empty or not entry_values.gt(0).all():
            raise ValueError("buy_price must contain positive values")
    _validate_relationship(
        levels["target"],
        levels["entry_limit"],
        pd.Series.gt,
        "sell_price must be above buy_price",
    )
    _validate_relationship(
        levels["fixed_stop"],
        levels["entry_limit"],
        pd.Series.lt,
        "stop_loss must be below buy_price",
    )
    trailing_values = levels["trailing_pct"].dropna()
    if not trailing_values.empty and not (
        trailing_values.gt(0) & trailing_values.lt(1)
    ).all():
        raise ValueError("trailing_stop_pct values must be between 0 and 1")

    if allow_short and not entry_at_market:
        short_entry_values = levels["short_entry_limit"].dropna()
        if short_entry_values.empty or not short_entry_values.gt(0).all():
            raise ValueError("Short entry prices must contain positive values")
        _validate_relationship(
            levels["short_target"],
            levels["short_entry_limit"],
            pd.Series.lt,
            "Short targets must be below short entry prices",
        )
        _validate_relationship(
            levels["short_fixed_stop"],
            levels["short_entry_limit"],
            pd.Series.gt,
            "Short stop prices must be above short entry prices",
        )

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
    if float(fee) < 0 or float(slip_bps) < 0:
        raise ValueError("commission_per_order and slippage_bps cannot be negative")
    if priority not in {"stop", "target"}:
        raise ValueError('same_day_exit_priority must be either "stop" or "target"')
    if entry_bar_exit_policy not in {"defer", "stop", "target"}:
        raise ValueError('entry_bar_exit_policy must be "defer", "stop", or "target"')
    if level_update_mode not in {"dynamic", "entry"}:
        raise ValueError('level_update_mode must be either "dynamic" or "entry"')
    normalized_market_side = str(market_entry_side).strip().lower()
    if normalized_market_side not in {"long", "short"}:
        raise ValueError('market_entry_side must be either "long" or "short"')
    if entry_at_market and normalized_market_side == "short" and not allow_short:
        raise ValueError("market_entry_side='short' requires allow_short=True")
    if target_pct is not None and not 0 < float(target_pct) < 1:
        raise ValueError("target_pct must be between 0 and 1 or None")
    if fixed_stop_pct is not None and not 0 < float(fixed_stop_pct) < 1:
        raise ValueError("fixed_stop_pct must be between 0 and 1 or None")


def _buy_fill(open_price, limit_price, slip_bps):
    raw_price = min(float(open_price), float(limit_price))
    fill_price = min(float(limit_price), raw_price * (1 + float(slip_bps) / 10_000))
    return raw_price, fill_price


def _sell_limit_fill(open_price, limit_price, slip_bps):
    raw_price = max(float(open_price), float(limit_price))
    fill_price = max(float(limit_price), raw_price * (1 - float(slip_bps) / 10_000))
    return raw_price, fill_price


def _sell_stop_fill(open_price, stop_price, slip_bps):
    raw_price = min(float(open_price), float(stop_price))
    fill_price = raw_price * (1 - float(slip_bps) / 10_000)
    return raw_price, fill_price


def _buy_stop_fill(open_price, stop_price, slip_bps):
    raw_price = max(float(open_price), float(stop_price))
    fill_price = raw_price * (1 + float(slip_bps) / 10_000)
    return raw_price, fill_price


def _market_fill(close_price, side, slip_bps):
    multiplier = (
        1 + float(slip_bps) / 10_000
        if side == "buy"
        else 1 - float(slip_bps) / 10_000
    )
    return float(close_price), float(close_price) * multiplier


def _aligned_benchmark_equity(dates, benchmark, capital):
    if benchmark is None or benchmark.empty:
        return pd.Series(np.nan, index=pd.DatetimeIndex(dates))
    price_column = "Adj Close" if benchmark["Adj Close"].notna().any() else "Close"
    benchmark_close = benchmark.set_index("Date")[price_column].sort_index()
    aligned = benchmark_close.reindex(pd.DatetimeIndex(dates), method="ffill")
    if aligned.notna().any():
        first_value = aligned.dropna().iloc[0]
        return aligned / first_value * float(capital)
    return pd.Series(np.nan, index=pd.DatetimeIndex(dates))


def _exit_records(
    position,
    signed_shares,
    cash,
    date,
    raw_exit,
    fill_exit,
    exit_reason,
    holding_sessions,
    fee,
    entry_number,
):
    quantity = abs(signed_shares)
    gross_value = quantity * fill_exit
    if position["side"] == "LONG":
        cash += gross_value - float(fee)
        pnl = gross_value - float(fee) - position["entry_cash_flow"]
        action = "SELL"
    else:
        cash -= gross_value + float(fee)
        pnl = position["entry_cash_flow"] - gross_value - float(fee)
        action = "BUY_TO_COVER"
    return_pct = pnl / position["return_basis"] * 100
    transaction = {
        "Date": date,
        "action": action,
        "side": position["side"],
        "reason": exit_reason,
        "raw_price": raw_exit,
        "fill_price": fill_exit,
        "shares": quantity,
        "gross_value": gross_value,
        "commission": float(fee),
        "cash_after": cash,
        "realized_pnl": pnl,
        "entry_number": entry_number,
    }
    round_trip = {
        "entry_number": entry_number,
        "side": position["side"],
        "entry_date": position["entry_date"],
        "exit_date": date,
        "entry_price": position["entry_price"],
        "exit_price": fill_exit,
        "shares": quantity,
        "exit_reason": exit_reason,
        "holding_sessions": holding_sessions,
        "pnl": pnl,
        "return_pct": return_pct,
    }
    return cash, transaction, round_trip


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
    allow_short=False,
    short_entry_limit=None,
    short_target=None,
    short_fixed_stop=None,
    entry_bar_exit_policy="defer",
    level_update_mode="dynamic",
    allow_stop_widening=False,
    entry_at_market=False,
    market_entry_side="long",
    target_pct=None,
    fixed_stop_pct=None,
):
    """Run a one-position long/short backtest and return result data frames.

    ``entry_at_market`` enters at an eligible session's opening price. When
    ``target_pct`` or ``fixed_stop_pct`` is supplied, that exit level is
    calculated from the actual slipped entry fill and locked to the position.
    This lets every re-entry establish fresh percentage-based price levels.
    """
    prices = normalize_history(history)
    benchmark_prices = (
        normalize_history(benchmark) if benchmark is not None else pd.DataFrame()
    )
    short_entry_limit = target if allow_short and short_entry_limit is None else short_entry_limit
    short_target = entry_limit if allow_short and short_target is None else short_target
    levels = pd.DataFrame(
        {
            "entry_limit": _coerce_level(entry_limit, len(prices), "buy_price"),
            "target": _coerce_level(target, len(prices), "sell_price"),
            "fixed_stop": _coerce_level(fixed_stop, len(prices), "stop_loss"),
            "trailing_pct": _coerce_level(
                trailing_pct, len(prices), "trailing_stop_pct"
            ),
            "short_entry_limit": _coerce_level(
                short_entry_limit, len(prices), "short_entry_price"
            ),
            "short_target": _coerce_level(
                short_target, len(prices), "short_target_price"
            ),
            "short_fixed_stop": _coerce_level(
                short_fixed_stop, len(prices), "short_stop_loss"
            ),
        }
    )
    _validate_backtest_inputs(
        prices,
        levels,
        capital,
        size_pct,
        share_limit,
        entries_limit,
        wait_days,
        minimum_holding_days,
        holding_limit,
        fee,
        slip_bps,
        priority,
        allow_short,
        entry_bar_exit_policy,
        level_update_mode,
        entry_at_market,
        market_entry_side,
        target_pct,
        fixed_stop_pct,
    )

    cash = float(capital)
    shares = 0.0
    entries = 0
    next_entry_index = 0
    position = None
    transactions = []
    round_trips = []
    equity_rows = []
    entry_bar_ambiguities = 0

    for row_index, row in prices.iterrows():
        date = row["Date"]
        current = levels.iloc[row_index]
        action_today = "HOLD"
        active_stop = np.nan

        # Exits are evaluated before new entries, and never on the entry candle.
        if position is not None and row_index > position["entry_index"]:
            holding_sessions = row_index - position["entry_index"]
            minimum_hold_met = (
                minimum_holding_days is None
                or holding_sessions >= int(minimum_holding_days)
            )
            trailing_value = (
                position["entry_trailing_pct"]
                if level_update_mode == "entry"
                else current["trailing_pct"]
            )
            trailing_level = None
            target_level = None
            stop_candidates = []

            if position["side"] == "LONG":
                if pd.notna(trailing_value):
                    trailing_level = position["highest_completed_high"] * (
                        1 - trailing_value
                    )
                current_fixed_stop = (
                    position["entry_fixed_stop"]
                    if fixed_stop_pct is not None or level_update_mode == "entry"
                    else current["fixed_stop"]
                )
                if (
                    pd.isna(current_fixed_stop)
                    and not allow_stop_widening
                    and pd.notna(position["last_fixed_stop"])
                ):
                    current_fixed_stop = position["last_fixed_stop"]
                if pd.notna(current_fixed_stop):
                    if (
                        level_update_mode == "dynamic"
                        and not allow_stop_widening
                        and pd.notna(position["last_fixed_stop"])
                    ):
                        current_fixed_stop = max(
                            float(current_fixed_stop), float(position["last_fixed_stop"])
                        )
                    position["last_fixed_stop"] = current_fixed_stop
                    stop_candidates.append(current_fixed_stop)
                if trailing_level is not None:
                    stop_candidates.append(trailing_level)
                active_stop = max(stop_candidates) if stop_candidates else np.nan
                if (
                    pd.notna(active_stop)
                    and not allow_stop_widening
                    and pd.notna(position["last_active_stop"])
                ):
                    active_stop = max(float(active_stop), float(position["last_active_stop"]))
                if pd.notna(active_stop):
                    position["last_active_stop"] = active_stop
                target_level = (
                    position["entry_target"]
                    if target_pct is not None or level_update_mode == "entry"
                    else current["target"]
                )
                target_hit = (
                    minimum_hold_met
                    and pd.notna(target_level)
                    and row["High"] >= target_level
                )
                stop_hit = (
                    minimum_hold_met
                    and pd.notna(active_stop)
                    and row["Low"] <= active_stop
                )
            else:
                if pd.notna(trailing_value):
                    trailing_level = position["lowest_completed_low"] * (
                        1 + trailing_value
                    )
                current_fixed_stop = (
                    position["entry_fixed_stop"]
                    if fixed_stop_pct is not None or level_update_mode == "entry"
                    else current["short_fixed_stop"]
                )
                if (
                    pd.isna(current_fixed_stop)
                    and not allow_stop_widening
                    and pd.notna(position["last_fixed_stop"])
                ):
                    current_fixed_stop = position["last_fixed_stop"]
                if pd.notna(current_fixed_stop):
                    if (
                        level_update_mode == "dynamic"
                        and not allow_stop_widening
                        and pd.notna(position["last_fixed_stop"])
                    ):
                        current_fixed_stop = min(
                            float(current_fixed_stop), float(position["last_fixed_stop"])
                        )
                    position["last_fixed_stop"] = current_fixed_stop
                    stop_candidates.append(current_fixed_stop)
                if trailing_level is not None:
                    stop_candidates.append(trailing_level)
                active_stop = min(stop_candidates) if stop_candidates else np.nan
                if (
                    pd.notna(active_stop)
                    and not allow_stop_widening
                    and pd.notna(position["last_active_stop"])
                ):
                    active_stop = min(float(active_stop), float(position["last_active_stop"]))
                if pd.notna(active_stop):
                    position["last_active_stop"] = active_stop
                target_level = (
                    position["entry_target"]
                    if target_pct is not None or level_update_mode == "entry"
                    else current["short_target"]
                )
                target_hit = (
                    minimum_hold_met
                    and pd.notna(target_level)
                    and row["Low"] <= target_level
                )
                stop_hit = (
                    minimum_hold_met
                    and pd.notna(active_stop)
                    and row["High"] >= active_stop
                )

            exit_reason = None
            raw_exit = None
            fill_exit = None
            chosen_exit = (
                "stop"
                if stop_hit and (not target_hit or priority == "stop")
                else "target" if target_hit else None
            )
            if chosen_exit == "stop":
                exit_reason = (
                    "TRAILING_STOP"
                    if trailing_level is not None and active_stop == trailing_level
                    else "STOP_LOSS"
                )
                fill_function = (
                    _sell_stop_fill if position["side"] == "LONG" else _buy_stop_fill
                )
                raw_exit, fill_exit = fill_function(row["Open"], active_stop, slip_bps)
            elif chosen_exit == "target":
                exit_reason = "TARGET"
                fill_function = (
                    _sell_limit_fill if position["side"] == "LONG" else _buy_fill
                )
                raw_exit, fill_exit = fill_function(row["Open"], target_level, slip_bps)
            elif (
                minimum_hold_met
                and holding_limit is not None
                and holding_sessions >= int(holding_limit)
            ):
                exit_reason = "TIME_LIMIT"
                market_side = "sell" if position["side"] == "LONG" else "buy"
                raw_exit, fill_exit = _market_fill(row["Close"], market_side, slip_bps)
            elif liquidate_at_end and row_index == len(prices) - 1:
                exit_reason = "END_OF_PERIOD"
                market_side = "sell" if position["side"] == "LONG" else "buy"
                raw_exit, fill_exit = _market_fill(row["Close"], market_side, slip_bps)

            if exit_reason is not None:
                exit_side = position["side"]
                cash, transaction, round_trip = _exit_records(
                    position,
                    shares,
                    cash,
                    date,
                    raw_exit,
                    fill_exit,
                    exit_reason,
                    holding_sessions,
                    fee,
                    entries,
                )
                transactions.append(transaction)
                round_trips.append(round_trip)
                shares = 0.0
                position = None
                next_entry_index = row_index + int(wait_days) + 1
                action_today = f"EXIT {exit_side}: {exit_reason}"

        can_consider_entry = (
            position is None
            and action_today == "HOLD"
            and row_index >= next_entry_index
            and entries < int(entries_limit)
            and (reentry or entries == 0)
        )
        if entry_at_market and can_consider_entry:
            entry_side = str(market_entry_side).strip().upper()
        else:
            long_signal = (
                can_consider_entry
                and pd.notna(current["entry_limit"])
                and row["Low"] <= current["entry_limit"]
            )
            short_signal = (
                can_consider_entry
                and allow_short
                and pd.notna(current["short_entry_limit"])
                and row["High"] >= current["short_entry_limit"]
            )

            # Long takes precedence if a daily candle touches both entry channels.
            entry_side = "LONG" if long_signal else "SHORT" if short_signal else None
        if entry_side is not None:
            if entry_at_market:
                market_side = "buy" if entry_side == "LONG" else "sell"
                raw_entry, fill_entry = _market_fill(
                    row["Open"], market_side, slip_bps
                )
            elif entry_side == "LONG":
                raw_entry, fill_entry = _buy_fill(
                    row["Open"], current["entry_limit"], slip_bps
                )
            else:
                raw_entry, fill_entry = _sell_limit_fill(
                    row["Open"], current["short_entry_limit"], slip_bps
                )
            available_for_shares = max(0.0, cash * float(size_pct) - float(fee))
            quantity = available_for_shares / fill_entry
            if share_limit is not None:
                quantity = min(quantity, float(share_limit))
            if use_whole_shares:
                quantity = float(np.floor(quantity))

            entry_notional = quantity * fill_entry
            entry_cash_flow = (
                entry_notional + float(fee)
                if entry_side == "LONG"
                else entry_notional - float(fee)
            )
            can_fund = entry_side == "SHORT" or entry_cash_flow <= cash + 1e-9
            if quantity > 0 and can_fund:
                entry_target = (
                    fill_entry
                    * (1 + float(target_pct) if entry_side == "LONG" else 1 - float(target_pct))
                    if target_pct is not None
                    else current["target"] if entry_side == "LONG" else current["short_target"]
                )
                entry_fixed_stop = (
                    fill_entry
                    * (1 - float(fixed_stop_pct) if entry_side == "LONG" else 1 + float(fixed_stop_pct))
                    if fixed_stop_pct is not None
                    else current["fixed_stop"] if entry_side == "LONG" else current["short_fixed_stop"]
                )
                cash += -entry_cash_flow if entry_side == "LONG" else entry_cash_flow
                shares = quantity if entry_side == "LONG" else -quantity
                entries += 1
                position = {
                    "side": entry_side,
                    "entry_index": row_index,
                    "entry_date": date,
                    "entry_price": fill_entry,
                    "entry_cash_flow": entry_cash_flow,
                    "return_basis": entry_notional + float(fee),
                    "highest_completed_high": float(row["High"]),
                    "lowest_completed_low": float(row["Low"]),
                    "entry_target": entry_target,
                    "entry_fixed_stop": entry_fixed_stop,
                    "entry_trailing_pct": current["trailing_pct"],
                    "last_fixed_stop": entry_fixed_stop,
                    "last_active_stop": entry_fixed_stop,
                }
                if entry_side == "LONG":
                    current["entry_limit"] = fill_entry
                    current["target"] = entry_target
                    current["fixed_stop"] = entry_fixed_stop
                else:
                    current["short_entry_limit"] = fill_entry
                    current["short_target"] = entry_target
                    current["short_fixed_stop"] = entry_fixed_stop
                transactions.append(
                    {
                        "Date": date,
                        "action": "BUY" if entry_side == "LONG" else "SELL_SHORT",
                        "side": entry_side,
                        "reason": (
                            "MARKET_ENTRY"
                            if entry_at_market
                            else "BUY_LIMIT" if entry_side == "LONG" else "SHORT_ENTRY"
                        ),
                        "raw_price": raw_entry,
                        "fill_price": fill_entry,
                        "shares": quantity,
                        "gross_value": entry_notional,
                        "commission": float(fee),
                        "cash_after": cash,
                        "realized_pnl": np.nan,
                        "entry_number": entries,
                    }
                )
                action_today = f"ENTER {entry_side}"

        # Daily bars cannot reveal whether an exit level was touched before or
        # after an intraday entry. Record that ambiguity and apply the caller's
        # explicit policy; the backwards-compatible default defers all exits.
        if position is not None and position["entry_index"] == row_index:
            entry_minimum_hold_met = (
                minimum_holding_days is None
                or int(minimum_holding_days) == 0
            )
            if position["side"] == "LONG":
                entry_target_level = position["entry_target"]
                entry_stop_level = position["entry_fixed_stop"]
                entry_target_hit = (
                    entry_minimum_hold_met
                    and pd.notna(entry_target_level)
                    and row["High"] >= entry_target_level
                )
                entry_stop_hit = (
                    entry_minimum_hold_met
                    and pd.notna(entry_stop_level)
                    and row["Low"] <= entry_stop_level
                )
            else:
                entry_target_level = position["entry_target"]
                entry_stop_level = position["entry_fixed_stop"]
                entry_target_hit = (
                    entry_minimum_hold_met
                    and pd.notna(entry_target_level)
                    and row["Low"] <= entry_target_level
                )
                entry_stop_hit = (
                    entry_minimum_hold_met
                    and pd.notna(entry_stop_level)
                    and row["High"] >= entry_stop_level
                )

            if entry_target_hit or entry_stop_hit:
                entry_bar_ambiguities += 1

            chosen_entry_exit = None
            if entry_bar_exit_policy == "stop":
                chosen_entry_exit = "stop" if entry_stop_hit else "target" if entry_target_hit else None
            elif entry_bar_exit_policy == "target":
                chosen_entry_exit = "target" if entry_target_hit else "stop" if entry_stop_hit else None

            if chosen_entry_exit is not None:
                exit_side = position["side"]
                if chosen_entry_exit == "stop":
                    exit_reason = "STOP_LOSS"
                    fill_function = _sell_stop_fill if exit_side == "LONG" else _buy_stop_fill
                    raw_exit, fill_exit = fill_function(entry_stop_level, entry_stop_level, slip_bps)
                else:
                    exit_reason = "TARGET"
                    fill_function = _sell_limit_fill if exit_side == "LONG" else _buy_fill
                    raw_exit, fill_exit = fill_function(entry_target_level, entry_target_level, slip_bps)
                cash, transaction, round_trip = _exit_records(
                    position,
                    shares,
                    cash,
                    date,
                    raw_exit,
                    fill_exit,
                    exit_reason,
                    0,
                    fee,
                    entries,
                )
                transactions.append(transaction)
                round_trips.append(round_trip)
                shares = 0.0
                position = None
                next_entry_index = row_index + int(wait_days) + 1
                action_today = f"EXIT {exit_side}: {exit_reason} (ENTRY BAR)"

        # A last-session entry cannot be evaluated later, so close it at that close.
        if position is not None and liquidate_at_end and row_index == len(prices) - 1:
            exit_side = position["side"]
            market_side = "sell" if exit_side == "LONG" else "buy"
            raw_exit, fill_exit = _market_fill(row["Close"], market_side, slip_bps)
            cash, transaction, round_trip = _exit_records(
                position,
                shares,
                cash,
                date,
                raw_exit,
                fill_exit,
                "END_OF_PERIOD",
                row_index - position["entry_index"],
                fee,
                entries,
            )
            transactions.append(transaction)
            round_trips.append(round_trip)
            shares = 0.0
            position = None
            action_today = f"EXIT {exit_side}: END_OF_PERIOD"

        market_value = shares * row["Close"]
        strategy_equity = cash + market_value
        displayed_levels = current.to_dict()
        if position is not None:
            if position["side"] == "LONG":
                displayed_levels["entry_limit"] = position["entry_price"]
                displayed_levels["target"] = position["entry_target"]
                displayed_levels["fixed_stop"] = position["entry_fixed_stop"]
            else:
                displayed_levels["short_entry_limit"] = position["entry_price"]
                displayed_levels["short_target"] = position["entry_target"]
                displayed_levels["short_fixed_stop"] = position["entry_fixed_stop"]
        equity_rows.append(
            {
                "Date": date,
                "Close": row["Close"],
                "cash": cash,
                "shares": shares,
                "position_side": position["side"] if position is not None else None,
                "market_value": market_value,
                "strategy_equity": strategy_equity,
                "action": action_today,
                "active_stop": active_stop,
                **displayed_levels,
            }
        )

        if position is not None:
            position["highest_completed_high"] = max(
                position["highest_completed_high"], float(row["High"])
            )
            position["lowest_completed_low"] = min(
                position["lowest_completed_low"], float(row["Low"])
            )

    equity = pd.DataFrame(equity_rows)
    total_return_price = prices["Adj Close"].where(
        prices["Adj Close"].notna(), prices["Close"]
    )
    equity["buy_hold_equity"] = (
        float(capital) * total_return_price / total_return_price.iloc[0]
    )
    if not benchmark_prices.empty:
        equity["benchmark_equity"] = _aligned_benchmark_equity(
            equity["Date"], benchmark_prices, capital
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
        ((ending_equity / float(capital)) ** (365.25 / elapsed_days) - 1) * 100
        if ending_equity > 0
        else np.nan
    )
    buy_hold_return_pct = (
        equity["buy_hold_equity"].iloc[-1] / float(capital) - 1
    ) * 100
    benchmark_return_pct = (
        (equity["benchmark_equity"].dropna().iloc[-1] / float(capital) - 1) * 100
        if "benchmark_equity" in equity
        and equity["benchmark_equity"].notna().any()
        else None
    )
    completed_trades = len(round_trips_frame)
    winning_trades = int((round_trips_frame["pnl"] > 0).sum()) if completed_trades else 0
    equity_returns = equity["strategy_equity"].pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    annualized_volatility_pct = (
        float(equity_returns.std() * np.sqrt(252) * 100)
        if len(equity_returns) > 1
        else np.nan
    )
    sharpe_ratio = (
        float(np.sqrt(252) * equity_returns.mean() / equity_returns.std())
        if len(equity_returns) > 1 and equity_returns.std() > 0
        else np.nan
    )
    downside_returns = equity_returns[equity_returns < 0]
    sortino_ratio = (
        float(np.sqrt(252) * equity_returns.mean() / downside_returns.std())
        if len(downside_returns) > 1 and downside_returns.std() > 0
        else np.nan
    )
    gross_profit = (
        float(round_trips_frame.loc[round_trips_frame["pnl"] > 0, "pnl"].sum())
        if completed_trades
        else 0.0
    )
    gross_loss = (
        abs(float(round_trips_frame.loc[round_trips_frame["pnl"] < 0, "pnl"].sum()))
        if completed_trades
        else 0.0
    )
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.nan
    total_commissions = float(transactions_frame["commission"].sum()) if not transactions_frame.empty else 0.0
    turnover = (
        float(transactions_frame["gross_value"].sum() / equity["strategy_equity"].abs().mean())
        if not transactions_frame.empty and equity["strategy_equity"].abs().mean() > 0
        else 0.0
    )

    summary_row = {
        "start_date": equity["Date"].iloc[0],
        "end_date": equity["Date"].iloc[-1],
        "initial_capital": float(capital),
        "ending_equity": ending_equity,
        "strategy_return_pct": total_return_pct,
        "annualized_return_pct": annualized_return_pct,
        "buy_hold_return_pct": buy_hold_return_pct,
        "excess_vs_buy_hold_pct_points": total_return_pct - buy_hold_return_pct,
        "max_drawdown_pct": equity["drawdown_pct"].min(),
        "annualized_volatility_pct": annualized_volatility_pct,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "completed_trades": completed_trades,
        "long_trades": (
            int((round_trips_frame["side"] == "LONG").sum())
            if completed_trades
            else 0
        ),
        "short_trades": (
            int((round_trips_frame["side"] == "SHORT").sum())
            if completed_trades
            else 0
        ),
        "winning_trades": winning_trades,
        "win_rate_pct": (
            winning_trades / completed_trades * 100
            if completed_trades
            else np.nan
        ),
        "total_realized_pnl": (
            round_trips_frame["pnl"].sum() if completed_trades else 0.0
        ),
        "exposure_pct": equity["shares"].ne(0).mean() * 100,
        "average_trade_pnl": (
            round_trips_frame["pnl"].mean() if completed_trades else np.nan
        ),
        "average_holding_sessions": (
            round_trips_frame["holding_sessions"].mean()
            if completed_trades
            else np.nan
        ),
        "profit_factor": profit_factor,
        "total_commissions": total_commissions,
        "turnover_multiple": turnover,
        "entry_bar_ambiguities": entry_bar_ambiguities,
    }
    if benchmark_return_pct is not None:
        summary_row["benchmark_return_pct"] = benchmark_return_pct
        summary_row["excess_vs_benchmark_pct_points"] = (
            total_return_pct - benchmark_return_pct
        )
    summary = pd.DataFrame([summary_row])

    return {
        "price_history": prices,
        "equity_curve": equity,
        "transactions": transactions_frame,
        "round_trips": round_trips_frame,
        "summary": summary,
        "allow_short": bool(allow_short),
        "execution_assumptions": {
            "entry_bar_exit_policy": entry_bar_exit_policy,
            "level_update_mode": level_update_mode,
            "allow_stop_widening": bool(allow_stop_widening),
            "entry_at_market": bool(entry_at_market),
            "market_entry_side": str(market_entry_side).strip().lower(),
            "target_pct": target_pct,
            "fixed_stop_pct": fixed_stop_pct,
            "short_model": "simplified; excludes borrow fees, margin calls, and dividends owed",
        },
    }
