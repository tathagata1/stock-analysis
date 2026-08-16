import pandas as pd
import pytest

from backtesting.engine import run_price_intent_backtest


def _history(opens, highs, lows, closes, adjusted=None):
    adjusted = closes if adjusted is None else adjusted
    return pd.DataFrame({
        "Date": pd.date_range("2026-01-01", periods=len(closes)),
        "Open": opens,
        "High": highs,
        "Low": lows,
        "Close": closes,
        "Adj Close": adjusted,
        "Volume": [1000] * len(closes),
    })


def test_protective_stop_ignores_minimum_holding_constraint():
    history = _history([10, 10, 10], [11, 11, 11], [9, 5, 9], [10, 6, 10])
    result = run_price_intent_backtest(
        history, history, entry_limit=10, target=20, fixed_stop=8,
        capital=100, use_whole_shares=False, minimum_holding_days=2,
        liquidate_at_end=True,
    )
    assert result["transactions"].iloc[-1]["reason"] == "STOP_LOSS"
    assert result["transactions"].iloc[-1]["Date"] == history.loc[1, "Date"]


def test_entry_bar_policy_is_explicit_and_counted():
    history = _history([10, 10], [21, 11], [7, 9], [10, 10])
    result = run_price_intent_backtest(
        history, history, entry_limit=10, target=20, fixed_stop=8,
        capital=100, use_whole_shares=False, entry_bar_exit_policy="stop",
        liquidate_at_end=False,
    )
    assert result["round_trips"].iloc[0]["exit_reason"] == "STOP_LOSS"
    assert result["round_trips"].iloc[0]["holding_sessions"] == 0
    assert result["summary"].iloc[0]["entry_bar_ambiguities"] == 1


def test_dynamic_stop_cannot_widen_by_default():
    history = _history([10, 10, 10], [11, 11, 11], [9, 7.5, 9], [10, 9, 10])
    result = run_price_intent_backtest(
        history, history, entry_limit=[10, 10, 10], target=[20, 20, 20],
        fixed_stop=[8, 7, 7], capital=100, use_whole_shares=False,
        liquidate_at_end=True,
    )
    assert result["transactions"].iloc[-1]["reason"] == "STOP_LOSS"
    assert result["transactions"].iloc[-1]["fill_price"] == pytest.approx(8)


def test_buy_hold_and_benchmark_use_adjusted_close():
    history = _history([10, 10], [11, 11], [9, 9], [10, 10], adjusted=[10, 12])
    result = run_price_intent_backtest(
        history, history, entry_limit=1, target=2, fixed_stop=0.5,
        capital=100, liquidate_at_end=True,
    )
    summary = result["summary"].iloc[0]
    assert summary["buy_hold_return_pct"] == pytest.approx(20)
    assert summary["benchmark_return_pct"] == pytest.approx(20)
    assert "annualized_volatility_pct" in result["summary"]

