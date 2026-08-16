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


def test_minimum_holding_period_gates_protective_stop():
    history = _history([10, 10, 10], [11, 11, 11], [9, 5, 7], [10, 6, 8])
    result = run_price_intent_backtest(
        history, history, entry_limit=10, target=20, fixed_stop=8,
        capital=100, use_whole_shares=False, minimum_holding_days=2,
        liquidate_at_end=True,
    )
    assert result["transactions"].iloc[-1]["reason"] == "STOP_LOSS"
    assert result["transactions"].iloc[-1]["Date"] == history.loc[2, "Date"]


def test_stop_and_target_take_priority_over_maximum_holding_exit():
    history = _history(
        [10, 10, 10], [11, 11, 21], [9, 9, 5], [10, 10, 10]
    )
    result = run_price_intent_backtest(
        history, history, entry_limit=10, target=20, fixed_stop=8,
        capital=100, use_whole_shares=False, minimum_holding_days=2,
        holding_limit=2, priority="stop", liquidate_at_end=False,
    )
    assert result["transactions"].iloc[-1]["reason"] == "STOP_LOSS"


def test_target_takes_priority_over_maximum_holding_exit():
    history = _history(
        [10, 10, 10], [11, 11, 13], [9, 9, 9], [10, 10, 12]
    )
    result = run_price_intent_backtest(
        history, history, entry_limit=10, target=12, fixed_stop=8,
        capital=100, use_whole_shares=False, minimum_holding_days=2,
        holding_limit=2, liquidate_at_end=False,
    )
    assert result["transactions"].iloc[-1]["reason"] == "TARGET"


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


def test_omitted_benchmark_skips_benchmark_calculations():
    history = _history([10, 10], [11, 11], [9, 9], [10, 10])
    result = run_price_intent_backtest(
        history, None, entry_limit=1, target=2, fixed_stop=0.5,
        capital=100, liquidate_at_end=True,
    )
    assert "benchmark_equity" not in result["equity_curve"]
    assert "benchmark_return_pct" not in result["summary"]
    assert "excess_vs_benchmark_pct_points" not in result["summary"]


def test_market_reentries_recalculate_percentage_levels_from_each_fill():
    history = _history(
        opens=[100, 100, 100, 120, 120],
        highs=[101, 101, 111, 121, 133],
        lows=[99, 99, 99, 119, 119],
        closes=[100, 100, 110, 120, 132],
    )
    result = run_price_intent_backtest(
        history,
        history,
        entry_limit=None,
        target=None,
        fixed_stop=None,
        capital=100,
        use_whole_shares=False,
        entries_limit=2,
        wait_days=0,
        entry_at_market=True,
        entry_signal=[1, 0, 1, 0, 0],
        target_pct=0.10,
        fixed_stop_pct=0.05,
        liquidate_at_end=False,
    )

    entries = result["transactions"].query("action == 'BUY'")
    exits = result["transactions"].query("action == 'SELL'")
    assert entries["fill_price"].tolist() == pytest.approx([100, 120])
    assert entries["reason"].tolist() == ["MARKET_ENTRY", "MARKET_ENTRY"]
    assert exits["fill_price"].tolist() == pytest.approx([110, 132])
    assert exits["reason"].tolist() == ["TARGET", "TARGET"]
    assert result["equity_curve"].loc[1, "target"] == pytest.approx(110)
    assert result["equity_curve"].loc[1, "fixed_stop"] == pytest.approx(95)
    assert result["equity_curve"].loc[3, "target"] == pytest.approx(132)
    assert result["equity_curve"].loc[3, "fixed_stop"] == pytest.approx(114)


def test_market_entry_percentage_stop_is_based_on_slipped_fill():
    history = _history(
        opens=[100, 100, 100],
        highs=[101, 101, 101],
        lows=[99, 99, 94],
        closes=[100, 100, 95],
    )
    result = run_price_intent_backtest(
        history,
        history,
        entry_limit=None,
        capital=100,
        use_whole_shares=False,
        entry_at_market=True,
        entry_signal=[1, 0, 0],
        target_pct=0.10,
        fixed_stop_pct=0.05,
        slip_bps=100,
        liquidate_at_end=False,
    )

    entry = result["transactions"].iloc[0]
    exit_order = result["transactions"].iloc[1]
    assert entry["fill_price"] == pytest.approx(101)
    assert result["equity_curve"].loc[1, "target"] == pytest.approx(111.1)
    assert result["equity_curve"].loc[1, "fixed_stop"] == pytest.approx(95.95)
    assert exit_order["reason"] == "STOP_LOSS"


def test_market_entry_can_use_trailing_stop_without_initial_fixed_stop():
    history = _history(
        opens=[100, 100, 105],
        highs=[101, 110, 106],
        lows=[99, 99, 104],
        closes=[100, 109, 105],
    )
    result = run_price_intent_backtest(
        history,
        None,
        entry_limit=None,
        capital=100,
        use_whole_shares=False,
        entry_at_market=True,
        entry_signal=[0.5, 0, 0],
        trailing_pct=0.05,
        target_pct=0.50,
        liquidate_at_end=False,
    )

    assert pd.isna(result["equity_curve"].loc[1, "fixed_stop"])
    assert result["transactions"]["reason"].tolist() == [
        "MARKET_ENTRY",
        "TRAILING_STOP",
    ]
    assert result["transactions"].iloc[-1]["fill_price"] == pytest.approx(104.5)


def test_market_entry_direction_is_selected_from_lagged_indicator_signal():
    history = _history(
        opens=[100, 100, 100],
        highs=[101, 101, 101],
        lows=[99, 99, 99],
        closes=[100, 100, 100],
    )
    result = run_price_intent_backtest(
        history,
        None,
        entry_limit=None,
        capital=100,
        use_whole_shares=False,
        entry_at_market=True,
        entry_signal=[-0.5, 0.5, 0],
        allow_short=True,
        target_pct=0.10,
        fixed_stop_pct=0.05,
        liquidate_at_end=False,
    )

    entry = result["transactions"].iloc[0]
    assert entry["Date"] == history.loc[1, "Date"]
    assert entry["action"] == "SELL_SHORT"
    assert entry["side"] == "SHORT"
    assert result["equity_curve"].loc[1, "decision_signal"] == pytest.approx(-0.5)


def test_bearish_indicator_signal_does_not_short_when_disabled():
    history = _history(
        opens=[100, 100, 100],
        highs=[101, 101, 101],
        lows=[99, 99, 99],
        closes=[100, 100, 100],
    )
    result = run_price_intent_backtest(
        history,
        None,
        entry_limit=None,
        capital=100,
        use_whole_shares=False,
        entry_at_market=True,
        entry_signal=[-0.5, 0, 0],
        allow_short=False,
        target_pct=0.10,
        fixed_stop_pct=0.05,
        liquidate_at_end=False,
    )

    assert result["transactions"].empty


def test_bearish_indicator_sells_existing_long_when_shorting_is_disabled():
    history = _history(
        opens=[100, 100, 90],
        highs=[101, 101, 91],
        lows=[99, 99, 89],
        closes=[100, 100, 90],
    )
    result = run_price_intent_backtest(
        history,
        None,
        entry_limit=None,
        capital=100,
        use_whole_shares=False,
        entry_at_market=True,
        entry_signal=[0.5, -0.5, 0],
        allow_short=False,
        target_pct=0.50,
        fixed_stop_pct=0.50,
        liquidate_at_end=False,
    )

    assert result["transactions"]["action"].tolist() == ["BUY", "SELL"]
    assert result["transactions"]["reason"].tolist() == [
        "MARKET_ENTRY",
        "SIGNAL_EXIT",
    ]
    assert result["transactions"].iloc[-1]["fill_price"] == pytest.approx(90)
