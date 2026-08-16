import pandas as pd

from backtesting.signal import run_composite_signal_backtest


def test_composite_signal_is_lagged_one_session():
    predictions = pd.DataFrame({
        "Date": pd.date_range("2026-01-01", periods=3),
        "Open": [10, 11, 12],
        "High": [11, 12, 13],
        "Low": [9, 10, 11],
        "Close": [10, 11, 12],
        "Adj Close": [10, 11, 12],
        "Volume": [1000, 1000, 1000],
        "Signal": [0.5, -0.5, -0.5],
    })

    result = run_composite_signal_backtest(
        predictions,
        long_entry_threshold=0.1,
        long_exit_threshold=0.0,
        capital=100,
    )

    assert list(result["transactions"]["Date"]) == [
        pd.Timestamp("2026-01-02"),
        pd.Timestamp("2026-01-03"),
    ]
    assert list(result["transactions"]["reason"]) == [
        "LAGGED_SIGNAL_ENTRY",
        "SIGNAL_EXIT",
    ]
    assert result["summary"].iloc[0]["signal_lag_sessions"] == 1


def test_first_session_signal_cannot_trade_same_session():
    predictions = pd.DataFrame({
        "Date": [pd.Timestamp("2026-01-01")],
        "Open": [10], "High": [11], "Low": [9], "Close": [10],
        "Adj Close": [10], "Volume": [1000], "Signal": [1.0],
    })

    result = run_composite_signal_backtest(predictions, capital=100)

    assert result["transactions"].empty
