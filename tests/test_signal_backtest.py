import pandas as pd

import config.config as config
from backtesting.signal import (
    build_technical_entry_signal,
    run_composite_signal_backtest,
)


def test_technical_entry_signal_waits_for_complete_indicators():
    rows = config.TECHNICAL_MIN_HISTORY + 5
    prices = pd.Series(range(rows), dtype=float) + 100
    history = pd.DataFrame({
        "Date": pd.date_range("2025-01-01", periods=rows),
        "Open": prices,
        "High": prices + 2,
        "Low": prices - 2,
        "Close": prices + 1,
        "Adj Close": prices + 1,
        "Volume": pd.Series(range(rows), dtype=float) * 100 + 1000,
    })

    signal = build_technical_entry_signal(history)

    assert signal.iloc[: config.TECHNICAL_MIN_HISTORY - 1].isna().all()
    assert signal.iloc[config.TECHNICAL_MIN_HISTORY - 1 :].notna().all()


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
