import numpy as np
import pandas as pd

import config.config as config
from analysis_functions.technical_analysis import (
    calculate_buy_score,
    calculate_sell_score,
    get_technical_analysis_calculations,
    high_volume,
)


def test_high_volume_is_prefix_invariant():
    prefix = pd.DataFrame({"Volume": [100, 100, 400]})
    extended = pd.DataFrame({"Volume": [100, 100, 400, 10000]})

    prefix_flags = high_volume(prefix, window=2)
    extended_flags = high_volume(extended, window=2).iloc[: len(prefix)]

    pd.testing.assert_series_equal(
        prefix_flags.reset_index(drop=True),
        extended_flags.reset_index(drop=True),
    )
    assert bool(prefix_flags.iloc[-1]) is True


def test_missing_indicators_do_not_create_bearish_penalty():
    row = pd.Series({
        "RSI": np.nan,
        "SMA10": np.nan,
        "SMA20": np.nan,
        "EMA10": np.nan,
        "EMA20": np.nan,
        "Close": 10.0,
        "VWAP": np.nan,
        "%K": np.nan,
        "%D": np.nan,
        "Lower_Band": np.nan,
        "Upper_Band": np.nan,
        "MACD": np.nan,
        "Signal_Line": np.nan,
        "High_Volume": pd.NA,
        "ATR_Pct": np.nan,
    })

    assert calculate_buy_score(row) == 0
    assert calculate_sell_score(row) == 0


def test_short_history_is_marked_unavailable():
    rows = 20
    frame = pd.DataFrame({
        "Date": pd.date_range("2026-01-01", periods=rows),
        "Open": np.arange(rows) + 100,
        "High": np.arange(rows) + 102,
        "Low": np.arange(rows) + 99,
        "Close": np.arange(rows) + 101,
        "Adj Close": np.arange(rows) + 101,
        "Volume": np.arange(rows) * 100 + 1000,
        "TICKER": "TEST",
    })

    result = get_technical_analysis_calculations(frame)

    assert len(result) < config.TECHNICAL_MIN_HISTORY
    assert not result["technical_data_available"].any()
    assert result["ATR_Pct"].isna().all()

