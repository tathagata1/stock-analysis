import json
import os
from types import SimpleNamespace

import pandas as pd

import dao.dao as dao


def test_expired_index_cache_is_used_when_refresh_fails(tmp_path, monkeypatch):
    cache_file = tmp_path / "sp500_tickers.json"
    cache_file.write_text(json.dumps({"tickers": ["A", "B"]}), encoding="utf-8")
    os.utime(cache_file, (1, 1))
    monkeypatch.setattr(dao, "get_index_constituents", lambda index_name: [])
    tickers, metadata = dao.get_index_tickers_cached(
        "sp500", cache_dir=tmp_path, max_age_hours=0
    )
    assert tickers == ["A", "B"]
    assert metadata["stale_fallback"] is True
    assert metadata["refresh_failed"] is True
    assert cache_file.exists()


def _statement(rows, dates, values_by_row):
    return pd.DataFrame(
        {date: [values_by_row[row][index] for row in rows] for index, date in enumerate(dates)},
        index=rows,
    )


def test_point_in_time_snapshots_use_ttm_and_conservative_overlap_lag(monkeypatch):
    quarterly_dates = list(pd.to_datetime([
        "2025-09-30", "2025-06-30", "2025-03-31", "2024-12-31", "2024-09-30"
    ]))
    annual_dates = list(pd.to_datetime(["2024-12-31", "2023-12-31"]))
    quarterly_income = _statement(
        ["Total Revenue", "Net Income", "Diluted EPS", "Diluted Average Shares"],
        quarterly_dates,
        {
            "Total Revenue": [40, 30, 20, 10, 5],
            "Net Income": [4, 3, 2, 1, 0.5],
            "Diluted EPS": [0.4, 0.3, 0.2, 0.1, 0.05],
            "Diluted Average Shares": [100, 100, 100, 100, 100],
        },
    )
    annual_income = _statement(
        ["Total Revenue", "Net Income", "Diluted EPS", "Diluted Average Shares"],
        annual_dates,
        {
            "Total Revenue": [100, 80],
            "Net Income": [10, 8],
            "Diluted EPS": [1.0, 0.8],
            "Diluted Average Shares": [100, 100],
        },
    )
    quarterly_balance = _statement(
        ["Total Assets", "Stockholders Equity", "Ordinary Shares Number"],
        quarterly_dates,
        {
            "Total Assets": [500, 480, 460, 440, 420],
            "Stockholders Equity": [250, 240, 230, 220, 210],
            "Ordinary Shares Number": [100, 100, 100, 100, 100],
        },
    )
    annual_balance = _statement(
        ["Total Assets", "Stockholders Equity", "Ordinary Shares Number"],
        annual_dates,
        {
            "Total Assets": [440, 400],
            "Stockholders Equity": [220, 200],
            "Ordinary Shares Number": [100, 100],
        },
    )
    fake = SimpleNamespace(
        quarterly_income_stmt=quarterly_income,
        quarterly_balance_sheet=quarterly_balance,
        quarterly_cashflow=pd.DataFrame(),
        income_stmt=annual_income,
        financials=annual_income,
        balance_sheet=annual_balance,
        cashflow=pd.DataFrame(),
    )
    monkeypatch.setattr(dao.yf, "Ticker", lambda symbol: fake)
    snapshots = dao.get_point_in_time_financial_snapshots("TEST")
    september = snapshots[snapshots["report_date"] == pd.Timestamp("2025-09-30")].iloc[0]
    overlap = snapshots[snapshots["report_date"] == pd.Timestamp("2024-12-31")]
    assert september["statement_frequency"] == "quarterly_ttm"
    assert september["Total Revenue"] == 100
    assert september["Previous Total Revenue"] == 65
    assert september["Shares Outstanding"] == 100
    assert len(overlap) == 1
    assert overlap.iloc[0]["statement_frequency"] == "annual"
    assert overlap.iloc[0]["available_from"] == pd.Timestamp("2025-03-31")


def test_key_statistics_include_target_price():
    assert dao.STAT_FIELD_MAP["Target Mean Price"] == "targetMeanPrice"

