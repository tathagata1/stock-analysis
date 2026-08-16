import pandas as pd

import analysis_interfaces.interface_index_search as workflow


def _analysis(ticker):
    return {
        "ticker": ticker,
        "df_pred": pd.DataFrame([{
            "Signal": 0.2,
            "Signal_Text": "WEAK BUY",
            "analysis_status": "COMPLETE",
            "signal_coverage": 1.0,
        }]),
        "stats": pd.Series({"TICKER": ticker}),
        "recent_signal": {
            "signal_text": "WEAK BUY",
            "signal_number": 0.2,
            "analysis_status": "COMPLETE",
            "signal_coverage": 1.0,
        },
    }


def test_constituent_failure_is_recorded_and_scan_continues(monkeypatch):
    monkeypatch.setattr(workflow.dao, "get_index_tickers", lambda **kwargs: ["GOOD", "BAD"])

    def build(ticker, **kwargs):
        if ticker == "BAD":
            raise ValueError("missing data")
        return _analysis(ticker)

    monkeypatch.setattr(workflow, "build_stock_analysis", build)
    result = workflow.run_index_search_workflow("sp500", None, use_ticker_cache=False)
    assert list(result["prediction_summary"]["TICKER"]) == ["GOOD"]
    assert list(result["failures"]["TICKER"]) == ["BAD"]


def test_multi_index_scan_reuses_overlapping_ticker_analysis(monkeypatch):
    universes = {"one": ["A", "B"], "two": ["B", "C"]}
    monkeypatch.setattr(
        workflow.dao, "get_index_tickers",
        lambda index_name, limit=None: universes[index_name],
    )
    calls = []

    def build(ticker, **kwargs):
        calls.append(ticker)
        return _analysis(ticker)

    monkeypatch.setattr(workflow, "build_stock_analysis", build)
    result = workflow.run_multi_index_search_workflow(
        ["one", "two"], {}, use_ticker_cache=False
    )
    assert calls == ["A", "B", "C"]
    assert result["unique_tickers_analyzed"] == 3

