import numpy as np
import pandas as pd
import pytest

import analysis_types.prediction as prediction


def _signal_frame(**overrides):
    values = {
        "technical_analysis_buy_score": [1.0],
        "technical_analysis_sell_score": [0.0],
        "technical_data_available": [True],
        "sentiment_analysis_score": [np.nan],
        "fundamental_analysis_score": [1.0],
        "fundamental_analysis_coverage": [1.0],
        "multifactor_analysis_score": [1.0],
        "multifactor_analysis_coverage": [1.0],
    }
    values.update(overrides)
    return pd.DataFrame(values)


def test_missing_sentiment_is_excluded_from_weight_denominator(monkeypatch):
    monkeypatch.setattr(prediction.config, "TECHNICAL_SIGNAL_WEIGHT", 1.0)
    monkeypatch.setattr(prediction.config, "SENTIMENT_SIGNAL_WEIGHT", 1.0)
    monkeypatch.setattr(prediction.config, "FUNDAMENTAL_SIGNAL_WEIGHT", 1.0)
    monkeypatch.setattr(prediction.config, "MULTIFACTOR_SIGNAL_WEIGHT", 1.0)
    monkeypatch.setattr(prediction.config, "MIN_SIGNAL_COVERAGE", 0.5)

    result = prediction.add_total_signal(_signal_frame())

    assert result.loc[0, "Signal"] == pytest.approx(1.0)
    assert result.loc[0, "signal_coverage"] == pytest.approx(0.75)
    assert result.loc[0, "analysis_status"] == "PARTIAL"


def test_low_coverage_is_insufficient_data(monkeypatch):
    monkeypatch.setattr(prediction.config, "TECHNICAL_SIGNAL_WEIGHT", 1.0)
    monkeypatch.setattr(prediction.config, "SENTIMENT_SIGNAL_WEIGHT", 1.0)
    monkeypatch.setattr(prediction.config, "FUNDAMENTAL_SIGNAL_WEIGHT", 1.0)
    monkeypatch.setattr(prediction.config, "MULTIFACTOR_SIGNAL_WEIGHT", 1.0)
    monkeypatch.setattr(prediction.config, "MIN_SIGNAL_COVERAGE", 0.5)
    frame = _signal_frame(
        fundamental_analysis_score=[np.nan],
        fundamental_analysis_coverage=[0.0],
        multifactor_analysis_score=[np.nan],
        multifactor_analysis_coverage=[0.0],
    )

    result = prediction.convert_signal_to_text(prediction.add_total_signal(frame))

    assert pd.isna(result.loc[0, "Signal"])
    assert result.loc[0, "Signal_Text"] == "INSUFFICIENT DATA"


def test_empty_history_fails_before_external_enrichment():
    with pytest.raises(ValueError, match="No price history"):
        prediction.get_prediction(pd.DataFrame())

