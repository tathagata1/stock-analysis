import analysis_functions.sentiment_analysis as sentiment
import pytest


def test_malformed_sentiment_payload_does_not_discard_valid_scores(monkeypatch):
    monkeypatch.setattr(
        sentiment,
        "get_news_sentiment",
        lambda stock, payloads: [
            '{"score": 0.6, "confidence": 0.95}',
            "not json",
            '{"score": -0.2, "confidence": 0.99}',
        ],
    )
    assert sentiment.apply_sentiment_analysis("TEST") == pytest.approx(0.2)
