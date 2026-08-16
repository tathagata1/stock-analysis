from types import SimpleNamespace

import dao.dao as dao


def test_gpt_sentiment_uses_strict_json_schema(monkeypatch):
    captured = {}

    class FakeCompletions:
        @staticmethod
        def create(**kwargs):
            captured.update(kwargs)
            message = SimpleNamespace(content='{"score":0.25,"confidence":0.8}')
            return SimpleNamespace(choices=[SimpleNamespace(message=message)])

    class FakeClient:
        def __init__(self, api_key):
            captured["api_key"] = api_key
            self.chat = SimpleNamespace(completions=FakeCompletions())

    monkeypatch.setattr(dao, "OpenAI", FakeClient)
    monkeypatch.setattr(dao.config, "chatgpt_key", "test-key")
    monkeypatch.setattr(dao.config, "OPENAI_SENTIMENT_MODEL", "gpt-5.1")

    payload = dao.get_gpt_score_with_confidence("TEST", "Earnings improved.")

    assert payload == '{"score":0.25,"confidence":0.8}'
    assert captured["api_key"] == "test-key"
    assert captured["model"] == "gpt-5.1"
    schema_format = captured["response_format"]
    assert schema_format["type"] == "json_schema"
    assert schema_format["json_schema"]["strict"] is True
    schema = schema_format["json_schema"]["schema"]
    assert schema["required"] == ["score", "confidence"]
    assert schema["additionalProperties"] is False
    assert "<article>" in captured["messages"][1]["content"]
