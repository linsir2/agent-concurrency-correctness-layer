"""S24-9：`LLMClient` 的 usage 解析（真实 token 计量的前提）。

RIPR：
- Reachability：`chat_with_usage` 走真实响应路径（monkeypatch `_post`）；
- Infection：OpenAI / Anthropic 两套字段名都能取到正确数值；
- Propagation：缺 usage → `usage_missing=True`（**不估算**）；
- Revealability：`chat()` 仍只返回文本（既有调用点不变）。
"""

from __future__ import annotations

from harness.llm import LLMClient, LLMConfig, LLMReply, usage_from_response


def _client(monkeypatch, response: dict):
    import harness.llm as mod

    monkeypatch.setattr(mod, "_post", lambda *a, **kw: response)
    return LLMClient(LLMConfig(api_key="test-key", max_retries=0))


def test_openai_style_usage(monkeypatch):
    client = _client(monkeypatch, {
        "model": "m1",
        "choices": [{"message": {"content": "hello"}}],
        "usage": {"prompt_tokens": 11, "completion_tokens": 7,
                  "total_tokens": 18,
                  "prompt_tokens_details": {"cached_tokens": 3},
                  "completion_tokens_details": {"reasoning_tokens": 2}},
    })
    rep = client.chat_with_usage([{"role": "user", "content": "x"}])
    assert isinstance(rep, LLMReply)
    assert (rep.text, rep.prompt_tokens, rep.completion_tokens) == ("hello", 11, 7)
    assert rep.total_tokens == 18
    assert rep.cached_tokens == 3
    assert rep.reasoning_tokens == 2
    assert rep.model == "m1" and rep.attempts == 1
    assert rep.tokens == 18


def test_anthropic_style_usage():
    got = usage_from_response({"usage": {
        "input_tokens": 10, "output_tokens": 4,
        "cache_read_input_tokens": 6,
        "cache_creation_input_tokens": 2}})
    assert got["prompt_tokens"] == 10
    assert got["completion_tokens"] == 4
    assert got["cached_tokens"] == 6
    assert got["total_tokens"] == 14
    assert got["usage_missing"] is False


def test_missing_usage_is_marked_not_estimated(monkeypatch):
    client = _client(monkeypatch, {
        "choices": [{"message": {"content": "abc"}}]})
    rep = client.chat_with_usage([{"role": "user", "content": "x"}])
    assert rep.usage_missing is True
    assert rep.total_tokens == 0
    assert rep.tokens == 0                     # 绝不按字符数估算


def test_chat_still_returns_text(monkeypatch):
    client = _client(monkeypatch, {
        "choices": [{"message": {"content": "ok"}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
    assert client.chat([{"role": "user", "content": "x"}]) == "ok"


def test_retry_then_success_records_attempt(monkeypatch):
    import harness.llm as mod

    calls = {"n": 0}

    def flaky(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("boom")
        return {"choices": [{"message": {"content": "late"}}],
                "usage": {"prompt_tokens": 2, "completion_tokens": 3,
                          "total_tokens": 5}}

    monkeypatch.setattr(mod, "_post", flaky)
    cfg = LLMConfig(api_key="k", max_retries=1, backoff_s=0.0)
    rep = LLMClient(cfg).chat_with_usage([{"role": "user", "content": "x"}])
    assert rep.text == "late" and rep.attempts == 2
    assert rep.total_tokens == 5


def test_negative_usage_is_clamped():
    got = usage_from_response({"usage": {"prompt_tokens": -5,
                                         "completion_tokens": "x",
                                         "total_tokens": 3}})
    assert got["prompt_tokens"] == 0 and got["completion_tokens"] == 0
    assert got["total_tokens"] == 3
