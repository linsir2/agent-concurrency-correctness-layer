"""S23-2：OpenAI Agents SDK 适配器（guardrail + root 注入）。

不跑真模型：直接构造 `ToolContext` 并驱动 guardrail / `on_invoke_tool`，
与 SDK 内部调用顺序一致（input guardrail → 工具 → output guardrail）。
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess

import pytest

agents = pytest.importorskip("agents", reason="需要 openai-agents 0.13.x")

from agents import Agent, FunctionTool
from agents.tool_context import ToolContext
from agents.tool_guardrails import (
    ToolInputGuardrailData,
    ToolOutputGuardrailData,
)
from conftest import make_harness

from harness import RscSession
from harness.facade import Receipt
from harness.integrations.openai_agents import rsc_agent
from harness.worktree import WorktreeManager


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args],
                   check=True, capture_output=True, text=True)


@pytest.fixture()
def repo(tmp_path):
    base = tmp_path / "repo"
    base.mkdir()
    _git(base, "init", "-q")
    _git(base, "config", "user.email", "t@example.com")
    _git(base, "config", "user.name", "t")
    (base / "seed.txt").write_text("init", encoding="utf-8")
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


@pytest.fixture()
def session(tmp_path, repo):
    h = make_harness(run_limits=1000)
    return RscSession(h, tenant="t", min_budget=1,
                      worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                      state_dir=str(tmp_path / "state"))


SCHEMA = {"type": "object",
          "properties": {"root": {"type": "string"},
                         "value": {"type": "string"}},
          "required": ["root", "value"]}


def _make_tool(hook=None):
    calls = {"n": 0}

    async def invoke(ctx, input_json):
        args = json.loads(input_json)
        calls["n"] += 1
        if hook is not None:
            hook()
        with open(os.path.join(args["root"], "a.txt"), "w", encoding="utf-8") as fh:  # noqa: ASYNC230  # 测试内同步写
            fh.write(args["value"])
        return "ok"

    tool = FunctionTool(name="write_file", description="write a.txt",
                        params_json_schema=json.loads(json.dumps(SCHEMA)),
                        on_invoke_tool=invoke)
    return tool, calls


def _ctx(call_id: str, arguments: str = '{"value":"v1"}') -> ToolContext:
    # `usage` 传哑对象：0.13.6 与新版 `openai` 的 InputTokensDetails 字段漂移，
    # 默认构造会 ValidationError；guardrail 不用 usage。
    return ToolContext(context=None, usage=object(), tool_name="write_file",
                       tool_call_id=call_id, tool_arguments=arguments)


def _drive(tool, ctx, arguments='{"value":"v1"}'):
    """按 SDK 顺序：input → invoke → output；返回 (input 结果, 输出值, output 结果)。"""
    g_in = asyncio.run(tool.tool_input_guardrails[0].run(
        ToolInputGuardrailData(context=ctx, agent=None)))
    out = None
    if g_in.behavior["type"] == "allow":
        out = asyncio.run(tool.on_invoke_tool(ctx, arguments))
        g_out = asyncio.run(tool.tool_output_guardrails[0].run(
            ToolOutputGuardrailData(context=ctx, agent=None, output=out)))
    else:
        g_out = None
    return g_in, out, g_out


# ---------------------------------------------------------------- 用例
def test_happy_path_commits_and_strips_root(session):
    h = session.h
    tool, calls = _make_tool()
    agent = rsc_agent(session, Agent(name="a1", tools=[tool]),
                      writes={"write_file": ("a.txt",)})
    wrapped = agent.tools[0]
    assert wrapped is not tool
    # root 已从 schema 移除（模型不必提供）
    assert "root" not in wrapped.params_json_schema["properties"]
    assert wrapped.params_json_schema["required"] == ["value"]

    ctx = _ctx("call-1")
    g_in, out, g_out = _drive(wrapped, ctx)
    assert g_in.behavior["type"] == "allow"
    assert out == "ok" and calls["n"] == 1
    assert g_out.behavior["type"] == "allow"
    assert h.workspace.get("a.txt") == b"v1"
    assert h.workspace.version("a.txt") == 1


def test_same_args_different_call_id_dedups(session):
    seen: list = []
    orig = session.apply

    def spy(*a, **kw):
        r = orig(*a, **kw)
        seen.append(r)
        return r

    session.apply = spy                                   # type: ignore[method-assign]
    tool, _ = _make_tool()
    agent = rsc_agent(session, Agent(name="a1", tools=[tool]),
                      writes={"write_file": ("a.txt",)})
    wrapped = agent.tools[0]
    assert _drive(wrapped, _ctx("c1"))[0].behavior["type"] == "allow"
    assert _drive(wrapped, _ctx("c2"))[0].behavior["type"] == "allow"
    assert all(isinstance(r, Receipt) for r in seen), seen
    assert seen[0].receipt_id == seen[1].receipt_id and seen[1].dedup is True
    assert session.h.workspace.version("a.txt") == 1


def test_arguments_are_canonicalized_for_idempotency(session):
    """评审 P1：JSON 空白差异不得产生不同业务键。"""
    seen: list = []
    orig = session.apply

    def spy(*a, **kw):
        r = orig(*a, **kw)
        seen.append(r)
        return r

    session.apply = spy                                   # type: ignore[method-assign]
    tool, _ = _make_tool()
    agent = rsc_agent(session, Agent(name="a1", tools=[tool]),
                      writes={"write_file": ("a.txt",)})
    wrapped = agent.tools[0]
    assert _drive(wrapped, _ctx("c1", '{"value":"v1"}'))[0].behavior["type"] == "allow"
    assert _drive(wrapped, _ctx("c2", '{"value": "v1"}'))[0].behavior["type"] == "allow"
    assert seen[0].receipt_id == seen[1].receipt_id and seen[1].dedup is True
    assert session.h.workspace.version("a.txt") == 1


def test_tool_exception_aborts_token(session):
    """评审 P1：工具抛错时 SDK 不跑 output guardrail → 适配器必须自撤 token。"""
    async def boom(ctx, input_json):
        raise RuntimeError("tool failed")

    tool = FunctionTool(name="write_file", description="boom",
                        params_json_schema=json.loads(json.dumps(SCHEMA)),
                        on_invoke_tool=boom)
    agent = rsc_agent(session, Agent(name="a1", tools=[tool]),
                      writes={"write_file": ("a.txt",)})
    wrapped = agent.tools[0]
    ctx = _ctx("c1")
    g_in = asyncio.run(wrapped.tool_input_guardrails[0].run(
        ToolInputGuardrailData(context=ctx, agent=None)))
    assert g_in.behavior["type"] == "allow"
    token = next(iter(session._index))
    with pytest.raises(RuntimeError):
        asyncio.run(wrapped.on_invoke_tool(ctx, '{"value":"v1"}'))
    assert token not in session._index, "token 索引必须清干净"
    assert session.h.acbs[token].state.value in ("CANCELLED", "FAILED"), \
        session.h.acbs[token].state


def test_missing_tool_call_id_trips(session):
    tool, calls = _make_tool()
    agent = rsc_agent(session, Agent(name="a1", tools=[tool]),
                      writes={"write_file": ("a.txt",)})
    ctx = ToolContext(context=None, usage=object(), tool_name="write_file",
                      tool_call_id="", tool_arguments="{}")
    g = asyncio.run(agent.tools[0].tool_input_guardrails[0].run(
        ToolInputGuardrailData(context=ctx, agent=None)))
    assert g.behavior["type"] == "raise_exception"
    assert calls["n"] == 0


def test_stale_write_rejects_content(session, tmp_path, repo):
    other = RscSession(session.h, tenant="t", min_budget=1,
                       worktrees=WorktreeManager(str(repo), str(tmp_path / "wt2")),
                       state_dir=str(tmp_path / "state2"))

    def interleave():
        tok = other.begin("other", "race", writes=("a.txt",))
        other.apply(tok.token, [("a.txt", b"other")],
                    idempotency_key="other-1", agent_id="other")

    tool, _ = _make_tool(hook=interleave)
    agent = rsc_agent(session, Agent(name="a1", tools=[tool]),
                      writes={"write_file": ("a.txt",)})
    _, out, g_out = _drive(agent.tools[0], _ctx("c1"))
    assert out == "ok"
    assert g_out.behavior["type"] == "reject_content"
    assert "RSC_CONFLICT" in g_out.behavior["message"]
    assert session.h.workspace.get("a.txt") == b"other"


def test_root_missing_without_shared_cwd_raises(session):
    tool = FunctionTool(name="plain", description="no root",
                        params_json_schema={"type": "object",
                                            "properties": {"value": {"type": "string"}},
                                            "required": ["value"]},
                        on_invoke_tool=lambda ctx, s: None)
    with pytest.raises(ValueError):
        rsc_agent(session, Agent(name="a1", tools=[tool]),
                  writes={"plain": ("a.txt",)})


def test_output_without_input_token_trips(session):
    tool, _ = _make_tool()
    agent = rsc_agent(session, Agent(name="a1", tools=[tool]),
                      writes={"write_file": ("a.txt",)})
    g = asyncio.run(agent.tools[0].tool_output_guardrails[0].run(
        ToolOutputGuardrailData(context=_ctx("never-seen"), agent=None,
                                output="x")))
    assert g.behavior["type"] == "raise_exception"


def test_shared_cwd_mode(tmp_path, repo, monkeypatch):
    h = make_harness(run_limits=1000)
    session = RscSession(h, tenant="t", min_budget=1,
                         worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                         state_dir=str(tmp_path / "state"))
    monkeypatch.chdir(tmp_path)

    async def plain(ctx, input_json):
        value = json.loads(input_json)["value"]
        with open("a.txt", "w", encoding="utf-8") as fh:  # noqa: ASYNC230
            fh.write(value)
        return "ok"

    tool = FunctionTool(name="plain", description="no root",
                        params_json_schema={"type": "object",
                                            "properties": {"value": {"type": "string"}},
                                            "required": ["value"]},
                        on_invoke_tool=plain)
    agent = rsc_agent(session, Agent(name="a1", tools=[tool]),
                      writes={"plain": ("a.txt",)}, shared_cwd=True)
    wrapped = agent.tools[0]
    g_in, _out, g_out = _drive(wrapped, _ctx("c1"))
    assert g_in.behavior["type"] == "allow" and g_out.behavior["type"] == "allow"
    assert h.workspace.get("a.txt") == b"v1"
