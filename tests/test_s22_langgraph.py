"""S22 C8：LangGraph 中间件（sync/async、root 注入、读回、失败映射）。

不跑真模型：构造真实 `ToolCallRequest`/`ToolRuntime`，直接调中间件方法。
评审新增回归：懒建路径、DELETE、worktree 缺失、未钉住拒绝、async 冲突、
root_arg 签名不匹配 passthrough。
"""

from __future__ import annotations

import asyncio
import os
import subprocess

import pytest

langchain = pytest.importorskip("langchain", reason="需要 langchain/langgraph")

from conftest import make_harness
from langchain_core.tools import tool as lc_tool
from langgraph.prebuilt.tool_node import ToolCallRequest, ToolRuntime
from langgraph.types import Command

from harness import ManualClock, RscSession
from harness.facade import Deny
from harness.integrations.langgraph import RscDenied, RscMiddleware, rsc_writes
from harness.worktree import WorktreeManager


# ---------------------------------------------------------------- helpers
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
    (base / "a.txt").write_text("hello", encoding="utf-8")
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


@rsc_writes("a.txt", root_arg="root")
def _edit(root: str, value: str) -> str:
    """Write a.txt under root."""
    with open(os.path.join(root, "a.txt"), "w", encoding="utf-8") as fh:
        fh.write(value)
    return "ok"


_EDIT_TOOL = lc_tool(_edit)


def _request(tool_call_id: str, args: dict, state: dict, agent: str = "a1",
             tool=None):
    state = dict(state, agent_id=agent)
    runtime = ToolRuntime(state=state, context={"agent_id": agent},
                          config={"thread_id": f"th-{agent}"},
                          stream_writer=None, tool_call_id=tool_call_id, store=None)
    return ToolCallRequest(
        tool_call={"name": "edit", "args": args, "id": tool_call_id},
        tool=tool or _EDIT_TOOL, state=state, runtime=runtime)


def _handler(request):
    args = request.tool_call["args"]
    return _edit(root=args["root"], value=args["value"])


def _session(h, repo, tmp_path):
    return RscSession(h, worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                      state_dir=str(tmp_path / "state"))


def _rt(agent="a1"):
    return type("RT", (), {"context": {"agent_id": agent}, "config": {}})()


# ================================================================ 未声明 → 放行
def test_c8_undeclared_tool_passes_through(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))

    def plain(x: int) -> int:
        """Double it."""
        return x

    tool = lc_tool(plain)
    req = ToolCallRequest(
        tool_call={"name": "plain", "args": {"x": 1}, "id": "c0"},
        tool=tool, state={},
        runtime=ToolRuntime(state={}, context={}, config={},
                            stream_writer=None, tool_call_id="c0", store=None))
    assert m.wrap_tool_call(req, lambda r: "passthrough") == "passthrough"
    assert h.workspace.get("a.txt") is None


# ================================================================ root 注入 + 提交
def test_c8_root_injection_and_commit(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))
    state = {}
    tok = m.before_agent(state, _rt("a1"))
    assert tok["rsc_token"]
    state = {**state, **tok}
    req = _request("c1", {"value": "from-tool"}, state)

    assert m.wrap_tool_call(req, _handler) == "ok"
    assert h.workspace.get("a.txt") == b"from-tool"
    m.after_agent(state, None)
    assert s.worktree_for(tok["rsc_token"]) is None


# ================================================================ async
def test_c8_async_variant(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))
    state = { **m.before_agent({}, _rt("a1"))}

    async def handler(request):
        return _handler(request)

    out = asyncio.run(m.awrap_tool_call(
        _request("c2", {"value": "async"}, state), handler))
    assert out == "ok"
    assert h.workspace.get("a.txt") == b"async"


def test_c8_async_conflict(repo, tmp_path):
    """async 的非 happy 分支：并发写 → Command。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m1 = RscMiddleware(s, agent_id_from=lambda st, rt: "a1", writes=("a.txt",))
    m2 = RscMiddleware(s, agent_id_from=lambda st, rt: "a2", writes=("a.txt",))
    st1 = m1.before_agent({}, None)
    st2 = m2.before_agent({}, None)

    assert m1.wrap_tool_call(_request("c1", {"value": "v1"}, st1, "a1"),
                             _handler) == "ok"

    async def handler(request):
        return _handler(request)

    out = asyncio.run(m2.awrap_tool_call(
        _request("c2", {"value": "v2"}, st2, "a2"), handler))
    assert isinstance(out, Command)
    assert out.update["messages"][0].tool_call_id == "c2"
    assert h.workspace.get("a.txt") == b"v1"


# ================================================================ 冲突 → Command
def test_c8_conflict_returns_command(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m1 = RscMiddleware(s, agent_id_from=lambda st, rt: "a1", writes=("a.txt",))
    m2 = RscMiddleware(s, agent_id_from=lambda st, rt: "a2", writes=("a.txt",))
    st1 = m1.before_agent({}, None)
    st2 = m2.before_agent({}, None)

    assert m1.wrap_tool_call(_request("c1", {"value": "v1"}, st1, "a1"),
                             _handler) == "ok"
    out = m2.wrap_tool_call(_request("c2", {"value": "v2"}, st2, "a2"), _handler)

    assert isinstance(out, Command)
    msg = out.update["messages"][0]
    assert msg.tool_call_id == "c2"
    assert msg.status == "error" and "CONFLICT" in msg.content
    assert h.workspace.get("a.txt") == b"v1"


# ================================================================ 过期 → 抛错
def test_c8_expired_token_raises(repo, tmp_path):
    h = make_harness(run_limits=1000, clock=ManualClock())
    s = RscSession(h, worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                   token_ttl_ms=1000, state_dir=str(tmp_path / "state"))
    m = RscMiddleware(s, writes=("a.txt",))
    state = { **m.before_agent({}, _rt("a1"))}
    h.clock.advance(1001)
    h.tick()
    with pytest.raises(RscDenied):
        m.wrap_tool_call(_request("c1", {"value": "x"}, state), _handler)


# ================================================================ Wait / Deny 映射
def test_c8_wait_returns_error_toolmessage(monkeypatch, repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))
    state = { **m.before_agent({}, _rt("a1"))}
    monkeypatch.setattr(s, "apply", lambda *a, **k: __import__(
        "harness.facade", fromlist=["Wait"]).Wait("budget"))
    out = m.wrap_tool_call(_request("c1", {"value": "x"}, state), _handler)
    assert out.status == "error" and "RSC_WAIT" in out.content
    assert out.tool_call_id == "c1"


def test_c8_deny_raises(monkeypatch, repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))
    state = { **m.before_agent({}, _rt("a1"))}
    monkeypatch.setattr(s, "apply", lambda *a, **k: Deny("capability"))
    with pytest.raises(RscDenied):
        m.wrap_tool_call(_request("c1", {"value": "x"}, state), _handler)


# ================================================================ 评审新增回归
def test_c8_no_session_denied(repo, tmp_path):
    """评审 P1-1：不用 before_agent → fail-closed（不懒建、不静默覆盖）。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))
    with pytest.raises(RscDenied) as ei:
        m.wrap_tool_call(_request("c1", {"value": "v1"}, {}, "a1"), _handler)
    assert "no_session" in str(ei.value)
    assert h.workspace.get("a.txt") is None


def test_c8_delete_read_back(repo, tmp_path):
    """评审：工具删掉文件 → 提交 DELETE。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))
    state = { **m.before_agent({}, _rt("a1"))}
    assert m.wrap_tool_call(_request("c1", {"value": "v1"}, state), _handler) == "ok"

    def deleter(request):
        os.remove(os.path.join(request.tool_call["args"]["root"], "a.txt"))
        return "deleted"

    out = m.wrap_tool_call(_request("c2", {"value": "v2"}, state), deleter)
    assert out == "deleted"
    assert h.workspace.get("a.txt") is None, "删除必须落账"


def test_c8_worktree_missing_denied(repo, tmp_path):
    """评审 P0：worktree 目录消失 → 在执行工具**之前**就 fail-closed。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))
    state = { **m.before_agent({}, _rt("a1"))}
    assert m.wrap_tool_call(_request("c1", {"value": "v1"}, state), _handler) == "ok"
    s.worktrees.destroy(s.worktree_for(state["rsc_token"]))
    with pytest.raises(RscDenied) as ei:
        m.wrap_tool_call(_request("c2", {"value": "v2"}, state), _handler)
    assert "worktree_missing" in str(ei.value)
    assert h.workspace.get("a.txt") == b"v1", "worktree 缺失不得误删账本"


def test_c8_unpinned_resource_denied(repo, tmp_path):
    """评审 P1：`writes` 漏传 → 未钉住资源硬拒绝（不静默覆盖）。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, agent_id_from=lambda st, rt: "a1", writes=("other.txt",))
    state = { **m.before_agent({}, _rt("a1"))}
    with pytest.raises(RscDenied) as ei:
        m.wrap_tool_call(_request("c1", {"value": "v"}, state), _handler)
    assert "unpinned" in str(ei.value)


def test_c8_root_arg_signature_mismatch_passthrough(repo, tmp_path):
    """评审 P1-9：工具不接 root → passthrough（LangChain 会静默吞掉参数）。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))

    @rsc_writes("a.txt", root_arg="root")
    def no_root(value: str) -> str:
        """No root param."""
        return value

    tool = lc_tool(no_root)
    state = { **m.before_agent({}, _rt("a1"))}
    out = m.wrap_tool_call(_request("c1", {"value": "v"}, state, tool=tool),
                           lambda r: "passthrough")
    assert out == "passthrough"
    assert h.workspace.get("a.txt") is None


# ================================================================ 幂等键
def test_c8_idempotency_key_stable_across_retries(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    m = RscMiddleware(s, writes=("a.txt",))
    state = { **m.before_agent({}, _rt("a1"))}
    req = _request("same-call", {"value": "v"}, state)
    assert m.wrap_tool_call(req, _handler) == "ok"
    n = len([e for e in h.event_log.all() if e.kind.value == "RESOURCE_COMMITTED"])
    assert m.wrap_tool_call(req, _handler) == "ok"
    assert len([e for e in h.event_log.all()
                if e.kind.value == "RESOURCE_COMMITTED"]) == n


# ================================================================ 需要 Z3
def test_c8_requires_worktrees(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = RscSession(h, worktrees=None)
    with pytest.raises(ValueError):
        RscMiddleware(s)
