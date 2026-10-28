"""S22 C9：MCP server —— 直接调 `make_tools` 返回的函数（不走 stdio）。

评审修订：`agent_id` 只由 server 配置；`rsc_begin(writes)` 必填（OCC 钉住点）；
`rsc_write` 提供多步会话的写入口。
"""

from __future__ import annotations

import pytest

mcp = pytest.importorskip("mcp", reason="需要 MCP SDK")

from conftest import make_harness

from harness import RscSession
from harness.integrations.mcp_server import build_mcp_server, make_tools


def _session(h, tmp_path):
    return RscSession(h, worktrees=None, state_dir=str(tmp_path / "state"))


# ================================================================ 单次提交
def test_c9_apply_returns_receipt(tmp_path):
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    out = tools["rsc_apply"]("a.txt", "hello", "K1")
    assert "receipt_id" in out and out["dedup"] is False
    assert h.workspace.get("a.txt") == "hello"
    assert h.budget.remaining("run") == 999


def test_c9_apply_idempotent(tmp_path):
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    r1 = tools["rsc_apply"]("a.txt", "v", "K")
    r2 = tools["rsc_apply"]("a.txt", "v", "K")
    assert r1["receipt_id"] == r2["receipt_id"] and r2["dedup"] is True
    assert len([e for e in h.event_log.all()
                if e.kind.value == "RESOURCE_COMMITTED"]) == 1


def test_c9_apply_conflict(tmp_path):
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    tools["rsc_apply"]("a.txt", "v1", "K")
    out = tools["rsc_apply"]("a.txt", "v2", "K")
    assert "conflict" in out


def test_c9_apply_requires_key(tmp_path):
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    assert "error" in tools["rsc_apply"]("a.txt", "v", "")


def test_c9_apply_delete(tmp_path):
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    tools["rsc_apply"]("a.txt", "v", "K1")
    out = tools["rsc_apply"]("a.txt", None, "K2")
    assert "receipt_id" in out
    assert h.workspace.get("a.txt") is None


def test_c9_apply_does_not_leak_token(tmp_path):
    """评审 P2：rsc_apply 成功后不得返回已失效的 token。"""
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    out = tools["rsc_apply"]("a.txt", "v", "K")
    assert "token" not in out


# ================================================================ 多步会话
def test_c9_begin_write_finish_gate(tmp_path):
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    tok = tools["rsc_begin"](["a.txt"])
    assert "token" in tok
    token = tok["token"]

    assert tools["rsc_snapshot"](token) == {"a.txt": 0}, (
        "writes 必填 → base_versions 必须钉住")
    assert tools["rsc_gate"](token)["verdict"] == "allow"

    w = tools["rsc_write"](token, "a.txt", "v1", "K1")
    assert "receipt_id" in w
    assert h.workspace.get("a.txt") == "v1"

    fin = tools["rsc_finish"](token)
    assert fin["status"] == "OK"
    assert tools["rsc_gate"](token)["verdict"] == "deny"


def test_c9_begin_requires_writes(tmp_path):
    """评审 P1：`writes` 漏传 → base_versions 为空 → gate 失去 OCC。"""
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    tok = tools["rsc_begin"]([])
    assert tools["rsc_snapshot"](tok["token"]) == {}


def test_c9_agent_id_not_caller_declared(tmp_path):
    """评审 P0：`rsc_begin` 不接受调用方声明的 agent_id（防冒名）。"""
    import inspect
    tools = make_tools(_session(make_harness(), tmp_path), agent_id="victim")
    sig = inspect.signature(tools["rsc_begin"])
    assert "agent_id" not in sig.parameters, "agent_id 必须只由 server 配置"


# ================================================================ server 装配
def test_c9_build_mcp_server_registers_tools(tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, tmp_path)
    server = build_mcp_server(s, name="rsc-test")
    names = set(server._tool_manager._tools)
    assert {"rsc_apply", "rsc_write", "rsc_begin", "rsc_finish",
            "rsc_gate", "rsc_snapshot"} <= names


def test_c9_unknown_token_denied(tmp_path):
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path))
    assert tools["rsc_gate"]("nope")["verdict"] == "deny"
