"""S7：MCP 工具的 token 归属校验（不依赖 FastMCP —— `make_tools` 是纯函数）。

背景：sidecar 有 `_owned()` → 403 token_owner_mismatch，MCP 之前没有任何归属检查，
只靠 token 保密当权限。`facade.owner_of()` 已存在，这里复用。

RIPR：Reachability=走到 rsc_write 的所有权分支；Infection=owner != aid；
Propagation=返回 token_owner_mismatch 且账本零写入；Revealability=错误码精确。
"""

from __future__ import annotations

from conftest import make_harness

from harness import RscSession
from harness.integrations.mcp_server import make_tools


def _session(h, tmp_path):
    return RscSession(h, worktrees=None, state_dir=str(tmp_path / "state"))


def test_cross_agent_token_rejected(tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, tmp_path)
    alice = make_tools(s, agent_id="alice")
    bob = make_tools(s, agent_id="bob")

    token = alice["rsc_begin"](["a.txt"])["token"]
    out = bob["rsc_write"](token, "a.txt", "v", "K1")
    assert out == {"error": "token_owner_mismatch"}, out
    assert h.workspace.version("a.txt") == 0, "越权写必须零副作用"

    ok = alice["rsc_write"](token, "a.txt", "v", "K1")
    assert "receipt_id" in ok, "真正的 owner 仍可写"


def test_cross_agent_cannot_finish_or_gate(tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, tmp_path)
    alice = make_tools(s, agent_id="alice")
    bob = make_tools(s, agent_id="bob")

    token = alice["rsc_begin"](["a.txt"])["token"]
    assert bob["rsc_finish"](token) == {"error": "token_owner_mismatch"}
    assert bob["rsc_gate"](token) == {"error": "token_owner_mismatch"}
    assert bob["rsc_snapshot"](token) == {"error": "token_owner_mismatch"}

    # 未被误结算：owner 仍能 finish
    assert alice["rsc_finish"](token)["status"] == "OK"


def test_unknown_token_keeps_old_error_path(tmp_path):
    h = make_harness(run_limits=1000)
    tools = make_tools(_session(h, tmp_path), agent_id="alice")
    out = tools["rsc_write"]("no-such-token", "a.txt", "v", "K1")
    assert "token_owner_mismatch" not in str(out)
    assert "error" in out
