"""#2：能力令牌穿透门面（opt-in）。

背景：`CapabilityToken` 原本只在 `AgentRuntime` 里强制；门面 `RscSession`
直接调 `commit_batch`，完全绕开 runtime → 走门面的所有适配器（6 个宿主 +
sidecar + MCP）四维能力全部失效。

设计（最小改动）：
- `RscSession(capability=...)` 才启用；不传 = 现状（56 处构造点零改动）。
- `begin`：身份绑定 + 声明写集 fail-fast + 把 token 绑到 ACB。
- `apply`：在任何副作用前做能力门禁（path / tool 两维）。

RIPR：
- Reachability：真实走 begin/apply 的所有权与能力分支；
- Infection：cap 的 paths/tools 不含目标，或 agent_id 不匹配；
- Propagation：返回 `Deny.code` 精确，且账本零写入；
- Revealability：错误码带资源/工具名。
"""

from __future__ import annotations

from conftest import make_harness

from harness import Deny, Receipt, RscSession, Token


def _session(h, tmp_path, **kw) -> RscSession:
    return RscSession(h, worktrees=None, state_dir=str(tmp_path / "state"), **kw)


def test_deny_path_outside_scope(tmp_path):
    h = make_harness()
    cap = h.policy.issue("agent-a", tools={"*"}, paths={"a.txt"})
    s = _session(h, tmp_path, capability=cap)
    out = s.begin("agent-a", "goal", writes=["b.txt"])
    assert isinstance(out, Deny) and out.code == "capability_path_denied:b.txt", out
    assert h.workspace.version("b.txt") == 0


def test_deny_tool_outside_scope(tmp_path):
    h = make_harness()
    cap = h.policy.issue("agent-a", tools={"edit"}, paths={"*"})
    s = _session(h, tmp_path, capability=cap)
    tok = s.begin("agent-a", "goal", writes=["a.txt"])
    assert isinstance(tok, Token)
    out = s.apply(tok.token, [("a.txt", "v")], idempotency_key="k1",
                  agent_id="agent-a", tool="rm")
    assert isinstance(out, Deny) and out.code == "capability_tool_denied:rm", out
    assert h.workspace.version("a.txt") == 0, "越权工具必须零副作用"


def test_identity_mismatch_at_begin(tmp_path):
    h = make_harness()
    cap = h.policy.issue("agent-a", tools={"*"}, paths={"*"})
    s = _session(h, tmp_path, capability=cap)
    out = s.begin("agent-b", "goal", writes=["a.txt"])
    assert isinstance(out, Deny) and out.code == "capability_identity_mismatch", out


def test_opt_in_keeps_old_behavior(tmp_path):
    """不传 capability → 完全走旧路径（默认零行为）。"""
    h = make_harness()
    s = _session(h, tmp_path)
    tok = s.begin("agent-a", "goal", writes=["a.txt"])
    assert isinstance(tok, Token)
    out = s.apply(tok.token, [("a.txt", "v")], idempotency_key="k1",
                  agent_id="agent-a")
    assert isinstance(out, Receipt) and h.workspace.get("a.txt") == "v"


def test_principal_is_business_agent_not_acb_id(tmp_path):
    """负对照：princial 必须是业务 `agent_id`。

    `policy.check_capability` 做 `resolved.agent_id in (principal, "*")`；
    若误传 `acb.id`（形如 `A-rsc-xxx-agent-a-1`），合法写会被全部误拒。
    本用例在正确实现下必须成功 —— 若 principal 传错则红。
    """
    h = make_harness()
    cap = h.policy.issue("agent-a", tools={"edit"}, paths={"a.txt"})
    s = _session(h, tmp_path, capability=cap)
    tok = s.begin("agent-a", "goal", writes=["a.txt"])
    out = s.apply(tok.token, [("a.txt", "v")], idempotency_key="k1",
                  agent_id="agent-a", tool="edit")
    assert isinstance(out, Receipt), out


def test_capability_denied_writes_nothing(tmp_path):
    h = make_harness()
    cap = h.policy.issue("agent-a", tools={"edit"}, paths={"a.txt"})
    s = _session(h, tmp_path, capability=cap)
    tok = s.begin("agent-a", "goal", writes=["a.txt"])
    seq_before = h.event_log.tail()          # begin 的准入事件不计入
    out = s.apply(tok.token, [("a.txt", "v")], idempotency_key="k1",
                  agent_id="agent-a", tool="deploy")
    assert isinstance(out, Deny)
    assert h.workspace.version("a.txt") == 0
    assert h.event_log.tail() == seq_before, "被拒的提交不得产生任何事件"


def test_capability_required_without_token_is_fail_closed(tmp_path):
    import pytest
    h = make_harness()
    with pytest.raises(ValueError):
        _session(h, tmp_path, capability_required=True)


def test_capability_accepts_dict_spec(tmp_path):
    h = make_harness()
    s = _session(h, tmp_path, capability={
        "agent_id": "agent-a", "tools": ["edit"], "paths": ["a.txt"]})
    tok = s.begin("agent-a", "goal", writes=["a.txt"])
    assert isinstance(tok, Token)
    out = s.apply(tok.token, [("a.txt", "v")], idempotency_key="k1",
                  agent_id="agent-a", tool="edit")
    assert isinstance(out, Receipt), out


def test_rsc_tool_passes_tool_name(tmp_path):
    """验具名穿透：`@rsc_tool` 必须把函数名作为 tool 传给门面。

    否则 tool 维度会被静默跳过（永远放行），能力三缺一。
    """
    import pytest
    from harness.ergonomics import RscToolDenied, rsc_session, rsc_tool

    h = make_harness()
    cap = h.policy.issue("agent-a", tools={"other"}, paths={"a.txt"})
    s = _session(h, tmp_path, capability=cap)

    @rsc_tool(writes=["a.txt"])
    def edit(ctx):
        ctx.write("a.txt", "v")

    with pytest.raises(RscToolDenied) as exc:
        with rsc_session(s, "agent-a"):
            edit()
    assert exc.value.code == "capability_tool_denied:edit", exc.value.code
    assert h.workspace.version("a.txt") == 0
