"""MS7 出口条件：CapabilityToken 能力强制（B4，模型不可绕过）。

对应 milestones.md MS7：「CapabilityToken 强制 + 越权被拒」。
"""

from __future__ import annotations

from conftest import admit, decision, finish, make_harness
from harness import CapabilityToken, TaskState


def test_capability_denied_blocks_tool():
    h = make_harness()
    h.policy.issue(CapabilityToken("A-T1", tools={"read"}, paths={"/src"}))

    assert h.policy.check_capability("A-T1", "read", "/src/a.ts") is True
    assert h.policy.check_capability("A-T1", "write", "/src/a.ts") is False
    assert h.policy.check_capability("A-T1", "read", "/etc/passwd") is False
    # 未签发 token → 参考实现默认放行（生产实现应改为拒绝）
    assert h.policy.check_capability("A-UNKNOWN", "read", "/src/a.ts") is True


def test_toolgateway_enforces_capability():
    """B4：检查点在 gateway 强制执行，模型不可绕过。"""
    h = make_harness()
    plan = h.planner.plan("CAP", [{"task_id": "T1", "min_budget": 10}])
    h.plans["CAP"] = plan
    acb = admit(h, "CAP", plan, "T1")

    h.policy.issue(CapabilityToken(acb.id, tools={"read"}, paths={"/src"}))
    assert h.policy.check_capability(acb.id, "deploy") is False

    # 即使模型发出该动作，也会被能力检查拦下
    act = __import__("conftest").tool("deploy")
    h.run_task(acb, [decision(1, act)])
    assert h.effects.state(h.idem_key(acb, act, 0)) is None
