"""S11a：统一 gateway 前置 + Action 显式字段（`docs/s11-egress-enforcement.md` v1.1 §2）。

| 用例 | 判据 |
|---|---|
| S11-1 | **穷举** `_HANDLERS` 全 kind（stage/commit/tool/request_hitl/finish）：带 `network="evil.com"` 的 action 必被拦且 handler **未执行** |
| S11-1 负对照 | monkeypatch `_gateway_check` 恒 True → handler 被调用（证明拦截点是 gate 而非别的路径） |
| S11-1b | `_commit_staged` / `handle_commit_result` 为本地动作，**不经** gate（K-51 边界固定） |
| S11-4 | `allows_network` 通配 / CIDR / 端口；`allows_data_class` 分级序 |
"""

from __future__ import annotations

import pytest
from conftest import make_harness

from harness import (
    ACB,
    Action,
    CapabilityToken,
    Harness,
    Result,
    Status,
    TaskState,
    Write,
)
from harness import runtime as runtime_mod


def _restricted_token(h, acb):
    """给 acb 签发「只允许 api.example.com / internal」的 token（非通配）。"""
    h.policy.issue(acb.id, tools={"*"}, paths={"*"},
                   network={"api.example.com"}, data_class="internal")
    return h.policy.token(acb.id)


def _action(kind: str, **overrides) -> Action:
    base: dict = {"kind": kind}
    if kind == "stage":
        base.update(resource="r", content={"v": 1})
    elif kind == "commit":
        base.update(writes=[Write("r")])
    elif kind == "tool":
        base.update(tool="http", args={"p": "/x"})
    base.update(overrides)
    return Action(**base)


def _spy(kind: str, calls: list):
    def handler(rt, acb, action, i):
        calls.append(kind)
        return Status.OK
    return handler


KINDS = sorted(runtime_mod._HANDLERS)


# ================================================================ S11-1
@pytest.mark.parametrize("kind", KINDS)
def test_s11_1_gate_blocks_every_kind(kind, monkeypatch):
    """穷举：统一前置覆盖全部 kind，且 handler **未被执行**。"""
    h = make_harness()
    acb = ACB(id=f"A-S11-{kind}", run_id="R-S11", task_id="T")
    _restricted_token(h, acb)
    calls: list[str] = []
    monkeypatch.setitem(runtime_mod._HANDLERS, kind, _spy(kind, calls))

    res = h.runtime._do_action(acb, _action(kind, network="evil.com"), 0)

    assert res == Status.DENIED_CAPABILITY
    assert res.detail == {"reason": "gateway"}
    assert calls == [], f"handler {kind!r} 在 gate 拒绝后仍被执行"


@pytest.mark.parametrize("kind", KINDS)
def test_s11_1_gate_blocks_data_class_for_every_kind(kind, monkeypatch):
    """data_class 维度同样统一前置（低 token 发高等级 → 拦）。"""
    h = make_harness()
    acb = ACB(id=f"A-S11D-{kind}", run_id="R-S11D", task_id="T")
    _restricted_token(h, acb)                       # data_class="internal"
    calls: list[str] = []
    monkeypatch.setitem(runtime_mod._HANDLERS, kind, _spy(kind, calls))

    res = h.runtime._do_action(
        acb, _action(kind, data_class="restricted"), 0)

    assert res == Status.DENIED_CAPABILITY
    assert calls == []


@pytest.mark.parametrize("kind", KINDS)
def test_s11_1_negative_control_gate_true_runs_handler(kind, monkeypatch):
    """负对照：gate 恒 True → handler 必被调用（拦截点确实是 gate）。"""
    h = make_harness()
    acb = ACB(id=f"A-S11N-{kind}", run_id="R-S11N", task_id="T")
    _restricted_token(h, acb)
    calls: list[str] = []
    monkeypatch.setitem(runtime_mod._HANDLERS, kind, _spy(kind, calls))
    monkeypatch.setattr(h.runtime, "_gateway_check", lambda a, act: True)

    res = h.runtime._do_action(acb, _action(kind, network="evil.com"), 0)

    assert res == Status.OK
    assert calls == [kind], "gate 放行后 handler 未被调用"


def test_s11_action_fields_backward_compatible():
    """S11-8：Action 新增字段有默认值 → 旧构造不受影响。"""
    action = Action(kind="finish")
    assert action.network is None
    assert action.data_class is None
    assert Action(kind="stage", resource="r", content=1).network is None


# ================================================================ 终审 1-1 / 1-2 / 2-1
def test_final_1_1_garbage_token_denied_everywhere():
    """垃圾 token：gate 全 kind 拒绝 + try_admit 拒绝（不再只判非 None）。"""
    h = make_harness()
    acb = ACB(id="A-GARBAGE", run_id="R-G", task_id="T-G")
    acb.capability = "garbage-token"

    for kind in KINDS:
        assert h.runtime._gateway_check(acb, _action(kind)) is False, \
            f"垃圾 token 在 {kind!r} 被放行"

    plan = h.planner.plan("RG", [{"task_id": "T1", "min_budget": 10}])
    denied = h.dispatcher.try_admit(plan["tasks"]["T1"], "RG",
                                    token="garbage-token")
    assert denied.state is TaskState.INADMISSIBLE
    assert denied.failure == "DENIED_CAPABILITY"
    assert denied.reservation_ids == [] and denied.budget_node is None


def test_final_1_1_token_must_bind_to_agent():
    """token 的 agent_id 必须与 ACB 绑定；别人的 token 不得复用。"""
    h = make_harness()
    acb = ACB(id="A-REAL", run_id="R-B", task_id="T-B")
    acb.capability = h.policy.issue("A-OTHER", tools={"*"}, paths={"*"},
                                    network={"*"}, data_class="*")

    assert h.runtime._gateway_check(acb, Action(kind="finish")) is False


def test_final_1_2_minted_token_reusable_across_actions():
    """同一 minted token 连续判定 N 次都必须 True（jti 去重只用于消息重放）。"""
    h = Harness(auth_secret="final-1-2")
    token = h.policy.mint("A-MULTI", tools={"*"}, paths={"*"},
                          network={"*"}, data_class="public")

    results = [h.policy.check_capability("A-MULTI", token, "http", "/src/a")
               for _ in range(5)]
    assert results == [True] * 5, f"多动作任务第二次起被 jti 去重误拒：{results}"
    # 消息重放语义仍在 Signer 层保留
    assert h.auth.verify(token) is None


def test_final_2_1_mint_defaults_to_lowest_data_class():
    """`mint` 未显式给 data_class → 默认最低等级（不得通配 restricted）。"""
    h = Harness(auth_secret="final-2-1")
    token = h.policy.mint("A-PUB", tools={"*"}, paths={"*"})

    assert h.policy.allows_data_class(token, "public") is True
    assert h.policy.allows_data_class(token, "internal") is False
    assert h.policy.allows_data_class(token, "restricted") is False


# ================================================================ S11-1b
def test_s11_1b_commit_staged_bypasses_gate_but_do_action_does_not():
    """K-51：本地提交不经 gate；同一无 token 路径经 `_do_action` 必被拦。"""
    h = Harness()                                   # 无 token（deny-by-default）
    acb = ACB(id="A-S11-1B", run_id="R-S11-1B", task_id="T1")
    h.workspace.stage(acb, "r", {"v": 1})

    assert h.runtime._gateway_check(acb, Action(kind="commit")) is False

    st = h.runtime._commit_staged(acb)
    assert st == Status.OK, "本地提交不得被 egress gate 拦截（K-51）"
    assert h.workspace.get("r") == {"v": 1}

    h2 = Harness()
    acb2 = ACB(id="A-S11-1B-2", run_id="R-S11-1B", task_id="T2")
    denied = h2.runtime._do_action(acb2, Action(kind="finish"), 0)
    assert denied == Status.DENIED_CAPABILITY


def test_s11_1b_handle_commit_result_does_not_call_gate(monkeypatch):
    """K-51：`handle_commit_result` 入口不经 gate（重试路径同为本地动作）。"""
    h = Harness()
    acb = ACB(id="A-S11-1B-3", run_id="R-S11-1B", task_id="T3")

    def _boom(*args, **kwargs):
        raise AssertionError("gate 不应在 handle_commit_result 内被调用")

    monkeypatch.setattr(h.runtime, "_gateway_check", _boom)
    assert h.runtime.handle_commit_result(acb, Result(Status.OK)) == Status.OK


# ================================================================ S11-4
def test_s11_4_allows_network_wildcard_cidr_port():
    token = CapabilityToken(
        "a", network={"*.example.com", "10.0.0.0/8", "api.svc:8443"})

    assert token.allows_network("a.example.com") is True
    assert token.allows_network("deep.a.example.com") is True
    assert token.allows_network("example.com") is False      # 通配只匹配子域
    assert token.allows_network("evil.com") is False

    assert token.allows_network("10.1.2.3") is True
    assert token.allows_network("10.1.2.3:443") is True      # 无端口约束
    assert token.allows_network("11.0.0.1") is False
    assert token.allows_network("not-an-ip") is False

    assert token.allows_network("api.svc:8443") is True
    assert token.allows_network("api.svc:443") is False
    assert token.allows_network("api.svc") is False          # 端口不匹配


def test_s11_4_allows_data_class_graded():
    """分级序：低 token 发高等级 → False；高 token 发低等级 → True。"""
    internal = CapabilityToken("a", data_class="internal")
    assert internal.allows_data_class("public") is True
    assert internal.allows_data_class("internal") is True
    assert internal.allows_data_class("confidential") is False
    assert internal.allows_data_class("restricted") is False

    restricted = CapabilityToken("a", data_class="restricted")
    for level in ("public", "internal", "confidential", "restricted"):
        assert restricted.allows_data_class(level) is True

    assert CapabilityToken("a", data_class="*").allows_data_class("restricted") is True
    # 未知等级 → 退回字符串相等（向后兼容旧测试的 "secret"）
    assert internal.allows_data_class("secret") is False
    assert CapabilityToken("a", data_class="secret").allows_data_class("secret") is True


def test_s11_4_policy_engine_delegates_new_semantics():
    h = make_harness()
    token = h.policy.issue("A-S11-4", tools={"*"}, paths={"*"},
                           network={"*.example.com", "10.0.0.0/8"},
                           data_class="internal")
    assert h.policy.allows_network(token, "x.example.com") is True
    assert h.policy.allows_network(token, "10.9.8.7") is True
    assert h.policy.allows_network(token, "evil.com") is False
    assert h.policy.allows_data_class(token, "public") is True
    assert h.policy.allows_data_class(token, "restricted") is False
