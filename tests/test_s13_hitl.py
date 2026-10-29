"""S13a：HITL 三态 + fail-closed + tick 驱动超时 + 跨进程恢复（修 B-1）。

对应 `docs/s13-orchestration.md` §2.2 / §3 的 S13-1 / S13-2 / S13-3 / S13-13，
以及评审清单 F1–F6、F8、F9、F10、F12 的回归点。
"""

from __future__ import annotations

import pytest
from conftest import admit, decision, finish, hitl, make_harness, seed, tool

from harness.kernel.tasks import TaskService
from harness import ACB, EventKind, Harness, ManualClock, Status, TaskState
from harness.orchestration import _UNSET
from harness.runtime import CrashInjected


def _admit_waiting(h: Harness, run_id: str = "R1", task_id: str = "T1",
                   min_budget: int = 10):
    """准入并跑到第一次 `request_hitl`（返回时应当仍在 `WAITING_APPROVAL`）。"""
    plan = h.planner.plan(run_id, [{"task_id": task_id,
                                    "min_budget": min_budget}])
    h.plans[run_id] = plan
    acb = admit(h, run_id, plan, task_id)
    st = h.run_task(acb, [decision(1, hitl())])
    assert st == Status.NEEDS_APPROVAL
    assert acb.state is TaskState.WAITING_APPROVAL
    assert acb.hitl_pending and acb.hitl_pending["approval_id"]
    return acb


def _tool_events(h: Harness) -> list:
    return [e for e in h.event_log.all() if e.kind == EventKind.TOOL_COMPLETED]


def _approval_events(h: Harness, decision: str | None = None) -> list:
    out = [e for e in h.event_log.all()
           if e.kind == EventKind.APPROVAL_RESOLVED]
    if decision is not None:
        out = [e for e in out if (e.payload or {}).get("decision") == decision]
    return out


# ================================================================ S13-1
def test_s13_1_unset_keeps_waiting_approval():
    """`_UNSET`：保持 WAITING_APPROVAL，不执行后续 action、不失败；不占槽位。"""
    h = make_harness()
    v0 = seed(h, "r", "base")
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")

    # hitl 之后的 tool 不得执行（runtime.run 必须在 WAITING 处停下）
    st = h.run_task(acb, [decision(1, hitl(), tool("read", {"p": "/x"}))])

    assert st == Status.NEEDS_APPROVAL
    assert acb.state is TaskState.WAITING_APPROVAL
    assert len(acb.hitl_pending["approval_id"]) == 32
    assert acb.hitl_result is None
    assert h.workspace.version("r") == v0
    assert _tool_events(h) == []
    # F5：等待态**不占**共享槽位（否则 max_concurrent 下会饿死其他任务）
    assert acb.id not in h.slots.active_keys()
    assert _approval_events(h) == []          # 审批尚未结算 → 无 RESOLVED


def test_s13_1_explicit_denial_fails_and_releases():
    """`None`（显式拒绝）→ FAILED(APPROVAL_DENIED) + 预算/槽位结算 + 审计事件。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    approval_id = acb.hitl_pending["approval_id"]

    # 新签名：跨 run 不串键（run_id, approval_id）
    h.hitl.set_result(acb.run_id, approval_id, None)
    st = h.run_task(acb, [decision(2, finish())])

    assert st == Status.DENIED
    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_DENIED"
    assert acb.hitl_outcome["decision"] == "denied"     # F11
    assert acb.hitl_pending is None                      # F8：结算后清理
    assert acb.id not in h.slots.active_keys()
    assert h.budget.root.subtree_reserved == 0
    assert _approval_events(h, "denied")                 # F8：拒绝也有审计
    h.budget.check_invariants()


@pytest.mark.parametrize("bad", [
    "denied",                                   # 任意字符串
    {"approved": "false"},                      # 伪布尔（truthy）
    {"approved": 1},
    {"approved": True, "extra": 1},             # 多余键
    object(),                                   # 任意对象
])
def test_s13_1_set_result_schema_rejects_non_strict_verdict(bad):
    """F1：`set_result` 入口只接受 `None | {"approved": bool}`，其余 `ValueError`。"""
    h = make_harness()
    with pytest.raises(ValueError):
        h.hitl.set_result("R1", "a" * 32, bad)


def test_s13_1_invalid_verdict_fails_closed():
    """F1：即便内部状态被写入非法判词，gate 也必须拒绝（APPROVAL_INVALID）。"""
    h = make_harness()
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    approval_id = acb.hitl_pending["approval_id"]
    # 绕过 `set_result` 的 schema 校验，直接注入非法判词 → gate 侧必须 fail-closed
    h.hitl._pending[(acb.run_id, approval_id)] = {"approved": "false"}

    st = h.run_task(acb, [decision(2, tool("read", {"p": "/x"}))])

    assert st == Status.DENIED
    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_INVALID"
    assert acb.hitl_outcome["decision"] == "invalid"
    assert _tool_events(h) == []
    assert h.budget.root.subtree_reserved == 0


def test_s13_1_waiting_releases_slot_for_other_tasks():
    """F5：`max_concurrent=1` 时等待审批的任务不得占槽位饿死其他任务。"""
    h = make_harness(run_limits=100, max_concurrent=1)
    seed(h, "r", "base")
    a = _admit_waiting(h, run_id="R1", task_id="T1")
    assert a.id not in h.slots.active_keys()

    plan2 = h.planner.plan("R2", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R2"] = plan2
    b = admit(h, "R2", plan2, "T1")
    assert b.state is TaskState.ADMITTED          # 曾因槽位泄漏变 INADMISSIBLE/NO_SLOT

    st_b = h.run_task(b, [decision(1, finish())])
    assert st_b == Status.OK and b.state is TaskState.DONE
    assert b.id not in h.slots.active_keys()

    # 批准 A 后重新占用槽位并继续执行
    h.hitl.set_result(a.run_id, a.hitl_pending["approval_id"],
                      {"approved": True})
    st_a = h.run_task(a, [decision(2, finish())])
    assert st_a == Status.OK and a.state is TaskState.DONE
    assert a.id not in h.slots.active_keys()
    assert _approval_events(h, "approved")


def test_s13_1_deadline_timeout_releases():
    """deadline 到期（tick 判定）→ FAILED(APPROVAL_TIMEOUT) + 租约/预算/槽位释放。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=1000)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    deadline = acb.hitl_pending["deadline_ms"]

    clock.advance(deadline - clock.now_ms() - 1)
    h.tick()
    assert acb.state is TaskState.WAITING_APPROVAL      # 未到 deadline 不超时

    clock.advance(1)
    h.tick()
    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_TIMEOUT"
    assert acb.hitl_outcome["decision"] == "timeout"
    assert acb.hitl_pending is None
    assert not [lease for lease in h.intents._leases.values() if lease.valid]
    assert acb.id not in h.slots.active_keys()
    assert h.budget.root.subtree_reserved == 0
    assert _approval_events(h, "timeout")
    h.budget.check_invariants()


def test_s13_1_negative_control_removing_gate_breaks_waiting(monkeypatch):
    """F9 故障注入：删掉 `_hitl_gate` 判态后，S13-1 主断言（仍 WAITING）必红。"""
    h = make_harness()
    seed(h, "r", "base")
    acb = _admit_waiting(h)

    # 故障注入：移除入口判态 → 已挂起的审批被忽略，runtime 直接执行
    monkeypatch.setattr(TaskService, "hitl_gate", lambda self, acb_: None)
    h.run_task(acb, [decision(2, tool("read", {"p": "/x"}))])

    # 主用例的两条断言（仍 WAITING / 无 TOOL_COMPLETED）必须失败
    with pytest.raises(AssertionError):
        assert acb.state is TaskState.WAITING_APPROVAL
    with pytest.raises(AssertionError):
        assert _tool_events(h) == []


# ================================================================ S13-2
def _shared_recover_harness(h: Harness) -> Harness:
    """与 h **共享** CheckpointStore + EventLog 的第二个 Harness（模拟跨进程恢复）。"""
    h2 = Harness(run_limits=h._run_limits, event_log=h.event_log,
                 checkpoints=h.checkpoints, clock=h.clock)
    h2.policy.default_token = h2.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h2


def test_s13_2_cross_process_recovery_preserves_waiting(monkeypatch):
    """崩溃在 `save` 之后 / 决策之前 → 恢复后仍 WAITING；批准后可继续。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    plan = h.planner.plan("R2", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R2"] = plan
    acb = admit(h, "R2", plan, "T1")

    def crashy(acb_):
        raise CrashInjected(1, 0)          # 正好在 save 之后、决策之前

    monkeypatch.setattr(h.hitl, "resolve", crashy)
    with pytest.raises(CrashInjected):
        h.run_task(acb, [decision(1, hitl())])

    assert acb.state is TaskState.WAITING_APPROVAL
    approval_id = acb.hitl_pending["approval_id"]
    ref = acb.result_ref
    assert ref

    # ---- 跨进程恢复：共享 store + event_log ----
    h2 = _shared_recover_harness(h)
    rec = h2.recover(ref)

    assert rec.state is TaskState.WAITING_APPROVAL
    assert rec.generation == acb.generation + 1        # generation 变了，但键不变
    assert rec.hitl_pending["approval_id"] == approval_id
    assert h2.hitl.resolve(rec) is _UNSET              # 新进程里还没人审批
    assert _approval_events(h2) == []
    assert _tool_events(h2) == []
    # F6：能力随 payload 持久化（有效 token → dict）；恢复后可直接解析
    assert rec.capability is not None
    assert h2.policy.resolve_token(rec.capability) is not None

    # 批准 → advance("resume") 继续执行；结算后清理 pending + 写审计事件
    h2.hitl.set_result("R2", approval_id, {"approved": True})
    st2 = h2.run_task(rec, [decision(3, finish())])
    assert st2 == Status.OK
    assert rec.state is TaskState.DONE
    assert rec.hitl_pending is None
    assert rec.hitl_outcome["decision"] == "approved"
    assert _approval_events(h2, "approved")


def test_s13_2_pending_lost_fails_closed():
    """F2：`WAITING_APPROVAL` 但 `hitl_pending` 不可用 → 必须结算（不得永久挂起）。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")

    # ① B-1 崩溃窗口产物：hitl_pending=None
    a = _admit_waiting(h, run_id="R1", task_id="T1")
    a.hitl_pending = None
    st = h.run_task(a, [decision(2, tool("read", {"p": "/x"}))])
    assert st == Status.DENIED
    assert a.state is TaskState.FAILED
    assert a.failure == "APPROVAL_STATE_LOST"
    assert a.hitl_outcome["decision"] == "lost"
    assert _tool_events(h) == []
    assert a.id not in h.slots.active_keys()
    assert h.budget.root.subtree_reserved == 0
    assert _approval_events(h, "lost")

    # ② 缺 deadline_ms：同样不得 `continue` 永久等待
    b = _admit_waiting(h, run_id="R2", task_id="T1")
    b.hitl_pending = {"approval_id": b.hitl_pending["approval_id"]}
    h.tick()
    assert b.state is TaskState.FAILED
    assert b.failure == "APPROVAL_STATE_LOST"
    h.budget.check_invariants()


# ================================================================ S13-3
def test_s13_3_approval_key_does_not_cross_runs():
    """两 run 同 task_id：只批 R1 → R1 DONE、R2 仍 WAITING。"""
    h = make_harness()
    seed(h, "r", "base")
    a1 = _admit_waiting(h, run_id="R1", task_id="T1")
    a2 = _admit_waiting(h, run_id="R2", task_id="T1")

    h.hitl.set_result("R1", a1.hitl_pending["approval_id"],
                      {"approved": True})
    st = h.run_task(a1, [decision(2, finish())])
    assert st == Status.OK
    assert a1.state is TaskState.DONE

    st2 = h.run_task(a2, [decision(2, finish())])
    assert st2 == Status.NEEDS_APPROVAL
    assert a2.state is TaskState.WAITING_APPROVAL


def test_s13_3_negative_control_legacy_task_key_cannot_cross_runs():
    """F3：legacy `set_result(task_id, verdict)` 已被删除；跨 run 不串键。"""
    h = make_harness()
    seed(h, "r", "base")
    a3 = _admit_waiting(h, run_id="R3", task_id="T1")

    # 旧签名（两个位置参数）不再被接受
    with pytest.raises(TypeError):
        h.hitl.set_result("T1", {"approved": True})       # type: ignore[call-arg]
    # 空 approval_id 会让一次提交命中该 run 的全部空 pending → 必须拒绝
    with pytest.raises(ValueError):
        h.hitl.set_result("R3", "", {"approved": True})

    # 用错误的 run_id 提交同一 approval_id → 不得串到 R3
    h.hitl.set_result("R4", a3.hitl_pending["approval_id"],
                      {"approved": True})
    st = h.run_task(a3, [decision(2, finish())])
    assert st == Status.NEEDS_APPROVAL
    assert a3.state is TaskState.WAITING_APPROVAL
    assert _tool_events(h) == []


# ================================================================ S13-13
def test_s13_13_tick_deadline_boundary():
    """`ManualClock`：`deadline-1` 未超时，`deadline` 超时。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=500)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    deadline = acb.hitl_pending["deadline_ms"]

    clock.advance(deadline - clock.now_ms() - 1)
    h.tick()
    assert acb.state is TaskState.WAITING_APPROVAL
    assert acb.failure is None

    clock.advance(1)
    h.tick()
    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_TIMEOUT"


def test_s13_13_run_task_calls_tick_before_gate_and_settles(monkeypatch):
    """F9：`tick` 必须在 gate **之前**，且 tick 真的让过期审批结算。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=1000)
    seed(h, "r", "base")
    acb = _admit_waiting(h)

    order: list[str] = []
    orig_tick = h.tick
    orig_gate = h.tasks.hitl_gate

    def spy_tick(now_ms=None):
        order.append("tick")
        return orig_tick(now_ms)

    def spy_gate(acb_):
        order.append("gate")
        return orig_gate(acb_)

    monkeypatch.setattr(h, "tick", spy_tick)
    monkeypatch.setattr(h.tasks, "hitl_gate", spy_gate)

    clock.advance(5000)                        # 已过期
    st = h.run_task(acb, [decision(2, tool("read", {"p": "/x"}))])

    assert order == ["tick", "gate"]           # 顺序（空 tick 不再能蒙混过关）
    assert st == Status.APPROVAL_TIMEOUT       # tick 真的结算了过期审批
    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_TIMEOUT"
    assert _tool_events(h) == []


def test_s13_13_recover_settles_expired_approval():
    """`recover()` 结束即 tick：恢复出的过期审批被立即结算。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=1000)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    ref = acb.result_ref

    clock.advance(5000)                        # 已过期
    rec = h.recover(ref)

    assert rec.state is TaskState.FAILED
    assert rec.failure == "APPROVAL_TIMEOUT"
    assert rec.hitl_pending is None
    assert rec.id not in h.slots.active_keys()
    h.budget.check_invariants()


# ================================================================ F4
def test_s13_4_tick_timeout_is_terminal_for_runtime():
    """F4：tick 判超时后，`runtime.run` 必须拒绝终态（不得真执行/覆盖 failure）。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=1000)
    seed(h, "r", "base")
    acb = _admit_waiting(h)

    clock.advance(5000)
    h.tick()
    assert acb.state is TaskState.FAILED

    st = h.runtime.run(acb, [decision(9, tool("read", {"p": "/x"}))])
    assert st == Status.APPROVAL_TIMEOUT
    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_TIMEOUT"    # 不得被 BUDGET_EXHAUSTED 覆盖
    assert _tool_events(h) == []


# ================================================================ F6 / F10
def test_s13_0_capability_and_schema_version_persisted():
    """F6/F10：payload 存**有效 token**（即使 `acb.capability is None`）+ schema_version。"""
    h = make_harness()
    acb = ACB(id="A-CAP", run_id="R-CAP", task_id="T1")
    assert acb.capability is None               # 生产路径：token 在 policy._tokens

    payload = h.checkpoints.load(h.checkpoints.save(acb))
    assert payload["capability"] is not None
    assert payload["capability"]["agent_id"] == "*"
    assert payload["schema_version"] == 3

    # 旧 schema（无 capability / 无 schema_version）→ CAPABILITY_LOST +
    # needs_readmit，不静默继承 default_token
    ref = h.checkpoints.save(acb)
    h.checkpoints._store[ref].pop("capability")
    h.checkpoints._store[ref].pop("schema_version")     # 模拟 F10 前的旧 payload
    recovered = h.recover(ref)
    assert recovered.failure == "CAPABILITY_LOST"
    assert recovered.needs_readmit is True
    # 空串哨兵：有效 token 不得落回 policy.default_token（否则 fail-open）
    assert recovered.capability == ""
    assert h.identity.effective_token(recovered) == ""
    assert h.policy.resolve_token(h.identity.effective_token(recovered)) is None
