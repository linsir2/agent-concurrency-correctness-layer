"""MS4 出口：runtime 表驱动 + guard/续租 + 游标 + 调度 + 前驱闸门 + 环检测。

对应 `docs/s6-runtime-and-e2e.md` §2.1/§2.2/§2.3/§2.5/§2.6 与 T19。
"""

from __future__ import annotations

import pytest
from conftest import admit, decision, finish, make_harness, seed

from harness import (
    ACB,
    Action,
    EventKind,
    ManualClock,
    Result,
    Status,
    TaskState,
    Write,
)
from harness import runtime as runtime_module

pytestmark = pytest.mark.ms4


# ---------------------------------------------------------------- T19
def test_t19_registry_dispatches_five_kinds():
    """T19：stage/commit/tool/finish/request_hitl 五种 kind 均可派发。"""
    h = make_harness()
    seed(h, "r", {"v": 0})
    acb = ACB(id="A-T19", run_id="R19", task_id="T1")

    results = [
        h.runtime._do_action(acb, Action(kind="stage", resource="r",
                                         content={"v": 1}), 0),
    ]
    results.append(h.runtime._do_action(acb, Action(kind="commit",
                                                    writes=[Write("r")]), 1))

    node = h.budget.create_node("B-T19", 10)
    resv = h.budget.reserve("B-T19", 5)
    acb.budget_node = node
    acb.reservation_ids = [resv.reservation_id]
    results.append(h.runtime._do_action(
        acb, Action(kind="tool", tool="read", args={"p": "/x"}, cost=1), 2))
    results.append(h.runtime._do_action(acb, Action(kind="finish"), 3))

    hitl_acb = ACB(id="A-T19H", run_id="R19", task_id="T2")
    results.append(h.runtime._do_action(hitl_acb, Action(kind="request_hitl"), 0))

    assert all(isinstance(r, Result) for r in results)
    assert [r.status for r in results] == [
        Status.OK, Status.OK, Status.OK, Status.OK, Status.NEEDS_APPROVAL]


def test_t19_unknown_kind_maps_invalid_input():
    """T19：未知 kind → INVALID_INPUT + reason=unknown_action。"""
    h = make_harness()
    acb = ACB(id="A-U", run_id="R", task_id="T")
    res = h.runtime._do_action(acb, Action(kind="does_not_exist"), 0)
    assert isinstance(res, Result)
    assert res == Status.INVALID_INPUT
    assert res.detail["reason"] == "unknown_action"


def test_t19_handler_exception_maps_upstream_failed(monkeypatch):
    """T19：handler 异常 → UPSTREAM_FAILED；裸 Status 归一化为 Result。"""
    h = make_harness()
    acb = ACB(id="A-E", run_id="R", task_id="T")

    def boom(rt, acb_, action, i):
        raise RuntimeError("kaboom")

    monkeypatch.setitem(runtime_module._HANDLERS, "boom", boom)
    res = h.runtime._do_action(acb, Action(kind="boom"), 0)
    assert res == Status.UPSTREAM_FAILED
    assert res.detail["reason"] == "RuntimeError"

    monkeypatch.setitem(runtime_module._HANDLERS, "bare_ok",
                        lambda rt, acb_, action, i: Status.OK)
    bare = h.runtime._do_action(acb, Action(kind="bare_ok"), 0)
    assert isinstance(bare, Result)
    assert bare == Status.OK


def test_t19_duplicate_registration_raises():
    """T19：重复注册同一 kind → ValueError。"""
    with pytest.raises(ValueError):
        runtime_module.register_action("stage")(lambda *a: None)
    with pytest.raises(ValueError):
        runtime_module.register_action("finish")(lambda *a: None)


# ---------------------------------------------------------------- guard
def test_guard_predicate_order_and_failure_reason():
    """§2.2：谓词顺序（cancel → renew → budget → deadline → max_steps → no_progress）。"""
    h = make_harness(run_limits=100)
    acb = ACB(id="A-G", run_id="G", task_id="T")

    assert h.runtime.guard(acb) == (False, "BUDGET_EXHAUSTED")
    assert acb.failure == "BUDGET_EXHAUSTED"

    node = h.budget.create_node("B-G", 100)
    resv = h.budget.reserve("B-G", 10)
    acb.budget_node = node
    acb.reservation_ids = [resv.reservation_id]
    acb.failure = None
    assert h.runtime.guard(acb) == (True, None)

    acb.step = 5
    acb.deadline = 5
    acb.no_progress_steps = h.aging_max + 1
    assert h.runtime.guard(acb) == (False, "DEADLINE_EXCEEDED")
    assert acb.failure == "DEADLINE_EXCEEDED"

    acb.deadline = None
    acb.step = h.max_steps + 1
    assert h.runtime.guard(acb) == (False, "MAX_STEPS")
    assert acb.failure == "MAX_STEPS"

    acb.step = 0
    assert h.runtime.guard(acb) == (False, "NO_PROGRESS")
    assert acb.failure == "NO_PROGRESS"

    acb.failure = None
    h.run_coordinator.cancel_run("G")
    assert h.runtime.guard(acb) == (False, "CANCELLED")
    assert acb.failure is None


def test_guard_renew_or_fail_three_states():
    """§2.2：无租约 → None；有效 → None；过期 → LEASE_EXPIRED；已 revoke → None。"""
    clock = ManualClock()
    h = make_harness(clock=clock)
    acb = ACB(id="A-L", run_id="L", task_id="T")

    assert h.runtime._renew_or_fail(acb) is None

    lease = h.intents.reserve("r", acb.id, 0, ttl_ms=100)
    assert h.runtime._renew_or_fail(acb) is None

    clock.advance(200)
    assert h.runtime._renew_or_fail(acb) == "LEASE_EXPIRED"

    h.intents.revoke(lease)
    assert h.runtime._renew_or_fail(acb) is None


# ---------------------------------------------------------------- event_cursor
def test_event_cursor_materialized_on_save_and_recover():
    """§2.3：save 写 materialized_cursor；recover 优先用它，旧格式回退 cursor。"""
    h = make_harness()
    acb = ACB(id="A-C", run_id="C", task_id="T")
    for i in range(3):
        h.event_log.append(EventKind.TASK_COMPLETED, f"x{i}", {})
    acb.cursor = h.event_log.tail()
    ref = h.checkpoints.save(acb)

    data = h.checkpoints.load(ref)
    assert data["materialized_cursor"] == acb.cursor
    assert data["event_cursor"] == h.event_log.tail()

    stored = h.checkpoints._store[ref]
    stored["cursor"] = 0
    stored["materialized_cursor"] = 2
    recovered = h.recover(ref)
    assert recovered.cursor == 2

    h2 = make_harness()
    ref2 = h2.checkpoints.save(ACB(id="A-C2", run_id="C2", task_id="T2"))
    stored2 = h2.checkpoints._store[ref2]
    stored2.pop("materialized_cursor")
    stored2["cursor"] = 4
    legacy = h2.recover(ref2)
    assert legacy.cursor == 4


# ---------------------------------------------------------------- scheduler
def test_drain_scheduler_consumes_without_recursion():
    """§2.6：真消费 ready_queue；只启动 ADMITTED；每任务只 run 一次、不递归。"""
    h = make_harness()
    plan = h.planner.plan("DR", [{"task_id": f"T{i}", "min_budget": 10}
                                 for i in (1, 2, 3)])
    h.plans["DR"] = plan
    a1 = admit(h, "DR", plan, "T1")
    a2 = admit(h, "DR", plan, "T2")
    a3 = admit(h, "DR", plan, "T3")
    a3.state = TaskState.DONE

    ran: list[str] = []
    orig_run = h.runtime.run

    def spy(acb, decisions, crash_at=None):
        ran.append(acb.task_id)
        return orig_run(acb, decisions, crash_at)

    h.runtime.run = spy
    nested: list[int] = []
    h.run_task = lambda *a, **k: nested.append(1)

    for tid in ("T1", "T2", "T3"):
        h.scheduler.push(tid)
    h._drain_scheduler()

    assert h.scheduler.is_empty()
    assert ran == ["T1", "T2"]
    assert nested == []
    assert a1.state is TaskState.DONE
    assert a2.state is TaskState.DONE


# ---------------------------------------------------------------- 前驱闸门
def test_predecessor_gate_blocks_until_done_and_negative_control():
    """§2.5：前驱未完成 → INADMISSIBLE/PREDECESSOR_NOT_DONE；无前驱 → 立即可准入。"""
    h = make_harness()
    plan = h.planner.plan("PG", [{"task_id": "T1", "min_budget": 10},
                                 {"task_id": "T2", "min_budget": 10}])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["PG"] = plan

    blocked = h.dispatcher.try_admit(plan["tasks"]["T2"], "PG")
    assert blocked.state is TaskState.INADMISSIBLE
    assert blocked.failure == "PREDECESSOR_NOT_DONE"
    assert blocked.reservation_ids == [] and blocked.budget_node is None

    plan["tasks"]["T1"]["state"] = TaskState.DONE
    allowed = h.dispatcher.try_admit(plan["tasks"]["T2"], "PG")
    assert allowed.state is TaskState.ADMITTED

    plan2 = h.planner.plan("PGN", [{"task_id": "T1", "min_budget": 10},
                                   {"task_id": "T2", "min_budget": 10}])
    h.plans["PGN"] = plan2
    free = h.dispatcher.try_admit(plan2["tasks"]["T2"], "PGN")
    assert free.state is TaskState.ADMITTED


# ---------------------------------------------------------------- 环检测
def test_cycle_detection_rejects_edge():
    """§2.5：含环的逻辑边 → add_logical_edge 抛 ValueError，且不落盘。"""
    h = make_harness()
    plan = h.planner.plan("CY", [{"task_id": "T1"}, {"task_id": "T2"},
                                 {"task_id": "T3"}])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.planner.add_logical_edge(plan, "T2", "T3")

    with pytest.raises(ValueError):
        h.planner.add_logical_edge(plan, "T3", "T1")
    assert len(plan["edges"]) == 2


# ================================================================ 对抗评审修复
def test_d1_handler_returns_none_maps_upstream_failed(monkeypatch):
    """D1：handler 返回 None → UPSTREAM_FAILED(bad_return)，不再 Result(None) 崩溃。"""
    h = make_harness()
    acb = ACB(id="A-D1", run_id="R", task_id="T")
    monkeypatch.setitem(runtime_module._HANDLERS, "none_kind", lambda *a: None)
    res = h.runtime._do_action(acb, Action(kind="none_kind"), 0)
    assert res == Status.UPSTREAM_FAILED
    assert res.detail["reason"] == "bad_return"


def test_d4_retry_with_fresh_lease_revokes_no_residue():
    """D4：REJECT_STALE 重试后不遗留 valid 租约。"""
    h = make_harness()
    seed(h, "r", {"v": 0})
    plan = h.planner.plan("D4", [{"task_id": "T1", "committed": ["r"], "min_budget": 10}])
    h.plans["D4"] = plan
    acb = admit(h, "D4", plan, "T1")
    acb.staged["r"] = {"v": 1}
    acb.base_version["r"] = 1
    acb.read_set["r"] = 1
    h.runtime._retry_with_fresh_lease(acb, Result(Status.REJECT_STALE, {"resource": "r"}))
    assert [l for l in h.intents._active.get("r", []) if l.valid] == []


def test_d6_patch_renamed_task_still_gates_successor():
    """D6：patch 改名 T1→T1' 后边仍写旧 id；T1' DONE 后 T2 可 admit。"""
    h = make_harness()
    plan = h.planner.plan("D6", [{"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
                                 {"task_id": "T2", "min_budget": 10}])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["D6"] = plan
    blocked = h.dispatcher.try_admit(plan["tasks"]["T2"], "D6")
    assert blocked.state is TaskState.INADMISSIBLE
    assert blocked.failure == "PREDECESSOR_NOT_DONE"

    new_plan = h.planner.patch(plan, {"failing_subgoals": ["S1"], "missing_deps": []})
    h.plans["D6"] = new_plan
    still_blocked = h.dispatcher.try_admit(new_plan["tasks"]["T2"], "D6")
    assert still_blocked.state is TaskState.INADMISSIBLE

    new_plan["tasks"]["T1'"]["state"] = TaskState.DONE
    allowed = h.dispatcher.try_admit(new_plan["tasks"]["T2"], "D6")
    assert allowed.state is TaskState.ADMITTED


def test_d9_successor_scheduled_after_predecessor_done():
    """D9：T1 DONE 后，其后继 T2 被 push 到调度器并执行。

    场景：T1/T2 先准入（尚无依赖），随后补逻辑边 T1→T2（replan/动态依赖），
    T1 完成时 `_finalize` 应把新就绪的 T2 推入调度器。
    """
    h = make_harness()
    plan = h.planner.plan("D9", [{"task_id": "T1", "min_budget": 10},
                                 {"task_id": "T2", "min_budget": 10}])
    h.plans["D9"] = plan
    a1 = admit(h, "D9", plan, "T1")
    a2 = admit(h, "D9", plan, "T2")               # 尚无依赖 → 可准入
    h.planner.add_logical_edge(plan, "T1", "T2")  # 事后补依赖

    ran: list[str] = []
    orig_run = h.runtime.run

    def spy(acb, decisions, crash_at=None):
        ran.append(acb.task_id)
        return orig_run(acb, decisions, crash_at)

    h.runtime.run = spy
    h.run_task(a1, [decision(1, finish())])
    assert a1.state is TaskState.DONE
    assert a2.state is TaskState.DONE
    assert ran == ["T1", "T2"]


def test_d10_same_task_id_new_acb_runs():
    """D10：同 task_id 多 ACB 时，调度器按 acb.id 命中新 ACB（旧 ACB 不被误命中）。"""
    h = make_harness()
    plan = h.planner.plan("D10", [{"task_id": "T1", "min_budget": 10}])
    h.plans["D10"] = plan
    old = admit(h, "D10", plan, "T1")
    old.state = TaskState.FAILED
    new = admit(h, "D10", plan, "T1")
    assert new.state is TaskState.ADMITTED

    ran: list[str] = []
    orig_run = h.runtime.run

    def spy(acb, decisions, crash_at=None):
        ran.append(acb.id)
        return orig_run(acb, decisions, crash_at)

    h.runtime.run = spy
    h.run_task(new, [decision(1, finish())])
    assert new.state is TaskState.DONE
    assert ran == [new.id]


def test_d14_old_acb_failure_after_patch_no_keyerror():
    """D14：patch 后旧 ACB 失败，_finalize 用 .get() 安全处理，不 KeyError。"""
    h = make_harness()
    plan = h.planner.plan("D14", [{"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["D14"] = plan
    old = admit(h, "D14", plan, "T1")
    new_plan = h.planner.patch(plan, {"failing_subgoals": ["S1"], "missing_deps": []})
    h.plans["D14"] = new_plan
    old.state = TaskState.FAILED
    old.failure = "X"
    h._finalize(old)                 # 不抛 KeyError
    assert old.state is TaskState.FAILED
