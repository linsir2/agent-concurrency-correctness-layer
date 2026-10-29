"""S13b：replan（alias 三处消费点 + 预算 + 再准入清理 + D10 不双跑）。

对应 `docs/s13-orchestration.md` §2.3 与 §3 的 S13-4 / S13-5 / S13-6 / S13-7。
"""

from __future__ import annotations

import pytest
from conftest import admit, decision, finish, make_harness

from harness import ACB, Harness, Status, TaskState


def _bare_harness(**kwargs) -> Harness:
    """裸 Harness + 通配 token（预算/epoch 用例需要注入 `replan_budget`）。"""
    h = Harness(**kwargs)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h


def _plan_with_edge(h: Harness, run_id: str = "S4") -> dict:
    plan = h.planner.plan(run_id, [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "min_budget": 10},
    ])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans[run_id] = plan
    return plan


def _patched(h: Harness, plan: dict, run_id: str = "S4") -> dict:
    new_plan, _ = h.planner.patch(
        plan, {"failing_subgoals": ["S1"], "missing_deps": []})
    h.plans[run_id] = new_plan
    return new_plan


def _acb_for(h: Harness, run_id: str, task_id: str):
    return next((a for a in h.acbs.values()
                 if a.run_id == run_id and a.task_id == task_id), None)


# ================================================================ S13-4 alias
def test_s13_4_rebuild_ready_set_returns_successor_via_alias():
    """① `rebuild_ready_set` 在 T1→T1' 改名后仍把 T2 判为 ready。"""
    h = make_harness()
    plan = _plan_with_edge(h)
    new_plan = _patched(h, plan)
    new_plan["tasks"]["T1'"]["state"] = TaskState.DONE

    ready = h.reconcile.rebuild_ready_set("S4")
    assert "T2" in ready
    assert h.planner.resolve_alias("T1", new_plan) == "S1"


def test_s13_4_negative_control_without_alias_ready_empty():
    """负对照：**在途 ACB 的旧 task_id** 仍只能靠别名解析到当前 task。

    S14 K-67(B) 之后 `patch()` 会把 `edges` 端点物化为 canonical id，所以
    「后继就绪」不再依赖别名；但 `_finalize` / `_readmit_after_replan` 仍按
    **活跃 ACB 的旧 `task_id`** 解析 —— 这条路径**仍必须**依赖别名。
    这正是 K-67 剪枝判据必须含「活跃 ACB 的 task_id」的原因（用旧判据
    「只在 edges.from 里找」会剪掉它 → `replan_no_task`）。
    """
    h = make_harness()
    plan = _plan_with_edge(h)
    new_plan = _patched(h, plan)

    # ① edges 已物化：端点都是 canonical id，不再引用被改名的旧 id
    assert [e["to"] for e in new_plan["edges"]] == ["T2"]
    assert all(e["from"] in new_plan["tasks"] for e in new_plan["edges"])

    # ② 后继就绪确实不再依赖别名（旧负对照的前提已随 K-67(B) 改变）
    new_plan["tasks"]["T1'"]["state"] = TaskState.DONE
    assert "T2" in h.reconcile.rebuild_ready_set("S4")

    # ③ 但「旧 task_id → 当前 task」仍然只能用别名解析；去掉别名即解析不到
    assert h.planner.task_for(new_plan, "T1")["task_id"] == "T1'"
    h.planner.resolve_alias = lambda old_id, plan=None: old_id
    stale = h.planner.task_for(new_plan, "T1")
    assert stale is None or stale["task_id"] != "T1'", (
        "去掉别名解析后仍能解析到当前 task —— 说明别名不是唯一映射（判据失效）")


def test_s13_4_execution_pushes_and_runs_successor_after_rename():
    """② 执行层：`_finalize(T1')` 后 `_drain_scheduler` 真跑 T2。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("S4E", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "min_budget": 10},
    ])
    h.plans["S4E"] = plan
    a1 = admit(h, "S4E", plan, "T1")
    a2 = admit(h, "S4E", plan, "T2")      # 尚无前驱约束 → 先准入
    h.planner.add_logical_edge(plan, "T1", "T2")
    a1.state = TaskState.QUARANTINED      # R8：replan 只接受 QUARANTINED/REPLANNING

    new_plan = h.replan(a1, plan,
                        diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert new_plan is not None
    fresh = _acb_for(h, "S4E", "T1'")
    assert fresh is not None and a1.id not in h.acbs

    ran: list[str] = []
    orig_run = h.runtime.run

    def spy(acb, decisions=None, crash_at=None):
        ran.append(acb.task_id)
        return orig_run(acb, decisions, crash_at)

    h.runtime.run = spy
    h.run_task(fresh, [decision(1, finish())])

    assert fresh.state is TaskState.DONE
    assert a2.state is TaskState.DONE
    assert ran == ["T1'", "T2"]


def test_s13_4_two_replans_still_resolve_old_ids():
    """③ 两次 replan（T1''）后：edges 全是 canonical id，后继仍能就绪，别名表有界。

    S14 K-67 变更（判据修正）：
    - K-67(B) `patch()` **物化 edges 端点** → 边不再引用被改名的旧 id，后继就绪
      不再依赖别名（对照 S13 时「必须靠别名兜底」）；
    - K-67(A) 别名表**有界**：第一代 `T1` 在再准入后已无任何引用 → 被剪；
      `_readmit_after_replan` 需要旧 id 的窗口在剪枝**之前**，故安全。
    """
    h = make_harness(run_limits=100)
    plan = _plan_with_edge(h, "S4T")
    a1 = admit(h, "S4T", plan, "T1")
    a1.state = TaskState.QUARANTINED

    first = h.replan(plan=plan, acb=_acb_for(h, "S4T", "T1"),
                     diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert first is not None and first["version"] == 2
    assert _acb_for(h, "S4T", "T1'") is not None
    # K-67(B)：edges 端点已物化为 canonical（不再引用旧 id `T1`）
    assert all(e["from"] in first["tasks"] for e in first["edges"])
    # K-67(A)：别名表有界（旧代至多保留到再准入结束）
    assert len(first["_id_aliases"]) <= 1

    _acb_for(h, "S4T", "T1'").state = TaskState.QUARANTINED
    second = h.replan(plan=first, acb=_acb_for(h, "S4T", "T1'"),
                      diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert second is not None and second["version"] == 3
    assert "T1''" in second["tasks"]
    # 有界：不随 replan 次数线性增长（无剪枝时此处会是 2：{T1, T1'}）
    assert len(second["_id_aliases"]) <= 1
    assert all(e["from"] in second["tasks"] for e in second["edges"])
    assert all(e["to"] in second["tasks"] for e in second["edges"])

    # 关键行为：当前代完成 → 后继仍能就绪（不靠别名也成立，因为边已物化）
    second["tasks"]["T1''"]["state"] = TaskState.DONE
    assert "T2" in h.reconcile.rebuild_ready_set("S4T")
    h.budget.check_invariants()


# ================================================================ S13-5 预算
def test_s13_5_zero_budget_dead_letter_without_patch():
    """`replan_budget=0`：不 patch（事件数/版本不变）、计数不变、DEAD_LETTER。"""
    h = _bare_harness(run_limits=100, replan_budget=0)
    plan = h.planner.plan("B0", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["B0"] = plan
    acb = admit(h, "B0", plan, "T1")
    acb.state = TaskState.QUARANTINED        # 指向 DEAD_LETTER 的合法边
    old_node_id = acb.budget_node.node_id

    events_before = [e for e in h.event_log.all()
                     if e.kind.value == "PLAN_PATCHED"]

    result = h.replan(acb, plan,
                      diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})

    assert result is None
    assert acb.state is TaskState.DEAD_LETTER
    assert acb.replan_count == 0
    assert plan["version"] == 1
    assert [e for e in h.event_log.all()
            if e.kind.value == "PLAN_PATCHED"] == events_before
    # R9：identity 断言（不是计数快照）——旧节点必须被摘除，不留永久残留
    assert old_node_id not in h.budget._nodes
    assert acb.budget_node is None
    assert h.slots.active == 0
    h.budget.check_invariants()


def test_s13_5_budget_one_first_ok_second_dead_letter():
    """`replan_budget=1`：第一次成功、第二次 DLQ（且只产生 1 条 PLAN_PATCHED）。"""
    h = _bare_harness(run_limits=100, replan_budget=1)
    plan = h.planner.plan("B1", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["B1"] = plan
    acb = admit(h, "B1", plan, "T1")
    acb.state = TaskState.QUARANTINED

    first = h.replan(acb, plan,
                     diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert first is not None and first["version"] == 2
    fresh = _acb_for(h, "B1", "T1'")
    assert fresh is not None and fresh.replan_count == 1

    fresh.state = TaskState.REPLANNING       # 第二次的合法入口态
    second = h.replan(fresh, first,
                      diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})

    assert second is None
    assert fresh.state is TaskState.DEAD_LETTER
    assert fresh.replan_count == 1
    assert first["version"] == 2
    assert len([e for e in h.event_log.all()
                if e.kind.value == "PLAN_PATCHED"]) == 1
    h.budget.check_invariants()


def test_s13_5_save_recover_then_replan_still_dead_letter():
    """落盘往返：DEAD_LETTER 快照 recover 后再 replan 仍 DLQ（计数不归零）。"""
    h = _bare_harness(run_limits=100, replan_budget=0)
    plan = h.planner.plan("BR", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["BR"] = plan
    acb = admit(h, "BR", plan, "T1")
    acb.state = TaskState.QUARANTINED
    assert h.replan(acb, plan,
                    diagnosis={"failing_subgoals": ["S1"], "missing_deps": []}) is None
    ref = acb.result_ref
    assert ref

    recovered = h.recover(ref)
    assert recovered.state is TaskState.DEAD_LETTER
    assert recovered.replan_count == 0

    h.plans["BR"] = plan
    again = h.replan(recovered, plan,
                     diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    # R8：DEAD_LETTER 不是 replan 合法入口 → 明确 INVALID_INPUT（不再静默 no-op）
    assert again == Status.INVALID_INPUT
    assert recovered.state is TaskState.DEAD_LETTER
    assert plan["version"] == 1


# ================================================================ S13-6 再准入
def test_s13_6_readmit_cleans_old_node_and_keeps_other_run_lease():
    """再准入：新节点有预算、旧节点 settle+移除、R1 旧 epoch fence、R2 租约仍有效。"""
    h = _bare_harness(run_limits=100, epoch_scope="run")
    plan = h.planner.plan("R1", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    old_node = acb.budget_node
    old_rid = acb.reservation_ids[0]
    assert h.budget.consume(old_rid, 3) == Status.OK     # 已消耗 3
    baseline = h.budget.root.subtree_reserved
    acb.state = TaskState.QUARANTINED

    other_lease = h.intents.reserve("r2", "A-R2-X", 0, 60_000, run_id="R2")

    new_plan = h.replan(acb, plan,
                        diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert new_plan is not None
    fresh = _acb_for(h, "R1", "T1'")

    # ① 新 ACB 有预算节点（扣掉已消耗：min_budget 10 - 3 = 7）
    assert fresh is not None and fresh.budget_node is not None
    assert fresh.budget_node.node_id != old_node.node_id
    fresh_rid = fresh.reservation_ids[0]
    assert h.budget._reservations[fresh_rid][1].amount == 7

    # ② 旧节点已 settle 且移除（无残留：节点表只剩新节点，subtree 守恒）
    #    consumed>0 → remove_node 因永久账返回 False → archive_node 折叠，
    #    预留从 SETTLED 转 ARCHIVED（均为终态）
    assert h.budget._reservations[old_rid][1].state in ("SETTLED", "ARCHIVED")
    assert h.budget._reservations[old_rid][1].state != "ACTIVE"
    assert old_node.node_id not in h.budget._nodes
    assert len([n for n in h.budget._nodes if n.startswith("B-")]) == 1
    assert h.budget.root.subtree_reserved == baseline
    h.budget.check_invariants()

    # ③ R1 旧 epoch 写 → REJECT_STALE（durable 侧推进而本地未跟）
    h._epoch_store.bump_epoch("R1")
    assert h.budget.consume(fresh_rid, 1) == Status.REJECT_STALE

    # ④ per-run 负对照：另一 run 的租约不受 R1 bump 影响
    assert h.intents.is_valid(other_lease) is True


# ================================================================ S13-7 不双跑
def test_s13_7_drain_does_not_double_run_same_task_id():
    """同 task_id 两个 ADMITTED（旧代 + 新代）→ 只跑当前版本那个。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("S7", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["S7"] = plan

    old = h.dispatcher.try_admit(plan["tasks"]["T1"], "S7", plan=plan)
    h.acbs[old.id] = old
    h.run_coordinator.register(old)

    # replan 版本 2（本用例不改名，确保 task_id 相同）
    new_plan, _ = h.planner.patch(plan, {"failing_subgoals": [], "missing_deps": []})
    h.plans["S7"] = new_plan
    new = h.dispatcher.try_admit(new_plan["tasks"]["T1"], "S7", plan=new_plan)
    h.acbs[new.id] = new
    h.run_coordinator.register(new)

    assert old.state is TaskState.ADMITTED and new.state is TaskState.ADMITTED
    assert old.spec["plan_version"] == 1 and new.spec["plan_version"] == 2

    ran: list[str] = []
    orig_run = h.runtime.run

    def spy(acb, decisions=None, crash_at=None):
        ran.append(acb.id)
        return orig_run(acb, decisions, crash_at)

    h.runtime.run = spy
    h.scheduler.push(old.id)
    h.scheduler.push(new.id)
    h.dispatch.drain_scheduler()

    assert ran == [new.id]                 # 恰好 1 个（旧代被跳过）
    assert old.state is TaskState.ADMITTED
    assert new.state is TaskState.DONE
    assert (old.id, "stale_plan_version") in h.dispatch.skipped


def test_s13_7_dead_letter_is_valid_transition_from_replanning():
    """S13-4/5 依赖的合法边：REPLANNING --replan_budget_exhausted--> DEAD_LETTER。"""
    from harness.types import TRANSITIONS
    assert TRANSITIONS[(TaskState.REPLANNING, "replan_budget_exhausted")] \
        is TaskState.DEAD_LETTER


# ================================================ R1 新身份 token（不复用旧 token）
def test_r1_readmit_uses_fresh_identity_token_and_runs():
    """R1：旧 token 绑定旧 ACB id；replan 后新 ACB 必须以**新身份**通过能力校验。"""
    h = Harness(run_limits=100)              # 无 default_token → deny-by-default
    plan = h.planner.plan("R1T", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R1T"] = plan
    old_id = "A-R1T-T1-1"
    tok = h.policy.issue(old_id, tools={"*"}, paths={"*"},
                         network={"*"}, data_class="*")
    acb = h.dispatcher.try_admit(plan["tasks"]["T1"], "R1T", token=tok, plan=plan)
    h.acbs[acb.id] = acb
    assert acb.id == old_id and acb.state is TaskState.ADMITTED
    acb.state = TaskState.QUARANTINED

    new_plan = h.replan(acb, plan,
                        diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert new_plan is not None and new_plan["version"] == 2
    fresh = _acb_for(h, "R1T", "T1'")
    assert fresh is not None and fresh.state is TaskState.ADMITTED
    assert fresh.id != old_id
    # 不复用旧 token：新 ACB 的有效 token 必须绑定到新 id（且 scope 保留）
    resolved = h.policy.resolve_token(h.identity.effective_token(fresh))
    assert resolved is not None and resolved.agent_id == fresh.id
    assert resolved.allows_tool("*")
    # 能真跑（gateway 身份绑定通过）
    h.run_task(fresh, [decision(1, finish())])
    assert fresh.state is TaskState.DONE


# ================================================================ R2 跨 run 串味
def test_r2_alias_is_plan_scoped_no_cross_run_leak():
    """R2：Run A 的 patch 不得让 Run B 的旧 id 解析到 A 的 subgoal。"""
    h = make_harness()
    plan_a = h.planner.plan("RA", [
        {"task_id": "T2", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["RA"] = plan_a
    h.planner.patch(plan_a, {"failing_subgoals": ["S1"], "missing_deps": []})

    plan_b = h.planner.plan("RB", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "logical_subgoal_id": "S2", "min_budget": 10},
        {"task_id": "T3", "logical_subgoal_id": "S3", "min_budget": 10},
    ])
    h.planner.add_logical_edge(plan_b, "T2", "T3")
    h.plans["RB"] = plan_b
    assert h.planner.resolve_alias("T2", plan_b) == "T2"   # 恒等（无别名表）

    a1 = admit(h, "RB", plan_b, "T1")
    h.run_task(a1, [decision(1, finish())])

    assert a1.state is TaskState.DONE
    assert plan_b["tasks"]["T3"]["state"] is TaskState.PLANNED
    assert "T3" not in h.reconcile.rebuild_ready_set("RB")


# ============================================== R3 propagate_failure + CANCELLED 语义
def test_r3_propagate_failure_normalizes_alias_and_cancels_successor():
    """R3：`T1'` 取消后，边的旧 id `T1` 仍能匹配 → 后继也被 CANCELLED，不再被推起。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R3", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "min_budget": 10},
    ])
    h.plans["R3"] = plan
    h.planner.add_logical_edge(plan, "T1", "T2")
    a1 = admit(h, "R3", plan, "T1")
    a1.state = TaskState.QUARANTINED
    new_plan = h.replan(a1, plan,
                        diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    fresh = _acb_for(h, "R3", "T1'")
    assert new_plan is not None and fresh is not None

    fresh.state = TaskState.CANCELLED
    h.tasks.finalize(fresh)

    assert new_plan["tasks"]["T2"]["state"] is TaskState.CANCELLED
    assert h.scheduler.is_empty()
    assert _acb_for(h, "R3", "T2") is None


def test_r3_cancelled_predecessor_is_not_satisfied():
    """R3：CANCELLED 不再算「前驱满足」→ 后继既不 ready 也不能准入。"""
    h = make_harness()
    plan = h.planner.plan("R3N", [
        {"task_id": "T1", "min_budget": 10},
        {"task_id": "T2", "min_budget": 10},
    ])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["R3N"] = plan
    plan["tasks"]["T1"]["state"] = TaskState.CANCELLED

    assert h.reconcile.rebuild_ready_set("R3N") == []
    denied = h.dispatcher.try_admit(plan["tasks"]["T2"], "R3N", plan=plan)
    assert denied.state is TaskState.INADMISSIBLE
    assert denied.failure == "PREDECESSOR_NOT_DONE"


# ================================================================ R4 计数不泄漏
def test_r4_append_batch_failure_rolls_back_budget_and_retries():
    """R4：`append_batch` 抛 → `replan_count` 回滚、attempts 留下、重试仍可用。"""
    h = _bare_harness(run_limits=100, replan_budget=2)
    plan = h.planner.plan("R4A", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R4A"] = plan
    acb = admit(h, "R4A", plan, "T1")
    acb.state = TaskState.QUARANTINED

    orig = h.event_log.append_batch

    def boom(*args, **kwargs):
        raise RuntimeError("append failed")

    h.event_log.append_batch = boom
    with pytest.raises(RuntimeError):
        h.replan(acb, plan,
                 diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert acb.replan_count == 0
    assert acb.replan_attempts == 1
    assert plan["version"] == 1
    assert acb.state is TaskState.QUARANTINED

    h.event_log.append_batch = orig
    assert h.replan(acb, plan,
                    diagnosis={"failing_subgoals": ["S1"], "missing_deps": []}) \
        is not None
    assert acb.replan_count == 1


def test_r4_patch_failure_restores_quarantine_not_stuck_replanning():
    """R4：patch 造环抛 → 计数回滚、REPLANNING 拉回 QUARANTINED，不卡非终态。"""
    h = _bare_harness(run_limits=100, replan_budget=2)
    plan = h.planner.plan("R4P", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "min_budget": 10},
    ])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["R4P"] = plan
    acb = admit(h, "R4P", plan, "T1")
    acb.state = TaskState.REPLANNING

    with pytest.raises(ValueError):
        h.replan(acb, plan, diagnosis={
            "failing_subgoals": [], "missing_deps": [{"from": "T2", "to": "T1"}]})
    assert acb.replan_count == 0
    assert acb.replan_attempts == 1
    assert acb.state is TaskState.QUARANTINED
    assert plan["version"] == 1


# ================================================ R5 后继自动准入（生产常态）
def test_r5_successor_is_admitted_and_run_without_pre_admission():
    """R5：只准入 T1；T1 DONE 后 `_finalize` 必须**就地准入** T2 并跑完。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R5", [
        {"task_id": "T1", "min_budget": 10},
        {"task_id": "T2", "min_budget": 10},
    ])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["R5"] = plan
    a1 = admit(h, "R5", plan, "T1")

    h.run_task(a1, [decision(1, finish())])

    a2 = _acb_for(h, "R5", "T2")
    assert a2 is not None and a2.state is TaskState.DONE
    assert plan["tasks"]["T2"]["state"] is TaskState.DONE
    assert ("R5:T2", "push_no_current_acb") not in h.dispatch.skipped
    h.budget.check_invariants()


# ================================================================ R6 同版本双准入
def test_r6_same_version_double_admit_runs_only_once():
    """R6：同一 `(run, task, plan_version)` 两次 try_admit → 复用同一 ACB，只跑一次。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R6", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R6"] = plan

    first = h.dispatcher.try_admit(plan["tasks"]["T1"], "R6")
    second = h.dispatcher.try_admit(plan["tasks"]["T1"], "R6")
    assert first.state is TaskState.ADMITTED
    assert second.id == first.id
    assert len([a for a in h.acbs.values()
                if a.run_id == "R6" and a.task_id == "T1"]) <= 1
    h.acbs[first.id] = first

    ran: list[str] = []
    orig_run = h.runtime.run

    def spy(acb, decisions=None, crash_at=None):
        ran.append(acb.id)
        return orig_run(acb, decisions, crash_at)

    h.runtime.run = spy
    h.scheduler.push(first.id)
    h.scheduler.push(second.id)
    h.dispatch.drain_scheduler()
    assert ran == [first.id]


# ============================================================ R7 superseded 一致性
def test_r7_finalize_after_replan_does_not_resurrect_old_acb():
    """R7：replan 后旧 ACB 失效；`_finalize(old)` 不得覆盖快照 → recover 无僵尸。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R7", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R7"] = plan
    acb = admit(h, "R7", plan, "T1")
    acb.state = TaskState.QUARANTINED
    assert h.replan(acb, plan,
                    diagnosis={"failing_subgoals": ["S1"], "missing_deps": []}) \
        is not None
    old_ref = acb.result_ref
    assert acb.superseded is True

    h.tasks.finalize(acb)                       # 旧路径的迟到结算
    assert acb.result_ref == old_ref        # 快照未被覆盖

    recovered = h.recover(old_ref)
    assert recovered.superseded is True
    assert recovered.state is TaskState.CANCELLED
    assert not (recovered.state is TaskState.QUARANTINED and recovered.needs_readmit)


# ============================================================ R8 入口态前置
def test_r8_replan_rejects_done_and_budget_zero_admitted():
    """R8：DONE / ADMITTED 不得 replan（INVALID_INPUT），不得静默 no-op。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R8", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R8"] = plan
    acb = admit(h, "R8", plan, "T1")
    acb.state = TaskState.DONE
    assert h.replan(acb, plan,
                    diagnosis={"failing_subgoals": ["S1"], "missing_deps": []}) \
        == Status.INVALID_INPUT
    assert plan["version"] == 1

    h0 = _bare_harness(run_limits=100, replan_budget=0)
    plan0 = h0.planner.plan("R8Z", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h0.plans["R8Z"] = plan0
    a0 = admit(h0, "R8Z", plan0, "T1")     # 仍 ADMITTED
    assert h0.replan(a0, plan0,
                     diagnosis={"failing_subgoals": ["S1"], "missing_deps": []}) \
        == Status.INVALID_INPUT
    assert a0.state is TaskState.ADMITTED


# ============================================================ R10 边去重 / 规范化
def test_r10_add_edge_idempotent_and_normalizes_renamed_ids():
    """R10：同边重复加不增长；改名后旧 id 端点规范到当前 task；未知端点 fail-fast。"""
    h = make_harness()
    plan = h.planner.plan("R10", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "min_budget": 10},
    ])
    h.plans["R10"] = plan
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.planner.add_logical_edge(plan, "T1", "T2")
    assert len(plan["edges"]) == 1

    new_plan, _ = h.planner.patch(
        plan, {"failing_subgoals": ["S1"], "missing_deps": []})
    h.planner.add_logical_edge(new_plan, "T1", "T2")   # 旧 id 归一化后仍去重
    assert len(new_plan["edges"]) == 1

    with pytest.raises(ValueError):
        h.planner.add_logical_edge(new_plan, "T-UNKNOWN", "T2")


# ============================================================ R11 同 subgoal 多后继
def test_r11_duplicate_subgoal_fails_fast():
    """R11：同一 plan 内重复 `logical_subgoal_id` → plan() 直接拒绝。"""
    h = make_harness()
    with pytest.raises(ValueError):
        h.planner.plan("R11", [
            {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
            {"task_id": "T2", "logical_subgoal_id": "S1", "min_budget": 10},
        ])


def test_r11_finalize_does_not_push_sibling_subgoal_successors():
    """R11：同 subgoal 的任务不能靠 subgoal 相等把别人的后继推起来。"""
    h = make_harness(run_limits=100)
    plan = {
        "run_id": "R11F", "version": 1, "edges": [],
        "tasks": {
            "T1": {"task_id": "T1", "logical_subgoal_id": "S1",
                   "footprint": {"min_budget": 10}, "state": TaskState.DONE,
                   "plan_version": 1},
            "T2": {"task_id": "T2", "logical_subgoal_id": "S1",
                   "footprint": {"min_budget": 10}, "state": TaskState.DONE,
                   "plan_version": 1},
            "T3": {"task_id": "T3", "logical_subgoal_id": "S3",
                   "footprint": {"min_budget": 10}, "state": TaskState.PLANNED,
                   "plan_version": 1},
            "T4": {"task_id": "T4", "logical_subgoal_id": "S4",
                   "footprint": {"min_budget": 10}, "state": TaskState.PLANNED,
                   "plan_version": 1},
        },
    }
    plan["edges"] = [{"from": "T1", "to": "T3"}, {"from": "T2", "to": "T4"}]
    h.plans["R11F"] = plan
    a1 = ACB(id="A-R11F-T1-1", run_id="R11F", task_id="T1",
             logical_subgoal_id="S1")
    a1.spec["plan_version"] = 1
    a1.state = TaskState.DONE
    h.acbs[a1.id] = a1

    h.tasks.finalize(a1)

    assert _acb_for(h, "R11F", "T3") is not None   # 真后继被就地准入
    assert _acb_for(h, "R11F", "T4") is None       # 同 subgoal 的旁支不得被推起


# ================================================ R12 覆盖缺口（计数往返 / 歧义等）
def test_r12_replan_count_survives_recover_then_still_dead_letter():
    """R12/P0-5：`replan_count>0` 落盘 → recover 仍保留 → 再 replan 直接 DLQ。"""
    h = _bare_harness(run_limits=100, replan_budget=1)
    plan = h.planner.plan("R12C", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R12C"] = plan
    acb = admit(h, "R12C", plan, "T1")
    acb.state = TaskState.QUARANTINED
    first = h.replan(acb, plan,
                     diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert first is not None
    fresh = _acb_for(h, "R12C", "T1'")
    assert fresh.replan_count == 1
    ref = fresh.result_ref

    recovered = h.recover(ref)
    assert recovered.replan_count == 1
    h.plans["R12C"] = first
    recovered.state = TaskState.REPLANNING
    again = h.replan(recovered, first,
                     diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert again is None
    assert recovered.state is TaskState.DEAD_LETTER
    assert recovered.replan_count == 1


def test_r12_resolve_current_acb_ambiguity_is_deterministic():
    """R12/R5：歧义候选按 `(plan_version, generation, acb.id)` 选一个，落选者 readmit。"""
    h = make_harness()
    plan = h.planner.plan("R12A", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R12A"] = plan
    a = ACB(id="A-R12A-T1-1", run_id="R12A", task_id="T1")
    b = ACB(id="A-R12A-T1-2", run_id="R12A", task_id="T1")
    for acb, gen in ((a, 1), (b, 2)):
        acb.spec["plan_version"] = 1
        acb.generation = gen
        acb.state = TaskState.ADMITTED
        h.acbs[acb.id] = acb

    picked = h.tasks.resolve_current_acb("R12A", "T1", 1, plan=plan)
    assert picked.id == b.id
    assert a.needs_readmit is True
    assert (("R12A:T1", "ambiguous_pick_best")) in h.dispatch.skipped


def test_r12_readmit_failure_keeps_old_acb_and_readmit_flag():
    """R12：再准入失败（槽位耗尽）→ 旧 ACB 保留 + needs_readmit，不抛。"""
    h = _bare_harness(run_limits=100)
    plan = h.planner.plan("R12F", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["R12F"] = plan
    acb = admit(h, "R12F", plan, "T1")
    acb.state = TaskState.QUARANTINED

    h.slots._limit = 0                       # 模拟容量耗尽（旧槽位已释放）
    new_plan = h.replan(acb, plan,
                        diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert new_plan is not None
    assert _acb_for(h, "R12F", "T1'") is None
    assert acb.needs_readmit is True
    assert acb.id in h.acbs
    assert acb.state is TaskState.QUARANTINED

