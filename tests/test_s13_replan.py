"""S13b：replan（alias 三处消费点 + 预算 + 再准入清理 + D10 不双跑）。

对应 `docs/s13-orchestration.md` §2.3 与 §3 的 S13-4 / S13-5 / S13-6 / S13-7。
"""

from __future__ import annotations

from conftest import admit, decision, finish, make_harness

from harness import Harness, Status, TaskState


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
    """负对照：去掉 alias 解析 → 旧 id `T1` 落在 done 之外 → T2 不就绪。"""
    h = make_harness()
    plan = _plan_with_edge(h)
    new_plan = _patched(h, plan)
    new_plan["tasks"]["T1'"]["state"] = TaskState.DONE
    assert "T2" in h.reconcile.rebuild_ready_set("S4")

    h.planner.resolve_alias = lambda old_id, plan=None: old_id
    assert h.reconcile.rebuild_ready_set("S4") == []


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
    """③ 两次 replan（T1''）后，T1 / T1' 仍一跳解析到 S1。"""
    h = make_harness(run_limits=100)
    plan = _plan_with_edge(h, "S4T")
    admit(h, "S4T", plan, "T1")

    first = h.replan(plan=plan, acb=_acb_for(h, "S4T", "T1"),
                     diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert first is not None and first["version"] == 2
    assert _acb_for(h, "S4T", "T1'") is not None

    second = h.replan(plan=first, acb=_acb_for(h, "S4T", "T1'"),
                      diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})
    assert second is not None and second["version"] == 3
    assert "T1''" in second["tasks"]
    assert h.planner.resolve_alias("T1", second) == "S1"
    assert h.planner.resolve_alias("T1'", second) == "S1"
    assert h.planner.task_for(second, "T1")["task_id"] == "T1''"

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

    events_before = [e for e in h.event_log.all()
                     if e.kind.value == "PLAN_PATCHED"]
    nodes_before = len(h.budget._nodes)

    result = h.replan(acb, plan,
                      diagnosis={"failing_subgoals": ["S1"], "missing_deps": []})

    assert result is None
    assert acb.state is TaskState.DEAD_LETTER
    assert acb.replan_count == 0
    assert plan["version"] == 1
    assert [e for e in h.event_log.all()
            if e.kind.value == "PLAN_PATCHED"] == events_before
    assert len(h.budget._nodes) == nodes_before
    assert h.slots.active == 0
    h.budget.check_invariants()


def test_s13_5_budget_one_first_ok_second_dead_letter():
    """`replan_budget=1`：第一次成功、第二次 DLQ（且只产生 1 条 PLAN_PATCHED）。"""
    h = _bare_harness(run_limits=100, replan_budget=1)
    plan = h.planner.plan("B1", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10}])
    h.plans["B1"] = plan
    acb = admit(h, "B1", plan, "T1")

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
    assert h.replan(recovered, plan,
                    diagnosis={"failing_subgoals": ["S1"], "missing_deps": []}) is None
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
    h._drain_scheduler()

    assert ran == [new.id]                 # 恰好 1 个（旧代被跳过）
    assert old.state is TaskState.ADMITTED
    assert new.state is TaskState.DONE
    assert (old.id, "stale_plan_version") in h._drain_skipped


def test_s13_7_dead_letter_is_valid_transition_from_replanning():
    """S13-4/5 依赖的合法边：REPLANNING --replan_budget_exhausted--> DEAD_LETTER。"""
    from harness.types import TRANSITIONS
    assert TRANSITIONS[(TaskState.REPLANNING, "replan_budget_exhausted")] \
        is TaskState.DEAD_LETTER
