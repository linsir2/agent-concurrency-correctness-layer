"""S14A 边界缺口：K-69（`tick` 的持久候选集 + 外部驱动器）。

对应 `docs/s14-boundaries-and-ms8-closure.md` §2.1 / §2.2 与 §4 的
S14-1 / S14-2 / S14-3 / S14-4 / S14-5。

**为什么这些用例有牙**：S13a 的 `tick` 只遍历 `self.acbs`；`recover()` 一次
只恢复 1 个 ACB → 未 `recover` 的等待态**永远不在扫描范围**。下面的
`test_s14_1_*` 在旧实现下必红（settled_state 仍是 WAITING_APPROVAL）。
"""

from __future__ import annotations

import pytest
from conftest import admit, decision, finish, hitl, make_harness, seed

from harness import ACB, Harness, ManualClock, Status, TaskState
from harness.checkpoint import FileCheckpointStore
from harness.storage._lock import FencingRejected


def _admit_waiting(h: Harness, run_id: str = "R1", task_id: str = "T1",
                   min_budget: int = 10):
    """准入并跑到第一次 `request_hitl`（返回时仍在 `WAITING_APPROVAL`）。"""
    plan = h.planner.plan(run_id, [{"task_id": task_id,
                                    "min_budget": min_budget}])
    h.plans[run_id] = plan
    acb = admit(h, run_id, plan, task_id)
    st = h.run_task(acb, [decision(1, hitl())])
    assert st == Status.NEEDS_APPROVAL
    assert acb.state is TaskState.WAITING_APPROVAL
    assert acb.hitl_pending and acb.hitl_pending["approval_id"]
    return acb


def _shared_harness(h: Harness) -> Harness:
    """与 `h` **共享** CheckpointStore + EventLog + clock 的第二个 Harness。

    注意：**不调用 `recover()`** —— 这正是 K-69 的场景（supervisor 只恢复它
    关心的 ACB，其它等待态不在内存）。
    """
    h2 = Harness(run_limits=h._run_limits, event_log=h.event_log,
                 checkpoints=h.checkpoints, clock=h.clock)
    h2.policy.default_token = h2.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h2


def _persisted_state(h: Harness, _acb_id: str) -> dict:
    """读该 ACB 的**最新**快照（`_finalize` 会写新 ref，旧 ref 是历史）。"""
    ref = h.checkpoints.latest_ref_for_acb(_acb_id)
    assert ref is not None, f"{_acb_id} 没有任何落盘"
    return h.checkpoints.load_strict(ref)


# ================================================================ S14-1
def test_s14_1_unrecovered_waiting_is_settled_by_tick():
    """**K-69 主用例**：未 `recover` 的等待态，`tick` 也必须结算（旧实现必红）。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=1000)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    assert acb.result_ref, "等待态必须已落盘（否则本用例测不到持久真相）"
    deadline = acb.hitl_pending["deadline_ms"]

    # 进程 B：共享 store，**不 recover**，内存里没有该 ACB
    h2 = _shared_harness(h)
    assert acb.id not in h2.acbs, "前提：进程 B 内存中没有该 ACB"

    # 未到 deadline → 不结算
    h2.tick(now_ms=deadline - 1)
    assert _persisted_state(h, acb.id)["state"] == "WAITING_APPROVAL", (
        "未到期的等待态不得被结算")

    # 到期 → 必须结算（旧实现：tick 只看 self.acbs → 这里仍是 WAITING_APPROVAL）
    h2.tick(now_ms=deadline)
    payload = _persisted_state(h, acb.id)
    assert payload["state"] == "FAILED", (
        f"未 recover 的等待态逾期未结算（实际 {payload['state']}）—— K-69 缺口")
    assert payload["failure"] == "APPROVAL_TIMEOUT"
    assert payload["hitl_pending"] is None
    assert payload["hitl_outcome"]["decision"] == "timeout"


def test_s14_1_negative_control_memory_only_tick_misses_persisted():
    """负对照：`persist_scan=False`（等价旧行为）→ 未 recover 的等待态**不被结算**。

    证明 S14-1 的绿不是碰巧，而是持久扫描真的在起作用。
    """
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=1000)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    deadline = acb.hitl_pending["deadline_ms"]

    h2 = _shared_harness(h)
    h2.tick(now_ms=deadline, persist_scan=False)      # 旧行为：仅内存

    assert _persisted_state(h, acb.id)["state"] == "WAITING_APPROVAL", (
        "仅扫内存时不该结算（若此处被结算，说明 persist_scan 未生效）")


# ================================================================ S14-2
def test_s14_2_tick_is_a_fixed_point():
    """`tick(now)` 之后 `next_deadline_ms()` 必须 `> now` 或 `None`（一次收敛）。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=500)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    deadline = acb.hitl_pending["deadline_ms"]

    assert h.next_deadline_ms() == deadline, "等待态的最近到期点应等于其 deadline"

    h.tick(now_ms=deadline)
    nxt = h.next_deadline_ms()
    assert nxt is None or nxt > deadline, (
        f"tick 后仍有未收敛的到期点：{nxt}（不动点性质被破坏）")


def test_s14_2_next_deadline_prefers_earliest():
    """两个等待态 → `next_deadline_ms()` 取**最早**；全部结算后为 `None`。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock, hitl_timeout_ms=500)
    seed(h, "r", "base")
    a = _admit_waiting(h, run_id="R1", task_id="T1")
    b = _admit_waiting(h, run_id="R2", task_id="T1")

    da = a.hitl_pending["deadline_ms"]
    db = b.hitl_pending["deadline_ms"]
    early = min(da, db)
    assert h.next_deadline_ms() == early

    h.tick(now_ms=max(da, db))
    assert h.next_deadline_ms() is None
    assert a.state is TaskState.FAILED and b.state is TaskState.FAILED


# ================================================================ S14-3
def test_s14_3_pump_reports_remaining_until_zero():
    """`pump()` 返回**仍待结算**数：到期前 >0、到期后 0（驱动器可据此退出）。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=500)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    deadline = acb.hitl_pending["deadline_ms"]

    assert h.pump(now_ms=deadline - 1) > 0, "未到期 → 仍有待结算项"
    assert h.pump(now_ms=deadline) == 0, "到期结算后 → 无待结算项"


# ================================================================ S14-4
def test_s14_4_without_checkpoint_saves_tick_is_memory_only():
    """零破坏：没有任何落盘时 `_tick_candidates()` 与内存完全一致。"""
    h = make_harness()
    assert h.checkpoints.refs_all() == [], "前提：无落盘"
    assert h.dispatch._tick_candidates() == list(h.acbs.values())


# ================================================================ S14-5
def test_s14_5_tick_finalize_does_not_violate_lock_order(tmp_path):
    """锁序红线：durable 模式下 `tick` 触发 `_finalize` **不得** `LockOrderViolation`。

    旧写法（持 store 锁调 `_finalize`）会让 `budget(1)` 套在 `store(5)` 里 →
    反向获取。这里用 `FileCheckpointStore` 真的走两把锁。
    """
    clock = ManualClock()
    store = FileCheckpointStore(str(tmp_path))
    h = Harness(run_limits=1000, clock=clock, checkpoints=store,
                hitl_timeout_ms=500)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    deadline = acb.hitl_pending["deadline_ms"]

    # 不抛即通过（LockOrderViolation 会在 _FileLock._push 处直接抛）
    h.tick(now_ms=deadline)

    assert acb.state is TaskState.FAILED
    assert acb.failure == "APPROVAL_TIMEOUT"
    assert not [lease for lease in h.intents._leases.values() if lease.valid]
    assert acb.id not in h.slots.active_keys()
    h.budget.check_invariants()
    assert h.state.tick_skipped == [], f"不应有跳过项：{h.state.tick_skipped}"


def test_s14_5_hydrate_failure_is_recorded_not_silent(tmp_path, monkeypatch):
    """水合失败必须记 `_tick_skipped`（不静默），且不影响其它候选的结算。"""
    clock = ManualClock()
    store = FileCheckpointStore(str(tmp_path))
    h = Harness(run_limits=1000, clock=clock, checkpoints=store,
                hitl_timeout_ms=500)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    deadline = acb.hitl_pending["deadline_ms"]

    # 让「读持久」失败一次：refs_all 正常，但 load_strict 抛
    orig = store.load_strict
    calls = {"n": 0}

    def boom(ref):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("injected load failure")
        return orig(ref)

    monkeypatch.setattr(store, "load_strict", boom)

    h2 = _shared_harness(h)
    h2.tick(now_ms=deadline)

    assert ("", "refs_all_failed") not in h2.state.tick_skipped
    assert any(reason == "tick_load_failed" for _, reason in h2.state.tick_skipped), (
        f"水合失败未被记录：{h2.state.tick_skipped}")


@pytest.mark.parametrize("resume", ["APPROVED", "TIMEOUT"])
def test_s14_5_pump_drives_both_outcomes(resume):
    """`pump` 是唯一驱动面：批准后不再计入待结算；超时才计入并最终清零。"""
    clock = ManualClock()
    h = make_harness(clock=clock, hitl_timeout_ms=500)
    seed(h, "r", "base")
    acb = _admit_waiting(h)
    deadline = acb.hitl_pending["deadline_ms"]
    approval_id = acb.hitl_pending["approval_id"]

    if resume == "APPROVED":
        h.hitl.set_result(acb.run_id, approval_id, {"approved": True})
        h.tick(now_ms=deadline)
        assert acb.state is TaskState.WAITING_APPROVAL  # 决策优先，不判超时
        assert acb.failure is None
        st = h.run_task(acb, [decision(2, finish())])
        assert st == Status.OK and acb.state is TaskState.DONE
        assert h.pump(now_ms=deadline) == 0
    else:
        assert h.pump(now_ms=deadline) == 0
        assert acb.state is TaskState.FAILED
        assert acb.failure == "APPROVAL_TIMEOUT"


# ================================================================ S14-6（K-68）
def test_s14_6_update_children_cas_does_not_lose_update():
    """**K-68**：两个写者改同一父的 `children_ids`，`expect_ref` 陈旧也不丢更新。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R", [{"task_id": "T1", "logical_subgoal_id": "S1",
                                 "min_budget": 10}])
    h.plans["R"] = plan
    parent = admit(h, "R", plan, "T1")
    h.checkpoints.save(parent)                 # 先落盘（否则没有 ref）
    ref0 = h.checkpoints.latest_ref_for_acb(parent.id)
    assert ref0

    # 写者 A（以 ref0 为期望）追加 CA
    h.checkpoints.update_children(
        parent.id, lambda ch: list(ch) + [("CA", None)], expect_ref=ref0)
    # 写者 B 仍拿**陈旧** ref0 追加 CB → 内部重读重应用，A 的写入不得被覆盖
    h.checkpoints.update_children(
        parent.id, lambda ch: list(ch) + [("CB", None)], expect_ref=ref0)

    latest = h.checkpoints.load_strict(h.checkpoints.latest_ref_for_acb(parent.id))
    ids = [str(c[0]) for c in (latest.get("children_ids") or [])]
    assert "CA" in ids and "CB" in ids, f"丢更新：{ids}"


def test_s14_6_update_children_negative_control_stale_snapshot_loses(tmp_path):
    """负对照：**不做**条件写（直接按旧快照整份覆盖）会丢掉另一写者的条目。"""
    store = FileCheckpointStore(str(tmp_path))
    h = Harness(run_limits=100, checkpoints=store)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    plan = h.planner.plan("R", [{"task_id": "T1", "logical_subgoal_id": "S1",
                                 "min_budget": 10}])
    h.plans["R"] = plan
    parent = admit(h, "R", plan, "T1")
    store.save(parent)

    # 故意模拟「旧实现」：先读 payload，再基于它整份写，中间被别人插入一次
    stale = store.load(store.latest_ref_for_acb(parent.id))
    store.update_children(parent.id, lambda ch: list(ch) + [("CA", None)])
    # 旧写法会用 stale 的 children 覆盖 → CB 覆盖掉 CA
    stale["children_ids"] = [[str(c[0]), None]
                             for c in list(stale.get("children_ids") or [])]
    stale["children_ids"].append(["CB", None])
    store._save_payload(stale)

    latest = store.load_strict(store.latest_ref_for_acb(parent.id))
    ids = [str(c[0]) for c in (latest.get("children_ids") or [])]
    assert "CA" not in ids and "CB" in ids, (
        f"负对照未生效（应丢 CA）：{ids}")


def test_s14_6_update_children_retries_exhausted_raises(tmp_path):
    """重试耗尽 → 显式 `FencingRejected`（不静默丢更新）。"""
    store = FileCheckpointStore(str(tmp_path))
    h = Harness(run_limits=100, checkpoints=store)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    plan = h.planner.plan("R", [{"task_id": "T1", "logical_subgoal_id": "S1",
                                 "min_budget": 10}])
    h.plans["R"] = plan
    parent = admit(h, "R", plan, "T1")
    store.save(parent)

    # 每次读 latest 都返回一个**新的**假 ref → 期望永远对不上 → 耗尽
    seq = iter([f"F{i}" for i in range(100)])
    store.latest_ref_for_acb = lambda _aid: next(seq)
    with pytest.raises(FencingRejected):
        store.update_children(
            parent.id, lambda ch: list(ch), expect_ref="NEVER", retries=3)


# ================================================================ S14-7（K-71a）
def test_s14_7_inline_depth_exceeded_is_terminal_and_propagates():
    """**K-71(a)**：深度超限是**终局** → `GUARD_VIOLATION` + `FAILED` + 传播。"""
    from harness.harness import _MAX_INLINE_DEPTH

    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    plan = h.planner.plan("R", [{"task_id": "T1", "logical_subgoal_id": "S1",
                                 "min_budget": 10}])
    h.plans["R"] = plan
    acb = admit(h, "R", plan, "T1")

    h.state.tls.depth = _MAX_INLINE_DEPTH              # 已到上限
    res = h.dispatch._run_one_inline(acb, [decision(1, finish())])

    assert res.status is Status.GUARD_VIOLATION, (
        f"旧实现返回 INVALID_INPUT（可重试语义），实际 {res.status}")
    assert res.detail.get("reason") == "INLINE_DEPTH_EXCEEDED"
    assert acb.state is TaskState.FAILED
    assert acb.failure == "INLINE_DEPTH_EXCEEDED"
    # 终态 → 不占槽位/预算，Run 可 finalize
    assert acb.id not in h.slots.active_keys()
    assert h.budget.root.subtree_reserved == 0
    assert (acb.id, "inline_depth_exceeded") in h.dispatch.skipped
    h.budget.check_invariants()


# ================================================================ S14-8/9（K-67）
def test_s14_8_patch_materializes_edges_to_canonical_ids():
    """**K-67(B)**：`patch()` 后 edges 端点全是 canonical id（不再引用旧 id）。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "logical_subgoal_id": "S2", "min_budget": 10},
    ])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["R"] = plan

    new_plan, _ = h.planner.patch(plan, {"failing_subgoals": ["S1"]})

    assert set(new_plan["tasks"]) == {"T1'", "T2"}
    assert new_plan["edges"] == [
        {"from": "T1'", "to": "T2", "kind": new_plan["edges"][0]["kind"]}], (
        f"edges 未物化（仍引用旧 T1）：{new_plan['edges']}")


def test_s14_9_prune_keeps_alias_referenced_by_active_acb():
    """**K-67(A) 判据修正**：别名只被**活跃 ACB 的 task_id** 引用时**不得**被剪。

    旧判据（只看 `edges.from`）会剪掉它 → `_readmit_after_replan` 取不到 task
    → `replan_no_task`。这是本次调研的代码级修正。
    """
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R", [
        {"task_id": "T1", "logical_subgoal_id": "S1", "min_budget": 10},
        {"task_id": "T2", "logical_subgoal_id": "S2", "min_budget": 10},
    ])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["R"] = plan
    new_plan, _ = h.planner.patch(plan, {"failing_subgoals": ["S1"]})
    assert new_plan["_id_aliases"] == {"T1": "S1"}

    # 无活跃 ACB 引用 T1 → 被剪
    h.tasks.prune_id_aliases("R", new_plan)
    assert new_plan["_id_aliases"] == {}

    # 有活跃 ACB 持有旧 task_id `T1` → **不得剪**
    plan2, _ = h.planner.patch(plan, {"failing_subgoals": ["S1"]})
    stale = ACB(id="LIVE1", run_id="R", task_id="T1", logical_subgoal_id="S1")
    h.acbs["LIVE1"] = stale
    h.tasks.prune_id_aliases("R", plan2)
    assert plan2["_id_aliases"] == {"T1": "S1"}, (
        f"在途 ACB 的别名被剪（旧判据漏洞）：{plan2['_id_aliases']}")
