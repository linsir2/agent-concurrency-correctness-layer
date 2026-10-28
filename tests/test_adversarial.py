"""13 个对抗用例的可执行测试（对应 use-cases-and-adversarial-tests.md A1–A13）。"""

from __future__ import annotations

import pytest
from conftest import (admit, commit, decision, finish, hitl, make_harness,
                      seed, stage, state_of, tool)
from harness import Mode, Status, TaskState, Write, EventKind, TASK_TERMINAL, Harness
from harness.runtime import CrashInjected
from harness.storage.blob import canonical_bytes


# ---------------------------------------------------------------- A1
def test_a1_compensation_cross_no_overwrite():
    """A1：走**完整 compensate 流程** → REJECT_COMPENSATE → PIVOT，不覆写 B 的值。"""
    h = make_harness()
    v0 = seed(h, "fileX", "base")

    plan = h.planner.plan("A1", [{"task_id": "T1"}, {"task_id": "T2"}])
    h.plans["A1"] = plan

    # A 提交 fileX@v1（内容 A）
    a = admit(h, "A1", plan, "T1")
    a.base_version["fileX"] = v0
    a.read_set["fileX"] = v0
    a.staged["fileX"] = "A"
    assert h.commit_batch(a, [Write("fileX", content="A")]) == Status.OK
    hash_of_A = h.workspace.content_hash("fileX")
    # 内容寻址：指纹必须等于 sha256(canonical_bytes("A"))（而非同值自比）
    assert hash_of_A == h.workspace._blobs.put(canonical_bytes("A"))

    # B 提交 fileX@v2（内容 B）
    b = admit(h, "A1", plan, "T2")
    b.base_version["fileX"] = v0 + 1
    b.read_set["fileX"] = v0 + 1
    b.staged["fileX"] = "B"
    assert h.commit_batch(b, [Write("fileX", content="B")]) == Status.OK
    assert h.workspace.get("fileX") == "B"
    assert h.workspace.content_hash("fileX") != hash_of_A      # 内容确实变了

    # A 触发补偿（模拟同批后续写失败）→ 反写前复验失败 → PIVOT
    w = Write("fileX", content="A")
    w.version_before = v0
    w.applied_hash = hash_of_A                 # A apply 时的 hash
    h.compensate(a, [w])

    assert w.pivot is True                     # 已升级 PIVOT
    assert h.workspace.get("fileX") == "B"     # B 的值未被覆写
    assert {"resource": "fileX", "state": "PIVOT"} in h.compensation_journal


# ---------------------------------------------------------------- A2
def test_a2_parent_done_child_budget_transfer():
    """A2：父 DONE 后子仍 RUNNING → 预算过户 Run 级，子不被误杀。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("A2", [{"task_id": "P", "min_budget": 60},
                                 {"task_id": "C", "min_budget": 40}])
    h.plans["A2"] = plan

    p = admit(h, "A2", plan, "P")
    assert p.state == TaskState.ADMITTED
    c = admit(h, "A2", plan, "C")
    assert c.state == TaskState.ADMITTED

    h.run_task(p, [decision(1, finish())])
    assert p.state == TaskState.DONE

    # 子仍 active（未终态）→ 不应 finalize
    assert h.run_coordinator.is_cancelled(c) is False
    # 父终态释放预算后：root.remaining = 100 − 0(own) − 40(child) = 60
    assert h.budget.root.remaining == 60
    assert h.budget.root.children_reserved == 40


# ---------------------------------------------------------------- A3
def test_a3_compensation_crash_midway():
    """A3：补偿中途崩溃 → compensation_journal 对账 → 只补未完成部分。"""
    h = make_harness()
    v1 = seed(h, "r1", "base1")
    v2 = seed(h, "r2", "base2")

    plan = h.planner.plan("A3", [{"task_id": "T1"}])
    h.plans["A3"] = plan
    acb = admit(h, "A3", plan, "T1")
    acb.staged = {"r1": "v1", "r2": "v2"}
    # 走框架 commit 路径：提交后由 runtime 刷新 read_set/base_version（无需手工刷新）
    h.run_task(acb, [decision(1, commit("r1"), commit("r2"))])
    assert h.workspace.get("r1") == "v1"
    assert h.workspace.get("r2") == "v2"

    # 模拟：w2 补偿完成（journal 记录），w1 补偿中途崩溃 → 恢复后只补 w1
    w1 = Write("r1", content="v1")
    w1.version_before = v1
    w1.applied_hash = h.workspace.content_hash("r1")
    w2 = Write("r2", content="v2")
    w2.version_before = v2
    w2.applied_hash = h.workspace.content_hash("r2")
    w2.compensated = True
    h.compensate(acb, [w1, w2])       # w2 已 compensated → SKIPPED

    assert w1.compensated is True
    assert w2.compensated is True
    assert h.workspace.get("r1") == "base1"     # w1 已回滚
    assert h.workspace.get("r2") == "v2"        # w2 的补偿在崩溃前已完成（保持 v2）
    states = [e["state"] for e in h.compensation_journal]
    assert "COMPENSATED" in states               # w1
    assert "SKIPPED" in states                   # w2


# ---------------------------------------------------------------- A4
def test_a4_budget_double_spend():
    """A4：并发 reserve 不超支；remaining 口径一致。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("A4", [{"task_id": "T1", "min_budget": 60},
                                 {"task_id": "T2", "min_budget": 60}])
    h.plans["A4"] = plan

    t1 = admit(h, "A4", plan, "T1")
    assert t1.state == TaskState.ADMITTED
    t2 = admit(h, "A4", plan, "T2")
    assert t2.state == TaskState.INADMISSIBLE       # 剩余 40 < 60

    assert h.budget.root.remaining == 40
    assert h.budget.root.own_reserved + h.budget.root.children_reserved == 60


# ---------------------------------------------------------------- A5
def test_a5_deep_nested_cancellation_broadcast():
    """A5：**真嵌套** A→B→C→D → 取消根 → RunCoordinator 广播，所有后代租约失效。

    C10：父子关系必须同时写 `parent_id` / `children_ids`（旧夹具只有 `children`
    + 同一 run_id → 取消时三者都是 roots，`_walk_children` 从未被调用，级联断言
    恒真）。
    """
    h = make_harness()
    plan = h.planner.plan("A5", [{"task_id": "A"}, {"task_id": "B"},
                                 {"task_id": "C"}, {"task_id": "D"}])
    h.plans["A5"] = plan

    # 构造嵌套：A.children=[B], B.children=[C], C.children=[D]（三源一致）
    acbs = {}
    parent = None
    for tid in ["A", "B", "C", "D"]:
        acb = admit(h, "A5", plan, tid)
        if parent is not None:
            parent.children.append(acb)
            parent.children_ids.append((acb.id, None))
            acb.parent = parent
            acb.parent_id = parent.id
        acbs[tid] = acb
        parent = acb
        h.run_coordinator.register(acb)

    # 每个后代都持有一个有效租约
    for tid, acb in acbs.items():
        h.intents.reserve("res-" + tid, acb.id, 0, 60000)
    assert all(any(l.agent_id == a.id and l.valid for l in h.intents._leases.values())
               for a in acbs.values())

    h.run_coordinator.cancel_run("A5")

    for tid, acb in acbs.items():
        assert h.run_coordinator.is_cancelled(acb) is True
    # 所有后代的租约都已失效（非空真断言）
    stale = [l for l in h.intents._leases.values() if l.agent_id in {a.id for a in acbs.values()}]
    assert len(stale) == 4
    assert all(not l.valid for l in stale)
    # C10：级联确实走过父链（否则同 run 的 ACB 会被当作 roots 直接取消）
    assert all(a.state is TaskState.CANCELLED for a in acbs.values())


def test_a5_persisted_tree_cancelled_after_memory_loss(tmp_path):
    """C10：A5 树 + 持久化（parent_id/children_ids）+ 清空内存 → 仍全撤。"""
    h = Harness(run_limits=1000, checkpoint_root=str(tmp_path))
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    plan = h.planner.plan("A5P", [{"task_id": "A"}, {"task_id": "B"},
                                  {"task_id": "C"}, {"task_id": "D"}])
    h.plans["A5P"] = plan

    order = ["A", "B", "C", "D"]
    acbs = {}
    parent = None
    for tid in order:
        acb = admit(h, "A5P", plan, tid)
        if parent is not None:
            parent.children.append(acb)
            acb.parent_id = parent.id
        acbs[tid] = acb
        parent = acb
        h.run_coordinator.register(acb)
    # 先存子，再回填父的 (child_id, ref) 并二次落盘（与 spawn 两阶段一致）
    refs = {tid: h.checkpoints.save(acb) for tid, acb in acbs.items()}
    for tid, acb in acbs.items():
        acb.children_ids = [(c.id, refs[c.task_id])
                            for c in acb.children]
        h.checkpoints.save(acb)
    for tid, acb in acbs.items():
        h.intents.reserve("p-" + tid, acb.id, 0, 60_000)

    h.acbs.clear()          # 进程内已无该 run

    h.run_coordinator.cancel_run("A5P")

    for tid, acb in acbs.items():
        ref = h.checkpoints.latest_ref_for_acb(acb.id)
        assert ref is not None
        assert h.checkpoints.load_strict(ref)["state"] == "CANCELLED"
    stale = [l for l in h.intents._leases.values()
             if l.agent_id in {a.id for a in acbs.values()}]
    assert len(stale) == 4 and all(not l.valid for l in stale)


# ---------------------------------------------------------------- A6
def test_a6_replan_storm_idempotent():
    """A6：verifier 抖动 → 重规划 → idem_key 复用 → 不重复副作用。"""
    h = make_harness()
    plan = h.planner.plan("A6", [{"task_id": "T1", "logical_subgoal_id": "S1"}])
    h.plans["A6"] = plan

    acb = admit(h, "A6", plan, "T1")
    decisions = [decision(1, tool("deploy", {"svc": "x"}), finish())]
    h.run_task(acb, decisions)

    key = h.idem_key(acb, decisions[0].actions[0], 0)
    assert h.effects.state(key) == "CONFIRMED"

    # 重规划：remake 任务复用 logical_subgoal_id
    diagnosis = {"failing_subgoals": ["S1"], "missing_deps": []}
    new_plan, _ = h.planner.patch(plan, diagnosis)
    h.plans["A6"] = new_plan
    assert new_plan["tasks"]["T1'"]["logical_subgoal_id"] == "S1"

    # 旧任务 FAILED 不波及新 Plan 版本的 remake
    h.propagate_failure("T1", plan_version=1)
    assert new_plan["tasks"]["T1'"]["state"] == TaskState.PLANNED

    # 重跑同一子目标 → idem_key 相同 → EffectJournal 命中 → 跳过
    acb2 = admit(h, "A6", new_plan, "T1'")
    h.run_task(acb2, decisions)
    deploy_events = [e for e in h.event_log.all() if e.kind.value == "TOOL_COMPLETED"]
    assert len(deploy_events) == 1          # 只发生一次


# ---------------------------------------------------------------- A7
def test_a7_event_storm_cursor_snapshot_replay():
    """A7：事件风暴 → cursor 落后 → 重放（含订阅过滤），事件不丢。"""
    h = make_harness()
    seed(h, "r", 0)
    plan = h.planner.plan("A7", [{"task_id": "T1"}])
    h.plans["A7"] = plan
    acb = admit(h, "A7", plan, "T1")
    acb.subscriptions.add("r")                 # 订阅 r → 事件会应用到视图

    acb.cursor = h.event_log.tail()
    baseline = len(h.event_log.all())
    for i in range(5000):
        h.event_log.append("RESOURCE_COMMITTED", "r", {"content": i, "version": i})

    assert acb.cursor < h.event_log.tail()     # cursor 落后
    view = h.context.context_view(acb)
    assert acb.cursor == h.event_log.tail()    # 重放后推进
    assert view["r"] == 4999                   # 最后一条事件已应用到视图
    assert len(h.event_log.all()) == baseline + 5000    # 事件不丢


def test_a7_backpressure_degrades_to_snapshot():
    """A7b：订阅者在途超过 max_inflight → 触发背压（降级为快照重放）。"""
    h = make_harness()
    h.subscriptions._max_inflight = 3
    seed(h, "r", 0)
    plan = h.planner.plan("A7B", [{"task_id": "T1"}])
    h.plans["A7B"] = plan
    acb = admit(h, "A7B", plan, "T1")
    h.subscriptions.add(acb.id, "r")          # 读即订阅

    for _ in range(5):
        h.event_log.append("RESOURCE_COMMITTED", "r", {"content": 1})
    assert h.subscriptions.inflight(acb.id) == 5
    assert h.subscriptions.is_backpressured(acb.id) is True   # 5 >= 3

    h.subscriptions.ack(acb.id)
    h.subscriptions.ack(acb.id)
    assert h.subscriptions.inflight(acb.id) == 3
    assert h.subscriptions.is_backpressured(acb.id) is True
    h.subscriptions.ack(acb.id)
    assert h.subscriptions.is_backpressured(acb.id) is False


# ---------------------------------------------------------------- A8
def test_a8_hitl_suspend_crash_before_decision_stays_waiting(monkeypatch):
    """A8①：挂起 → 崩溃（决策前）→ recover 仍 WAITING，预算未结算。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    plan = h.planner.plan("A8", [{"task_id": "T1", "min_budget": 30}])
    h.plans["A8"] = plan
    acb = admit(h, "A8", plan, "T1")

    def crashy(acb_):
        raise CrashInjected(1, 0)          # save 之后、决策之前

    monkeypatch.setattr(h.hitl, "resolve", crashy)
    with pytest.raises(CrashInjected):
        h.run_task(acb, [decision(1, stage("r", "x"), hitl())])

    # 挂起前 staged 被丢弃、锁被释放；审批尚未结算
    assert acb.state is TaskState.WAITING_APPROVAL
    assert acb.staged == {}
    assert not [lease for lease in h.intents._leases.values() if lease.valid]
    assert not [v for v in h.intents._active.values() if v]
    approval_id = acb.hitl_pending["approval_id"]
    ref = acb.result_ref

    recovered = h.recover(ref)
    assert recovered.state is TaskState.WAITING_APPROVAL
    assert recovered.hitl_pending["approval_id"] == approval_id
    assert recovered.hitl_outcome is None
    # 预算未被误结算：预留仍 ACTIVE、subtree_reserved 原样
    assert recovered.reservation_ids != []
    assert recovered.budget_node is not None
    assert h.budget.root.subtree_reserved == 30
    h.budget.check_invariants()


def test_a8_hitl_approved_crash_then_recover_and_finish():
    """A8②：批准后崩溃 → recover RUNNING → 继续执行 DONE，预算正确结算。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    plan = h.planner.plan("A8", [{"task_id": "T1", "min_budget": 30}])
    h.plans["A8"] = plan
    acb = admit(h, "A8", plan, "T1")

    assert h.run_task(acb, [decision(1, stage("r", "x"), hitl())]) \
        == Status.NEEDS_APPROVAL
    assert acb.state is TaskState.WAITING_APPROVAL
    h.hitl.set_result(acb.run_id, acb.hitl_pending["approval_id"],
                      {"approved": True})

    # 批准后执行第 2 轮，在 `finish` 之前崩溃（不产生 UNKNOWN 副作用）
    with pytest.raises(CrashInjected):
        h.run_task(acb, [decision(2, finish())], crash_at=(2, 0))
    assert acb.state is TaskState.RUNNING          # 已过审批，非挂起

    recovered = h.recover(acb.result_ref)
    assert recovered.state in (TaskState.RUNNING, TaskState.DONE)
    assert recovered.reservation_ids != []
    assert recovered.budget_node is not None
    assert h.budget.root.subtree_reserved == 30

    # 恢复后继续执行 → DONE + 预算结算干净
    st = h.run_task(recovered, [decision(3, finish())])
    assert st == Status.OK
    assert recovered.state is TaskState.DONE
    assert recovered.hitl_pending is None
    assert h.budget.root.subtree_reserved == 0
    h.budget.check_invariants()


# ---------------------------------------------------------------- A9
def test_a9_lease_renew_revoke_race():
    """A9：revoke 后 renew 不可复活；旧租约提交被拒。"""
    h = make_harness()
    seed(h, "r", "base")
    lease = h.intents.reserve("r", "A", 0, 60000)

    h.supervisor_note_crash(type("ACB", (), {"id": "A"})())
    assert h.intents.is_valid(lease) is False
    assert h.intents.renew(lease) is False       # revoke 后不可复活

    acb = type("ACB", (), {"base_version": {"r": 0}, "read_set": {"r": 0},
                           "id": "A", "staged": {"r": "x"}})()
    st = h.workspace.prepare(acb, Write("r", content="x"), lease, Mode.COMMIT)
    assert st == Status.REJECT_STALE


# ---------------------------------------------------------------- A10
def test_a10_write_skew_logical_deps():
    """A10：逻辑不变量 write skew —— 声明完整时准入拦截（残留：漏声明靠事后）。"""
    h = make_harness()
    seed(h, "file1", {})
    seed(h, "file2", {})

    plan = h.planner.plan("A10", [
        {"task_id": "T1", "committed": ["file1"], "logical_deps": [{"kind": "CONTRACT", "id": "c1"}]},
        {"task_id": "T2", "committed": ["file2"], "logical_deps": [{"kind": "CONTRACT", "id": "c1"}]},
    ])
    h.plans["A10"] = plan

    t1 = admit(h, "A10", plan, "T1")
    assert t1.state == TaskState.ADMITTED          # 第一个声明者 → 准入

    # 第二个 agent 声明**同一契约** → fail-fast（write skew 拦截）
    t2 = admit(h, "A10", plan, "T2")
    assert t2.state == TaskState.INADMISSIBLE
    assert t2.failure == "CONTRACT_CONFLICT"

    # 同资源**写-写不拦**（放行至提交层 CAS 裁决）
    ra = h.intents.reserve("shared-res", t1.id, 0, 60000)
    rb = h.intents.reserve("shared-res", t2.id, 0, 60000)
    assert ra.valid and rb.valid                # 两个租约都成立


# ---------------------------------------------------------------- A11
def test_a11_finalize_not_triggered_with_nonterminal():
    """A11：存在非终态任务时不 finalize —— 断言 Run 的可观测状态（事件/账本/槽位）。"""
    h = make_harness()
    plan = h.planner.plan("A11", [{"task_id": "T1", "min_budget": 10},
                                  {"task_id": "T2", "min_budget": 10}])
    h.plans["A11"] = plan

    t1 = admit(h, "A11", plan, "T1")
    t2 = admit(h, "A11", plan, "T2")
    h.run_task(t1, [decision(1, finish())])
    assert t1.state is TaskState.DONE

    assert plan["tasks"]["T2"]["state"] not in TASK_TERMINAL
    assert h.event_log.has("T2", EventKind.TASK_COMPLETED) is False
    assert t2.budget_node is not None
    assert t2.budget_node.node_id in h.budget._nodes
    assert t2.id in h.slots.active_keys()


# ---------------------------------------------------------------- A12
def test_a12_concurrent_reserve_no_overcommit():
    """A12：**真并发** reserve 单写者 → 不超支。"""
    import threading

    h = make_harness(run_limits=10)
    results: list[bool] = []
    lock = threading.Lock()

    def try_reserve(amount):
        ok = h.budget.reserve("run", amount)
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=try_reserve, args=(6,)) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(1 for r in results if r) == 1        # 只有一个成功
    assert h.budget.root.own_reserved == 6         # 不超支
    assert h.budget.root.own_reserved <= h.budget.root.limits


# ---------------------------------------------------------------- A13
def test_a13_per_action_guard_budget():
    """A13：单 turn 多动作 → per-action guard → 预算击穿前 FAILED。"""
    h = make_harness(run_limits=10)
    plan = h.planner.plan("A13", [{"task_id": "T1", "min_budget": 3}])
    h.plans["A13"] = plan
    acb = admit(h, "A13", plan, "T1")

    decisions = [decision(1, *[tool(f"t{i}", cost=2) for i in range(5)])]
    h.run_task(acb, decisions)
    assert acb.state == TaskState.FAILED
    assert acb.failure == "BUDGET_EXHAUSTED"
