"""MS2 出口条件：预算守恒 / 幂等释放 / 准入零残留 / 能力收口。

对应 `docs/s4-budget-and-admission.md` v1.1 §8：
- T23：预算守恒 + 幂等释放（累计账 + 子树守恒）
- T24：并发原子性（正例 + 阴性对照 + yield point 注入）
- T9 ：准入失败零残留（四级：slots / 能力 / 预算 / 逻辑）
- T10：`run_task` 走 dispatcher
- T14：能力收口（生产路径不能自证身份）
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest
from conftest import admit, commit, decision, finish, make_harness, seed, tool

from harness import (
    ACB,
    BudgetManager,
    CapabilityToken,
    Harness,
    Reservation,
    Status,
    TaskState,
)


# ================================================================ T23
def test_ms1_budget_conservation_and_idempotent_release():
    """T23：consumed 永久账；release 幂等；子树守恒；非法金额拒绝。"""
    # ① consumed 永久账
    h = make_harness(run_limits=100)
    node = h.budget.create_node("B1", 100)
    r1 = h.budget.reserve("B1", 60)
    assert r1 is not None and r1.amount == 60 and r1.balance == 60
    assert h.budget.root.remaining == 40
    assert h.budget.consume(r1.reservation_id, 40) == Status.OK
    assert h.budget.release(r1.reservation_id) == 20        # 只还未消耗
    assert h.budget.root.remaining == 60                    # 已花 40 不退
    assert node.consumed == 40
    h.budget.check_invariants()

    # ①′ 边界：释放后还能再预留 40，41 必须拒绝
    assert h.budget.reserve("B1", 40) is not None
    assert h.budget.reserve("B1", 41) is None
    h.budget.check_invariants()

    # ② release 幂等
    assert h.budget.release(r1.reservation_id) == 0
    assert h.budget.release("nonexistent") == 0

    # ③ 子树守恒（证伪 P0-2：子预留必须穿透到祖先）
    h2 = make_harness(run_limits=100)
    h2.budget.create_node("P", 100)
    h2.budget.create_node("C", 100, parent_id="P")
    assert h2.budget.reserve("C", 90) is not None
    assert h2.budget.root.remaining == 10                   # v1 是 100
    assert h2.budget.reserve("run", 50) is None
    h2.budget.check_invariants()

    # ④ 自身 + 子（证伪 P0-3：节点自身不得越过 limits）
    h3 = make_harness(run_limits=200)
    h3.budget.create_node("P", 100)
    h3.budget.create_node("C", 100, parent_id="P")
    assert h3.budget.reserve("C", 80) is not None
    assert h3.budget.reserve("P", 50) is None               # 50 + 80 > 100
    assert h3.budget.reserve("P", 20) is not None           # 20 + 80 = 100
    h3.budget.check_invariants()

    # ⑤ 负 / 零 / 非整数（P 节点存在，失败只因金额非法而非节点缺失）
    h.budget.create_node("P", 100)
    assert h.budget.reserve("P", -1) is None
    assert h.budget.reserve("P", 0) is None
    assert h.budget.reserve("P", True) is None
    h.budget.check_invariants()

    # ⑥ 负消耗不得反向记账（独立节点，不复用 h3 的满额 P 子树）
    h4 = make_harness(run_limits=100)
    h4.budget.create_node("N", 100)
    r = h4.budget.reserve("N", 10)
    assert h4.budget.consume(r.reservation_id, -5) == Status.INVALID_INPUT   # bad_amount
    assert h4.budget.consume(r.reservation_id, 0) == Status.INVALID_INPUT
    assert h4.budget.consume(r.reservation_id, True) == Status.INVALID_INPUT
    assert h4.budget.consume(r.reservation_id, 100) == Status.BUDGET_EXHAUSTED
    assert h4.budget.consume("unknown-rid", 1) == Status.INVALID_INPUT       # 与耗尽区分
    assert r.consumed == 0                                                  # 未反向记账
    h4.budget.check_invariants()


# ================================================================ T24
L, A, N = 10, 6, 5


def _race(reserve_fn) -> list:
    """N 个线程在同一起跑线并发调用 reserve_fn。"""
    results = [None] * N
    errors: list = []
    barrier = threading.Barrier(N)

    def worker(i):
        try:
            barrier.wait(timeout=5)
            results[i] = reserve_fn(i)
        except BaseException as e:               # noqa: BLE001 - 线程异常须回收
            errors.append(e)

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=10)
    assert all(not t.is_alive() for t in ts), "线程未结束 → 疑似死锁"
    assert not errors, f"线程异常: {errors}"
    return results


def _assert_conserved(h, results) -> None:
    """正例与阴性对照**共用**的判据（不得写成恒真的 `assert False`）。"""
    ok = [r for r in results if r is not None]
    assert all(r.amount == A and r.balance == A for r in ok)         # 返回对象自洽
    assert len({r.reservation_id for r in ok}) == len(ok)            # id 唯一
    assert len(ok) == L // A, f"成功数应为 {L // A}，实际 {len(ok)}"
    assert h.budget.root.subtree_reserved == A * len(ok)
    assert h.budget.root.remaining == L - A * len(ok)
    assert h.budget.root.remaining < A
    h.budget.check_invariants()


@pytest.mark.serial
def test_ms1_concurrent_reserve_no_overcommit():
    """T24 ①：真实实现的并发 reserve 不超支。"""
    h = make_harness(run_limits=L)
    _assert_conserved(h, _race(lambda i: h.budget.reserve("run", A)))


@pytest.mark.serial
def test_ms1_concurrent_reserve_negative_control(monkeypatch):
    """T24 ②：非原子实现必须被**同一判据**抓住（先证伪坏实现，再证伪判据）。"""
    def broken(self, node_id, amount):                   # 非原子 check-then-act
        node = self._nodes.get(node_id)
        if node is None or node.subtree_reserved + amount > node.limits:
            return None
        time.sleep(1e-4)                                 # 放大竞态窗口
        node.subtree_reserved += amount
        node.self_reserved += amount
        return Reservation(uuid.uuid4().hex, node_id, amount)

    monkeypatch.setattr(BudgetManager, "reserve", broken)
    assert BudgetManager.reserve is broken               # ③ 证明补丁生效

    h = make_harness(run_limits=L)
    res = _race(lambda i: h.budget.reserve("run", A))
    ok = [r for r in res if r is not None]
    assert len(ok) > L // A, "坏实现应超支（否则注入无效）"   # ④ 先证明坏实现真的坏了
    with pytest.raises(AssertionError):
        _assert_conserved(h, res)                        # ⑤ 再证明判据能抓住


@pytest.mark.serial
def test_ms1_concurrent_reserve_with_yield_point(monkeypatch):
    """T24 ③：真实实现 + 检查后切线程，仍须守恒（锁覆盖 check + deduct）。"""
    orig = BudgetManager._chain
    calls = {"n": 0}

    def yielding_chain(self, node):
        calls["n"] += 1
        time.sleep(0)                                    # 让出 GIL（检查与扣减之间）
        return orig(self, node)

    monkeypatch.setattr(BudgetManager, "_chain", yielding_chain)
    h = make_harness(run_limits=L)
    _assert_conserved(h, _race(lambda i: h.budget.reserve("run", A)))
    assert calls["n"] >= N                               # 证明注入点被走到


# ================================================================ T9
def _snap(h) -> dict:
    """四级准入失败前后的全字段快照（含 counter/reservations/slots/events）。"""
    return {
        "nodes": {nid: (n.self_reserved, n.subtree_reserved, n.consumed, id(n))
                  for nid, n in h.budget._nodes.items()},
        "reservations": {rid: (r.state, r.consumed)
                         for rid, (_, r) in h.budget._reservations.items()},
        "counter": getattr(h.budget, "_counter", None),
        "root_children": [(c.node_id, id(c)) for c in h.budget.root.children],
        "root_subtree": h.budget.root.subtree_reserved,
        "valid_leases": {lid for lid, lease in h.intents._leases.items() if lease.valid},
        "active_intents": {r: len(v) for r, v in h.intents._active.items()},
        "logical": {dep: dict(holders) for dep, holders in h.intents._logical.items()},
        "acbs": set(h.acbs),
        "slots": h.slots.active,
        "events": len(h.event_log.all()),
    }


@pytest.mark.ms2
def test_ms2_admit_rollback_no_residue():
    """T9：slots / 能力 / 预算 / 逻辑四级失败均零残留（含节点身份）。"""
    # ① 预算失败
    h = make_harness(run_limits=100)
    plan = h.planner.plan("R", [{"task_id": "T1", "min_budget": 60},
                                {"task_id": "T2", "min_budget": 60}])
    acb1 = h.dispatcher.try_admit(plan["tasks"]["T1"], "R")
    assert acb1.state is TaskState.ADMITTED
    mid = _snap(h)
    acb2 = h.dispatcher.try_admit(plan["tasks"]["T2"], "R")
    assert acb2.state is TaskState.INADMISSIBLE
    assert acb2.failure == "BUDGET_EXHAUSTED"
    assert _snap(h) == mid, "失败准入留下残留"
    assert acb2.reservation_ids == [] and acb2.budget_node is None

    # ② 逻辑冲突（先声明不冲突的 c2，再声明冲突的 c1 → 两个都回滚）
    h2 = make_harness()
    plan2 = h2.planner.plan("R2", [
        {"task_id": "T1", "min_budget": 10,
         "logical_deps": [{"kind": "CONTRACT", "id": "c1"}]},
        {"task_id": "T2", "min_budget": 10,
         "logical_deps": [{"kind": "CONTRACT", "id": "c2"},
                          {"kind": "CONTRACT", "id": "c1"}]},
    ])
    a1 = h2.dispatcher.try_admit(plan2["tasks"]["T1"], "R2")
    assert a1.state is TaskState.ADMITTED
    before2 = _snap(h2)
    a2 = h2.dispatcher.try_admit(plan2["tasks"]["T2"], "R2")
    assert a2.state is TaskState.INADMISSIBLE
    assert a2.failure == "CONTRACT_CONFLICT"
    assert _snap(h2) == before2, "逻辑冲突失败准入留下残留"
    assert a2.reservation_ids == [] and a2.budget_node is None
    assert a2.id not in h2.intents._logical.get("c2", {})   # 已声明的 c2 被撤销

    # ③ 能力失败（deny-by-default）
    h3 = Harness(run_limits=100)
    plan3 = h3.planner.plan("R3", [{"task_id": "T1", "min_budget": 10}])
    before3 = _snap(h3)
    a3 = h3.dispatcher.try_admit(plan3["tasks"]["T1"], "R3", required_tools={"read"})
    assert a3.state is TaskState.INADMISSIBLE
    assert a3.failure == "DENIED_CAPABILITY"
    assert _snap(h3) == before3, "能力失败准入留下残留"

    # ④ NO_SLOT（先占满 slots）
    h4 = Harness(run_limits=100, max_concurrent=1)
    plan4 = h4.planner.plan("R4", [{"task_id": "T1", "min_budget": 10},
                                   {"task_id": "T2", "min_budget": 10}])
    a4 = h4.dispatcher.try_admit(plan4["tasks"]["T1"], "R4")
    assert a4.state is TaskState.ADMITTED
    assert h4.slots.active == 1
    before4 = _snap(h4)
    a5 = h4.dispatcher.try_admit(plan4["tasks"]["T2"], "R4")
    assert a5.state is TaskState.INADMISSIBLE
    assert a5.failure == "NO_SLOT"
    assert _snap(h4) == before4, "槽位失败准入留下残留"
    assert h4.slots.active == 1


# ================================================================ T10
def _run_task_spec(h, plan, task_id: str, decisions) -> ACB:
    """新入口：task spec → dispatcher → run（与生产路径同构）。"""
    acb = h.dispatcher.try_admit(plan["tasks"][task_id], plan["run_id"])
    if acb.state is not TaskState.ADMITTED:
        return acb
    h.plans[plan["run_id"]] = plan
    h.acbs[acb.id] = acb
    h.run_coordinator.register(acb)
    h.run_task(acb, decisions)
    return acb


@pytest.mark.ms2
def test_ms2_run_task_goes_through_dispatcher():
    """T10：`run_task` 必走 dispatcher；结束后预算结算、槽位释放。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("T10", [{"task_id": "T1", "min_budget": 20}])
    h.plans["T10"] = plan

    calls = []
    orig = h.dispatcher.try_admit

    def spy(*args, **kwargs):
        calls.append(1)
        return orig(*args, **kwargs)

    h.dispatcher.try_admit = spy
    acb = _run_task_spec(h, plan, "T1", [decision(1, finish())])

    assert len(calls) == 1                                            # ① 走 dispatcher
    assert acb.reservation_ids != []                                  # ② 拿到预留
    assert acb.state is TaskState.DONE
    # ③ 同步结束后预算已结算：finish 不消耗 → 全额归还
    assert all(h.budget._reservations[rid][1].state == "SETTLED"
               for rid in acb.reservation_ids)
    assert h.budget.root.remaining == h.budget.root.limits
    assert h.budget.root.subtree_reserved == 0
    assert h.slots.active == 0                                        # ④ 槽位已释放


# ================================================================ T14
def test_i4_capability_production_no_self_issue():
    """T14：能力收口——新签名可用、旧路径默认拒绝、deny-by-default、准入同一判定。"""
    # ① 新签名可用
    h = Harness()
    token = h.policy.issue("A1", tools={"read"}, paths={"/src"})
    assert isinstance(token, CapabilityToken)
    assert h.policy.check_capability("A1", "read", "/src/a.ts") is True
    assert h.policy.check_capability("A1", "write", "/src/a.ts") is False

    # ② 生产路径不能自证身份（默认 allow_legacy=False）
    with pytest.raises(ValueError):
        h.policy.issue(CapabilityToken("A1", tools={"*"}))

    # ③ deny-by-default
    assert h.policy.check_capability("A-unknown", "read", "/x") is False

    # ④ 准入与运行时同一判定：required_tools ⊄ token → DENIED_CAPABILITY
    plan = h.planner.plan("T14", [{"task_id": "T1", "min_budget": 10}])
    h.plans["T14"] = plan
    # acb.id 由 dispatcher 生成：A-{run}-{task}-{attempt}，首次 attempt=1
    h.policy.issue("A-T14-T1-1", tools={"read"}, paths={"*"})
    denied = h.dispatcher.try_admit(plan["tasks"]["T1"], "T14",
                                    required_tools={"deploy"})
    assert denied.state is TaskState.INADMISSIBLE
    assert denied.failure == "DENIED_CAPABILITY"
    assert denied.reservation_ids == [] and denied.budget_node is None
    assert h.slots.active == 0


# ================================================================ 清单回归
@pytest.mark.ms2
def test_ms2_tool_budget_consumed_before_side_effects():
    """P0-1：预算消费先于副作用；消费失败不执行工具、不写事件。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("C1", [{"task_id": "T1", "min_budget": 2}])
    act = tool("deploy", {"x": 1}, cost=5)                 # 余额 2 < 5
    acb = h.dispatch_and_run(plan, "T1", [decision(1, act)])

    assert acb.state is TaskState.FAILED
    assert acb.failure == "BUDGET_EXHAUSTED"
    assert h.effects.state(h.idem_key(acb, act, 0)) is None
    assert [e for e in h.event_log.all() if e.kind.value == "TOOL_COMPLETED"] == []


@pytest.mark.ms2
def test_ms2_run_preserves_quarantined_state_and_failure():
    """P0-2：已隔离 / rebase 隔离后，run 不得改状态或覆盖 failure。"""
    # ① 运行前已隔离
    h = make_harness()
    acb = ACB(id="A-Q1", run_id="Q", task_id="T1")
    acb.state = TaskState.QUARANTINED
    acb.failure = "manual_quarantine"
    assert h.runtime.run(acb, [decision(1, finish())]) == Status.SEMANTIC_REJECTED
    assert acb.state is TaskState.QUARANTINED
    assert acb.failure == "manual_quarantine"

    # ② handle_commit_result 隔离：failure 保留隔离原因（非 SEMANTIC_REJECTED）
    h2 = make_harness()
    v0 = seed(h2, "r", {"a": 0})
    plan = h2.planner.plan("Q2", [{"task_id": "T1", "min_budget": 10}])
    h2.plans["Q2"] = plan
    target = admit(h2, "Q2", plan, "T1")
    target.staged["r"] = {"a": 2}
    target.base_version["r"] = v0
    target.read_set["r"] = v0
    h2.workspace.put("r", {"a": 1})                        # 竞争者推进版本
    h2.run_task(target, [decision(1, commit("r"))])

    assert target.state is TaskState.QUARANTINED
    assert target.failure == "no_pre_image"   # 未被 "SEMANTIC_REJECTED" 覆盖


@pytest.mark.ms2
def test_ms2_checkpoint_slots_and_recover_readmit():
    """P0-3/P0-4：checkpoint 记录 slots_active；无预算信息 → needs_readmit 且零残留。"""
    h = Harness(run_limits=100, max_concurrent=1)
    acb = ACB(id="A-R1", run_id="R", task_id="T1")
    h.slots.try_acquire(acb.id)
    ref = h.checkpoints.save(acb)
    assert h.checkpoints.load(ref)["slots_active"] == [acb.id]

    # 恢复时槽位已满 → QUARANTINED
    h.slots.release(acb.id)
    h.slots.try_acquire("other")
    recovered = h.recover(ref)
    assert recovered.state is TaskState.QUARANTINED
    assert recovered.failure == "NO_SLOT"

    # 无 budget_node_id：不建节点、不改 budget，标记 needs_readmit
    h2 = Harness(run_limits=100)
    ref2 = h2.checkpoints.save(ACB(id="A-R2", run_id="R", task_id="T2"))
    rec2 = h2.recover(ref2)
    assert rec2.needs_readmit is True
    assert rec2.reservation_ids == [] and rec2.budget_node is None
    assert "B-T2" not in h2.budget._nodes
    assert h2.budget.root.subtree_reserved == 0


@pytest.mark.ms2
def test_ms2_restore_budget_children_never_overwritten():
    """P0-5：有 children 的节点子树重算（不用快照覆盖）；冲突 rid 跳过。"""
    h = make_harness(run_limits=100)
    parent = h.budget.create_node("P", 100)
    h.budget.reserve("P", 30)
    h.budget.create_node("C", 50, parent_id="P")
    h.budget.reserve("C", 20)
    assert parent.subtree_reserved == 50

    other = h.budget.create_node("OTHER", 100)
    conflict = h.budget.reserve("OTHER", 30)
    data = {
        "budget_node_id": "P", "budget_parent_id": "run", "budget_limits": 100,
        "budget_self": 30, "budget_subtree": 90, "budget_consumed": 10,
        "reservation_ids": [next(iter(parent.reservations)),
                            conflict.reservation_id],
    }
    acb = ACB(id="A-RB", run_id="R", task_id="T1")
    h._restore_budget(acb, data)

    assert parent.subtree_reserved == 50                   # 30 + 20，不是快照 90
    assert conflict.reservation_id not in acb.reservation_ids
    assert h.budget._reservations[conflict.reservation_id][0] is other
    h.budget.check_invariants()


@pytest.mark.ms2
def test_ms2_restore_budget_uses_limits_and_parent():
    """高-6：用 checkpoint 的 budget_limits/parent 重建，不用 max(...) 当上限。"""
    h = make_harness(run_limits=100)
    data = {
        "budget_node_id": "B-X", "budget_parent_id": "run", "budget_limits": 10,
        "budget_self": 6, "budget_subtree": 6, "budget_consumed": 2,
        "reservation_ids": ["rid-x"],
    }
    acb = ACB(id="A-RX", run_id="R", task_id="T1")
    h._restore_budget(acb, data)

    node = h.budget._nodes["B-X"]
    assert node.limits == 10                               # 不是 max(6,6,2,1)
    assert node.parent is h.budget.root
    assert acb.reservation_ids == ["rid-x"]
    h.budget.check_invariants()


@pytest.mark.ms2
def test_ms2_capability_path_and_guard_uses_active_reservation():
    """高-7/8：准入逐个含 path 判定；guard 只认 ACB 名下 ACTIVE 预留。"""
    h = Harness(run_limits=100)
    plan = h.planner.plan("CAP", [{"task_id": "T1", "min_budget": 10},
                                  {"task_id": "T2", "min_budget": 10}])
    h.policy.issue("A-CAP-T1-1", tools={"read"}, paths={"/src"})
    ok = h.dispatcher.try_admit(plan["tasks"]["T1"], "CAP",
                                required_tools={"read": "/src/a.py"})
    assert ok.state is TaskState.ADMITTED
    h.policy.issue("A-CAP-T2-1", tools={"read"}, paths={"/src"})
    bad = h.dispatcher.try_admit(plan["tasks"]["T2"], "CAP",
                                 required_tools={"read": "/etc/passwd"})
    assert bad.state is TaskState.INADMISSIBLE
    assert bad.failure == "DENIED_CAPABILITY"

    node = h.budget.create_node("N", 10)
    resv = h.budget.reserve("N", 5)
    acb = ACB(id="A-G", run_id="G", task_id="T")
    acb.budget_node = node
    acb.reservation_ids = [resv.reservation_id]
    assert h.runtime.guard(acb) is True
    h.budget.settle(resv.reservation_id)
    assert h.runtime.guard(acb) is False
    assert acb.failure == "BUDGET_EXHAUSTED"


@pytest.mark.ms2
def test_ms2_admit_limits_margin_and_dispatch_and_run():
    """高-9/10：limits 留重试余量、只预留 min_budget；dispatch_and_run 为唯一入口。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("D", [{"task_id": "T1", "min_budget": 10}])
    acb = h.dispatch_and_run(plan, "T1", [decision(1, finish())])

    assert acb.state is TaskState.DONE
    assert acb.budget_node.limits == 10 + acb.retry_budget + 1
    rid = acb.reservation_ids[0]
    assert h.budget._reservations[rid][1].amount == 10     # 只预留 min_budget
    assert h.plans["D"] is plan and h.acbs[acb.id] is acb
    assert h.slots.active == 0

