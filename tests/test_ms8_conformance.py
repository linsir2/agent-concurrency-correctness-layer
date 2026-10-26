"""MS8 出口条件：conformance suite —— 不变量 I1–I8 的可执行校验。

对应设计文档 §11 不变量 与 §18.4 conformance suite。
"""

from __future__ import annotations

from conftest import admit, commit, decision, finish, make_harness, seed, stage, tool
from harness import Mode, Status, TaskState, Write


def test_i1_single_writer_commit():
    """I1：同一资源同一时刻只有一个写生效（提交层 CAS）。"""
    h = make_harness()
    v0 = seed(h, "r", "v0")
    plan = h.planner.plan("I1", [{"task_id": "T1", "min_budget": 10},
                                 {"task_id": "T2", "min_budget": 10}])
    h.plans["I1"] = plan
    a, b = admit(h, "I1", plan, "T1"), admit(h, "I1", plan, "T2")
    for acb in (a, b):
        acb.base_version["r"] = v0
        acb.read_set["r"] = v0
    a.staged["r"] = "A"
    b.staged["r"] = "B"

    assert h.commit_batch(a, [Write("r", content="A")]) == Status.OK
    assert h.commit_batch(b, [Write("r", content="B")]) == Status.CONFLICT
    assert h.workspace.get("r") == "A"


def test_i2_no_lost_update():
    """I2：覆盖写被拒并 rebase（不静默丢失）。"""
    h = make_harness()
    v0 = seed(h, "r", "v0")
    plan = h.planner.plan("I2", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I2"] = plan
    acb = admit(h, "I2", plan, "T1")
    acb.base_version["r"] = v0
    acb.read_set["r"] = v0
    acb.staged["r"] = "v1"
    assert h.commit_batch(acb, [Write("r", content="v1")]) == Status.OK

    acb.base_version["r"] = v0                 # 陈旧 base
    acb.read_set["r"] = v0
    acb.staged["r"] = "v2-stale"
    assert h.commit_batch(acb, [Write("r", content="v2-stale")]) == Status.CONFLICT
    assert h.workspace.get("r") == "v1"       # 未被覆盖


def test_i3_fencing_monotonic():
    """I3：revoke 后旧租约被拒。"""
    h = make_harness()
    seed(h, "r", "v0")
    lease = h.intents.reserve("r", "A", 0, 60000)
    assert h.intents.is_valid(lease) is True
    h.supervisor_note_crash(type("ACB", (), {"id": "A"})())
    assert h.intents.is_valid(lease) is False


def test_i5_budget_conservation():
    """I5：consumed ≤ self_reserved ≤ subtree_reserved ≤ limits；父约束不被子突破。"""
    h = make_harness(run_limits=100)
    plan = h.planner.plan("I5", [{"task_id": "T1", "min_budget": 60},
                                 {"task_id": "T2", "min_budget": 60}])
    h.plans["I5"] = plan
    t1 = admit(h, "I5", plan, "T1")
    t2 = admit(h, "I5", plan, "T2")
    assert t1.state == TaskState.ADMITTED
    assert t2.state == TaskState.INADMISSIBLE

    # 节点身份由 dispatcher 生成（含 run/attempt），不得按固定 'B-T1' 查找
    node = t1.budget_node
    assert node is not None and t1.reservation_ids != []
    assert h.budget._nodes[node.node_id] is node
    assert all(h.budget._reservations[rid][0] is node for rid in t1.reservation_ids)

    # 预留 / 消费边界
    assert node.self_reserved == 60
    assert node.subtree_reserved == 60
    assert node.consumed == 0
    assert node.consumed <= node.self_reserved <= node.subtree_reserved <= node.limits
    assert t2.budget_node is None and t2.reservation_ids == []

    h.budget.check_invariants()
    assert h.budget.root.subtree_reserved == 60
    assert h.budget.root.remaining == 40


def test_i7_effect_exactly_once():
    """I7：重复投递去重（幂等键）。"""
    h = make_harness()
    plan = h.planner.plan("I7", [{"task_id": "T1", "logical_subgoal_id": "S"}])
    h.plans["I7"] = plan
    acb = admit(h, "I7", plan, "T1")
    act = tool("deploy", {"x": 1})
    key = h.idem_key(acb, act, 0)

    h.effects.begin(acb, "deploy", {"x": 1}, key)
    h.effects.confirm(key, acb.id)
    h.effects.begin(acb, "deploy", {"x": 1}, key)      # 重复 begin 不应覆盖
    assert h.effects.state(key) == "CONFIRMED"


def test_i6_context_consistent_prefix():
    """I6：`materialized + ordered events` 是一个一致前缀（事件按 seq 有序）。"""
    h = make_harness()
    v0 = seed(h, "r", {"v": 0})
    plan = h.planner.plan("I6", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I6"] = plan
    acb = admit(h, "I6", plan, "T1")
    acb.subscriptions.add("r")
    h.subscriptions.add(acb.id, "r")

    for i in range(1, 6):
        acb.base_version["r"] = v0 + i - 1
        acb.read_set["r"] = v0 + i - 1
        acb.staged["r"] = {"v": i}
        assert h.commit_batch(acb, [Write("r", content={"v": i})]) == Status.OK

    view = h.context.context_view(acb)
    # 视图反映「已确认的最新版本」（一致前缀，非字节等价重放）
    assert view["r"] == {"v": 5}
    # 事件按 seq 有序
    seqs = [e.seq for e in h.event_log.replay(subject="r")]
    assert seqs == sorted(seqs)


def test_i8_event_atomic_with_replace():
    """I8：资源提交与 WorkspaceEvent 同事务（本实现：同一临界区）。"""
    h = make_harness()
    v0 = seed(h, "r", "v0")
    plan = h.planner.plan("I8", [{"task_id": "T1", "min_budget": 10}])
    h.plans["I8"] = plan
    acb = admit(h, "I8", plan, "T1")
    acb.base_version["r"] = v0
    acb.read_set["r"] = v0
    acb.staged["r"] = "v1"

    assert h.commit_batch(acb, [Write("r", content="v1")]) == Status.OK
    assert h.workspace.version("r") == v0 + 1
    assert h.event_log.has("r", __import__("harness").EventKind.RESOURCE_COMMITTED) is True
