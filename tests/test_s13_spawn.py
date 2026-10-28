"""S13c：spawn_child（能力子集 / 预算守恒 / 取消级联 / 有界 / 无双重预留）。

对应 `docs/s13-orchestration.md` v1.1 §2.4 与 §3 的 S13-7 / S13-8 / S13-9 /
S13-10 / S13-11（本文件另覆盖 `ResultEnvelope` 的 `DONE_PARTIAL` 产生路径）。

夹具陷阱：能力子集用例必须用**裸 `Harness(auth_secret=...)` + `mint`**，
不能用 `make_harness`（`default_token="*"` 会全放行，抹掉 attenuation）。
"""

from __future__ import annotations

import threading

import pytest

from conftest import decision, finish, make_harness, tool

from harness import ACB, CapabilityToken, Harness, Scope, Status, TaskState
from harness.policy import CapabilityAttenuationError, attenuate_token


# ---------------------------------------------------------------- helpers
def _bare_with_token(**kwargs) -> Harness:
    """裸 Harness + 通配 token（仅预算/取消用例；能力用例不得使用）。"""
    h = Harness(**kwargs)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h


def _parent_plan(h: Harness, run_id: str, *, min_budget: int = 6,
                 child_alloc: int = 6) -> tuple[dict, ACB]:
    plan = h.planner.plan(run_id, [{
        "task_id": "P", "logical_subgoal_id": "SP",
        "min_budget": min_budget, "child_alloc": child_alloc}])
    h.plans[run_id] = plan
    parent = h.dispatcher.try_admit(plan["tasks"]["P"], run_id, plan=plan)
    h.acbs[parent.id] = parent
    h.run_coordinator.register(parent)
    assert parent.state is TaskState.ADMITTED
    return plan, parent


def _child_ids(acb: ACB) -> list[str]:
    return [str(entry[0]) for entry in acb.children_ids]


def _reservation_state(h: Harness, rid: str) -> str:
    return h.budget._reservations[rid][1].state


def _snapshot(h: Harness) -> dict:
    return {
        "nodes": set(h.budget._nodes),
        "reservations": set(h.budget._reservations),
        "slots": h.slots.active,
        "events": len(h.event_log.all()),
    }


def _tool_completed(h: Harness, name: str) -> list:
    return [e for e in h.event_log.all()
            if e.kind.value == "TOOL_COMPLETED"
            and (e.payload or {}).get("tool") == name]


# =========================================================== S13-7 能力子集
def test_s13_7_attenuate_token_intersection_and_grade_order():
    """`attenuate_token`：通配感知交集 + 分级序取低 + 未知等级 fail-closed。"""
    parent = CapabilityToken("A-P", tools={"read", "write"},
                             paths={"/src", "/tmp"}, network={"api.example.com"},
                             data_class="internal")
    child = attenuate_token(parent, Scope(
        tools={"read", "deploy"}, paths={"/src", "/tmp/x", "/etc"},
        network={"api.example.com", "evil.example.com"},
        data_class="confidential"), "A-C")
    assert child.tools == {"read"}
    assert child.paths == {"/src"}
    assert child.network == {"api.example.com"}
    assert child.data_class == "internal"        # 取低，不是字面量 min
    assert child.agent_id == "A-C"
    assert child.parent_id == "A-P"

    # 父 `*` → 保留 requested；子 `*` 且父非 `*` → 取父
    wild = CapabilityToken("A-W", tools={"*"}, paths={"*"}, network={"*"},
                           data_class="*")
    c1 = attenuate_token(wild, Scope(tools={"x"}, paths={"y"}, network={"z"},
                                    data_class="restricted"), "A-C1")
    assert c1.tools == {"x"} and c1.data_class == "restricted"
    c2 = attenuate_token(parent, Scope(tools={"*"}, paths={"*"}, network={"*"},
                                      data_class="*"), "A-C2")
    assert c2.tools == {"read", "write"} and c2.paths == {"/src", "/tmp"}

    with pytest.raises(CapabilityAttenuationError):
        attenuate_token(parent, Scope(tools=set(), data_class="secret"), "A-C3")


def test_s13_7_child_token_is_subset_and_still_allows_inside():
    """①交集不变量 ②正控制（交集内仍允许）④identity ⑤不是同一 token 对象。"""
    h = Harness(auth_secret="s13-secret", run_limits=100)
    plan, parent = _parent_plan(h, "SP7")
    pid = parent.id
    tok = h.policy.mint(pid, tools={"read", "write"}, paths={"/src", "/tmp"},
                        network={"api.example.com"}, data_class="internal")
    # mint 后需重新以该 token 准入（同一 ACB key 会被复用 → 显式 replace）
    h.dispatcher.try_admit(plan["tasks"]["P"], "SP7", token=tok, plan=plan)
    parent.capability = tok
    assert parent.id == pid

    child = h.spawn_child(parent, {
        "task_id": "C1", "min_budget": 2,
        "scope": Scope(tools={"read", "deploy"}, paths={"/src", "/tmp/x", "/etc"},
                       network={"api.example.com", "evil.example.com"},
                       data_class="confidential"),
    }, plan)
    assert child.state is TaskState.ADMITTED
    resolved = h.policy.resolve_token(child.capability)
    assert resolved is not None
    assert resolved.agent_id == child.id                    # ④
    assert resolved.parent_id == parent.id                  # 可追溯
    assert resolved.tools == {"read"}                       # ①
    assert resolved.paths == {"/src"}
    assert resolved.network == {"api.example.com"}
    assert resolved.data_class == "internal"
    assert child.capability is not parent.capability        # ⑤

    # ② 正控制：交集内的 read 仍允许（防止「返回空 token」蒙混）
    h.run_task(child, [decision(1, tool("read"), finish())])
    assert child.state is TaskState.DONE
    assert len(_tool_completed(h, "read")) == 1


def test_s13_7_out_of_scope_runtime_denied_without_side_effects():
    """③ 越界运行时 DENIED_CAPABILITY：无 TOOL_COMPLETED、无预算消耗。"""
    h = Harness(auth_secret="s13-secret", run_limits=100)
    plan, parent = _parent_plan(h, "SP7D")
    tok = h.policy.mint(parent.id, tools={"read"}, paths={"*"},
                        network={"*"}, data_class="internal")
    parent.capability = tok
    child = h.spawn_child(parent, {
        "task_id": "C1", "min_budget": 2,
        "scope": Scope(tools={"read"}, data_class="public"),
    }, plan)
    node = child.budget_node
    rid = child.reservation_ids[0]

    h.run_task(child, [decision(1, tool("write"), finish())])

    assert child.state is TaskState.FAILED
    assert child.failure == "DENIED_CAPABILITY"
    assert _tool_completed(h, "write") == []
    assert node.consumed == 0
    assert h.budget._reservations[rid][1].consumed == 0


def test_s13_7_swapping_capability_to_wider_token_is_still_denied():
    """反例：`acb.capability` 换成更宽 token（无父/父不匹配）→ 运行时仍拒绝。"""
    h = Harness(auth_secret="s13-secret", run_limits=100)
    plan, parent = _parent_plan(h, "SP7W")
    tok = h.policy.mint(parent.id, tools={"read"}, paths={"*"},
                        network={"*"}, data_class="internal")
    parent.capability = tok

    child = h.spawn_child(parent, {
        "task_id": "C1", "min_budget": 2,
        "scope": Scope(tools={"read"}, data_class="public"),
    }, plan)
    # 换成「同身份、更宽 scope、但无父链」的自签 token
    child.capability = h.policy.issue(
        child.id, tools={"*"}, paths={"*"}, network={"*"}, data_class="restricted")
    assert h.policy.resolve_token(child.capability) is not None   # 自身可解析
    assert h.runtime._gateway_check(
        child, tool("write")) is False                            # 但 lineage 拒绝

    h.run_task(child, [decision(1, tool("write"), finish())])
    assert child.state is TaskState.FAILED
    assert child.failure == "DENIED_CAPABILITY"
    assert _tool_completed(h, "write") == []

    # 反例 2：直接换成父 token（agent 身份不符）
    child2 = h.spawn_child(parent, {
        "task_id": "C2", "min_budget": 2,
        "scope": Scope(tools={"read"}, data_class="public"),
    }, plan)
    child2.capability = parent.capability
    h.run_task(child2, [decision(1, tool("read"), finish())])
    assert child2.state is TaskState.FAILED
    assert child2.failure == "DENIED_CAPABILITY"
    assert _tool_completed(h, "read") == []


# =========================================================== S13-8 预算守恒
def test_s13_8_concurrent_spawn_only_one_succeeds():
    """8 线程同时 spawn（父 remaining=10，每个要 6）→ 恰好 1 成功、零残留。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SP8", min_budget=6, child_alloc=6)
    assert parent.budget_node.remaining == 10
    before = _snapshot(h)
    children_before = len(parent.children)

    barrier = threading.Barrier(8)
    results: list[ACB] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        barrier.wait()
        child = h.spawn_child(parent, {"task_id": f"C{i}", "min_budget": 6},
                              plan)
        with lock:
            results.append(child)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = [c for c in results if c.state is TaskState.ADMITTED]
    denied = [c for c in results if c.state is TaskState.INADMISSIBLE]
    assert len(ok) == 1
    assert len(denied) == 7
    assert all(c.failure == "CHILD_BUDGET_EXHAUSTED" for c in denied)
    assert all(c.id not in h.acbs for c in denied)

    # 预算守恒：父自身预留 == 成功切片量（不是「只看没超」）
    success = ok[0]
    success_amount = h.budget._reservations[success.reservation_ids[0]][1].amount
    assert success_amount == 6
    assert parent.budget_node.self_reserved == success_amount
    assert parent.budget_node.subtree_reserved == 6 + 6
    assert h.budget.root.subtree_reserved == 12

    # 零残留快照
    after = _snapshot(h)
    assert after["nodes"] - before["nodes"] == {f"B-{success.id}"}
    assert len(after["reservations"] - before["reservations"]) == 1
    assert after["slots"] == before["slots"] + 1
    assert after["events"] == before["events"] + 2      # NODE_CREATED + RESERVED
    assert len(parent.children) == children_before + 1
    assert len(parent.children_ids) == 1
    assert parent.state is TaskState.ADMITTED
    h.budget.check_invariants()


# =========================================================== S13-9 取消级联
def test_s13_9_cancel_cascades_children_and_persists(tmp_path):
    """①可发现性 ②CANCELLED ③load_strict 见 CANCELLED ④租约 ⑤预算 ⑥槽位/事件。"""
    h = _bare_with_token(run_limits=100, checkpoint_root=str(tmp_path),
                         spawn_depth=2)
    plan, parent = _parent_plan(h, "SP9", min_budget=2, child_alloc=20)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2}, plan)
    grand = h.spawn_child(child, {"task_id": "G", "min_budget": 2}, plan)

    # ① 可发现性：children_ids 含 child 且带 checkpoint_ref（文件版可反查）
    assert child.id in _child_ids(parent)
    ref = next(r for cid, r in parent.children_ids if cid == child.id)
    assert ref is not None

    for name, acb in (("p", parent), ("c", child), ("g", grand)):
        h.intents.reserve("res-" + name, acb.id, 0, 60_000)
    assert all(l.valid for l in h.intents._leases.values())
    all_rids = {rid for acb in (parent, child, grand)
                for rid in acb.reservation_ids}
    child_rids = set(child.reservation_ids)

    h.run_coordinator.cancel_run("SP9")

    # ② 状态 CANCELLED
    assert parent.state is TaskState.CANCELLED
    assert child.state is TaskState.CANCELLED
    assert grand.state is TaskState.CANCELLED

    # ③ 持久化：load_strict 读到 CANCELLED
    for acb in (parent, child, grand):
        latest = h.checkpoints.latest_ref_for_acb(acb.id)
        assert latest is not None
        assert h.checkpoints.load_strict(latest)["state"] == "CANCELLED"

    # ④ 租约全 invalid
    stale = [l for l in h.intents._leases.values()
             if l.agent_id in {parent.id, child.id, grand.id}]
    assert len(stale) == 3
    assert all(not l.valid for l in stale)

    # ⑤ 预算全 SETTLED/ARCHIVED + 不变量
    assert len(all_rids) == 3
    for rid in all_rids:
        assert _reservation_state(h, rid) in ("SETTLED", "ARCHIVED")
    h.budget.check_invariants()

    # ⑥ 槽位/事件：无活跃槽位；child 的结算事件恰好一条
    assert h.slots.active == 0
    settled = [e for e in h.event_log.all()
               if e.kind.value == "BUDGET_SETTLED"
               and (e.payload or {}).get("rid") in child_rids]
    assert len(settled) == 1


def test_s13_9_cancel_discovers_child_after_crash_from_persisted_ref(tmp_path):
    """崩溃前已 spawn：进程内已无 child，cancel_run 仍读持久化 children_ids 级联。"""
    h = _bare_with_token(run_limits=100, checkpoint_root=str(tmp_path))
    plan, parent = _parent_plan(h, "SP9C", min_budget=2, child_alloc=6)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2}, plan)
    cid, rid = child.id, child.reservation_ids[0]
    h.intents.reserve("res-c", cid, 0, 60_000)

    # 模拟崩溃：进程内丢失 child（内存 parent.children 也清空，只剩持久化链）
    h.acbs.pop(cid, None)
    parent.children = [c for c in parent.children if c.id != cid]

    h.run_coordinator.cancel_run("SP9C")

    latest = h.checkpoints.latest_ref_for_acb(cid)
    assert latest is not None
    assert h.checkpoints.load_strict(latest)["state"] == "CANCELLED"
    assert _reservation_state(h, rid) in ("SETTLED", "ARCHIVED")
    assert all(not l.valid for l in h.intents._leases.values())
    assert h.run_coordinator.is_cancelled(ACB(id=cid, run_id="SP9C", task_id="C"))
    assert h.slots.active == 0
    h.budget.check_invariants()


def test_s13_9_cancel_uses_acb_id_for_independent_child_run():
    """子用**独立 run_id**：`is_cancelled` 必须先查 acb_id（否则看不到取消）。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SP9R", min_budget=2, child_alloc=6)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2,
                                   "run_id": "CHILD"}, plan)
    assert child.run_id == "CHILD"

    h.run_coordinator.cancel_run("SP9R")

    assert child.state is TaskState.CANCELLED
    assert h.run_coordinator.is_cancelled(
        ACB(id=child.id, run_id="CHILD", task_id="C"))
    h.budget.check_invariants()


def test_s13_9_negative_control_cascade_false_leaves_child_nonterminal():
    """负对照：`cascade=False` → 子仍非终态（父已 CANCELLED）。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SP9N", min_budget=2, child_alloc=6)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2}, plan)

    h.run_coordinator.cancel_run("SP9N", cascade=False)

    assert parent.state is TaskState.CANCELLED
    assert child.state in (TaskState.ADMITTED, TaskState.PLANNED)
    assert child.state not in (TaskState.CANCELLED, TaskState.FAILED,
                               TaskState.DONE, TaskState.DEAD_LETTER)


# ============================================================ S13-10 有界
def test_s13_10_depth_fanout_budget_limits_reject_without_residue():
    """超 depth / fanout / min_budget / 预算 → 明确 failure，父不崩、零残留。"""
    # depth：1 层（默认）后子再 spawn → 拒绝
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SP10D", min_budget=2, child_alloc=20)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2}, plan)
    before = _snapshot(h)
    deep = h.spawn_child(child, {"task_id": "G", "min_budget": 2}, plan)
    assert deep.state is TaskState.INADMISSIBLE
    assert deep.failure == "SPAWN_DEPTH_EXCEEDED"
    assert _snapshot(h) == before
    assert parent.state is TaskState.ADMITTED and child.state is TaskState.ADMITTED

    # fanout：max_fanout=2
    h2 = _bare_with_token(run_limits=100, max_fanout=2)
    plan2, p2 = _parent_plan(h2, "SP10F", min_budget=2, child_alloc=20)
    c1 = h2.spawn_child(p2, {"task_id": "C1", "min_budget": 2}, plan2)
    c2 = h2.spawn_child(p2, {"task_id": "C2", "min_budget": 2}, plan2)
    assert c1.state is TaskState.ADMITTED and c2.state is TaskState.ADMITTED
    before2 = _snapshot(h2)
    third = h2.spawn_child(p2, {"task_id": "C3", "min_budget": 2}, plan2)
    assert third.state is TaskState.INADMISSIBLE
    assert third.failure == "SPAWN_FANOUT_EXCEEDED"
    assert _snapshot(h2) == before2
    assert p2.state is TaskState.ADMITTED

    # min_budget_per_child=2，请求 1 → 拒绝
    h3 = _bare_with_token(run_limits=100, min_budget_per_child=2)
    plan3, p3 = _parent_plan(h3, "SP10M", min_budget=2, child_alloc=20)
    before3 = _snapshot(h3)
    too_small = h3.spawn_child(p3, {"task_id": "C", "min_budget": 1}, plan3)
    assert too_small.state is TaskState.INADMISSIBLE
    assert too_small.failure == "CHILD_BUDGET_TOO_SMALL"
    assert _snapshot(h3) == before3

    # 预算：父 remaining=10，请求 99 → CHILD_BUDGET_EXHAUSTED
    h4 = _bare_with_token(run_limits=100)
    plan4, p4 = _parent_plan(h4, "SP10B", min_budget=6, child_alloc=6)
    before4 = _snapshot(h4)
    broke = h4.spawn_child(p4, {"task_id": "C", "min_budget": 99}, plan4)
    assert broke.state is TaskState.INADMISSIBLE
    assert broke.failure == "CHILD_BUDGET_EXHAUSTED"
    assert _snapshot(h4) == before4
    assert broke.id not in h4.acbs
    h4.budget.check_invariants()


# ======================================================= S13-11 无双重预留
def test_s13_11_spawn_reserves_child_node_once_not_twice():
    """`parent.subtree_reserved == child.min_budget`（不是 2×）；root 同样。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SP11", min_budget=6, child_alloc=6)
    # 隔离 spawn 增量：先结算父自身预留（否则 subtree 含父的 6）
    h.budget.settle(parent.reservation_ids[0])
    parent.reservation_ids = []
    assert parent.budget_node.subtree_reserved == 0
    assert h.budget.root.subtree_reserved == 0

    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 6}, plan)

    assert parent.budget_node.subtree_reserved == child.spec["spawn"]["min_budget"]
    assert parent.budget_node.subtree_reserved == 6
    assert parent.budget_node.self_reserved == 0          # 父未二次预留
    assert child.budget_node.self_reserved == 6
    assert h.budget.root.subtree_reserved == 6
    h.budget.check_invariants()


# =================================================== ResultEnvelope 结构化回传
def test_result_envelope_produces_done_partial():
    """`DONE_PARTIAL` 产生路径：一子 DONE + 一子 FAILED → 父汇总为 partial。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SPENV", min_budget=2, child_alloc=20)
    done_child = h.spawn_child(parent, {"task_id": "C1", "min_budget": 2}, plan)
    failed_child = h.spawn_child(parent, {"task_id": "C2", "min_budget": 2}, plan)

    h.run_task(done_child, [decision(1, finish())])
    assert done_child.state is TaskState.DONE

    failed_child.state = TaskState.FAILED
    failed_child.failure = "BOOM"
    h._finalize(failed_child)

    env = h.result_envelope(parent)
    assert env.status == "DONE_PARTIAL"
    assert env.coverage == {"C1": "DONE", "C2": "FAILED"}
    assert len(env.artifacts) == 2
    assert env.conflicts == [{"acb_id": failed_child.id, "reason": "BOOM"}]
    assert env.budget_usage == 0

    h.apply_result_envelope(parent)
    assert parent.state is TaskState.DONE_PARTIAL
    h.budget.check_invariants()
