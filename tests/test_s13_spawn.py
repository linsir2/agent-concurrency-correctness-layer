"""S13c：spawn_child（能力子集 / 预算守恒 / 取消级联 / 有界 / 无双重预留）。

对应 `docs/s13-orchestration.md` v1.1 §2.4 与 §3 的 S13-7 / S13-8 / S13-9 /
S13-10 / S13-11（本文件另覆盖 `ResultEnvelope` 的 `DONE_PARTIAL` 产生路径）。

夹具陷阱：能力子集用例必须用**裸 `Harness(auth_secret=...)` + `mint`**，
不能用 `make_harness`（`default_token="*"` 会全放行，抹掉 attenuation）。

S13c 评审修复对应的新/改用例（C1-C10）集中在本文件 + `test_adversarial.py`：
- C1：两阶段窗口 / 孤儿结算 / ②事务崩溃回滚
- C2：清空内存后从持久化 run 清单重建父子图撤销
- C3：`DONE_PARTIAL` 仅在全部子终态时产生 / 过早 partial / 全取消 / 子缺失 / 幂等
- C4：paths/network 语义包含（不是集合精确交集）
- C5：跨进程恢复后 token 注册 + 父链按需加载
- C6：lineage 锚定父能力快照 + `parent_id` 不可变
- C7：`child_alloc` 可推导默认 + 逐子记账 + slice 受剩余额约束
- C8：depth 写入 payload + 父链断链 fail-closed + fanout 只计未终态
"""

from __future__ import annotations

import threading

import pytest

from conftest import decision, finish, tool

from harness import ACB, CapabilityToken, EventKind, Harness, Scope, Status, TaskState
from harness.event_log import WalEventLog
from harness.policy import CapabilityAttenuationError, attenuate_token
from harness.runtime import CrashInjected


# ---------------------------------------------------------------- helpers
def _bare_with_token(**kwargs) -> Harness:
    """裸 Harness + 通配 token（仅预算/取消用例；能力用例不得使用）。"""
    h = Harness(**kwargs)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    return h


def _parent_plan(h: Harness, run_id: str, *, min_budget: int = 6,
                 child_alloc: int = 20) -> tuple[dict, ACB]:
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


def _persisted_state(h: Harness, acb_id: str) -> str | None:
    ref = h.checkpoints.latest_ref_for_acb(acb_id)
    if ref is None:
        return None
    return h.checkpoints.load_strict(ref)["state"]


# =========================================================== S13-7 能力子集
def test_s13_7_attenuate_token_intersection_and_grade_order():
    """`attenuate_token`：通配感知 + 分级序取低 + 未知等级 fail-closed。

    C4：paths/network 按**语义包含**（父 `/src` 覆盖请求 `/src/lib`，保留
    更窄的请求项；请求更宽时保留父项）。
    """
    parent = CapabilityToken("A-P", tools={"read", "write"},
                             paths={"/src", "/tmp"}, network={"api.example.com"},
                             data_class="internal")
    child = attenuate_token(parent, Scope(
        tools={"read", "deploy"}, paths={"/src", "/tmp/x", "/etc"},
        network={"api.example.com", "evil.example.com"},
        data_class="confidential"), "A-C")
    assert child.tools == {"read"}
    # 父 /src 覆盖 /src；父 /tmp 覆盖更窄的 /tmp/x；/etc 无交集被丢弃
    assert child.paths == {"/src", "/tmp/x"}
    assert child.network == {"api.example.com"}
    assert child.data_class == "internal"        # 取低，不是字面量 min
    assert child.agent_id == "A-C"
    assert child.parent_id == "A-P"
    # C6：父能力快照随 token 冻结
    assert child.parent_scope is not None
    assert child.parent_scope["paths"] == ["/src", "/tmp"]
    assert child.parent_scope_digest

    # 父 `*` → 保留 requested；子 `*` 且父非 `*` → 取父
    wild = CapabilityToken("A-W", tools={"*"}, paths={"*"}, network={"*"},
                           data_class="*")
    c1 = attenuate_token(wild, Scope(tools={"x"}, paths={"y"}, network={"z"},
                                    data_class="restricted"), "A-C1")
    assert c1.tools == {"x"} and c1.data_class == "restricted"
    c2 = attenuate_token(parent, Scope(tools={"*"}, paths={"*"}, network={"*"},
                                      data_class="*"), "A-C2")
    assert c2.tools == {"read", "write"} and c2.paths == {"/src", "/tmp"}

    # C4：network 语义包含（父网段覆盖请求 IP → 保留请求；请求更宽 → 保留父）
    net_parent = CapabilityToken("A-N", network={"10.0.0.0/8"},
                                 data_class="internal")
    net_narrow = attenuate_token(
        net_parent, Scope(network={"10.1.2.3"}, data_class="internal"), "A-N1")
    assert net_narrow.network == {"10.1.2.3"}
    net_wide = attenuate_token(
        net_parent, Scope(network={"0.0.0.0/0"}, data_class="internal"), "A-N2")
    assert net_wide.network == {"10.0.0.0/8"}
    wild_parent = CapabilityToken("A-NW", network={"*.example.com"},
                                  data_class="internal")
    net_host = attenuate_token(
        wild_parent, Scope(network={"api.example.com"}, data_class="internal"),
        "A-N3")
    assert net_host.network == {"api.example.com"}

    # 请求更宽（`/` 覆盖父 `/src`）→ 保留父项（收窄）
    wide_req = attenuate_token(parent, Scope(paths={"/"}, data_class="internal"),
                              "A-C4")
    assert wide_req.paths == {"/src", "/tmp"}

    # C4：未知等级在任何分支都 fail-closed（含 a="*" 的旁路）
    with pytest.raises(CapabilityAttenuationError):
        attenuate_token(parent, Scope(tools=set(), data_class="secret"), "A-C3")
    with pytest.raises(CapabilityAttenuationError):
        attenuate_token(wild, Scope(tools=set(), data_class="secret"), "A-C5")


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
    assert resolved.paths == {"/src", "/tmp/x"}
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


# ====================================================== C6 lineage 不可绕过
def test_s13_c6_lineage_is_anchored_to_frozen_parent_scope():
    """C6(a)：覆盖全局 `_tokens[parent_id]` 为宽 token + 伪造 parent_id → 仍拒绝。

    lineage 只认签发时冻结的父能力快照；无快照的自签 token 一律 fail-closed。
    """
    h = Harness(auth_secret="s13-secret", run_limits=100)
    plan, parent = _parent_plan(h, "S6A")
    tok = h.policy.mint(parent.id, tools={"read"}, paths={"/src"},
                        network={"api.example.com"}, data_class="internal")
    parent.capability = tok

    child = h.spawn_child(parent, {
        "task_id": "C", "min_budget": 2,
        "scope": Scope(tools={"read"}, paths={"/src"}, data_class="internal"),
    }, plan)
    assert h.policy.resolve_token(child.capability) is not None

    # 攻击：把全局父 token 覆盖成宽 token，再伪造 child.parent_id
    h.policy.issue(parent.id, tools={"*"}, paths={"*"}, network={"*"},
                   data_class="restricted")

    # ① 原 token 仍只能做 read（快照收窄，不被全局宽 token 放宽）
    assert h.policy.resolve_token(child.capability).tools == {"read"}
    assert h.policy.check_capability(
        child.id, child.capability, "write", "") is False

    # ② 自签宽 token + 伪造 parent_id（无快照）→ fail-closed
    forged = h.policy.issue(child.id, tools={"*"}, paths={"*"},
                            network={"*"}, data_class="restricted")
    forged.parent_id = parent.id
    child.capability = forged
    assert h.policy.resolve_token(forged) is None
    h.run_task(child, [decision(1, tool("write"), finish())])
    assert child.state is TaskState.FAILED
    assert child.failure == "DENIED_CAPABILITY"


def test_s13_c6_replan_keeps_parent_lineage():
    """C6：`replan` 再准入不得把子洗成 root（parent_id 继承 + 父树引用同步）。"""
    h = Harness(auth_secret="s13-secret", run_limits=100)
    plan, parent = _parent_plan(h, "S6R")
    tok = h.policy.mint(parent.id, tools={"read"}, paths={"/src"},
                        network={"api.example.com"}, data_class="internal")
    parent.capability = tok
    child = h.spawn_child(parent, {
        "task_id": "C", "min_budget": 2,
        "scope": Scope(tools={"read"}, paths={"/src"}, data_class="internal"),
    }, plan)
    child.state = TaskState.QUARANTINED

    new_plan = h.replan(child, plan, diagnosis={
        "failing_subgoals": [child.logical_subgoal_id], "missing_deps": []})
    assert isinstance(new_plan, dict)
    fresh = h.acbs.get(next(aid for aid, a in h.acbs.items()
                            if a.task_id == "C'" or a.task_id == "C"))
    assert fresh.state is TaskState.ADMITTED
    assert fresh.parent_id == parent.id                    # 不可变
    assert fresh.capability.parent_id == parent.id
    assert h.policy.resolve_token(fresh.capability) is not None
    assert fresh.id in _child_ids(parent)
    assert child.id not in _child_ids(parent)


def test_s13_c6_requeue_keeps_parent_lineage_and_denies_forged_widen():
    """C6(b)：生产可达路径 —— 对 spawn 子 `requeue_readmit` 不得洗掉 parent_id。"""
    h = Harness(auth_secret="s13-secret", run_limits=100)
    plan, parent = _parent_plan(h, "S6B")
    tok = h.policy.mint(parent.id, tools={"read"}, paths={"/src"},
                        network={"api.example.com"}, data_class="internal")
    parent.capability = tok

    child = h.spawn_child(parent, {
        "task_id": "C", "min_budget": 2,
        "scope": Scope(tools={"read"}, paths={"/src"}, data_class="internal"),
    }, plan)
    fresh = h.requeue_readmit(child)
    assert fresh is not child
    assert fresh.parent_id == parent.id                       # 不可变
    assert fresh.capability.parent_id == parent.id
    assert h.policy.resolve_token(fresh.capability) is not None
    # 父树引用同步到新身份
    assert fresh.id in _child_ids(parent)
    assert child.id not in _child_ids(parent)

    # 攻击：换自签宽 token + 伪造 parent_id → 仍拒绝
    forged = h.policy.issue(fresh.id, tools={"*"}, paths={"*"},
                            network={"*"}, data_class="restricted")
    forged.parent_id = parent.id
    fresh.capability = forged
    assert h.policy.resolve_token(forged) is None
    assert h.runtime._gateway_check(fresh, tool("write")) is False
    h.run_task(fresh, [decision(1, tool("write"), finish())])
    assert fresh.state is TaskState.FAILED
    assert fresh.failure == "DENIED_CAPABILITY"


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
    """①可发现性 ②CANCELLED ③load_strict 见 CANCELLED ④租约 ⑤预算 ⑥槽位/事件。

    C9：子/孙使用**独立 run_id** —— 若 `_walk_children` 失效，仅凭 `cancel_run`
    广播无法把它们变成 CANCELLED（级联部分不再是恒真）。
    """
    h = _bare_with_token(run_limits=100, checkpoint_root=str(tmp_path),
                         spawn_depth=2)
    plan, parent = _parent_plan(h, "SP9", min_budget=2, child_alloc=20)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2,
                                   "run_id": "CHILD-C"}, plan)
    grand = h.spawn_child(child, {"task_id": "G", "min_budget": 2,
                                  "run_id": "CHILD-G"}, plan)
    assert child.run_id == "CHILD-C" and grand.run_id == "CHILD-G"

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

    # ② 状态 CANCELLED（子/孙 run_id 不同 → 只能靠父链级联）
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


def test_s13_9_negative_control_cascade_false_leaves_child_nonterminal():
    """负对照：`cascade=False` → 子仍非终态（父已 CANCELLED）。

    C9：子用**独立 run_id**，否则 `is_cancelled(child)` 仅凭 run_id 广播已为
    True → guard 报 CANCELLED，负对照不成立。这里断言 guard 非 CANCELLED。
    """
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SP9N", min_budget=2, child_alloc=6)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2,
                                   "run_id": "CHILD-N"}, plan)

    h.run_coordinator.cancel_run("SP9N", cascade=False)

    assert parent.state is TaskState.CANCELLED
    assert child.state in (TaskState.ADMITTED, TaskState.PLANNED)
    assert child.state not in (TaskState.CANCELLED, TaskState.FAILED,
                               TaskState.DONE, TaskState.DEAD_LETTER)
    ok, reason = h.runtime.guard(child)
    assert ok is True
    assert reason != "CANCELLED"
    assert h.run_coordinator.is_cancelled(child) is False


# =============================================== C2 持久化 run 清单重建父子图
def test_s13_c2_cancel_rebuilds_tree_from_persisted_run_index(tmp_path):
    """C2：`latest_run` 根指针被后代覆盖也不漏取消 —— 枚举 run 全部 ref 重建图。

    复现旧 bug：spawn P、A、B 后对 B `save`（最新 run 指针指向 B），清空
    `h.acbs` 再 `cancel_run` → 旧实现只取消 B，父/A 的预留仍 ACTIVE。
    """
    h = _bare_with_token(run_limits=100, checkpoint_root=str(tmp_path))
    plan, parent = _parent_plan(h, "SP9P", min_budget=2, child_alloc=20)
    child_a = h.spawn_child(parent, {"task_id": "A", "min_budget": 2}, plan)
    child_b = h.spawn_child(parent, {"task_id": "B", "min_budget": 2}, plan)
    h.checkpoints.save(child_b)          # latest_run 被后代覆盖成 B
    assert h.checkpoints.latest_ref_for_run("SP9P") == \
        h.checkpoints.latest_ref_for_acb(child_b.id)

    rids = {rid for acb in (parent, child_a, child_b)
            for rid in acb.reservation_ids}
    assert len(rids) == 3
    h.acbs.clear()                       # 内存无该 run（只能靠持久化图）

    h.run_coordinator.cancel_run("SP9P")

    for acb in (parent, child_a, child_b):
        assert _persisted_state(h, acb.id) == "CANCELLED"
        assert h.run_coordinator.is_cancelled(
            ACB(id=acb.id, run_id=acb.run_id, task_id=acb.task_id))
    for rid in rids:
        assert _reservation_state(h, rid) in ("SETTLED", "ARCHIVED")
    assert not [s for s in h._drain_skipped
                if s[1] == "cancel_skip_missing_ref"]
    assert h.slots.active == 0
    h.budget.check_invariants()


# ============================================== C1 两阶段落盘 / 孤儿 / 回滚
def test_s13_c1_pending_ref_none_cancel_finds_child_by_id():
    """C1：(child_id, None) 的崩溃窗口 —— 按 child_id 找回子快照并取消。

    旧实现 `_cancel_from_ref(None)` 直接 `cancel_skip_missing_ref`（静默漏取消）。
    """
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SP9X", min_budget=2, child_alloc=20)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2}, plan)
    cid, rid = child.id, child.reservation_ids[0]
    # 模拟 phase ① 落盘后崩溃（②③ 未完成）：持久化只有 (cid, None)
    info = {"task_id": "C", "min_budget": 2, "slice": 6,
            "run_id": "SP9X", "logical_subgoal_id": "S-C", "depth": 1,
            "parent_id": parent.id}
    parent.children_pending[cid] = info
    parent.children_ids = [(cid, None)]
    parent.children = []
    h._save_strict(parent)
    h.acbs.pop(cid, None)

    h.run_coordinator.cancel_run("SP9X")

    assert _persisted_state(h, cid) == "CANCELLED"
    assert _reservation_state(h, rid) in ("SETTLED", "ARCHIVED")
    assert ("None", "cancel_skip_missing_ref") not in h._drain_skipped
    h.budget.check_invariants()


def test_s13_c1_missing_child_snapshot_settles_orphan_budget_not_silently():
    """C1：子快照不存在但父节点的 ACTIVE 预留存在 → 孤儿结算 + 事件留痕。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SPO", min_budget=2, child_alloc=20)
    cid = "A-SPO-C-orphan"
    info = {"task_id": "C", "min_budget": 2, "slice": 6,
            "run_id": "SPO", "logical_subgoal_id": "S-C", "depth": 1,
            "parent_id": parent.id}
    parent.children_pending[cid] = info
    h._save_strict(parent)
    # phase ② 已建节点/预留，但 phase ③ 之前崩溃（无子快照）
    with h.budget.suppressed():
        node = h.budget.create_node(
            f"B-{cid}", 6, parent_id=parent.budget_node.node_id, run_id="SPO")
        node.owner = cid
        resv = h.budget.reserve(node.node_id, 2)
    h.budget.flush_pending()
    assert h.budget._reservations[resv.reservation_id][1].state == "ACTIVE"

    h.run_coordinator.cancel_run("SPO")

    assert h.budget._reservations[resv.reservation_id][1].state in (
        "SETTLED", "ARCHIVED")
    assert f"B-{cid}" not in h.budget._nodes
    assert (cid, "spawn_orphan_reclaimed") in h._drain_skipped
    orphans = [e for e in h.event_log.all()
               if e.kind is EventKind.SPAWN_ORPHAN_RECLAIMED]
    assert len(orphans) == 1
    assert (orphans[0].payload or {}).get("child_id") == cid
    h.budget.check_invariants()


def test_s13_c1_recover_promotes_pending_child_found_by_ref(tmp_path):
    """C1：`recover` 遇到 `children_pending`（phase ① 已落盘）→ 找回子并转正。"""
    h = _bare_with_token(run_limits=100, checkpoint_root=str(tmp_path))
    plan, parent = _parent_plan(h, "SPR", min_budget=2, child_alloc=20)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 2}, plan)
    cid = child.id
    # 模拟 phase ② 后 / ③ 前崩溃：父只有 pending（ref 尚未回填、未记账）
    parent.children_pending[cid] = {
        "task_id": "C", "min_budget": 2, "slice": 6, "run_id": "SPR",
        "logical_subgoal_id": child.logical_subgoal_id, "depth": 1,
        "parent_id": parent.id}
    parent.children_ids = []
    parent.children = []
    parent.spec["child_alloc_used"] = 0
    h._save_strict(parent)
    pref = h.checkpoints.latest_ref_for_acb(parent.id)
    assert pref is not None
    h.acbs.pop(cid, None)
    h.acbs.pop(parent.id, None)

    rec = h.recover(pref)

    assert rec.children_pending == {}
    assert cid in [str(e[0]) for e in rec.children_ids]
    assert rec.spec["child_alloc_used"] == 6       # 成功子补记账
    h.budget.check_invariants()


def test_s13_c1_phase2_crash_injection_rolls_back_then_reraises():
    """C1：spawn ② 事务遇 `CrashInjected` → 回滚（零残留）后**必须重抛**。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SPC", min_budget=2, child_alloc=20)
    before = _snapshot(h)

    orig_reserve = h.budget.reserve

    def boom(*args, **kwargs):
        raise CrashInjected(0, 0)

    h.budget.reserve = boom
    try:
        with pytest.raises(CrashInjected):
            h.spawn_child(parent, {"task_id": "C", "min_budget": 2}, plan)
    finally:
        h.budget.reserve = orig_reserve

    assert _snapshot(h) == before
    assert parent.children_pending == {}
    assert parent.children_ids == []
    assert parent.children == []
    h.budget.check_invariants()


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


# ======================================================= C7 child_alloc / slice
def test_s13_c7_child_alloc_derived_and_nested_grandchildren():
    """C7：准入缺省 child_alloc 可推导；子份额由父剩余额推导（不是写死 0）。"""
    h = _bare_with_token(run_limits=100, spawn_depth=2)
    plan = h.planner.plan("S7", [{"task_id": "P", "min_budget": 1}])
    h.plans["S7"] = plan
    parent = h.dispatcher.try_admit(plan["tasks"]["P"], "S7", plan=plan)
    h.acbs[parent.id] = parent
    h.run_coordinator.register(parent)
    expected = max(h.max_fanout, 1) * h.min_budget_per_child
    assert parent.spec["child_alloc"] == expected
    assert parent.budget_node.limits == 1 + expected + parent.retry_budget + 1

    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 1}, plan)
    assert child.spec["spawn"]["slice"] == min(
        1 + child.retry_budget + 1, expected)
    assert child.spec["child_alloc"] == \
        child.spec["spawn"]["slice"] - 1
    assert child.spec["child_alloc"] > 0          # 孙辈有预算（不是 0）

    grand = h.spawn_child(child, {"task_id": "G", "min_budget": 1}, plan)
    assert grand.state is TaskState.ADMITTED
    great = h.spawn_child(grand, {"task_id": "GG", "min_budget": 1}, plan)
    assert great.state is TaskState.INADMISSIBLE
    assert great.failure == "SPAWN_DEPTH_EXCEEDED"
    h.budget.check_invariants()


def test_s13_c7_admission_rejects_explicit_child_alloc_too_small():
    """C7：准入校验 `child_alloc >= min_budget_per_child`（显式 1 < 2）。"""
    h = _bare_with_token(run_limits=100, min_budget_per_child=2)
    plan = h.planner.plan("S7S", [{"task_id": "T", "min_budget": 2,
                                   "child_alloc": 1}])
    h.plans["S7S"] = plan
    acb = h.dispatcher.try_admit(plan["tasks"]["T"], "S7S", plan=plan)
    assert acb.state is TaskState.INADMISSIBLE
    assert acb.failure == "INVALID_INPUT"
    assert acb.spec.get("admission_error") == "child_alloc_too_small"


def test_s13_c7_slice_is_bounded_by_child_alloc_remaining():
    """C7：`slice = min(child_alloc_remaining, min+retry+1)`，用尽 → 拒绝。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "S7B", min_budget=1, child_alloc=3)
    c1 = h.spawn_child(parent, {"task_id": "C1", "min_budget": 1}, plan)
    assert c1.state is TaskState.ADMITTED
    assert c1.spec["spawn"]["slice"] == 3
    assert parent.spec["child_alloc_used"] == 3
    c2 = h.spawn_child(parent, {"task_id": "C2", "min_budget": 1}, plan)
    assert c2.state is TaskState.INADMISSIBLE
    assert c2.failure == "CHILD_BUDGET_EXHAUSTED"
    h.budget.check_invariants()


# ========================================================= C8 depth / fanout
def test_s13_c8_depth_survives_parent_memory_loss_and_fails_closed():
    """C8：depth 写入子 payload；父链断链也不得低估（旧实现返回 1 → 越界）。"""
    h = _bare_with_token(run_limits=100, spawn_depth=2)
    plan, parent = _parent_plan(h, "S8", min_budget=1, child_alloc=20)
    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 1}, plan)
    grand = h.spawn_child(child, {"task_id": "G", "min_budget": 1}, plan)
    assert child.spec["spawn"]["depth"] == 1
    assert grand.spec["spawn"]["depth"] == 2

    # 模拟恢复后内存缺失中间父（C 不在 h.acbs）
    h.acbs.pop(child.id, None)
    deeper = h.spawn_child(grand, {"task_id": "GG", "min_budget": 1}, plan)
    assert deeper.state is TaskState.INADMISSIBLE
    assert deeper.failure == "SPAWN_DEPTH_EXCEEDED"

    # 完全无持久化 depth 且父缺失 → fail-closed（不返回 1）
    orphan = ACB(id="A-S8-X", run_id="S8", task_id="X")
    orphan.parent_id = "A-S8-missing"
    rejected = h.spawn_child(orphan, {"task_id": "Y", "min_budget": 1}, plan)
    assert rejected.failure == "SPAWN_DEPTH_EXCEEDED"


def test_s13_c8_fanout_counts_only_nonterminal_children():
    """C8：终态子不占 `max_fanout`；未终态（含 pending）才计数。"""
    h = _bare_with_token(run_limits=100, max_fanout=2)
    plan, parent = _parent_plan(h, "S8F", min_budget=1, child_alloc=20)
    c1 = h.spawn_child(parent, {"task_id": "C1", "min_budget": 1}, plan)
    c2 = h.spawn_child(parent, {"task_id": "C2", "min_budget": 1}, plan)
    assert c1.state is TaskState.ADMITTED and c2.state is TaskState.ADMITTED
    assert h.spawn_child(parent, {"task_id": "C3", "min_budget": 1},
                         plan).failure == "SPAWN_FANOUT_EXCEEDED"

    h.run_task(c1, [decision(1, finish())])
    assert c1.state is TaskState.DONE
    c3 = h.spawn_child(parent, {"task_id": "C3", "min_budget": 1}, plan)
    assert c3.state is TaskState.ADMITTED
    h.budget.check_invariants()

    # ref=None 的 pending 子也占名额（不因「内存无 ACB」被漏计）
    hp = _bare_with_token(run_limits=100, max_fanout=1)
    planp, pp = _parent_plan(hp, "S8P", min_budget=1, child_alloc=20)
    pp.children_ids = [("A-S8P-pending", None)]
    blocked = hp.spawn_child(pp, {"task_id": "C", "min_budget": 1}, planp)
    assert blocked.failure == "SPAWN_FANOUT_EXCEEDED"


# ======================================================= S13-11 无双重预留
def test_s13_11_spawn_reserves_child_node_once_not_twice():
    """C9：真实态 `parent.subtree == 父自身 6 + 子 6 = 12`，且父未二次预留。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SP11", min_budget=6, child_alloc=6)
    assert parent.budget_node.self_reserved == 6
    assert parent.budget_node.subtree_reserved == 6
    assert h.budget.root.subtree_reserved == 6

    child = h.spawn_child(parent, {"task_id": "C", "min_budget": 6}, plan)

    # 真实态：父 6（自身）+ 子 6；双重预留会变成 18 / 父 self 12
    assert parent.budget_node.subtree_reserved == 12
    assert parent.budget_node.self_reserved == 6
    assert child.budget_node.self_reserved == 6
    assert h.budget.root.subtree_reserved == 12
    h.budget.check_invariants()


# =================================================== ResultEnvelope 结构化回传
def test_result_envelope_does_not_emit_premature_partial():
    """C3：子只是 ADMITTED（未终态）→ RUNNING，父不 finalize、预算不动。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SPENV0", min_budget=2, child_alloc=20)
    c1 = h.spawn_child(parent, {"task_id": "C1", "min_budget": 2}, plan)
    c2 = h.spawn_child(parent, {"task_id": "C2", "min_budget": 2}, plan)

    env = h.result_envelope(parent)
    assert env.status == "RUNNING"
    assert env.coverage == {"C1": "ADMITTED", "C2": "ADMITTED"}
    h.apply_result_envelope(parent)

    assert parent.state is TaskState.ADMITTED
    assert parent.budget_node is not None
    assert _reservation_state(h, parent.reservation_ids[0]) == "ACTIVE"
    assert c1.state is TaskState.ADMITTED and c2.state is TaskState.ADMITTED
    h.budget.check_invariants()


def test_result_envelope_all_cancelled_is_not_done_partial():
    """C3：全 CANCELLED → CANCELLED（旧 `elif pending` 会误报 DONE_PARTIAL）。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SPENV1", min_budget=2, child_alloc=20)
    c1 = h.spawn_child(parent, {"task_id": "C1", "min_budget": 2}, plan)
    c2 = h.spawn_child(parent, {"task_id": "C2", "min_budget": 2}, plan)
    h.run_coordinator._settle_cancel(c1)
    h.run_coordinator._settle_cancel(c2)

    env = h.result_envelope(parent)
    assert env.status == "CANCELLED"
    assert env.coverage == {"C1": "CANCELLED", "C2": "CANCELLED"}
    h.apply_result_envelope(parent)
    assert parent.state is TaskState.ADMITTED      # 非 DONE_PARTIAL 不 finalize
    h.budget.check_invariants()


def test_result_envelope_missing_child_requires_reconciliation():
    """C3：`children_ids` 有、内存/快照都没有 → NEEDS_RECONCILIATION（绝不 DONE）。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SPENV2", min_budget=2, child_alloc=20)
    parent.children_ids = [("A-ghost", None)]

    env = h.result_envelope(parent)
    assert env.status == "NEEDS_RECONCILIATION"
    h.apply_result_envelope(parent)
    assert parent.state is TaskState.ADMITTED
    assert parent.budget_node is not None


def test_result_envelope_loads_persisted_children_when_memory_lost():
    """C3：`children_ids` 有、内存无 → 按 ref 加载（ref=None 走 child_id 反查），
    绝不因「内存无子」而误报 DONE。"""
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "SPENV3", min_budget=2, child_alloc=20)
    c1 = h.spawn_child(parent, {"task_id": "C1", "min_budget": 2}, plan)
    c2 = h.spawn_child(parent, {"task_id": "C2", "min_budget": 2}, plan)
    h.run_task(c1, [decision(1, finish())])
    c2.state = TaskState.FAILED
    c2.failure = "BOOM"
    h._finalize(c2)

    # 清空内存 + 抹掉 ref（只剩 child_id），验证 `_ref_for_child` 反查
    h.acbs.pop(c1.id, None)
    h.acbs.pop(c2.id, None)
    parent.children = []
    parent.children_ids = [(c1.id, None), (c2.id, None)]

    env = h.result_envelope(parent)
    assert env.status == "DONE_PARTIAL"
    assert env.coverage == {"C1": "DONE", "C2": "FAILED"}
    assert len(env.artifacts) == 2


def test_result_envelope_produces_done_partial_and_is_idempotent():
    """C3：一子 DONE + 一子 FAILED → DONE_PARTIAL；归档预算、二次调用幂等。"""
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

    rid = parent.reservation_ids[0]
    h.apply_result_envelope(parent)
    assert parent.state is TaskState.DONE_PARTIAL
    # C3：DONE_PARTIAL 是任务级终态 → 父预算节点归档、槽位释放
    assert parent.budget_node is None
    assert _reservation_state(h, rid) in ("SETTLED", "ARCHIVED")
    assert parent.id not in h.slots.active_keys()
    h.budget.check_invariants()

    # 幂等：第二次调用不改状态 / 不重复结算
    settled_before = len([e for e in h.event_log.all()
                          if e.kind is EventKind.BUDGET_SETTLED])
    env2 = h.apply_result_envelope(parent)
    assert env2.status == "DONE_PARTIAL"
    assert parent.state is TaskState.DONE_PARTIAL
    assert len([e for e in h.event_log.all()
                if e.kind is EventKind.BUDGET_SETTLED]) == settled_before

    # 终态 guard：`run_task` 不得把 DONE_PARTIAL 覆盖成 DONE / FAILED
    assert h.run_task(parent, []) == Status.OK
    assert parent.state is TaskState.DONE_PARTIAL
    assert h.run_task(parent, [decision(1, tool("read"), finish())]) == Status.OK
    assert parent.state is TaskState.DONE_PARTIAL
    assert _tool_completed(h, "read") == []


# ============================================================ X3 子 ref 复用
def test_s13_x3_replan_child_updates_parent_ref_to_fresh_child():
    """X3：子 replan 后父 `children_ids` 的 ref 必须指向**新子**.

    旧实现复用旧子的 ref（且在 save(fresh) 之前写父），重启后
    `result_envelope` 按该 ref 载回旧子 → 永远 pending、父不 finalize；
    `_walk_children` 也漏掉新身份。这里同时覆盖「父在内存」与「父不在内存
    （load→改→save）」两条路径。
    """
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "RX3", min_budget=2, child_alloc=20)
    child = h.spawn_child(parent, {"task_id": "C1", "min_budget": 2}, plan)
    old_id = child.id
    old_ref = next(r for cid, r in parent.children_ids if cid == old_id)
    assert old_ref is not None

    child.state = TaskState.QUARANTINED
    child.failure = "X3"
    plan2 = h.replan(child, plan, diagnosis={"failing_subgoals": ["S-C1"]})
    assert isinstance(plan2, dict)
    fresh = next(a for a in h.acbs.values() if a.task_id == "C1'")

    entries = dict(parent.children_ids)
    assert old_id not in entries
    ref = entries[fresh.id]
    assert ref is not None and ref != old_ref
    data = h.checkpoints.load(ref)
    assert data["acb_id"] == fresh.id          # 载到的是**新子**，不是旧子
    assert data["state"] == TaskState.ADMITTED.value
    assert data["superseded"] is False

    # 父不在内存 → 必须 load→改→save（不能直接 return 静默丢弃）
    h.acbs.pop(parent.id, None)
    fresh.state = TaskState.QUARANTINED
    fresh.failure = "X3-2"
    plan3 = h.replan(fresh, plan2, diagnosis={"failing_subgoals": ["S-C1"]})
    assert isinstance(plan3, dict)
    fresh2 = next(a for a in h.acbs.values() if a.task_id == "C1''")

    parent_ref = h.checkpoints.latest_ref_for_acb(parent.id)
    assert parent_ref is not None
    persisted = h.checkpoints.load(parent_ref)
    peach = {str(cid): cref for cid, cref in persisted["children_ids"]}
    assert fresh.id not in peach
    assert fresh2.id in peach and peach[fresh2.id] is not None
    assert h.checkpoints.load(peach[fresh2.id])["acb_id"] == fresh2.id
    h.budget.check_invariants()


# ============================================================ X6 跨 run 子
def test_s13_x6_cross_run_child_cancelled_via_parent_id_index():
    """X6：跨 run 子 + 父 `children_ids` 缺失（老 checkpoint）→ 取消不得静默漏掉。

    恢复时按 `spec.spawn.parent_id` / `parent_id` 反扫建立 `parent → [child]`
    索引，`_walk_children` 据此找回跨 run 子。
    """
    h = _bare_with_token(run_limits=100)
    plan, parent = _parent_plan(h, "R6P", min_budget=2, child_alloc=20)
    child = h.spawn_child(parent, {
        "task_id": "C1", "min_budget": 2, "run_id": "R6C"}, plan)
    assert child.run_id == "R6C"
    assert child.state is TaskState.ADMITTED

    # 模拟老 checkpoint：父没有任何 children_ids/children 记录；清空内存索引，
    # 强制走「反扫快照」重建。
    parent.children_ids = []
    parent.children = []
    h._children_index = {}
    h._children_index_built = False

    h.run_coordinator.cancel_run("R6P")
    assert child.state is TaskState.CANCELLED      # 跨 run 子仍被级联取消
    assert (child.id, "cancel_skip_missing_ref") not in h._drain_skipped
    h.budget.check_invariants()


# ============================================================ X2 前驱语义
def test_s13_x2_done_partial_satisfies_predecessor_and_successor_runs():
    """X2：T1 → T2，T1 汇总为 `DONE_PARTIAL` 后 T2 必须可调度并执行。

    旧实现 `PREDECESSOR_SATISFIED={DONE}` → T2 永久 PLANNED、ready 为空、
    Run 永不 finalize（同波评审看不到的跨波缺陷）。
    """
    h = _bare_with_token(run_limits=100)
    plan = h.planner.plan("RDP", [
        {"task_id": "T1", "logical_subgoal_id": "S1",
         "min_budget": 2, "child_alloc": 20},
        {"task_id": "T2", "logical_subgoal_id": "S2", "min_budget": 2}])
    h.planner.add_logical_edge(plan, "T1", "T2")
    h.plans["RDP"] = plan
    parent = h.dispatcher.try_admit(plan["tasks"]["T1"], "RDP", plan=plan)
    h.acbs[parent.id] = parent
    h.run_coordinator.register(parent)
    assert parent.state is TaskState.ADMITTED

    done_child = h.spawn_child(parent, {"task_id": "C1", "min_budget": 2}, plan)
    failed_child = h.spawn_child(parent, {"task_id": "C2", "min_budget": 2}, plan)
    h.run_task(done_child, [decision(1, finish())])
    assert done_child.state is TaskState.DONE
    failed_child.state = TaskState.FAILED
    failed_child.failure = "BOOM"
    h._finalize(failed_child)

    env = h.apply_result_envelope(parent)
    assert env.status == "DONE_PARTIAL"
    assert parent.state is TaskState.DONE_PARTIAL

    # X2：T1 的 DONE_PARTIAL 必须满足前驱 → T2 已准入并进入 ready 队列
    t2 = next((a for a in h.acbs.values()
               if a.run_id == "RDP" and a.task_id == "T2"), None)
    assert t2 is not None and t2.state is TaskState.ADMITTED

    # 真被调度并执行（不是只 push 了队列）
    h._drain_scheduler()
    assert t2.state is TaskState.DONE
    assert plan["tasks"]["T2"]["state"] is TaskState.DONE
    h.budget.check_invariants()


# ================================================= C5 跨进程恢复后的能力链
def test_s13_c5_restored_child_keeps_lineage_and_can_spawn_grandchild(tmp_path):
    """C5：恢复后子 token 可追溯（父链按需加载）→ 能 DONE 且能 spawn 孙辈。"""
    root = tmp_path / "cp"
    wal = str(tmp_path / "wal.log")
    log = WalEventLog(wal)
    h1 = Harness(auth_secret="s13-secret", event_log=log,
                 checkpoint_root=str(root), run_limits=100, spawn_depth=2)
    plan, parent = _parent_plan(h1, "S5", min_budget=2, child_alloc=20)
    tok = h1.policy.mint(parent.id, tools={"read"}, paths={"/src"},
                         network={"api.example.com"}, data_class="internal")
    parent.capability = tok
    child = h1.spawn_child(parent, {
        "task_id": "C", "min_budget": 2,
        "scope": Scope(tools={"read"}, paths={"/src"}, data_class="internal"),
    }, plan)
    cref = h1.checkpoints.latest_ref_for_acb(child.id)
    assert cref is not None
    log.close()

    log2 = WalEventLog(wal)
    h2 = Harness(auth_secret="s13-secret", event_log=log2,
                 checkpoint_root=str(root), run_limits=100, spawn_depth=2)
    restored = h2.recover(cref)
    assert restored.state is not TaskState.QUARANTINED
    # `_restore_capability` 注册了子 token + 按需加载父链
    assert h2.policy.resolve_token(restored.capability) is not None
    assert h2.policy.check_capability(
        restored.id, restored.capability, "read", "") is True
    assert h2.policy.check_capability(
        restored.id, restored.capability, "write", "") is False

    # 恢复后仍可 spawn 孙辈（父 token 已注册、child_alloc 由快照恢复）
    grand = h2.spawn_child(restored, {"task_id": "G", "min_budget": 1}, None)
    assert grand.state is TaskState.ADMITTED
    assert h2.policy.resolve_token(grand.capability) is not None

    h2.run_task(restored, [decision(1, finish())])
    assert restored.state is TaskState.DONE
