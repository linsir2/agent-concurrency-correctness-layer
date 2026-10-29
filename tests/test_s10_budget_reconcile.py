"""S10a：跨进程预算对账（`docs/s10-cross-process-reconciliation.md` v1.1 §2/§5）。

| 用例 | 判据 |
|---|---|
| S10-1 | **跨进程不超发**：前提断言兄弟摘要存在 + P1 双 ACB 各自 checkpoint → 关 WAL → P2 从磁盘 recover → `root.subtree_reserved == 600` → 再 reserve(700) 被拒；**负对照**：禁用事件重放 → 必超发 |
| S10-2 | `run_limits` 漂移 → `QUARANTINED`；一致 → 正常 |
| S10-3 | **重放锚定事件身份**：①事件层（`event_id` 全在 `_applied_event_ids`；`payload.consumed == resv.consumed`）②节点层（节点数 + 每节点 self/subtree/consumed/folded 逐一相等）③全局等式 |
| S10-4 | **树完整性（真实路径）**：用 **epoch 过期**构造缺失兄弟（走 `_restore_budget` 的 epoch 过滤，**不删文件**）→ `needs_readmit`；**负对照**：完整树不抛且 `declared == rebuilt` |
| S10-5 | 全局对账等式 3 条 |
| S10-9 | 预算事件 `notify=False` → `_event_count` 不受影响 → 现有 gc 用例仍绿 |
"""

from __future__ import annotations

import threading

import pytest
from conftest import admit, make_harness, seed

from harness import (
    ACB,
    Event,
    EventKind,
    Harness,
    ManualClock,
    Status,
    TaskState,
    Write,
)
from harness.budget import BudgetManager, BudgetNode, InvariantError
from harness.checkpoint import FileCheckpointStore
from harness.event_log import WalEventLog
from harness.orchestration import SlotTracker


def _two_acb_plan(h: Harness, run_id: str, min_budget: int = 300) -> dict:
    """同 `parent_budget=run` 的两个 ACB（各 min_budget）。"""
    plan = h.planner.plan(run_id, [
        {"task_id": "T1", "min_budget": min_budget, "parent_budget": "run"},
        {"task_id": "T2", "min_budget": min_budget, "parent_budget": "run"},
    ])
    h.plans[run_id] = plan
    return plan


def _budget_event_kinds() -> set:
    return {
        EventKind.BUDGET_NODE_CREATED, EventKind.BUDGET_RESERVED,
        EventKind.BUDGET_CONSUMED, EventKind.BUDGET_RELEASED,
        EventKind.BUDGET_SETTLED, EventKind.BUDGET_ARCHIVED,
        EventKind.BUDGET_EPOCH,
    }


# ================================================================ S10-1
def test_s10_1_cross_process_no_overissue(tmp_path, monkeypatch):
    """S10-1：跨进程兄弟可见 → 不超发；负对照（禁用重放）→ 必超发。"""
    root = tmp_path / "cp"
    wal = str(tmp_path / "wal.log")
    log = WalEventLog(wal)
    h1 = Harness(run_limits=1000, event_log=log, checkpoint_root=str(root))
    plan = _two_acb_plan(h1, "R")
    a1 = admit(h1, "R", plan, "T1")
    a2 = admit(h1, "R", plan, "T2")
    assert a1.state is TaskState.ADMITTED and a2.state is TaskState.ADMITTED
    n1, n2 = a1.budget_node.node_id, a2.budget_node.node_id
    assert h1.budget.root.subtree_reserved == 600

    ref1 = h1.checkpoints.save(a1)
    ref2 = h1.checkpoints.save(a2)
    log.close()                                   # 关闭 WAL（P2 重开）

    # ---- 前提断言：每个 payload 的 budget_siblings 含另一兄弟（否则测错对象）----
    p1 = h1.checkpoints.load_strict(ref1)
    p2 = h1.checkpoints.load_strict(ref2)
    assert n2 in p1["budget_siblings"], \
        f"ACB1 的 payload 未含兄弟 {n2}（测错对象）：{p1['budget_siblings']}"
    assert n1 in p2["budget_siblings"], \
        f"ACB2 的 payload 未含兄弟 {n1}（测错对象）：{p2['budget_siblings']}"

    # ---- P2：新 Harness 同 root，从磁盘取 ref → recover ----
    log2 = WalEventLog(wal)
    h2 = Harness(run_limits=1000, event_log=log2, checkpoint_root=str(root))
    disk_ref = h2.checkpoints.latest_ref_for_acb(a1.id)
    assert disk_ref == ref1, "latest_ref_for_acb 未从磁盘取到 P1 的 ref"
    rec = h2.recover(disk_ref)
    assert rec.budget_node is not None
    # 兄弟可见：重放后整棵树 = 600（P1 + P2 各 300）
    assert h2.budget.root.subtree_reserved == 600, \
        f"跨进程兄弟不可见 → subtree={h2.budget.root.subtree_reserved}（应 600）"
    assert h2.budget.reserve("run", 700) is None, "600 + 700 > 1000 必须被拒（超发）"

    # ---- 负对照：禁用事件重放 → 必超发（证明测试有牙）----
    log3 = WalEventLog(wal)
    h3 = Harness(run_limits=1000, event_log=log3, checkpoint_root=str(root))

    def _no_replay(cutoff):
        return True, []

    monkeypatch.setattr(h3, "_replay_budget_events", _no_replay)
    # 补丁生效断言（参照 test_ms2 的 `assert BudgetManager.reserve is broken`）：
    # 若方法被重命名，patch 会静默失效 → 负对照恒绿、测试无牙。
    assert h3._replay_budget_events is _no_replay, \
        "补丁未生效：`_replay_budget_events` 可能已重命名"
    h3.recover(disk_ref)
    assert h3.budget.root.subtree_reserved == 300, \
        "禁用重放后应只剩本 ACB 的 300（负对照前提）"
    assert h3.budget.reserve("run", 700) is not None, \
        "负对照失效：禁用重放后竟未超发（测试无牙）"
    assert h3.budget.root.subtree_reserved == 1000


# ================================================================ S10-2
def test_s10_2_run_limits_drift(tmp_path):
    """S10-2：`run_limits` 漂移 → QUARANTINED；一致 → 正常。"""
    root = tmp_path / "cp"
    h1 = Harness(run_limits=1000, checkpoint_root=str(root))
    plan = h1.planner.plan("R", [{"task_id": "T1", "min_budget": 10}])
    h1.plans["R"] = plan
    a1 = admit(h1, "R", plan, "T1")
    ref = h1.checkpoints.save(a1)

    # ① 一致 → 正常恢复
    h2 = Harness(run_limits=1000, checkpoint_root=str(root))
    rec = h2.recover(ref)
    assert rec.state is TaskState.ADMITTED
    assert rec.needs_readmit is False
    assert rec.failure != "RUN_LIMITS_DRIFT"

    # ② 漂移 → QUARANTINED + needs_readmit
    h3 = Harness(run_limits=2000, checkpoint_root=str(root))
    rec3 = h3.recover(ref)
    assert rec3.state is TaskState.QUARANTINED
    assert rec3.failure == "RUN_LIMITS_DRIFT"
    assert rec3.needs_readmit is True
    assert rec3.budget_node is None and rec3.reservation_ids == []

    # ③ 漂移信号不得被 requeue_readmit 静默抹除（否则上限被静默改写）
    again = h3.requeue_readmit(rec3)
    assert again is rec3
    assert again.state is TaskState.QUARANTINED
    assert again.failure == "RUN_LIMITS_DRIFT"
    assert again.needs_readmit is True


# ================================================================ S10-3
def test_s10_3_replay_anchors_event_identity(tmp_path):
    """S10-3：重放锚定**事件身份**（事件层 + 节点层 + 全局等式）。"""
    root = tmp_path / "cp"
    wal = str(tmp_path / "wal.log")
    log = WalEventLog(wal)
    h1 = Harness(run_limits=1000, event_log=log, checkpoint_root=str(root))
    plan = _two_acb_plan(h1, "R")
    a1 = admit(h1, "R", plan, "T1")
    a2 = admit(h1, "R", plan, "T2")
    assert h1.budget.consume(a1.reservation_ids[0], 40) == Status.OK
    assert h1.budget.consume(a2.reservation_ids[0], 10) == Status.OK
    ref1 = h1.checkpoints.save(a1)
    h1.checkpoints.save(a2)
    cursor = h1.checkpoints.load_strict(ref1)["budget_cursor"]
    # checkpoint 之后**新增**预算事件（验证 event_id 去重集合跨进程生效）
    assert h1.budget.consume(a1.reservation_ids[0], 5) == Status.OK
    log.close()

    log2 = WalEventLog(wal)
    h2 = Harness(run_limits=1000, event_log=log2, checkpoint_root=str(root))
    h2.recover(h2.checkpoints.latest_ref_for_acb(a1.id))

    # ① 事件层：cursor 之后新增事件的 event_id 全在 _applied_event_ids
    after = [e for e in h2.budget.budget_events() if e.seq > cursor]
    assert after, "checkpoint 之后未新增预算事件（测试前提不成立）"
    for ev in after:
        assert ev.event_id in h2._applied_event_ids, \
            f"事件 {ev.event_id}（seq={ev.seq}）未入去重集合 → 重放会双份"
    # 每个 rid 能在重建树找到；其 consumed 等于**最后一条** CONSUMED 事件，
    # 且逐条单调递增（set 语义下不会回退）
    last_consumed: dict[str, int] = {}
    for ev in h2.budget.budget_events():
        if ev.kind != EventKind.BUDGET_CONSUMED:
            continue
        rid = ev.payload["rid"]
        assert rid in h2.budget._reservations, f"rid {rid} 未在重建树找到"
        prev = last_consumed.get(rid)
        assert prev is None or ev.payload["consumed"] > prev, "consumed 必须单调"
        last_consumed[rid] = ev.payload["consumed"]
    assert last_consumed, "无 CONSUMED 事件（测试前提不成立）"
    for rid, consumed in last_consumed.items():
        assert consumed == h2.budget._reservations[rid][1].consumed, \
            f"rid {rid} 重放后 consumed 不等（事件 vs 树）"

    # ② 节点层：节点数 + 每节点 self/subtree/consumed/folded 逐一相等
    assert set(h2.budget._nodes) == set(h1.budget._nodes), \
        f"节点集不一致：{set(h2.budget._nodes)} vs {set(h1.budget._nodes)}"
    for nid, n1 in h1.budget._nodes.items():
        n2 = h2.budget._nodes[nid]
        assert (n2.self_reserved, n2.subtree_reserved, n2.consumed,
                n2.folded_consumed) == (
            n1.self_reserved, n1.subtree_reserved, n1.consumed,
            n1.folded_consumed), f"节点 {nid} 重放后不等价"

    # ③ 全局等式
    h2.budget.check_invariants(cross_process=True)


# ================================================================ S10-4
def test_s10_4_tree_integrity_epoch_expiry(tmp_path):
    """S10-4：用 **epoch 过期**构造缺失兄弟（真实路径，不删文件）→ needs_readmit。"""
    root = tmp_path / "cp"
    wal = str(tmp_path / "wal.log")
    log = WalEventLog(wal)
    h1 = Harness(run_limits=1000, event_log=log, checkpoint_root=str(root))
    plan = _two_acb_plan(h1, "R")
    a1 = admit(h1, "R", plan, "T1")               # epoch 0
    # epoch 过期：bump → 旧 epoch 的账被 fence（真实路径，不删任何文件）
    h1.bump_epoch("lease_expired")
    a2 = admit(h1, "R", plan, "T2")               # epoch 1
    h1.checkpoints.save(a2)
    log.close()

    log2 = WalEventLog(wal)
    h2 = Harness(run_limits=1000, event_log=log2, checkpoint_root=str(root))
    rec = h2.recover(h2.checkpoints.latest_ref_for_acb(a2.id))
    # 缺席兄弟（epoch 0，已 fence）→ 不重建 + needs_readmit
    assert a1.budget_node.node_id not in h2.budget._nodes, \
        "epoch 过期的兄弟不应被重建"
    assert rec.needs_readmit is True, "树不完整必须 needs_readmit"
    h2.budget.check_invariants(cross_process=True)    # 合法缺席 → 不抛

    # ---- 负对照：完整树必须不抛且 declared == rebuilt ----
    root_b = tmp_path / "cp_b"
    wal_b = str(tmp_path / "wal_b.log")
    logb = WalEventLog(wal_b)
    hb = Harness(run_limits=1000, event_log=logb, checkpoint_root=str(root_b))
    planb = _two_acb_plan(hb, "RB")
    b1 = admit(hb, "RB", planb, "T1")
    admit(hb, "RB", planb, "T2")
    hb.checkpoints.save(b1)
    logb.close()

    logb2 = WalEventLog(wal_b)
    hb2 = Harness(run_limits=1000, event_log=logb2, checkpoint_root=str(root_b))
    recb = hb2.recover(hb2.checkpoints.latest_ref_for_acb(b1.id))
    assert recb.needs_readmit is False, "完整树不应 needs_readmit"
    assert hb2._declared_budget_nodes() == (set(hb2.budget._nodes) - {"run"}), \
        "完整树必须 declared == rebuilt"
    hb2.budget.check_invariants(cross_process=True)


# ================================================================ S10-5
def test_s10_5_global_reconciliation_equations(tmp_path):
    """S10-5：重放后 3 条全局对账等式成立（嵌套树 + 消费 + 结算）。"""
    root = tmp_path / "cp"
    wal = str(tmp_path / "wal.log")
    log = WalEventLog(wal)
    h1 = Harness(run_limits=1000, event_log=log, checkpoint_root=str(root))
    h1.budget.create_node("P", 500)
    h1.budget.create_node("C1", 200, parent_id="P")
    h1.budget.create_node("C2", 200, parent_id="P")
    r1 = h1.budget.reserve("C1", 120)
    r2 = h1.budget.reserve("C2", 80)
    assert h1.budget.consume(r1.reservation_id, 30) == Status.OK
    assert h1.budget.settle(r2.reservation_id) == 80
    acb = ACB(id="A-S105", run_id="R", task_id="T1")
    acb.budget_node = h1.budget._nodes["C1"]
    acb.reservation_ids = [r1.reservation_id]
    h1.checkpoints.save(acb)
    log.close()

    log2 = WalEventLog(wal)
    h2 = Harness(run_limits=1000, event_log=log2, checkpoint_root=str(root))
    h2.recover(h2.checkpoints.latest_ref_for_acb("A-S105"))
    h2.budget.check_invariants(cross_process=True)

    nodes = h2.budget._nodes
    existing: set[str] = set()
    for n in nodes.values():
        existing.update(n.reservations)

    # E1：root.subtree_reserved == Σ self_reserved
    assert h2.budget.root.subtree_reserved == sum(
        n.self_reserved for n in nodes.values())

    # E2：Σ consumed == Σ 现有节点预留 consumed + Σ folded
    ledger_consumed = sum(h2.budget._reservations[r][1].consumed for r in existing)
    assert sum(n.consumed for n in nodes.values()) == (
        ledger_consumed + sum(n.folded_consumed for n in nodes.values()))

    # E3：Σ ACTIVE amount == Σ self_reserved − Σ 非ACTIVE.consumed − Σ folded
    active_amount = sum(h2.budget._reservations[r][1].amount for r in existing
                        if h2.budget._reservations[r][1].state == "ACTIVE")
    nonactive_consumed = sum(h2.budget._reservations[r][1].consumed for r in existing
                             if h2.budget._reservations[r][1].state != "ACTIVE")
    assert active_amount == (
        sum(n.self_reserved for n in nodes.values())
        - nonactive_consumed - sum(n.folded_consumed for n in nodes.values()))


# ================================================================ S10-9
def test_s10_9_budget_events_do_not_trigger_gc():
    """S10-9：预算事件 `notify=False` → `_event_count` 不受影响 → gc 用例仍绿。"""
    h = make_harness(gc_every=3, tombstone_ttl_s=0.0)
    h.budget.create_node("B", 100)
    resv = h.budget.reserve("B", 10)
    assert h.budget.consume(resv.reservation_id, 5) == Status.OK
    assert h.budget.settle(resv.reservation_id) == 5
    assert h.budget.archive_node("B") is True
    rid = resv.reservation_id
    assert h.budget._reservations[rid][1].state == "ARCHIVED"

    # 正对照：预算事件确实落到了 EventLog（否则本用例恒真、无意义）
    kinds = _budget_event_kinds()
    assert [e for e in h.event_log.all() if e.kind in kinds], \
        "预算事件未落 EventLog（notify=False 之外还须 append）"
    # 预算事件全部 notify=False → `_event_count` 不受影响
    assert h.state.event_count == 0, \
        f"预算事件触发了订阅者：_event_count={h.state.event_count}（应 0）"

    # 现有 gc 用例语义不变：第 3 个**普通**事件才触发 gc
    h.event_log.append(EventKind.TASK_COMPLETED, "x1", {})
    h.event_log.append(EventKind.TASK_COMPLETED, "x2", {})
    assert rid in h.budget._reservations              # 第 2 个事件不触发
    h.event_log.append(EventKind.TASK_COMPLETED, "x3", {})
    assert rid not in h.budget._reservations          # 第 3 个事件触发 gc


# ================================================================ 单写者租约 / epoch RMW
def test_s10_writer_lease_takeover_and_epoch_rmw(tmp_path):
    """S10a §2.5/§3.4：单写者租约（过期才可接管）+ epoch 同一 flock 内 RMW。"""
    clock = ManualClock()
    root = tmp_path / "cp"
    h1 = Harness(run_limits=1000, checkpoint_root=str(root), lease_ms=1000,
                 clock=clock)
    assert h1._try_acquire_writer_lease() is True
    assert h1._renew_writer_lease() is True

    # 第二个进程：旧租约仍有效 → 不能接管（避免双写者）
    # 用不同 writer_id 模拟**另一个写者进程**（同进程多 Harness 共享 writer 身份）
    h2 = Harness(run_limits=1000, checkpoint_root=str(root), lease_ms=1000,
                 clock=clock, writer_id="harness-other-process")
    assert h2._try_acquire_writer_lease() is False

    clock.advance(2000)                       # 租约过期
    assert h2._try_acquire_writer_lease() is True     # 接管（含 bump_epoch + 落盘）
    assert h2._renew_writer_lease() is True

    # epoch RMW：同一 flock 内 read-modify-write（跨进程读到同一单调值）
    e0 = h2.checkpoints.read_epoch()
    assert h2.checkpoints.bump_epoch() == e0 + 1
    assert h2.checkpoints.read_epoch() == e0 + 1
    assert h2._restore_epoch() == e0 + 1
    assert h2.intents.get_epoch() == e0 + 1
    assert h2.budget._epoch == e0 + 1
    # epoch 单调：不允许回绕
    assert h2.intents.set_epoch(0) == e0 + 1


def test_s10_slot_tracker_shared_storage(tmp_path):
    """S10a §2.5：SlotTracker 下沉共享存储 → 槽位与预算不各超各的。"""
    root = tmp_path / "cp"
    h1 = Harness(run_limits=1000, max_concurrent=1, checkpoint_root=str(root))
    assert h1.slots.try_acquire("A") is True
    # 第二个进程看到共享槽位 → 不会各自超发
    h2 = Harness(run_limits=1000, max_concurrent=1, checkpoint_root=str(root))
    assert h2.slots.try_acquire("B") is False
    assert h2.slots.active_keys() == ["A"]
    assert h1.slots.active == 1
    # 释放后另一进程可获取
    h1.slots.release("A")
    assert h2.slots.active_keys() == []
    assert h2.slots.try_acquire("B") is True
    assert h1.slots.active_keys() == ["B"]


# ================================================================ 事件溯源单测
def test_s10_budget_event_payload_shape(tmp_path):
    """预算事件 payload 必含 node_id/rid/amount/consumed/state/limits/parent_id/epoch。"""
    root = tmp_path / "cp"
    h = Harness(run_limits=1000, checkpoint_root=str(root))
    h.budget.create_node("B", 100)
    resv = h.budget.reserve("B", 10)
    assert h.budget.consume(resv.reservation_id, 3) == Status.OK
    kinds = _budget_event_kinds()
    events = [e for e in h.event_log.all() if e.kind in kinds]
    assert {EventKind.BUDGET_NODE_CREATED, EventKind.BUDGET_RESERVED,
            EventKind.BUDGET_CONSUMED} <= {e.kind for e in events}
    reserved = next(e for e in events if e.kind == EventKind.BUDGET_RESERVED)
    for key in ("node_id", "rid", "amount", "consumed", "state", "limits",
                "parent_id", "budget_epoch"):
        assert key in reserved.payload, f"BUDGET_RESERVED payload 缺字段 {key}"
    assert reserved.payload["node_id"] == "B"
    assert reserved.payload["rid"] == resv.reservation_id
    assert reserved.payload["amount"] == 10
    assert reserved.payload["parent_id"] == "run"
    created = next(e for e in events if e.kind == EventKind.BUDGET_NODE_CREATED)
    assert created.payload["limits"] == 100


def test_s10_restore_budget_old_dict_branch_unchanged():
    """回归防护 2：`_restore_budget` 兼容手构 dict（无新字段 → 旧分支）。"""
    h = make_harness(run_limits=100)
    data = {
        "budget_node_id": "B-X", "budget_parent_id": "run", "budget_limits": 10,
        "budget_self": 6, "budget_subtree": 6, "budget_consumed": 2,
        "reservation_ids": ["rid-x"],
    }
    acb = ACB(id="A-S10-RB", run_id="R", task_id="T1")
    h.recovery.restore_budget(acb, data)
    assert h.budget._nodes["B-X"].limits == 10
    assert acb.reservation_ids == ["rid-x"]


# ================================================================ 生产路径租约
def test_s10_writer_lease_blocks_commit_and_recover(tmp_path):
    """单写者租约在**生产路径**生效：他人持有时 commit/recover 拒绝服务。"""
    clock = ManualClock()
    root = tmp_path / "cp"
    h1 = Harness(run_limits=1000, checkpoint_root=str(root), lease_ms=1000,
                 clock=clock, writer_id="writer-1")
    seed(h1, "r", {"v": 0})
    acb1 = ACB(id="A-LEASE", run_id="R", task_id="T1")
    ref = h1.checkpoints.save(acb1)

    h2 = Harness(run_limits=1000, checkpoint_root=str(root), lease_ms=1000,
                 clock=clock, writer_id="writer-2")
    assert h2._writer_lease_ok is False, "构造期 acquire 失败必须被记录"
    st = h2.commit_batch(ACB(id="A-L2", run_id="R", task_id="T2"),
                         [Write("r", content={"v": 1})])
    assert st == Status.UPSTREAM_FAILED
    assert st.detail.get("reason") == "writer_lease_held"
    rec = h2.recover(ref)
    assert rec.state is TaskState.QUARANTINED
    assert rec.failure == "WRITER_LEASE_HELD"
    assert rec.needs_readmit is True

    clock.advance(2000)                       # 旧租约过期 → 可接管
    assert h2._ensure_writer_lease() is True
    st2 = h2.commit_batch(ACB(id="A-L3", run_id="R", task_id="T3"),
                          [Write("r", content={"v": 2})])
    assert st2 == Status.OK, "接管后写入口必须恢复服务"


# ================================================================ 预算写 fencing
def test_s10_stale_budget_epoch_writes_rejected(tmp_path):
    """慢写者（旧 budget_epoch）被接管后，预算写操作必须被 fence。

    S11b K-46：fencing epoch 绑 WAL（`wal.log.epoch`）——两实例共享同一 WAL、
    不同 checkpoint root；接管者 bump 后，慢写者的预算写必须被拒。
    """
    wal_path = str(tmp_path / "wal.log")
    log1 = WalEventLog(wal_path)
    h1 = Harness(run_limits=1000, event_log=log1,
                 checkpoint_root=str(tmp_path / "cp1"), writer_id="writer-1")
    h1.budget.create_node("B", 100)
    resv = h1.budget.reserve("B", 10)
    assert resv is not None

    log2 = WalEventLog(wal_path)
    h2 = Harness(run_limits=1000, event_log=log2,
                 checkpoint_root=str(tmp_path / "cp2"), writer_id="writer-2")
    h2.bump_epoch("takeover")                 # WAL epoch 前进，h1 本地仍为旧值

    assert h1.budget._epoch < log1._wal.read_epoch()
    assert h1.budget.reserve("B", 5) is None, "旧 epoch 不得再 reserve"
    r = h1.budget.consume(resv.reservation_id, 1)
    assert r == Status.REJECT_STALE
    assert r.detail["reason"] == "budget_epoch_fenced"
    assert h1.budget.release(resv.reservation_id) == 0
    assert h1.budget.settle(resv.reservation_id) == 0

    # 重新同步 epoch 后恢复可写（证明拒绝是 fencing，不是永久损坏）
    h1._restore_epoch()
    assert h1.budget.reserve("B", 5) is not None


# ================================================================ 槽位 CAS
def test_s10_slot_tracker_cas_atomic_across_stores(tmp_path):
    """两个独立 store（模拟两进程）并发占槽：同一 flock 内 CAS → 不击穿上限。"""
    root = tmp_path / "cp"
    stores = [FileCheckpointStore(root) for _ in range(2)]
    trackers = [SlotTracker(1, shared=s) for s in stores]
    barrier = threading.Barrier(2)
    results: list[tuple[str, bool]] = []

    def worker(tracker, key):
        barrier.wait()
        results.append((key, tracker.try_acquire(key)))

    threads = [threading.Thread(target=worker, args=(t, k))
               for t, k in zip(trackers, ["A", "B"])]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(1 for _, ok in results if ok) == 1, \
        f"max_concurrent=1 被击穿：{results}"
    assert len(stores[0].read_slots()) == 1
    assert trackers[0].active == 1


# ================================================================ 重放回滚 / 树完整性
def test_s10_replay_failure_rolls_back_built_nodes(monkeypatch):
    """重放中途失败：按 built 强制摘除（remove_node 对有预留/子返回 False 不可用）。"""
    h = Harness(run_limits=1000)
    h.budget.create_node("P", 100)
    h.budget.create_node("C", 50, parent_id="P")
    h.budget.reserve("C", 10)

    monkeypatch.setattr(BudgetManager, "apply_budget_event",
                        lambda self, ev: False)
    ok, built = h.recovery.replay_budget_events(0)
    assert ok is False and built == []
    assert set(h.budget._nodes) == {"run"}, f"回滚残留节点：{set(h.budget._nodes)}"
    assert h.budget.root.children == []
    assert h.budget.root.subtree_reserved == 0
    assert h.budget.root.self_reserved == 0


def test_s10_tree_integrity_detects_rebuilt_but_not_declared():
    """单向检查的假阴性：`rebuilt ⊃ declared` 必须失败（不只是 declared - rebuilt）。"""
    h = Harness(run_limits=100)
    h.budget.create_node("P", 100)
    h.budget._nodes["GHOST"] = BudgetNode("GHOST", 10, h.budget.root)
    with pytest.raises(InvariantError, match="not declared"):
        h.budget.check_invariants(cross_process=True)


def test_s10_tree_integrity_requires_explicit_fence_event():
    """epoch 豁免必须锚定**显式 BUDGET_EPOCH 事件**，不能只比较数值。"""
    h = Harness(run_limits=100)
    h.budget.create_node("P", 100)
    h.budget._nodes.pop("P")                  # 缺失节点
    h.budget.set_epoch(5, emit=False)         # 只抬数值、无 fence 记录
    with pytest.raises(InvariantError, match="not rebuilt"):
        h.budget.check_invariants(cross_process=True)

    h.budget.set_epoch(6, emit=True)          # 补显式 fence 记录 → 合法缺席
    h.budget.check_invariants(cross_process=True)


def test_s10_replay_duplicate_rid_mismatch_rejected():
    """同 rid 但 amount/node_id/limits 不同 → 判不一致（不得静默吞掉）。"""
    h = Harness(run_limits=100)
    h.budget.create_node("B", 100)
    resv = h.budget.reserve("B", 10)
    reserved = next(e for e in h.budget.budget_events()
                    if e.kind == EventKind.BUDGET_RESERVED)
    bad = Event(seq=reserved.seq + 1, kind=EventKind.BUDGET_RESERVED,
                subject=reserved.subject,
                payload=dict(reserved.payload, amount=99))
    assert h.budget.apply_budget_event(bad) is False
    assert h.budget._reservations[resv.reservation_id][1].amount == 10

    moved = Event(seq=reserved.seq + 2, kind=EventKind.BUDGET_RESERVED,
                  subject=reserved.subject,
                  payload=dict(reserved.payload, node_id="run"))
    assert h.budget.apply_budget_event(moved) is False


def test_s10_reset_baseline_resets_epoch_under_lock():
    """`_reset_baseline` 必须归零 epoch（否则放大树完整性豁免面）。"""
    h = Harness(run_limits=100)
    h.budget.create_node("B", 100)
    h.budget.set_epoch(7, emit=False)
    h.budget._reset_baseline()
    assert h.budget._epoch == 0
    assert set(h.budget._nodes) == {"run"}
