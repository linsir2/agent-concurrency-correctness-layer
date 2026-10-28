"""S20：K-94~K-96 + 阶段三 MVP（`docs/s20-tenant-metering-and-k94.md` §2）。"""

from __future__ import annotations

import itertools
import os

import pytest
from conftest import admit, decision, finish, make_harness, seed

from harness import EventKind, Status
from harness.budget import BudgetManager

try:
    import pysyncobj  # noqa: F401
    _HAS_RAFT = True
except ImportError:                                   # pragma: no cover
    _HAS_RAFT = False

if _HAS_RAFT:
    from harness.storage.raft import (
        RaftBudgetManager,
        RaftCluster,
        RaftUnavailable,
        _ReplicatedLog,
    )

_SEQ = itertools.count(0)
_REGISTERED: list[str] = []


def _base() -> int:
    return 31600 + (os.getpid() % 200) * 20 + next(_SEQ) * 12


def _require_raft() -> None:
    if _HAS_RAFT:
        return
    reason = "raft 环境缺失：pysyncobj 未安装（pip install .[raft]）"
    if reason not in _REGISTERED:
        _REGISTERED.append(reason)
        print(f"\n[S20 raft 环境登记] {reason}")
    pytest.xfail(reason)


# ============================================================ S20-1
def test_s20_1_prune_failsafe_without_live_rids():
    """**K-95**：不传 `live_rids` → **拒绝剪枝**（默认安全，不是默认放行）。"""
    bm = BudgetManager(event_log=None, tombstone_ttl_s=0)
    bm.create_node("p", 100, "run")
    r = bm.reserve("p", 5)
    bm.release(r.reservation_id, now=1000)

    assert bm.prune(now=2000) == 0, "缺 live_rids 必须不剪（fail-safe）"
    assert r.reservation_id in bm._reservations
    assert bm._prune_refused == 1
    # 显式传空集合才真的剪
    assert bm.prune(now=2000, live_rids=set()) == 1


# ============================================================ S20-2/3（K-94）
def test_s20_2_claims_pruned_for_dead_acbs():
    """**K-94**：终态 ACB 的 claim 被转成墓碑并移出 `claims`（有界）。"""
    _require_raft()
    from harness.types import ACB

    bm = RaftBudgetManager(base_port=_base())
    try:
        for n in bm._cluster.nodes:
            n.bm._tombstone_ttl_s = 0
        bm.create_node("p", 100, "run")
        r = bm.reserve("p", 5)
        live = ACB(id="A-live", run_id="R", task_id="T1")
        live.claim_epoch = 7
        dead = ACB(id="A-dead", run_id="R", task_id="T2")
        dead.claim_epoch = 9

        bm.consume(r.reservation_id, 1, acb=live)
        bm.consume(r.reservation_id, 1, acb=dead)
        assert set(bm._replica.claims) == {"A-live", "A-dead"}

        bm.prune(now=2000, live_rids=set(), live_acb_ids={"A-live"})
        assert "A-live" in bm._replica.claims, "活 ACB 的 claim 不得删"
        assert "A-dead" not in bm._replica.claims, "终态 ACB 的 claim 应被剪"
        assert bm._replica.claim_tombstones.get("A-dead") == 9, "必须留墓碑"
    finally:
        bm.close()


def test_s20_3_dead_worker_still_fenced_after_prune():
    """**K-94 负对照**：剪掉 claims 后，旧 worker 仍被**墓碑**拒绝（不复活）。"""
    _require_raft()
    from harness.types import ACB

    bm = RaftBudgetManager(base_port=_base())
    try:
        for n in bm._cluster.nodes:
            n.bm._tombstone_ttl_s = 0
        bm.create_node("p", 100, "run")
        r = bm.reserve("p", 10)

        hi = ACB(id="A-1", run_id="R", task_id="T1")
        hi.claim_epoch = 9
        assert bm.consume(r.reservation_id, 1, acb=hi) == 1

        # 把该 ACB 判终态并剪掉其 claim（转墓碑）
        bm.prune(now=2000, live_rids=set(), live_acb_ids=set())
        assert "A-1" not in bm._replica.claims
        assert bm._replica.claim_tombstones.get("A-1") == 9

        stale = ACB(id="A-1", run_id="R", task_id="T1")
        stale.claim_epoch = 3
        assert bm.consume(r.reservation_id, 1, acb=stale) == 0, (
            "墓碑必须继续 fence 旧 worker（裸删会让 fencing 被抹掉）")
    finally:
        bm.close()


def test_s20_3_tombstones_are_bounded():
    """墓碑**有界**（≤ `claim_tombstone_max`）。"""
    _require_raft()
    from harness.types import ACB

    bm = RaftBudgetManager(base_port=_base())
    try:
        for n in bm._cluster.nodes:
            n.bm._tombstone_ttl_s = 0
            n.claim_tombstone_max = 3
        bm.create_node("p", 1000, "run")
        r = bm.reserve("p", 100)
        for i in range(6):
            a = ACB(id=f"A{i}", run_id="R", task_id="T")
            a.claim_epoch = i + 1
            bm.consume(r.reservation_id, 1, acb=a)
        bm.prune(now=2000, live_rids=set(), live_acb_ids=set())
        assert len(bm._replica.claims) == 0
        assert len(bm._replica.claim_tombstones) <= 3, (
            f"墓碑未按上限挤出：{len(bm._replica.claim_tombstones)}")
    finally:
        bm.close()


# ============================================================ S20-4（K-96）
def test_s20_4_verify_connected_after_ready():
    """**K-96**：就绪后复核 peer 连通性（3 节点全通 → 不抛）。"""
    _require_raft()
    cluster = RaftCluster(_ReplicatedLog, base_port=_base())
    try:
        assert cluster.wait_ready(20.0)
        cluster.verify_connected()          # 不应抛
    finally:
        cluster.close()


def test_s20_4_verify_connected_reports_bad_peer(monkeypatch):
    """负对照：有 peer 未连通 → 抛错并**列出该 peer**。"""
    _require_raft()
    cluster = RaftCluster(_ReplicatedLog, base_port=_base())
    try:
        assert cluster.wait_ready(20.0)
        lead = cluster.leader()
        # `isNodeConnected(node)` 收的是 Node 对象
        monkeypatch.setattr(lead, "isNodeConnected", lambda node: False,
                            raising=False)
        with pytest.raises(RaftUnavailable) as ei:
            cluster.verify_connected()
        assert "未连通" in str(ei.value)
    finally:
        cluster.close()


# ============================================================ S20-5/6 多租户
def test_s20_5_tenant_quota_isolation():
    """**多租户配额**：两 tenant 各 10，A 用满后 B 仍可预留（互不侵占）。"""
    h = make_harness(run_limits=1000)
    ta = h.ensure_tenant("acme", 10)
    tb = h.ensure_tenant("globex", 10)
    assert ta is not None and tb is not None
    assert h.ensure_tenant("acme", 10) is not None      # 幂等

    a1 = h.budget.create_node("a1", 10, "tenant:acme", tenant="acme")
    b1 = h.budget.create_node("b1", 10, "tenant:globex", tenant="globex")
    assert a1 is not None and b1 is not None

    assert h.budget.reserve("a1", 10) is not None
    assert h.budget.reserve("a1", 1) is None, "A 超配额应被拒"
    # B 完全不受 A 影响
    assert h.budget.reserve("b1", 10) is not None
    assert h.budget._nodes["tenant:globex"].subtree_reserved == 10
    h.budget.check_invariants()


def test_s20_6_tenant_quota_negative_control():
    """负对照：不建 tenant 层（直接挂 run）→ 两节点共享根配额（会互相挤压）。"""
    h = make_harness(run_limits=10)
    n1 = h.budget.create_node("n1", 10, "run")
    n2 = h.budget.create_node("n2", 10, "run")
    assert n1 is not None and n2 is not None
    assert h.budget.reserve("n1", 10) is not None
    assert h.budget.reserve("n2", 1) is None, (
        "无 tenant 层时共享根配额 → n2 被 n1 挤掉（证明 tenant 层的作用）")


# ============================================================ S20-7/8 计量
def test_s20_7_usage_report_is_idempotent():
    """**计量幂等**：同一日志重算两次结果相同，且与事件数一致。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    h.run_task(acb, [decision(1, finish())])

    u1 = h.usage_report()
    u2 = h.usage_report()
    assert u1 == u2, "同一日志两次重算必须相同（幂等）"
    actual = sum(1 for e in h.event_log.all()
                 if e.kind is EventKind.RESOURCE_COMMITTED)
    assert u1["actions"] == actual
    assert u1["runs"] >= 1


def test_s20_8_usage_report_by_tenant():
    """按 tenant 归集：只统计挂在该 tenant 节点下的预留事件。"""
    h = make_harness(run_limits=1000)
    h.ensure_tenant("acme", 100)
    n = h.budget.create_node("n1", 100, "tenant:acme", tenant="acme")
    assert n is not None
    h.budget.reserve("n1", 5)
    h.budget.reserve("n1", 5)

    r = h.usage_report(tenant="acme")
    assert r["tenant"] == "acme"
    assert r["reservations"] == 2, f"应按 tenant 归集，实际 {r}"
    # 未打 tenant 的不计入
    assert h.usage_report(tenant="other")["reservations"] == 0


# ============================================================ S20-9 统一 Admin
def test_s20_9_submit_command_routes_all_admin_ops():
    """**统一 Admin 面**：4 条命令都真路由，且幂等。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    h.run_task(acb, [decision(1, finish())])

    # PRUNE：幂等
    assert h.run_coordinator.submit_command({"kind": "PRUNE"}) is Status.OK
    assert h.run_coordinator.submit_command({"kind": "PRUNE"}) is Status.OK

    # KILL_RUN：幂等（已全终态 → 无操作但 OK）
    assert h.run_coordinator.submit_command(
        {"kind": "KILL_RUN", "run_id": "R1"}) is Status.OK
    assert h.run_coordinator.submit_command(
        {"kind": "KILL_RUN", "run_id": "R1"}) is Status.OK

    # PURGE_RUN：全终态 → OK
    assert h.run_coordinator.submit_command(
        {"kind": "PURGE_RUN", "run_id": "R1"}) is Status.OK
    # 缺参 → INVALID_INPUT
    assert h.run_coordinator.submit_command(
        {"kind": "PURGE_RUN", "run_id": ""}) is Status.INVALID_INPUT
    # DLQ_REDRIVE：找不到 ACB → INVALID_INPUT（不静默 OK）
    assert h.run_coordinator.submit_command(
        {"kind": "DLQ_REDRIVE", "acb_id": "nope"}) is Status.INVALID_INPUT
