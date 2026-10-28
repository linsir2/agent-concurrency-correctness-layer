"""S21：K-97 租户准入 · K-99 分布式租约 · K-100 墓碑窗口 · 优先级份额。

对应 `docs/s21-tenant-admission-and-leases.md` §2。
"""

from __future__ import annotations

import itertools
import os

import pytest
from conftest import admit, decision, hitl, make_harness, seed

from harness import ManualClock, TaskState

try:
    import pysyncobj  # noqa: F401
    _HAS_RAFT = True
except ImportError:                                   # pragma: no cover
    _HAS_RAFT = False

if _HAS_RAFT:
    from harness.storage.raft import RaftBudgetManager

_SEQ = itertools.count(0)
_REGISTERED: list[str] = []


def _base() -> int:
    return 33800 + (os.getpid() % 200) * 20 + next(_SEQ) * 12


def _require_raft() -> None:
    if _HAS_RAFT:
        return
    reason = "raft 环境缺失：pysyncobj 未安装（pip install .[raft]）"
    if reason not in _REGISTERED:
        _REGISTERED.append(reason)
        print(f"\n[S21 raft 环境登记] {reason}")
    pytest.xfail(reason)


# ============================================================ S21-1/2/3
def _plan(h, run_id, task_id, **extra):
    spec = {"task_id": task_id, "min_budget": 10}
    spec.update(extra)
    return h.planner.plan(run_id, [spec])


def test_s21_1_tenant_auto_attached_to_parent_budget():
    """**K-97**：`plan["tenant"]` → 预算节点自动挂在 `tenant:<t>` 下。"""
    h = make_harness(run_limits=1000)
    h.ensure_tenant("acme", 50)

    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10,
                                  "tenant": "acme"}])
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    assert acb.state is TaskState.ADMITTED

    node = h.budget._nodes.get(acb.budget_node.node_id)
    assert node is not None
    assert node.parent.node_id == "tenant:acme", (
        f"应挂在 tenant:acme 下，实际 {node.parent.node_id}")
    # 租户用量会计到租户节点
    assert h.budget._nodes["tenant:acme"].subtree_reserved >= 10


def test_s21_2_no_tenant_keeps_legacy_parent(monkeypatch):
    """**K-97 负对照**：无 tenant → 父节点仍是 `"run"`（与现状逐字节相同）。"""
    h = make_harness(run_limits=100)
    plan = _plan(h, "R1", "T1")
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    assert acb.state is TaskState.ADMITTED
    assert acb.budget_node.parent.node_id == "run"


def test_s21_2_explicit_parent_budget_wins():
    """显式 `footprint.parent_budget` 优先于 tenant 解析。"""
    h = make_harness(run_limits=1000)
    h.ensure_tenant("acme", 50)
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10,
                                  "tenant": "acme",
                                  "parent_budget": "custom"}])
    h.plans["R1"] = plan
    h.budget.create_node("custom", 100, "run", tenant="acme")
    acb = admit(h, "R1", plan, "T1")
    assert acb.state is TaskState.ADMITTED
    assert acb.budget_node.parent.node_id == "custom"


def test_s21_3_tenant_quota_blocks_without_affecting_others():
    """**K-97 配额生效**：本租户超额被拒，**另一个租户不受影响**。"""
    h = make_harness(run_limits=1000)
    h.ensure_tenant("acme", 10)
    h.ensure_tenant("globex", 100)

    p1 = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10,
                                "tenant": "acme"}])
    h.plans["R1"] = p1
    a1 = admit(h, "R1", p1, "T1")
    assert a1.state is TaskState.ADMITTED
    assert h.budget._nodes["tenant:acme"].subtree_reserved == 10

    p2 = h.planner.plan("R2", [{"task_id": "T2", "min_budget": 10,
                                "tenant": "acme"}])
    h.plans["R2"] = p2
    a2 = admit(h, "R2", p2, "T2")
    assert a2.state is not TaskState.ADMITTED, "acme 超额必须被拒"

    # globex 完全不受影响
    p3 = h.planner.plan("R3", [{"task_id": "T3", "min_budget": 50,
                                "tenant": "globex"}])
    h.plans["R3"] = p3
    a3 = admit(h, "R3", p3, "T3")
    assert a3.state is TaskState.ADMITTED


# ============================================================ S21-4/5（K-99）
def test_s21_4_reap_expired_leases():
    """**K-99**：过期且已失效的租约被回收；`valid=True` 的不删。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    seed(h, "r", "base")
    plan = _plan(h, "R1", "T1")
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")

    # 造一个已失效（epoch bump）且过期的租约
    lease = h.intents.reserve("res1", acb.id, 0, ttl_ms=1000, mode="WRITE",
                              run_id="R1")
    assert lease is not None
    h.intents.revoke(lease)             # valid=False
    clock.advance(2000)                  # 过期

    before = len(h.intents._leases)
    assert h.intents.reap_expired(clock.now_ms()) >= 1
    assert len(h.intents._leases) < before
    # 语义不变：过期租约在回收前后都无效
    assert h.intents.is_valid(lease) is False


def test_s21_5_reap_never_deletes_valid_leases():
    """**K-99 负对照**：未过期（或 `valid=True`）的租约**绝不被回收**。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    seed(h, "r", "base")
    plan = _plan(h, "R1", "T1")
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")

    live = h.intents.reserve("res1", acb.id, 0, ttl_ms=10_000, mode="WRITE",
                             run_id="R1")
    assert live is not None
    expired_but_valid = h.intents.reserve("res2", acb.id, 0, ttl_ms=1000,
                                          mode="WRITE", run_id="R1")
    assert expired_but_valid is not None

    clock.advance(5000)                  # res2 过期但 valid=True
    assert h.intents.reap_expired(clock.now_ms()) == 0, (
        "valid=True 的租约不得被删（哪怕已过期）")
    assert h.intents.is_valid(live) is True
    assert live.lease_id in h.intents._leases


def test_s21_5_tick_triggers_reap():
    """`tick()` 会触发回收（K-99 与 aging 共用驱动面）。"""
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    seed(h, "r", "base")
    plan = _plan(h, "R1", "T1")
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    lease = h.intents.reserve("res1", acb.id, 0, ttl_ms=1000, mode="WRITE",
                              run_id="R1")
    h.intents.revoke(lease)
    clock.advance(5000)
    before = len(h.intents._leases)
    h.tick()
    assert len(h.intents._leases) < before


# ============================================================ S21-6/7（K-100）
def test_s21_6_claim_window_scales_with_lease():
    """**K-100**：窗口 = `tombstone_max × max_lease_ttl_s`，随租约时长自适应。"""
    h = make_harness(run_limits=100, lease_ms=60_000)
    assert h.max_lease_ttl_s == 60
    assert h.claim_window_s() == 1024 * 60, (
        f"窗口应为 1024×60，实际 {h.claim_window_s()}")

    h2 = make_harness(run_limits=100, lease_ms=3_600_000)   # 1 小时
    assert h2.max_lease_ttl_s == 3600
    assert h2.claim_window_s() == 1024 * 3600, "租约越长窗口必须越大"


def test_s21_7_tombstone_eviction_respects_lease_window():
    """**K-100 时点安全**：墓碑只有「入墓满 `max_lease_ttl_s`」才可被忘记。

    淘汰一个仍可能被旧 worker 使用的墓碑，会让 `claims.get` 回落到 0 →
    旧 epoch 复活。因此容量不足且无足够老的墓碑时**不淘汰**（宁可暂时超界），
    并计数 `claim_window_exceeded`；时间推进超过窗口后才允许淘汰。
    """
    _require_raft()
    from harness.types import ACB

    bm = RaftBudgetManager(base_port=_base())
    try:
        for n in bm._cluster.nodes:
            n.bm._tombstone_ttl_s = 0
            n.claim_tombstone_max = 2
        bm.create_node("p", 1000, "run")
        r = bm.reserve("p", 100)
        for i in range(4):
            a = ACB(id=f"A{i}", run_id="R", task_id="T")
            a.claim_epoch = i + 1
            bm.consume(r.reservation_id, 1, acb=a)

        # now=2000，墓碑年龄 0 < 3600 → 全部「太年轻」，不得淘汰
        bm.prune(now=2000, live_rids=set(), live_acb_ids=set(),
                 max_lease_ttl_s=3600)
        assert len(bm._replica.claim_tombstones) == 4, (
            "年轻墓碑不得被忘记（否则旧 worker 复活）")
        assert bm.claim_window_exceeded >= 1, "拒绝淘汰必须可观测"

        # 时间推进满窗口 → 墓碑可安全忘记，回落到上限
        bm.prune(now=2000 + 3600, live_rids=set(), live_acb_ids=set(),
                 max_lease_ttl_s=3600)
        assert len(bm._replica.claim_tombstones) <= 2, (
            f"满窗后应淘汰到上限，实际 {len(bm._replica.claim_tombstones)}")
    finally:
        bm.close()


# ============================================================ S21-8/9/10
def test_s21_8_tier_reserved_share():
    """**优先级份额**：premium 预留 6、standard 预留 3（quota 10）。"""
    h = make_harness(run_limits=1000)
    h.ensure_tenant("t1", 10, min_share=6)
    assert h.budget._nodes["tenant:t1"].limits == 10
    assert h.budget._nodes["tenant:t1:reserved"].limits == 6

    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 5}])
    plan["tenant"] = "t1"             # run 级租户归属
    plan["tier"] = "premium"          # tier 在 plan 层（与 tenant 一致）
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    assert acb.state is TaskState.ADMITTED
    assert acb.budget_node.parent.node_id == "tenant:t1:reserved", (
        f"premium 应挂预留节点，实际 {acb.budget_node.parent.node_id}")


def test_s21_9_oversubscribed_tier_borrows_shared_pool():
    """超额借用：未知 tier → 挂 tenant 节点（共享池）。"""
    h = make_harness(run_limits=1000)
    h.ensure_tenant("t1", 10, min_share=6)
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 5}])
    plan["tenant"] = "t1"
    plan["tier"] = "unknown-tier"
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    assert acb.state is TaskState.ADMITTED
    assert acb.budget_node.parent.node_id == "tenant:t1"


def test_s21_10_share_exceeding_quota_is_rejected():
    """**份额上限校验**：`min_share > quota` → 显式拒绝（不静默截断）。

    **失败零残留**：校验在建任何节点**之前**，故 `tenant:t1` 也不得留下。
    """
    h = make_harness(run_limits=1000)
    with pytest.raises(ValueError):
        h.ensure_tenant("t1", 10, min_share=20)
    assert "tenant:t1:reserved" not in h.budget._nodes
    assert "tenant:t1" not in h.budget._nodes, "失败不得留下半成品租户节点"


def test_s21_10_quota_conflict_is_rejected():
    """同名租户**不同 quota** → 显式拒绝（不静默沿用旧配置）。"""
    h = make_harness(run_limits=1000)
    h.ensure_tenant("t1", 10)
    assert h.budget._nodes["tenant:t1"].limits == 10
    with pytest.raises(ValueError):
        h.ensure_tenant("t1", 20)
    assert h.budget._nodes["tenant:t1"].limits == 10, "旧配置不得被静默沿用/篡改"


def test_s21_10_share_zero_is_noop():
    """`min_share=0`（默认）→ 不建预留节点（向后兼容）。"""
    h = make_harness(run_limits=1000)
    node = h.ensure_tenant("t1", 10)
    assert node is not None
    assert "tenant:t1:reserved" not in h.budget._nodes


def test_s21_12_node_tenant_matches_actual_parent():
    """`node.tenant` 必须与**实际挂载点**一致（计量归集不能错位）。

    tenant 声明了但节点不存在 → 实际挂在 `run` 下（未隔离），
    此时 `node.tenant` 必须为空，否则 `usage_report` 把未隔离用量记到该租户。
    """
    h = make_harness(run_limits=1000)
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10,
                                  "tenant": "ghost"}])
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    assert acb.state is TaskState.ADMITTED
    assert acb.budget_node.parent.node_id == "run"
    assert acb.budget_node.tenant == "", "未隔离的用量不得记到 ghost 租户"
    assert h.usage_report().get("by_tenant", {}).get("ghost", 0) == 0


def test_s21_11_tombstone_cap_is_replicated():
    """**K-100 RSM 确定性**：墓碑上限必须随命令复制，各副本淘汰结果一致。

    反例（修复前）：`prune` 读副本本地 `claim_tombstone_max` → 三副本设不同
    上限时淘汰数量不同 → 状态机发散。修复后上限作为命令参数下发。
    """
    _require_raft()
    from harness.types import ACB

    bm = RaftBudgetManager(base_port=_base())
    try:
        for i, n in enumerate(bm._cluster.nodes):
            n.bm._tombstone_ttl_s = 0
            n.claim_tombstone_max = 2 + i          # 故意制造配置漂移 2/3/4
        bm.create_node("p", 1000, "run")
        r = bm.reserve("p", 100)
        for i in range(6):
            a = ACB(id=f"A{i}", run_id="R", task_id="T")
            a.claim_epoch = i + 1
            bm.consume(r.reservation_id, 1, acb=a)
        bm.prune(now=2000, live_rids=set(), live_acb_ids=set(),
                 max_lease_ttl_s=0)

        def _converged() -> bool:
            return len({len(n.claim_tombstones) for n in bm._cluster.nodes}) == 1

        # `_bump_and_wait` 只等 leader 应用；follower 应用是异步的 → 轮询收敛
        # （不用 sleep 猜时长）。
        assert bm._cluster.wait_until(_converged, timeout=10.0), (
            f"各副本墓碑数量必须一致（否则 RSM 发散），实际 "
            f"{[len(n.claim_tombstones) for n in bm._cluster.nodes]}")
        sizes = [len(n.claim_tombstones) for n in bm._cluster.nodes]
        assert len(set(sizes)) == 1, f"墓碑数量发散：{sizes}"
        expected = int(bm._replica.claim_tombstone_max)   # leader 侧取值
        assert sizes[0] == expected, (
            f"应按命令下发的上限 {expected} 淘汰，实际 {sizes[0]}")
    finally:
        bm.close()


# ============================================================ S21-13..16（终审回归）
def test_s21_13_reap_epoch_stale_leases():
    """**终审 P1**：epoch 失效（`valid` 仍为 True）的租约也必须被回收。

    全局 `Harness.bump_epoch()` 只推进 epoch、不逐个置 `valid=False`；
    若回收器只认 `valid=False`，这些 `is_valid=False` 的租约将永不回收。
    """
    clock = ManualClock()
    h = make_harness(run_limits=100, clock=clock)
    seed(h, "r", "base")
    plan = _plan(h, "R1", "T1")
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    lease = h.intents.reserve("res1", acb.id, 0, ttl_ms=0, mode="WRITE",
                              run_id="R1")
    assert lease is not None and lease.valid
    before = len(h.intents._leases)

    h.bump_epoch()                       # 全局 epoch 推进（不改 valid）
    assert lease.valid is True, "前提：全局 bump 只推进 epoch"
    assert h.intents.is_valid(lease) is False, "epoch 不匹配 → 语义上已失效"
    assert h.intents.reap_expired() >= 1, "epoch 失效租约必须被回收"
    assert len(h.intents._leases) < before


def test_s21_14_plan_tenant_via_dispatch_and_run():
    """**终审 P1**：plan 级 tenant/tier 经「唯一生产入口」生效。

    `dispatch_and_run` 必须在 `try_admit` **之前**登记 plan，否则
    `_tenant_of`/`_tier_of` 读不到 plan 级 tenant → 静默挂到 `run`。
    """
    h = make_harness(run_limits=1000)
    h.ensure_tenant("t1", 100, min_share=50)
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 5}])
    plan["tenant"] = "t1"
    plan["tier"] = "premium"
    acb = h.dispatch_and_run(plan, "T1", [decision(1, hitl())])
    assert acb.budget_node is not None, "HITL 等待态保留预算节点"
    assert acb.budget_node.parent.node_id == "tenant:t1:reserved"


def test_s21_15_tenant_label_survives_replay():
    """**终审 P1**：预算事件重放后 `node.tenant` 不丢（计量不归零）。"""
    h = make_harness(run_limits=1000)
    h.ensure_tenant("t1", 100)
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10,
                                  "tenant": "t1"}])
    h.plans["R1"] = plan
    admit(h, "R1", plan, "T1")
    assert h.usage_report(tenant="t1")["reservations"] >= 1

    ok, _built = h._replay_budget_events(0)
    assert ok
    assert h.budget._nodes["tenant:t1"].tenant == "t1", "重放后租户标签必须保留"
    assert h.usage_report(tenant="t1")["reservations"] >= 1, (
        "重放后计量不得归零")


def test_s21_16_ensure_tenant_rejects_bad_input():
    """终审 P2：小数份额 / 含冒号租户名 / 缺失父节点 → 显式拒绝（零残留）。"""
    h = make_harness(run_limits=1000)
    with pytest.raises(ValueError):
        h.ensure_tenant("t1", 10, min_share=6.9)
    with pytest.raises(ValueError):
        h.ensure_tenant("a:b", 10)
    with pytest.raises(ValueError):
        h.ensure_tenant("t1", 10, parent="no-such-parent")
    assert "tenant:t1" not in h.budget._nodes


def test_s21_17_raft_create_node_carries_tenant():
    """**终审 P1**：Raft 后端 `create_node` 必须接受并复制 `tenant`（三副本一致）。"""
    _require_raft()
    bm = RaftBudgetManager(base_port=_base())
    try:
        node = bm.create_node("B-x", 10, "run", tenant="acme")
        assert node is not None and node.tenant == "acme"
        assert bm._cluster.wait_until(
            lambda: all(
                getattr(n.bm._nodes.get("B-x"), "tenant", None) == "acme"
                for n in bm._cluster.nodes), timeout=10.0), (
            "tenant 标签必须随共识复制到所有副本")
    finally:
        bm.close()
