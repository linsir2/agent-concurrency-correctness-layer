"""S19：K-88~K-93 收口（`docs/s19-budget-gc-and-hardening.md` §2）。

重点：K-88 的**精密三栅栏剪枝**（状态/引用/时间）+ 剪后不变量仍然成立。
"""

from __future__ import annotations

import itertools
import os

import pytest
from conftest import admit, decision, finish, make_harness, seed

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
        pick_free_base,
    )

_SEQ = itertools.count(0)
_REGISTERED: list[str] = []


def _base() -> int:
    return 33600 + (os.getpid() % 200) * 20 + next(_SEQ) * 12


def _require_raft() -> None:
    if _HAS_RAFT:
        return
    reason = "raft 环境缺失：pysyncobj 未安装（pip install .[raft]）"
    if reason not in _REGISTERED:
        _REGISTERED.append(reason)
        print(f"\n[S19 raft 环境登记] {reason}")
    pytest.xfail(reason)


def _bm(**kw) -> BudgetManager:
    return BudgetManager(event_log=None, **kw)


# ============================================================ S19-1
def test_s19_1_prune_removes_settled_after_ttl():
    """**K-88**：SETTLED 条目超过保留期且无引用 → 被剪。"""
    bm = _bm(tombstone_ttl_s=10)
    bm.create_node("p", 100, "run")
    r = bm.reserve("p", 5)
    assert r is not None
    bm.consume(r.reservation_id, 5)
    bm.settle(r.reservation_id, now=1000)
    assert r.reservation_id in bm._reservations

    # 未到 TTL：不剪
    assert bm.prune(now=1005, live_rids=set()) == 0
    # 到 TTL：剪掉
    assert bm.prune(now=1010, live_rids=set()) == 1
    assert r.reservation_id not in bm._reservations


# ============================================================ S19-2
def test_s19_2_invariants_hold_before_and_after_prune():
    """**剪枝必须折账**：剪前剪后 `check_invariants()` 都通过，`consumed` 不变。"""
    bm = _bm(tombstone_ttl_s=1)
    bm.create_node("p", 100, "run")
    r = bm.reserve("p", 10)
    assert r is not None
    bm.consume(r.reservation_id, 7)
    bm.settle(r.reservation_id, now=1000)

    node = bm._nodes["p"]
    consumed_before = node.consumed
    folded_before = node.folded_consumed
    bm.check_invariants()

    assert bm.prune(now=2000, live_rids=set()) == 1

    assert node.consumed == consumed_before, "永久账不得因剪枝变化"
    assert node.folded_consumed == folded_before + 7, (
        "被剪条目的 consumed 必须折入 folded_consumed（否则 E2/I5c 破坏）")
    bm.check_invariants()        # 剪后仍成立


# ============================================================ S19-3 三栅栏负对照
def test_s19_3_fence_state_active_is_never_pruned():
    """① 状态栅栏：ACTIVE 永不剪（还在占额度）。"""
    bm = _bm(tombstone_ttl_s=1)
    bm.create_node("p", 100, "run")
    r = bm.reserve("p", 5)
    assert r is not None
    assert bm.prune(now=10_000, live_rids=set()) == 0, "ACTIVE 不得被剪"
    assert r.reservation_id in bm._reservations


def test_s19_3_fence_live_reference_blocks_prune():
    """② 引用栅栏：仍被活 ACB 引用的条目**不剪**（即使已 SETTLED 且过期）。"""
    bm = _bm(tombstone_ttl_s=1)
    bm.create_node("p", 100, "run")
    r = bm.reserve("p", 5)
    bm.settle(r.reservation_id, now=1000)

    # 活 ACB 仍持有该 rid → 必须保留
    assert bm.prune(now=10_000, live_rids={r.reservation_id}) == 0
    assert r.reservation_id in bm._reservations
    # 引用消失后才剪
    assert bm.prune(now=10_000, live_rids=set()) == 1


def test_s19_3_fence_time_blocks_prune():
    """③ 时间栅栏：未到保留期不剪（Kafka tombstone retention 同构）。"""
    bm = _bm(tombstone_ttl_s=100)
    bm.create_node("p", 100, "run")
    r = bm.reserve("p", 5)
    bm.release(r.reservation_id, now=1000)
    assert bm.prune(now=1050, live_rids=set()) == 0, "未到 TTL 不得剪"
    assert bm.prune(now=1100, live_rids=set()) == 1


def test_s19_3_max_age_tightens_window():
    """③' `max_age_s` 只能**收紧**保留期（全局 TTL=10000 时按 5s 剪）。

    边界语义：`now - settled_at >= retention` 即剪（含等号）。
    """
    bm = _bm(tombstone_ttl_s=10_000)
    bm.create_node("p", 100, "run")
    r = bm.reserve("p", 5)
    bm.release(r.reservation_id, now=1000)       # settled_at = 1000
    # 默认 TTL：远未到 → 不剪
    assert bm.prune(now=1004, live_rids=set()) == 0
    # 收紧到 5s：age=4 <5 → 不剪
    assert bm.prune(now=1004, live_rids=set(), max_age_s=5) == 0
    # age=5 → 达到保留期 → 剪
    assert bm.prune(now=1005, live_rids=set(), max_age_s=5) == 1


# ============================================================ S19-4 有界性
def test_s19_4_reservation_table_is_bounded():
    """**有界**：循环 100 次 reserve→release→prune，条目数**不随次数增长**。"""
    bm = _bm(tombstone_ttl_s=0)          # TTL=0：立即可剪（聚焦有界性）
    bm.create_node("p", 1000, "run")
    peak = 0
    for i in range(100):
        r = bm.reserve("p", 1)
        assert r is not None
        bm.release(r.reservation_id, now=1000 + i)
        bm.prune(now=1000 + i, live_rids=set())
        peak = max(peak, len(bm._reservations))
        assert len(bm._reservations) <= 1, (
            f"第 {i} 轮后残留 {len(bm._reservations)} 条（应 ≤1）")
    assert peak <= 1
    bm.check_invariants()


def test_s19_4_without_ttl_would_grow():
    """**负对照**：TTL 很长且不 prune → 条目线性增长（证明修剪是必要的）。"""
    bm = _bm(tombstone_ttl_s=10**9)
    bm.create_node("p", 10_000, "run")
    for i in range(50):
        r = bm.reserve("p", 1)
        bm.release(r.reservation_id, now=1000)
    assert len(bm._reservations) == 50, "不修剪时必须仍残留（对照）"
    assert bm.prune(now=1000, live_rids=set()) == 0, "TTL 未到 → 剪不动"


# ============================================================ S19-7
def test_s19_7_remaining_vs_check_semantics():
    """**K-91**：`remaining()` 是剩余额度；`check()` 是 ANY 语义（两者不同）。"""
    bm = _bm()
    bm.create_node("p", 10, "run")
    r = bm.reserve("p", 6)
    assert r is not None

    assert bm.remaining("p") == 4
    # check 是「任一 ACTIVE 预留余额够付」：R1 余额 6 → check(6) True
    assert bm.check("p", 6) is True
    assert bm.check("p", 7) is False
    # 但剩余额度只有 4 —— 这正是两者不能混用的证明
    assert bm.remaining("p") < 7


def test_s19_7_harness_prune_budget_uses_live_acbs():
    """`Harness.prune_budget` 从**活 ACB** 算 `live_rids`（活引用不被剪）。"""
    h = make_harness(run_limits=100)
    h.budget._tombstone_ttl_s = 0            # 立即到期
    seed(h, "r", "base")
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    assert acb.reservation_ids, "准入后应有预留"

    # ACB 未终态 → 其 rid 属活引用 → 不剪
    assert h.prune_budget() == 0
    assert all(rid in h.budget._reservations for rid in acb.reservation_ids)


# ============================================================ S19-5/6/8/9（raft）
def test_s19_5_claim_fencing_replicated():
    """**K-89**：claim epoch 单调复制；旧 worker 的低 epoch 被拒。"""
    _require_raft()
    from harness.types import ACB

    bm = RaftBudgetManager(base_port=_base())
    try:
        bm.create_node("p", 50, "run")
        r = bm.reserve("p", 10)
        assert r is not None

        hi = ACB(id="A1", run_id="R", task_id="T1")
        hi.claim_epoch = 5
        assert bm.consume(r.reservation_id, 1, acb=hi) == 1

        stale = ACB(id="A1", run_id="R", task_id="T1")
        stale.claim_epoch = 4
        assert bm.consume(r.reservation_id, 1, acb=stale) == 0

        # 三副本的 claims 一致
        target = int(bm._replica.version)

        def converged() -> bool:
            return all(int(n.version) >= target for n in bm._cluster.nodes)

        assert bm._cluster.wait_until(converged, 20.0)
        claims = [dict(n.claims) for n in bm._cluster.nodes]
        assert claims[0] == claims[1] == claims[2] == {"A1": 5}
    finally:
        bm.close()


def test_s19_6_archive_event_carries_expires_at():
    """**K-90**：`BUDGET_ARCHIVED` 事件带 `expires_at`，重放不再取 wall clock。"""
    h = make_harness(run_limits=100)
    seed(h, "r", "base")
    plan = h.planner.plan("R1", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R1"] = plan
    acb = admit(h, "R1", plan, "T1")
    h.run_task(acb, [decision(1, finish())])

    # **自己造一次带确定 now 的归档**（`run_task` 也会归档，但用的是 wall clock，
    # 断言它等于 5000 会误判 —— 取「我们这条」而不是「最后一条」）
    bm2 = _bm(tombstone_ttl_s=10)
    bm2.event_log = h.event_log
    bm2.create_node("px", 10, "run")
    r = bm2.reserve("px", 1)
    bm2.consume(r.reservation_id, 1)
    bm2.settle(r.reservation_id, now=4000)
    assert bm2.archive_node("px", now=5000) is True

    archived = [e for e in h.event_log.all()
                if e.kind.name == "BUDGET_ARCHIVED"
                and (e.payload or {}).get("node_id") == "px"]
    assert archived, "应有 px 的 BUDGET_ARCHIVED 事件"
    payload = archived[-1].payload or {}
    assert "expires_at" in payload, f"归档事件缺 expires_at：{payload}"
    assert int(payload["expires_at"]) == 5000 + 10, (
        f"归档事件必须带**确定的** expires_at（不是 wall clock）：{payload}")


def test_s19_8_from_addrs_precheck_fails_fast():
    """**K-92**：地址不可达时**快速失败**并列出地址（不是干等 20s）。"""
    _require_raft()
    import time as _t

    port = pick_free_base(_base(), 3)
    addrs = [f"127.0.0.1:{port + i}" for i in range(3)]
    t0 = _t.monotonic()
    with pytest.raises(RaftUnavailable) as ei:
        RaftCluster.from_addrs(_ReplicatedLog, addrs=addrs, precheck=True,
                               precheck_timeout=0.2)
    elapsed = _t.monotonic() - t0
    assert elapsed < 5.0, f"预检未快速失败（{elapsed:.1f}s）"
    assert "不可达" in str(ei.value)


def test_s19_8_precheck_auto_allows_bootstrap():
    """`precheck="auto"`：全不可达 = 首次启动 → 放行（不误伤建簇）。"""
    _require_raft()
    port = pick_free_base(_base(), 3)
    addrs = [f"127.0.0.1:{port + i}" for i in range(3)]
    cluster = RaftCluster.from_addrs(_ReplicatedLog, addrs=addrs)
    try:
        assert cluster.wait_ready(20.0), "auto 模式不应挡住首次启动"
    finally:
        cluster.close()


def test_s19_9_remove_node_owner_replicated():
    """**K-93**：`remove_node` 的 owner 校验走共识（错 owner 被拒）。"""
    _require_raft()
    bm = RaftBudgetManager(base_port=_base())
    try:
        bm.create_node("p", 10, "run")
        # 未设置 owner → 空 owner 可删
        assert bm.remove_node("p") is True
        assert "p" not in bm._bm._nodes

        bm.create_node("q", 10, "run")
        bm._bm._nodes["q"].owner = "owner-a"
        assert bm.remove_node("q", owner="owner-b") is False, "错 owner 必须拒绝"
        assert "q" in bm._bm._nodes
    finally:
        bm.close()


def test_s19_raft_prune_is_replicated():
    """K-88 在 Raft 上：`prune` 的参数随命令复制 → 三副本一致。"""
    _require_raft()
    bm = RaftBudgetManager(base_port=_base())
    try:
        # 各副本统一 TTL（真实部署里本就是同一配置；这里显式对齐以免误判）
        for n in bm._cluster.nodes:
            n.bm._tombstone_ttl_s = 0
        bm.create_node("p", 100, "run")
        r = bm.reserve("p", 5)
        bm.release(r.reservation_id, now=1000)
        assert bm.prune(now=2000, live_rids=set()) == 1

        target = int(bm._replica.version)

        def converged() -> bool:
            return all(int(n.version) >= target for n in bm._cluster.nodes)

        assert bm._cluster.wait_until(converged, 20.0)
        for n in bm._cluster.nodes:
            assert r.reservation_id not in n.bm._reservations
            n.bm.check_invariants()
    finally:
        bm.close()
