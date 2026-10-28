"""S18：K-87（预算树纳入共识）+ K-82（线性一致读）+ K-83（外部地址集群）。

对应 `docs/s18-budget-rsm-and-linearizable-read.md` §2 的 S18-1..S18-8。
"""

from __future__ import annotations

import itertools
import os

import pytest

from harness import EventKind

try:
    import pysyncobj  # noqa: F401
    _HAS_RAFT = True
except ImportError:                                   # pragma: no cover
    _HAS_RAFT = False

if _HAS_RAFT:
    from harness.storage.raft import (
        RaftBudgetManager,
        RaftCluster,
        RaftEventLog,
        _ReplicatedLog,
        pick_free_base,
    )

pytestmark = pytest.mark.raft

_SEQ = itertools.count(0)
_REGISTERED: list[str] = []


def _base() -> int:
    return 35600 + (os.getpid() % 200) * 20 + next(_SEQ) * 12


def _require() -> None:
    if _HAS_RAFT:
        return
    reason = "raft 环境缺失：pysyncobj 未安装（pip install .[raft]）"
    if reason not in _REGISTERED:
        _REGISTERED.append(reason)
        print(f"\n[S18 raft 环境登记] {reason}")
    pytest.xfail(reason)


# ============================================================ S18-1
def test_s18_1_parent_child_tree_reservation():
    """**K-87 核心**：父子预算树真实存在 —— 子预留会推高祖先的 `subtree_reserved`。

    S17 的扁平账本做不到这一点（`_subtree_nodes` 只返回自身）。
    """
    _require()
    bm = RaftBudgetManager(base_port=_base())
    try:
        bm.create_node("run", 100)
        bm.create_node("child", 40, "run")
        bm.create_node("grand", 20, "child")

        assert bm.reserve("grand", 5) is not None
        # 链式传播：grand(5) → child.subtree(5) → run.subtree(5)
        assert bm._bm._nodes["grand"].self_reserved == 5
        assert bm._bm._nodes["child"].subtree_reserved == 5
        assert bm._bm._nodes["run"].subtree_reserved == 5
        # 树结构真实（父指针与 children）
        child = bm._bm._nodes["child"]
        assert child.parent is bm._bm._nodes["run"]
        assert bm._bm._nodes["grand"] in child.children
        assert len(bm._bm._subtree_nodes(bm._bm._nodes["child"])) == 2
    finally:
        bm.close()


# ============================================================ S18-2
def test_s18_2_archive_folds_consumed_and_replicates():
    """**K-87**：归档把子树 `consumed` 折入父级（I5c），且**三副本一致**。"""
    _require()
    bm = RaftBudgetManager(base_port=_base())
    try:
        bm.create_node("run", 100)
        bm.create_node("child", 40, "run")
        r = bm.reserve("child", 10)
        assert r is not None
        bm.consume(r.reservation_id, 10)
        bm.settle(r.reservation_id)

        assert bm.archive_node("child") is True
        parent = bm._bm._nodes["run"]
        assert parent.folded_consumed == 10, "永久账必须折入父级（I5c）"
        assert "child" not in bm._bm._nodes, "子树已摘除"

        # 三副本收敛后账目完全一致
        def same() -> bool:
            sig = []
            for n in bm._cluster.nodes:
                nodes = n.bm._nodes
                run = nodes.get("run")
                sig.append((tuple(sorted(nodes)), int(run.folded_consumed),
                            int(run.subtree_reserved), int(run.consumed)))
            return len(set(sig)) == 1

        assert bm._cluster.wait_until(same, 20.0), (
            f"副本不一致：{[(tuple(sorted(n.bm._nodes)), n.bm._nodes['run'].folded_consumed) for n in bm._cluster.nodes]}")
    finally:
        bm.close()


# ============================================================ S18-3
def test_s18_3_invariants_checkable_on_any_replica():
    """**K-87**：`check_invariants()` 在**非绑定副本**上也通过（集群一致）。"""
    _require()
    bm = RaftBudgetManager(base_port=_base())
    try:
        bm.create_node("run", 100)
        bm.create_node("child", 50, "run")
        r = bm.reserve("child", 20)
        assert r is not None
        bm.consume(r.reservation_id, 7)

        # **先等收敛**（否则在「任意日志前缀」上校验不变量都会通过，是假证据）
        target = int(bm._replica.version)

        def converged() -> bool:
            return all(int(n.version) >= target for n in bm._cluster.nodes)

        assert bm._cluster.wait_until(converged, 20.0), "副本未收敛"
        # 在**另一个**副本上直接校验真实账本（S17 时机上只有 leader 有完整树）
        for node in bm._cluster.nodes[1:]:
            node.bm.check_invariants()
            assert int(node.bm._nodes["child"].self_reserved) == 20
        # 绑定副本也一致
        bm.check_invariants()
    finally:
        bm.close()


# ============================================================ S18-4
def test_s18_4_chain_limit_rejects_overspend():
    """链式限额：父 10、子 40 → 子在父额度耗尽后必须被拒。"""
    _require()
    bm = RaftBudgetManager(base_port=_base())
    try:
        # 注意：根 "run" 已存在（limits=1000），不能改其限额
        # → 用中间层 p(10) 才能验证「父额度耗尽则子不得超发」
        bm.create_node("p", 10, "run")
        bm.create_node("child", 40, "p")
        assert bm.reserve("child", 8) is not None
        # 再要 5：p 只有 10、已占 8 → 拒（链式检查，不只查 child 自己的 40）
        assert bm.reserve("child", 5) is None, "父额度已耗尽，子不得超发"
        assert bm._bm._nodes["p"].subtree_reserved == 8
    finally:
        bm.close()


# ============================================================ S18-5
def test_s18_5_tree_survives_restart(tmp_path):
    """**K-87 + K-86**：树 + 归档后重启 → 结构与账目完整恢复。"""
    _require()
    base = _base()
    data = str(tmp_path)

    bm = RaftBudgetManager(base_port=base, data_dir=data)
    try:
        bm.create_node("run", 100)
        bm.create_node("child", 40, "run")
        r = bm.reserve("child", 6)
        bm.consume(r.reservation_id, 6)
        bm.settle(r.reservation_id)
        bm.archive_node("child")
        bm._cluster.compact()
    finally:
        bm.close()

    bm2 = RaftBudgetManager(base_port=base, data_dir=data)
    try:
        run = bm2._bm._nodes.get("run")
        assert run is not None, "根节点未恢复"
        assert run.folded_consumed == 6, (
            f"归档折叠未恢复：{run.folded_consumed}")
        assert "child" not in bm2._bm._nodes
        # rid 计数继续（K-81 + 重启）
        nid = "leaf"
        leaf = bm2.create_node(nid, 10, "run")
        assert leaf is not None and leaf.limits == 10
        assert bm2.reserve(nid, 1) is not None
        assert bm2._bm._nodes["run"].subtree_reserved >= 1, (
            "重启后树关系必须仍生效（子预留要计入父）")
    finally:
        bm2.close()


# ============================================================ S18-6
def test_s18_6_barrier_gives_linearizable_read():
    """**K-82**：`barrier()` 后的读必须看到**经由其他节点**提交的写入。

    审查 P0-1 指出原断言（`tail() >= 2` 在绑定副本上本就成立）是恒真的；
    这里改为「写入发生在另一个节点上」，只有 barrier 真正刷新本地视图才成立。
    """
    _require()
    log = RaftEventLog(base_port=_base())
    try:
        log.append(EventKind.EFFECT_INTENT, "r")
        assert log.tail() == 1

        # 从**另一个节点**再写一条：绑定副本需要等复制才能看到
        other = next(n for n in log._cluster.nodes if n is not log._replica)
        other.append_record({"kind": "EFFECT_INTENT", "subject": "r",
                             "payload": {}})
        assert log._cluster.wait_until(
            lambda: len(other.records) >= 2, 20.0)

        log.barrier()                      # 读穿日志：等 quorum + 刷新本地视图
        assert log.tail() == 2, (
            f"barrier 后仍看不到另一节点写入（tail={log.tail()}）—— 线性一致不成立")

        with log.linearizable():
            assert len(log.all()) == 2
    finally:
        log.close()


def test_s18_6_barrier_negative_control_without_barrier():
    """负对照：不做 barrier 时，**非绑定副本**可能落后（说明 barrier 不是白做的）。"""
    _require()
    log = RaftEventLog(base_port=_base())
    try:
        for _ in range(5):
            log.append(EventKind.EFFECT_INTENT, "r")

        follower = next(n for n in log._cluster.nodes if n is not log._replica)
        # 立刻读 follower：可能尚未应用全部 5 条（允许 0..5，只要不超）
        assert len(follower.records) <= 5
        # barrier 后绑定副本一定看到 5 条
        log.barrier()
        assert log.tail() >= 5
    finally:
        log.close()


# ============================================================ S18-7
def test_s18_7_require_leader_vs_barrier():
    """`require_leader` 在 follower 抛错；`barrier` 不抛（但写一条日志）。"""
    _require()
    log = RaftEventLog(base_port=_base(), require_leader=True)
    try:
        log.append(EventKind.EFFECT_INTENT, "r")
        lead = log._cluster.leader()
        follower = next(n for n in log._cluster.nodes if n is not lead)

        from harness.storage.raft import NotLeader
        from harness.storage.raft import RaftEventLog as _L
        view = _L.__new__(_L)
        view._require_leader = True
        view._replica = follower
        view._cluster = log._cluster
        view._timeout = 5.0
        with pytest.raises(NotLeader):
            view.all()

        # barrier 不会因角色抛错，且**确实写入了一条 no-op 日志**
        before = int(getattr(log._replica, "barrier_count", 0) or 0)
        log.barrier()
        assert int(getattr(log._replica, "barrier_count", 0)) > before
    finally:
        log.close()


# ============================================================ S18-8
def test_s18_8_from_addrs_validates_and_builds():
    """**K-83**：外部地址集群；非法输入在**建簇前**被拒。"""
    _require()
    with pytest.raises(ValueError):
        RaftCluster.from_addrs(_ReplicatedLog, addrs=["127.0.0.1:1"])
    with pytest.raises(ValueError):
        RaftCluster.from_addrs(_ReplicatedLog, addrs=[
            "127.0.0.1:7001", "127.0.0.1:7001", "127.0.0.1:7003"])
    with pytest.raises(ValueError):
        RaftCluster.from_addrs(_ReplicatedLog, addrs=[
            "bad", "127.0.0.1:7002", "127.0.0.1:7003"])

    base = pick_free_base(_base(), 3)
    addrs = [f"127.0.0.1:{base + i}" for i in range(3)]
    cluster = RaftCluster.from_addrs(_ReplicatedLog, addrs=addrs)
    try:
        assert cluster.wait_ready(20.0), "外部地址集群未就绪"
        assert cluster.leader() is not None
    finally:
        cluster.close()


# ============================================================ 契约
def test_s18_raft_budget_is_protocol_compliant():
    """`RaftBudgetManager` 仍满足 `BudgetManager` 契约（S18 重写后回归）。"""
    _require()
    from harness import protocols

    bm = RaftBudgetManager(base_port=_base())
    try:
        declared = {m for m in vars(protocols.BudgetManager)
                    if not m.startswith("__") and not m.startswith("_abc")
                    and m not in ("_is_protocol", "_is_runtime_protocol")}
        missing = sorted(m for m in declared if not hasattr(bm, m))
        assert not missing, f"缺成员：{missing}"
    finally:
        bm.close()


# ============================================================ 审查回归
def test_review_archive_returns_real_result():
    """审查 P0-3：`archive_node` 必须回传**真实**结果（不是恒 True）。"""
    _require()
    bm = RaftBudgetManager(base_port=_base())
    try:
        bm.create_node("p", 20, "run")
        bm.create_node("c", 10, "p")
        r = bm.reserve("c", 5)
        assert r is not None
        # 仍有 ACTIVE 预留 → 归档必须被拒
        assert bm.archive_node("c") is False, "有 ACTIVE 预留时不得归档"
        assert "c" in bm._bm._nodes
        # 不存在的节点同样为 False（而不是恒 True）
        assert bm.archive_node("ghost") is False
    finally:
        bm.close()


def test_review_create_node_conflicting_limits_returns_none():
    """审查 P1-4：同名不同 limits 必须返回 None（不静默返回旧节点）。"""
    _require()
    bm = RaftBudgetManager(base_port=_base())
    try:
        bm.create_node("p", 20, "run")
        assert bm.create_node("p", 20, "run") is not None      # 幂等：同限额
        assert bm.create_node("p", 99, "run") is None, (
            "限额冲突必须拒绝（否则调用方以为改成功）")
        assert bm._bm._nodes["p"].limits == 20
    finally:
        bm.close()


def test_review_consume_with_claim_fails_closed():
    """claim fencing：**S18** 是 fail-closed 拒绝；**S19** 起改为复制 claims。

    S19 语义（升级）：`claim_epoch` 作为命令参数参与复制，单调比较 ——
    高 epoch 先写入后，**低 epoch 消费被拒**（旧 worker 被 fence），
    而不是「带 claim 就一律拒绝」。这里锁定新语义。
    """
    _require()
    from harness.types import ACB

    bm = RaftBudgetManager(base_port=_base())
    try:
        bm.create_node("p", 20, "run")
        r = bm.reserve("p", 5)
        assert r is not None

        hi = ACB(id="A-1", run_id="R", task_id="T1")
        hi.claim_epoch = 3
        assert bm.consume(r.reservation_id, 1, acb=hi) == 1   # 高 epoch 通过

        stale = ACB(id="A-1", run_id="R", task_id="T1")
        stale.claim_epoch = 2                                  # 旧 worker
        assert bm.consume(r.reservation_id, 1, acb=stale) == 0, (
            "低 epoch 必须被 fence（返回 0，不静默记账）")
        # 不带 claim 仍可正常消费（向后兼容）
        assert bm.consume(r.reservation_id, 1) == 1
    finally:
        bm.close()


def test_review_assembly_surface_exists():
    """审查 P0-5：`checkpoint`/`Harness` 会访问的装配面必须存在。"""
    _require()
    bm = RaftBudgetManager(base_port=_base())
    try:
        for name in ("_lock", "_last_budget_seq", "_reset_baseline", "root",
                     "_nodes", "_reservations", "check_invariants", "gc"):
            assert hasattr(bm, name), f"缺装配面 {name}"
        with bm._lock:                        # 必须可作上下文管理器
            pass
        bm._reset_baseline()
    finally:
        bm.close()
