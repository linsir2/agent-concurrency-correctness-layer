"""S17：K-81~K-86 边界收口（`docs/s17-boundaries-and-ms10.md` §2）。

覆盖 S17-1..S17-7。**fail-closed**：未装 `pysyncobj` 时 xfail 并显式登记。
"""

from __future__ import annotations

import itertools
import os

import pytest

from harness import EventKind, protocols

try:
    import pysyncobj  # noqa: F401
    _HAS_RAFT = True
except ImportError:                                   # pragma: no cover
    _HAS_RAFT = False

if _HAS_RAFT:
    from harness.storage.raft import (
        EpochNotBumpable,
        NotLeader,
        RaftBudgetManager,
        RaftCluster,
        RaftEpochSource,
        RaftEventLog,
        _ReplicatedLog,
        pick_free_base,
        production_conf,
        raft_conf,
    )

pytestmark = pytest.mark.raft

_PORT_SEQ = itertools.count(0)
_REGISTERED: list[str] = []


def _base_port() -> int:
    return 41600 + (os.getpid() % 200) * 20 + next(_PORT_SEQ) * 12


def _require_raft() -> None:
    if _HAS_RAFT:
        return
    reason = "raft 环境缺失：pysyncobj 未安装（pip install .[raft]）"
    if reason not in _REGISTERED:
        _REGISTERED.append(reason)
        print(f"\n[S17 raft 环境登记] {reason}")
    pytest.xfail(reason)


# ============================================================ S17-1
def test_s17_1_deterministic_rid_across_replicas():
    """**K-81 核心**：`reserve` 的 id 由**复制计数器**决定 → 各副本完全一致。

    这正是 `uuid4()` 做不到的：若用 uuid，三副本的 rid 各不相同 → 状态发散。
    """
    _require_raft()
    bm = RaftBudgetManager(base_port=_base_port())
    try:
        bm.create_node("n1", 100)
        r1 = bm.reserve("n1", 10)
        r2 = bm.reserve("n1", 20)
        assert r1 is not None and r2 is not None
        assert r1.reservation_id == "R1"
        assert r2.reservation_id == "R2"

        # 绑定副本（写入等待的那一个）必须是权威的
        assert int(bm._replica._seq) == 2
        assert "R1" in dict(getattr(bm._replica, "_resv", {}))

        # 复制是**异步**的：其余副本需等待收敛（不能立刻断言三者同步）
        assert bm._cluster.wait_until(
            lambda: all(int(getattr(n, "_seq", 0)) == 2
                        for n in bm._cluster.nodes), 20.0), (
            f"副本未收敛：{[int(getattr(n, '_seq', 0)) for n in bm._cluster.nodes]}")
        resv_maps = [dict(getattr(n, "_resv", {})) for n in bm._cluster.nodes]
        assert resv_maps[0] == resv_maps[1] == resv_maps[2], (
            "收敛后各副本账本必须完全一致（含确定性 rid）")
        assert resv_maps[0]["R1"][1] == 10 and resv_maps[0]["R2"][1] == 20
    finally:
        bm.close()


def test_s17_1_rid_continues_after_restart(tmp_path):
    """**K-81 + K-86**：重启（journal/快照恢复）后 rid 继续单调，不重复。"""
    _require_raft()
    base = _base_port()
    data = str(tmp_path)

    bm = RaftBudgetManager(base_port=base, data_dir=data)
    try:
        bm.create_node("n1", 100)
        for _ in range(3):
            assert bm.reserve("n1", 1) is not None
        assert int(bm._replica._seq) == 3
        bm._cluster.compact()
    finally:
        bm.close()

    # 重启：同一 data_dir → 计数器与账本从 journal/快照恢复
    bm2 = RaftBudgetManager(base_port=base, data_dir=data)
    try:
        assert int(bm2._replica._seq) == 3, (
            f"重启后计数器未恢复：{int(bm2._replica._seq)}")
        r = bm2.reserve("n1", 1)
        assert r is not None and r.reservation_id == "R4", (
            f"重启后 rid 必须继续单调（实际 {getattr(r, 'reservation_id', None)}）")
    finally:
        bm2.close()


def _ReplicatedBudgetForTest():
    from harness.storage.raft import _ReplicatedBudget
    return _ReplicatedBudget


# ============================================================ S17-2
def test_s17_2_raft_budget_does_not_overspend():
    """quorum 串行化：并发预留不超限（超出被拒，且不分配 id）。"""
    _require_raft()
    bm = RaftBudgetManager(base_port=_base_port())
    try:
        bm.create_node("n1", 10)
        ok1 = bm.reserve("n1", 6)
        ok2 = bm.reserve("n1", 6)      # 6+6 > 10 → 拒绝
        assert ok1 is not None
        assert ok2 is None
        assert bm._reserved_of("n1") == 6
        assert bm.check("n1", 4) is True
        assert bm.check("n1", 5) is False
    finally:
        bm.close()


def test_s17_2_release_returns_capacity():
    """release 把未消耗部分退回（可再次预留）。"""
    _require_raft()
    bm = RaftBudgetManager(base_port=_base_port())
    try:
        bm.create_node("n1", 10)
        r = bm.reserve("n1", 8)
        assert r is not None
        assert bm._reserved_of("n1") == 8
        bm.release(r.reservation_id)
        assert bm._reserved_of("n1") == 0
        assert bm.reserve("n1", 10) is not None
    finally:
        bm.close()


# ============================================================ S17-3
def test_s17_3_raft_budget_satisfies_protocol():
    """`RaftBudgetManager` 满足 `protocols.BudgetManager` 结构契约。"""
    _require_raft()
    bm = RaftBudgetManager(base_port=_base_port())
    try:
        declared = {m for m in vars(protocols.BudgetManager)
                    if not m.startswith("__") and not m.startswith("_abc")
                    and m not in ("_is_protocol", "_is_runtime_protocol")}
        missing = sorted(m for m in declared if not hasattr(bm, m))
        assert not missing, f"缺成员：{missing}"
        assert isinstance(bm, protocols.BudgetManager)
    finally:
        bm.close()


# ============================================================ S17-4
def test_s17_4_require_leader_rejects_follower_read():
    """**K-82**：`require_leader=True` 时 follower 读抛 `NotLeader`（不返回陈旧值）。"""
    _require_raft()
    log = RaftEventLog(base_port=_base_port(), require_leader=True)
    try:
        log.append(EventKind.EFFECT_INTENT, "r")
        lead = log._cluster.leader()
        follower = next(n for n in log._cluster.nodes if n is not lead)

        # 构造一个「副本是 follower」的 log 视图
        from harness.storage.raft import RaftEventLog as _L
        view = _L.__new__(_L)          # 不初始化集群，仅测读校验逻辑
        view._require_leader = True
        view._replica = follower
        view._cluster = log._cluster
        view._timeout = 5.0

        with pytest.raises(NotLeader):
            view.all()
    finally:
        log.close()


def test_s17_4_default_mode_allows_follower_read():
    """默认（`require_leader=False`）允许 follower 读（最终一致，文档已声明）。"""
    _require_raft()
    log = RaftEventLog(base_port=_base_port())
    try:
        log.append(EventKind.EFFECT_INTENT, "r")
        assert len(log.all()) == 1       # 不抛
    finally:
        log.close()


# ============================================================ S17-5
def test_s17_5_production_conf_validates():
    """**K-83**：生产配置校验（node_id 在 addrs 内、≥3 节点、data_dir 非空）。"""
    _require_raft()
    addrs = ["10.0.0.1:4321", "10.0.0.2:4321", "10.0.0.3:4321"]
    conf = production_conf(node_id=addrs[1], addrs=addrs, data_dir="/tmp/x")
    assert conf.journalFile.endswith("node1.journal")
    assert conf.fullDumpFile.endswith("node1.dump")
    assert conf.raftMinTimeout > conf.appendEntriesPeriod * 3
    assert conf.connectionTimeout >= conf.raftMaxTimeout

    with pytest.raises(ValueError):
        production_conf(node_id="10.0.0.9:4321", addrs=addrs, data_dir="/tmp/x")
    with pytest.raises(ValueError):
        production_conf(node_id=addrs[0], addrs=addrs[:2], data_dir="/tmp/x")
    with pytest.raises(ValueError):
        production_conf(node_id=addrs[0], addrs=addrs, data_dir="")


# ============================================================ S17-6
def test_s17_6_snapshot_and_restart_recovers(tmp_path):
    """**K-86**：`journalFile` + `fullDumpFile` + `compact()` → 重启完整恢复。"""
    _require_raft()
    base = _base_port()
    data = str(tmp_path)

    cluster = RaftCluster(_ReplicatedLog, base_port=base, data_dir=data)
    try:
        assert cluster.wait_ready(20.0)
        node = cluster.nodes[0]
        for i in range(6):
            node.append_record({"kind": "EFFECT_INTENT", "subject": "r",
                                "payload": {"i": i}})
        assert cluster.wait_until(
            lambda: len(getattr(node, "records", [])) >= 6, 20.0)
        cluster.compact()
        assert len(node.records) == 6
    finally:
        cluster.close()

    cluster2 = RaftCluster(_ReplicatedLog, base_port=base, data_dir=data)
    try:
        assert cluster2.wait_ready(20.0)
        node2 = cluster2.nodes[0]
        assert [r["payload"]["i"] for r in node2.records] == list(range(6)), (
            f"重启未恢复：{node2.records}")
    finally:
        cluster2.close()


# ============================================================ S17-7
def test_s17_7_bump_epoch_is_explicitly_unavailable():
    """**K-85**：`bump_epoch` 显式不可用（不再「返回同值」误导调用方）。"""
    _require_raft()
    cluster = RaftCluster(_ReplicatedLog, base_port=_base_port())
    try:
        assert cluster.wait_ready(20.0)
        src = RaftEpochSource(cluster)
        assert src.supports_bump is False
        with pytest.raises(EpochNotBumpable):
            src.bump_epoch()
        assert src.read_epoch() >= 1
    finally:
        cluster.close()


# ============================================================ 辅助
def test_s17_raft_conf_accepts_journal_and_dump(tmp_path):
    """`raft_conf` 支持 journal/dump（K-86 的配置入口）。"""
    _require_raft()
    conf = raft_conf(journal_file=str(tmp_path / "a.j"),
                     full_dump_file=str(tmp_path / "a.d"))
    assert conf.journalFile == str(tmp_path / "a.j")
    assert conf.fullDumpFile == str(tmp_path / "a.d")
    assert conf.logCompactionMinEntries >= 1


def test_s17_pick_free_base_skips_busy_ports():
    """端口探测会跳过被占用的端口（K-84 挂起根因的防线）。"""
    _require_raft()
    import socket
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind(("127.0.0.1", 0))
    busy = holder.getsockname()[1]
    try:
        chosen = pick_free_base(busy, 3)
        assert chosen != busy, "占用的端口不得被选中"
        assert chosen > busy or chosen < busy
    finally:
        holder.close()
