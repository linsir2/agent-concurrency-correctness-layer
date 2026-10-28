"""S16 / MS9：Raft 后端（`docs/s16-ms9-raft.md` §3）。

覆盖 S16-1..S16-10。**fail-closed**：未装 `pysyncobj` 时 xfail 并显式登记
（不是静默 skip）。
"""

from __future__ import annotations

import itertools
import os
import socket

import pytest

from harness import EventKind, Harness, Status, TaskState, protocols

try:
    import pysyncobj  # noqa: F401
    _HAS_RAFT = True
except ImportError:                                   # pragma: no cover
    _HAS_RAFT = False

if _HAS_RAFT:
    from harness.storage.raft import (
        RaftCluster,
        RaftEpochSource,
        RaftEventLog,
        RaftUnavailable,
        _ReplicatedLog,
        raft_conf,
    )

pytestmark = pytest.mark.raft

_PORT_SEQ = itertools.count(0)
_REGISTERED: list[str] = []


def _base_port() -> int:
    """每测试独立端口段（pid + 序号派生），降低 CI 冲突概率。"""
    return 21000 + (os.getpid() % 200) * 20 + next(_PORT_SEQ) * 12


def _require_raft() -> None:
    if _HAS_RAFT:
        return
    reason = "raft 环境缺失：pysyncobj 未安装（pip install .[raft]）"
    if reason not in _REGISTERED:
        _REGISTERED.append(reason)
        print(f"\n[S16 raft 环境登记] {reason}")
    pytest.xfail(reason)


@pytest.fixture(scope="module")
def shared_cluster():
    """**K-84**：模块级共享集群，仅供「只读集群、不需状态隔离」的用例复用。

    每测试新建 3 节点集群约 0.5–0.7s；这里把纯观测类用例（选主 / epoch）
    合并到一次建簇，缩短 Raft 子集耗时（不改任何断言）。
    """
    _require_raft()
    cluster = RaftCluster(_ReplicatedLog, base_port=_base_port())
    if not cluster.wait_ready(20.0):
        cluster.close()
        pytest.fail("共享集群未在 20s 内就绪")
    try:
        yield cluster
    finally:
        cluster.close()


@pytest.fixture
def raft_log():
    _require_raft()
    log = RaftEventLog(base_port=_base_port())
    yield log
    log.close()


def _port_free(port: int) -> bool:
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


# ============================================================ S16-1
def test_s16_1_cluster_elects_leader(shared_cluster):
    """3 节点均 ready，且能选出 leader（20s 上限内）。"""
    cluster = shared_cluster
    assert all(n.isReady() for n in cluster.nodes)
    assert cluster.leader() is not None
    assert cluster.quorum() is True


# ============================================================ S16-2
def test_s16_2_seq_assigned_by_leader_no_gaps():
    """**MS9 核心**：并发 append 的 seq 严格 1..N，无空洞、无重复。"""
    _require_raft()
    log = RaftEventLog(base_port=_base_port())
    try:
        n = 20
        evs = [log.append(EventKind.EFFECT_INTENT, "r", {"i": i}) for i in range(n)]
        seqs = [e.seq for e in evs]
        assert sorted(seqs) == list(range(1, n + 1)), f"seq 不连续：{sorted(seqs)}"
        assert len(set(seqs)) == n, "seq 重复"
        assert log.tail() == n
    finally:
        log.close()


def test_s16_2_concurrent_append_serialized():
    """并发（多线程）append 仍得到无重复的连续 seq（quorum 串行化）。"""
    _require_raft()
    import threading
    log = RaftEventLog(base_port=_base_port())
    try:
        out: list[int] = []
        lock = threading.Lock()

        def worker(i: int) -> None:
            ev = log.append(EventKind.EFFECT_INTENT, "r", {"t": i})
            with lock:
                out.append(ev.seq)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert len(out) == 8
        assert sorted(out) == list(range(1, 9)), f"并发下 seq 异常：{sorted(out)}"
    finally:
        log.close()


# ============================================================ S16-3
def test_s16_3_replica_convergence():
    """三副本最终收敛到相同内容与相同 seq。"""
    _require_raft()
    log = RaftEventLog(base_port=_base_port())
    try:
        for i in range(5):
            log.append(EventKind.EFFECT_INTENT, "r", {"i": i})

        def converged() -> bool:
            return all(len(getattr(n, "records", ())) >= 5 for n in log._cluster.nodes)

        assert log._cluster.wait_until(converged, 20.0), "副本未收敛"
        copies = [[(r["seq"], r["subject"], (r.get("payload") or {}).get("i"))
                   for r in n.records] for n in log._cluster.nodes]
        assert copies[0] == copies[1] == copies[2]
    finally:
        log.close()


# ============================================================ S16-5
def test_s16_5_raft_log_satisfies_protocol(raft_log):
    """`RaftEventLog` 通过 `protocols.EventLog` 结构契约。"""
    assert isinstance(raft_log, protocols.EventLog)


# ============================================================ S16-6
def test_s16_6_replay_and_has_filter(raft_log):
    """`replay(subject)` / `has` 的过滤语义与内存版一致。"""
    raft_log.append(EventKind.EFFECT_INTENT, "a")
    raft_log.append(EventKind.EFFECT_INTENT, "b")
    raft_log.append(EventKind.EFFECT_CONFIRMED, "a")

    assert [e.subject for e in raft_log.replay("a")] == ["a", "a"]
    assert len(raft_log.all()) == 3
    assert raft_log.has("a", EventKind.EFFECT_INTENT) is True
    assert raft_log.has("a", EventKind.EFFECT_CONFIRMED) is True
    assert raft_log.has("b", EventKind.EFFECT_CONFIRMED) is False


def test_s16_6_append_batch_contiguous(raft_log):
    """`append_batch` 一批事件的 seq 连续、`tail` 一次推进。"""
    evs = raft_log.append_batch([
        {"kind": EventKind.EFFECT_INTENT, "subject": "r"},
        {"kind": EventKind.EFFECT_CONFIRMED, "subject": "r"},
    ])
    assert [e.seq for e in evs] == [1, 2]
    assert raft_log.tail() == 2


# ============================================================ S16-7
def test_s16_7_harness_runs_e2e_with_raft(raft_log):
    """**装配验证**：Harness 用 Raft 事件日志跑一个真实任务到 `DONE`。"""
    from conftest import admit, decision, finish

    h = Harness(event_log=raft_log, run_limits=100)
    h.policy.default_token = h.policy.issue(
        "*", tools={"*"}, paths={"*"}, network={"*"}, data_class="*")
    plan = h.planner.plan("R", [{"task_id": "T1", "min_budget": 10}])
    h.plans["R"] = plan
    acb = admit(h, "R", plan, "T1")

    st = h.run_task(acb, [decision(1, finish())])
    assert st is Status.OK, f"Raft 后端下任务未完成：{st}"
    assert acb.state is TaskState.DONE
    # 事件确实进了 Raft 日志
    assert raft_log.tail() > 0


# ============================================================ S16-8
def test_s16_8_epoch_comes_from_consensus(shared_cluster):
    """epoch 由 Raft term 推进；本地 `bump_epoch()` **明确不可用**（S17 K-85）。

    S17 变更：原实现「返回同值」会让调用方误以为已推进 → 改为显式抛错，
    并暴露 `supports_bump=False`，避免静默语义漂移。
    """
    from harness.storage.raft import EpochNotBumpable

    cluster = shared_cluster
    if True:
        epoch = RaftEpochSource(cluster)
        e1 = epoch.read_epoch()
        assert e1 >= 1, "leader 的 term 应 >= 1"
        assert epoch.supports_bump is False
        with pytest.raises(EpochNotBumpable):
            epoch.bump_epoch()
        assert epoch.read_epoch() == e1, "读 epoch 不应被失败的 bump 改变"


# ============================================================ S16-10
def test_s16_10_missing_pysyncobj_fails_closed(monkeypatch):
    """无 `pysyncobj` → `RaftUnavailable`（**不静默降级**为单机）。"""
    _require_raft()
    from harness.storage import raft as raft_mod
    monkeypatch.setattr(raft_mod, "_HAS_PYSYNCOBJ", False)
    with pytest.raises(RaftUnavailable):
        raft_mod.require_pysyncobj()
    with pytest.raises(RaftUnavailable):
        RaftEventLog(base_port=_base_port())


# ============================================================ 辅助
def test_s16_ports_are_free_and_config_valid():
    """端口可用性 + `raft_conf` 满足 pysyncobj 的配置断言（回归给定约束）。"""
    _require_raft()
    base = _base_port()
    assert all(_port_free(base + i) for i in range(3)), "端口被占用"
    conf = raft_conf()
    assert conf.raftMinTimeout > conf.appendEntriesPeriod * 3
    assert conf.connectionTimeout >= conf.raftMaxTimeout
    assert conf.autoTick is False, "必须手动/受控 tick（autoTick=False）"
