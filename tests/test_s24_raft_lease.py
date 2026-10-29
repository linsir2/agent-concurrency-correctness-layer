"""S24-8：跨主机租约（真实 Raft 复制，pysyncobj 可选）。

覆盖：多 host 竞争只有一个 winner、接管后旧 epoch 被拒（续租 + 提交点）、
租约网络分区下的诚实边界（本沙箱只做本地 3 节点）、缺依赖 fail-closed。

**不假绿**：未安装 pysyncobj → 显式 `xfail` 并登记原因（与本仓库既有 raft 用例一致）。
"""

from __future__ import annotations

import pytest
from conftest import make_harness

from harness import RscSession
from harness.clock import ManualClock
from harness.facade import Wait
from harness.storage._lock import FencingRejected
from harness.storage.raft import (
    RaftEventLog,
    RaftLeaseStore,
    RaftUnavailable,
)


def _require_raft() -> None:
    try:
        import pysyncobj  # noqa: F401
    except ImportError:                                # pragma: no cover
        pytest.xfail("raft 环境缺失：pysyncobj 未安装（pip install .[raft]）")


def _store(port: int) -> RaftLeaseStore:
    return RaftLeaseStore(base_port=port, timeout=20.0)


# ---------------------------------------------------------------- 纯租约
def test_only_one_host_can_hold_lease():
    _require_raft()
    store = _store(47_000)
    try:
        g1 = store.acquire(owner="host-a", instance="i1",
                           now_ms=1000, ttl_ms=500)
        assert g1.ok and g1.epoch == 1
        g2 = store.acquire(owner="host-b", instance="i2",
                           now_ms=1010, ttl_ms=500)
        assert g2.outcome == "held" and not g2.ok
        assert store.read_epoch() == 1
        # 租约状态在各副本收敛
        assert store.cluster.wait_until(
            lambda: all(int(n.lease.epoch) == 1
                        for n in store.cluster.nodes), timeout=10.0)
    finally:
        store.close()


def test_takeover_bumps_epoch_and_rejects_old_renew():
    _require_raft()
    store = _store(47_100)
    try:
        assert store.acquire(owner="a", instance="i1",
                             now_ms=1000, ttl_ms=100).epoch == 1
        g = store.acquire(owner="b", instance="i2", now_ms=1101, ttl_ms=100,
                          expected_epoch=1)
        assert (g.outcome, g.epoch) == ("acquired", 2)
        old = store.renew(owner="a", instance="i1", epoch=1,
                          now_ms=1102, ttl_ms=100)
        assert old.outcome == "fenced"
        assert not store.authorizes(owner="a", instance="i1", epoch=1,
                                    now_ms=1102)
        assert store.authorizes(owner="b", instance="i2", epoch=2,
                                now_ms=1102)
    finally:
        store.close()


def test_release_allows_next_host_with_higher_epoch():
    _require_raft()
    store = _store(47_200)
    try:
        assert store.acquire(owner="a", instance="i1",
                             now_ms=1000, ttl_ms=1000).epoch == 1
        assert store.release(owner="a", instance="i1", epoch=1).outcome == "released"
        g = store.acquire(owner="b", instance="i2", now_ms=1001, ttl_ms=1000,
                          expected_epoch=1)
        assert (g.outcome, g.epoch) == ("acquired", 2)
    finally:
        store.close()


# ---------------------------------------------------------------- 提交点 fence
def test_shared_cluster_rejects_stale_append():
    """租约与事件同一 RSM：旧 epoch 的 append 在**提交点**被拒，tail 不变。"""
    _require_raft()
    store = _store(47_300)
    try:
        log = RaftEventLog(cluster=store.cluster)
        stamp = {"owner": "a", "instance": "i1", "epoch": 1, "now_ms": 1000}
        log.set_lease_fence(lambda: dict(stamp))
        assert store.acquire(owner="a", instance="i1",
                             now_ms=1000, ttl_ms=10_000).ok
        log.append("TOOL_COMPLETED", "x", {})
        tail = log.tail()
        assert tail == 1

        # 另一个 host 接管（epoch → 2），旧 stamp 仍然指向 epoch 1
        takeover = store.acquire(owner="b", instance="i2", now_ms=20_000,
                                 ttl_ms=10_000, expected_epoch=1)
        assert takeover.epoch == 2
        with pytest.raises(FencingRejected):
            log.append("TOOL_COMPLETED", "y", {})
        assert log.tail() == tail
    finally:
        store.close()


def test_set_lease_fence_requires_lease_cluster():
    _require_raft()
    log = RaftEventLog(base_port=47_400, timeout=20.0)
    try:
        with pytest.raises(RaftUnavailable):
            log.set_lease_fence(dict)
    finally:
        log.close()


# ---------------------------------------------------------------- Harness 接线
def test_harness_with_raft_lease_blocks_second_writer():
    _require_raft()
    store = _store(47_500)
    try:
        clock = ManualClock()
        h1 = make_harness(lease_store=store, clock=clock, lease_ms=1000)
        assert h1.state.writer_lease_ok is True
        s1 = RscSession(h1, tenant="t", min_budget=1)
        tok = s1.begin("a", "g", writes=["a.md"])
        assert s1.apply(tok.token, [("a.md", "v1")],
                        idempotency_key="k1", agent_id="a")

        clock.advance(1001)
        h2 = make_harness(lease_store=store, clock=clock, lease_ms=1000)
        assert h2.state.writer_lease_ok is True
        assert store.read_epoch() == 2

        before = h1.event_log.tail()
        denied = s1.apply(tok.token, [("a.md", "v2")],
                          idempotency_key="k2", agent_id="a")
        assert isinstance(denied, Wait)
        assert denied.reason == "writer_lease_held"
        assert h1.event_log.tail() == before
    finally:
        store.close()


# ---------------------------------------------------------------- fail-closed
def test_missing_pysyncobj_refuses_to_start(monkeypatch):
    import harness.storage.raft as raft_mod

    monkeypatch.setattr(raft_mod, "_HAS_PYSYNCOBJ", False)
    with pytest.raises(RaftUnavailable):
        raft_mod.RaftLeaseStore(base_port=47_600)
