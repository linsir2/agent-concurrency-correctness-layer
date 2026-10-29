"""S24-P0：事件层 lease fence 必须与租约共用同一 Raft 集群。

不依赖 pysyncobj：给内存 Harness 挂上「具备 lease 状态的 replica」与假
cluster 对象，直接验证 `_wire_lease_fence` 的身份检查（错配 → fail-closed）。
"""

from __future__ import annotations

import types

import pytest
from conftest import make_harness

from harness.storage.lease import LeaseUnavailable, MemoryLeaseStore


def _harness_with_raft_shape():
    store = MemoryLeaseStore()
    h = make_harness(lease_store=store)
    # 模拟 RaftEventLog 的形态（有 _replica.lease 与 set_lease_fence）
    h.event_log._replica = types.SimpleNamespace(lease=object())
    h.event_log.set_lease_fence = lambda provider: None
    h.state.lease_store.cluster = object()
    h.event_log._cluster = object()
    return h


def test_mismatched_cluster_is_rejected():
    h = _harness_with_raft_shape()
    with pytest.raises(LeaseUnavailable):
        h.leases.wire_lease_fence()


def test_same_cluster_is_accepted():
    h = _harness_with_raft_shape()
    shared = object()
    h.state.lease_store.cluster = shared
    h.event_log._cluster = shared
    h.leases.wire_lease_fence()                     # 不抛
    # 没装 lease 状态的日志 → 直接跳过（不假装有 fence）
    h2 = make_harness(lease_store=MemoryLeaseStore())
    h2.leases.wire_lease_fence()
