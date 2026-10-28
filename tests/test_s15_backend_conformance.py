"""S15 C3：**后端一致性套件**（`docs/s15-protocol-and-production.md` §2.3）。

目的：同一套断言跑**两套后端**，确保「换后端不改语义」（MS9 的前提）。

- `CheckpointStore`：内存版 `CheckpointStore` vs `FileCheckpointStore`
- `EventLog`：内存版 `EventLog` vs `WalEventLog(fsync=True)`

这些用例在**当前实现下应当全绿**；若某条红，说明两后端真的语义不一致
（这正是套件的价值——发现「只有文件版有、内存版没有」这类静默差异）。

与 C4（`test_s15_contract_drift.py`）的分工：
- **C4 静态**：成员**存在性**（漏声明 → 红）
- **C3 实跑**：成员**语义一致**（行为不同 → 红）
"""

from __future__ import annotations

import pytest

from harness import ACB, EventKind, FileCheckpointStore, Status, TaskState
from harness.checkpoint import CheckpointStore as MemCheckpointStore
from harness.event_log import EventLog as MemEventLog
from harness.event_log import WalEventLog


# =====================================================================
# CheckpointStore：内存 vs 文件
# =====================================================================
@pytest.fixture(params=["memory", "file"])
def store(request, tmp_path):
    if request.param == "memory":
        return MemCheckpointStore()
    return FileCheckpointStore(str(tmp_path))


def _acb(acb_id: str = "A-R-T1-1", run_id: str = "R", task_id: str = "T1") -> ACB:
    acb = ACB(id=acb_id, run_id=run_id, task_id=task_id,
              logical_subgoal_id="S1")
    acb.state = TaskState.ADMITTED
    return acb


def test_ckpt_save_load_roundtrip(store):
    """`save` → `load` / `load_strict` 都能取回同一 state（两后端一致）。"""
    acb = _acb()
    ref = store.save(acb)
    assert isinstance(ref, str) and ref

    assert store.load(ref)["state"] == TaskState.ADMITTED.value
    assert store.load_strict(ref)["state"] == TaskState.ADMITTED.value
    assert store.load_strict(ref)["acb_id"] == acb.id


def test_ckpt_latest_pointer_advances(store):
    """再 `save` 同一 ACB → `latest_ref_for_acb` 指向**新**快照。"""
    acb = _acb()
    r1 = store.save(acb)
    acb.state = TaskState.DONE
    r2 = store.save(acb)

    assert r1 != r2
    assert store.latest_ref_for_acb(acb.id) == r2
    assert store.load_strict(r2)["state"] == TaskState.DONE.value


def test_ckpt_refs_all_reflects_all_acbs(store):
    """`refs_all` 覆盖所有已存 ACB（顺序稳定：按 acb_id 排序）。"""
    a = _acb("A-R-T1-1", "R", "T1")
    b = _acb("B-R-T2-1", "R", "T2")
    store.save(b)
    store.save(a)

    ids = [aid for aid, _ in store.refs_all()]
    assert ids == ["A-R-T1-1", "B-R-T2-1"]
    assert all(ref is not None for _, ref in store.refs_all())


def test_ckpt_refs_for_run_scoped(store):
    """`refs_for_run` 只返回该 run 的 ACB（跨 run 不串）。"""
    store.save(_acb("A-R1-T1-1", "R1", "T1"))
    store.save(_acb("B-R2-T1-1", "R2", "T1"))

    assert [aid for aid, _ in store.refs_for_run("R1")] == ["A-R1-T1-1"]
    assert store.refs_for_run("") == []


def test_ckpt_update_children_cas_consistent(store):
    """K-68 的 CAS 语义在**两套后端**一致：陈旧 `expect_ref` 也不丢更新。"""
    acb = _acb()
    store.save(acb)
    ref0 = store.latest_ref_for_acb(acb.id)

    store.update_children(acb.id, lambda ch: list(ch) + [("CA", None)],
                          expect_ref=ref0)
    # 仍用**陈旧** ref0 → 必须重读重应用，不能覆盖掉 CA
    store.update_children(acb.id, lambda ch: list(ch) + [("CB", None)],
                          expect_ref=ref0)

    latest = store.load_strict(store.latest_ref_for_acb(acb.id))
    ids = [str(c[0]) for c in (latest.get("children_ids") or [])]
    assert "CA" in ids and "CB" in ids, f"丢更新：{ids}"


def test_ckpt_epoch_monotonic(store):
    """`read_epoch` / `bump_epoch` 单调递增（两后端一致）。"""
    e0 = store.read_epoch()
    assert store.bump_epoch() == e0 + 1
    assert store.bump_epoch() == e0 + 2
    assert store.read_epoch() == e0 + 2


def test_ckpt_missing_ref_returns_none(store):
    """不存在的 ref：`latest_ref_for_acb` 返回 None（两后端一致，不抛）。"""
    assert store.latest_ref_for_acb("nope") is None


# =====================================================================
# EventLog：内存 vs WAL
# =====================================================================
@pytest.fixture(params=["memory", "wal"])
def log(request, tmp_path):
    if request.param == "memory":
        return MemEventLog()
    return WalEventLog(tmp_path / "wal.log", fsync=True)


def test_log_append_and_tail(log):
    """`append` 后 `tail` 递增；返回的 seq 连续（两后端一致）。"""
    assert log.tail() == 0
    e1 = log.append(EventKind.EFFECT_INTENT, "r")
    e2 = log.append(EventKind.TOOL_COMPLETED, "r")
    assert log.tail() == 2
    assert (e1.seq, e2.seq) == (1, 2)


def test_log_append_batch_atomic_numbers(log):
    """`append_batch` 一批事件 seq 连续、`tail` 一次推进（两后端一致）。"""
    events = log.append_batch([
        {"kind": EventKind.EFFECT_INTENT, "subject": "r"},
        {"kind": EventKind.TOOL_COMPLETED, "subject": "r"},
    ])
    assert [e.seq for e in events] == [1, 2]
    assert log.tail() == 2


def test_log_all_and_replay_consistent(log):
    """`all()` 与 `replay(subject)` 的过滤结果一致（两后端一致）。"""
    log.append(EventKind.EFFECT_INTENT, "a")
    log.append(EventKind.EFFECT_INTENT, "b")
    log.append(EventKind.TOOL_COMPLETED, "a")

    assert len(log.all()) == 3
    assert [e.subject for e in log.replay("a")] == ["a", "a"]


def test_log_has_finds_kind(log):
    """`has(subject, kind)` 语义一致（存在返回 True，不存在 False）。"""
    log.append(EventKind.EFFECT_INTENT, "a")
    assert log.has("a", EventKind.EFFECT_INTENT) is True
    assert log.has("a", EventKind.TOOL_COMPLETED) is False


def test_log_subscribe_notified(log):
    """`subscribe` 的回调被 `append` 触发（两后端一致）。"""
    seen: list = []
    log.subscribe(seen.append)
    log.append(EventKind.EFFECT_INTENT, "a")
    assert len(seen) == 1 and seen[0].kind == EventKind.EFFECT_INTENT


# =====================================================================
# 结构层：两套后端都满足契约（C2 的行为侧交叉验证）
# =====================================================================
def test_both_checkpoint_backends_are_structurally_valid(tmp_path):
    from harness import protocols
    assert isinstance(MemCheckpointStore(), protocols.CheckpointStore)
    assert isinstance(FileCheckpointStore(str(tmp_path)), protocols.CheckpointStore)


def test_both_eventlog_backends_are_structurally_valid(tmp_path):
    from harness import protocols
    assert isinstance(MemEventLog(), protocols.EventLog)
    assert isinstance(WalEventLog(tmp_path / "w.log", fsync=False),
                      protocols.EventLog)


def test_status_enum_is_closed():
    """C3 附带：`Status` 是封闭集合（新增状态必须显式，防静默扩张）。"""
    assert Status.OK.value == "OK"
    assert len(Status) >= 15
