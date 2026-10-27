"""S9c 文件版 checkpoint 测试（`docs/s9-persistence-and-receipts.md` v1.1 §2.4）。

判据：
- **S9-6** 单进程重开：save → 新实例（同 root）`load_strict` → payload 一致；
  `latest_ref_for_acb` 正确。
- **S9-8** ref 唯一：同 root 两实例交替 save → seq 严格递增、ref 两两不同且可 load；
  `store_uuid` 跨实例一致。
- **S9-9** 损坏：篡改 payload → `load_strict` 抛 `CheckpointCorrupt`（带头部）；
  `recover` 得到 `QUARANTINED` + `CHECKPOINT_CORRUPT`；后续仍抛（不静默修复）。
- **S9-10** 旧格式：扁平 payload（无 envelope）可加载（兼容路径）。
- **S9-11** codec：`decode(encode(DELETE)) is DELETE`；copy/deepcopy/pickle 往返仍是单例；
  用户数据含保留键 `__type__` 被拒绝。
"""

from __future__ import annotations

import copy
import json
import pickle

import pytest

from harness import ACB, Harness, TaskState
from harness.checkpoint import CheckpointCorrupt, FileCheckpointStore
from harness.rebase import DELETE
from harness.storage import codec
from harness.storage.blob import canonical_bytes


def _acb(acb_id: str = "A-S9-6", run_id: str = "R-S9-6",
         task_id: str = "T-S9-6", subgoal: str = "S-S9-6") -> ACB:
    acb = ACB(id=acb_id, run_id=run_id, task_id=task_id,
              logical_subgoal_id=subgoal)
    acb.generation = 2
    acb.turn_seq = 3
    acb.cursor = 5
    acb.staged = {"r": {"v": 1}, "gone": DELETE}
    acb.read_set = {"r": 1}
    acb.base_version = {"r": 0}
    return acb


# ================================================================ S9-6
def test_s9_6_single_process_reopen(tmp_path):
    """S9-6：同 root 新实例 `load_strict` 读取一致；latest 指针正确。"""
    root = tmp_path / "cp"
    acb = _acb()
    s1 = FileCheckpointStore(root)
    ref = s1.save(acb)
    assert ref == acb.result_ref

    s2 = FileCheckpointStore(root)
    payload = s2.load_strict(ref)
    assert payload["acb_id"] == acb.id
    assert payload["run_id"] == acb.run_id
    assert payload["task_id"] == acb.task_id
    assert payload["logical_subgoal_id"] == acb.logical_subgoal_id
    assert payload["staged"] == {"r": {"v": 1}, "gone": DELETE}
    assert payload["staged"]["gone"] is DELETE      # 身份必须是模块级单例
    assert payload["event_cursor"] == 5             # 未注入 event_log → 退回 cursor

    assert s2.latest_ref_for_acb(acb.id) == ref
    assert s2.latest_ref_for_run(acb.run_id) == ref
    assert (s2.latest_ref_for_task(acb.run_id, acb.task_id,
                                   acb.logical_subgoal_id) == ref)


# ================================================================ S9-8
def test_s9_8_ref_unique_across_store_instances(tmp_path):
    """S9-8：两实例交替 save → seq 严格递增、ref 唯一、均可 load。"""
    root = tmp_path / "cp"
    s1 = FileCheckpointStore(root)
    s2 = FileCheckpointStore(root)
    assert s1.store_uuid == s2.store_uuid

    refs: list[str] = []
    for i in range(8):
        store = s1 if i % 2 == 0 else s2
        acb = _acb(acb_id=f"A-S9-8-{i}", subgoal="S-S9-8")
        refs.append(store.save(acb))

    seqs = [int(ref.split("-")[2]) for ref in refs]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)              # 严格递增、不重用
    assert len(set(refs)) == len(refs)              # ref 两两不同

    reader = FileCheckpointStore(root)
    for i, ref in enumerate(refs):
        assert reader.load_strict(ref)["acb_id"] == f"A-S9-8-{i}"
    assert reader.latest_ref_for_acb("A-S9-8-7") == refs[-1]


# ================================================================ S9-9
def test_s9_9_corrupt_payload_quarantined_and_not_repaired(tmp_path):
    """S9-9：篡改 payload → CheckpointCorrupt（带 header）→ recover 隔离。"""
    root = tmp_path / "cp"
    store = FileCheckpointStore(root)
    acb = _acb(acb_id="A-S9-9", run_id="R-S9-9", task_id="T-S9-9")
    ref = store.save(acb)

    ref_path = store._ref_path(ref)
    doc = json.loads(ref_path.read_text(encoding="utf-8"))
    doc["payload"]["staged"] = {"r": {"tampered": True}}   # payload_checksum 失配
    ref_path.write_text(json.dumps(doc), encoding="utf-8")

    strict = FileCheckpointStore(root)              # 新实例：绕过缓存
    with pytest.raises(CheckpointCorrupt) as excinfo:
        strict.load_strict(ref)
    assert excinfo.value.header["acb_id"] == "A-S9-9"
    assert excinfo.value.header["ref"] == ref
    assert excinfo.value.reason == "payload_checksum"

    h = Harness(checkpoints=FileCheckpointStore(root))
    recovered = h.recover(ref)
    assert recovered.id == "A-S9-9"
    assert recovered.state is TaskState.QUARANTINED
    assert recovered.failure == "CHECKPOINT_CORRUPT"
    assert recovered.needs_readmit is True

    # 不静默修复：后续严格读取仍然失败，且磁盘内容未被改写
    with pytest.raises(CheckpointCorrupt):
        FileCheckpointStore(root).load_strict(ref)


# ================================================================ S9-10
def test_s9_10_legacy_flat_payload_loadable(tmp_path):
    """S9-10：扁平旧格式（无 envelope）走兼容路径可加载。"""
    root = tmp_path / "cp"
    store = FileCheckpointStore(root)
    legacy_payload = {
        "acb_id": "A-LEGACY", "run_id": "R-LEGACY", "task_id": "T-LEGACY",
        "logical_subgoal_id": "", "generation": 1, "state": "RUNNING",
        "turn_seq": 1, "staged": {"r": "v1"}, "read_set": {}, "base_version": {},
        "cursor": 3, "materialized_cursor": 3, "pre_image": {},
        "last_committed_seq": 0, "aging_deadline": None,
        "effect_journal_offset": 0, "hitl_result": None,
        "rebase_attempts": 0, "retry_budget": 3, "reservation_ids": [],
        "needs_readmit": False, "slots_active": [], "budget_node_id": None,
        "budget_parent_id": None, "budget_limits": None, "budget_self": 0,
        "budget_subtree": 0, "budget_consumed": 0, "budget_folded_consumed": 0,
        "event_cursor": 3, "applied_event_ids": [], "failure": None,
        "spec": {}, "deadline": None, "no_progress_steps": 0,
    }
    ref = f"CP-{store.store_uuid[:8]}-{0:020d}-{'a' * 12}"
    ref_path = store._ref_path(ref)
    store._ensure_dir(ref_path.parent)
    ref_path.write_text(json.dumps(legacy_payload), encoding="utf-8")

    reader = FileCheckpointStore(root)
    assert reader.load(ref)["acb_id"] == "A-LEGACY"
    assert reader.load_strict(ref)["staged"] == {"r": "v1"}


# ================================================================ S9-11
def test_s9_11_codec_delete_singleton_and_reserved_key():
    """S9-11：DELETE 单例往返 + 保留键拒绝 + 严格类型。"""
    encoded = codec.encode(DELETE)
    assert encoded == {"__type__": "DELETE"}
    assert codec.decode(encoded) is DELETE
    assert codec.decode(codec.encode({"x": [DELETE]}))["x"][0] is DELETE
    assert codec.decode(codec.encode(b"\x00bytes")) == b"\x00bytes"

    assert copy.deepcopy(DELETE) is DELETE
    assert copy.copy(DELETE) is DELETE
    assert pickle.loads(pickle.dumps(DELETE)) is DELETE

    with pytest.raises(ValueError):
        codec.encode({"__type__": "DELETE"})        # 顶层保留键
    with pytest.raises(ValueError):
        codec.encode({"nested": [{"__type__": "DELETE"}]})   # 递归保留键

    for bad in ({1, 2}, (1, 2), object(), float("nan"), float("inf")):
        with pytest.raises(TypeError):
            codec.encode(bad)

    # blob.canonical_bytes 与 codec 同严格度（禁止 default=str / NaN）
    with pytest.raises(ValueError):
        canonical_bytes({"__type__": "DELETE"})
    with pytest.raises(TypeError):
        canonical_bytes({1, 2})
    with pytest.raises(TypeError):
        canonical_bytes(float("nan"))
