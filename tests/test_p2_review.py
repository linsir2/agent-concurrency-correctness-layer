"""对抗评审 **P2 清单**回归用例（`docs/s9-persistence-and-receipts.md` 续）。

覆盖 P2-1..P2-7 的**修复后**判据（每条独立复现 + 断言隔离/原子行为）：

- P2-1 ref 写入非原子 → `tests/test_s9_sigkill.py`（阶段 barrier 走真实 `_write_new`）
- P2-2 `next_seq` 修正不落盘 → seq 复用
- P2-3 latest 指针无单调 CAS + 32bit hash + 不校验 acb_id
- P2-4 `_receipt_ids` 重建不校验链连续性
- P2-5 scope 锁跨进程不互斥 + dedup 检查在锁外
- P2-6 畸形收据事件让启动/验签抛 `KeyError`
- P2-7 blob 读不校验内容哈希
"""

from __future__ import annotations

import json

import pytest
from conftest import _acb, make_harness, seed

from harness import ACB, EventKind, Harness, Mode, ReceiptConflict, Status, Write
from harness.checkpoint import FileCheckpointStore
from harness.event_log import WalEventLog
from harness.spec.receipt import ReceiptMalformed, build_receipt
from harness.storage.blob import BlobCorrupt, FileBlobStore, canonical_bytes


def _mk_acb(acb_id: str, run_id: str = "R-P2", gen: int = 1) -> ACB:
    acb = ACB(id=acb_id, run_id=run_id, task_id="T-P2",
              logical_subgoal_id="S-P2")
    acb.generation = gen
    acb.staged = {"r": {"v": 1}}
    return acb


# ================================================================ P2-2 seq 复用
def test_p2_2_stale_meta_next_seq_persisted(tmp_path):
    """P2-2：`_init_meta` 修正 `next_seq` 必须落盘 → 重开后 `save` 不复用 seq。

    复现：save 后手工把 `store.json` 的 `next_seq` 改回旧值（stale meta），
    重开 store 再 save —— 修复前 `_init_meta` 只改内存不落盘，`save` 重读磁盘
    stale 值 → 分配到已用 seq（撞 ref 文件）；修复后修正立即回写，seq 严格递增。
    """
    root = tmp_path / "cp"
    store = FileCheckpointStore(root)
    ref0 = store.save(_mk_acb("A-P2-2-0"))
    seq0 = int(ref0.split("-")[2])

    # 手工把 next_seq 退回旧值（模拟 stale meta）
    meta_path = store._meta_path
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["next_seq"] = 0
    meta_path.write_text(json.dumps(meta), encoding="utf-8")

    reopened = FileCheckpointStore(root)          # `_init_meta` 修正并落盘
    assert int(reopened._meta["next_seq"]) >= seq0 + 1

    ref1 = reopened.save(_mk_acb("A-P2-2-1"))
    seq1 = int(ref1.split("-")[2])
    assert seq1 > seq0, "seq 不得复用（stale meta 不得串号）"
    assert reopened.load_strict(ref1)["acb_id"] == "A-P2-2-1"
    # 修正确实回写到了磁盘（新实例读到的也是修正值）
    on_disk = json.loads(meta_path.read_text(encoding="utf-8"))
    assert int(on_disk["next_seq"]) >= seq0 + 1


# ================================================================ P2-3 指针单调/hash/校验
def test_p2_3_pointer_hash_is_64bit(tmp_path):
    """P2-3：指针文件名 hash 加长到 sha256 前 16 hex（64bit）。"""
    store = FileCheckpointStore(tmp_path / "cp")
    name = store._latest_path("latest", "A-P2-3").name
    stem = name[: -len(".json")]
    assert len(stem) == 16
    assert all(ch in "0123456789abcdef" for ch in stem)


def test_p2_3_lagging_writer_cannot_regress_latest(tmp_path):
    """P2-3：滞后写者（更低 generation/save_seq）不得把 latest 回退。"""
    store = FileCheckpointStore(tmp_path / "cp")
    a1 = _mk_acb("A-P2-3", gen=1)
    ref1 = store.save(a1)
    a2 = _mk_acb("A-P2-3", gen=2)
    ref2 = store.save(a2)
    assert store.latest_ref_for_acb("A-P2-3") == ref2

    # 滞后写者：拿旧 ref + 更低 (generation, save_seq) 回写同一指针
    path = store._latest_path("latest", "A-P2-3")
    store._write_pointer(path, ref1, a1, int(ref1.split("-")[2]), 1)
    assert store.latest_ref_for_acb("A-P2-3") == ref2, \
        "latest 指针不得回退到旧 ref"

    # 更新（更高）写者仍可前进
    a3 = _mk_acb("A-P2-3", gen=3)
    ref3 = store.save(a3)
    assert store.latest_ref_for_acb("A-P2-3") == ref3


def test_p2_3_pointer_validates_acb_id(tmp_path):
    """P2-3：`_read_pointer` 校验 `acb_id` → hash 碰撞串号不得误报 latest。"""
    store = FileCheckpointStore(tmp_path / "cp")
    a = _mk_acb("A-P2-3A")
    store.save(a)
    # 把「B 的指针」写到 A 的指针路径（模拟 hash 碰撞）
    path = store._latest_path("latest", "A-P2-3A")
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["acb_id"] = "A-P2-3B"
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert store.latest_ref_for_acb("A-P2-3A") is None, \
        "键不匹配的指针不得返回 ref（防串号）"
    assert store.latest_ref_for_acb("A-P2-3B") is None


# ================================================================ P2-4 收据链连续性
def test_p2_4_rebuild_detects_duplicate_receipt_id(tmp_path):
    """P2-4：重建时重复 `receipt_id` → 标 tainted → `verify` False 且拒绝扩展。"""
    h = make_harness(auth_secret="s3cr3t")
    acb = _acb(h, "r")
    seed(h, "r", {"v": 0})
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": 1}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK
    scope = (acb.run_id, acb.logical_subgoal_id)
    original = h.receipts_of(*scope)[0]
    assert h.verify_receipts(*scope) is True

    # 手工追加一条**同 receipt_id** 的重复收据事件
    dup = dict(original.to_dict())
    dup["prev_receipt_hash"] = "0" * 16          # 乱序 + 重复 id
    h.event_log.append(EventKind.RECEIPT_ISSUED, dup["receipt_id"], dup,
                       producer=acb.id, run_id=acb.run_id)

    h2 = Harness(event_log=h.event_log, auth_secret="s3cr3t")
    assert scope in h2._tainted_scopes, "重复 receipt_id 必须标 tainted"
    assert h2.verify_receipts(*scope) is False
    # 拒绝继续扩展坏链
    acb2 = _acb(h2, "r2")
    h2.workspace.stage(acb2, "r2", {"v": 1})
    with pytest.raises(ReceiptConflict):
        h2.commit_batch(acb2, [Write("r2")])


def test_p2_4_rebuild_detects_out_of_order_chain(tmp_path):
    """P2-4：`prev_receipt_hash` 接不上 → 标 tainted（不得静默折叠）。"""
    h = Harness(auth_secret="s3cr3t")
    r1 = build_receipt(receipt_id="R1", run_id="R", task_id="T",
                       logical_subgoal_id="S", resource="r",
                       version_before=0, version_after=1, secret="s3cr3t")
    r2 = build_receipt(receipt_id="R2", run_id="R", task_id="T",
                       logical_subgoal_id="S", resource="r",
                       version_before=1, version_after=2,
                       prev_receipt_hash="deadbeef" + "0" * 8,   # 断链
                       secret="s3cr3t")
    for r in (r1, r2):
        h.event_log.append(EventKind.RECEIPT_ISSUED, r.receipt_id, r.to_dict(),
                           producer="A", run_id=r.run_id)
    h._rebuild_receipts()
    assert ("R", "S") in h._tainted_scopes
    assert h.verify_receipts("R", "S") is False


# ================================================================ P2-5 dedup 在锁内
def _run_pair(fn1, fn2) -> list:
    """并发跑两个 callable，返回结果列表（断言无线程泄漏/异常）。"""
    import threading
    results: list = []
    errors: list = []

    def worker(fn):
        try:
            results.append(fn())
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(fn1,)),
               threading.Thread(target=worker, args=(fn2,))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not any(t.is_alive() for t in threads)
    assert errors == [], f"并发执行出现异常：{errors}"
    return results


def test_p2_5_dedup_and_head_inside_scope_critical_section():
    """P2-5 正对照：同 scope 不同资源并发提交 → 去重/head 读取在临界区内 → 链不分叉。

    负对照（scope 锁换 `nullcontext()` → 链分叉）见
    `tests/test_s9_receipts.py::test_s9_5_negative_control_nullcontext_forks_chain`。
    """
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "r1", {"v": 0})
    seed(h, "r2", {"v": 0})
    a = _acb(h, "r1")
    b = _acb(h, "r2")
    a.base_version["r1"] = 1
    a.staged["r1"] = {"v": 1}
    b.base_version["r2"] = 1
    b.staged["r2"] = {"v": 1}

    results = _run_pair(lambda: h.commit_batch(a, [Write("r1")]),
                        lambda: h.commit_batch(b, [Write("r2")]))
    assert all(r == Status.OK for r in results), f"并发提交应成功：{results}"

    receipts = h.receipts_of(a.run_id, a.logical_subgoal_id)
    assert len(receipts) == 2, "同 scope 两资源应各得一条收据"
    assert receipts[0].prev_receipt_hash != receipts[1].prev_receipt_hash, \
        "收据必须接在彼此的 head 上（不得同 head 分叉）"
    assert h.verify_receipts(a.run_id, a.logical_subgoal_id) is True


# ================================================================ P2-6 畸形收据
def test_p2_6_malformed_receipt_does_not_break_startup(tmp_path):
    """P2-6：缺 `run_id` 的 `RECEIPT_ISSUED` 事件不得让 Harness 启动/验签抛。"""
    h = make_harness(auth_secret="s3cr3t")
    # 一条畸形事件：缺 run_id
    h.event_log.append(EventKind.RECEIPT_ISSUED, "RID-BAD",
                       {"receipt_id": "RID-BAD", "task_id": "T"},   # 缺 run_id
                       producer="A", run_id="")
    # 缺 task_id
    h.event_log.append(EventKind.RECEIPT_ISSUED, "RID-BAD2",
                       {"receipt_id": "RID-BAD2", "run_id": "R-BAD"},
                       producer="A", run_id="R-BAD")

    # 新实例（模拟重启）→ 不得抛
    h2 = Harness(event_log=h.event_log, auth_secret="s3cr3t")
    assert h2.receipts_of("R-BAD") == [], "畸形事件不得进入 receipts_of"
    assert h2._malformed_receipts, "畸形事件必须被记录（不静默）"
    assert ("R-BAD", "") in h2._tainted_scopes
    assert h2.verify_receipts("R-BAD", "") is False


def test_p2_6_receipt_from_dict_raises_malformed_not_keyerror():
    """P2-6：`from_dict` 缺必填字段 → `ReceiptMalformed`（而非 `KeyError`）。"""
    with pytest.raises(ReceiptMalformed):
        from harness.spec.receipt import Receipt
        Receipt.from_dict({"receipt_id": "R", "task_id": "T"})   # 缺 run_id


# ================================================================ P2-7 blob 内容哈希
def test_p2_7_blob_get_validates_content_hash(tmp_path):
    """P2-7：`FileBlobStore.get` 校验 sha256(data) == blob_hash，失配报错。"""
    store = FileBlobStore(tmp_path / "blobs")
    data = b"hello-p2"
    blob_hash = store.put(data)
    assert store.get(blob_hash) == data

    path = store._path(blob_hash)
    path.write_bytes(b"hello-p2!")               # 位翻转/坏盘
    with pytest.raises(BlobCorrupt):
        store.get(blob_hash)


def test_p2_7_missing_blob_isolated_not_silent(tmp_path):
    """P2-7：重建遇缺失 blob → 隔离（记入 `isolated_resources`），不静默。"""
    from harness import IntentRegistry, WorkspaceManager

    log = WalEventLog(str(tmp_path / "wal.log"))
    ws = WorkspaceManager(log, IntentRegistry())
    ws.put("r", {"v": 1})                        # 产生 blob

    # 新 workspace 用**空** blob store 重建 → blob 缺失
    ws2 = WorkspaceManager(log, IntentRegistry(), blobs=FileBlobStore(tmp_path / "empty"))
    assert "r" in ws2.isolated_resources, "缺失 blob 必须被隔离记录（不静默）"
    assert ws2.get("r") is None


# ================================================================ P2-C 冲突隔离 + 对账
def test_p2_c_receipt_conflict_isolates_scope_and_reconciles():
    """P2-C：ReceiptConflict 必须隔离 scope（verify False）+ `reconcile_scope` 可恢复。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "b", {"v": 0})
    acb = _acb(h, "b")
    acb.base_version["b"] = 1
    acb.staged["b"] = {"v": 1}
    assert h.commit_batch(acb, [Write("b")]) == Status.OK
    scope = (acb.run_id, acb.logical_subgoal_id)

    # 强制 dedup 失效 → 冲突（提交事件仍在，去重被绕过）
    orig = h.receipts.dedup_event
    h.receipts.dedup_event = lambda *a, **k: None
    try:
        with pytest.raises(ReceiptConflict):
            h.commit_batch(acb, [Write("b")])
    finally:
        h.receipts.dedup_event = orig
    assert scope in h._tainted_scopes, "冲突必须把 scope 加入 _tainted_scopes"
    assert h.verify_receipts(*scope) is False, "隔离后 verify 必须为 False"

    # 对账 → 解除隔离
    st = h.reconcile_scope(*scope)
    assert st == Status.OK and st.detail["backfilled"] == 0
    assert scope not in h._tainted_scopes
    # 重试同逻辑写 → dedup 命中（不再永久冲突）
    acb.base_version["b"] = h.workspace.version("b")
    st2 = h.commit_batch(acb, [Write("b")])
    assert st2 == Status.OK and st2.detail.get("dedup") is True
    assert h.verify_receipts(*scope) is True


def test_p2_c_reconcile_backfills_missing_commit_event():
    """P2-C：提交事件缺失（收据仍在）→ `reconcile_scope` 补写 → 重试去重命中。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "r", {"v": 1})
    acb = _acb(h, "r")
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": 2}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK     # v2，E1 + R1
    scope = (acb.run_id, acb.logical_subgoal_id)
    commit_key = h._commit_key(acb, "r", h.blobs.put(canonical_bytes({"v": 2})),
                               Mode.COMMIT)

    # 模拟提交事件丢失（收据仍在）：从内存事件日志移除 RESOURCE_COMMITTED，
    # 但保留 workspace manifest（投影已发生，版本仍是 2）
    h.event_log._events = [e for e in h.event_log._events
                           if e.kind != EventKind.RESOURCE_COMMITTED]
    h.event_log._by_subject = {}
    for e in h.event_log._events:
        h.event_log._by_subject.setdefault(e.subject, []).append(e)
    h._rebuild_receipts()
    assert commit_key in h._receipt_ids
    assert h.workspace.version("r") == 2

    # 重试 → 冲突（dedup 找不到提交事件）
    acb.base_version["r"] = h.workspace.version("r")
    with pytest.raises(ReceiptConflict):
        h.commit_batch(acb, [Write("r")])
    assert scope in h._tainted_scopes

    # 对账：补写缺失的 RESOURCE_COMMITTED → 解除隔离
    st = h.reconcile_scope(*scope)
    assert st == Status.OK and st.detail["backfilled"] == 1
    assert scope not in h._tainted_scopes
    # 重试 → dedup 命中补写的事件
    acb.base_version["r"] = h.workspace.version("r")
    st2 = h.commit_batch(acb, [Write("r")])
    assert st2 == Status.OK and st2.detail.get("dedup") is True


def test_p2_c_reconcile_needs_manual_when_resource_overwritten():
    """P2-C：资源已被后续写覆盖 → 无法补写 → `NEEDS_RECONCILIATION`（需人工）。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "r", {"v": 1})
    acb = _acb(h, "r")
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": 2}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK     # v2，R1（version_after=2）
    scope = (acb.run_id, acb.logical_subgoal_id)

    # 资源被后续写推进到 v3 → 收据对应的版本已不是当前版本
    acb.base_version["r"] = 2
    acb.staged["r"] = {"v": 3}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK     # v3

    # 移除 v2 的提交事件（模拟丢失），保留收据
    h.event_log._events = [e for e in h.event_log._events
                           if not (e.kind == EventKind.RESOURCE_COMMITTED
                                   and (e.payload or {}).get("version") == 2)]
    h.event_log._by_subject = {}
    for e in h.event_log._events:
        h.event_log._by_subject.setdefault(e.subject, []).append(e)
    h._rebuild_receipts()
    assert h.workspace.version("r") == 3

    st = h.reconcile_scope(*scope)
    assert st == Status.NEEDS_RECONCILIATION
    assert st.detail["reason"] == "resource_overwritten"
    assert scope in h._tainted_scopes


# ================================================================ P3-E tmp 兜底清理
def test_p3_e_write_new_cleans_tmp_on_non_eexist_link_error(tmp_path, monkeypatch):
    """P3-E：`os.link` 抛非 FileExistsError（ENOSPC/EIO/EXDEV）→ tmp 必须被清理。"""
    import harness.checkpoint as cp

    store = FileCheckpointStore(tmp_path / "cp")
    path = store._ref_path("CP-" + "0" * 8 + "-00000000000000000000-" + "a" * 12)
    store._ensure_dir(path.parent)
    tmp = path.with_name(path.name + ".tmp")

    def boom_link(*a, **k):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(cp.os, "link", boom_link)
    with pytest.raises(OSError):
        store._write_new(path, b"payload")
    assert not tmp.exists(), "link 失败必须清理 tmp（否则污染 _has_ref_files）"
    assert not path.exists()


def test_p3_e_has_ref_files_ignores_tmp(tmp_path):
    """P3-E：只剩残留 `.tmp` 时 `_has_ref_files` 必须为 False（不误判 refs 非空）。"""
    root = tmp_path / "cp"
    store = FileCheckpointStore(root)
    path = store._ref_path("CP-" + "0" * 8 + "-00000000000000000000-" + "a" * 12)
    store._ensure_dir(path.parent)
    path.with_name(path.name + ".tmp").write_bytes(b"half")
    assert store._has_ref_files() is False
    # store.json 缺失 + 只剩残留 tmp → 重开不得抛（旧实现误判 refs 非空 → RuntimeError）
    store._meta_path.unlink()
    reopened = FileCheckpointStore(root)          # 不得抛
    assert reopened.store_uuid
    # 对照：真实 ref 存在且 store.json 缺失 → 必须拒绝静默重建
    root2 = tmp_path / "cp2"
    store2 = FileCheckpointStore(root2)
    store2.save(_mk_acb("A-P3E-REF"))
    store2._meta_path.unlink()
    with pytest.raises(RuntimeError):
        FileCheckpointStore(root2)


# ================================================================ P3-F latest_run/task seq 优先
def test_p3_f_latest_run_seq_priority_over_generation(tmp_path):
    """P3-F：`latest_run` 以 save_seq 为权威 → 「seq 更大但 gen 更小」的合法保存不被挡。"""
    store = FileCheckpointStore(tmp_path / "cp")
    a1 = _mk_acb("A-P3F-1", run_id="R-P3F", gen=2)      # 先保存 → seq=0
    ref1 = store.save(a1)
    assert store.load_strict(ref1)["acb_id"] == "A-P3F-1"
    a2 = _mk_acb("A-P3F-2", run_id="R-P3F", gen=1)      # 更晚保存 → seq=1（更大）
    ref2 = store.save(a2)
    # save_seq 权威 → latest_run / latest_task 指向更晚保存的 ref2
    assert ref1 != ref2
    assert store.latest_ref_for_run("R-P3F") == ref2
    assert store.latest_ref_for_task("R-P3F", "T-P2", "S-P2") == ref2


def test_p3_f_latest_acb_generation_still_authoritative(tmp_path):
    """P3-F：`latest`（per ACB）仍以 generation 为权威（fence 旧代际，不回退）。"""
    store = FileCheckpointStore(tmp_path / "cp")
    a1 = _mk_acb("A-P3F-G", gen=1)
    ref1 = store.save(a1)
    a2 = _mk_acb("A-P3F-G", gen=2)
    ref2 = store.save(a2)
    assert store.latest_ref_for_acb("A-P3F-G") == ref2
    # 滞后写者（更低 generation + 更低 seq）不得回退
    path = store._latest_path("latest", "A-P3F-G")
    store._write_pointer(path, ref1, a1, 0, 1)
    assert store.latest_ref_for_acb("A-P3F-G") == ref2
