"""对抗评审清单 8 条反例的回归用例（P0-1/2/3、P1-4/5/6/7/8）。

与 `scripts/repro_review_counterexamples.py` 一一对应：修复前全部 FAIL，
修复后全部 PASS。每条都给出「反例构造 → 断言」，不放宽任何既有断言。

- P0-1 `commit_batch` dedup 分支静默丢写（不补投影）
- P0-2 幂等键含可变 `base_version` → 刷新后重试去重失效
- P0-3 all-or-nothing dedup 部分命中 → 重复收据 + 收据冲突未隔离
- P1-4 嵌套 bytes 被裸 `json.loads` 读回成 dict
- P1-5 `stage` 非法内容未 fail-fast → `_finalize` 落盘抛 → 槽位泄漏
- P1-6 quarantine 后 latest 悬空 → `recover` 造幽灵 ACB
- P1-7 retention 跳 floor 丢 `[cursor, floor)` 的订阅事件
- P1-8 `recover` 不恢复 subscriptions/materialized → context_view 为空
"""

from __future__ import annotations

import pytest
from conftest import _acb, admit, decision, make_harness, seed, stage

from harness import (ACB, EventKind, Harness, Mode, ReceiptConflict, Status,
                     TaskState, WorkspaceManager, Write)
from harness.checkpoint import FileCheckpointStore
from harness.context import ContextAssembler
from harness.event_log import EventLog
from harness.runtime import CrashInjected
from harness.spec.receipt import verify_chain_detailed
from harness.storage.blob import FileBlobStore, canonical_bytes


def _count(h, kind: EventKind) -> int:
    return len([e for e in h.event_log.all() if e.kind == kind])


def _crash_before_projection(h, times: int = 1):
    """注入：第 N 次 `_apply_locked` 抛 `CrashInjected`（模拟 append 后、投影前崩溃）。"""
    orig = h.workspace._apply_locked
    state = {"n": 0}

    def boom(*a, **k):
        if state["n"] < times:
            state["n"] += 1
            raise CrashInjected(0, 0)
        return orig(*a, **k)

    h.workspace._apply_locked = boom
    return orig


# ================================================================ P0-1
def test_review_p0_1_dedup_reprojects_and_does_not_swallow_new_write():
    """P0-1：dedup 命中必须补投影；后续基于对齐版本的新写不得被误短路。"""
    h = make_harness()
    seed(h, "r", {"v": 0})
    acb = _acb(h, "r")
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": 1}

    orig = _crash_before_projection(h)
    try:
        with pytest.raises(CrashInjected):
            h.commit_batch(acb, [Write("r")])
    finally:
        h.workspace._apply_locked = orig
    assert h.workspace.version("r") == 1, "前提：事件已提交但投影未发生"

    st = h.commit_batch(acb, [Write("r")])
    assert st == Status.OK and st.detail.get("dedup") is True
    assert h.workspace.get("r") == {"v": 1}, "dedup 必须补投影（否则 manifest 过期）"

    h.workspace.stage(acb, "r", {"v": 2})
    st2 = h.commit_batch(acb, [Write("r")])
    assert st2 == Status.OK and st2.detail.get("dedup") is not True, "新内容不得被短路"
    assert h.workspace.get("r") == {"v": 2}, "新内容不得静默丢失"


# ================================================================ P0-2
def test_review_p0_2_commit_key_stable_across_refresh():
    """P0-2：幂等键不含 base_version → 刷新后重试仍去重（不重复提交/收据）。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "r", {"v": 0})
    acb = _acb(h, "r")
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": 1}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK

    acb.base_version["r"] = h.workspace.version("r")     # 运行期刷新
    n_commit = _count(h, EventKind.RESOURCE_COMMITTED)
    n_receipt = _count(h, EventKind.RECEIPT_ISSUED)

    st = h.commit_batch(acb, [Write("r")])
    assert st == Status.OK and st.detail.get("dedup") is True
    assert _count(h, EventKind.RESOURCE_COMMITTED) == n_commit, "不得重复提交"
    assert _count(h, EventKind.RECEIPT_ISSUED) == n_receipt, "不得重复收据"


# ================================================================ P0-3
def test_review_p0_3_partial_dedup_no_duplicate_receipt():
    """P0-3：部分命中时逐写过滤——已去重的写只补投影，不重复 append 收据。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "a", {"v": 0})
    seed(h, "b", {"v": 0})
    acb = _acb(h, "a")
    acb.base_version["a"] = 1
    acb.base_version["b"] = 1
    acb.staged["a"] = {"v": 1}
    acb.staged["b"] = {"v": 1}

    orig = _crash_before_projection(h)
    try:
        with pytest.raises(CrashInjected):
            h.commit_batch(acb, [Write("a")])
    finally:
        h.workspace._apply_locked = orig
    assert h.workspace.version("a") == 1, "前提：a 已提交但未投影"

    st = h.commit_batch(acb, [Write("a"), Write("b")])   # a 命中、b 新增
    assert st == Status.OK
    assert h.workspace.get("a") == {"v": 1}
    assert h.workspace.get("b") == {"v": 1}

    receipts = h.receipts_of(acb.run_id, acb.logical_subgoal_id)
    ids = [r.receipt_id for r in receipts]
    assert len(ids) == len(set(ids)), f"收据 receipt_id 重复：{ids}"
    assert h.verify_receipts(acb.run_id, acb.logical_subgoal_id) is True


def test_review_p0_3_receipt_conflict_isolated(monkeypatch):
    """P0-3：append 前断言 receipt_id 不冲突；冲突抛 `ReceiptConflict`（隔离该 scope）。

    P2-C：冲突后 scope 必须进入 `_tainted_scopes` → `verify_receipts` 对该 scope
    返回 `False`（不再只隔离 ACB）；且必须存在 `reconcile_scope` 对账入口。
    """
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "b", {"v": 0})
    acb = _acb(h, "b")
    acb.base_version["b"] = 1
    acb.staged["b"] = {"v": 1}
    assert h.commit_batch(acb, [Write("b")]) == Status.OK

    commit_key = h._commit_key(acb, "b", h.blobs.put(canonical_bytes({"v": 1})),
                               Mode.COMMIT)
    assert commit_key in h._receipt_ids, "前提：收据已签发"
    # 模拟「提交事件丢失、但收据仍在」→ 去重失效，append 前断言必须拦住
    monkeypatch.setattr(h, "_dedup_event", lambda *a, **k: None)

    with pytest.raises(ReceiptConflict):
        h.commit_batch(acb, [Write("b")])
    assert _count(h, EventKind.RESOURCE_COMMITTED) == 1, "冲突后不得追加提交事件"
    scope = (acb.run_id, acb.logical_subgoal_id)
    assert scope in h._tainted_scopes, "冲突必须隔离 scope（P2-C）"
    assert h.verify_receipts(*scope) is False, "隔离后 verify 必须为 False（P2-C）"
    # 链本身未被破坏（冲突在 append 前被拦住）：直接校验链连续 + 签名
    receipts = h.receipts_of(*scope)
    assert verify_chain_detailed(receipts, "s3cr3t") is True, "收据链本身未被破坏"
    assert callable(getattr(h, "reconcile_scope", None)), "缺少对账入口（P2-C）"


# ================================================================ P1-A
def test_review_p1_a_blob_corrupt_isolated_not_raises(tmp_path):
    """P1-A：位翻转单个 blob → 重建 / Harness 启动**不抛**；损坏资源被隔离，其他资源正常。"""
    blobs = FileBlobStore(tmp_path / "blobs")
    h = make_harness(blobs=blobs)
    seed(h, "r", {"v": 1})
    seed(h, "r2", {"v": 2})

    bh = h.workspace.content_hash("r")
    path = blobs._path(bh)
    raw = bytearray(path.read_bytes())
    raw[0] ^= 0x01                                   # 位翻转
    path.write_bytes(bytes(raw))

    # 重建（新 workspace 实例）：不得抛
    ws = WorkspaceManager(h.event_log, h.intents, blobs=blobs)
    assert "r" in ws.isolated_resources, "损坏 blob 的资源必须被隔离"
    assert ws.get("r2") == {"v": 2}, "其他资源必须正常"
    # Harness 启动不得抛
    Harness(event_log=h.event_log, blobs=blobs)
    # 其他资源仍可正常写入
    ws.put("r2", {"v": 3})
    assert ws.get("r2") == {"v": 3}


def test_review_p1_a_corrupt_prev_blob_isolated(tmp_path):
    """P1-A：前像 blob（prev_hash 分支）损坏同样隔离，不打挂启动。"""
    blobs = FileBlobStore(tmp_path / "blobs")
    h = make_harness(blobs=blobs)
    seed(h, "r", {"v": 1})
    b1 = h.workspace.content_hash("r")               # v1 的 blob
    acb = _acb(h, "r")
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": 2}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK    # v2（prev_hash = b1）

    p1 = blobs._path(b1)
    raw = bytearray(p1.read_bytes())
    raw[0] ^= 0x01
    p1.write_bytes(bytes(raw))

    ws = WorkspaceManager(h.event_log, h.intents, blobs=blobs)
    assert "r" in ws.isolated_resources, "前像损坏必须隔离该资源"
    Harness(event_log=h.event_log, blobs=blobs)      # 启动不得抛


# ================================================================ P1-B
def test_review_p1_b_stale_content_dedup_not_swallowed():
    """P1-B：陈旧内容去重 → 新写必须生效（值变回 A、version 前进），且不永久冲突。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "r", {"v": "A"})                 # v1 = A
    acb = _acb(h, "r")
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": "A"}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK      # v2 = A

    acb.base_version["r"] = 2
    acb.staged["r"] = {"v": "B"}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK      # v3 = B

    acb.base_version["r"] = 3
    acb.staged["r"] = {"v": "A"}
    st = h.commit_batch(acb, [Write("r")])                     # 应为新写 v4 = A
    assert st == Status.OK
    assert st.detail.get("dedup") is not True, "陈旧内容不得命中 dedup"
    assert h.workspace.get("r") == {"v": "A"}, "新写必须生效"
    assert h.workspace.version("r") == 4, "version 必须前进"
    # 合法重写应换 receipt_id（不得永久冲突）；链仍可验证
    receipts = h.receipts_of(acb.run_id, acb.logical_subgoal_id)
    ids = [r.receipt_id for r in receipts]
    assert len(ids) == len(set(ids)), f"收据 receipt_id 重复：{ids}"
    assert h.verify_receipts(acb.run_id, acb.logical_subgoal_id) is True


# ================================================================ P1-4
def test_review_p1_4_nested_bytes_roundtrip_and_rebase():
    """P1-4：嵌套 bytes 提交 → 读回（含重建）→ rebase 全程保持 bytes 类型。"""
    h = make_harness()
    seed(h, "b", {"x": b"abc"})
    plan = h.planner.plan("NB", [{"task_id": "T1", "contingent": ["b"]}])
    h.plans["NB"] = plan
    acb = admit(h, "NB", plan, "T1")

    h.workspace.stage(acb, "b", {"x": b"abd"})
    assert h.commit_batch(acb, [Write("b")]) == Status.OK

    out = h.workspace.get("b")
    assert out == {"x": b"abd"}, f"嵌套 bytes 读回被损坏：{out!r}"
    assert isinstance(out["x"], bytes)

    # 重建实例（同一 EventLog + blob）后仍是 bytes
    from harness import WorkspaceManager
    ws2 = WorkspaceManager(h.event_log, h.intents, blobs=h.blobs)
    assert ws2.get("b") == {"x": b"abd"}
    assert isinstance(ws2.get("b")["x"], bytes)

    # rebase（merge3）能处理嵌套 bytes
    base, local, committed = {"x": b"abc"}, {"x": b"abd"}, {"x": b"abc"}
    assert h.workspace.rebase_json(base, local, committed)["merged"] == {"x": b"abd"}


# ================================================================ P1-5
def test_review_p1_5_stage_fail_fast_and_no_slot_leak():
    """P1-5：stage 非法内容 fail-fast；非法内容提交后槽位/预算必须结算（不泄漏）。"""
    h = make_harness()
    seed(h, "r", {"v": 0})
    plan = h.planner.plan("CE5", [{"task_id": "T1", "min_budget": 10}])
    h.plans["CE5"] = plan
    acb = admit(h, "CE5", plan, "T1")

    res = h.workspace.stage(acb, "r", {1, 2})
    assert res == Status.INVALID_INPUT and res.detail["reason"] == "bad_content"
    assert acb.staged == {}, "非法内容不得进入 staged"
    h.slots.release(acb.id)
    for rid in list(acb.reservation_ids):
        h.budget.settle(rid)
    if acb.budget_node is not None:
        h.budget.archive_node(acb.budget_node.node_id)
        acb.budget_node = None

    acb2 = h.dispatch_and_run(plan, "T1", [decision(1, stage("r", {1, 2}))])
    assert acb2.state is TaskState.FAILED
    assert h.slots.active == 0, "槽位必须被释放（不得泄漏）"
    assert h.budget.root.remaining == h.budget.root.limits, "预算必须结算归还"
    h.budget.check_invariants()


def test_review_p1_5_try_admit_rejects_bad_content():
    """P1-5：try_admit 对不可编码的用户内容 fail-fast（INVALID_INPUT + 零残留）。"""
    h = make_harness()
    plan = h.planner.plan("CE5B", [{"task_id": "T1", "min_budget": 10}])
    plan["tasks"]["T1"]["merge_policy"] = {"a": {1, 2}}      # set → 不可编码
    h.plans["CE5B"] = plan
    before = h.slots.active
    acb = h.dispatcher.try_admit(plan["tasks"]["T1"], "CE5B")
    assert acb.state is TaskState.INADMISSIBLE
    assert acb.failure == "INVALID_INPUT"
    assert acb.spec["admission_error"] == "bad_task_content"
    assert h.slots.active == before, "准入失败不得占用槽位"
    assert acb.reservation_ids == [] and acb.budget_node is None


# ================================================================ P1-6
def test_review_p1_6_quarantine_rebuilds_latest_no_ghost_acb(tmp_path):
    """P1-6：quarantine 后 latest 不悬空；recover 不造幽灵 ACB（acb_id=''）。"""
    root = tmp_path / "cp"
    acb = ACB(id="A-CE6", run_id="R-CE6", task_id="T-CE6")
    store = FileCheckpointStore(root)
    ref = store.save(acb)
    store._ref_path(ref).write_text("not-json{{{", encoding="utf-8")   # 破坏 ref

    store2 = FileCheckpointStore(root)                    # 触发 quarantine
    assert store2.latest_ref_for_acb(acb.id) is None, "latest 不得悬空"

    h = Harness(checkpoints=FileCheckpointStore(root))
    rec = h.recover(ref)
    assert rec.id == acb.id, f"不得构造幽灵 ACB，实际 id={rec.id!r}"
    assert "" not in h.acbs
    assert rec.state is TaskState.QUARANTINED


def test_review_p1_6_invalid_ref_is_checkpoint_corrupt(tmp_path):
    """P1-6：非法 ref → `CheckpointCorrupt(bad_ref)`（不再裸 ValueError）。"""
    store = FileCheckpointStore(tmp_path / "cp")
    from harness import CheckpointCorrupt
    with pytest.raises(CheckpointCorrupt) as excinfo:
        store.load_strict("not-a-ref")
    assert excinfo.value.reason == "bad_ref"


# ================================================================ P1-7
def test_review_p1_7_retention_jump_requires_watermark():
    """P1-7：快照非空但水位 < floor → 不跳，从 cursor 老实重放（不丢订阅状态）。"""
    log = EventLog()
    for i in range(3):                       # seq1..3：订阅资源 r
        log.append(EventKind.RESOURCE_COMMITTED, "r",
                   {"content": {"i": i}, "version": i})
    for i in range(3):                       # seq4..6：别的 subject
        log.append(EventKind.RESOURCE_COMMITTED, "other",
                   {"content": i, "version": i})

    class _WS:
        def get(self, resource):
            return None

    acb = ACB(id="A-CE7", run_id="R-CE7", task_id="T-CE7")
    acb.subscriptions.add("r")
    acb.cursor = 0
    acb.materialized = {"r": "SNAP"}
    acb.materialized_watermark = 0           # 快照不含 [cursor, floor)

    asm = ContextAssembler(log, _WS(), retention=3)
    view = asm.context_view(acb)
    assert asm.snapshots_forced == 0, "水位不足不得跳"
    assert asm.retention_bypassed == 1
    assert view["r"] == {"i": 2}, f"跳 floor 丢了订阅事件，实际 {view['r']!r}"


def test_review_p1_7_retention_jump_allowed_when_watermark_covers_floor():
    """P1-7 正对照：快照水位 >= floor → 允许跳（快照已含前缀）。"""
    log = EventLog()
    for i in range(6):
        log.append(EventKind.RESOURCE_COMMITTED, "other",
                   {"content": i, "version": i})

    class _WS:
        def get(self, resource):
            return None

    acb = ACB(id="A-CE7B", run_id="R-CE7B", task_id="T-CE7B")
    acb.subscriptions.add("r")
    acb.cursor = 0
    acb.materialized = {"r": "SNAP"}
    acb.materialized_watermark = log.tail()  # 水位覆盖到 tail（>= floor）

    asm = ContextAssembler(log, _WS(), retention=3)
    view = asm.context_view(acb)
    assert asm.snapshots_forced == 1
    assert view["r"] == "SNAP"


# ================================================================ P1-8
def test_review_p1_8_recover_restores_subscriptions_materialized():
    """P1-8：recover 恢复 subscriptions/materialized（+水位）→ context_view 非空。"""
    h = make_harness()
    seed(h, "r", {"v": 0})
    plan = h.planner.plan("CE8", [{"task_id": "T1", "min_budget": 10}])
    h.plans["CE8"] = plan
    acb = admit(h, "CE8", plan, "T1")
    acb.subscriptions.add("r")
    acb.materialized = {}
    for i in range(3):
        h.event_log.append(EventKind.RESOURCE_COMMITTED, "r",
                           {"content": {"i": i}, "version": i})
    ref = h.checkpoints.save(acb)

    h2 = Harness(event_log=h.event_log, checkpoints=h.checkpoints)
    rec = h2.recover(ref)
    assert rec.subscriptions == {"r"}
    assert rec.materialized == {}
    view = h2.context.context_view(rec)
    assert view.get("r") == {"i": 2}, f"恢复后 context_view 为空：{view!r}"
