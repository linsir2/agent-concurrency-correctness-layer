#!/usr/bin/env python3
"""对抗评审清单 8 个反例的复现脚本（修复前应全部 FAIL，修复后全部 PASS）。

用法：
    python3 scripts/repro_review_counterexamples.py            # 全部
    python3 scripts/repro_review_counterexamples.py 1 2 3      # 指定条目

每条反例独立运行（互不污染），输出 `[PASS]/[FAIL] 条目N: 描述`，
并以非零退出码结束（若仍有 FAIL）。
"""

from __future__ import annotations

import os
import sys
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))

import conftest as _c
from conftest import _acb, admit, decision, make_harness, seed

from harness import (
    ACB,
    EventKind,
    Harness,
    Status,
    TaskState,
    Write,
)
from harness.checkpoint import FileCheckpointStore
from harness.runtime import CrashInjected


def _count(h, kind):
    return len([e for e in h.event_log.all() if e.kind == kind])


# ------------------------------------------------------------------ CE1
def ce1_dedup_drops_write():
    """P0-1：dedup 命中后不补投影 → 后续基于过期版本的新写被误短路丢写。"""
    h = make_harness()
    seed(h, "r", {"v": 0})
    acb = _acb(h, "r")
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": 1}

    orig = h.workspace._apply_locked
    calls = {"n": 0}

    def boom(*a, **k):
        if calls["n"] == 0:
            calls["n"] += 1
            raise CrashInjected(0, 0)
        return orig(*a, **k)

    h.workspace._apply_locked = boom
    try:
        h.commit_batch(acb, [Write("r")])
    except CrashInjected:
        pass
    finally:
        h.workspace._apply_locked = orig

    assert h.workspace.version("r") == 1, "前提：事件已提交但投影未发生"

    st = h.commit_batch(acb, [Write("r")])
    assert st == Status.OK
    assert st.detail.get("dedup") is True, "重试应命中 dedup"
    assert h.workspace.get("r") == {"v": 1}, \
        "dedup 必须补投影，否则 manifest 仍是过期 v0"

    # 基于已对齐的版本再写新内容：不得被误短路
    h.workspace.stage(acb, "r", {"v": 2})
    st2 = h.commit_batch(acb, [Write("r")])
    assert st2 == Status.OK and st2.detail.get("dedup") is not True, \
        "新内容不得被 dedup 短路"
    assert h.workspace.get("r") == {"v": 2}, "新内容不得静默丢失"


# ------------------------------------------------------------------ CE2
def ce2_commit_key_has_mutable_base_version():
    """P0-2：幂等键含 base_version → 刷新后重试去重失效（重复提交 + 重复收据）。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "r", {"v": 0})
    acb = _acb(h, "r")
    acb.base_version["r"] = 1
    acb.staged["r"] = {"v": 1}
    assert h.commit_batch(acb, [Write("r")]) == Status.OK

    acb.base_version["r"] = h.workspace.version("r")   # 运行期刷新为写入后版本
    n_commit = _count(h, EventKind.RESOURCE_COMMITTED)
    n_receipt = _count(h, EventKind.RECEIPT_ISSUED)

    st = h.commit_batch(acb, [Write("r")])             # 同内容重试
    assert st == Status.OK and st.detail.get("dedup") is True, "同内容重试应命中 dedup"
    assert _count(h, EventKind.RESOURCE_COMMITTED) == n_commit, "不得重复提交"
    assert _count(h, EventKind.RECEIPT_ISSUED) == n_receipt, "不得重复收据"


# ------------------------------------------------------------------ CE3
def ce3_partial_dedup_duplicates_receipt():
    """P0-3：部分命中时整批 append → 已签发的写被重复 append 收据。"""
    h = make_harness(auth_secret="s3cr3t")
    seed(h, "a", {"v": 0})
    seed(h, "b", {"v": 0})
    acb = _acb(h, "a")
    acb.base_version["a"] = 1
    acb.base_version["b"] = 1
    acb.staged["a"] = {"v": 1}
    acb.staged["b"] = {"v": 1}

    # 提交 a 后、投影前崩溃 → 事件已 append、manifest 仍 v1
    orig = h.workspace._apply_locked
    calls = {"n": 0}

    def boom(*a, **k):
        if calls["n"] == 0:
            calls["n"] += 1
            raise CrashInjected(0, 0)
        return orig(*a, **k)

    h.workspace._apply_locked = boom
    try:
        h.commit_batch(acb, [Write("a")])
    except CrashInjected:
        pass
    finally:
        h.workspace._apply_locked = orig
    assert h.workspace.version("a") == 1, "前提：a 已提交但未投影"

    st = h.commit_batch(acb, [Write("a"), Write("b")])   # a 命中、b 新增
    assert st == Status.OK, "部分命中仍应成功提交新增写"
    assert h.workspace.get("a") == {"v": 1}
    assert h.workspace.get("b") == {"v": 1}

    scope = (acb.run_id, acb.logical_subgoal_id)
    receipts = h.receipts_of(*scope)
    ids = [r.receipt_id for r in receipts]
    assert len(ids) == len(set(ids)), f"收据 receipt_id 重复：{ids}"
    assert h.verify_receipts(acb.run_id, acb.logical_subgoal_id) is True, \
        "收据链必须可离线验证"


# ------------------------------------------------------------------ CE4
def ce4_nested_bytes_corrupted():
    """P1-4：嵌套 bytes 走严格 codec 编码，但 _decode 只裸 json.loads → 读回变 dict。"""
    h = make_harness()
    acb = _acb(h, "b")
    h.workspace.stage(acb, "b", {"x": b"abc"})
    assert h.commit_batch(acb, [Write("b")]) == Status.OK
    out = h.workspace.get("b")
    assert out == {"x": b"abc"}, f"嵌套 bytes 读回被损坏：{out!r}"
    assert isinstance(out["x"], bytes)

    # rebase（merge3）也须能处理嵌套 bytes
    base = {"x": b"abc"}
    local = {"x": b"abd"}
    committed = {"x": b"abc"}
    merged = h.workspace.rebase_json(base, local, committed)["merged"]
    assert merged == {"x": b"abd"}, f"嵌套 bytes rebase 失败：{merged!r}"


# ------------------------------------------------------------------ CE5
def ce5_stage_bad_content_leaks_slot():
    """P1-5：stage 非法内容未 fail-fast → commit 抛 → _finalize save 抛 → 槽位泄漏。"""
    h = make_harness()
    seed(h, "r", {"v": 0})
    plan = h.planner.plan("CE5", [{"task_id": "T1", "min_budget": 10}])
    h.plans["CE5"] = plan
    acb = admit(h, "CE5", plan, "T1")

    res = h.workspace.stage(acb, "r", {1, 2})
    assert res == Status.INVALID_INPUT, "stage 必须对非法内容 fail-fast"
    assert acb.staged == {}, "非法内容不得进入 staged"
    # 清理手工准入的槽位 / 预留 / 节点，使后续断言只看 acb2
    h.slots.release(acb.id)
    for rid in list(acb.reservation_ids):
        h.budget.settle(rid)
    if acb.budget_node is not None:
        h.budget.archive_node(acb.budget_node.node_id)
        acb.budget_node = None

    acb2 = h.dispatch_and_run(plan, "T1", [decision(1, _c.stage("r", {1, 2}))])
    assert acb2.state is TaskState.FAILED, "非法内容提交必须失败"
    assert h.slots.active == 0, "槽位必须被释放（不得泄漏）"
    assert h.budget.root.remaining == h.budget.root.limits, "预算必须结算归还"
    h.budget.check_invariants()


# ------------------------------------------------------------------ CE6
def ce6_quarantine_dangling_latest_ghost_acb(tmp_root):
    """P1-6：quarantine 改名后 latest 悬空 → recover 造幽灵 ACB(acb_id='')。"""
    root = tmp_root / "cp"
    acb = ACB(id="A-CE6", run_id="R-CE6", task_id="T-CE6")
    store = FileCheckpointStore(root)
    ref = store.save(acb)

    ref_path = store._ref_path(ref)
    ref_path.write_text("not-json{{{", encoding="utf-8")   # 破坏 ref 文件

    store2 = FileCheckpointStore(root)                       # 触发 quarantine
    assert store2.latest_ref_for_acb(acb.id) is None, \
        "quarantine 后 latest 不得悬空指向不存在的 ref"

    h = Harness(checkpoints=FileCheckpointStore(root))
    rec = h.recover(ref)
    assert rec.id == acb.id, f"不得构造幽灵 ACB，实际 id={rec.id!r}"
    assert "" not in h.acbs, "不得把幽灵 ACB 注册到 acbs['']"
    assert rec.state is TaskState.QUARANTINED


# ------------------------------------------------------------------ CE7
def ce7_retention_jump_loses_state():
    """P1-7：materialized 非空 ≠ 覆盖到 floor → 跳 floor 丢 [cursor, floor) 的订阅事件。"""
    from harness import ContextAssembler, EventLog

    log = EventLog()
    for i in range(3):                       # seq1..3：订阅资源 r 的事件
        log.append(EventKind.RESOURCE_COMMITTED, "r", {"content": {"i": i}, "version": i})
    for i in range(3):                       # seq4..6：别的 subject
        log.append(EventKind.RESOURCE_COMMITTED, "other", {"content": i, "version": i})

    acb = ACB(id="A-CE7", run_id="R-CE7", task_id="T-CE7")
    acb.subscriptions.add("r")
    acb.cursor = 0
    acb.materialized = {"r": "SNAP"}
    acb.materialized_watermark = 0           # 快照不含 [cursor, floor)

    asm = ContextAssembler(log, h_workspace_stub(), retention=3)
    view = asm.context_view(acb)
    assert view["r"] == {"i": 2}, \
        f"跳 floor 丢了订阅事件，真实最新应为 {{'i': 2}}，实际 {view['r']!r}"


def h_workspace_stub():
    class _WS:
        def get(self, resource):
            return None
    return _WS()


# ------------------------------------------------------------------ CE8
def ce8_recover_drops_subscriptions_materialized():
    """P1-8：recover 不恢复 subscriptions/materialized → 恢复后 context_view 为空。"""
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
    assert rec.subscriptions == {"r"}, f"subscriptions 未恢复：{rec.subscriptions!r}"
    assert rec.materialized == {}, f"materialized 未恢复：{rec.materialized!r}"

    view = h2.context.context_view(rec)
    assert view.get("r") == {"i": 2}, \
        f"恢复后 context_view 为空（事件被订阅过滤掉）：{view!r}"


# ------------------------------------------------------------------ runner
CASES = [
    ("1", ce1_dedup_drops_write),
    ("2", ce2_commit_key_has_mutable_base_version),
    ("3", ce3_partial_dedup_duplicates_receipt),
    ("4", ce4_nested_bytes_corrupted),
    ("5", ce5_stage_bad_content_leaks_slot),
    ("6", ce6_quarantine_dangling_latest_ghost_acb),
    ("7", ce7_retention_jump_loses_state),
    ("8", ce8_recover_drops_subscriptions_materialized),
]


def main(argv):
    import tempfile
    from pathlib import Path

    wanted = set(argv[1:]) if len(argv) > 1 else None
    failed = []
    for num, fn in CASES:
        if wanted and num not in wanted:
            continue
        try:
            if num == "6":
                with tempfile.TemporaryDirectory() as d:
                    fn(Path(d))
            else:
                fn()
        except BaseException:  # noqa: BLE001  # 逐条复现：失败继续跑下一条
            failed.append(num)
            print(f"[FAIL] 条目{num}: {fn.__doc__.strip()}")
            traceback.print_exc()
        else:
            print(f"[PASS] 条目{num}: {fn.__doc__.strip()}")
    print("-" * 72)
    print(f"总计：{len(CASES) if not wanted else len(wanted)} 条，失败 {len(failed)} 条")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
