"""MS1 出口条件：冲突处理回路（S3 / T6、T20、T21、T22）。

对应 docs/s3-rebase-and-compensation.md §5 / §9：
- T6：create 型写可补偿（Delete 哨兵不是 None）
- T20：CONFLICT → 语义 rebase（3-way merge）→ 合并提交 → REBASING→RUNNING
- T21：有界重试 → QUARANTINED（非终态）→ escalate → DEAD_LETTER（预算释放）
- T22：Delete 哨兵补偿交叉（同内容不同写入者 → REJECT_COMPENSATE）
"""

from __future__ import annotations

import pytest
from conftest import _acb, make_harness, seed

from harness import (
    DELETE,
    TASK_TERMINAL,
    EventKind,
    Mode,
    Result,
    Status,
    TaskState,
    Write,
)


# ---------------------------------------------------------------- T6
@pytest.mark.ms1
def test_ms1_create_is_compensable():
    h = make_harness()
    acb = _acb(h, "new.ts")
    acb.base_version["new.ts"] = 0
    acb.staged["new.ts"] = {"code": 1}
    assert h.commit_batch(acb, [Write("new.ts", content={"code": 1})]) == Status.OK
    v_after = h.workspace.version("new.ts")
    n_blobs = len(h.blobs._blobs)

    applied_hash = h.workspace.content_hash("new.ts")
    w = Write("new.ts", content={"code": 1})
    w.version_before = 0
    w.applied_hash = applied_hash
    assert h.workspace.compute_reverse_patch(w) is DELETE      # ① 哨兵不是 None
    h.compensate(acb, [w])
    assert "new.ts" not in h.workspace.manifest                # ② 资源被移除
    assert h.workspace.get("new.ts") is None
    assert h.workspace.version("new.ts") == v_after + 1        # ③ tombstone 保留版本

    # ④ DELETE 哨兵未被当作普通内容落盘：
    #    删除事件的 payload 必须 deleted==True 且 blob_hash==""，且 blob 数量未增加。
    deletes = [e for e in h.event_log.all()
               if e.kind == EventKind.RESOURCE_COMMITTED
               and (e.payload or {}).get("deleted") is True]
    assert len(deletes) == 1
    assert deletes[0].subject == "new.ts"
    assert deletes[0].payload["blob_hash"] == ""
    assert len(h.blobs._blobs) == n_blobs

    # ⑤ 重复补偿同一 create 型写 → 幂等 COMPENSATED、不二次删、不 pivot
    ver_after_comp = h.workspace.version("new.ts")
    h.compensate(acb, [Write("new.ts", content={"code": 1},
                             version_before=0, applied_hash=applied_hash)])
    assert h.workspace.get("new.ts") is None                   # 未二次删
    assert h.workspace.version("new.ts") == ver_after_comp      # 版本未再推进
    assert len([e for e in h.event_log.all()
                if e.kind == EventKind.RESOURCE_COMMITTED
                and (e.payload or {}).get("deleted") is True]) == 1
    assert not any(e.kind == EventKind.PIVOT for e in h.event_log.all())  # 不 pivot
    assert h.compensation_journal[-1] == {"resource": "new.ts", "state": "COMPENSATED"}


# ---------------------------------------------------------------- T20
@pytest.mark.ms1
def test_ms1_rebase_loop_merges_and_commits(monkeypatch):
    import harness.runtime as runtime_mod

    h = make_harness()
    v0 = seed(h, "f.json", {"a": 1})
    a = _acb(h, "f.json"); a.state = TaskState.RUNNING
    a.base_version["f.json"] = v0; a.read_set["f.json"] = v0
    a.staged["f.json"] = {"a": 1, "b": 2}
    assert h.commit_batch(a, [Write("f.json")]) == Status.OK

    b = _acb(h, "f.json"); b.state = TaskState.RUNNING
    b.base_version["f.json"] = v0; b.read_set["f.json"] = v0
    b.staged["f.json"] = {"a": 1, "c": 3}
    r = h.commit_batch(b, [Write("f.json")])
    assert r == Status.CONFLICT                                  # ① 先冲突

    # ② 包装 advance 记录真实状态转移（不能只看终态）
    transitions: list[tuple[str, str]] = []
    _orig_advance = runtime_mod.advance

    def _spy(acb, event):
        res = _orig_advance(acb, event)
        if res == Status.OK:
            transitions.append((res.detail.get("from"), res.detail.get("to")))
        return res

    monkeypatch.setattr(runtime_mod, "advance", _spy)

    r2 = h.runtime.handle_commit_result(b, r)
    assert r2 == Status.OK                                       # ③ 自动 rebase 后成功
    assert h.workspace.get("f.json") == {"a": 1, "b": 2, "c": 3}
    assert h.workspace.version("f.json") == v0 + 2
    assert b.state is TaskState.RUNNING
    assert b.rebase_attempts == 1
    assert ("RUNNING", "REBASING") in transitions                 # ④ 进入 rebase 的合法边
    assert ("REBASING", "RUNNING") in transitions                 # ⑤ 真实回边（非仅终态）


# ---------------------------------------------------------------- T21
@pytest.mark.ms1
def test_ms1_rebase_retry_bounded_then_quarantined():
    h = make_harness()
    v0 = seed(h, "s.json", {"strict": False})
    a = _acb(h, "s.json"); a.state = TaskState.RUNNING
    a.base_version["s.json"] = v0; a.read_set["s.json"] = v0
    a.staged["s.json"] = {"strict": True}
    assert h.commit_batch(a, [Write("s.json")]) == Status.OK

    b = _acb(h, "s.json"); b.state = TaskState.RUNNING; b.retry_budget = 2
    b.base_version["s.json"] = v0; b.read_set["s.json"] = v0
    b.staged["s.json"] = {"strict": "yes"}          # 类型不同 → 永远冲突，resolver 返回 None

    node = b.budget_node
    consumed_before = node.consumed
    r = h.commit_batch(b, [Write("s.json")])
    for _ in range(20):                              # 有界重试（不得无界 while）
        if b.state is TaskState.QUARANTINED:
            break
        h.runtime.handle_commit_result(b, r)
        r = Result(Status.CONFLICT, {"resource": "s.json", "version": v0 + 1,
                                     "expected": v0, "diff": {}})
    assert b.state is TaskState.QUARANTINED                      # ① 有界内必须隔离
    assert node.consumed > consumed_before                       # ② 重试期间预算被消耗
    assert b.rebase_attempts == b.retry_budget + 1               # ③ 有界（存在 acb 上）
    assert TaskState.QUARANTINED not in TASK_TERMINAL            # ④ 非终态
    assert h.escalate_quarantined(b) == Status.OK
    assert b.state is TaskState.DEAD_LETTER                       # ⑤ escalate 用合法边
    # ⑥ 累计账语义：settle 只还未消耗部分，已消耗永久留在 self_reserved
    assert node.self_reserved == node.consumed
    h.budget.check_invariants()


# ---------------------------------------------------------------- T22
@pytest.mark.ms1
def test_ms1_delete_sentinel_cross_rejected():
    # ① 同内容不同写入者：不得删除
    h = make_harness()
    acb = _acb(h, "n.ts"); acb.base_version["n.ts"] = 0; acb.staged["n.ts"] = {"code": 1}
    assert h.commit_batch(acb, [Write("n.ts", content={"code": 1})]) == Status.OK
    w = Write("n.ts", content={"code": 1}); w.version_before = 0
    w.applied_hash = h.workspace.content_hash("n.ts")

    h.workspace.put("n.ts", {"code": 1})            # B 重建同内容（版本已推进）
    st = h.commit_batch(acb, [w], Mode.COMPENSATE)
    assert st == Status.REJECT_COMPENSATE           # 版本不符 → 拒
    assert h.workspace.get("n.ts") == {"code": 1}   # 未删他人新值

    # ② 不同内容重建：同样拒绝
    h.workspace.put("n.ts", {"code": 999})
    assert h.commit_batch(acb, [w], Mode.COMPENSATE) == Status.REJECT_COMPENSATE
    assert h.workspace.get("n.ts") == {"code": 999}

    # ③ 重复补偿同一 create 型写 → 幂等 COMPENSATED、不二次删、不 pivot
    hi = make_harness()
    acb2 = _acb(hi, "p.ts"); acb2.base_version["p.ts"] = 0; acb2.staged["p.ts"] = {"code": 1}
    assert hi.commit_batch(acb2, [Write("p.ts", content={"code": 1})]) == Status.OK
    ph = hi.workspace.content_hash("p.ts")
    cw = Write("p.ts", content={"code": 1}); cw.version_before = 0; cw.applied_hash = ph
    hi.compensate(acb2, [cw])
    assert hi.workspace.get("p.ts") is None
    ver_after_first = hi.workspace.version("p.ts")

    hi.compensate(acb2, [Write("p.ts", content={"code": 1},
                               version_before=0, applied_hash=ph)])
    assert hi.workspace.get("p.ts") is None                     # 未二次删
    assert hi.workspace.version("p.ts") == ver_after_first       # 版本未再推进
    assert not any(e.kind == EventKind.PIVOT for e in hi.event_log.all())   # 不 pivot
    assert hi.compensation_journal[-1] == {"resource": "p.ts", "state": "COMPENSATED"}

    # ④ version_before≠0 且无前像 → 不可补偿（compute_reverse_patch 返回 None）
    ghost = Write("ghost.ts", content={"x": 1})
    ghost.version_before = 5
    assert h.workspace.compute_reverse_patch(ghost) is None
