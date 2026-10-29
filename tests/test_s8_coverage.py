"""S8 覆盖率收口（文档 `s7-benchmark-and-coverage.md` v1.1 §4.2/§4.4/§4.5）。

对应关系：
- A §4.2 三个高价值缺口：`recover(COMPENSATE)` 两段式 / resolver 成功路径 / `_restore_path`
- B §4.4 死代码：`auth.gc_seen` / `budget.gc` + `archive_node` 守卫
- C §4.5 S8-1..S8-4：子进程 SIGKILL / hypothesis 属性 / 端到端 3 agent / 故障窗口枚举
- D §4.1：budget.py 剩余缺口（check() / create_node / remove_node 守卫）

已知缺陷 K-28（C1 静默丢写）**已修复**：`runtime.handle_commit_result` 在
`REJECT_STALE_READ` 时对陈旧 staged 重跑 merge3，回归测试见
`tests/test_k28_bench_regressions.py`；本套件保留 resolver 反证用例（§4.2 ③）。
"""

from __future__ import annotations

import copy
import os
import random
import subprocess
import sys
import textwrap
import time

import pytest
from conftest import _acb, admit, commit, decision, make_harness, seed, stage, tool

from harness import (
    ACB,
    EventKind,
    Harness,
    ManualClock,
    Mode,
    Status,
    TaskState,
    Write,
    merge3,
)
from harness import runtime as runtime_module
from harness.event_log import WalEventLog
from harness.runtime import CrashInjected, _validate_resolution
from harness.storage.blob import canonical_bytes, sha256_hex


# ================================================================ A. §4.2 高价值缺口
# ---------------------------------------------------------------- A1 recover(COMPENSATE)
def test_s8_recover_compensate_two_phase(monkeypatch):
    """§4.2：`recover(resume_policy="COMPENSATE")` 两段式。

    **测量声明：进程内模拟，非持久恢复** —— journal 与 checkpoint 均为内存态，
    崩溃点用 monkeypatch 在进程内注入；真实持久恢复（WAL 落盘 + 新进程重放）
    不在本用例覆盖范围。

    ① 共享 event_log + checkpoints + blobs；monkeypatch `comp_journal.mark`
       在 `state == "COMPENSATED"` 处抛 `CrashInjected` → 第一次 recover 抛，
       断言 journal 状态（条目停在 COMPENSATING）；
    ② 第二次 recover → DONE_PARTIAL + 幂等（无重复反写：内容 hash 不变、版本不推进）
       + 预留已结算 + 节点已归档 + `check_invariants()` 不抛。
    """
    h = make_harness(run_limits=100)
    acb = _acb(h, "r")
    acb.state = TaskState.RUNNING
    h.workspace.stage(acb, "r", {"v": 1})
    assert h.commit_batch(acb, [Write("r")]) == Status.OK
    ph = h.workspace.content_hash("r")
    applied = [Write("r", content={"v": 1}, version_before=0, applied_hash=ph)]
    entries = h.comp_journal.plan(acb, applied)
    assert len(entries) == 1
    cid = entries[0].comp_id
    ref = h.checkpoints.save(acb)

    orig_mark = h.comp_journal.mark

    def crashy_mark(comp_id, state, **kw):
        if state == "COMPENSATED":
            raise CrashInjected(0, 0)
        return orig_mark(comp_id, state, **kw)

    monkeypatch.setattr(h.comp_journal, "mark", crashy_mark)

    # ① 第一次 recover：反写已落盘，但 COMPENSATED 标记前崩溃 → 抛 CrashInjected
    with pytest.raises(CrashInjected):
        h.recover(ref, resume_policy="COMPENSATE")
    assert h.comp_journal._entries[cid].state == "COMPENSATING"
    assert h.workspace.get("r") is None            # 反写已发生
    assert h.workspace.version("r") == 2

    # 解除注入，第二次 recover 续做（条目已 done → 不再反写）
    monkeypatch.undo()
    hash_before = h.workspace.content_hash("r")
    ver_before = h.workspace.version("r")

    rec = h.recover(ref, resume_policy="COMPENSATE")
    assert rec.state is TaskState.DONE_PARTIAL
    assert rec.failure is None
    # 幂等：无重复反写（内容 hash / 版本不变）
    assert h.workspace.content_hash("r") == hash_before
    assert h.workspace.version("r") == ver_before
    # 预留已结算（归档置 ARCHIVED）+ 节点已归档
    assert rec.reservation_ids != []
    assert all(h.budget._reservations[rid][1].state == "ARCHIVED"
               for rid in rec.reservation_ids)
    assert rec.budget_node is None
    h.budget.check_invariants()


# ---------------------------------------------------------------- A2 resolver 成功路径
def test_s8_validate_resolution_semantics():
    """§4.2①：`_validate_resolution` 直接单测（非法类型 False / 合法 True / 干净合并新增键 True）。"""
    base = {"a": 0}
    local = {"a": 1}
    committed = {"a": 2}
    mr = merge3(base, local, committed)
    assert not mr.ok and mr.conflicts

    # 非法类型（list）→ False
    assert _validate_resolution([1, 2], mr, base, committed) is False
    # 正对照：合法 dict → True
    assert _validate_resolution({"a": 1}, mr, base, committed) is True

    # 「干净合并新增键」回归（§4.3）：local 新增 b、committed 缺失 → 合法输出
    base2 = {"a": 0}
    local2 = {"a": 0, "b": 1}
    committed2 = {"a": 0}
    mr2 = merge3(base2, local2, committed2)
    assert mr2.ok and mr2.merged == {"a": 0, "b": 1}
    assert _validate_resolution({"a": 0, "b": 1}, mr2, base2, committed2, local2) is True


def _conflict_scenario(h, resolver):
    """构造真实冲突：两个 agent 基于同一 v0 stage，竞争者先提交（记录前像），本 agent 冲突。"""
    v0 = seed(h, "r", {"a": 0})
    plan = h.planner.plan("R", [{"task_id": "T1", "contingent": ["r"]},
                                 {"task_id": "T2", "contingent": ["r"]}])
    h.plans["R"] = plan
    acb = admit(h, "R", plan, "T1")
    other = admit(h, "R", plan, "T2")
    h.workspace.stage(acb, "r", {"a": 1})          # base = v0
    h.workspace.stage(other, "r", {"a": 2})        # base = v0
    # 竞争者先提交（_apply_locked 记录 v1 前像），本 agent 随后提交 → CONFLICT
    h.run_task(other, [decision(1, commit("r"))])
    assert h.workspace.version("r") == v0 + 1
    h.resolver = resolver
    return acb


def test_s8_resolver_invalid_type_never_persisted():
    """§4.2②：注入返回 list 的 resolver → 非法值从未落盘 + 首个 _rebase_write 捕获 resolver_failed。"""
    h = make_harness()
    calls = {"n": 0}

    def bad_resolver(_acb, _resource, _conflicts, _ctx):
        calls["n"] += 1
        return [1, 2]

    acb = _conflict_scenario(h, bad_resolver)

    captured: list[str | None] = []
    orig_rebase = h.runtime._rebase_write

    def spy_rebase(acb_, result):
        out = orig_rebase(acb_, result)
        captured.append(acb_.failure)
        return out

    h.runtime._rebase_write = spy_rebase

    staged_before = copy.deepcopy(acb.staged)
    ver_before = h.workspace.version("r")
    get_before = copy.deepcopy(h.workspace.get("r"))

    h.run_task(acb, [decision(1, commit("r"))])

    assert calls["n"] >= 1                         # resolver 被调用
    assert captured and captured[0] == "resolver_failed"
    # 非法值从未落盘
    assert acb.staged == staged_before
    assert h.workspace.version("r") == ver_before
    assert h.workspace.get("r") == get_before
    assert acb.state is TaskState.QUARANTINED       # 终态被 _quarantine 覆盖


def test_s8_resolver_validator_bypass_counterproof(monkeypatch):
    """§4.2③ 反证：校验器恒 True → 非法值被接受（staged 变 list）。"""
    h = make_harness()
    acb = _conflict_scenario(h, lambda _acb, _res, _conf, _ctx: [1, 2])
    monkeypatch.setattr(runtime_module, "_validate_resolution",
                        lambda *a, **k: True)

    captured: dict = {}
    orig_rebase = h.runtime._rebase_write

    def spy_rebase(acb_, result):
        out = orig_rebase(acb_, result)
        captured.setdefault("staged", copy.deepcopy(acb_.staged))
        return out

    h.runtime._rebase_write = spy_rebase
    h.run_task(acb, [decision(1, commit("r"))])

    assert captured["staged"]["r"] == [1, 2]         # 非法值被接受（反证成立）


# ---------------------------------------------------------------- A3 _restore_path
def _nested_budget_checkpoint(h, acb_id):
    """构造嵌套预算节点（run → B-parent → B-child）并保存 checkpoint。"""
    h.budget.create_node("B-parent", 100)
    child = h.budget.create_node("B-child", 50, parent_id="B-parent")
    acb = ACB(id=acb_id, run_id="R", task_id="T1")
    acb.budget_node = child
    child.owner = acb.id
    resv = h.budget.reserve("B-child", 20)
    acb.reservation_ids = [resv.reservation_id]
    return h.checkpoints.save(acb)


def test_s8_restore_path_ancestor_missing_real_path(monkeypatch):
    """§4.2：祖先缺失走真实路径 —— 新进程 + 嵌套 parent_budget → 逐级 create_node。"""
    h = make_harness(run_limits=1000)
    ref = _nested_budget_checkpoint(h, "A-RP")

    h2 = Harness(run_limits=1000, checkpoints=h.checkpoints)
    assert set(h2.budget._nodes) == {"run"}

    calls: list[tuple[str, str]] = []
    orig_create = h2.budget.create_node

    def spy_create(node_id, limits, parent_id="run"):
        calls.append((node_id, parent_id))
        return orig_create(node_id, limits, parent_id=parent_id)

    monkeypatch.setattr(h2.budget, "create_node", spy_create)

    rec = h2.recover(ref)
    assert calls == [("B-parent", "run"), ("B-child", "B-parent")]
    node = h2.budget._nodes["B-child"]
    assert node.parent.node_id == "B-parent"
    assert h2.budget._nodes["B-parent"].parent is h2.budget.root
    assert rec.budget_node is node
    h2.budget.check_invariants()


def test_s8_restore_path_rollback_on_intermediate_failure(monkeypatch):
    """§4.2：中间节点 create_node 返回 None → 逆序 remove_node + needs_readmit + 无残留。"""
    h = make_harness(run_limits=1000)
    ref = _nested_budget_checkpoint(h, "A-RR")

    h2 = Harness(run_limits=1000, checkpoints=h.checkpoints)
    removed: list[str] = []
    orig_create = h2.budget.create_node
    orig_remove = h2.budget.remove_node

    def fail_child(node_id, limits, parent_id="run"):
        if node_id == "B-child":
            return None
        return orig_create(node_id, limits, parent_id=parent_id)

    def spy_remove(node_id, **kw):
        removed.append(node_id)
        return orig_remove(node_id, **kw)

    monkeypatch.setattr(h2.budget, "create_node", fail_child)
    monkeypatch.setattr(h2.budget, "remove_node", spy_remove)

    rec = h2.recover(ref)
    assert removed == ["B-parent"]                  # 逆序回滚
    assert rec.needs_readmit is True
    assert rec.budget_node is None
    assert rec.reservation_ids == []
    assert "B-parent" not in h2.budget._nodes
    assert "B-child" not in h2.budget._nodes


def test_s8_restore_path_cycle_swallowed_by_restore_budget():
    """§4.2：环（合成 dict）→ `_restore_budget` 吞掉 ValueError，标记 needs_readmit。"""
    h = make_harness(run_limits=1000)
    with pytest.raises(ValueError):
        h.recovery.restore_path(["run", "B1", "B1"])

    acb = ACB(id="A-RC", run_id="R", task_id="T1")
    data = {
        "budget_node_id": "B1",
        "budget_path": ["run", "B1", "B1"],
        "budget_path_limits": {"run": 1000, "B1": 100},
        "reservations": [],
    }
    h.recovery.restore_budget(acb, data)
    assert acb.needs_readmit is True
    assert acb.budget_node is None
    assert acb.reservation_ids == []
    assert "B1" not in h.budget._nodes


# ================================================================ B. §4.4 死代码
# ---------------------------------------------------------------- B4 auth.gc_seen
def test_s8_auth_gc_seen():
    """§4.4：`make_harness(auth_gc_every=2)` + 过 exp 的 token → 追加 2 个事件触发 gc_seen。"""
    clock = ManualClock()
    h = make_harness(auth_secret="s3cret", auth_gc_every=2, clock=clock)
    assert h.state.auth is not None

    token = h.auth.mint("A1", exp=int(time.time()) + 1)
    assert h.auth.verify(token) is not None
    assert len(h.auth._seen) == 1

    # 推进注入时钟过 exp（gc 用注入时钟的 epoch 秒）
    clock.advance(int(time.time()) * 1000 + 5000)
    assert h.clock.now_ms() / 1000.0 >= int(time.time()) + 1

    h.event_log.append(EventKind.TASK_COMPLETED, "x1", {})
    assert len(h.auth._seen) == 1                     # 第 1 个事件不触发
    h.event_log.append(EventKind.TASK_COMPLETED, "x2", {})
    assert len(h.auth._seen) == 0                     # 第 2 个事件触发 gc_seen


# ---------------------------------------------------------------- B5 budget.gc
def test_s8_budget_gc_reclaims_expired_tombstone():
    """§4.4：`gc_every=3` + ARCHIVED tombstone → 到期后被回收。"""
    h = make_harness(gc_every=3, tombstone_ttl_s=0.0)
    h.budget.create_node("B", 100)
    resv = h.budget.reserve("B", 10)
    assert h.budget.consume(resv.reservation_id, 5) == Status.OK
    assert h.budget.settle(resv.reservation_id) == 5
    assert h.budget.archive_node("B") is True
    rid = resv.reservation_id
    assert h.budget._reservations[rid][1].state == "ARCHIVED"

    h.event_log.append(EventKind.TASK_COMPLETED, "x1", {})
    h.event_log.append(EventKind.TASK_COMPLETED, "x2", {})
    assert rid in h.budget._reservations              # 第 2 个事件不触发
    h.event_log.append(EventKind.TASK_COMPLETED, "x3", {})
    assert rid not in h.budget._reservations          # 第 3 个事件触发 gc


def test_s8_archive_node_guards():
    """§4.4：`archive_node` 的 node=None 与「有 ACTIVE 预留拒绝」两个守卫分支。"""
    h = make_harness(run_limits=100)
    assert h.budget.archive_node("nonexistent") is False

    h.budget.create_node("B", 100)
    resv = h.budget.reserve("B", 10)
    assert h.budget.archive_node("B") is False       # 有 ACTIVE 预留 → 拒绝
    assert "B" in h.budget._nodes
    assert resv.state == "ACTIVE"
    assert h.budget._nodes["B"].subtree_reserved == 10


# ================================================================ D. budget.py 剩余缺口
def test_s8_budget_check_and_node_guards():
    """§4.1：覆盖 budget.py 剩余分支（check / check_reservations / create_node / remove_node）。"""
    h = make_harness(run_limits=100)
    assert h.budget.check("nonexistent") is False
    node = h.budget.create_node("B", 100)
    assert h.budget.check("B") is False
    resv = h.budget.reserve("B", 10)
    assert h.budget.check("B") is True
    assert h.budget.check("B", 20) is False
    assert h.budget.check("B", 10) is True
    assert h.budget.check_reservations([resv.reservation_id], 1) is True
    assert h.budget.check_reservations(["nope"], 1) is False
    h.budget.settle(resv.reservation_id)
    assert h.budget.check_reservations([resv.reservation_id], 1) is False
    assert h.budget.check("B") is False

    # reserve / settle / consume 的守卫分支
    assert h.budget.reserve("nonexistent", 10) is None
    assert h.budget.settle("nonexistent") == 0
    assert h.budget.settle(resv.reservation_id) == 0          # 已结算
    assert h.budget.consume(resv.reservation_id, 1) == Status.INVALID_INPUT

    # create_node 守卫分支
    assert h.budget.create_node("NEG", 0) is None
    assert h.budget.create_node("NEG", -5) is None
    assert h.budget.create_node("B", 100) is node              # 同 limits 复用
    assert h.budget.create_node("B", 200) is None             # 不同 limits 拒绝
    assert h.budget.create_node("ORPHAN", 10, parent_id="missing") is None

    # remove_node 守卫分支
    assert h.budget.remove_node("nonexistent") is False
    assert h.budget.remove_node("B") is True                 # 已结算 → 可删
    assert "B" not in h.budget._nodes

    n2 = h.budget.create_node("B2", 100)
    n2.owner = "someone"
    assert h.budget.remove_node("B2", owner="other") is False  # owner 守卫
    assert h.budget.remove_node("B2", owner="someone") is True
    h.budget.check_invariants()


# ================================================================ C. §4.5 S8-1..S8-4
# ---------------------------------------------------------------- S8-1 子进程 SIGKILL
@pytest.mark.fault
def test_s8_1_wal_sigkill_truncation(tmp_path):
    """S8-1（层 A）：子进程写若干帧后 SIGKILL（含未 fsync 的半帧）→ 父进程重开断言 tail/truncated_at。"""
    path = str(tmp_path / "wal.log")
    n = 5
    script = textwrap.dedent(f"""
        import os, signal, struct, sys
        sys.path.insert(0, {os.getcwd()!r})
        from harness.event_log import WalEventLog
        from harness import EventKind
        log = WalEventLog({path!r}, fsync=True)
        for i in range({n}):
            log.append(EventKind.TOOL_COMPLETED, "s", {{"i": i}}, producer="child")
        fd = os.open({path!r}, os.O_WRONLY | os.O_APPEND)
        body = b'{{"partial":'
        header = struct.pack(">4sII", b"MWAL", 100, 0)
        os.write(fd, header + body)          # 未 fsync 的半帧
        os.kill(os.getpid(), signal.SIGKILL)
    """)
    proc = subprocess.Popen([sys.executable, "-c", script])
    proc.wait(timeout=20)
    assert proc.returncode != 0

    log = WalEventLog(path, fsync=False)
    assert log.tail() == n                        # 已 fsync 的帧全部保留
    assert log.truncated_at is not None           # 尾部半帧被识别为截断
    assert log.truncated_at > 0
    assert len(log.all()) == n
    log.close()


# ---------------------------------------------------------------- S8-2 属性测试
hypothesis = pytest.importorskip("hypothesis")
from hypothesis import settings as _hs
from hypothesis import strategies as _st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    rule,
)


class _S8_2InterleaveMachine(RuleBasedStateMachine):
    """S8-2：RuleBasedStateMachine 跑 commit_batch 交错 → I1 / I5 / I7 不变量。

    **P2-9 判定（断言变更是否削弱）**：改动**合理**，不是削弱。
    - 原断言 `version == v0 + ok_count` 在「同内容重提交按内容去重」语义下
      **不可能成立**：`commit_batch` 对同内容重提交返回 `OK` 且 `dedup=True`，
      `ok_count` 自增 1 而版本不变 → 等式必然被破坏（只要机器产生一次 dedup）。
      故原断言不是「更强」，而是**与语义矛盾**（会在合法 dedup 上误报失败）。
    - 新断言 `version == v0 + ok_count − dedup_count` 是**精确等式**：每个 OK
      提交要么推进版本（非 dedup），要么命中 dedup（幂等不推进）。
    - 为杜绝「随便返回 OK」（把新内容误判为 dedup 也能满足等式），额外用
      `_assert_genuine_dedup` 校验命中事件的 `(run_id, logical_subgoal_id,
      resource, mode, deleted)` 与当前 acb 一致 —— 而不是「hash ∈ 全局集合」。
      仅断言后者会**漏报跨 subgoal 去重**（a/b 共用资源 r、subgoal 不同：
      若实现漏掉 subgoal 分量，b 会误判 dedup，而 hash 确实在全局集合里 → 两条
      断言同时通过，测试漏报）。
    """

    def __init__(self):
        super().__init__()
        self.h = None
        self.v0 = 0
        self.a = None
        self.b = None
        self.ok_count = 0
        self.dedup_count = 0

    @initialize()
    def _init_state(self):
        self.h = make_harness(run_limits=100, probe=lambda key: True)
        self.v0 = seed(self.h, "r", {"n": 0})
        plan = self.h.planner.plan(
            "SM", [{"task_id": "A", "contingent": ["r"]},
                   {"task_id": "B", "contingent": ["r"]}])
        self.h.plans["SM"] = plan
        self.a = admit(self.h, "SM", plan, "A")
        self.b = admit(self.h, "SM", plan, "B")

    @rule(value=_st.integers(0, 9))
    def stage_a(self, value):
        self.h.workspace.stage(self.a, "r", {"n": value})

    @rule(value=_st.integers(0, 9))
    def stage_b(self, value):
        self.h.workspace.stage(self.b, "r", {"n": value})

    @rule()
    def commit_a(self):
        staged = copy.deepcopy(self.a.staged.get("r"))
        st = self.h.commit_batch(self.a, [Write("r")])
        if st == Status.OK:
            self.ok_count += 1
            if st.detail.get("dedup") is True:
                self.dedup_count += 1
                self._assert_genuine_dedup(self.a, staged)

    @rule()
    def commit_b(self):
        staged = copy.deepcopy(self.b.staged.get("r"))
        st = self.h.commit_batch(self.b, [Write("r")])
        if st == Status.OK:
            self.ok_count += 1
            if st.detail.get("dedup") is True:
                self.dedup_count += 1
                self._assert_genuine_dedup(self.b, staged)

    def _assert_genuine_dedup(self, acb, staged) -> None:
        """P2-9 强化：dedup 必须命中「与当前 acb 同 scope 的提交事件」。

        仅断言 `version == v0 + ok_count − dedup_count` 会让 dedup 成为
        「随便返回 OK」的合法出口（把新内容误判为 dedup 也能满足等式）。
        旧判据「`content_hash ∈ 全局已提交 hash`」**不足以**堵住：若实现漏掉
        subgoal 分量，跨 subgoal 的误判 dedup 也会通过（hash 确实在全局集合里）
        → 测试漏报（P2-D）。

        新判据要求命中事件的 `(run_id, logical_subgoal_id, resource, mode,
        deleted)` 与当前 acb 一致，且其 `version` 就是该资源的最新版本
        （`_effective_version`，即 dedup 只能命中最新提交，P1-B 的镜像断言）。
        """
        assert staged is not None, "dedup 命中时 staged 不得为空"
        resource = "r"
        blob_hash = sha256_hex(canonical_bytes(staged))
        matched = [
            ev for ev in self.h.event_log.all()
            if ev.kind == EventKind.RESOURCE_COMMITTED
            and ev.run_id == acb.run_id
            and (ev.payload or {}).get("logical_subgoal_id", "") == acb.logical_subgoal_id
            and (ev.payload or {}).get("resource") == resource
            and (ev.payload or {}).get("mode", Mode.COMMIT.value) == Mode.COMMIT.value
            and bool((ev.payload or {}).get("deleted", False)) is False
            and (ev.payload or {}).get("blob_hash", "") == blob_hash
        ]
        assert matched, (
            "dedup 必须命中与当前 acb 同 scope 的提交事件："
            f"run={acb.run_id!r} subgoal={acb.logical_subgoal_id!r} "
            f"resource={resource!r} mode={Mode.COMMIT.value}"
        )
        latest_version = self.h.workspace._effective_version(resource)
        assert all(
            int((ev.payload or {}).get("version", 0) or 0) == latest_version
            for ev in matched
        ), "dedup 命中事件必须是该资源的最新版本（不得命中陈旧提交）"

    @rule()
    def tool_idempotent(self):
        act = tool("deploy", {"x": 1})
        key = self.h.idem_key(self.a, act, 0)
        self.h.effects.begin(self.a, act)
        self.h.runtime._do_action(self.a, act, 0)
        self.h.runtime._do_action(self.a, act, 0)
        assert self.h.effects.state(key) == "CONFIRMED"
        n = len([e for e in self.h.event_log.all()
                 if e.kind == EventKind.TOOL_COMPLETED and e.idempotency_key == key])
        assert n <= 1

    @invariant()
    def i1_single_writer_version(self):
        # 每个 OK 提交要么推进版本、要么命中 dedup（幂等不推进）→ 严格等式
        # （`ok_count >= dedup_count` 由等式蕴含，冗余，已删；见 P3-G）
        assert self.h.workspace.version("r") == self.v0 + self.ok_count - self.dedup_count

    @invariant()
    def i5_budget_conserved(self):
        self.h.budget.check_invariants()
        assert self.h.budget.root.remaining >= 0


TestS8_2InterleaveMachine = _S8_2InterleaveMachine.TestCase
TestS8_2InterleaveMachine.settings = _hs(max_examples=15, stateful_step_count=12,
                                        deadline=None)


# ---------------------------------------------------------------- S8-3 端到端 3 agent
def test_s8_3_end_to_end_three_agents():
    """S8-3：经 `dispatch_and_run` 的端到端 3 agent（各自提交独立资源）。"""
    h = make_harness(run_limits=100)
    for r in ("r1", "r2", "r3"):
        seed(h, r, {"v": 0})
    plan = h.planner.plan("E3", [
        {"task_id": "T1", "contingent": ["r1"]},
        {"task_id": "T2", "contingent": ["r2"]},
        {"task_id": "T3", "contingent": ["r3"]},
    ])
    a1 = h.dispatch_and_run(plan, "T1", [decision(1, stage("r1", {"v": 1}), commit("r1"))])
    a2 = h.dispatch_and_run(plan, "T2", [decision(1, stage("r2", {"v": 2}), commit("r2"))])
    a3 = h.dispatch_and_run(plan, "T3", [decision(1, stage("r3", {"v": 3}), commit("r3"))])

    assert [a.state for a in (a1, a2, a3)] == [TaskState.DONE] * 3
    assert h.workspace.get("r1") == {"v": 1}
    assert h.workspace.get("r2") == {"v": 2}
    assert h.workspace.get("r3") == {"v": 3}
    assert h.workspace.version("r1") == 2
    assert h.workspace.version("r2") == 2
    assert h.workspace.version("r3") == 2
    assert h.slots.active == 0
    assert h.budget.root.remaining == h.budget.root.limits
    h.budget.check_invariants()


# ---------------------------------------------------------------- S8-4 故障窗口枚举
def test_s8_4_fault_window_enumeration():
    """S8-4：固定种子枚举补偿中途崩溃窗口（k=0/1/2）→ 无中间态 + 恢复后不变量成立。"""
    rng = random.Random(20261027)                 # 固定种子（确定性）
    windows = [0, 1, 2]
    assert rng.random() >= 0

    for k in windows:
        h = make_harness(compensate_crash_at=k)
        acb = _acb(h, "r1")
        acb.state = TaskState.RUNNING
        for res, val in (("r1", {"v": 1}), ("r2", {"v": 2})):
            h.workspace.stage(acb, res, val)
            assert h.commit_batch(acb, [Write(res, content=val)]) == Status.OK
        applied = [Write(res, content=val, version_before=0,
                         applied_hash=h.workspace.content_hash(res))
                   for res, val in (("r1", {"v": 1}), ("r2", {"v": 2}))]
        ref = h.checkpoints.save(acb)

        crashed = False
        try:
            h.compensate(acb, applied)
        except CrashInjected:
            crashed = True
        assert crashed == (k < 2)                        # k=0/1 崩溃，k=2 正常完成

        # 解除崩溃注入（make_harness 把 _run_one_compensation 包成 crashy 实例属性）
        h.commit.run_one_compensation = Harness._run_one_compensation.__get__(h, Harness)

        # 崩溃后：每个资源要么完全补偿（删除），要么保持原样 —— 无中间态
        for res, val in (("r1", {"v": 1}), ("r2", {"v": 2})):
            assert h.workspace.get(res) in (None, val)

        rec = h.recover(ref, resume_policy="COMPENSATE")
        assert rec.state in (TaskState.DONE_PARTIAL, TaskState.DEAD_LETTER,
                             TaskState.QUARANTINED)
        # 恢复后：全部补偿完成 + 幂等（内容 hash 不变）
        assert h.workspace.get("r1") is None
        assert h.workspace.get("r2") is None
        h.budget.check_invariants()
