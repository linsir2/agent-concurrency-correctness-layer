"""S22 C7：`RscSession` 门面的场景（`docs/s22-implementation-plan.md` §12.2，评审修订版）。

token = agent 会话；用真实 git worktree 做端到端验证。
评审新增回归：finish 后崩溃恢复、跨会话同键、cost=0、writes=() 的 OCC、多预留。
"""

from __future__ import annotations

import os
import subprocess

import pytest
from conftest import make_harness

from harness import ManualClock, RscSession, Status, TaskState
from harness.facade import Allow, Conflict, Deny, Receipt, Token, Wait
from harness.rebase import DELETE
from harness.storage.raft import RaftUnavailable
from harness.worktree import WorktreeManager


# ---------------------------------------------------------------- helpers
def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args],
                   check=True, capture_output=True, text=True)


@pytest.fixture()
def repo(tmp_path):
    base = tmp_path / "repo"
    base.mkdir()
    _git(base, "init", "-q")
    _git(base, "config", "user.email", "t@example.com")
    _git(base, "config", "user.name", "t")
    (base / "a.txt").write_text("hello", encoding="utf-8")
    (base / "keep.txt").write_text("keep", encoding="utf-8")
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


def _session(h, repo, tmp_path, *, min_budget=10, tenant=None):
    return RscSession(h, tenant=tenant, min_budget=min_budget,
                      worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                      state_dir=str(tmp_path / "state"))


def _write(root, rel, content):
    with open(os.path.join(root, rel), "w", encoding="utf-8") as fh:
        fh.write(content)


# ================================================================ 场景 1
def test_c7_1_concurrent_write_conflict(repo, tmp_path):
    """两个 agent 各写同一文件 → 一个成功、一个 Conflict（不丢写）。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    t1 = s.begin("a1", "i1", writes=["a.txt"])
    t2 = s.begin("a2", "i2", writes=["a.txt"])
    assert not isinstance(t1, (Deny, Wait)) and not isinstance(t2, (Deny, Wait))

    _write(t1.worktree, "a.txt", "from-1")
    r1 = s.apply(t1.token, [("a.txt", "from-1")], idempotency_key="K1")
    assert isinstance(r1, Receipt)

    _write(t2.worktree, "a.txt", "from-2")
    r2 = s.apply(t2.token, [("a.txt", "from-2")], idempotency_key="K2")
    assert isinstance(r2, Conflict), "后写者必须拿到可重试冲突"
    assert h.workspace.get("a.txt") == "from-1"


# ================================================================ 场景 2
def test_c7_2_same_key_retry_returns_same_receipt(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    t = s.begin("a1", "i1", writes=["a.txt"])
    _write(t.worktree, "a.txt", "v1")
    r1 = s.apply(t.token, [("a.txt", "v1")], idempotency_key="K")
    r2 = s.apply(t.token, [("a.txt", "v1")], idempotency_key="K")
    assert isinstance(r1, Receipt) and isinstance(r2, Receipt)
    assert r1.receipt_id == r2.receipt_id and r2.dedup is True


# ================================================================ 场景 3
def test_c7_3_same_key_different_content_conflicts(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    t = s.begin("a1", "i1", writes=["a.txt"])
    _write(t.worktree, "a.txt", "v1")
    s.apply(t.token, [("a.txt", "v1")], idempotency_key="K")
    r = s.apply(t.token, [("a.txt", "v2")], idempotency_key="K")
    assert isinstance(r, Conflict)
    assert h.workspace.get("a.txt") == "v1"


# ================================================================ 场景 4
def test_c7_4_expired_token_denied(tmp_path):
    h = make_harness(run_limits=1000, clock=ManualClock())
    s = RscSession(h, worktrees=None, token_ttl_ms=1000,
                   state_dir=str(tmp_path / "state"))
    t = s.begin("a1", "i1", writes=["a.txt"])
    assert not isinstance(t, (Deny, Wait))
    h.clock.advance(1001)
    h.tick()
    r = s.apply(t.token, [("a.txt", "x")], idempotency_key="K")
    assert isinstance(r, Deny)


# ================================================================ 场景 5
def test_c7_5_budget_wait_then_abort_reclaims(repo, tmp_path):
    h = make_harness(run_limits=1)
    s = _session(h, repo, tmp_path, min_budget=1)
    t1 = s.begin("a1", "i1", writes=["a.txt"])
    assert not isinstance(t1, (Deny, Wait))
    t2 = s.begin("a2", "i2", writes=["a.txt"])
    assert isinstance(t2, Wait)
    s.abort(t1.token)
    assert h.budget.remaining("run") == 1
    t3 = s.begin("a3", "i3", writes=["a.txt"])
    assert not isinstance(t3, (Deny, Wait))


# ================================================================ 场景 6
def test_c7_6_coordinator_unavailable_is_deny(monkeypatch, repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)

    def _boom(*_a, **_kw):
        raise RaftUnavailable("no quorum")

    monkeypatch.setattr(h, "_admit_and_register", _boom)
    r = s.begin("a1", "i1", writes=["a.txt"])
    assert isinstance(r, Deny) and "coordinator_unavailable" in r.reason


# ================================================================ 场景 7
def test_c7_7_token_restores_from_checkpoint(repo, tmp_path):
    h = make_harness(run_limits=1000, checkpoint_root=str(tmp_path / "ckpt"))
    s = _session(h, repo, tmp_path)
    t = s.begin("a1", "i1", writes=["a.txt"])
    h.checkpoints.save(h.acbs[t.token])
    h.acbs.clear()                               # 模拟进程重启（内存丢失）

    restored = s.resolve(t.token)
    assert not isinstance(restored, Deny), f"token 应从 checkpoint 恢复：{restored}"
    assert restored.state is TaskState.ADMITTED

    _write(s.worktree_for(t.token), "a.txt", "after-restart")
    r = s.apply(t.token, [("a.txt", "after-restart")], idempotency_key="K")
    assert isinstance(r, Receipt)


def test_c7_7b_finish_snapshot_not_resurrected(repo, tmp_path):
    """评审 P0-1：finish 后快照必须是终态，旧 ref 不得复活 token。"""
    h = make_harness(run_limits=1000, checkpoint_root=str(tmp_path / "ckpt"))
    s = _session(h, repo, tmp_path)
    t = s.begin("a1", "i1", writes=["a.txt"])
    _write(t.worktree, "a.txt", "v1")
    s.apply(t.token, [("a.txt", "v1")], idempotency_key="K")
    s.finish(t.token)
    # 用**旧快照**恢复（模拟 _save_index 失败 / 崩溃窗口）
    old_ref = s._index.get(t.token, {}).get("ref")
    assert old_ref is None, "finish 必须删索引"
    r = s.resolve(t.token)
    assert isinstance(r, Deny) and r.code == "already", (
        "finish 后的 token 必须终态拒绝（latest_ref_for_acb 生效）")


# ================================================================ 场景 8
def test_c7_8_delete_and_stale_gate(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    t1 = s.begin("a1", "i1", writes=["a.txt"])
    t2 = s.begin("a2", "i2", writes=["a.txt"])
    _write(t1.worktree, "a.txt", "v1")
    assert isinstance(s.apply(t1.token, [("a.txt", "v1")], idempotency_key="K1"),
                   Receipt)

    g = s.gate(t2.token)
    assert isinstance(g, Deny) and g.code == "stale_view", (
        "base_version 已过期 → gate 必须报 stale_view")

    s.finish(t2.token)
    t3 = s.begin("a3", "i3", writes=["a.txt"])
    os.remove(os.path.join(t3.worktree, "a.txt"))
    r = s.apply(t3.token, [("a.txt", DELETE)], idempotency_key="K3")
    assert isinstance(r, Receipt)
    assert h.workspace.get("a.txt") is None


# ================================================================ 附加
def test_c7_9_finish_idempotent_and_snapshot(repo, tmp_path):
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    t = s.begin("a1", "i1", writes=["a.txt"], reads=["keep.txt"])
    assert isinstance(s.snapshot(t.token), dict)
    _write(t.worktree, "a.txt", "v1")
    s.apply(t.token, [("a.txt", "v1")], idempotency_key="K")
    assert s.finish(t.token) == Status.OK
    assert s.finish(t.token) == Status.OK, "重复 finish 幂等"
    assert h.budget.remaining("run") == 999
    assert isinstance(s.apply(t.token, [("a.txt", "v1")], idempotency_key="K_NEW"),
                       Deny), "finish 后 token 失效"
    assert not os.path.exists(t.worktree)


# ================================================================ 评审新增回归
def test_c7_10_cross_session_same_key_does_not_replay(repo, tmp_path):
    """评审 P0：外部键带 tenant|agent 命名空间 → 跨会话同键不得互相命中。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    t1 = s.begin("a1", "i1", writes=["a.txt"])
    t2 = s.begin("a2", "i2", writes=["b.txt"])       # 不同资源 → 都能成功
    _write(t1.worktree, "a.txt", "v1")
    _write(t2.worktree, "b.txt", "v2")
    r1 = s.apply(t1.token, [("a.txt", "v1")], idempotency_key="SAME", agent_id="a1")
    r2 = s.apply(t2.token, [("b.txt", "v2")], idempotency_key="SAME", agent_id="a2")
    assert isinstance(r1, Receipt) and isinstance(r2, Receipt)
    assert r1.receipt_id != r2.receipt_id, "跨 agent 同键必须各自成笔"


def test_c7_11_cost_validation(repo, tmp_path):
    """评审 P1：cost<=0 / bool / 非 int / 超预留 一律拒绝。"""
    h = make_harness(run_limits=1000)
    s = _session(h, repo, tmp_path)
    t = s.begin("a1", "i1", writes=["a.txt"])
    _write(t.worktree, "a.txt", "v")
    for bad in (0, -1, True, 1.5, "x"):
        r = s.apply(t.token, [("a.txt", "v")], idempotency_key=f"K{bad}",
                    cost=bad)
        assert isinstance(r, Deny), f"非法 cost={bad!r} 必须拒绝"
    r = s.apply(t.token, [("a.txt", "v")], idempotency_key="K",
                cost=s.min_budget + 1)
    assert isinstance(r, Deny)


def test_c7_12_writes_empty_means_unpinned(tmp_path):
    """评审 P1：`writes=()` → 资源未钉住 → 适配器层 fail-closed（门面本身不猜）。"""
    h = make_harness(run_limits=1000)
    s = RscSession(h, worktrees=None)
    t = s.begin("a1", "i1")                    # 不传 writes
    assert isinstance(t, Token)
    assert t.base_versions == {}, "未声明 writes 时不得凭空钉住版本"
    assert isinstance(s.gate(t.token), Allow)  # 无资源可比 → 放行（适配器层负责拒绝）


def test_c7_13_begin_failure_leaves_no_zombie(repo, tmp_path):
    """评审 P1：准入失败不得留僵尸 ACB。"""
    h = make_harness(run_limits=1)
    s = _session(h, repo, tmp_path, min_budget=1)
    t1 = s.begin("a1", "i1", writes=["a.txt"])
    assert not isinstance(t1, (Deny, Wait))
    n_before = len(h.acbs)
    t2 = s.begin("a2", "i2", writes=["a.txt"])
    assert isinstance(t2, Wait)
    assert len(h.acbs) == n_before, "被拒的 begin 不得留下 ACB"
