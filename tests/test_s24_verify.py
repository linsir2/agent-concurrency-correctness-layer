"""S24-1：Verifier 门禁 —— 提交前在「候选副本 + 账本合并视图」上跑命令。

RIPR：
- Reachability：命令确实被执行（写标记文件证明）；
- Infection：红 / 超时 / 无工作区 / 已记录失败时真的改变结果；
- Propagation：`Deny(verify_failed…)` 以宿主可见形式返回；
- Revealability：账本版本未推进、预算未扣、收据带 `verified`。

核心场景（`examples/semantic_conflict` 的自动化版）：agent-A 改名后已提交，
agent-B 的工作区仍是旧快照 —— 只有「合并视图」能让 B 的验证变红。
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from conftest import make_harness

from harness import RscSession
from harness.facade import Deny, Receipt
from harness.worktree import WorktreeManager

CORE = "def compute_rate(x):\n    return x * 2\n"
API = "from app.core import compute_rate\n\n\ndef handler():\n    return compute_rate(3)\n"
FEATURE = "from app.core import compute_rate\n\n\ndef feature():\n    return compute_rate(5)\n"
# 仓库级检查：导入 app 包下所有模块（能抓「改名后仍被引用」这类语义冲突）
CHECK_PY = ("import importlib, pkgutil\nimport app\n"
            "for m in pkgutil.iter_modules(app.__path__):\n"
            "    importlib.import_module(f'app.{m.name}')\n")
CHECK = [sys.executable, "check.py"]


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True,
                   capture_output=True, text=True)


@pytest.fixture()
def repo(tmp_path):
    base = tmp_path / "repo"
    (base / "app").mkdir(parents=True)
    _git(base, "init", "-q")
    _git(base, "config", "user.email", "t@example.com")
    _git(base, "config", "user.name", "t")
    (base / "app/__init__.py").write_text("", encoding="utf-8")
    (base / "app/core.py").write_text(CORE, encoding="utf-8")
    (base / "app/api.py").write_text(API, encoding="utf-8")
    (base / "check.py").write_text(CHECK_PY, encoding="utf-8")
    _git(base, "add", ".")
    _git(base, "commit", "-qm", "init")
    return base


def _session(tmp_path, repo, **kw):
    h = make_harness(run_limits=100)
    return RscSession(h, tenant="t", min_budget=1,
                      worktrees=WorktreeManager(str(repo), str(tmp_path / "wt")),
                      state_dir=str(tmp_path / "state"), **kw), h


def _write(root, rel, text):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path) or root, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


# ---------------------------------------------------------------- 基础
def test_verify_runs_command_in_candidate_tree(tmp_path, repo):
    session, _ = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    _write(tok.worktree, "app/feature.py", "X = 1\n")
    marker = tmp_path / "ran.txt"
    res = session.verify(tok.token,
                         [sys.executable, "-c",
                          f"open({str(marker)!r}, 'w').write('1')"])
    assert res.status.value in ("OK", "VALID")
    assert marker.read_text() == "1"


def test_verify_green_allows_commit(tmp_path, repo):
    session, h = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    _write(tok.worktree, "app/feature.py", FEATURE)
    assert session.verify(tok.token, CHECK).status.value in ("OK", "VALID")
    r = session.apply(tok.token, [("app/feature.py", FEATURE)],
                      idempotency_key="k", agent_id="a1")
    assert isinstance(r, Receipt) and r.verified is True
    assert h.workspace.version("app/feature.py") == 1


def test_verify_red_blocks_commit_and_keeps_ledger(tmp_path, repo):
    """agent 自己写错（引用不存在的符号）→ 验证红 → 不得提交。"""
    session, h = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    bad = "from app.core import nope\n"
    _write(tok.worktree, "app/feature.py", bad)
    assert session.verify(tok.token, CHECK).status.value == "DENIED"
    reserved = h.budget.remaining("run")            # begin 预留后的余额
    r = session.apply(tok.token, [("app/feature.py", bad)],
                      idempotency_key="k", agent_id="a1")
    assert isinstance(r, Deny) and r.code == "verify_failed"
    assert h.workspace.version("app/feature.py") == 0
    assert h.budget.remaining("run") == reserved     # 未提交 → 不再扣预算


def test_auto_verify_on_apply_when_configured(tmp_path, repo):
    session, h = _session(tmp_path, repo, verify_cmd=CHECK)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    bad = "from app.core import nope\n"
    _write(tok.worktree, "app/feature.py", bad)
    r = session.apply(tok.token, [("app/feature.py", bad)],
                      idempotency_key="k", agent_id="a1")
    assert isinstance(r, Deny) and r.code == "verify_failed"
    assert h.workspace.version("app/feature.py") == 0


def test_verify_timeout_is_denied(tmp_path, repo):
    session, _ = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    res = session.verify(tok.token,
                         [sys.executable, "-c", "import time; time.sleep(5)"],
                         timeout_s=1)
    assert res.status.value == "DENIED"
    assert "verify_timeout" in res.detail["tail"]


def test_verify_requires_worktree(tmp_path):
    h = make_harness(run_limits=10)
    session = RscSession(h, tenant="t", min_budget=1, worktrees=None)
    tok = session.begin("a1", "i", writes=["a.md"])
    res = session.verify(tok.token, [sys.executable, "-c", "pass"])
    assert res.status.value == "DENIED"
    assert res.detail["reason"] == "verify_requires_worktree"


def test_verify_required_without_cmd_rejected_at_construction():
    h = make_harness(run_limits=10)
    with pytest.raises(ValueError):
        RscSession(h, verify_required=True)


def test_verify_cmd_accepts_string_and_rejects_empty(tmp_path, repo):
    session, _ = _session(tmp_path, repo, verify_cmd="python3 -c 'pass'")
    assert session.verify_cmd == ["python3", "-c", "pass"]
    with pytest.raises(ValueError):
        _session(tmp_path / "x", repo, verify_cmd="   ")
    with pytest.raises(TypeError):
        _session(tmp_path / "y", repo, verify_cmd=123)


# ---------------------------------------------------------------- 核心场景
def test_merged_view_catches_semantic_conflict(tmp_path, repo):
    """A 改名并同步改调用方后提交；B 新增调用旧名字的文件 → 合并视图必须变红。"""
    session, h = _session(tmp_path, repo, verify_cmd=CHECK)

    ta = session.begin("agent-a", "rename",
                       writes=["app/core.py", "app/api.py"])
    renamed = CORE.replace("compute_rate", "compute_rate_v2")
    fixed_api = API.replace("compute_rate", "compute_rate_v2")
    _write(ta.worktree, "app/core.py", renamed)
    _write(ta.worktree, "app/api.py", fixed_api)
    ra = session.apply(ta.token,
                       [("app/core.py", renamed), ("app/api.py", fixed_api)],
                       idempotency_key="a:1", agent_id="agent-a")
    assert isinstance(ra, Receipt), ra
    session.finish(ta.token)

    # B 只改 app/feature.py（与 A 完全不重叠），但调用旧名字
    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    _write(tb.worktree, "app/feature.py", FEATURE)
    rb = session.apply(tb.token, [("app/feature.py", FEATURE)],
                       idempotency_key="b:1", agent_id="agent-b")
    assert isinstance(rb, Deny) and "verify_failed" in rb.reason
    assert h.workspace.version("app/feature.py") == 0
    session.abort(tb.token, "semantic_conflict")


def test_merged_view_green_when_compatible(tmp_path, repo):
    """兼容改动（B 用新名字）→ 合并视图通过 → 正常提交且收据 verified。"""
    session, h = _session(tmp_path, repo, verify_cmd=CHECK)
    ta = session.begin("agent-a", "rename",
                       writes=["app/core.py", "app/api.py"])
    renamed = CORE.replace("compute_rate", "compute_rate_v2")
    fixed_api = API.replace("compute_rate", "compute_rate_v2")
    _write(ta.worktree, "app/core.py", renamed)
    _write(ta.worktree, "app/api.py", fixed_api)
    assert isinstance(session.apply(
        ta.token, [("app/core.py", renamed), ("app/api.py", fixed_api)],
        idempotency_key="a:1", agent_id="a"), Receipt)
    session.finish(ta.token)

    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    fixed = FEATURE.replace("compute_rate", "compute_rate_v2")
    _write(tb.worktree, "app/feature.py", fixed)
    rb = session.apply(tb.token, [("app/feature.py", fixed)],
                       idempotency_key="b:1", agent_id="agent-b")
    assert isinstance(rb, Receipt) and rb.verified is True
    assert h.workspace.version("app/feature.py") == 1


def test_verification_tree_does_not_touch_worktree(tmp_path, repo):
    """验证在副本里跑：工作区不被污染（命令写文件也不影响候选内容）。"""
    session, _ = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    _write(tok.worktree, "app/feature.py", "X = 1\n")
    target = os.path.join(tok.worktree, "app/feature.py")
    with open(target, "rb") as fh:
        before = fh.read()
    session.verify(tok.token,
                   [sys.executable, "-c",
                    "open('app/feature.py','w').write('POLLUTED')"])
    with open(target, "rb") as fh:
        after = fh.read()
    assert before == after
