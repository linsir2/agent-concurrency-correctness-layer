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
from harness.rebase import DELETE
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
    (base / "pyproject.toml").write_text("[tool]\n", encoding="utf-8")
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


# ---------------------------------------------------------------- S24-P1：多步
def test_steps_run_in_shared_tree_and_report_each(tmp_path, repo):
    session, _ = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    res = session.verify(tok.token, steps=[
        [sys.executable, "-c", "open('step1.txt','w').write('1')"],
        [sys.executable, "-c",
         "import os,sys; sys.exit(0 if os.path.exists('step1.txt') else 7)"],
    ])
    assert res.status.value == "OK", res.detail
    assert [s["exit_code"] for s in res.detail["steps"]] == [0, 0]


def test_steps_failfast_skips_remaining(tmp_path, repo):
    session, _ = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    marker = tmp_path / "step3.txt"
    res = session.verify(tok.token, steps=[
        [sys.executable, "-c", "pass"],
        [sys.executable, "-c", "raise SystemExit(3)"],
        [sys.executable, "-c", f"open({str(marker)!r}, 'w').write('1')"],
    ])
    assert res.status.value == "DENIED"
    assert res.detail["failed_step"] == 1
    assert res.detail["exit_code"] == 3
    assert res.detail["steps"][2] == {
        "cmd": [sys.executable, "-c",
                f"open({str(marker)!r}, 'w').write('1')"], "skipped": True}
    assert not marker.exists()


def test_steps_conflict_with_cmd_rejected(tmp_path, repo):
    session, _ = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    with pytest.raises(ValueError):
        session.verify(tok.token, [sys.executable, "-c", "pass"],
                       steps=[[sys.executable, "-c", "pass"]])


# ---------------------------------------------------------------- S24-P1：删除/新增
def test_ledger_delete_is_visible_in_verify_tree(tmp_path, repo):
    """A 删除 app/core.py 并改掉 api；B 的验证必须看到文件已不存在。"""
    session, h = _session(tmp_path, repo)
    ta = session.begin("agent-a", "delete",
                       writes=["app/core.py", "app/api.py"])
    ra = session.apply(ta.token, [("app/core.py", DELETE),
                                  ("app/api.py", "def handler():\n    return 3\n")],
                       idempotency_key="a:del", agent_id="agent-a")
    assert isinstance(ra, Receipt), ra
    session.finish(ta.token)
    assert h.workspace.deleted_resources() == ["app/core.py"]

    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    script = ("import os, sys\n"
              "sys.exit(0 if not os.path.exists('app/core.py') else 9)\n")
    res = session.verify(tb.token, [sys.executable, "-c", script])
    assert res.status.value == "OK", res.detail


def test_pending_delete_is_visible_in_verify_tree(tmp_path, repo):
    session, _ = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py", "app/api.py"])
    script = ("import os, sys\n"
              "sys.exit(0 if not os.path.exists('app/api.py') else 9)\n")
    res = session.verify(tok.token, [sys.executable, "-c", script],
                         pending=[("app/api.py", DELETE)])
    assert res.status.value == "OK", res.detail


def test_ledger_create_is_visible_in_verify_tree(tmp_path, repo):
    """A 提交新文件 app/new.py；B 的验证必须能 import 它。"""
    session, _ = _session(tmp_path, repo)
    ta = session.begin("agent-a", "new", writes=["app/new.py"])
    ra = session.apply(ta.token, [("app/new.py", "VALUE = 7\n")],
                       idempotency_key="a:new", agent_id="agent-a")
    assert isinstance(ra, Receipt), ra
    session.finish(ta.token)

    tb = session.begin("agent-b", "feature", writes=["app/feature.py"])
    script = "import app.new, sys; sys.exit(0 if app.new.VALUE == 7 else 9)\n"
    res = session.verify(tb.token, [sys.executable, "-c", script])
    assert res.status.value == "OK", res.detail


# ---------------------------------------------------------------- S24-P1：include/exclude
def test_include_keeps_init_and_root_config(tmp_path, repo):
    session, _ = _session(tmp_path, repo, verify_include=("app",))
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    script = ("import app, sys\n"
              "assert getattr(app, '__file__', None), 'missing __init__'\n"
              "assert open('pyproject.toml').read().strip() == '[tool]'\n")
    res = session.verify(tok.token, [sys.executable, "-c", script])
    assert res.status.value == "OK", res.detail


def test_include_empty_is_config_error(tmp_path, repo):
    session, _ = _session(tmp_path, repo, verify_include=("nope",))
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    res = session.verify(tok.token, [sys.executable, "-c", "pass"])
    assert res.status.value == "DENIED"
    assert res.detail["code"] == "verify_config_invalid"


def test_exclude_protected_or_wildcard_include_rejected(tmp_path, repo):
    with pytest.raises(ValueError):
        _session(tmp_path / "a", repo, verify_exclude=("__init__.py",))
    with pytest.raises(ValueError):
        _session(tmp_path / "b", repo, verify_include=("app/*.py",))


def test_diagnostics_off_by_default(tmp_path, repo):
    session, _ = _session(tmp_path, repo)
    tok = session.begin("a1", "i", writes=["app/feature.py"])
    res = session.verify(tok.token, [sys.executable, "-c", "pass"])
    assert "copy_ms" not in res.detail and "files" not in res.detail
    session2, _ = _session(tmp_path / "d", repo, verify_diagnostics=True)
    tok2 = session2.begin("a1", "i", writes=["app/feature.py"])
    res2 = session2.verify(tok2.token, [sys.executable, "-c", "pass"])
    assert {"copy_ms", "files", "run_ms"} <= set(res2.detail)
